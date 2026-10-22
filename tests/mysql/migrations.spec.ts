/**
 * MySqlWebhookStore's migrations, on the kit's MySQL StoreSchema (@nestjs/store-kit's own suite covers its machinery: the
 * lock and its bounded wait, crashes, metadata-lock waits): a new database and a rerun, processes starting together,
 * the DDL migrationStatements() lists against what migrate() runs and the tables it makes, a run that failed halfway
 * resuming where it stopped, sql_require_primary_key, a schema behind the code (and ahead of it), the options and
 * readiness in the store's words, and the script through drizzle-kit's MySQL migrator.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { drizzle } from 'drizzle-orm/mysql2';
import { migrate as drizzleMigrate } from 'drizzle-orm/mysql2/migrator';
import mysql from 'mysql2/promise';
import pg from 'pg';
import { fromPg } from '../../lib/postgres/index.js';
import { fromMysql2, MySqlWebhookStore, WebhookSchemaError, type SqlExecutor, type SqlTransaction } from '../../lib/mysql/index.js';
import { mysqlWebhookStoreSchema } from '../../lib/mysql/migrations/index.js';
import { onMysql, recording, testDatabase } from './support.js';

const { database, reason } = await testDatabase('mystore_migrations');
const pools: mysql.Pool[] = [];

// Each test's pools end with it: the server may be shared, and a file's pools would otherwise add up.
afterEach(async () => {
  await Promise.all(pools.splice(0).map((pool) => pool.end()));
});

/** A pool of its own, as each process has: one connection is all migrate() uses. */
const pool = (options: mysql.PoolOptions = {}) => {
  const opened = mysql.createPool({ uri: database!.url, connectionLimit: 1, ...options });
  pools.push(opened);
  return opened;
};

const store = (schema: string, options: { migrate?: boolean; executor?: SqlExecutor } = {}) =>
  new MySqlWebhookStore({ executor: options.executor ?? fromMysql2(pool()), schema, migrate: options.migrate });

const endpoint = (id: string) => ({
  id,
  tenant: 'shop-1',
  url: 'https://hooks.example.com/store',
  eventTypes: ['order.shipped'],
  description: null,
  enabled: true,
  disabledReason: null,
  failingSince: null,
  createdAt: 1,
  updatedAt: 1,
  secrets: [{ secret: 'whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw', createdAt: 1, expiresAt: null }],
});

const rows = async (sql: string, params: unknown[] = []) => (await database!.admin.query(sql, params))[0] as Array<Record<string, unknown>>;

/** The tables whose names start with `<schema>_`. */
const tables = async (schema: string) =>
  (
    await rows('SELECT TABLE_NAME AS name FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME', [
      schema.length + 1,
      `${schema}_`,
    ])
  ).map((row) => row.name);

/** The DDL a run sent, in order: not the lock, the reads, nor the progress records. */
const ddl = (statements: Array<{ text: string }>) => statements.map((statement) => statement.text).filter((text) => /^(CREATE|ALTER|DROP)\b/.test(text));

/** Everything about a schema's tables that migrations define, with the schema's name taken out, read through `db`. */
async function catalog(db: SqlTransaction, schema: string) {
  const read = (sql: string) => db.query(sql, [schema.length + 1, `${schema}_`]);
  const anonymize = (value: unknown) => JSON.parse(JSON.stringify(value).replaceAll(`${schema}_`, '<schema>_'));
  return anonymize({
    tables: await read(
      `SELECT TABLE_NAME AS table_name, ENGINE AS engine, TABLE_COLLATION AS collation_name
FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME`,
    ),
    columns: await read(
      `SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, CAST(ORDINAL_POSITION AS CHAR) AS position, COLUMN_TYPE AS type, IS_NULLABLE AS nullable,
  COLUMN_DEFAULT AS column_default, COLLATION_NAME AS collation_name, COLUMN_KEY AS column_key, EXTRA AS extra
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME, ORDINAL_POSITION`,
    ),
    indexes: await read(
      `SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, CAST(SEQ_IN_INDEX AS CHAR) AS seq, COLUMN_NAME AS column_name, CAST(NON_UNIQUE AS CHAR) AS non_unique
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, ?) = ? ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`,
    ),
    versions: await db.query(`SELECT CAST(version AS CHAR) AS version, name FROM \`${schema}_migrations\` WHERE applied_at IS NOT NULL ORDER BY version`),
  });
}

describe('migrate() on MySQL', () => {
  onMysql(reason);

  it("creates the kit's tables, the store's, and the version's record on a new database, and applies nothing the second time", async () => {
    const first = store('m_fresh');
    expect(await first.migrate()).toEqual([1]);
    expect(await tables('m_fresh')).toEqual([
      'm_fresh_deliveries',
      'm_fresh_delivery_attempts',
      'm_fresh_endpoints',
      'm_fresh_locks',
      'm_fresh_messages',
      'm_fresh_migrations',
    ]);
    expect(await rows('SELECT version, name, started, applied, applied_at > 0 AS stamped FROM m_fresh_migrations')).toEqual([
      { version: 1, name: 'initial', started: 4, applied: 4, stamped: 1 },
    ]);
    expect(MySqlWebhookStore.schemaVersion).toBe(1);

    const recorder = recording(fromMysql2(pool()));
    expect(await store('m_fresh', { executor: recorder.executor }).migrate()).toEqual([]);
    expect(recorder.statements.some((statement) => statement.text.includes('GET_LOCK'))).toBe(false);
    expect(await first.migrate()).toEqual([]);
    expect(await rows('SELECT COUNT(*) AS n FROM m_fresh_migrations')).toEqual([{ n: 1 }]);
    await expect(store('m_fresh', { migrate: false }).onModuleInit()).resolves.toBeUndefined();
  });

  it('applies the migrations once when processes start together, each on a connection of its own', async () => {
    const starting = Array.from({ length: 4 }, () => store('m_together'));
    const applied = await Promise.all(starting.map((s) => s.migrate()));
    expect(applied.filter((versions) => versions.length > 0)).toEqual([[1]]);
    expect(await rows('SELECT version FROM m_together_migrations')).toEqual([{ version: 1 }]);

    await Promise.all(starting.map((s) => s.onModuleInit()));
    await starting[1]!.createEndpoint(endpoint('ep_1'));
    expect(await starting[3]!.getEndpoint('ep_1')).toMatchObject({ id: 'ep_1', eventTypes: ['order.shipped'] });
  });

  it('runs the DDL migrationStatements() lists; the statements run one at a time with another tool make the same tables, which serve', async () => {
    const recorder = recording(fromMysql2(pool()));
    await store('m_migrated', { executor: recorder.executor }).migrate();
    const statements = MySqlWebhookStore.migrationStatements({ schema: 'm_migrated' });
    expect(statements).toEqual(mysqlWebhookStoreSchema.statements({ schema: 'm_migrated' }));
    expect(ddl(recorder.statements)).toEqual(statements.filter((statement) => !statement.startsWith('INSERT')));
    expect(statements.filter((statement) => statement.startsWith('INSERT'))).toEqual([
      "INSERT INTO `m_migrated_migrations` (version, name, applied_at) VALUES (1, 'initial', UNIX_TIMESTAMP() * 1000)",
    ]);

    // As a team applies them with its own tool: TypeORM's queryRunner.query() and mysql2 run one statement per call.
    const connection = await database!.admin.getConnection();
    try {
      for (const statement of MySqlWebhookStore.migrationStatements({ schema: 'm_script' })) {
        await connection.query(statement);
      }
    } finally {
      connection.release();
    }
    const executor = fromMysql2(pool());
    expect(await catalog(executor, 'm_script')).toEqual(await catalog(executor, 'm_migrated'));

    const scripted = store('m_script', { migrate: false, executor });
    await expect(scripted.onModuleInit()).resolves.toBeUndefined();
    await scripted.createEndpoint(endpoint('ep_1'));
    expect(await scripted.findSubscribedEndpoints('shop-1', 'order.shipped')).toHaveLength(1);
  });

  it('keys the fan-out, indexes what the worker and the log read, keeps text in utf8mb4 compared exactly, on InnoDB, without foreign keys', async () => {
    await store('m_keys').migrate();
    const indexes = await rows(
      `SELECT TABLE_NAME AS t, INDEX_NAME AS i, NON_UNIQUE AS non_unique, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS c
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('m_keys_endpoints', 'm_keys_messages', 'm_keys_deliveries', 'm_keys_delivery_attempts')
GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE ORDER BY TABLE_NAME, INDEX_NAME`,
    );
    expect(indexes.map((index) => `${index.t} ${index.i}${index.non_unique ? '' : ' UNIQUE'}: ${index.c}`)).toEqual([
      'm_keys_deliveries deliveries_created: created_at,id',
      'm_keys_deliveries deliveries_due: status,next_attempt_at,created_at,id',
      'm_keys_deliveries deliveries_endpoint: endpoint_id,created_at,id',
      'm_keys_deliveries deliveries_finished: completed_at',
      'm_keys_deliveries deliveries_message_endpoint UNIQUE: message_id,endpoint_id',
      'm_keys_deliveries deliveries_status: status,created_at,id',
      'm_keys_deliveries deliveries_tenant: tenant,created_at,id',
      'm_keys_deliveries PRIMARY UNIQUE: id',
      'm_keys_delivery_attempts delivery_attempts_delivery: delivery_id,at,attempt',
      'm_keys_delivery_attempts PRIMARY UNIQUE: seq',
      'm_keys_endpoints endpoints_tenant: tenant,created_at,id',
      'm_keys_endpoints PRIMARY UNIQUE: id',
      'm_keys_messages PRIMARY UNIQUE: id',
    ]);

    // Every text column of the store's tables in utf8mb4, compared by its characters: keys, types, statuses.
    const text = await rows(
      `SELECT DISTINCT CHARACTER_SET_NAME AS charset, COLLATION_NAME AS collation_name FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, 7) = 'm_keys_' AND TABLE_NAME NOT IN ('m_keys_migrations', 'm_keys_locks') AND COLLATION_NAME IS NOT NULL`,
    );
    expect(text).toEqual([{ charset: 'utf8mb4', collation_name: 'utf8mb4_0900_bin' }]);
    expect(
      await rows("SELECT COLUMN_NAME AS c, COLUMN_TYPE AS t FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'm_keys_deliveries' AND COLUMN_NAME IN ('id', 'tenant', 'lease_owner', 'type') ORDER BY COLUMN_NAME"),
    ).toEqual([
      { c: 'id', t: 'varchar(255)' },
      { c: 'lease_owner', t: 'varchar(255)' },
      { c: 'tenant', t: 'varchar(256)' },
      { c: 'type', t: 'text' },
    ]);
    expect(await rows("SELECT DISTINCT ENGINE AS engine FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND LEFT(TABLE_NAME, 7) = 'm_keys_'")).toEqual([
      { engine: 'InnoDB' },
    ]);
    expect(await rows('SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE()')).toEqual([]);
  });

  it('resumes a run that failed halfway at the statement it stopped at: the ones before it are applied once, and the next run applies the rest', async () => {
    await rows('CREATE TABLE m_resumed_deliveries (id int NOT NULL PRIMARY KEY)');
    const error = await store('m_resumed').migrate().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WebhookSchemaError);
    expect(error).toMatchObject({ schema: 'm_resumed', version: 0, requiredVersion: 1, cause: { errno: 1050 } });
    expect((error as Error).message).toBe(
      'MySqlWebhookStore: migrating schema "m_resumed" from version 0 to 1 stopped at migration 1 (initial), statement 3 of 4: ' +
        "Table 'm_resumed_deliveries' already exists. The statements before it are applied, and migrating again resumes at it.",
    );
    expect(await rows('SELECT version, started, applied, applied_at FROM m_resumed_migrations')).toEqual([{ version: 1, started: 2, applied: 2, applied_at: null }]);
    expect(await tables('m_resumed')).toEqual(['m_resumed_deliveries', 'm_resumed_endpoints', 'm_resumed_locks', 'm_resumed_messages', 'm_resumed_migrations']);
    await expect(store('m_resumed', { migrate: false }).onModuleInit()).rejects.toThrow('is at version 0');

    await rows('DROP TABLE m_resumed_deliveries');
    const recorder = recording(fromMysql2(pool()));
    expect(await store('m_resumed', { executor: recorder.executor }).migrate()).toEqual([1]);
    expect(ddl(recorder.statements).filter((text) => !text.startsWith('CREATE TABLE IF NOT EXISTS')).map((text) => text.split(' (')[0])).toEqual([
      'CREATE TABLE `m_resumed_deliveries`',
      'CREATE TABLE `m_resumed_delivery_attempts`',
    ]);
    const executor = fromMysql2(pool());
    await store('m_resume_reference', { executor }).migrate();
    expect(await catalog(executor, 'm_resumed')).toEqual(await catalog(executor, 'm_resume_reference'));
  });

  it("gives every table a primary key: the kit's and the store's migrate with sql_require_primary_key on", async () => {
    const strict = pool();
    strict.on('connection', (connection) => {
      connection.query('SET SESSION sql_require_primary_key = ON');
    });
    expect(await store('m_keyed', { executor: fromMysql2(strict) }).migrate()).toEqual([1]);
    expect(await tables('m_keyed')).toHaveLength(6);
  });
});

describe('a MySQL schema behind the code, and one ahead of it', () => {
  onMysql(reason);

  it('fails the startup (and every call) with a WebhookSchemaError that says how to migrate, changing nothing', async () => {
    const behind = store('m_behind', { migrate: false });
    const error = await behind.onModuleInit().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WebhookSchemaError);
    expect(error).toMatchObject({ name: 'WebhookSchemaError', schema: 'm_behind', version: 0, requiredVersion: 1 });
    expect((error as Error).message).toBe(
      'MySqlWebhookStore: schema "m_behind" is at version 0, and this version of @nestjs/webhooks needs version 1. Apply its migrations: ' +
        'set `migrate: true` to apply them at startup, run `npx @nestjs/webhooks migrate --url <database url> --schema m_behind`, ' +
        "or apply `MySqlWebhookStore.migrationSql({ schema: 'm_behind', from: 0 })` with your migration tool.",
    );
    await expect(behind.getEndpoint('any')).rejects.toThrow(WebhookSchemaError);
    await expect(behind.claimDeliveries({ owner: 'w1', now: 1, leaseMs: 1_000, limit: 10 })).rejects.toThrow(WebhookSchemaError);
    expect(await tables('m_behind')).toEqual([]);

    // Checked again at the next call: migrated meanwhile, it serves.
    await store('m_behind').migrate();
    expect(await behind.getEndpoint('any')).toBeUndefined();
  });

  it('serves a schema ahead of the code, which a newer version of the package migrated during a rolling deploy', async () => {
    await store('m_ahead').migrate();
    await rows("INSERT INTO m_ahead_migrations (version, name, applied_at) VALUES (2, 'newer', 1)");

    const older = store('m_ahead', { migrate: false });
    await expect(older.onModuleInit()).resolves.toBeUndefined();
    await older.createEndpoint(endpoint('ep_1'));
    expect(await older.getEndpoint('ep_1')).toMatchObject({ id: 'ep_1' });
    expect(await store('m_ahead').migrate()).toEqual([]);
  });
});

describe('options and readiness, in the store’s words', () => {
  onMysql(reason);

  it("don't migrate by default with NODE_ENV=production, and do otherwise", async () => {
    const previous = process.env.NODE_ENV;
    try {
      process.env.NODE_ENV = 'production';
      await expect(store('m_production').onModuleInit()).rejects.toThrow(WebhookSchemaError);
      process.env.NODE_ENV = 'development';
      await expect(store('m_production').onModuleInit()).resolves.toBeUndefined();
    } finally {
      process.env.NODE_ENV = previous;
    }
    expect(await tables('m_production')).toContain('m_production_deliveries');
  });

  it('take a schema of lowercase letters, digits and underscores, at most 40 characters: the prefix of the tables', () => {
    const executor = fromMysql2(pool());
    for (const schema of ['Mixed_Case', 'bad-name', '1st', '', 'x'.repeat(41), 'a`b', 'a$1']) {
      expect(() => new MySqlWebhookStore({ executor, schema })).toThrow(TypeError);
      expect(() => MySqlWebhookStore.migrationSql({ schema })).toThrow(
        `MySqlWebhookStore: invalid schema ${JSON.stringify(schema)}. Use lowercase letters, digits and underscores, not starting with a digit, at most 40 characters: ` +
          "the store's tables are named <schema>_<table> in the connection's database.",
      );
    }
    expect(MySqlWebhookStore.migrationStatements({ schema: 'x'.repeat(40) })[5]).toContain(`\`${'x'.repeat(40)}_delivery_attempts\``);
  });

  it('refuse an executor that is none or of PostgreSQL, naming the subpath to import from, and a migrate that is no boolean', async () => {
    expect(() => new MySqlWebhookStore({ executor: {} as SqlExecutor })).toThrow('MySqlWebhookStore: `executor` must be a SqlExecutor, such as fromMysql2(pool), fromDrizzle(db)');
    const postgres = new pg.Pool({ connectionString: 'postgres://postgres@127.0.0.1:1/none' });
    try {
      // A PostgreSQL executor is a compile error first (the options take SqlExecutor<'mysql'>), then a TypeError.
      // @ts-expect-error
      expect(() => new MySqlWebhookStore({ executor: fromPg(postgres) })).toThrow(
        "MySqlWebhookStore runs on MySQL, and `executor` is a PostgreSQL executor: import the executor from '@nestjs/webhooks/mysql' (fromMysql2, fromDrizzle, fromTypeOrm, fromPrisma or fromKysely).",
      );
    } finally {
      await postgres.end();
    }
    expect(() => new MySqlWebhookStore({ executor: fromMysql2(pool()), migrate: 'yes' as unknown as boolean })).toThrow(
      'MySqlWebhookStore: `migrate` must be true or false, not "yes".',
    );
  });

  it('refuse a connection whose sql_mode is lax, and one without a database, before any statement of the store', async () => {
    const lax = pool();
    lax.on('connection', (connection) => {
      connection.query("SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'");
    });
    const laxStore = store('m_lax', { executor: fromMysql2(lax) });
    await expect(laxStore.onModuleInit()).rejects.toThrow(
      'MySqlWebhookStore needs a strict sql_mode (STRICT_TRANS_TABLES, MySQL\'s default), and this connection\'s is "NO_ENGINE_SUBSTITUTION"',
    );
    await expect(laxStore.getEndpoint('any')).rejects.toThrow('needs a strict sql_mode');

    const url = new URL(database!.url);
    url.pathname = '/';
    const noDatabase = mysql.createPool({ uri: url.toString(), connectionLimit: 1 });
    pools.push(noDatabase);
    await expect(store('m_nowhere', { executor: fromMysql2(noDatabase) }).onModuleInit()).rejects.toThrow(
      "MySqlWebhookStore keeps its tables in the connection's database, and this connection has none",
    );
    expect(await tables('m_lax')).toEqual([]);
  });
});

describe('migrationSql() and migrationStatements()', () => {
  it("print the default schema from a new database, with a header that says they don't run in one transaction, a range, and never a downgrade", () => {
    const script = MySqlWebhookStore.migrationSql();
    expect(script).toMatch(
      /^-- @nestjs\/webhooks: MySqlWebhookStore's schema "nest_webhooks" \(tables nest_webhooks_\*\), from version 0 to 1\.\n-- The statements don't run in one transaction: MySQL commits each DDL statement on its own\. Apply them in order, each once\.\n\n/,
    );
    expect(script).toContain('CREATE TABLE `nest_webhooks_deliveries` (');
    expect(script.endsWith("INSERT INTO `nest_webhooks_migrations` (version, name, applied_at) VALUES (1, 'initial', UNIX_TIMESTAMP() * 1000);\n")).toBe(true);
    expect(MySqlWebhookStore.migrationSql({ statementBreakpoints: true }).split('\n--> statement-breakpoint\n')).toHaveLength(MySqlWebhookStore.migrationStatements().length);
    expect(MySqlWebhookStore.migrationStatements({ from: 1 })).toEqual([]);

    expect(() => MySqlWebhookStore.migrationSql({ from: 1, to: 0 })).toThrow(RangeError);
    expect(() => MySqlWebhookStore.migrationSql({ from: 1, to: 0 })).toThrow("downgrades aren't supported");
    expect(() => MySqlWebhookStore.migrationStatements({ to: 2 })).toThrow('no migrations lead from version 0 to 2');
  });
});

describe("drizzle-kit's statement breakpoints, through Drizzle's MySQL migrator", () => {
  onMysql(reason);

  /** A drizzle-kit migrations folder with one custom migration, as `drizzle-kit generate --custom` makes it. */
  function migrationsFolder(sql: string): string {
    const folder = mkdtempSync(join(tmpdir(), 'whk-drizzle-mysql-'));
    mkdirSync(join(folder, 'meta'));
    writeFileSync(join(folder, '0000_webhooks.sql'), sql);
    writeFileSync(
      join(folder, 'meta', '_journal.json'),
      JSON.stringify({ version: '5', dialect: 'mysql', entries: [{ idx: 0, version: '5', when: 1790000000000, tag: '0000_webhooks', breakpoints: true }] }),
    );
    return folder;
  }

  it("runs migrationSql({ statementBreakpoints: true }) one statement at a time: the tables are migrate()'s, and the store serves on them", async () => {
    const executor = fromMysql2(pool());
    await store('m_by_kit', { executor }).migrate();
    const folder = migrationsFolder(MySqlWebhookStore.migrationSql({ schema: 'm_by_drizzle', statementBreakpoints: true }));
    try {
      await drizzleMigrate(drizzle(pool()), { migrationsFolder: folder, migrationsTable: 'drizzle_journal_by_drizzle' });
      expect(await catalog(executor, 'm_by_drizzle')).toEqual(await catalog(executor, 'm_by_kit'));
      const served = store('m_by_drizzle', { executor, migrate: false });
      await expect(served.onModuleInit()).resolves.toBeUndefined();
      await served.createEndpoint(endpoint('ep_1'));
      expect(await served.listEndpoints({ tenant: 'shop-1' })).toMatchObject([{ id: 'ep_1' }]);
    } finally {
      rmSync(folder, { recursive: true, force: true });
    }
  });

  it('is what mysql2 needs: the script without breakpoints fails there, as one call runs one statement', async () => {
    const folder = migrationsFolder(MySqlWebhookStore.migrationSql({ schema: 'm_one_call' }));
    try {
      await expect(drizzleMigrate(drizzle(pool()), { migrationsFolder: folder, migrationsTable: 'drizzle_journal_one_call' })).rejects.toSatisfy((error: Error) =>
        /SQL syntax/.test(`${error.message} ${(error.cause as Error | undefined)?.message}`),
      );
      expect(await tables('m_one_call')).toEqual([]);
    } finally {
      rmSync(folder, { recursive: true, force: true });
    }
  });
});
