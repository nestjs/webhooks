import { Logger, type OnModuleInit } from '@nestjs/common';
import {
  columns,
  quoteIdentifier,
  quoteTable,
  retryOnDeadlock,
  SqlParams,
  toBool,
  toInt,
  toJson,
  type MigrationSqlOptions,
  type MigrationStatementsOptions,
  type SqlExecutor,
  type StoreReadiness,
} from '@nestjs/store-kit/mysql';
import type { WebhookClaimedDelivery, WebhookClaimRequest, WebhookDeliveryStore, WebhookDeliveryStoreStats, WebhookDeliveryUpdate } from '../interfaces/webhook-delivery-store.interface.js';
import type { WebhookDelivery, WebhookDeliveryAttempt, WebhookDeliveryFilter, WebhookDeliveryQuery } from '../interfaces/webhook-delivery.interface.js';
import type {
  WebhookEndpointFailure,
  WebhookEndpointPatch,
  WebhookEndpointRecord,
  WebhookEndpointSecret,
  WebhookEndpointStore,
} from '../interfaces/webhook-endpoint-store.interface.js';
import type { WebhookEndpointQuery } from '../interfaces/webhook-endpoint.interface.js';
import type { WebhookMessage } from '../interfaces/webhook-message.interface.js';
import type { WebhooksStorage } from '../storage/webhooks.storage.js';
import {
  DELIVERY_ATTEMPT_COLUMNS,
  DELIVERY_COLUMNS,
  ENDPOINT_COLUMNS,
  MESSAGE_COLUMNS,
  toDelivery,
  toDeliveryAttempt,
  toEndpointRecord,
  toMessage,
  type SqlRow,
} from '../utils/sql-rows.util.js';
import { assertDeliveryFilter, DEFAULT_PAGE_SIZE, endpointFailure, inEndpointOrder, rotateSecrets, wholeMs } from '../utils/store-rules.util.js';
import type { MySqlWebhookStoreOptions } from './interfaces/mysql-webhook-store-options.interface.js';
import { mysqlWebhookStoreSchema } from './migrations/index.js';
import { KEY_LENGTHS } from './migrations/initial.migration.js';

const STORE = 'MySqlWebhookStore';

/** Deliveries per insert: 14 parameters each, so 14,000 a statement, well within Prisma's 65,535 (its adapter prepares). */
const DELIVERIES_PER_INSERT = 1_000;

/** Deliveries (and orphaned messages) retention deletes per transaction, so no transaction grows with the log. */
const PRUNED_PER_TRANSACTION = 1_000;

/** A claimed delivery's message, selected next to it under names of its own. */
const MESSAGE_PREFIX = 'm_';
const CLAIMED_MESSAGE_COLUMNS = MESSAGE_COLUMNS.map((column) => `CAST(m.${quoteIdentifier(column)} AS CHAR) AS ${quoteIdentifier(`${MESSAGE_PREFIX}${column}`)}`).join(', ');

/**
 * The first-party `WebhookEndpointStore` and `WebhookDeliveryStore` on MySQL (8.4 LTS and 9.x), through the client the
 * application already has (`fromMysql2()`, `fromDrizzle()`, `fromTypeOrm()`, `fromPrisma()`, `fromKysely()`). Its tables
 * live in the connection's database, named after its `schema` (`nest_webhooks_endpoints`...), which its migrations
 * create and bring up to date. Endpoint secrets are stored as the package hands them over: sealed when `WebhooksModule`
 * has `encryption` keys.
 *
 * ```ts
 * @Module({
 *   imports: [OutboxModule.forRoot(), WebhooksModule.forRoot()],
 *   providers: [
 *     {
 *       provide: MySqlWebhookStore,
 *       inject: [getDrizzleToken(), WebhooksStorage],
 *       useFactory: (db: Database, storage: WebhooksStorage) => new MySqlWebhookStore({ executor: fromDrizzle(db) }, storage),
 *     },
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * Given `storage`, it registers itself for both contracts (`storage.registerSource({ endpoints: this, deliveries: this
 * })`). It checks the server and its tables, or migrates them (`migrate`), in `onModuleInit` (so before the worker
 * starts), or at its first call outside Nest. Its own transactions run READ COMMITTED, and run again when MySQL rolls
 * one back to break a deadlock; no method takes the application's transaction.
 */
export class MySqlWebhookStore implements WebhookEndpointStore, WebhookDeliveryStore, OnModuleInit {
  /**
   * The SQL of the store's migrations as one script, for teams that apply migrations with their own tool (drizzle-kit,
   * Flyway...) and run the store with `migrate: false`: from version `from` (default `0`, a new database) to `to`
   * (default: the version this version of the package needs), with the bookkeeping that tells the store which versions
   * its tables have. MySQL commits each DDL statement on its own, so they don't run in one transaction: apply them in
   * order, each once. With `statementBreakpoints`, drizzle-kit's `--> statement-breakpoint` separates them, for a custom
   * drizzle-kit migration, which its MySQL migrator needs (mysql2 runs one statement per call). Downgrades aren't
   * supported.
   *
   * ```ts
   * // drizzle/0004_webhooks.sql, created empty by `npx drizzle-kit generate --custom --name=webhooks`
   * writeFileSync('drizzle/0004_webhooks.sql', MySqlWebhookStore.migrationSql({ statementBreakpoints: true }));
   * ```
   */
  static migrationSql(options: MigrationSqlOptions = {}): string {
    return mysqlWebhookStoreSchema.sql(options);
  }

  /**
   * `migrationSql()`'s statements, one per string, for a migration tool that runs one statement per call (TypeORM's
   * `queryRunner.query()`, mysql2): run them in order, each once.
   *
   * ```ts
   * export class Webhooks1790000000000 implements MigrationInterface {
   *   async up(queryRunner: QueryRunner): Promise<void> {
   *     for (const statement of MySqlWebhookStore.migrationStatements()) {
   *       await queryRunner.query(statement);
   *     }
   *   }
   * }
   * ```
   */
  static migrationStatements(options: MigrationStatementsOptions = {}): string[] {
    return mysqlWebhookStoreSchema.statements(options);
  }

  /** The schema version this version of the package needs: its last migration. */
  static readonly schemaVersion = mysqlWebhookStoreSchema.latest;

  private readonly logger = new Logger('WebhooksModule');
  private readonly executor: SqlExecutor;
  private readonly t: Record<'endpoints' | 'messages' | 'deliveries' | 'attempts', string>;
  /** The server checked and the tables migrated (`migrate`) or checked before the first statement: see `onModuleInit()`. */
  private readonly readiness: StoreReadiness;

  constructor(options: MySqlWebhookStoreOptions, storage?: WebhooksStorage) {
    const resolved = mysqlWebhookStoreSchema.resolveOptions(options);
    this.executor = resolved.executor;
    const t = (table: string) => quoteTable(resolved.schema, table, STORE);
    this.t = {
      endpoints: t('endpoints'),
      messages: t('messages'),
      deliveries: t('deliveries'),
      attempts: t('delivery_attempts'),
    };
    this.readiness = mysqlWebhookStoreSchema.readiness({ ...resolved, logger: this.logger });
    storage?.registerSource({ endpoints: this, deliveries: this });
  }

  /**
   * Checks the server (MySQL 8.0.19 or later, not MariaDB; a strict `sql_mode`; a current database), then migrates the
   * tables (`migrate`) or checks them, before the worker starts: startup fails if it can't serve.
   */
  async onModuleInit(): Promise<void> {
    await this.readiness.ready();
  }

  /**
   * Applies the migrations the tables haven't had yet, whatever `migrate` says, one statement at a time under a lock
   * (`GET_LOCK()`): of processes that migrate together, one applies them, and a run that failed resumes where it
   * stopped. Resolves to the versions it applied (`[]`: none were pending).
   */
  migrate(): Promise<number[]> {
    return this.readiness.migrate();
  }

  // ---------------------------------------------------------------- endpoints

  async createEndpoint(endpoint: WebhookEndpointRecord): Promise<void> {
    checkKey("an endpoint's id", endpoint.id, KEY_LENGTHS.id);
    checkKey("an endpoint's tenant", endpoint.tenant, KEY_LENGTHS.tenant);
    await this.readiness.ready();
    const p = new SqlParams();
    await this.executor.execute(
      `INSERT INTO ${this.t.endpoints} (${names(ENDPOINT_COLUMNS)})
VALUES (${p.text(endpoint.id)}, ${p.text(endpoint.tenant)}, ${p.text(endpoint.url)}, ${p.json(endpoint.eventTypes)}, ${p.text(endpoint.description)},
  ${p.bool(endpoint.enabled)}, ${p.text(endpoint.disabledReason)}, ${ms(p, endpoint.failingSince)}, ${p.json(endpoint.secrets)},
  ${ms(p, endpoint.createdAt)}, ${ms(p, endpoint.updatedAt)})`,
      p.values,
    );
  }

  async getEndpoint(id: string): Promise<WebhookEndpointRecord | undefined> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<SqlRow>(`SELECT ${columns(ENDPOINT_COLUMNS, 'e')} FROM ${this.t.endpoints} e WHERE e.id = ${p.text(id)}`, p.values);
    return row && toEndpointRecord(row);
  }

  async listEndpoints({ tenant, limit = DEFAULT_PAGE_SIZE, offset = 0 }: WebhookEndpointQuery): Promise<WebhookEndpointRecord[]> {
    await this.readiness.ready();
    const p = new SqlParams();
    const where = tenant === undefined ? '' : ` WHERE ${p.equals('e.tenant', tenant)}`;
    const rows = await this.executor.query<SqlRow>(
      `SELECT ${columns(ENDPOINT_COLUMNS, 'e')} FROM ${this.t.endpoints} e${where}
ORDER BY e.created_at DESC, e.id DESC LIMIT ${p.limit(limit)} OFFSET ${p.limit(offset)}`,
      p.values,
    );
    return rows.map(toEndpointRecord);
  }

  async findSubscribedEndpoints(tenant: string | null, type: string): Promise<WebhookEndpointRecord[]> {
    await this.readiness.ready();
    // Exactly this tenant: a message without one reaches only endpoints without one. JSON strings compare exactly.
    const p = new SqlParams();
    const rows = await this.executor.query<SqlRow>(
      `SELECT ${columns(ENDPOINT_COLUMNS, 'e')} FROM ${this.t.endpoints} e
WHERE e.enabled AND ${p.equals('e.tenant', tenant)} AND (JSON_CONTAINS(e.event_types, JSON_ARRAY(${p.text(type)})) OR JSON_CONTAINS(e.event_types, '["*"]'))
ORDER BY e.created_at, e.id`,
      p.values,
    );
    return rows.map(toEndpointRecord);
  }

  async updateEndpoint(id: string, patch: WebhookEndpointPatch, now: number): Promise<WebhookEndpointRecord | undefined> {
    await this.readiness.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      // Fields the patch leaves out (undefined) keep their value. The row stays locked until the commit: the read below
      // returns the endpoint as this update left it.
      const p = new SqlParams();
      const set = [
        ...(patch.url !== undefined ? [`url = ${p.text(patch.url)}`] : []),
        ...(patch.eventTypes !== undefined ? [`event_types = ${p.json(patch.eventTypes)}`] : []),
        ...(patch.description !== undefined ? [`description = ${p.text(patch.description)}`] : []),
        ...(patch.enabled !== undefined ? [`enabled = ${p.bool(patch.enabled)}`] : []),
        ...(patch.disabledReason !== undefined ? [`disabled_reason = ${p.text(patch.disabledReason)}`] : []),
        ...(patch.failingSince !== undefined ? [`failing_since = ${ms(p, patch.failingSince)}`] : []),
        `updated_at = ${ms(p, now)}`,
      ];
      const { affectedRows } = await tx.execute(`UPDATE ${this.t.endpoints} SET ${set.join(', ')} WHERE id = ${p.text(id)}`, p.values);
      if (affectedRows === 0) {
        return undefined;
      }

      const r = new SqlParams();
      const [row] = await tx.query<SqlRow>(`SELECT ${columns(ENDPOINT_COLUMNS, 'e')} FROM ${this.t.endpoints} e WHERE e.id = ${r.text(id)}`, r.values);
      return row && toEndpointRecord(row);
    });
  }

  async deleteEndpoint(id: string): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const { affectedRows } = await this.executor.execute(`DELETE FROM ${this.t.endpoints} WHERE id = ${p.text(id)}`, p.values);
    return affectedRows === 1;
  }

  async addEndpointSecret(id: string, secret: WebhookEndpointSecret, expireOthersAt: number, now: number): Promise<boolean> {
    await this.readiness.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      // The row lock serializes rotations: the second one reads the first one's secret.
      const p = new SqlParams();
      const [row] = await tx.query<SqlRow>(`SELECT ${columns(['secrets'], 'e')} FROM ${this.t.endpoints} e WHERE e.id = ${p.text(id)} FOR UPDATE`, p.values);
      if (!row) {
        return false;
      }

      const secrets = rotateSecrets(toJson(row.secrets) as WebhookEndpointSecret[], secret, expireOthersAt, now);
      const u = new SqlParams();
      await tx.execute(`UPDATE ${this.t.endpoints} SET secrets = ${u.json(secrets)} WHERE id = ${u.text(id)}`, u.values);
      return true;
    });
  }

  async recordEndpointFailure(id: string, failure: WebhookEndpointFailure): Promise<boolean> {
    await this.readiness.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      // The row lock serializes failures: of several past the threshold at once, the first disables the endpoint, and
      // the others read it disabled.
      const p = new SqlParams();
      const [row] = await tx.query<SqlRow>(
        `SELECT ${columns(['enabled', 'failing_since'], 'e')} FROM ${this.t.endpoints} e WHERE e.id = ${p.text(id)} FOR UPDATE`,
        p.values,
      );
      if (!row) {
        return false;
      }

      const failingSince = toInt(row.failing_since);
      const outcome = endpointFailure({ enabled: toBool(row.enabled), failingSince }, failure);
      // An endpoint failing for days isn't rewritten (with its secrets) at every failed attempt.
      if (outcome.failingSince === failingSince && !outcome.disable) {
        return false;
      }

      const u = new SqlParams();
      const set = [
        `failing_since = ${ms(u, outcome.failingSince)}`,
        ...(outcome.disable ? [`enabled = ${u.bool(false)}`, `disabled_reason = ${u.text(failure.reason)}`, `updated_at = ${ms(u, failure.at)}`] : []),
      ];
      await tx.execute(`UPDATE ${this.t.endpoints} SET ${set.join(', ')} WHERE id = ${u.text(id)}`, u.values);
      return outcome.disable;
    });
  }

  async recordEndpointSuccess(id: string): Promise<void> {
    await this.readiness.ready();
    const p = new SqlParams();
    await this.executor.execute(`UPDATE ${this.t.endpoints} SET failing_since = NULL WHERE id = ${p.text(id)} AND failing_since IS NOT NULL`, p.values);
  }

  // ---------------------------------------------------------------- deliveries

  async createDeliveries(message: WebhookMessage, deliveries: readonly WebhookDelivery[]): Promise<number> {
    checkKey("a message's id", message.id, KEY_LENGTHS.id);
    checkKey("a message's tenant", message.tenant, KEY_LENGTHS.tenant);
    for (const delivery of deliveries) {
      checkKey("a delivery's id", delivery.id, KEY_LENGTHS.id);
      checkKey("a delivery's message id", delivery.messageId, KEY_LENGTHS.id);
      checkKey("a delivery's endpoint id", delivery.endpointId, KEY_LENGTHS.id);
      checkKey("a delivery's tenant", delivery.tenant, KEY_LENGTHS.tenant);
    }
    await this.readiness.ready();
    const unique = firstPerPair(inEndpointOrder(deliveries));

    return retryOnDeadlock(this.executor, async (tx) => {
      // The message, unless it exists (never INSERT IGNORE, which turns other errors into warnings). Inserted or found,
      // its row is this transaction's until it commits: fan-outs of one message take turns, and each reads the
      // deliveries of the ones before it.
      const m = new SqlParams();
      await tx.execute(
        `INSERT INTO ${this.t.messages} (${names(MESSAGE_COLUMNS)})
VALUES (${m.text(message.id)}, ${m.text(message.type)}, ${m.text(message.tenant)}, ${m.text(message.body)}, ${ms(m, message.createdAt)})
ON DUPLICATE KEY UPDATE id = id`,
        m.values,
      );
      if (unique.length === 0) {
        return 0;
      }

      // The deliveries a fan-out of the message inserted before: a fan-out that runs again inserts only what is missing.
      const e = new SqlParams();
      const existing = await tx.query<SqlRow>(
        `SELECT ${columns(['message_id', 'endpoint_id'], 'd')} FROM ${this.t.deliveries} d WHERE ${e.in('d.message_id', [...new Set(unique.map((d) => d.messageId))])}`,
        e.values,
      );
      const inserted = new Set(existing.map((row) => pairKey(row.message_id!, row.endpoint_id!)));
      const missing = unique.filter((d) => !inserted.has(pairKey(d.messageId, d.endpointId)));

      let created = 0;
      for (let start = 0; start < missing.length; start += DELIVERIES_PER_INSERT) {
        const p = new SqlParams();
        const rows = missing.slice(start, start + DELIVERIES_PER_INSERT).map(
          (d) =>
            `(${p.text(d.id)}, ${p.text(d.messageId)}, ${p.text(d.endpointId)}, ${p.text(d.tenant)}, ${p.text(d.type)}, ${p.text(d.status)}, ` +
            `${p.int(d.attempts)}, ${ms(p, d.nextAttemptAt)}, ${ms(p, d.lastAttemptAt)}, ${p.int(d.lastStatusCode)}, ${p.text(d.lastError)}, ` +
            `${p.text(d.failureReason)}, ${ms(p, d.createdAt)}, ${ms(p, d.completedAt)})`,
        );
        const { affectedRows } = await tx.execute(`INSERT INTO ${this.t.deliveries} (${names(DELIVERY_COLUMNS)})\nVALUES ${rows.join(',\n  ')}`, p.values);
        created += affectedRows;
      }
      return created;
    });
  }

  async claimDeliveries({ owner, now, leaseMs, limit }: WebhookClaimRequest): Promise<WebhookClaimedDelivery[]> {
    checkKey("a lease's owner", owner, KEY_LENGTHS.owner);
    await this.readiness.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      // SKIP LOCKED: workers claiming at once take disjoint batches, and never wait for each other. A row whose lease
      // another claim committed meanwhile is read as it is now, and no longer matches. The index is pinned: through
      // server-side prepared statements (Prisma's adapter), MySQL scans and sorts a small table instead, and a locking
      // read then locks every due delivery until it commits, which the other workers skip.
      const p = new SqlParams();
      const due = await tx.query<SqlRow>(
        `SELECT ${columns(['id'], 'd')} FROM ${this.t.deliveries} d FORCE INDEX (deliveries_due)
WHERE d.status = 'pending' AND d.next_attempt_at <= ${ms(p, now)} AND (d.lease_until IS NULL OR d.lease_until <= ${ms(p, now)})
ORDER BY d.next_attempt_at, d.created_at, d.id
LIMIT ${p.limit(limit)}
FOR UPDATE SKIP LOCKED`,
        p.values,
      );
      if (due.length === 0) {
        return [];
      }

      const ids = due.map((row) => row.id!);
      const u = new SqlParams();
      await tx.execute(`UPDATE ${this.t.deliveries} SET lease_owner = ${u.text(owner)}, lease_until = ${ms(u, now + leaseMs)} WHERE ${u.in('id', ids)}`, u.values);

      const r = new SqlParams();
      const rows = await tx.query<SqlRow>(
        `SELECT ${columns(DELIVERY_COLUMNS, 'd')}, ${CLAIMED_MESSAGE_COLUMNS}
FROM ${this.t.deliveries} d JOIN ${this.t.messages} m ON m.id = d.message_id
WHERE ${r.in('d.id', ids)}
ORDER BY d.next_attempt_at, d.created_at, d.id`,
        r.values,
      );
      return rows.map((row) => ({ delivery: toDelivery(row), message: toMessage(row, MESSAGE_PREFIX) }));
    });
  }

  async recordDeliveryAttempt(id: string, owner: string, update: WebhookDeliveryUpdate): Promise<boolean> {
    await this.readiness.ready();
    const { attempt } = update;
    return retryOnDeadlock(this.executor, async (tx) => {
      const p = new SqlParams();
      const set = [
        `status = ${p.text(update.status)}`,
        `attempts = ${p.int(update.attempts)}`,
        `next_attempt_at = ${ms(p, update.nextAttemptAt)}`,
        `failure_reason = ${p.text(update.failureReason)}`,
        `completed_at = ${ms(p, update.completedAt)}`,
        'lease_owner = NULL',
        'lease_until = NULL',
        ...(attempt
          ? [`last_attempt_at = ${ms(p, attempt.at)}`, `last_status_code = ${p.int(attempt.statusCode)}`, `last_error = ${p.text(attempt.error)}`]
          : update.error !== undefined
            ? [`last_error = ${p.text(update.error)}`]
            : []),
      ];
      // Fenced by the lease: a worker that lost it to another writes nothing, its attempt included. The rows the UPDATE
      // matched, not the ones it changed: the kit's executors count them so.
      const { affectedRows } = await tx.execute(`UPDATE ${this.t.deliveries} SET ${set.join(', ')} WHERE id = ${p.text(id)} AND lease_owner = ${p.text(owner)}`, p.values);
      if (affectedRows !== 1) {
        return false;
      }

      // In the same transaction: the delivery's new state and its log row commit together.
      if (attempt) {
        const a = new SqlParams();
        await tx.execute(
          `INSERT INTO ${this.t.attempts} (${names(DELIVERY_ATTEMPT_COLUMNS)})
VALUES (${a.text(id)}, ${a.int(attempt.attempt)}, ${ms(a, attempt.at)}, ${a.int(Math.trunc(attempt.durationMs))}, ${a.int(attempt.statusCode)}, ${a.text(attempt.response)}, ${a.text(attempt.error)})`,
          a.values,
        );
      }
      return true;
    });
  }

  async releaseDeliveries(ids: readonly string[], owner: string, nextAttemptAt?: number): Promise<number> {
    if (ids.length === 0) {
      return 0;
    }

    await this.readiness.ready();
    return retryOnDeadlock(this.executor, async (tx) => {
      // Never earlier than it was: GREATEST() is NULL when either side is.
      const p = new SqlParams();
      const postpone = nextAttemptAt === undefined ? '' : `, next_attempt_at = GREATEST(COALESCE(next_attempt_at, ${ms(p, nextAttemptAt)}), ${ms(p, nextAttemptAt)})`;
      const { affectedRows } = await tx.execute(
        `UPDATE ${this.t.deliveries} SET lease_owner = NULL, lease_until = NULL${postpone}
WHERE ${p.in('id', [...new Set(ids)])} AND lease_owner = ${p.text(owner)}`,
        p.values,
      );
      return affectedRows;
    });
  }

  async getDelivery(id: string): Promise<WebhookDelivery | undefined> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<SqlRow>(`SELECT ${columns(DELIVERY_COLUMNS, 'd')} FROM ${this.t.deliveries} d WHERE d.id = ${p.text(id)}`, p.values);
    return row && toDelivery(row);
  }

  async getMessage(id: string): Promise<WebhookMessage | undefined> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<SqlRow>(`SELECT ${columns(MESSAGE_COLUMNS, 'm')} FROM ${this.t.messages} m WHERE m.id = ${p.text(id)}`, p.values);
    return row && toMessage(row);
  }

  async listDeliveries({ tenant, endpointId, messageId, status, type, failureReason, lastStatusCode, limit = DEFAULT_PAGE_SIZE, offset = 0 }: WebhookDeliveryQuery): Promise<WebhookDelivery[]> {
    await this.readiness.ready();
    const p = new SqlParams();
    const where = [
      ...(tenant !== undefined ? [p.equals('d.tenant', tenant)] : []),
      ...(endpointId !== undefined ? [`d.endpoint_id = ${p.text(endpointId)}`] : []),
      ...(messageId !== undefined ? [`d.message_id = ${p.text(messageId)}`] : []),
      ...(status !== undefined ? [`d.status = ${p.text(status)}`] : []),
      ...(type !== undefined ? [`d.type = ${p.text(type)}`] : []),
      ...(failureReason !== undefined ? [p.equals('d.failure_reason', failureReason)] : []),
      ...(lastStatusCode !== undefined ? [equalsInt(p, 'd.last_status_code', lastStatusCode)] : []),
    ];
    const rows = await this.executor.query<SqlRow>(
      `SELECT ${columns(DELIVERY_COLUMNS, 'd')} FROM ${this.t.deliveries} d${where.length > 0 ? ` WHERE ${where.join(' AND ')}` : ''}
ORDER BY d.created_at DESC, d.id DESC LIMIT ${p.limit(limit)} OFFSET ${p.limit(offset)}`,
      p.values,
    );
    return rows.map(toDelivery);
  }

  async listDeliveryAttempts(deliveryId: string): Promise<WebhookDeliveryAttempt[]> {
    await this.readiness.ready();
    const p = new SqlParams();
    const rows = await this.executor.query<SqlRow>(
      `SELECT ${columns(DELIVERY_ATTEMPT_COLUMNS, 'a')} FROM ${this.t.attempts} a WHERE a.delivery_id = ${p.text(deliveryId)} ORDER BY a.at, a.attempt, a.seq`,
      p.values,
    );
    return rows.map(toDeliveryAttempt);
  }

  async retryDeliveries(filter: WebhookDeliveryFilter, now: number): Promise<number> {
    assertDeliveryFilter(filter);
    const { ids, endpointId, tenant, status, since, failureReason, lastStatusCode } = filter;
    if (ids !== undefined && ids.length === 0) {
      return 0;
    }

    await this.readiness.ready();
    // One UPDATE in a READ COMMITTED transaction: it locks only the rows it matches (no gap locks), and waits for a row
    // another transaction holds, then checks it again as that one left it.
    return retryOnDeadlock(this.executor, async (tx) => {
      const p = new SqlParams();
      const set = `status = 'pending', attempts = 0, next_attempt_at = ${ms(p, now)}, failure_reason = NULL, completed_at = NULL, lease_owner = NULL, lease_until = NULL`;
      const where = [
        ...(ids !== undefined ? [p.in('id', [...ids])] : []),
        ...(endpointId !== undefined ? [`endpoint_id = ${p.text(endpointId)}`] : []),
        ...(tenant !== undefined ? [p.equals('tenant', tenant)] : []),
        ...(status !== undefined ? [`status = ${p.text(status)}`] : []),
        ...(since !== undefined ? [`created_at >= ${ms(p, +since)}`] : []),
        ...(failureReason !== undefined ? [p.equals('failure_reason', failureReason)] : []),
        ...(lastStatusCode !== undefined ? [equalsInt(p, 'last_status_code', lastStatusCode)] : []),
        // Checked again on the locked row: a delivery a worker claims meanwhile keeps its lease.
        `(lease_until IS NULL OR lease_until <= ${ms(p, now)})`,
      ];
      const { affectedRows } = await tx.execute(`UPDATE ${this.t.deliveries} SET ${set} WHERE ${where.join(' AND ')}`, p.values);
      return affectedRows;
    });
  }

  async deliveryStats(now: number): Promise<WebhookDeliveryStoreStats> {
    await this.readiness.ready();
    // Pending deliveries through their index, failed ones through theirs: never the whole log. Only pending deliveries
    // hold leases (a claim takes pending ones, and every write that finishes one clears its lease). SUM() of no rows
    // is NULL, read as 0.
    const p = new SqlParams();
    const [row] = await this.executor.query<SqlRow>(
      `SELECT CAST(COUNT(*) AS CHAR) AS pending,
  CAST(SUM(d.next_attempt_at <= ${ms(p, now)} AND (d.lease_until IS NULL OR d.lease_until <= ${ms(p, now)})) AS CHAR) AS due,
  CAST(SUM(d.lease_until > ${ms(p, now)}) AS CHAR) AS leased,
  (SELECT CAST(COUNT(*) AS CHAR) FROM ${this.t.deliveries} f WHERE f.status = 'failed') AS failed,
  CAST(MIN(CASE WHEN d.next_attempt_at <= ${ms(p, now)} THEN d.next_attempt_at END) AS CHAR) AS oldest_due_at
FROM ${this.t.deliveries} d
WHERE d.status = 'pending'`,
      p.values,
    );
    return {
      pending: toInt(row?.pending) ?? 0,
      due: toInt(row?.due) ?? 0,
      leased: toInt(row?.leased) ?? 0,
      failed: toInt(row?.failed) ?? 0,
      oldestDueAt: toInt(row?.oldest_due_at),
    };
  }

  async pruneDeliveries(before: number): Promise<number> {
    await this.readiness.ready();
    // Finished deliveries, a batch a transaction, each with its attempts: no foreign key cascades them. A finished
    // delivery holds no lease; one a manual retry holds is waited for, and read as the retry left it.
    let pruned = 0;
    for (;;) {
      const deleted = await retryOnDeadlock(this.executor, async (tx) => {
        const p = new SqlParams();
        // The index is pinned, as the claim's: a locking read that scanned the table would wait for the rows workers hold.
        const finished = await tx.query<SqlRow>(
          `SELECT ${columns(['id'], 'd')} FROM ${this.t.deliveries} d FORCE INDEX (deliveries_finished)
WHERE d.completed_at < ${ms(p, before)} AND d.status <> 'pending'
ORDER BY d.completed_at, d.id
LIMIT ${p.limit(PRUNED_PER_TRANSACTION)}
FOR UPDATE`,
          p.values,
        );
        if (finished.length === 0) {
          return 0;
        }

        const ids = finished.map((row) => row.id!);
        const a = new SqlParams();
        await tx.execute(`DELETE FROM ${this.t.attempts} WHERE ${a.in('delivery_id', ids)}`, a.values);
        const d = new SqlParams();
        return (await tx.execute(`DELETE FROM ${this.t.deliveries} WHERE ${d.in('id', ids)}`, d.values)).affectedRows;
      });
      pruned += deleted;
      if (deleted < PRUNED_PER_TRANSACTION) {
        break;
      }
    }

    // Messages left without deliveries. A fan-out of one of them holds its row until its deliveries commit: each batch
    // locks the rows first, then looks for deliveries again, in a statement of its own, which sees what such a fan-out
    // committed (a READ COMMITTED statement reads what was committed before it started).
    for (;;) {
      const p = new SqlParams();
      const orphans = await this.executor.query<SqlRow>(
        `SELECT ${columns(['id'], 'm')} FROM ${this.t.messages} m
WHERE NOT EXISTS (SELECT 1 FROM ${this.t.deliveries} d WHERE d.message_id = m.id)
ORDER BY m.id
LIMIT ${p.limit(PRUNED_PER_TRANSACTION)}`,
        p.values,
      );
      if (orphans.length === 0) {
        break;
      }

      const ids = orphans.map((row) => row.id!);
      await retryOnDeadlock(this.executor, async (tx) => {
        const l = new SqlParams();
        await tx.query(`SELECT ${columns(['id'], 'm')} FROM ${this.t.messages} m WHERE ${l.in('m.id', ids)} ORDER BY m.id FOR UPDATE`, l.values);
        const d = new SqlParams();
        await tx.execute(
          `DELETE FROM ${this.t.messages} WHERE ${d.in('id', ids)} AND NOT EXISTS (SELECT 1 FROM ${this.t.deliveries} d WHERE d.message_id = ${this.t.messages}.id)`,
          d.values,
        );
      });
      if (orphans.length < PRUNED_PER_TRANSACTION) {
        break;
      }
    }
    return pruned;
  }
}

/** Epoch milliseconds as a `bigint` parameter, whole (see `wholeMs()`). */
function ms(p: SqlParams, value: number | null): string {
  return p.bigint(wholeMs(value));
}

/** `column = value` of an integer column, or `column IS NULL` for `null`: `p.equals()` binds text. */
function equalsInt(p: SqlParams, column: string, value: number | null): string {
  return value === null ? `${column} IS NULL` : `${column} = ${p.int(value)}`;
}

/** An INSERT's column list, each quoted. */
function names(list: readonly string[]): string {
  return list.map((column) => quoteIdentifier(column)).join(', ');
}

function pairKey(messageId: string, endpointId: string): string {
  return JSON.stringify([messageId, endpointId]);
}

/**
 * One delivery per message and endpoint, the first in `deliveries`' order: what the unique key keeps when one insert
 * holds two (PostgreSQL's ON CONFLICT DO NOTHING skips the second; MySQL's plain INSERT would fail on it).
 */
function firstPerPair(deliveries: readonly WebhookDelivery[]): WebhookDelivery[] {
  const seen = new Set<string>();
  const first: WebhookDelivery[] = [];
  for (const delivery of deliveries) {
    const key = pairKey(delivery.messageId, delivery.endpointId);
    if (!seen.has(key)) {
      seen.add(key);
      first.push(delivery);
    }
  }
  return first;
}

/**
 * Refuses a key longer than its column before any statement runs: MySQL would fail the statement (strict mode) with a
 * message that names neither the store nor the limit. Characters are code points, as MySQL counts them.
 */
function checkKey(what: string, value: string | null, length: number): void {
  if (value === null || value.length <= length) {
    return;
  }

  const characters = [...value].length;
  if (characters > length) {
    throw new RangeError(`${STORE}: ${what} is at most ${length} characters on MySQL (a key column), and this one has ${characters}.`);
  }
}
