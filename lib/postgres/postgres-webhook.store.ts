import { Logger, type OnModuleInit } from '@nestjs/common';
import {
  columns,
  quoteSchema,
  SqlParams,
  toBool,
  toInt,
  toJson,
  type MigrationSqlOptions,
  type SqlExecutor,
  type SqlTransactionOptions,
  type StoreReadiness,
} from '@nestjs/store-kit/postgres';
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
import type { PostgresWebhookStoreOptions } from './interfaces/postgres-webhook-store-options.interface.js';
import { webhookStoreSchema } from './migrations/index.js';

/** A statement that waited for a lock sees what the lock's holder committed. */
const READ_COMMITTED: SqlTransactionOptions = { isolationLevel: 'read committed' };

/** Deliveries per insert: 14 parameters each, so 14,000 a statement, well within PostgreSQL's 65,535 (PGlite takes fewer). */
const DELIVERIES_PER_INSERT = 1_000;

/** A claimed delivery's message, selected next to it under names of its own. */
const MESSAGE_PREFIX = 'm_';

/**
 * The first-party `WebhookEndpointStore` and `WebhookDeliveryStore` on PostgreSQL, through the client the application
 * already has (`fromPg()`, `fromSequelize()`, `fromDrizzle()`, `fromTypeOrm()`, `fromPrisma()`, `fromKysely()`). It keeps
 * its tables in a schema of its own (`nest_webhooks` by default), which its migrations create and bring up to date.
 * Endpoint secrets are stored as the package hands them over: sealed when `WebhooksModule` has `encryption` keys.
 *
 * ```ts
 * @Module({
 *   imports: [OutboxModule.forRoot(), WebhooksModule.forRoot()],
 *   providers: [
 *     {
 *       provide: PostgresWebhookStore,
 *       inject: [getDrizzleToken(), WebhooksStorage],
 *       useFactory: (db: Database, storage: WebhooksStorage) => new PostgresWebhookStore({ executor: fromDrizzle(db) }, storage),
 *     },
 *   ],
 * })
 * export class AppModule {}
 * ```
 *
 * Given `storage`, it registers itself for both contracts (`storage.registerSource({ endpoints: this, deliveries: this
 * })`). It checks its schema, or migrates it (`migrate`), in `onModuleInit` (so before the worker starts), or at its
 * first call outside Nest.
 */
export class PostgresWebhookStore implements WebhookEndpointStore, WebhookDeliveryStore, OnModuleInit {
  /**
   * The SQL of the store's migrations, for teams that apply migrations with their own tool (drizzle-kit, TypeORM,
   * Prisma Migrate, Flyway...) and run the store with `migrate: false`: from version `from` (default `0`, a new
   * database) to `to` (default: the version this version of the package needs), with the bookkeeping that tells the
   * store which versions a schema has. A schema's version is `SELECT max(version) FROM <schema>.migrations`.
   * Downgrades aren't supported. From version 0 it starts with `CREATE SCHEMA IF NOT EXISTS`, which needs the CREATE
   * privilege on the database even when the schema exists: drop that statement if someone created the schema for you.
   * With `statementBreakpoints`, drizzle-kit's `--> statement-breakpoint` separates the statements, for a custom
   * drizzle-kit migration: its migrator then runs them one at a time, which PGlite needs.
   *
   * ```ts
   * // drizzle/0004_webhooks.sql, created empty by `npx drizzle-kit generate --custom --name=webhooks`
   * writeFileSync('drizzle/0004_webhooks.sql', PostgresWebhookStore.migrationSql({ statementBreakpoints: true }));
   * ```
   */
  static migrationSql(options: MigrationSqlOptions = {}): string {
    return webhookStoreSchema.sql(options);
  }

  /** The schema version this version of the package needs: its last migration. */
  static readonly schemaVersion = webhookStoreSchema.latest;

  private readonly logger = new Logger('WebhooksModule');
  private readonly executor: SqlExecutor;
  private readonly t: Record<'endpoints' | 'messages' | 'deliveries' | 'attempts', string>;
  /** The schema migrated (`migrate`) or checked before the first statement: see `onModuleInit()`. */
  private readonly readiness: StoreReadiness;

  constructor(options: PostgresWebhookStoreOptions, storage?: WebhooksStorage) {
    const resolved = webhookStoreSchema.resolveOptions(options);
    this.executor = resolved.executor;
    const s = quoteSchema(resolved.schema, 'PostgresWebhookStore');
    this.t = {
      endpoints: `${s}.endpoints`,
      messages: `${s}.messages`,
      deliveries: `${s}.deliveries`,
      attempts: `${s}.delivery_attempts`,
    };
    this.readiness = webhookStoreSchema.readiness({ ...resolved, logger: this.logger });
    storage?.registerSource({ endpoints: this, deliveries: this });
  }

  /** Migrates the schema (`migrate`) or checks it, before the worker starts: startup fails if it can't serve. */
  async onModuleInit(): Promise<void> {
    await this.readiness.ready();
  }

  /**
   * Applies the migrations the schema hasn't had yet, whatever `migrate` says, in one transaction under an advisory
   * lock: of processes that migrate together, one applies them. Resolves to the versions it applied (`[]`: none were
   * pending).
   */
  migrate(): Promise<number[]> {
    return this.readiness.migrate();
  }

  // ---------------------------------------------------------------- endpoints

  async createEndpoint(endpoint: WebhookEndpointRecord): Promise<void> {
    await this.readiness.ready();
    const p = new SqlParams();
    await this.executor.query(
      `INSERT INTO ${this.t.endpoints} (${ENDPOINT_COLUMNS.join(', ')})
VALUES (${p.text(endpoint.id)}, ${p.text(endpoint.tenant)}, ${p.text(endpoint.url)}, ${p.json(endpoint.eventTypes)}, ${p.text(endpoint.description)},
  ${p.bool(endpoint.enabled)}, ${p.text(endpoint.disabledReason)}, ${ms(p, endpoint.failingSince)}, ${p.json(endpoint.secrets)},
  ${ms(p, endpoint.createdAt)}, ${ms(p, endpoint.updatedAt)})`,
      p.values,
    );
  }

  async getEndpoint(id: string): Promise<WebhookEndpointRecord | undefined> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<SqlRow>(`SELECT ${columns(ENDPOINT_COLUMNS)} FROM ${this.t.endpoints} WHERE id = ${p.text(id)}`, p.values);
    return row && toEndpointRecord(row);
  }

  async listEndpoints({ tenant, limit = DEFAULT_PAGE_SIZE, offset = 0 }: WebhookEndpointQuery): Promise<WebhookEndpointRecord[]> {
    await this.readiness.ready();
    const p = new SqlParams();
    const where = tenant === undefined ? '' : ` WHERE ${p.equals('e.tenant', tenant)}`;
    const rows = await this.executor.query<SqlRow>(
      `SELECT ${columns(ENDPOINT_COLUMNS, 'e')} FROM ${this.t.endpoints} e${where}
ORDER BY e.created_at DESC, e.id DESC LIMIT ${p.int(limit)} OFFSET ${p.int(offset)}`,
      p.values,
    );
    return rows.map(toEndpointRecord);
  }

  async findSubscribedEndpoints(tenant: string | null, type: string): Promise<WebhookEndpointRecord[]> {
    await this.readiness.ready();
    // Exactly this tenant: a message without one reaches only endpoints without one.
    const p = new SqlParams();
    const rows = await this.executor.query<SqlRow>(
      `SELECT ${columns(ENDPOINT_COLUMNS, 'e')} FROM ${this.t.endpoints} e
WHERE e.enabled AND ${p.equals('e.tenant', tenant)} AND (e.event_types @> jsonb_build_array(${p.text(type)}) OR e.event_types @> '["*"]')
ORDER BY e.created_at, e.id`,
      p.values,
    );
    return rows.map(toEndpointRecord);
  }

  async updateEndpoint(id: string, patch: WebhookEndpointPatch, now: number): Promise<WebhookEndpointRecord | undefined> {
    await this.readiness.ready();
    // Fields the patch leaves out (undefined) keep their value.
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
    const [row] = await this.executor.query<SqlRow>(
      `UPDATE ${this.t.endpoints} e SET ${set.join(', ')} WHERE e.id = ${p.text(id)} RETURNING ${columns(ENDPOINT_COLUMNS, 'e')}`,
      p.values,
    );
    return row && toEndpointRecord(row);
  }

  async deleteEndpoint(id: string): Promise<boolean> {
    await this.readiness.ready();
    const p = new SqlParams();
    const deleted = await this.executor.query<SqlRow>(`DELETE FROM ${this.t.endpoints} WHERE id = ${p.text(id)} RETURNING id`, p.values);
    return deleted.length === 1;
  }

  async addEndpointSecret(id: string, secret: WebhookEndpointSecret, expireOthersAt: number, now: number): Promise<boolean> {
    await this.readiness.ready();
    return this.executor.transaction(async (tx) => {
      // The row lock serializes rotations: the second one reads the first one's secret.
      const p = new SqlParams();
      const [row] = await tx.query<SqlRow>(`SELECT secrets::text AS secrets FROM ${this.t.endpoints} WHERE id = ${p.text(id)} FOR UPDATE`, p.values);
      if (!row) {
        return false;
      }

      const secrets = rotateSecrets(toJson(row.secrets) as WebhookEndpointSecret[], secret, expireOthersAt, now);
      const u = new SqlParams();
      await tx.query(`UPDATE ${this.t.endpoints} SET secrets = ${u.json(secrets)} WHERE id = ${u.text(id)}`, u.values);
      return true;
    }, READ_COMMITTED);
  }

  async recordEndpointFailure(id: string, failure: WebhookEndpointFailure): Promise<boolean> {
    await this.readiness.ready();
    return this.executor.transaction(async (tx) => {
      // The row lock serializes failures: of several past the threshold at once, the first disables the endpoint, and
      // the others read it disabled.
      const p = new SqlParams();
      const [row] = await tx.query<SqlRow>(
        `SELECT enabled::text AS enabled, failing_since::text AS failing_since FROM ${this.t.endpoints} WHERE id = ${p.text(id)} FOR UPDATE`,
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
      const disable = outcome.disable ? `, enabled = false, disabled_reason = ${u.text(failure.reason)}, updated_at = ${ms(u, failure.at)}` : '';
      await tx.query(`UPDATE ${this.t.endpoints} SET failing_since = ${ms(u, outcome.failingSince)}${disable} WHERE id = ${u.text(id)}`, u.values);
      return outcome.disable;
    }, READ_COMMITTED);
  }

  async recordEndpointSuccess(id: string): Promise<void> {
    await this.readiness.ready();
    const p = new SqlParams();
    await this.executor.query(`UPDATE ${this.t.endpoints} SET failing_since = NULL WHERE id = ${p.text(id)} AND failing_since IS NOT NULL`, p.values);
  }

  // ---------------------------------------------------------------- deliveries

  async createDeliveries(message: WebhookMessage, deliveries: readonly WebhookDelivery[]): Promise<number> {
    await this.readiness.ready();
    // In one order, whatever the caller's: two fan-outs of a message racing on its deliveries never each wait for a row
    // the other inserted (a deadlock).
    const sorted = inEndpointOrder(deliveries);

    return this.executor.transaction(async (tx) => {
      const m = new SqlParams();
      await tx.query(
        `INSERT INTO ${this.t.messages} (${MESSAGE_COLUMNS.join(', ')})
VALUES (${m.text(message.id)}, ${m.text(message.type)}, ${m.text(message.tenant)}, ${m.text(message.body)}, ${ms(m, message.createdAt)})
ON CONFLICT (id) DO NOTHING`,
        m.values,
      );

      // The unique (message_id, endpoint_id) key: a fan-out that runs again inserts only what is missing.
      let created = 0;
      for (let start = 0; start < sorted.length; start += DELIVERIES_PER_INSERT) {
        const p = new SqlParams();
        const rows = sorted.slice(start, start + DELIVERIES_PER_INSERT).map(
          (d) =>
            `(${p.text(d.id)}, ${p.text(d.messageId)}, ${p.text(d.endpointId)}, ${p.text(d.tenant)}, ${p.text(d.type)}, ${p.text(d.status)}, ` +
            `${p.int(d.attempts)}, ${ms(p, d.nextAttemptAt)}, ${ms(p, d.lastAttemptAt)}, ${p.int(d.lastStatusCode)}, ${p.text(storable(d.lastError))}, ` +
            `${p.text(d.failureReason)}, ${ms(p, d.createdAt)}, ${ms(p, d.completedAt)})`,
        );
        const [row] = await tx.query<SqlRow>(
          `WITH created AS (
  INSERT INTO ${this.t.deliveries} (${DELIVERY_COLUMNS.join(', ')})
  VALUES ${rows.join(',\n    ')}
  ON CONFLICT DO NOTHING
  RETURNING 1
)
SELECT count(*)::text AS n FROM created`,
          p.values,
        );
        created += toInt(row?.n) ?? 0;
      }
      return created;
    }, READ_COMMITTED);
  }

  async claimDeliveries({ owner, now, leaseMs, limit }: WebhookClaimRequest): Promise<WebhookClaimedDelivery[]> {
    await this.readiness.ready();
    // SKIP LOCKED: workers polling at once take disjoint batches, and never wait for each other.
    const p = new SqlParams();
    const at = ms(p, now);
    const message = MESSAGE_COLUMNS.map((column) => `m.${column}::text AS ${MESSAGE_PREFIX}${column}`).join(', ');
    const rows = await this.executor.query<SqlRow>(
      `WITH claimed AS (
  UPDATE ${this.t.deliveries}
  SET lease_owner = ${p.text(owner)}, lease_until = ${ms(p, now + leaseMs)}
  WHERE id IN (
    SELECT d.id FROM ${this.t.deliveries} d
    WHERE d.status = 'pending' AND d.next_attempt_at <= ${at} AND (d.lease_until IS NULL OR d.lease_until <= ${at})
    ORDER BY d.next_attempt_at, d.created_at, d.id
    LIMIT ${p.int(limit)}
    FOR UPDATE SKIP LOCKED
  )
  RETURNING *
)
SELECT ${columns(DELIVERY_COLUMNS, 'claimed')}, ${message}
FROM claimed JOIN ${this.t.messages} m ON m.id = claimed.message_id
ORDER BY claimed.next_attempt_at, claimed.created_at, claimed.id`,
      p.values,
    );
    return rows.map((row) => ({ delivery: toDelivery(row), message: toMessage(row, MESSAGE_PREFIX) }));
  }

  async recordDeliveryAttempt(id: string, owner: string, update: WebhookDeliveryUpdate): Promise<boolean> {
    await this.readiness.ready();
    const { attempt } = update;
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
        ? [`last_attempt_at = ${ms(p, attempt.at)}`, `last_status_code = ${p.int(attempt.statusCode)}`, `last_error = ${p.text(storable(attempt.error))}`]
        : update.error !== undefined
          ? [`last_error = ${p.text(storable(update.error))}`]
          : []),
    ];
    // Fenced by the lease: a worker that lost it to another writes nothing, its attempt included.
    const recorded = `UPDATE ${this.t.deliveries} SET ${set.join(', ')} WHERE id = ${p.text(id)} AND lease_owner = ${p.text(owner)} RETURNING id`;

    // One statement, so one transaction: the delivery's new state and its log row commit together.
    const statement = attempt
      ? `WITH recorded AS (
  ${recorded}
), logged AS (
  INSERT INTO ${this.t.attempts} (${DELIVERY_ATTEMPT_COLUMNS.join(', ')})
  SELECT recorded.id, ${p.int(attempt.attempt)}, ${ms(p, attempt.at)}, ${p.int(Math.trunc(attempt.durationMs))}, ${p.int(attempt.statusCode)},
    ${p.text(storable(attempt.response))}, ${p.text(storable(attempt.error))}
  FROM recorded
)
SELECT recorded.id FROM recorded`
      : recorded;
    const rows = await this.executor.query<SqlRow>(statement, p.values);
    return rows.length === 1;
  }

  async releaseDeliveries(ids: readonly string[], owner: string, nextAttemptAt?: number): Promise<number> {
    if (ids.length === 0) {
      return 0;
    }

    await this.readiness.ready();
    const p = new SqlParams();
    const postpone = nextAttemptAt === undefined ? '' : `, next_attempt_at = greatest(next_attempt_at, ${ms(p, nextAttemptAt)})`;
    const released = await this.executor.query<SqlRow>(
      `UPDATE ${this.t.deliveries} SET lease_owner = NULL, lease_until = NULL${postpone}
WHERE ${p.in('id', [...new Set(ids)])} AND lease_owner = ${p.text(owner)}
RETURNING id`,
      p.values,
    );
    return released.length;
  }

  async getDelivery(id: string): Promise<WebhookDelivery | undefined> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<SqlRow>(`SELECT ${columns(DELIVERY_COLUMNS)} FROM ${this.t.deliveries} WHERE id = ${p.text(id)}`, p.values);
    return row && toDelivery(row);
  }

  async getMessage(id: string): Promise<WebhookMessage | undefined> {
    await this.readiness.ready();
    const p = new SqlParams();
    const [row] = await this.executor.query<SqlRow>(`SELECT ${columns(MESSAGE_COLUMNS)} FROM ${this.t.messages} WHERE id = ${p.text(id)}`, p.values);
    return row && toMessage(row);
  }

  async listDeliveries({ tenant, endpointId, messageId, status, type, limit = DEFAULT_PAGE_SIZE, offset = 0 }: WebhookDeliveryQuery): Promise<WebhookDelivery[]> {
    await this.readiness.ready();
    const p = new SqlParams();
    const where = [
      ...(tenant !== undefined ? [p.equals('d.tenant', tenant)] : []),
      ...(endpointId !== undefined ? [`d.endpoint_id = ${p.text(endpointId)}`] : []),
      ...(messageId !== undefined ? [`d.message_id = ${p.text(messageId)}`] : []),
      ...(status !== undefined ? [`d.status = ${p.text(status)}`] : []),
      ...(type !== undefined ? [`d.type = ${p.text(type)}`] : []),
    ];
    const rows = await this.executor.query<SqlRow>(
      `SELECT ${columns(DELIVERY_COLUMNS, 'd')} FROM ${this.t.deliveries} d${where.length > 0 ? ` WHERE ${where.join(' AND ')}` : ''}
ORDER BY d.created_at DESC, d.id DESC LIMIT ${p.int(limit)} OFFSET ${p.int(offset)}`,
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
    const { ids, endpointId, tenant, status, since } = filter;
    if (ids !== undefined && ids.length === 0) {
      return 0;
    }

    await this.readiness.ready();
    const p = new SqlParams();
    const at = ms(p, now);
    const where = [
      ...(ids !== undefined ? [p.in('id', [...ids])] : []),
      ...(endpointId !== undefined ? [`endpoint_id = ${p.text(endpointId)}`] : []),
      ...(tenant !== undefined ? [p.equals('tenant', tenant)] : []),
      ...(status !== undefined ? [`status = ${p.text(status)}`] : []),
      ...(since !== undefined ? [`created_at >= ${ms(p, +since)}`] : []),
      // Checked again on the locked row: a delivery a worker claims meanwhile keeps its lease.
      `(lease_until IS NULL OR lease_until <= ${at})`,
    ];
    const [row] = await this.executor.query<SqlRow>(
      `WITH retried AS (
  UPDATE ${this.t.deliveries}
  SET status = 'pending', attempts = 0, next_attempt_at = ${at}, failure_reason = NULL, completed_at = NULL, lease_owner = NULL, lease_until = NULL
  WHERE ${where.join(' AND ')}
  RETURNING 1
)
SELECT count(*)::text AS n FROM retried`,
      p.values,
    );
    return toInt(row?.n) ?? 0;
  }

  async deliveryStats(now: number): Promise<WebhookDeliveryStoreStats> {
    await this.readiness.ready();
    // Pending deliveries through their index, failed ones through theirs: never the whole log. Only pending deliveries
    // hold leases (a claim takes pending ones, and every write that finishes one clears its lease).
    const p = new SqlParams();
    const at = ms(p, now);
    const [row] = await this.executor.query<SqlRow>(
      `SELECT count(*)::text AS pending,
  count(*) FILTER (WHERE d.next_attempt_at <= ${at} AND (d.lease_until IS NULL OR d.lease_until <= ${at}))::text AS due,
  count(*) FILTER (WHERE d.lease_until > ${at})::text AS leased,
  (SELECT count(*) FROM ${this.t.deliveries} f WHERE f.status = 'failed')::text AS failed,
  min(d.next_attempt_at) FILTER (WHERE d.next_attempt_at <= ${at})::text AS oldest_due_at
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
    return this.executor.transaction(async (tx) => {
      // Their attempts go with them (ON DELETE CASCADE). A finished delivery holds no lease.
      const p = new SqlParams();
      const [row] = await tx.query<SqlRow>(
        `WITH pruned AS (
  DELETE FROM ${this.t.deliveries} WHERE status <> 'pending' AND completed_at < ${ms(p, before)}
  RETURNING 1
)
SELECT count(*)::text AS n FROM pruned`,
        p.values,
      );
      await tx.query(`DELETE FROM ${this.t.messages} m WHERE NOT EXISTS (SELECT 1 FROM ${this.t.deliveries} d WHERE d.message_id = m.id)`);
      return toInt(row?.n) ?? 0;
    }, READ_COMMITTED);
  }
}

/** Epoch milliseconds as a `bigint` parameter, whole (see `wholeMs()`). */
function ms(p: SqlParams, value: number | null): string {
  return p.bigint(wholeMs(value));
}

/**
 * Text PostgreSQL can store: it refuses NUL, which a partner's binary response body carries. The worker, failing to
 * record the attempt, would send the webhook again every time the lease expired.
 */
function storable(text: string | null): string | null {
  return text === null ? null : text.replaceAll('\u0000', '\uFFFD');
}
