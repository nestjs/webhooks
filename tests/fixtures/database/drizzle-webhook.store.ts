import { Injectable } from '@nestjs/common';
import { InjectDrizzle } from '@nestjs/drizzle';
import {
  WebhooksStorage,
  type WebhookClaimedDelivery,
  type WebhookClaimRequest,
  type WebhookDelivery,
  type WebhookDeliveryAttempt,
  type WebhookDeliveryFilter,
  type WebhookDeliveryQuery,
  type WebhookDeliveryStore,
  type WebhookDeliveryStoreStats,
  type WebhookDeliveryUpdate,
  type WebhookEndpointFailure,
  type WebhookEndpointPatch,
  type WebhookEndpointQuery,
  type WebhookEndpointRecord,
  type WebhookEndpointSecret,
  type WebhookEndpointStore,
  type WebhookMessage,
} from '../../../lib/index.js';
import { and, arrayOverlaps, asc, desc, eq, gte, inArray, isNull, lt, lte, ne, notExists, or, sql, type SQL } from 'drizzle-orm';
import type { Database } from './drizzle.js';
import { webhookDeliveries, webhookDeliveryAttempts, webhookEndpoints, webhookMessages } from './schema.js';

@Injectable()
export class DrizzleWebhookStore implements WebhookEndpointStore, WebhookDeliveryStore {
  constructor(
    @InjectDrizzle() private readonly db: Database,
    storage: WebhooksStorage,
  ) {
    storage.registerSource({ endpoints: this, deliveries: this });
  }

  // ---------------------------------------------------------------- endpoints

  async createEndpoint(endpoint: WebhookEndpointRecord): Promise<void> {
    await this.db.insert(webhookEndpoints).values(toEndpointRow(endpoint));
  }

  async getEndpoint(id: string): Promise<WebhookEndpointRecord | undefined> {
    const [row] = await this.db.select().from(webhookEndpoints).where(eq(webhookEndpoints.id, id));
    return row && toEndpoint(row);
  }

  async listEndpoints({ tenant, limit = 50, offset = 0 }: WebhookEndpointQuery): Promise<WebhookEndpointRecord[]> {
    const rows = await this.db
      .select()
      .from(webhookEndpoints)
      .where(tenantIs(webhookEndpoints.tenant, tenant))
      .orderBy(desc(webhookEndpoints.createdAt), desc(webhookEndpoints.id))
      .limit(limit)
      .offset(offset);
    return rows.map(toEndpoint);
  }

  async findSubscribedEndpoints(tenant: string | null, type: string): Promise<WebhookEndpointRecord[]> {
    const rows = await this.db
      .select()
      .from(webhookEndpoints)
      .where(
        and(
          eq(webhookEndpoints.enabled, true),
          // Exactly this tenant: a message without one reaches only endpoints without one.
          tenantIs(webhookEndpoints.tenant, tenant),
          arrayOverlaps(webhookEndpoints.eventTypes, [type, '*']),
        ),
      );
    return rows.map(toEndpoint);
  }

  async updateEndpoint(id: string, patch: WebhookEndpointPatch, now: number): Promise<WebhookEndpointRecord | undefined> {
    const [row] = await this.db
      .update(webhookEndpoints)
      .set({
        url: patch.url,
        eventTypes: patch.eventTypes && [...patch.eventTypes],
        description: patch.description,
        enabled: patch.enabled,
        disabledReason: patch.disabledReason,
        failingSince: patch.failingSince === undefined ? undefined : toDate(patch.failingSince),
        updatedAt: new Date(now),
      })
      .where(eq(webhookEndpoints.id, id))
      .returning();
    return row && toEndpoint(row);
  }

  async deleteEndpoint(id: string): Promise<boolean> {
    const deleted = await this.db.delete(webhookEndpoints).where(eq(webhookEndpoints.id, id)).returning({ id: webhookEndpoints.id });
    return deleted.length === 1;
  }

  addEndpointSecret(id: string, secret: WebhookEndpointSecret, expireOthersAt: number, now: number): Promise<boolean> {
    return this.db.transaction(async (tx) => {
      // The row lock serializes rotations: the second one sees the first one's secret.
      const [row] = await tx
        .select({ secrets: webhookEndpoints.secrets })
        .from(webhookEndpoints)
        .where(eq(webhookEndpoints.id, id))
        .for('update');
      if (!row) return false;
      const others = row.secrets
        .map((existing) => ({ ...existing, expiresAt: Math.min(existing.expiresAt ?? Infinity, expireOthersAt) }))
        .filter((existing) => existing.expiresAt > now);
      await tx
        .update(webhookEndpoints)
        .set({ secrets: [secret, ...others] })
        .where(eq(webhookEndpoints.id, id));
      return true;
    });
  }

  recordEndpointFailure(id: string, { at, disableIfFailingSince, reason }: WebhookEndpointFailure): Promise<boolean> {
    return this.db.transaction(async (tx) => {
      const [row] = await tx
        .select({ enabled: webhookEndpoints.enabled, failingSince: webhookEndpoints.failingSince })
        .from(webhookEndpoints)
        .where(eq(webhookEndpoints.id, id))
        .for('update');
      if (!row) return false;
      const failingSince = row.failingSince?.getTime() ?? at;
      const disable = row.enabled && disableIfFailingSince !== null && failingSince <= disableIfFailingSince;
      await tx
        .update(webhookEndpoints)
        .set({
          failingSince: new Date(failingSince),
          ...(disable ? { enabled: false, disabledReason: reason, updatedAt: new Date(at) } : {}),
        })
        .where(eq(webhookEndpoints.id, id));
      return disable;
    });
  }

  async recordEndpointSuccess(id: string): Promise<void> {
    await this.db
      .update(webhookEndpoints)
      .set({ failingSince: null })
      .where(and(eq(webhookEndpoints.id, id), sql`${webhookEndpoints.failingSince} IS NOT NULL`));
  }

  // ---------------------------------------------------------------- deliveries

  createDeliveries(message: WebhookMessage, deliveries: readonly WebhookDelivery[]): Promise<number> {
    return this.db.transaction(async (tx) => {
      await tx
        .insert(webhookMessages)
        .values({ ...message, createdAt: new Date(message.createdAt) })
        .onConflictDoNothing();
      if (deliveries.length === 0) return 0;
      // The unique (message_id, endpoint_id) key: a fan-out that runs again inserts only what is missing.
      const inserted = await tx
        .insert(webhookDeliveries)
        .values(deliveries.map(toDeliveryRow))
        .onConflictDoNothing()
        .returning({ id: webhookDeliveries.id });
      return inserted.length;
    });
  }

  claimDeliveries({ owner, now, leaseMs, limit }: WebhookClaimRequest): Promise<WebhookClaimedDelivery[]> {
    const at = new Date(now);
    return this.db.transaction(async (tx) => {
      // SKIP LOCKED: concurrent workers take disjoint batches, and never wait on each other.
      const due = await tx
        .select({ id: webhookDeliveries.id })
        .from(webhookDeliveries)
        .where(
          and(
            eq(webhookDeliveries.status, 'pending'),
            lte(webhookDeliveries.nextAttemptAt, at),
            or(isNull(webhookDeliveries.leaseUntil), lte(webhookDeliveries.leaseUntil, at)),
          ),
        )
        .orderBy(asc(webhookDeliveries.nextAttemptAt), asc(webhookDeliveries.createdAt), asc(webhookDeliveries.id))
        .limit(limit)
        .for('update', { skipLocked: true });
      if (due.length === 0) return [];
      const ids = due.map((row) => row.id);
      await tx
        .update(webhookDeliveries)
        .set({ leaseOwner: owner, leaseUntil: new Date(now + leaseMs) })
        .where(inArray(webhookDeliveries.id, ids));
      const rows = await tx
        .select()
        .from(webhookDeliveries)
        .innerJoin(webhookMessages, eq(webhookMessages.id, webhookDeliveries.messageId))
        .where(inArray(webhookDeliveries.id, ids))
        .orderBy(asc(webhookDeliveries.nextAttemptAt), asc(webhookDeliveries.createdAt), asc(webhookDeliveries.id));
      return rows.map((row) => ({ delivery: toDelivery(row.webhook_deliveries), message: toMessage(row.webhook_messages) }));
    });
  }

  recordDeliveryAttempt(id: string, owner: string, update: WebhookDeliveryUpdate): Promise<boolean> {
    const { attempt } = update;
    return this.db.transaction(async (tx) => {
      // Fenced by the lease: a worker that lost it to another writes nothing.
      const updated = await tx
        .update(webhookDeliveries)
        .set({
          status: update.status,
          attempts: update.attempts,
          nextAttemptAt: toDate(update.nextAttemptAt),
          failureReason: update.failureReason,
          completedAt: toDate(update.completedAt),
          leaseOwner: null,
          leaseUntil: null,
          ...(attempt
            ? { lastAttemptAt: new Date(attempt.at), lastStatusCode: attempt.statusCode, lastError: attempt.error }
            : update.error !== undefined
              ? { lastError: update.error }
              : {}),
        })
        .where(and(eq(webhookDeliveries.id, id), eq(webhookDeliveries.leaseOwner, owner)))
        .returning({ id: webhookDeliveries.id });
      if (updated.length === 0) return false;
      if (attempt) await tx.insert(webhookDeliveryAttempts).values({ ...attempt, at: new Date(attempt.at) });
      return true;
    });
  }

  async releaseDeliveries(ids: readonly string[], owner: string, nextAttemptAt?: number): Promise<number> {
    if (ids.length === 0) return 0;
    const released = await this.db
      .update(webhookDeliveries)
      .set({
        leaseOwner: null,
        leaseUntil: null,
        ...(nextAttemptAt === undefined
          ? {}
          : { nextAttemptAt: sql`greatest(${webhookDeliveries.nextAttemptAt}, ${new Date(nextAttemptAt).toISOString()}::timestamptz)` }),
      })
      .where(and(eq(webhookDeliveries.leaseOwner, owner), inArray(webhookDeliveries.id, [...ids])))
      .returning({ id: webhookDeliveries.id });
    return released.length;
  }

  async getDelivery(id: string): Promise<WebhookDelivery | undefined> {
    const [row] = await this.db.select().from(webhookDeliveries).where(eq(webhookDeliveries.id, id));
    return row && toDelivery(row);
  }

  async getMessage(id: string): Promise<WebhookMessage | undefined> {
    const [row] = await this.db.select().from(webhookMessages).where(eq(webhookMessages.id, id));
    return row && toMessage(row);
  }

  async listDeliveries({ tenant, endpointId, messageId, status, type, failureReason, lastStatusCode, limit = 50, offset = 0 }: WebhookDeliveryQuery): Promise<WebhookDelivery[]> {
    const rows = await this.db
      .select()
      .from(webhookDeliveries)
      .where(
        and(
          tenantIs(webhookDeliveries.tenant, tenant),
          endpointId === undefined ? undefined : eq(webhookDeliveries.endpointId, endpointId),
          messageId === undefined ? undefined : eq(webhookDeliveries.messageId, messageId),
          status === undefined ? undefined : eq(webhookDeliveries.status, status),
          type === undefined ? undefined : eq(webhookDeliveries.type, type),
          failureReason === undefined ? undefined : failureReason === null ? isNull(webhookDeliveries.failureReason) : eq(webhookDeliveries.failureReason, failureReason),
          lastStatusCode === undefined ? undefined : lastStatusCode === null ? isNull(webhookDeliveries.lastStatusCode) : eq(webhookDeliveries.lastStatusCode, lastStatusCode),
        ),
      )
      .orderBy(desc(webhookDeliveries.createdAt), desc(webhookDeliveries.id))
      .limit(limit)
      .offset(offset);
    return rows.map(toDelivery);
  }

  async listDeliveryAttempts(deliveryId: string): Promise<WebhookDeliveryAttempt[]> {
    const rows = await this.db
      .select()
      .from(webhookDeliveryAttempts)
      .where(eq(webhookDeliveryAttempts.deliveryId, deliveryId))
      .orderBy(asc(webhookDeliveryAttempts.at), asc(webhookDeliveryAttempts.attempt));
    return rows.map(({ seq: _seq, at, ...attempt }) => ({ ...attempt, at: at.getTime() }));
  }

  async retryDeliveries(filter: WebhookDeliveryFilter, now: number): Promise<number> {
    const at = new Date(now);
    const retried = await this.db
      .update(webhookDeliveries)
      .set({ status: 'pending', attempts: 0, nextAttemptAt: at, failureReason: null, completedAt: null, leaseOwner: null, leaseUntil: null })
      // One statement: a delivery a worker claims meanwhile keeps its lease (the condition is checked again on the locked row).
      .where(and(deliveryFilter(filter), or(isNull(webhookDeliveries.leaseUntil), lte(webhookDeliveries.leaseUntil, at))))
      .returning({ id: webhookDeliveries.id });
    return retried.length;
  }

  async deliveryStats(now: number): Promise<WebhookDeliveryStoreStats> {
    const at = new Date(now);
    const d = webhookDeliveries;
    const [row] = await this.db
      .select({
        pending: sql<number>`count(*) FILTER (WHERE ${d.status} = 'pending')`.mapWith(Number),
        due: sql<number>`count(*) FILTER (WHERE ${d.status} = 'pending' AND ${d.nextAttemptAt} <= ${at} AND (${d.leaseUntil} IS NULL OR ${d.leaseUntil} <= ${at}))`.mapWith(Number),
        leased: sql<number>`count(*) FILTER (WHERE ${d.leaseUntil} > ${at})`.mapWith(Number),
        failed: sql<number>`count(*) FILTER (WHERE ${d.status} = 'failed')`.mapWith(Number),
        oldestDueAt: sql<Date | null>`min(${d.nextAttemptAt}) FILTER (WHERE ${d.status} = 'pending' AND ${d.nextAttemptAt} <= ${at})`.mapWith(d.nextAttemptAt),
      })
      .from(d);
    return { ...row!, oldestDueAt: row!.oldestDueAt?.getTime() ?? null };
  }

  pruneDeliveries(before: number): Promise<number> {
    return this.db.transaction(async (tx) => {
      // Attempts go with their delivery (ON DELETE CASCADE).
      const pruned = await tx
        .delete(webhookDeliveries)
        .where(and(ne(webhookDeliveries.status, 'pending'), lt(webhookDeliveries.completedAt, new Date(before))))
        .returning({ id: webhookDeliveries.id });
      await tx
        .delete(webhookMessages)
        .where(notExists(tx.select({ one: sql`1` }).from(webhookDeliveries).where(eq(webhookDeliveries.messageId, webhookMessages.id))));
      return pruned.length;
    });
  }
}

/** `undefined`: any tenant. `null`: rows without one. */
function tenantIs(column: typeof webhookEndpoints.tenant | typeof webhookDeliveries.tenant, tenant: string | null | undefined): SQL | undefined {
  if (tenant === undefined) return undefined;
  return tenant === null ? isNull(column) : eq(column, tenant);
}

/** The filter's fields combined with AND; an empty filter is refused unless it says `all`. */
function deliveryFilter({ ids, endpointId, tenant, status, since, all }: WebhookDeliveryFilter): SQL | undefined {
  const conditions: (SQL | undefined)[] = [];
  if (ids) conditions.push(ids.length === 0 ? sql`false` : inArray(webhookDeliveries.id, [...ids]));
  if (endpointId !== undefined) conditions.push(eq(webhookDeliveries.endpointId, endpointId));
  if (tenant !== undefined) conditions.push(tenantIs(webhookDeliveries.tenant, tenant));
  if (status !== undefined) conditions.push(eq(webhookDeliveries.status, status));
  if (since !== undefined) conditions.push(gte(webhookDeliveries.createdAt, new Date(+since)));
  if (conditions.length === 0 && !all) throw new Error('Refusing an empty delivery filter; pass { all: true } to retry every delivery');
  return and(...conditions);
}

const toDate = (ms: number | null) => (ms === null ? null : new Date(ms));
const toMs = (date: Date | null) => (date === null ? null : date.getTime());

function toEndpointRow(endpoint: WebhookEndpointRecord): typeof webhookEndpoints.$inferInsert {
  return {
    ...endpoint,
    eventTypes: [...endpoint.eventTypes],
    secrets: [...endpoint.secrets],
    failingSince: toDate(endpoint.failingSince),
    createdAt: new Date(endpoint.createdAt),
    updatedAt: new Date(endpoint.updatedAt),
  };
}

function toEndpoint(row: typeof webhookEndpoints.$inferSelect): WebhookEndpointRecord {
  return {
    ...row,
    failingSince: toMs(row.failingSince),
    createdAt: row.createdAt.getTime(),
    updatedAt: row.updatedAt.getTime(),
  };
}

function toMessage(row: typeof webhookMessages.$inferSelect): WebhookMessage {
  return { ...row, createdAt: row.createdAt.getTime() };
}

function toDeliveryRow(delivery: WebhookDelivery): typeof webhookDeliveries.$inferInsert {
  return {
    ...delivery,
    nextAttemptAt: toDate(delivery.nextAttemptAt),
    lastAttemptAt: toDate(delivery.lastAttemptAt),
    createdAt: new Date(delivery.createdAt),
    completedAt: toDate(delivery.completedAt),
  };
}

function toDelivery({ leaseOwner: _owner, leaseUntil: _until, ...row }: typeof webhookDeliveries.$inferSelect): WebhookDelivery {
  return {
    ...row,
    nextAttemptAt: toMs(row.nextAttemptAt),
    lastAttemptAt: toMs(row.lastAttemptAt),
    createdAt: row.createdAt.getTime(),
    completedAt: toMs(row.completedAt),
  };
}

