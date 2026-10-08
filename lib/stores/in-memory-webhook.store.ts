import type { WebhookClaimedDelivery, WebhookClaimRequest, WebhookDeliveryStoreStats, WebhookDeliveryUpdate } from '../interfaces/webhook-delivery-store.interface.js';
import type { WebhookDelivery, WebhookDeliveryAttempt, WebhookDeliveryFilter, WebhookDeliveryQuery } from '../interfaces/webhook-delivery.interface.js';
import type { WebhookEndpointFailure, WebhookEndpointPatch, WebhookEndpointRecord, WebhookEndpointSecret } from '../interfaces/webhook-endpoint-store.interface.js';
import type { WebhookEndpointQuery } from '../interfaces/webhook-endpoint.interface.js';
import type { WebhookMessage } from '../interfaces/webhook-message.interface.js';
import type { WebhookEndpointStore } from '../interfaces/webhook-endpoint-store.interface.js';
import type { WebhookDeliveryStore } from '../interfaces/webhook-delivery-store.interface.js';

interface DeliveryRow {
  delivery: WebhookDelivery;
  leaseOwner: string | null;
  leaseUntil: number | null;
}

type Mutable<T> = { -readonly [K in keyof T]: T[K] };

/**
 * The default store, used when no source is registered, and the test double. Everything
 * lives in this process: lost on restart, not shared between instances. It implements both
 * contracts, `WebhookEndpointStore` and `WebhookDeliveryStore`, synchronously.
 */
export class InMemoryWebhookStore implements WebhookEndpointStore, WebhookDeliveryStore {
  private readonly endpointRows = new Map<string, WebhookEndpointRecord>();
  private readonly messages = new Map<string, WebhookMessage>();
  private readonly deliveryRows = new Map<string, DeliveryRow>();
  /** `messageId\0endpointId` → delivery id: the fan-out's unique key. */
  private readonly pairs = new Map<string, string>();
  private readonly attempts = new Map<string, WebhookDeliveryAttempt[]>();

  // ---------------------------------------------------------------- endpoints

  createEndpoint(endpoint: WebhookEndpointRecord): void {
    if (this.endpointRows.has(endpoint.id)) {
      throw new Error(`Duplicate webhook endpoint id ${endpoint.id}`);
    }
    this.endpointRows.set(endpoint.id, clone(endpoint));
  }

  getEndpoint(id: string): WebhookEndpointRecord | undefined {
    const row = this.endpointRows.get(id);
    return row && clone(row);
  }

  listEndpoints({ tenant, limit = 50, offset = 0 }: WebhookEndpointQuery): WebhookEndpointRecord[] {
    return [...this.endpointRows.values()]
      .filter((endpoint) => tenant === undefined || endpoint.tenant === tenant)
      .sort((a, b) => b.createdAt - a.createdAt || compareDesc(a.id, b.id))
      .slice(offset, offset + limit)
      .map(clone);
  }

  findSubscribedEndpoints(tenant: string | null, type: string): WebhookEndpointRecord[] {
    return [...this.endpointRows.values()]
      .filter(
        (endpoint) =>
          endpoint.enabled && endpoint.tenant === tenant && (endpoint.eventTypes.includes(type) || endpoint.eventTypes.includes('*')),
      )
      .map(clone);
  }

  updateEndpoint(id: string, patch: WebhookEndpointPatch, now: number): WebhookEndpointRecord | undefined {
    const row = this.endpointRows.get(id);
    if (!row) {
      return undefined;
    }

    const defined = Object.fromEntries(Object.entries(patch).filter(([, value]) => value !== undefined));
    const updated = { ...row, ...clone(defined), updatedAt: now };
    this.endpointRows.set(id, updated);
    return clone(updated);
  }

  deleteEndpoint(id: string): boolean {
    return this.endpointRows.delete(id);
  }

  addEndpointSecret(id: string, secret: WebhookEndpointSecret, expireOthersAt: number, now: number): boolean {
    const row = this.endpointRows.get(id);
    if (!row) {
      return false;
    }

    const others = row.secrets
      .map((existing) => ({
        ...existing,
        expiresAt: existing.expiresAt === null ? expireOthersAt : Math.min(existing.expiresAt, expireOthersAt),
      }))
      .filter((existing) => existing.expiresAt > now);

    this.endpointRows.set(id, { ...row, secrets: [clone(secret), ...others] });
    return true;
  }

  recordEndpointFailure(id: string, { at, disableIfFailingSince, reason }: WebhookEndpointFailure): boolean {
    const row = this.endpointRows.get(id);
    if (!row) {
      return false;
    }

    const failingSince = row.failingSince ?? at;
    const disable = row.enabled && disableIfFailingSince !== null && failingSince <= disableIfFailingSince;

    this.endpointRows.set(id, {
      ...row,
      failingSince,
      ...(disable ? { enabled: false, disabledReason: reason, updatedAt: at } : {}),
    });
    return disable;
  }

  recordEndpointSuccess(id: string): void {
    const row = this.endpointRows.get(id);
    if (row && row.failingSince !== null) {
      this.endpointRows.set(id, { ...row, failingSince: null });
    }
  }

  // ---------------------------------------------------------------- deliveries

  createDeliveries(message: WebhookMessage, deliveries: readonly WebhookDelivery[]): number {
    if (!this.messages.has(message.id)) {
      this.messages.set(message.id, clone(message));
    }

    let created = 0;
    for (const delivery of deliveries) {
      const pair = `${delivery.messageId}\u0000${delivery.endpointId}`;
      if (this.pairs.has(pair) || this.deliveryRows.has(delivery.id)) {
        continue;
      }
      this.pairs.set(pair, delivery.id);
      this.deliveryRows.set(delivery.id, { delivery: clone(delivery), leaseOwner: null, leaseUntil: null });
      created++;
    }

    return created;
  }

  claimDeliveries({ owner, now, leaseMs, limit }: WebhookClaimRequest): WebhookClaimedDelivery[] {
    const due = [...this.deliveryRows.values()]
      .filter((row) => this.claimable(row, now))
      .sort(
        (a, b) =>
          a.delivery.nextAttemptAt! - b.delivery.nextAttemptAt! ||
          a.delivery.createdAt - b.delivery.createdAt ||
          (a.delivery.id < b.delivery.id ? -1 : 1),
      )
      .slice(0, limit);

    return due.map((row) => {
      row.leaseOwner = owner;
      row.leaseUntil = now + leaseMs;
      return { delivery: clone(row.delivery), message: clone(this.messages.get(row.delivery.messageId)!) };
    });
  }

  recordDeliveryAttempt(id: string, owner: string, update: WebhookDeliveryUpdate): boolean {
    const row = this.deliveryRows.get(id);
    if (!row || row.leaseOwner !== owner) {
      return false;
    }

    const { attempt } = update;
    const delivery: Mutable<WebhookDelivery> = {
      ...row.delivery,
      status: update.status,
      attempts: update.attempts,
      nextAttemptAt: update.nextAttemptAt,
      failureReason: update.failureReason,
      completedAt: update.completedAt,
    };

    if (attempt) {
      delivery.lastAttemptAt = attempt.at;
      delivery.lastStatusCode = attempt.statusCode;
      delivery.lastError = attempt.error;
      this.attempts.set(id, [...(this.attempts.get(id) ?? []), clone(attempt)]);
    } else if (update.error !== undefined) {
      delivery.lastError = update.error;
    }

    row.delivery = delivery;
    row.leaseOwner = null;
    row.leaseUntil = null;
    return true;
  }

  releaseDeliveries(ids: readonly string[], owner: string, nextAttemptAt?: number): number {
    let released = 0;
    for (const id of new Set(ids)) {
      const row = this.deliveryRows.get(id);
      if (!row || row.leaseOwner !== owner) {
        continue;
      }

      row.leaseOwner = null;
      row.leaseUntil = null;
      if (nextAttemptAt !== undefined && row.delivery.nextAttemptAt !== null && row.delivery.nextAttemptAt < nextAttemptAt) {
        row.delivery = { ...row.delivery, nextAttemptAt };
      }
      released++;
    }

    return released;
  }

  getDelivery(id: string): WebhookDelivery | undefined {
    const row = this.deliveryRows.get(id);
    return row && clone(row.delivery);
  }

  getMessage(id: string): WebhookMessage | undefined {
    const message = this.messages.get(id);
    return message && clone(message);
  }

  listDeliveries({ tenant, endpointId, messageId, status, type, failureReason, lastStatusCode, limit = 50, offset = 0 }: WebhookDeliveryQuery): WebhookDelivery[] {
    return [...this.deliveryRows.values()]
      .map((row) => row.delivery)
      .filter(
        (d) =>
          (tenant === undefined || d.tenant === tenant) &&
          (endpointId === undefined || d.endpointId === endpointId) &&
          (messageId === undefined || d.messageId === messageId) &&
          (status === undefined || d.status === status) &&
          (type === undefined || d.type === type) &&
          (failureReason === undefined || d.failureReason === failureReason) &&
          (lastStatusCode === undefined || d.lastStatusCode === lastStatusCode),
      )
      .sort((a, b) => b.createdAt - a.createdAt || compareDesc(a.id, b.id))
      .slice(offset, offset + limit)
      .map(clone);
  }

  listDeliveryAttempts(deliveryId: string): WebhookDeliveryAttempt[] {
    return (this.attempts.get(deliveryId) ?? []).map(clone).sort((a, b) => a.at - b.at || a.attempt - b.attempt);
  }

  retryDeliveries(filter: WebhookDeliveryFilter, now: number): number {
    const { ids, endpointId, tenant, status, since, all } = filter;
    if (!all && ids === undefined && endpointId === undefined && tenant === undefined && status === undefined && since === undefined) {
      throw new Error('Refusing an empty delivery filter; pass { all: true } to retry every delivery');
    }

    const idSet = ids && new Set(ids);
    let retried = 0;

    for (const row of this.deliveryRows.values()) {
      const d = row.delivery;
      if (idSet && !idSet.has(d.id)) {
        continue;
      }
      if (endpointId !== undefined && d.endpointId !== endpointId) {
        continue;
      }
      if (tenant !== undefined && d.tenant !== tenant) {
        continue;
      }
      if (status !== undefined && d.status !== status) {
        continue;
      }
      if (since !== undefined && d.createdAt < +since) {
        continue;
      }
      if (row.leaseUntil !== null && row.leaseUntil > now) {
        continue;
      }

      row.delivery = { ...d, status: 'pending', attempts: 0, nextAttemptAt: now, failureReason: null, completedAt: null };
      row.leaseOwner = null;
      row.leaseUntil = null;
      retried++;
    }
    return retried;
  }

  deliveryStats(now: number): WebhookDeliveryStoreStats {
    let pending = 0;
    let due = 0;
    let leased = 0;
    let failed = 0;
    let oldestDueAt: number | null = null;

    for (const row of this.deliveryRows.values()) {
      const { status, nextAttemptAt } = row.delivery;
      if (row.leaseUntil !== null && row.leaseUntil > now) {
        leased++;
      }
      if (status === 'failed') {
        failed++;
      }

      if (status !== 'pending') {
        continue;
      }

      pending++;
      if (this.claimable(row, now)) {
        due++;
      }
      if (nextAttemptAt !== null && nextAttemptAt <= now && (oldestDueAt === null || nextAttemptAt < oldestDueAt)) {
        oldestDueAt = nextAttemptAt;
      }
    }

    return { pending, due, leased, failed, oldestDueAt };
  }

  pruneDeliveries(before: number): number {
    let pruned = 0;
    for (const [id, row] of this.deliveryRows) {
      const { status, completedAt, messageId, endpointId } = row.delivery;
      if (status === 'pending' || completedAt === null || completedAt >= before) {
        continue;
      }

      this.deliveryRows.delete(id);
      this.pairs.delete(`${messageId}\u0000${endpointId}`);
      this.attempts.delete(id);
      pruned++;
    }

    const referenced = new Set([...this.deliveryRows.values()].map((row) => row.delivery.messageId));
    for (const id of this.messages.keys()) {
      if (!referenced.has(id)) {
        this.messages.delete(id);
      }
    }

    return pruned;
  }

  private claimable(row: DeliveryRow, now: number): boolean {
    const { status, nextAttemptAt } = row.delivery;
    return status === 'pending' && nextAttemptAt !== null && nextAttemptAt <= now && (row.leaseUntil === null || row.leaseUntil <= now);
  }
}

function compareDesc(a: string, b: string): number {
  return a < b ? 1 : a > b ? -1 : 0;
}

/** A deep copy through JSON, as a database stores it. */
function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
