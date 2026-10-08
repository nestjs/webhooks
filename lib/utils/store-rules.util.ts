import type { WebhookDelivery, WebhookDeliveryFilter } from '../interfaces/webhook-delivery.interface.js';
import type { WebhookEndpointFailure, WebhookEndpointSecret } from '../interfaces/webhook-endpoint-store.interface.js';

// What a store decides the same way whatever keeps its rows: the SQL stores apply these to the row they locked.

/** `listEndpoints()` and `listDeliveries()` without a `limit`. */
export const DEFAULT_PAGE_SIZE = 50;

/**
 * Epoch milliseconds whole, as a `Date` holds them. The SQL stores keep times as `bigint`, and the kit's
 * `SqlParams.bigint()` refuses a fraction, which a fractional duration in the module's options would put into every
 * write of the worker.
 */
export function wholeMs(value: number | null): number | null {
  return value === null ? null : Math.trunc(value);
}

/**
 * The fan-out's deliveries in one order, whatever the caller's: by endpoint, then id. Two fan-outs of a message that
 * race on its deliveries then never each wait for a row the other inserted (a deadlock).
 */
export function inEndpointOrder(deliveries: readonly WebhookDelivery[]): WebhookDelivery[] {
  return [...deliveries].sort((a, b) => compare(a.endpointId, b.endpointId) || compare(a.id, b.id));
}

function compare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * `addEndpointSecret()`: `secret` first, then each other secret expiring at the earlier of its own expiry and
 * `expireOthersAt`, less those expired by `now`.
 */
export function rotateSecrets(
  secrets: readonly WebhookEndpointSecret[],
  secret: WebhookEndpointSecret,
  expireOthersAt: number,
  now: number,
): WebhookEndpointSecret[] {
  const others = secrets
    .map((existing) => ({ ...existing, expiresAt: existing.expiresAt === null ? expireOthersAt : Math.min(existing.expiresAt, expireOthersAt) }))
    .filter((existing) => existing.expiresAt > now);

  return [secret, ...others];
}

/**
 * `recordEndpointFailure()` on the endpoint as it stands: it has been failing since the first failure after its last
 * success, and this failure disables it if it's enabled and has been failing since `disableIfFailingSince` or earlier.
 */
export function endpointFailure(
  endpoint: { enabled: boolean; failingSince: number | null },
  { at, disableIfFailingSince }: WebhookEndpointFailure,
): { failingSince: number; disable: boolean } {
  const failingSince = endpoint.failingSince ?? at;
  return { failingSince, disable: endpoint.enabled && disableIfFailingSince !== null && failingSince <= disableIfFailingSince };
}

/** `retryDeliveries()` refuses a filter that names no deliveries, unless it says `all`. */
export function assertDeliveryFilter({ ids, endpointId, tenant, status, since, failureReason, lastStatusCode, all }: WebhookDeliveryFilter): void {
  if (
    !all &&
    ids === undefined &&
    endpointId === undefined &&
    tenant === undefined &&
    status === undefined &&
    since === undefined &&
    failureReason === undefined &&
    lastStatusCode === undefined
  ) {
    throw new Error('Refusing an empty delivery filter; pass { all: true } to retry every delivery');
  }
}
