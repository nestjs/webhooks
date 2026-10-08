import type { WebhookMessage } from './webhook-message.interface.js';
import type { WebhookDeliveryStoreStats } from './webhook-delivery-store.interface.js';

export type WebhookDeliveryStatus = 'pending' | 'succeeded' | 'failed';

export type WebhookDeliveryFailureReason =
  /** Every attempt of the retry budget failed. */
  | 'exhausted'
  /** Not retryable: `retryIf` said no, a blocked destination, `NonRetryableWebhookError`, 410 Gone. */
  | 'rejected'
  /** The endpoint was disabled when the delivery's turn came. */
  | 'endpoint-disabled'
  /** The endpoint was deleted. */
  | 'endpoint-deleted';

/** One message to one endpoint, and where it stands. */
export interface WebhookDelivery {
  readonly id: string;
  readonly messageId: string;
  readonly endpointId: string;
  /** Copied from the message, for tenant-scoped queries. */
  readonly tenant: string | null;
  /** Copied from the message, for listing. */
  readonly type: string;
  readonly status: WebhookDeliveryStatus;
  /** Attempts in this round (a manual retry starts a new round at 0). */
  readonly attempts: number;
  /** Epoch ms of the next attempt while `pending`, else null. */
  readonly nextAttemptAt: number | null;
  readonly lastAttemptAt: number | null;
  readonly lastStatusCode: number | null;
  readonly lastError: string | null;
  readonly failureReason: WebhookDeliveryFailureReason | null;
  readonly createdAt: number;
  /** Epoch ms when it succeeded or failed for good, else null. */
  readonly completedAt: number | null;
}

/** One attempt in a delivery's log. */
export interface WebhookDeliveryAttempt {
  readonly deliveryId: string;
  /** 1-based, within its round. */
  readonly attempt: number;
  /** Epoch ms when the attempt started. */
  readonly at: number;
  readonly durationMs: number;
  /** The response status, or null when there was no response (refused, timed out, reset). */
  readonly statusCode: number | null;
  /** The start of the response body (`delivery.maxResponseSize` bytes at most), or null. */
  readonly response: string | null;
  /** Why it failed, or null when it succeeded. */
  readonly error: string | null;
}

/** A delivery with its message and its attempt log: `WebhookDeliveries.get()`. */
export interface WebhookDeliveryDetails extends WebhookDelivery {
  readonly message: WebhookMessage;
  /** Oldest first. */
  readonly history: readonly WebhookDeliveryAttempt[];
}

export interface WebhookDeliveryQuery {
  /** `undefined`: every tenant. `null`: deliveries without a tenant. */
  tenant?: string | null;
  endpointId?: string;
  messageId?: string;
  status?: WebhookDeliveryStatus;
  type?: string;
  /** `undefined`: any reason. `null`: deliveries with no failure reason. */
  failureReason?: WebhookDeliveryFailureReason | null;
  /** `undefined`: any response status. `null`: no HTTP response was received. */
  lastStatusCode?: number | null;
  /** Default 50. */
  limit?: number;
  offset?: number;
}

/** Which deliveries `retry()` targets. An empty filter is refused; use `{ all: true }`. */
export interface WebhookDeliveryFilter {
  ids?: readonly string[];
  endpointId?: string;
  tenant?: string | null;
  status?: WebhookDeliveryStatus;
  /** Epoch ms or Date: deliveries created at or after this. */
  since?: number | Date;
  all?: boolean;
}

/** `WebhookWorker.stats()`: the store's counts, and this worker's view. */
export interface WebhookDeliveryStats extends WebhookDeliveryStoreStats {
  /** How long the longest-waiting due delivery has waited, in ms: the number to alert on. */
  lagMs: number;
  /** Attempts this worker is making right now. */
  inFlight: number;
}
