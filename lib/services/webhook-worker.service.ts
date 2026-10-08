import { Inject, Injectable, Logger, type OnApplicationBootstrap, type OnModuleDestroy } from '@nestjs/common';
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { computeBackoff, parseRetryAfter, resolveRetry } from '../utils/backoff.util.js';
import { signStandard } from '../signing/standard-webhooks.scheme.js';
import { standardSecretKey } from '../signing/secrets.util.js';
import type { ResolvedWebhooksConfig } from '../interfaces/resolved-webhooks-config.interface.js';
import { WEBHOOKS_CONFIG } from '../webhooks.constants.js';
import { NonRetryableWebhookError } from '../errors/non-retryable-webhook.error.js';
import { WebhookDeliveryTimeoutError } from '../errors/webhook-delivery-timeout.error.js';
import { WebhookResponseError } from '../errors/webhook-response.error.js';
import { WebhookDestinationBlockedError } from '../errors/webhook-destination-blocked.error.js';
import { describeError } from '../utils/describe-error.util.js';
import { truncateUtf8 } from '../utils/truncate-utf8.util.js';
import { WebhooksEvents } from '../events/webhooks-events.service.js';
import type { WebhooksEvent } from '../events/webhooks-events.interface.js';
import type { WebhookClaimedDelivery, WebhookDeliveryUpdate } from '../interfaces/webhook-delivery-store.interface.js';
import type { WebhookDelivery, WebhookDeliveryAttempt, WebhookDeliveryFailureReason, WebhookDeliveryStats } from '../interfaces/webhook-delivery.interface.js';
import type { WebhookEndpointRecord } from '../interfaces/webhook-endpoint-store.interface.js';
import { WebhooksStorage } from '../storage/webhooks.storage.js';
import type { WebhookEndpointStore } from '../interfaces/webhook-endpoint-store.interface.js';
import type { WebhookDeliveryStore } from '../interfaces/webhook-delivery-store.interface.js';
import { WebhookTransport } from '../transports/webhook.transport.js';
import type { WebhookTransportResponse } from '../interfaces/webhook-transport.interface.js';
import type { WebhookWorkerRunResult } from '../interfaces/webhook-worker.interface.js';

type Outcome = { kind: 'delivered' | 'retried' | 'failed' | 'lost'; throttleMs?: number; endpointDisabled?: boolean };

/** Statuses that ask the sender to slow down: the endpoint is paused in this worker. */
const THROTTLE_STATUSES = new Set([429, 502, 503, 504]);
const DEFAULT_THROTTLE_MS = 5_000;
/** Expired pauses are swept once the map grows past this. */
const PAUSE_SWEEP_SIZE = 1_000;

/**
 * Claims due deliveries with a lease, signs and sends each, and records the attempt.
 *
 * - One poll loop per process; `notify()` runs it now (the fan-out calls it). The loop runs
 *   in the async context the worker started in.
 * - Each claim has its own fencing token: a worker that stalled past its lease can't
 *   overwrite what the worker that re-claimed the delivery recorded.
 * - An endpoint's deliveries in a batch go one at a time; endpoints go in parallel
 *   (`worker.concurrency`). A 429, 502, 503 or 504 pauses the endpoint in this worker for
 *   `Retry-After` (5s without one, `backoff.maxDelay` at most): the rest of its batch and its
 *   deliveries in later batches are released to that time without an attempt.
 * - Every attempt is signed at send time: a fresh `webhook-timestamp`, the same `webhook-id`.
 * - `stop()` stops claiming, waits for attempts in flight, and releases what it hasn't started.
 */
@Injectable()
export class WebhookWorker implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(WebhookWorker.name);
  private started = false;
  private stopping = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private loop: Promise<void> | undefined;
  private again = false;
  private readonly runs = new Set<Promise<WebhookWorkerRunResult>>();
  /** Endpoints that asked for a breather, by id: no attempt in this worker until then. */
  private readonly pausedUntil = new Map<string, number>();
  private inFlight = 0;
  private inWorkerContext: <R>(fn: () => R) => R = (fn) => fn();

  constructor(
    private readonly storage: WebhooksStorage,
    private readonly transport: WebhookTransport,
    private readonly events: WebhooksEvents,
    @Inject(WEBHOOKS_CONFIG) private readonly config: ResolvedWebhooksConfig,
  ) {}

  onApplicationBootstrap() {
    if (this.config.worker.enabled) {
      this.start();
    }
  }

  async onModuleDestroy() {
    await this.stop();
  }

  get running(): boolean {
    return this.started && !this.stopping;
  }

  start(): void {
    if (this.started) {
      return;
    }

    this.started = true;
    this.stopping = false;
    this.inWorkerContext = AsyncLocalStorage.snapshot();
    this.schedule(0);
  }

  /** Poll now instead of at the next tick. */
  notify(): void {
    if (!this.running) {
      return;
    }

    if (this.loop) {
      this.again = true;
      return;
    }
    this.schedule(0);
  }

  /** Stops claiming, waits for attempts in flight (each for at most `delivery.timeout`), releases the rest. */
  async stop(): Promise<void> {
    this.stopping = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    await this.loop;
    await Promise.allSettled(this.runs);
    this.started = false;
  }

  async stats(): Promise<WebhookDeliveryStats> {
    const now = Date.now();
    const stats = await this.deliveries.deliveryStats(now);
    return {
      ...stats,
      lagMs: stats.oldestDueAt === null ? 0 : Math.max(0, now - stats.oldestDueAt),
      inFlight: this.inFlight,
    };
  }

  /** Claims one batch and delivers it. The poll loop calls this; tests and scripts can too. */
  runOnce(): Promise<WebhookWorkerRunResult> {
    const run = this.claimAndDeliver();
    this.runs.add(run);
    const forget = () => this.runs.delete(run);
    run.then(forget, forget);
    return run;
  }

  private get endpoints(): WebhookEndpointStore {
    return this.storage.endpoints;
  }

  private get deliveries(): WebhookDeliveryStore {
    return this.storage.deliveries;
  }

  private schedule(delay: number) {
    if (!this.running) {
      return;
    }

    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.inWorkerContext(() => this.poll()), delay);
    this.timer.unref?.();
  }

  private poll() {
    this.timer = undefined;
    if (this.loop) {
      this.again = true;
      return;
    }

    this.loop = (async () => {
      do {
        this.again = false;
        const result = await this.runOnce();
        if (result.claimed >= this.config.worker.batchSize) {
          this.again = true;
        }
      } while (this.again && this.running);
    })()
      .catch((error) => this.logger.error(`Worker loop failed: ${describeError(error)}`))
      .finally(() => {
        this.loop = undefined;
        this.schedule(this.config.worker.pollInterval);
      });
  }

  private async claimAndDeliver(): Promise<WebhookWorkerRunResult> {
    const result: WebhookWorkerRunResult = { claimed: 0, delivered: 0, retried: 0, failed: 0, released: 0, leaseLost: 0 };
    if (this.stopping) {
      return result;
    }

    const owner = randomUUID();
    const claimedAt = Date.now();
    let claimed: WebhookClaimedDelivery[];
    try {
      claimed = await this.deliveries.claimDeliveries({
        owner,
        now: claimedAt,
        leaseMs: this.config.worker.lease,
        limit: this.config.worker.batchSize,
      });
    } catch (error) {
      this.logger.error(`Webhook store claimDeliveries failed: ${describeError(error)}`);
      return result;
    }

    if (claimed.length === 0) {
      return result;
    }

    result.claimed = claimed.length;
    const leaseUntil = claimedAt + this.config.worker.lease;
    const groups = [...groupBy(claimed, (item) => item.delivery.endpointId).values()];
    await runPool(groups, this.config.worker.concurrency, async (group) => {
      try {
        await this.deliverGroup(group, owner, leaseUntil, result);
      } catch (error) {
        // Not a failed attempt (those are recorded): a store that is down, a bug. The rest of the group's leases expire.
        this.logger.error(`Worker failed on endpoint ${group[0]!.delivery.endpointId}: ${describeError(error)}`);
      }
    });

    return result;
  }

  private async deliverGroup(group: WebhookClaimedDelivery[], owner: string, leaseUntil: number, result: WebhookWorkerRunResult) {
    const endpointId = group[0]!.delivery.endpointId;
    const pausedUntil = this.pausedUntil.get(endpointId);
    if (pausedUntil !== undefined) {
      // It asked for a breather in an earlier batch: nothing goes out until then.
      if (pausedUntil > Date.now()) {
        return this.release(group, owner, result, pausedUntil);
      }
      this.pausedUntil.delete(endpointId);
    }

    let endpoint = await this.endpoints.getEndpoint(endpointId);
    for (let i = 0; i < group.length; i++) {
      // Never start an attempt that could outlive the lease: another worker could claim the delivery and send it too.
      if (this.stopping || Date.now() + this.config.timeoutMs > leaseUntil) {
        return this.release(group.slice(i), owner, result);
      }

      const outcome = await this.deliver(group[i]!, endpoint, owner);
      if (outcome.kind === 'delivered') {
        result.delivered++;
      } else if (outcome.kind === 'retried') {
        result.retried++;
      } else if (outcome.kind === 'failed') {
        result.failed++;
      } else {
        result.leaseLost++;
      }

      if (outcome.endpointDisabled && endpoint) {
        endpoint = { ...endpoint, enabled: false };
      }

      if (outcome.throttleMs !== undefined) {
        const until = Date.now() + outcome.throttleMs;
        this.pause(endpointId, until);
        return this.release(group.slice(i + 1), owner, result, until);
      }
    }
  }

  private pause(endpointId: string, until: number) {
    this.pausedUntil.set(endpointId, until);

    if (this.pausedUntil.size > PAUSE_SWEEP_SIZE) {
      const now = Date.now();
      for (const [id, at] of this.pausedUntil) {
        if (at <= now) {
          this.pausedUntil.delete(id);
        }
      }
    }
  }

  private async deliver(
    { delivery, message }: WebhookClaimedDelivery,
    endpoint: WebhookEndpointRecord | undefined,
    owner: string,
  ): Promise<Outcome> {
    if (!endpoint || !endpoint.enabled) {
      const reason: WebhookDeliveryFailureReason = endpoint ? 'endpoint-disabled' : 'endpoint-deleted';
      const error = endpoint ? `Endpoint ${endpoint.id} is disabled (${endpoint.disabledReason ?? 'manual'})` : `Endpoint ${delivery.endpointId} was deleted`;
      const now = Date.now();
      return this.record(delivery, owner, {
        update: { status: 'failed', attempts: delivery.attempts, nextAttemptAt: null, failureReason: reason, completedAt: now, error },
        event: { type: 'delivery-failed', delivery, error: new Error(error), statusCode: null, attempt: delivery.attempts, reason },
        kind: 'failed',
      });
    }

    const attempt = delivery.attempts + 1;
    const startedAt = Date.now();
    let response: WebhookTransportResponse | undefined;
    let error: unknown;

    this.inFlight++;
    try {
      response = await this.send(delivery, message.body, message.type, endpoint, attempt, startedAt);
    } catch (thrown) {
      error = thrown;
    } finally {
      this.inFlight--;
    }

    const now = Date.now();
    const durationMs = now - startedAt;
    const statusCode = response?.statusCode ?? null;
    const ok = statusCode !== null && statusCode >= 200 && statusCode < 300;

    const log: WebhookDeliveryAttempt = {
      deliveryId: delivery.id,
      attempt,
      at: startedAt,
      durationMs,
      statusCode,
      response: response ? truncateUtf8(response.body, this.config.maxResponseSize) : null,
      error: ok ? null : describeError(error ?? new WebhookResponseError(statusCode!)),
    };

    if (ok) {
      if (endpoint.failingSince !== null) {
        await this.safely('recordEndpointSuccess', () => this.endpoints.recordEndpointSuccess(endpoint.id));
      }
      return this.record(delivery, owner, {
        update: { status: 'succeeded', attempts: attempt, nextAttemptAt: null, failureReason: null, completedAt: now, attempt: log },
        event: { type: 'delivered', delivery, statusCode: statusCode!, attempt, durationMs },
        kind: 'delivered',
      });
    }

    // Capped at the backoff's ceiling: an endpoint's header never postpones its deliveries for longer
    // than a retry could, and a value too large for a date (20 digits reach Infinity) can't reach the store.
    const retryAfterHeader = parseRetryAfter(response?.headers['retry-after'], now);
    const retryAfterMs = retryAfterHeader === undefined ? undefined : Math.min(retryAfterHeader, this.maxRetryAfter());
    const failure = error ?? new WebhookResponseError(statusCode!, retryAfterMs);
    const gone = statusCode === 410;

    if (failure instanceof WebhookDestinationBlockedError) {
      this.logger.warn(`Refused to deliver ${delivery.id} to endpoint ${endpoint.id}: ${failure.reason}`);
      this.emit({ type: 'destination-blocked', endpointId: endpoint.id, tenant: endpoint.tenant, reason: failure.reason, address: failure.address });
    }

    const retryable =
      !gone &&
      !(failure instanceof WebhookDestinationBlockedError) &&
      !(failure instanceof NonRetryableWebhookError) &&
      this.retryIf(failure, attempt, delivery);

    const endpointDisabled = await this.safely('recordEndpointFailure', () =>
      this.endpoints.recordEndpointFailure(endpoint.id, {
        at: now,
        disableIfFailingSince: gone ? now : this.config.disableAfterMs === null ? null : now - this.config.disableAfterMs,
        reason: gone ? 'gone' : 'failing',
      }),
    );
    if (endpointDisabled === true) {
      const reason = gone ? 'gone' : 'failing';
      this.logger.warn(`Disabled webhook endpoint ${endpoint.id} (${reason === 'gone' ? 'it answered 410 Gone' : 'every attempt failed for too long'})`);
      this.emit({ type: 'endpoint-disabled', endpointId: endpoint.id, tenant: endpoint.tenant, reason });
    }

    const throttleMs =
      statusCode !== null && THROTTLE_STATUSES.has(statusCode) ? (retryAfterMs ?? Math.min(DEFAULT_THROTTLE_MS, this.maxRetryAfter())) : undefined;

    if (!retryable || attempt >= this.config.retry.attempts) {
      const reason: WebhookDeliveryFailureReason = retryable ? 'exhausted' : 'rejected';
      this.logger.warn(`Webhook delivery ${delivery.id} (${delivery.type}) failed after ${attempt} attempt(s) (${reason}): ${log.error}`);
      const outcome = await this.record(delivery, owner, {
        update: { status: 'failed', attempts: attempt, nextAttemptAt: null, failureReason: reason, completedAt: now, attempt: log },
        event: { type: 'delivery-failed', delivery, error: failure, statusCode, attempt, reason },
        kind: 'failed',
      });
      return { ...outcome, throttleMs, endpointDisabled: endpointDisabled === true };
    }

    const delayMs = Math.max(this.backoff(attempt, failure, delivery), retryAfterMs ?? 0);
    this.logger.warn(
      `Retrying webhook delivery ${delivery.id} (${delivery.type}) in ${delayMs}ms (attempt ${attempt} of ${this.config.retry.attempts} failed): ${log.error}`,
    );
    const outcome = await this.record(delivery, owner, {
      update: { status: 'pending', attempts: attempt, nextAttemptAt: now + delayMs, failureReason: null, completedAt: null, attempt: log },
      event: { type: 'retry-scheduled', delivery, error: failure, statusCode, attempt, delayMs },
      kind: 'retried',
    });
    return { ...outcome, throttleMs, endpointDisabled: endpointDisabled === true };
  }

  /** Signs with every live secret and sends, within `delivery.timeout` even if the transport ignores its signal. */
  private async send(
    delivery: WebhookDelivery,
    body: string,
    type: string,
    endpoint: WebhookEndpointRecord,
    attempt: number,
    now: number,
  ): Promise<WebhookTransportResponse> {
    const timestamp = Math.floor(now / 1000);
    const signatures = endpoint.secrets
      .filter((secret) => secret.expiresAt === null || secret.expiresAt > now)
      .map((secret) => `v1,${signStandard(standardSecretKey(this.config.secrets.open(endpoint.id, secret.secret)), delivery.messageId, timestamp, body)}`);
    if (signatures.length === 0) {
      throw new NonRetryableWebhookError(`Endpoint ${endpoint.id} has no live signing secret`);
    }

    const controller = new AbortController();
    const timeoutMs = this.config.timeoutMs;
    let timer: ReturnType<typeof setTimeout> | undefined;

    try {
      return await Promise.race([
        Promise.resolve().then(() =>
          this.transport.send(
            {
              url: endpoint.url,
              headers: {
                'content-type': 'application/json',
                'user-agent': this.config.userAgent,
                'webhook-id': delivery.messageId,
                'webhook-timestamp': String(timestamp),
                'webhook-signature': signatures.join(' '),
              },
              body,
              endpointId: endpoint.id,
              deliveryId: delivery.id,
              messageId: delivery.messageId,
              type,
            },
            { signal: controller.signal, attempt },
          ),
        ),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            const error = new WebhookDeliveryTimeoutError(timeoutMs);
            controller.abort(error);
            reject(error);
          }, timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  private async record(
    delivery: WebhookDelivery,
    owner: string,
    { update, event, kind }: { update: WebhookDeliveryUpdate; event: WebhooksEvent; kind: Outcome['kind'] },
  ): Promise<Outcome> {
    let recorded: boolean;
    try {
      recorded = await this.deliveries.recordDeliveryAttempt(delivery.id, owner, update);
    } catch (error) {
      this.logger.error(`Webhook store recordDeliveryAttempt failed for ${delivery.id}: ${describeError(error)}`);
      return { kind: 'lost' };
    }

    if (!recorded) {
      this.logger.warn(`Lease on webhook delivery ${delivery.id} was taken over; it may be sent more than once (same webhook-id)`);
      return { kind: 'lost' };
    }

    this.emit(event);
    return { kind };
  }

  private async release(items: WebhookClaimedDelivery[], owner: string, result: WebhookWorkerRunResult, nextAttemptAt?: number) {
    if (items.length === 0) {
      return;
    }

    try {
      result.released += await this.deliveries.releaseDeliveries(
        items.map((item) => item.delivery.id),
        owner,
        nextAttemptAt,
      );
    } catch (error) {
      this.logger.error(`Webhook store releaseDeliveries failed: ${describeError(error)}`);
    }
  }

  /** `retry.retryIf`, guarded: a throwing predicate retries. */
  private retryIf(error: unknown, attempt: number, delivery: WebhookDelivery): boolean {
    if (!this.config.retry.retryIf) {
      return true;
    }

    try {
      return this.config.retry.retryIf(error, attempt, delivery);
    } catch (predicateError) {
      this.logger.error(`retry.retryIf threw, retrying: ${describeError(predicateError)}`);
      return true;
    }
  }

  /** `retry.backoff`, guarded: a throwing function falls back to the default schedule. */
  private backoff(attempt: number, error: unknown, delivery: WebhookDelivery): number {
    try {
      return computeBackoff(this.config.retry.backoff, attempt, error, delivery);
    } catch (backoffError) {
      this.logger.error(`retry.backoff failed, using the default: ${describeError(backoffError)}`);
      return computeBackoff(resolveRetry(undefined).backoff, attempt, error, delivery);
    }
  }

  /** A `Retry-After` longer than the backoff's cap (1 day with a custom function) isn't honored beyond it. */
  private maxRetryAfter(): number {
    const { backoff } = this.config.retry;
    return typeof backoff === 'function' ? 86_400_000 : backoff.maxDelay;
  }

  private async safely<T>(operation: string, fn: () => T | Promise<T>): Promise<T | undefined> {
    try {
      return await fn();
    } catch (error) {
      this.logger.error(`Webhook store ${operation} failed: ${describeError(error)}`);
      return undefined;
    }
  }

  private emit(event: WebhooksEvent) {
    try {
      this.events.emit(event);
    } catch (error) {
      this.logger.error(`Webhooks event subscriber threw: ${describeError(error)}`);
    }
  }
}

function groupBy<T>(items: T[], key: (item: T) => string): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  for (const item of items) {
    const k = key(item);
    groups.set(k, [...(groups.get(k) ?? []), item]);
  }
  return groups;
}

async function runPool<T>(items: T[], limit: number, fn: (item: T) => Promise<void>) {
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      await fn(items[next++]!);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
}
