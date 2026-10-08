import { Injectable } from '@nestjs/common';
import { toMs } from '../utils/duration.util.js';
import type { Duration } from '../interfaces/duration.interface.js';
import { checkPage } from '../utils/query.util.js';
import { WebhookDeliveryNotFoundError } from '../errors/webhook-delivery-not-found.error.js';
import type { WebhookDelivery, WebhookDeliveryDetails, WebhookDeliveryFilter, WebhookDeliveryQuery, WebhookDeliveryStats } from '../interfaces/webhook-delivery.interface.js';
import type { WebhookTenantScope } from '../interfaces/webhook-endpoint.interface.js';
import { WebhooksStorage } from '../storage/webhooks.storage.js';
import { WebhookWorker } from './webhook-worker.service.js';

/**
 * The delivery log and replay: what an operator console or a partner's "recent deliveries"
 * page calls. There is no built-in controller: expose these behind your own guard, passing
 * the caller's tenant.
 */
@Injectable()
export class WebhookDeliveries {
  constructor(
    private readonly storage: WebhooksStorage,
    private readonly worker: WebhookWorker,
  ) {}

  private get store() {
    return this.storage.deliveries;
  }

  /** Newest first; filter by `tenant`, `endpointId`, `messageId`, `status`, `type`, `failureReason`, and `lastStatusCode`. */
  async list(query: WebhookDeliveryQuery = {}): Promise<WebhookDelivery[]> {
    return this.store.listDeliveries(checkPage(query, 'WebhookDeliveries.list()'));
  }

  /** The delivery, its message and every attempt (status code, duration, the start of the response, the error). */
  async get(id: string, scope: WebhookTenantScope = {}): Promise<WebhookDeliveryDetails | undefined> {
    const delivery = await this.find(id, scope);
    if (!delivery) {
      return undefined;
    }

    const [message, history] = await Promise.all([this.store.getMessage(delivery.messageId), this.store.listDeliveryAttempts(id)]);
    if (!message) {
      return undefined;
    }

    return { ...delivery, message, history };
  }

  /**
   * Sends again, now, with a fresh retry budget and the same `webhook-id`, so a receiver
   * that already processed it can skip it. Takes one delivery's id (any status: a replay
   * of a delivered webhook too), or a filter, typically every failed delivery of an
   * endpoint since its outage began: `{ endpointId, status: 'failed', since }`. A delivery
   * being attempted right now is left alone. Resolves to how many will be sent.
   */
  async retry(target: string | WebhookDeliveryFilter, scope: WebhookTenantScope = {}): Promise<number> {
    let filter: WebhookDeliveryFilter;
    if (typeof target === 'string') {
      if (!(await this.find(target, scope))) {
        throw new WebhookDeliveryNotFoundError(target);
      }
      filter = { ids: [target] };
    } else {

      const { ids, endpointId, tenant, status, since, all } = target ?? {};
      if (!all && ids === undefined && endpointId === undefined && tenant === undefined && status === undefined && since === undefined) {
        throw new Error('Refusing an empty delivery filter; pass { all: true } to retry every delivery');
      }
      if (ids !== undefined && (!Array.isArray(ids) || ids.some((id) => typeof id !== 'string'))) {
        throw new TypeError('WebhookDeliveries.retry(): ids must be an array of delivery ids');
      }

      // `new Date(userInput)` of a malformed value is an Invalid Date: `createdAt < NaN` is false for every
      // delivery, so in memory it would match all of them, and a database refuses the value.
      if (since !== undefined && !Number.isFinite(+since)) {
        throw new TypeError('WebhookDeliveries.retry(): since must be a Date or epoch milliseconds (got an invalid date)');
      }
      if (scope.tenant !== undefined && tenant !== undefined && tenant !== scope.tenant) {
        return 0;
      }

      filter = { ...target };
    }

    if (scope.tenant !== undefined) {
      filter.tenant = scope.tenant;
    }

    const count = await this.store.retryDeliveries(filter, Date.now());
    if (count > 0) {
      this.worker.notify();
    }
    return count;
  }

  /** Deletes deliveries that finished more than `olderThan` ago (`'30d'`), their logs and orphaned messages. */
  async prune(olderThan: Duration): Promise<number> {
    return this.store.pruneDeliveries(Date.now() - toMs(olderThan));
  }

  /** Pending, due, leased and failed counts, and `lagMs`: how overdue the most overdue delivery is. */
  stats(): Promise<WebhookDeliveryStats> {
    return this.worker.stats();
  }

  private async find(id: string, { tenant }: WebhookTenantScope): Promise<WebhookDelivery | undefined> {
    if (typeof id !== 'string' || id === '') {
      return undefined;
    }

    const delivery = await this.store.getDelivery(id);
    if (!delivery || (tenant !== undefined && delivery.tenant !== tenant)) {
      return undefined;
    }
    return delivery;
  }
}
