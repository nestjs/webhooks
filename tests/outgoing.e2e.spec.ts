/**
 * Sending: dispatch inside a transaction, fan-out per tenant and type, signing, retries,
 * throttling, endpoint health, manual retries, secret rotation, encryption at rest, the
 * endpoint API's checks, and the log. The outbox and the stores are the in-memory ones; the
 * transport records requests (a real HTTP round trip is in delivery-http.e2e.spec.ts).
 */
import { subscribe, unsubscribe } from 'node:diagnostics_channel';
import { StandardWebhooksScheme } from '../lib/signing/standard-webhooks.scheme.js';
import { standardSecretKey } from '../lib/signing/secrets.util.js';
import {
  HttpWebhookTransport,
  InMemoryWebhookStore,
  InvalidWebhookEndpointError,
  WebhookDeliveryNotFoundError,
  WebhookEndpointNotFoundError,
  WebhooksStorage,
  WebhookTransport,
  type WebhookDeliveryDetails,
  type WebhookRequest,
  type WebhooksEvent,
  type WebhookRetryOptions,
} from '../lib/index.js';
import { controllableClock, sendingApp } from './helpers.js';

const noJitter: WebhookRetryOptions = { attempts: 3, backoff: { delay: '1s', factor: 2, jitter: 'none' } };

afterEach(() => vi.restoreAllMocks());

describe('dispatching after commit', () => {
  it("sends one Standard Webhooks request per subscribed endpoint of the message's tenant", async () => {
    const t = await sendingApp({ eventTypes: ['order.shipped', 'order.cancelled'] });
    const shop1 = await t.endpoints.create({ url: 'https://shop1.example/hooks', eventTypes: ['order.shipped'], tenant: 'shop-1' });
    const shop1All = await t.endpoints.create({ url: 'https://shop1.example/all', eventTypes: ['*'], tenant: 'shop-1' });
    await t.endpoints.create({ url: 'https://shop1.example/cancel', eventTypes: ['order.cancelled'], tenant: 'shop-1' });
    await t.endpoints.create({ url: 'https://shop2.example/hooks', eventTypes: ['*'], tenant: 'shop-2' });
    await t.endpoints.create({ url: 'https://internal.example/hooks', eventTypes: ['*'] });

    const message = await t.transaction((tx) =>
      t.webhooks.dispatch(tx, { type: 'order.shipped', tenant: 'shop-1', data: { orderId: 'o-1', trackingNumber: 'TRK-1' } }),
    );
    expect(message.id).toMatch(/^msg_[0-9a-f]{32}$/);
    expect(t.transport.sent).toEqual([]); // nothing before the outbox relays the commit

    const run = await t.flush();
    expect(run).toMatchObject({ claimed: 2, delivered: 2, retried: 0, failed: 0 });
    expect(t.transport.sent.map((s) => s.url).sort()).toEqual(['https://shop1.example/all', 'https://shop1.example/hooks']);

    const sent = t.transport.single({ endpointId: shop1.id });

    expect(JSON.parse(sent.body)).toEqual({
      type: 'order.shipped',
      timestamp: new Date(message.createdAt).toISOString(),
      data: { orderId: 'o-1', trackingNumber: 'TRK-1' },
    });
    expect(sent.body).toBe(message.body);
    expect(sent.headers).toMatchObject({
      'content-type': 'application/json',
      'user-agent': 'NestJS-Webhooks/1.0',
      'webhook-id': message.id,
      'webhook-timestamp': expect.stringMatching(/^\d+$/),
    });

    // The receiver's side: Standard Webhooks verification with the endpoint's secret, and no other.
    expect(sent.isSignedWith(shop1.secret)).toBe(true);
    expect(sent.isSignedWith(shop1All.secret)).toBe(false);
    const verified = new StandardWebhooksScheme().verify({ headers: sent.headers, rawBody: Buffer.from(sent.body) }, [
      standardSecretKey(shop1.secret),
    ]);
    expect(verified).toMatchObject({ valid: true, id: message.id });

    const [delivery] = await t.deliveries.list({ endpointId: shop1.id });
    const details = (await t.deliveries.get(delivery!.id)) as WebhookDeliveryDetails;
    expect(details).toMatchObject({ status: 'succeeded', attempts: 1, lastStatusCode: 200, message: { id: message.id } });
    expect(details.history).toEqual([
      { deliveryId: delivery!.id, attempt: 1, at: expect.any(Number), durationMs: expect.any(Number), statusCode: 200, response: '', error: null },
    ]);

    expect(t.events.filter((e) => e.type === 'delivered')).toHaveLength(2);
    await t.close();
  });

  it('sends nothing for a rolled-back transaction', async () => {
    const t = await sendingApp();
    await t.endpoints.create({ url: 'https://shop1.example/hooks', eventTypes: ['*'], tenant: 'shop-1' });
    await expect(
      t.transaction(async (tx) => {
        await t.webhooks.dispatch(tx, { type: 'order.shipped', tenant: 'shop-1', data: {} });
        throw new Error('insert failed');
      }),
    ).rejects.toThrow('insert failed');
    expect(await t.flush()).toMatchObject({ claimed: 0 });
    expect(t.transport.sent).toEqual([]);
    await t.close();
  });

  it('keeps tenants apart: a message without a tenant never reaches a tenant, nor the reverse', async () => {
    const t = await sendingApp();
    const tenantless = await t.endpoints.create({ url: 'https://ops.example/hooks', eventTypes: ['*'] });
    const shop = await t.endpoints.create({ url: 'https://shop.example/hooks', eventTypes: ['*'], tenant: 'shop-1' });
    await t.transaction((tx) =>
      t.webhooks.dispatch(tx, [
        { type: 'order.shipped', data: { n: 1 } },
        { type: 'order.shipped', tenant: 'shop-1', data: { n: 2 } },
        { type: 'order.shipped', tenant: 'shop-9', data: { n: 3 } },
      ]),
    );
    await t.flush();
    expect(t.transport.filter({ endpointId: tenantless.id }).map((s) => s.data)).toEqual([{ n: 1 }]);
    expect(t.transport.filter({ endpointId: shop.id }).map((s) => s.data)).toEqual([{ n: 2 }]);
    expect(t.transport.sent).toHaveLength(2);
    await t.close();
  });

  it('skips disabled endpoints and endpoints created after the message', async () => {
    const clock = controllableClock();
    const t = await sendingApp();
    const disabled = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    await t.endpoints.update(disabled.id, { enabled: false });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'order.shipped', data: {} }));
    clock.advance(10);
    await t.endpoints.create({ url: 'https://late.example/', eventTypes: ['*'] });
    await t.flush();
    expect(t.transport.sent).toEqual([]);
    await t.close();
  });

  it('refuses a message it could not send, inside the caller\'s transaction', async () => {
    const t = await sendingApp({ eventTypes: ['order.shipped'] });
    const dispatch = (message: object) => t.transaction((tx) => t.webhooks.dispatch(tx, message as never));
    await expect(dispatch({ type: 'order.lost', data: {} })).rejects.toThrow(/not in the eventTypes option/);
    await expect(dispatch({ type: 'order..shipped', data: {} })).rejects.toThrow(/not a message type/);
    await expect(dispatch({ type: 'order.shipped' })).rejects.toThrow(/JSON-serializable/);
    await expect(dispatch({ type: 'order.shipped', data: { n: 1n } })).rejects.toThrow(/BigInt/);
    await expect(dispatch({ type: 'order.shipped', data: {}, id: 'msg.1' })).rejects.toThrow(/no "\."/);
    await expect(dispatch({ type: 'order.shipped', data: {}, tenant: '' })).rejects.toThrow(/tenant/);
    await t.close();
  });

  it('creates each delivery once when the outbox delivers the message twice', async () => {
    const t = await sendingApp();
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    const message = await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'order.shipped', data: {} }));
    await t.relay.runOnce();
    // What a redelivery of the outbox message does (a relay that lost its lease after publishing).
    const { WebhookFanOut } = await import('../lib/services/webhook-fan-out.service.js');
    await t.app.get(WebhookFanOut).fanOut(message);
    expect(await t.deliveries.list({ messageId: message.id })).toHaveLength(1);
    await t.close();
  });
});

describe('retries', () => {
  it('retries with backoff, with the same webhook-id and a fresh timestamp, and logs every attempt', async () => {
    const clock = controllableClock();
    const t = await sendingApp({ retry: noJitter });
    const endpoint = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    t.transport.respondWith((_request, attempt) => (attempt === 1 ? { statusCode: 500, body: 'database down' } : 200));
    const message = await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'order.shipped', data: {} }));
    expect(await t.flush()).toMatchObject({ retried: 1 });
    const [pending] = await t.deliveries.list({ endpointId: endpoint.id });
    expect(pending).toMatchObject({ status: 'pending', attempts: 1, lastStatusCode: 500, lastError: 'WebhookResponseError: Endpoint responded 500' });
    expect(pending!.nextAttemptAt! - pending!.lastAttemptAt!).toBeGreaterThanOrEqual(1_000);
    expect(t.events.find((e) => e.type === 'retry-scheduled')).toMatchObject({ attempt: 1, delayMs: expect.any(Number), statusCode: 500 });

    expect(await t.worker.runOnce()).toMatchObject({ claimed: 0 }); // not due yet
    clock.advance(1_100);
    expect(await t.worker.runOnce()).toMatchObject({ delivered: 1 });

    const [first, second] = t.transport.sent;
    expect(first!.headers['webhook-id']).toBe(message.id);
    expect(second!.headers['webhook-id']).toBe(message.id);
    expect(Number(second!.headers['webhook-timestamp'])).toBeGreaterThan(Number(first!.headers['webhook-timestamp']));
    expect(second!.body).toBe(first!.body);

    const details = await t.deliveries.get(pending!.id);
    expect(details!.history.map((a) => [a.attempt, a.statusCode, a.response])).toEqual([
      [1, 500, 'database down'],
      [2, 200, ''],
    ]);
    await t.close();
  });

  it('fails a delivery for good when its attempts run out, or when retryIf says no', async () => {
    const clock = controllableClock();
    const t = await sendingApp({ retry: { ...noJitter, retryIf: (error) => (error as { statusCode?: number }).statusCode !== 422 } });
    const endpoint = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    t.transport.respondWith(503);
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'order.shipped', data: {} }));
    await t.flush();
    for (let i = 0; i < 2; i++) {
      clock.advance(10_000);
      await t.worker.runOnce();
    }
    const [exhausted] = await t.deliveries.list({ endpointId: endpoint.id });
    expect(exhausted).toMatchObject({ status: 'failed', failureReason: 'exhausted', attempts: 3, nextAttemptAt: null });
    expect(t.events.find((e) => e.type === 'delivery-failed')).toMatchObject({ reason: 'exhausted', attempt: 3, statusCode: 503 });

    t.transport.respondWith(422);
    const other = await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'order.cancelled', data: {} }));
    clock.advance(10_000);
    await t.flush();
    const [rejected] = await t.deliveries.list({ messageId: other.id });
    expect(rejected).toMatchObject({ status: 'failed', failureReason: 'rejected', attempts: 1 });
    await t.close();
  });

  it('honors Retry-After, and holds back the rest of the endpoint\'s batch on 429', async () => {
    const t = await sendingApp({ retry: noJitter });
    const busy = await t.endpoints.create({ url: 'https://busy.example/', eventTypes: ['*'] });
    const calm = await t.endpoints.create({ url: 'https://calm.example/', eventTypes: ['*'] });
    t.transport.respondWith((request) => (request.url.includes('busy') ? { statusCode: 429, headers: { 'retry-after': '120' } } : 200));
    await t.transaction((tx) =>
      t.webhooks.dispatch(tx, [
        { type: 'order.shipped', data: { n: 1 } },
        { type: 'order.shipped', data: { n: 2 } },
        { type: 'order.shipped', data: { n: 3 } },
      ]),
    );
    const run = await t.flush();
    expect(run).toMatchObject({ claimed: 6, delivered: 3, retried: 1, released: 2 });
    expect(t.transport.filter({ endpointId: busy.id })).toHaveLength(1);
    expect(t.transport.filter({ endpointId: calm.id })).toHaveLength(3);

    const held = await t.deliveries.list({ endpointId: busy.id });
    const now = Date.now();
    for (const d of held) {
      expect(d.nextAttemptAt! - now).toBeGreaterThan(110_000);
    }
    expect(held.filter((d) => d.attempts === 0)).toHaveLength(2); // released without an attempt
    await t.close();
  });

  it('never honors a Retry-After beyond the backoff cap, for the retry or for the pause', async () => {
    const t = await sendingApp({ retry: { ...noJitter, backoff: { delay: '1s', maxDelay: '1h', jitter: 'none' } } });
    const busy = await t.endpoints.create({ url: 'https://busy.example/', eventTypes: ['*'] });
    // 20 digits: Number() * 1000 overflows to Infinity, which no date column holds.
    t.transport.respondWith({ statusCode: 429, headers: { 'retry-after': '99999999999999999999' } });
    await t.transaction((tx) => t.webhooks.dispatch(tx, [{ type: 'a.b', data: 1 }, { type: 'a.b', data: 2 }]));
    const now = Date.now();
    expect(await t.flush()).toMatchObject({ claimed: 2, retried: 1, released: 1 });
    const held = await t.deliveries.list({ endpointId: busy.id });
    expect(held).toHaveLength(2);
    for (const d of held) {
      expect(d.status).toBe('pending');
      expect(d.nextAttemptAt).not.toBeNull();
      expect(d.nextAttemptAt! - now).toBeGreaterThan(3_500_000);
      expect(d.nextAttemptAt! - now).toBeLessThanOrEqual(3_600_000 + 1_000);
    }
    expect(await t.deliveries.stats()).toMatchObject({ pending: 2, due: 0 });
    await t.close();
  });

  it('pauses an endpoint that asked for a breather across batches, until Retry-After has passed', async () => {
    const clock = controllableClock();
    const t = await sendingApp({ retry: noJitter, worker: { batchSize: 2 } });
    const busy = await t.endpoints.create({ url: 'https://busy.example/', eventTypes: ['*'] });
    const calm = await t.endpoints.create({ url: 'https://calm.example/', eventTypes: ['*'] });
    t.transport.respondWith((request) => (request.url.includes('busy') ? { statusCode: 503, headers: { 'retry-after': '60' } } : 200));
    await t.transaction((tx) => t.webhooks.dispatch(tx, Array.from({ length: 4 }, (_, n) => ({ type: 'a.b', data: n }))));
    await t.relay.runOnce();
    // Four batches of two: each has one delivery for the busy endpoint and one for the calm one.
    for (let i = 0; i < 4; i++) {
      await t.worker.runOnce();
    }
    expect(t.transport.filter({ endpointId: busy.id })).toHaveLength(1); // one probe, not one per batch
    expect(t.transport.filter({ endpointId: calm.id })).toHaveLength(4);
    const held = await t.deliveries.list({ endpointId: busy.id });
    expect(held.filter((d) => d.attempts === 0)).toHaveLength(3); // released without an attempt
    for (const d of held) {
      expect(d.nextAttemptAt! - Date.now()).toBeGreaterThan(55_000);
    }
    expect(await t.worker.runOnce()).toMatchObject({ claimed: 0 });
    // The pause ends when Retry-After has passed: the next attempt goes out (and the endpoint asks again).
    clock.advance(61_000);
    await t.worker.runOnce();
    expect(t.transport.filter({ endpointId: busy.id })).toHaveLength(2);
    await t.close();
  });

  it.each(['1.5', '-3', 'Thu, 24 Sep 2026'])('ignores a malformed Retry-After (%j) and pauses the endpoint for the default 5s', async (retryAfter) => {
    const clock = controllableClock();
    const t = await sendingApp({ retry: noJitter, worker: { batchSize: 2 } });
    const busy = await t.endpoints.create({ url: 'https://busy.example/', eventTypes: ['*'] });
    t.transport.respondWith({ statusCode: 503, headers: { 'retry-after': retryAfter } });
    await t.transaction((tx) => t.webhooks.dispatch(tx, [{ type: 'a.b', data: 1 }, { type: 'a.b', data: 2 }]));
    await t.relay.runOnce();

    expect(await t.worker.runOnce()).toMatchObject({ claimed: 2, retried: 1, released: 1 });
    const [released] = (await t.deliveries.list({ endpointId: busy.id })).filter((d) => d.attempts === 0);
    expect(released!.nextAttemptAt! - Date.now()).toBeGreaterThan(4_000);

    // Read as a date long past, it would have been "retry now": the released delivery would go out at once.
    expect(await t.worker.runOnce()).toMatchObject({ claimed: 0 });
    expect(t.transport.filter({ endpointId: busy.id })).toHaveLength(1);

    clock.advance(5_000);
    await t.worker.runOnce();
    expect(t.transport.filter({ endpointId: busy.id }).length).toBeGreaterThan(1);
    await t.close();
  });

  it('truncates a Unicode response at the UTF-8 byte limit', async () => {
    const t = await sendingApp({ delivery: { maxResponseSize: 5 } });
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    t.transport.respondWith({ statusCode: 200, body: 'a🙂b' });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
    await t.flush();
    const [delivery] = await t.deliveries.list({});
    const response = (await t.deliveries.get(delivery!.id))!.history[0]!.response!;
    expect(response).toBe('a🙂');
    expect(Buffer.byteLength(response, 'utf8')).toBe(5);
    await t.close();
  });

  it('keeps a response ending exactly on the UTF-8 byte boundary', async () => {
    const t = await sendingApp({ delivery: { maxResponseSize: 3 } });
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    t.transport.respondWith({ statusCode: 200, body: 'aéz' });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
    await t.flush();
    const [delivery] = await t.deliveries.list({});
    const response = (await t.deliveries.get(delivery!.id))!.history[0]!.response!;
    expect(response).toBe('aé');
    expect(Buffer.byteLength(response, 'utf8')).toBe(3);
    await t.close();
  });

  it('logs an empty response when delivery.maxResponseSize is zero', async () => {
    const t = await sendingApp({ delivery: { maxResponseSize: 0 } });
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    t.transport.respondWith({ statusCode: 200, body: '🙂' });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
    await t.flush();
    const [delivery] = await t.deliveries.list({});
    expect((await t.deliveries.get(delivery!.id))!.history[0]!.response).toBe('');
    await t.close();
  });

  it('keeps at most delivery.maxResponseSize of the response in the log, whatever the transport returns', async () => {
    const t = await sendingApp({ delivery: { maxResponseSize: 8_000 } });
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    t.transport.respondWith({ statusCode: 200, body: 'x'.repeat(10_000) });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
    await t.flush();
    const [delivery] = await t.deliveries.list({});
    expect((await t.deliveries.get(delivery!.id))!.history[0]!.response).toHaveLength(8_000);
    await t.close();
  });

  it('gives up on an attempt at the timeout, even with a transport that ignores its signal', async () => {
    class StuckTransport extends WebhookTransport {
      send(): Promise<never> {
        return new Promise(() => {});
      }
    }
    const t = await sendingApp({ transport: new StuckTransport(), delivery: { timeout: '100ms' }, retry: noJitter });
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'order.shipped', data: {} }));

    const started = performance.now();
    expect(await t.flush()).toMatchObject({ retried: 1 });
    expect(performance.now() - started).toBeLessThan(1_000);

    const [delivery] = await t.deliveries.list({});
    expect(delivery!.lastError).toBe('WebhookDeliveryTimeoutError: No response within 100ms');
    await t.close();
  });
});

describe('endpoint health', () => {
  it('disables an endpoint that answers 410 Gone, at once', async () => {
    const t = await sendingApp();
    const gone = await t.endpoints.create({ url: 'https://gone.example/', eventTypes: ['*'], tenant: 'shop-1' });
    t.transport.respondWith(410);
    await t.transaction((tx) => t.webhooks.dispatch(tx, [{ type: 'a.b', tenant: 'shop-1', data: 1 }, { type: 'a.b', tenant: 'shop-1', data: 2 }]));
    await t.flush();
    expect(await t.endpoints.get(gone.id)).toMatchObject({ enabled: false, disabledReason: 'gone' });
    const deliveries = await t.deliveries.list({ endpointId: gone.id });
    expect(deliveries.map((d) => d.failureReason).sort()).toEqual(['endpoint-disabled', 'rejected']);
    expect(t.transport.sent).toHaveLength(1);
    expect(t.events.filter((e) => e.type === 'endpoint-disabled')).toEqual([
      { type: 'endpoint-disabled', endpointId: gone.id, tenant: 'shop-1', reason: 'gone' },
    ]);

    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', tenant: 'shop-1', data: 3 }));
    await t.flush();
    expect(t.transport.sent).toHaveLength(1);
    await t.close();
  });

  it('disables an endpoint after disableEndpointAfter without a success, and a success resets the clock', async () => {
    const clock = controllableClock();
    const t = await sendingApp({ disableEndpointAfter: '1h', retry: false });
    const endpoint = await t.endpoints.create({ url: 'https://flaky.example/', eventTypes: ['*'] });
    const attempt = async (status: number) => {
      t.transport.respondWith(status);
      await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
      await t.flush();
    };
    await attempt(500);
    const failingSince = (await t.endpoints.get(endpoint.id))!.failingSince;
    expect(failingSince).not.toBeNull();
    clock.advance(30 * 60_000);
    await attempt(200);
    expect((await t.endpoints.get(endpoint.id))!.failingSince).toBeNull();
    await attempt(500);
    clock.advance(59 * 60_000);
    await attempt(500);
    expect((await t.endpoints.get(endpoint.id))!.enabled).toBe(true);
    clock.advance(2 * 60_000);
    await attempt(500);
    expect(await t.endpoints.get(endpoint.id)).toMatchObject({ enabled: false, disabledReason: 'failing' });
    // Re-enabling forgets the failures.
    expect(await t.endpoints.update(endpoint.id, { enabled: true })).toMatchObject({ enabled: true, disabledReason: null, failingSince: null });
    await t.close();
  });

  it('fails pending deliveries of an endpoint that was deleted or disabled, without sending', async () => {
    const t = await sendingApp();
    const deleted = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    const disabled = await t.endpoints.create({ url: 'https://b.example/', eventTypes: ['*'] });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
    await t.relay.runOnce();
    await t.endpoints.delete(deleted.id);
    await t.endpoints.update(disabled.id, { enabled: false });
    await t.worker.runOnce();
    expect(t.transport.sent).toEqual([]);
    expect((await t.deliveries.list({ endpointId: deleted.id }))[0]).toMatchObject({
      status: 'failed',
      failureReason: 'endpoint-deleted',
      lastError: `Endpoint ${deleted.id} was deleted`,
    });
    expect((await t.deliveries.list({ endpointId: disabled.id }))[0]).toMatchObject({ failureReason: 'endpoint-disabled' });
    await t.close();
  });

  it('refuses a destination the transport blocks, without retrying, and reports it', async () => {
    const transport = new HttpWebhookTransport({ lookup: async () => [{ address: '169.254.169.254', family: 4 }] });
    const t = await sendingApp({ transport });
    const endpoint = await t.endpoints.create({ url: 'https://metadata.attacker.example/latest', eventTypes: ['*'], tenant: 'evil' });
    const seen: unknown[] = [];
    const listener = (event: unknown) => seen.push(event);
    subscribe('nestjs:webhooks:destination-blocked', listener);
    try {
      await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', tenant: 'evil', data: {} }));
      await t.flush();
    } finally {
      unsubscribe('nestjs:webhooks:destination-blocked', listener);
    }

    const [delivery] = await t.deliveries.list({ endpointId: endpoint.id });
    expect(delivery).toMatchObject({ status: 'failed', failureReason: 'rejected', attempts: 1, lastStatusCode: null });
    expect(delivery!.lastError).toBe(
      'WebhookDestinationBlockedError: Refused to deliver to https://metadata.attacker.example/…: metadata.attacker.example resolves to 169.254.169.254, link-local (cloud metadata)',
    );

    expect(seen).toEqual([
      {
        type: 'destination-blocked',
        endpointId: endpoint.id,
        tenant: 'evil',
        reason: 'metadata.attacker.example resolves to 169.254.169.254, link-local (cloud metadata)',
        address: '169.254.169.254',
      },
    ]);
    expect((await t.endpoints.get(endpoint.id))!.failingSince).not.toBeNull();
    await t.close();
  });
});

describe('the delivery log and replay', () => {
  it('retries one delivery or every failed one of an endpoint, with the same webhook-id, within the tenant', async () => {
    const t = await sendingApp({ retry: false });
    const endpoint = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'], tenant: 'shop-1' });
    t.transport.respondWith(500);
    const [m1, m2] = await t.transaction((tx) =>
      t.webhooks.dispatch(tx, [{ type: 'a.b', tenant: 'shop-1', data: 1 }, { type: 'a.b', tenant: 'shop-1', data: 2 }]),
    );
    await t.flush();
    const failed = await t.deliveries.list({ endpointId: endpoint.id, status: 'failed' });
    expect(failed).toHaveLength(2);

    t.transport.respondWith(200);
    const one = failed.find((d) => d.messageId === m1!.id)!;
    await expect(t.deliveries.retry(one.id, { tenant: 'shop-2' })).rejects.toThrow(WebhookDeliveryNotFoundError);
    expect(await t.deliveries.get(one.id, { tenant: 'shop-2' })).toBeUndefined();
    expect(await t.deliveries.retry(one.id, { tenant: 'shop-1' })).toBe(1);
    await t.worker.runOnce();
    expect(t.transport.sent.at(-1)!.headers['webhook-id']).toBe(m1!.id);
    expect(await t.deliveries.get(one.id)).toMatchObject({ status: 'succeeded', attempts: 1 });
    expect((await t.deliveries.get(one.id))!.history).toHaveLength(2); // the log keeps the failed round

    // Replay a delivered webhook (the partner lost it), then every failed one since the outage began.
    expect(await t.deliveries.retry(one.id)).toBe(1);
    expect(await t.deliveries.retry({ endpointId: endpoint.id, status: 'failed', since: 0 }, { tenant: 'shop-1' })).toBe(1);
    expect(await t.deliveries.retry({ endpointId: endpoint.id, tenant: 'shop-2' }, { tenant: 'shop-1' })).toBe(0);
    await expect(t.deliveries.retry({})).rejects.toThrow(/all: true/);
    await t.worker.runOnce();
    expect(t.transport.sent.slice(-2).map((s) => s.headers['webhook-id']).sort()).toEqual([m1!.id, m2!.id].sort());
    await t.close();
  });

  it('refuses a retry filter it could not apply, instead of matching everything or failing in the store', async () => {
    const t = await sendingApp();
    // `new Date(req.body.since)` of a malformed input: NaN compares false with everything.
    await expect(t.deliveries.retry({ since: new Date('not a date') })).rejects.toThrow(TypeError);
    await expect(t.deliveries.retry({ since: Number.NaN })).rejects.toThrow(TypeError);
    await expect(t.deliveries.retry({ ids: 'dlv_1' as never })).rejects.toThrow(TypeError);
    await expect(t.deliveries.retry({ ids: [42] as never })).rejects.toThrow(TypeError);
    expect(await t.deliveries.retry({ since: new Date(0) })).toBe(0);
    // `limit` and `offset` reach SQL: a query string's NaN or a negative is refused here, not in the store.
    await expect(t.deliveries.list({ limit: Number.NaN })).rejects.toThrow(/WebhookDeliveries.list\(\): limit/);
    await expect(t.endpoints.list({ offset: -1 })).rejects.toThrow(/WebhookEndpoints.list\(\): offset/);
    expect(await t.endpoints.list({ limit: 1, offset: 0 })).toEqual([]);
    await t.close();
  });

  it('reports pending, due and failed counts and the lag, and prunes finished deliveries', async () => {
    const clock = controllableClock();
    const t = await sendingApp({ retry: false });
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
    await t.relay.runOnce();
    clock.advance(5_000);
    expect(await t.deliveries.stats()).toMatchObject({ pending: 1, due: 1, failed: 0, lagMs: expect.any(Number), inFlight: 0 });
    expect((await t.deliveries.stats()).lagMs).toBeGreaterThanOrEqual(5_000);

    t.transport.respondWith(500);
    await t.worker.runOnce();
    expect(await t.deliveries.stats()).toMatchObject({ pending: 0, failed: 1, lagMs: 0 });
    expect(await t.deliveries.prune('1d')).toBe(0);
    clock.advance(86_400_000 + 1);
    expect(await t.deliveries.prune('1d')).toBe(1);
    expect(await t.deliveries.list({})).toEqual([]);
    await t.close();
  });
});

describe('endpoints', () => {
  it('checks what a partner submits', async () => {
    const t = await sendingApp({ eventTypes: ['order.shipped'] });
    const create = (input: object) => t.endpoints.create({ url: 'https://a.example/', eventTypes: ['order.shipped'], ...input } as never);
    const refused = async (input: object, field: string, message: RegExp) => {
      const error = await create(input).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(InvalidWebhookEndpointError);
      expect(error).toMatchObject({ status: 400, field, message: expect.stringMatching(message) });
    };
    await refused({ url: 'http://a.example/' }, 'url', /only https/);
    await refused({ url: 'https://169.254.169.254/latest/meta-data/' }, 'url', /link-local/);
    await refused({ url: 'https://[::ffff:10.0.0.1]/' }, 'url', /IPv4-mapped of private/);
    await refused({ url: 'https://localhost:8080/' }, 'url', /loopback/);
    await refused({ url: 'https://u:p@a.example/' }, 'url', /credentials/);
    await refused({ eventTypes: [] }, 'eventTypes', /at least one/);
    await refused({ eventTypes: ['order.lost'] }, 'eventTypes', /Unknown message type "order.lost"/);
    await refused({ secret: 'whsec_short' }, 'secret', /24 to 64 bytes|base64/);
    await refused({ tenant: '' }, 'tenant', /tenant/);
    const ok = await create({ eventTypes: ['order.shipped', 'order.shipped', '*'] });
    expect(ok.eventTypes).toEqual(['order.shipped', '*']);
    expect(ok.secret).toMatch(/^whsec_[A-Za-z0-9+/]{43}=$/);
    expect(standardSecretKey(ok.secret)).toHaveLength(32);
    await t.close();
  });

  it('never lists secrets, and scopes every call to the tenant', async () => {
    const t = await sendingApp();
    const mine = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'], tenant: 'shop-1', description: 'Orders' });
    await t.endpoints.create({ url: 'https://b.example/', eventTypes: ['*'], tenant: 'shop-2' });
    const { secret: _secret, ...listed } = mine;
    expect(await t.endpoints.list({ tenant: 'shop-1' })).toEqual([listed]);
    expect(JSON.stringify(await t.endpoints.list())).not.toContain('whsec_');
    expect(JSON.stringify(await t.endpoints.get(mine.id))).not.toContain('whsec_');
    const other = { tenant: 'shop-2' };
    expect(await t.endpoints.get(mine.id, other)).toBeUndefined();
    await expect(t.endpoints.update(mine.id, { url: 'https://x.example/' }, other)).rejects.toThrow(WebhookEndpointNotFoundError);
    await expect(t.endpoints.delete(mine.id, other)).rejects.toThrow(WebhookEndpointNotFoundError);
    await expect(t.endpoints.getSecret(mine.id, other)).rejects.toThrow(WebhookEndpointNotFoundError);
    await expect(t.endpoints.rotateSecret(mine.id, other)).rejects.toThrow(WebhookEndpointNotFoundError);
    expect(await t.endpoints.getSecret(mine.id, { tenant: 'shop-1' })).toBe(mine.secret);
    await expect(t.endpoints.update(mine.id, { url: 'http://x.example/' })).rejects.toThrow(InvalidWebhookEndpointError);
    expect(await t.endpoints.update(mine.id, { url: 'https://x.example/v2', description: null })).toMatchObject({
      url: 'https://x.example/v2',
      description: null,
    });
    await t.close();
  });

  it('rotates a secret with an overlap: both sign until it ends, then only the new one', async () => {
    const clock = controllableClock();
    const t = await sendingApp({ secretRotationOverlap: '1h' });
    const endpoint = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    const next = await t.endpoints.rotateSecret(endpoint.id);
    expect(next).not.toBe(endpoint.secret);
    expect(await t.endpoints.getSecret(endpoint.id)).toBe(next);
    const sendOne = async () => {
      await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
      await t.flush();
      return t.transport.sent.at(-1)!;
    };
    const during = await sendOne();
    expect(during.headers['webhook-signature']!.split(' ')).toHaveLength(2);
    expect(during.isSignedWith(endpoint.secret)).toBe(true);
    expect(during.isSignedWith(next)).toBe(true);
    clock.advance(60 * 60_000 + 1);
    const after = await sendOne();
    expect(after.headers['webhook-signature']!.split(' ')).toHaveLength(1);
    expect(after.isSignedWith(next)).toBe(true);
    expect(after.isSignedWith(endpoint.secret)).toBe(false);
    // A leaked secret: retire it at once.
    const leaked = await t.endpoints.rotateSecret(endpoint.id, { overlap: 0 });
    const now = await sendOne();
    expect(now.isSignedWith(leaked)).toBe(true);
    expect(now.isSignedWith(next)).toBe(false);
    await t.close();
  });

  it('encrypts secrets at rest when encryption keys are configured', async () => {
    const store = new InMemoryWebhookStore();
    const key = 'k'.repeat(40);
    const t = await sendingApp(
      { encryption: { keys: [key] } },
      {
        override: (builder) =>
          builder.overrideProvider(WebhooksStorage).useFactory({
            factory: () => {
              const storage = new WebhooksStorage({ allowInMemoryStorage: true });
              storage.registerSource({ endpoints: store, deliveries: store });
              return storage;
            },
          }),
      },
    );
    const a = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    const b = await t.endpoints.create({ url: 'https://b.example/', eventTypes: ['*'] });
    const stored = store.getEndpoint(a.id)!.secrets[0]!.secret;
    expect(stored).toMatch(/^sealed\.v1\./);
    expect(stored).not.toContain(a.secret.slice(6));
    expect(await t.endpoints.getSecret(a.id)).toBe(a.secret);
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
    await t.flush();
    expect(t.transport.single({ endpointId: a.id }).isSignedWith(a.secret)).toBe(true);
    // A sealed secret copied to another endpoint's row doesn't open there.
    store.addEndpointSecret(b.id, { secret: stored, createdAt: 0, expiresAt: null }, 0, Date.now());
    expect(store.getEndpoint(b.id)!.secrets).toHaveLength(1);
    await expect(t.endpoints.getSecret(b.id)).rejects.toThrow(/failed authentication/);
    await t.close();
  });
});

describe('the worker in the background', () => {
  it('delivers on its own after the commit, and finishes an attempt in flight on shutdown', async () => {
    const t = await sendingApp({ worker: { enabled: true, pollInterval: '50ms' } });
    const { OutboxRelay } = await import('@nestjs/outbox');
    t.app.get(OutboxRelay).start();
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    t.transport.respondWith(async (request: WebhookRequest) => {
      if (JSON.parse(request.body).data.slow) {
        await gate;
      }
      return 200;
    });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: { slow: false } }));
    t.webhooks.notify();
    await vi.waitFor(async () => expect((await t.deliveries.list({ status: 'succeeded' })).length).toBe(1), { timeout: 3_000 });

    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: { slow: true } }));
    t.webhooks.notify();
    await vi.waitFor(() => expect(t.transport.sent).toHaveLength(2), { timeout: 3_000 });

    const closing = t.app.close();
    setTimeout(release, 50);
    await closing;

    const events: WebhooksEvent['type'][] = t.events.map((e) => e.type);
    expect(events).toEqual(['delivered', 'delivered']);
  });
});

describe('storage', () => {
  it('uses a registered store, and the in-memory one in tests', async () => {
    const t = await sendingApp();
    expect(t.app.get(WebhooksStorage).endpoints).toBeInstanceOf(InMemoryWebhookStore);
    expect(t.app.get(WebhooksStorage).deliveries).toBe(t.app.get(WebhooksStorage).endpoints);
    await t.close();
  });

});
