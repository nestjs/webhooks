/**
 * The management APIs an app exposes to partners and operators: every check on endpoint
 * input, updates and deletion, listing, secrets, the development delivery options, what
 * `dispatch()` accepts and sends, the fan-out's refusals, and the delivery log's queries.
 */
import { NonRetryableMessageError } from '@nestjs/outbox';
import { WebhookFanOut } from '../lib/services/webhook-fan-out.service.js';
import {
  InvalidWebhookEndpointError,
  WebhookDeliveryNotFoundError,
  WebhookEndpointNotFoundError,
  WebhooksError,
} from '../lib/index.js';
import { controllableClock, sendingApp } from './helpers.js';

afterEach(() => vi.restoreAllMocks());

describe('WebhookEndpoints input checks', () => {
  it.each<[string, object, string, RegExp]>([
    ['a URL that is not a string', { url: 42 }, 'url', /URL is empty/],
    ['an empty URL', { url: '' }, 'url', /URL is empty/],
    ['a URL with a fragment', { url: 'https://a.example/#x' }, 'url', /fragment/],
    ['a private address', { url: 'https://10.1.2.3/' }, 'url', /10\.1\.2\.3 is private/],
    ['an IPv6 loopback', { url: 'https://[::1]:8443/' }, 'url', /loopback/],
    ['a cloud metadata address', { url: 'https://[fd00:ec2::254]/' }, 'url', /metadata/],
    ['event types that are not an array', { eventTypes: 'order.shipped' }, 'eventTypes', /at least one/],
    ['a malformed event type', { eventTypes: ['order shipped'] }, 'eventTypes', /"order shipped" is not a message type/],
    ['a non-string event type', { eventTypes: [7] }, 'eventTypes', /7 is not a message type/],
    ['more than 100 event types', { eventTypes: Array.from({ length: 101 }, (_, i) => `t.e${i}`) }, 'eventTypes', /more than 100/],
    ['a description that is too long', { description: 'x'.repeat(1_025) }, 'description', /at most 1024/],
    ['a description that is not a string', { description: 42 }, 'description', /must be a string/],
    ['a tenant that is too long', { tenant: 't'.repeat(257) }, 'tenant', /at most 256/],
    ['a tenant that is not a string', { tenant: 42 }, 'tenant', /tenant must be null/],
    ['a secret with whitespace', { secret: `whsec_${Buffer.alloc(32).toString('base64')}\n` }, 'secret', /whitespace/],
  ])('refuses %s, naming the field, with status 400', async (_name, input, field, message) => {
    const t = await sendingApp();
    const error = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'], ...input } as never).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(InvalidWebhookEndpointError);
    expect(error).toBeInstanceOf(WebhooksError);
    expect(error).toMatchObject({ name: 'InvalidWebhookEndpointError', status: 400, field, message: expect.stringMatching(message) });
    expect(await t.endpoints.list()).toEqual([]);
    await t.close();
  });

  it('accepts the limits exactly, and stores the normalized URL', async () => {
    const t = await sendingApp();
    const endpoint = await t.endpoints.create({
      url: 'https://Hooks.Example.COM:443/a/../b?x=1',
      eventTypes: Array.from({ length: 100 }, (_, i) => `t.e${i}`),
      description: 'x'.repeat(1_024),
      tenant: 't'.repeat(256),
      secret: `whsec_${Buffer.alloc(24, 1).toString('base64')}`,
    });
    expect(endpoint).toMatchObject({ url: 'https://hooks.example.com/b?x=1', enabled: true, disabledReason: null, failingSince: null });
    expect(endpoint.id).toMatch(/^ep_[0-9a-f]{32}$/);
    expect(endpoint.eventTypes).toHaveLength(100);
    await t.close();
  });

  it('accepts http and private addresses only when the delivery options allow them; metadata never', async () => {
    const dev = await sendingApp({ delivery: { allowHttp: true, allowPrivateNetworks: true } });
    for (const url of ['http://localhost:3000/hooks', 'http://10.0.0.5/hooks', 'https://[fd12::1]/']) {
      await expect(dev.endpoints.create({ url, eventTypes: ['*'] })).resolves.toMatchObject({ url });
    }
    await expect(dev.endpoints.create({ url: 'http://169.254.169.254/', eventTypes: ['*'] })).rejects.toThrow(/link-local/);
    await expect(dev.endpoints.create({ url: 'ftp://a.example/', eventTypes: ['*'] })).rejects.toThrow(/only http: and https:/);
    await dev.close();

    const listed = await sendingApp({ delivery: { allowedAddresses: ['10.20.0.0/16'] } });
    await expect(listed.endpoints.create({ url: 'https://10.20.1.1/', eventTypes: ['*'] })).resolves.toBeDefined();
    await expect(listed.endpoints.create({ url: 'https://10.21.1.1/', eventTypes: ['*'] })).rejects.toThrow(InvalidWebhookEndpointError);
    await listed.close();
  });
});

describe('WebhookEndpoints management', () => {
  it('lists newest first, by tenant, with limit and offset', async () => {
    const clock = controllableClock();
    const t = await sendingApp();
    for (const [n, tenant] of [[1, 'shop-1'], [2, 'shop-2'], [3, 'shop-1'], [4, 'shop-1']] as const) {
      await t.endpoints.create({ url: `https://e${n}.example/`, eventTypes: ['*'], tenant });
      clock.advance(1_000);
    }
    const urls = (list: { url: string }[]) => list.map((e) => e.url);
    expect(urls(await t.endpoints.list())).toEqual(['https://e4.example/', 'https://e3.example/', 'https://e2.example/', 'https://e1.example/']);
    expect(urls(await t.endpoints.list({ tenant: 'shop-1', limit: 2 }))).toEqual(['https://e4.example/', 'https://e3.example/']);
    expect(urls(await t.endpoints.list({ tenant: 'shop-1', offset: 2 }))).toEqual(['https://e1.example/']);
    expect(await t.endpoints.list({ tenant: 'shop-9' })).toEqual([]);
    await t.close();
  });

  it('updates event types with the same checks, and records who disabled an endpoint', async () => {
    const clock = controllableClock();
    const t = await sendingApp({ eventTypes: ['order.shipped', 'order.cancelled'] });
    const endpoint = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['order.shipped'] });
    clock.advance(1_000);

    await expect(t.endpoints.update(endpoint.id, { eventTypes: ['order.lost'] })).rejects.toMatchObject({ field: 'eventTypes' });
    await expect(t.endpoints.update(endpoint.id, { description: 'x'.repeat(2_000) })).rejects.toMatchObject({ field: 'description' });
    const updated = await t.endpoints.update(endpoint.id, { eventTypes: ['order.cancelled', 'order.cancelled'], description: 'Cancellations' });
    expect(updated).toMatchObject({ eventTypes: ['order.cancelled'], description: 'Cancellations', url: 'https://a.example/' });
    expect(updated.updatedAt).toBeGreaterThanOrEqual(endpoint.createdAt + 1_000);

    expect(await t.endpoints.update(endpoint.id, { enabled: false })).toMatchObject({ enabled: false, disabledReason: 'manual' });
    // An empty patch changes nothing but the time.
    expect(await t.endpoints.update(endpoint.id, {})).toMatchObject({ enabled: false, disabledReason: 'manual', description: 'Cancellations' });
    await t.close();
  });

  it('deletes an endpoint once, and keeps its delivery log', async () => {
    const t = await sendingApp();
    const endpoint = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'a.b', data: {} }));
    await t.flush();

    await t.endpoints.delete(endpoint.id);
    expect(await t.endpoints.get(endpoint.id)).toBeUndefined();
    const error = await t.endpoints.delete(endpoint.id).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(WebhookEndpointNotFoundError);
    expect(error).toMatchObject({ status: 404, endpointId: endpoint.id, message: `Webhook endpoint ${endpoint.id} not found` });
    await expect(t.endpoints.update(endpoint.id, { enabled: true })).rejects.toThrow(WebhookEndpointNotFoundError);
    expect(await t.deliveries.list({ endpointId: endpoint.id })).toEqual([expect.objectContaining({ status: 'succeeded' })]);
    await t.close();
  });

  it('treats an empty or non-string id as not found', async () => {
    const t = await sendingApp();
    expect(await t.endpoints.get('')).toBeUndefined();
    expect(await t.endpoints.get(42 as never)).toBeUndefined();
    await expect(t.endpoints.getSecret('')).rejects.toThrow(WebhookEndpointNotFoundError);
    expect(await t.deliveries.get('')).toBeUndefined();
    await expect(t.deliveries.retry('dlv_missing')).rejects.toMatchObject({ status: 404, deliveryId: 'dlv_missing' });
    await expect(t.deliveries.retry('dlv_missing')).rejects.toThrow(WebhookDeliveryNotFoundError);
    await t.close();
  });

  it('rotates to a secret of the caller\'s choosing, checked like a new endpoint\'s', async () => {
    const t = await sendingApp();
    const endpoint = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    await expect(t.endpoints.rotateSecret(endpoint.id, { secret: 'whsec_short' })).rejects.toMatchObject({ field: 'secret', status: 400 });
    expect(await t.endpoints.getSecret(endpoint.id)).toBe(endpoint.secret);

    const chosen = `whsec_${Buffer.alloc(48, 9).toString('base64')}`;
    expect(await t.endpoints.rotateSecret(endpoint.id, { secret: chosen, overlap: '1m' })).toBe(chosen);
    expect(await t.endpoints.getSecret(endpoint.id)).toBe(chosen);
    await expect(t.endpoints.rotateSecret(endpoint.id, { overlap: 'soon' as never })).rejects.toThrow(/Invalid duration/);
    await t.close();
  });

  it('generates a different secret for every endpoint and rotation', async () => {
    const t = await sendingApp();
    const secrets = new Set<string>();
    for (let i = 0; i < 5; i++) {
      const endpoint = await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
      secrets.add(endpoint.secret);
      secrets.add(await t.endpoints.rotateSecret(endpoint.id));
    }
    expect(secrets.size).toBe(10);
    await t.close();
  });
});

describe('dispatch()', () => {
  it('returns one message per input, and sends a caller-chosen id as the webhook-id', async () => {
    const t = await sendingApp();
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    const id = `order_42-shipped_${'x'.repeat(100)}`;
    const messages = await t.transaction((tx) =>
      t.webhooks.dispatch(tx, [
        { type: 'order.shipped', data: 1, id },
        { type: 'order.shipped', data: 2 },
      ]),
    );
    expect(messages.map((m) => m.id)).toEqual([id, expect.stringMatching(/^msg_/)]);
    expect(messages[0]).toMatchObject({ type: 'order.shipped', tenant: null, createdAt: expect.any(Number) });

    await t.flush();
    expect(t.transport.sent.map((s) => s.headers['webhook-id']).sort()).toEqual(messages.map((m) => m.id).sort());
    await t.close();
  });

  it('refuses an id over 128 characters and a tenant over 256', async () => {
    const t = await sendingApp();
    const dispatch = (message: object) => t.transaction((tx) => t.webhooks.dispatch(tx, message as never));
    await expect(dispatch({ type: 'a.b', data: {}, id: 'x'.repeat(129) })).rejects.toThrow(/at most 128/);
    await expect(dispatch({ type: 'a.b', data: {}, tenant: 't'.repeat(257) })).rejects.toThrow(/at most 256/);
    await expect(dispatch({ data: {} })).rejects.toThrow(/undefined is not a message type/);
    await expect(dispatch(null as never)).rejects.toThrow(/not a message type/);
    await t.close();
  });

  it('serializes the data once, at dispatch: a later change to the object is not sent', async () => {
    const t = await sendingApp();
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    const data = { status: 'shipped', at: new Date('2026-01-02T03:04:05.000Z'), note: null as string | null };
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'order.shipped', data }));
    data.status = 'lost';
    data.note = 'changed';

    await t.flush();
    expect(t.transport.single().data).toEqual({ status: 'shipped', at: '2026-01-02T03:04:05.000Z', note: null });
    await t.close();
  });

  it('sends null data as JSON null', async () => {
    const t = await sendingApp();
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'account.closed', data: null }));
    await t.flush();
    expect(JSON.parse(t.transport.single().body)).toMatchObject({ type: 'account.closed', data: null });
    await t.close();
  });

  it('delivers to endpoints of every type they subscribe to, and to nothing else', async () => {
    const t = await sendingApp();
    const both = await t.endpoints.create({ url: 'https://both.example/', eventTypes: ['order.shipped', 'order.cancelled'] });
    const prefix = await t.endpoints.create({ url: 'https://prefix.example/', eventTypes: ['order'] });
    await t.transaction((tx) =>
      t.webhooks.dispatch(tx, [
        { type: 'order.shipped', data: 1 },
        { type: 'order.cancelled', data: 2 },
        { type: 'order.refunded', data: 3 },
      ]),
    );
    await t.flush();
    expect(t.transport.filter({ endpointId: both.id }).map((s) => s.type).sort()).toEqual(['order.cancelled', 'order.shipped']);
    expect(t.transport.filter({ endpointId: prefix.id })).toEqual([]);
    await t.close();
  });

  it('refuses an outbox message that is not a webhook message, for good', async () => {
    const t = await sendingApp();
    const fanOut = t.app.get(WebhookFanOut);
    await expect(fanOut.fanOut({ id: 'm', type: 'a.b' } as never)).rejects.toThrow(NonRetryableMessageError);
    await expect(fanOut.fanOut(null as never)).rejects.toThrow(/Not a webhook message: null/);
    await expect(fanOut.fanOut({ id: 'm', type: 'a.b', body: '{}', createdAt: 1, tenant: 7 } as never)).rejects.toThrow(NonRetryableMessageError);
    await t.close();
  });
});

describe('the delivery log', () => {
  it('filters by type, status and tenant, newest first, with limit and offset', async () => {
    const clock = controllableClock();
    const t = await sendingApp({ retry: false });
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'], tenant: 'shop-1' });
    await t.endpoints.create({ url: 'https://b.example/', eventTypes: ['*'], tenant: 'shop-2' });
    t.transport.respondWith((request) => (JSON.parse(request.body).data.fail ? 500 : 200));
    for (const [type, tenant, fail] of [
      ['order.shipped', 'shop-1', false],
      ['order.cancelled', 'shop-1', true],
      ['order.shipped', 'shop-2', true],
      ['order.shipped', 'shop-1', true],
    ] as const) {
      await t.transaction((tx) => t.webhooks.dispatch(tx, { type, tenant, data: { fail } }));
      clock.advance(1_000);
    }
    await t.flush();

    const shape = (list: { type: string; tenant: string | null; status: string }[]) => list.map((d) => `${d.type}/${d.tenant}/${d.status}`);
    expect(shape(await t.deliveries.list({ tenant: 'shop-1' }))).toEqual([
      'order.shipped/shop-1/failed',
      'order.cancelled/shop-1/failed',
      'order.shipped/shop-1/succeeded',
    ]);
    expect(shape(await t.deliveries.list({ type: 'order.shipped', status: 'failed' }))).toEqual(['order.shipped/shop-1/failed', 'order.shipped/shop-2/failed']);
    expect(shape(await t.deliveries.list({ limit: 1, offset: 1 }))).toEqual(['order.shipped/shop-2/failed']);
    expect(shape(await t.deliveries.list({ failureReason: 'exhausted', lastStatusCode: 500, tenant: 'shop-1' }))).toEqual([
      'order.shipped/shop-1/failed',
      'order.cancelled/shop-1/failed',
    ]);
    expect(shape(await t.deliveries.list({ failureReason: null, lastStatusCode: 200 }))).toEqual(['order.shipped/shop-1/succeeded']);
    expect(shape(await t.deliveries.list({ failureReason: 'exhausted', lastStatusCode: 500, limit: 1, offset: 1 }))).toEqual([
      'order.shipped/shop-2/failed',
    ]);
    clock.advance(1_000);
    await t.transaction((tx) => t.webhooks.dispatch(tx, { type: 'order.shipped', tenant: 'shop-1', data: { fail: false } }));
    await t.relay.runOnce();
    expect(shape(await t.deliveries.list({ lastStatusCode: null, failureReason: null }))).toEqual(['order.shipped/shop-1/pending']);
    await expect(t.deliveries.list({ offset: 1.5 })).rejects.toThrow(/offset must be a whole number/);
    await expect(t.deliveries.list({ limit: 0 })).rejects.toThrow(/limit must be a whole number of at least 1/);
    await t.close();
  });

  it('retries everything with { all: true } and by ids, starting a new round of attempts', async () => {
    const t = await sendingApp({ retry: { attempts: 2, backoff: { delay: '1h', jitter: 'none' } } });
    await t.endpoints.create({ url: 'https://a.example/', eventTypes: ['*'] });
    t.transport.respondWith(500);
    const [first, second] = await t.transaction((tx) => t.webhooks.dispatch(tx, [{ type: 'a.b', data: 1 }, { type: 'a.b', data: 2 }]));
    await t.flush();
    const byMessage = async (id: string) => (await t.deliveries.list({ messageId: id }))[0]!;
    expect(await byMessage(first!.id)).toMatchObject({ status: 'pending', attempts: 1 });

    expect(await t.deliveries.retry({ ids: [(await byMessage(first!.id)).id] })).toBe(1);
    expect(await byMessage(first!.id)).toMatchObject({ status: 'pending', attempts: 0, nextAttemptAt: expect.any(Number) });
    expect(await t.deliveries.retry({ all: true })).toBe(2);
    t.transport.respondWith(200);
    expect(await t.worker.runOnce()).toMatchObject({ delivered: 2 });
    // The new round's attempt is attempt 1 again; the log keeps both rounds.
    expect(t.transport.sent.slice(-2).map((s) => s.attempt)).toEqual([1, 1]);
    expect((await t.deliveries.get((await byMessage(second!.id)).id))!.history.map((a) => [a.attempt, a.statusCode])).toEqual([
      [1, 500],
      [1, 200],
    ]);
    await t.close();
  });
});
