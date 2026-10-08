import assert from 'node:assert/strict';
import type { WebhookDelivery, WebhookDeliveryAttempt } from '../interfaces/webhook-delivery.interface.js';
import type { WebhookEndpointRecord } from '../interfaces/webhook-endpoint-store.interface.js';
import type { WebhookMessage } from '../interfaces/webhook-message.interface.js';
import type { Awaitable } from '../interfaces/awaitable.interface.js';
import type { WebhookEndpointStore } from '../interfaces/webhook-endpoint-store.interface.js';
import type { WebhookDeliveryStore } from '../interfaces/webhook-delivery-store.interface.js';
import { prefixedId } from '../utils/uuid.util.js';

/** What a contract case runs against. */
export interface WebhookStoreHarness<S> {
  /** The store, on empty tables. */
  store: S;
  /** Called after the case, pass or fail. */
  close?(): Awaitable<void>;
}

export interface WebhookStoreContractOptions {
  /**
   * Adds the concurrency cases: rotations and failures racing on one endpoint
   * (`webhookEndpointStoreContract()`); workers claiming side by side, fan-outs of one
   * message racing, a stale worker racing a takeover, a manual retry racing a claim
   * (`webhookDeliveryStoreContract()`). Each case first opens up to eight of the store's
   * connections at once, so the racing calls don't start a connection apart. On PGlite they
   * pass serialized; run them against a server too, through a pool of several connections.
   * Default `false`.
   */
  concurrent?: boolean;
}

export interface WebhookStoreContractCase {
  name: string;
  run(): Promise<void>;
}

type Case<S> = [string, (store: S) => Promise<void>];

/** The contract every `WebhookEndpointStore` passes, as cases for any test runner. */
export function webhookEndpointStoreContract(
  createStore: () => Awaitable<WebhookStoreHarness<WebhookEndpointStore>>,
  options: WebhookStoreContractOptions = {},
): WebhookStoreContractCase[] {
  return toCases([...ENDPOINT_CASES, ...(options.concurrent ? ENDPOINT_CONCURRENT : [])], createStore);
}

/** The contract every `WebhookDeliveryStore` passes, as cases for any test runner. */
export function webhookDeliveryStoreContract(
  createStore: () => Awaitable<WebhookStoreHarness<WebhookDeliveryStore>>,
  options: WebhookStoreContractOptions = {},
): WebhookStoreContractCase[] {
  return toCases([...DELIVERY_CASES, ...(options.concurrent ? DELIVERY_CONCURRENT : [])], createStore);
}

function toCases<S>(cases: Case<S>[], create: () => Awaitable<WebhookStoreHarness<S>>): WebhookStoreContractCase[] {
  return cases.map(([name, body]) => ({
    name,
    async run() {
      const harness = await create();
      try {
        await body(harness.store);
      } finally {
        await harness.close?.();
      }
    },
  }));
}

// ------------------------------------------------------------------ fixtures

function endpoint(overrides: Partial<WebhookEndpointRecord> = {}): WebhookEndpointRecord {
  return {
    id: prefixedId('ep'),
    tenant: 'shop-1',
    url: 'https://hooks.example.com/store?token=a%20b',
    eventTypes: ['order.shipped', 'order.cancelled'],
    description: "The cat shelter's endpoint (Kätzchen & Kibble)",
    enabled: true,
    disabledReason: null,
    failingSince: null,
    createdAt: 1_000,
    updatedAt: 1_000,
    secrets: [{ secret: 'whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw', createdAt: 1_000, expiresAt: null }],
    ...overrides,
  };
}

function message(overrides: Partial<WebhookMessage> = {}): WebhookMessage {
  return {
    id: prefixedId('msg'),
    type: 'order.shipped',
    tenant: 'shop-1',
    // Exact bytes: spacing, key order, non-ASCII and escapes must round-trip.
    body: '{"type":"order.shipped", "timestamp":"1970-01-01T00:00:01.000Z","data":{"z":1,"a":"Łódź \\u2028 \\"q\\""}}',
    createdAt: 1_000,
    ...overrides,
  };
}

function delivery(m: WebhookMessage, endpointId: string, overrides: Partial<WebhookDelivery> = {}): WebhookDelivery {
  return {
    id: prefixedId('dlv'),
    messageId: m.id,
    endpointId,
    tenant: m.tenant,
    type: m.type,
    status: 'pending',
    attempts: 0,
    nextAttemptAt: m.createdAt,
    lastAttemptAt: null,
    lastStatusCode: null,
    lastError: null,
    failureReason: null,
    createdAt: m.createdAt,
    completedAt: null,
    ...overrides,
  };
}

function attempt(d: WebhookDelivery, n: number, at: number, statusCode: number | null = 500): WebhookDeliveryAttempt {
  return {
    deliveryId: d.id,
    attempt: n,
    at,
    durationMs: 12,
    statusCode,
    response: statusCode === null ? null : 'upstream said "no" ü',
    error: statusCode !== null && statusCode < 300 ? null : `failed ${n}`,
  };
}

const ids = (items: readonly { id: string }[]) => items.map((item) => item.id);

/** How many calls a concurrency case runs at once. */
const RACERS = 8;

/**
 * Makes `read` `RACERS` times at once, so a pooled store has its connections open before a
 * race: a pool opens them on demand, and calls that each wait for a new connection start a
 * handshake apart, so they would never overlap.
 */
async function openConnections(read: () => Awaitable<unknown>): Promise<void> {
  await Promise.all(Array.from({ length: RACERS }, () => read()));
}

/** The ids that occur more than once in `list`. */
const repeated = (list: readonly string[]) => [...new Set(list.filter((id, i) => list.indexOf(id) !== i))];

// ------------------------------------------------------------------ endpoints

const ENDPOINT_CASES: Case<WebhookEndpointStore>[] = [
  [
    'stores an endpoint and reads it back unchanged',
    async (store) => {
      const e = endpoint({ tenant: null, description: null });
      await store.createEndpoint(e);
      assert.deepEqual(await store.getEndpoint(e.id), e);

      const other = endpoint();
      await store.createEndpoint(other);
      assert.deepEqual(await store.getEndpoint(other.id), other);
      assert.equal(await store.getEndpoint('ep_missing'), undefined);
    },
  ],
  [
    "lists a tenant's endpoints newest first, with limit and offset",
    async (store) => {
      const a = endpoint({ createdAt: 1 });
      const b = endpoint({ createdAt: 2 });
      const c = endpoint({ createdAt: 3, tenant: 'shop-2' });
      const d = endpoint({ createdAt: 4, tenant: null });
      for (const e of [a, b, c, d]) {
        await store.createEndpoint(e);
      }

      assert.deepEqual(ids(await store.listEndpoints({ tenant: 'shop-1' })), [b.id, a.id]);
      assert.deepEqual(ids(await store.listEndpoints({ tenant: null })), [d.id]);
      assert.deepEqual(ids(await store.listEndpoints({})), [d.id, c.id, b.id, a.id]);
      assert.deepEqual(ids(await store.listEndpoints({ limit: 2, offset: 1 })), [c.id, b.id]);
    },
  ],
  [
    'lists 50 endpoints unless asked for more',
    async (store) => {
      for (let i = 0; i < 52; i++) {
        await store.createEndpoint(endpoint({ createdAt: i }));
      }
      assert.equal((await store.listEndpoints({})).length, 50);
      assert.equal((await store.listEndpoints({ limit: 60 })).length, 52);
    },
  ],
  [
    "finds the enabled endpoints of exactly the message's tenant that subscribe to its type",
    async (store) => {
      const shipped = endpoint();
      const everything = endpoint({ eventTypes: ['*'] });
      const cancelledOnly = endpoint({ eventTypes: ['order.cancelled'] });
      const disabled = endpoint({ enabled: false, disabledReason: 'manual' });
      const otherTenant = endpoint({ tenant: 'shop-2', eventTypes: ['*'] });
      const noTenant = endpoint({ tenant: null, eventTypes: ['*'] });
      const prefix = endpoint({ eventTypes: ['order'] });
      for (const e of [shipped, everything, cancelledOnly, disabled, otherTenant, noTenant, prefix]) {
        await store.createEndpoint(e);
      }

      assert.deepEqual(ids(await store.findSubscribedEndpoints('shop-1', 'order.shipped')).sort(), [shipped.id, everything.id].sort());
      assert.deepEqual(ids(await store.findSubscribedEndpoints(null, 'order.shipped')), [noTenant.id]);
      assert.deepEqual(ids(await store.findSubscribedEndpoints('shop-3', 'order.shipped')), []);
      assert.deepEqual(ids(await store.findSubscribedEndpoints("shop-1' OR '1'='1", 'order.shipped')), []);
    },
  ],
  [
    'updates only the fields in the patch, and returns the endpoint',
    async (store) => {
      const e = endpoint();
      await store.createEndpoint(e);

      const updated = await store.updateEndpoint(e.id, { url: 'https://new.example.com/h', description: null }, 2_000);
      assert.deepEqual(updated, { ...e, url: 'https://new.example.com/h', description: null, updatedAt: 2_000 });
      assert.deepEqual(await store.getEndpoint(e.id), updated);

      await store.updateEndpoint(e.id, { enabled: false, disabledReason: 'manual', failingSince: 5 }, 3_000);
      const back = await store.updateEndpoint(e.id, { enabled: true, disabledReason: null, failingSince: null, eventTypes: ['*'] }, 4_000);
      assert.deepEqual(back, { ...updated, eventTypes: ['*'], updatedAt: 4_000 });

      assert.equal(await store.updateEndpoint('ep_missing', { enabled: false }, 1), undefined);
    },
  ],
  [
    'deletes an endpoint once',
    async (store) => {
      const e = endpoint();
      await store.createEndpoint(e);
      assert.equal(await store.deleteEndpoint(e.id), true);
      assert.equal(await store.getEndpoint(e.id), undefined);
      assert.equal(await store.deleteEndpoint(e.id), false);
    },
  ],
  [
    'rotates a secret: the new one first, the others expire by the overlap, expired ones go',
    async (store) => {
      const e = endpoint({
        secrets: [
          { secret: 'whsec_current', createdAt: 1, expiresAt: null },
          { secret: 'whsec_late', createdAt: 0, expiresAt: 9_000 },
          { secret: 'whsec_gone', createdAt: 0, expiresAt: 1_500 },
        ],
      });
      await store.createEndpoint(e);

      assert.equal(await store.addEndpointSecret(e.id, { secret: 'whsec_new', createdAt: 2_000, expiresAt: null }, 5_000, 2_000), true);
      assert.deepEqual((await store.getEndpoint(e.id))!.secrets, [
        { secret: 'whsec_new', createdAt: 2_000, expiresAt: null },
        { secret: 'whsec_current', createdAt: 1, expiresAt: 5_000 },
        { secret: 'whsec_late', createdAt: 0, expiresAt: 5_000 },
      ]);

      // Overlap 0: the others expire now, and are dropped.
      await store.addEndpointSecret(e.id, { secret: 'whsec_newer', createdAt: 3_000, expiresAt: null }, 3_000, 3_000);
      assert.deepEqual((await store.getEndpoint(e.id))!.secrets, [{ secret: 'whsec_newer', createdAt: 3_000, expiresAt: null }]);

      assert.equal(await store.addEndpointSecret('ep_missing', { secret: 'x', createdAt: 0, expiresAt: null }, 0, 0), false);
    },
  ],
  [
    'records failures from the first one, and disables once they last long enough',
    async (store) => {
      const e = endpoint();
      await store.createEndpoint(e);
      assert.equal(await store.recordEndpointFailure(e.id, { at: 100, disableIfFailingSince: 0, reason: 'failing' }), false);
      assert.equal((await store.getEndpoint(e.id))!.failingSince, 100);

      // Still failing since 100, not 200.
      assert.equal(await store.recordEndpointFailure(e.id, { at: 200, disableIfFailingSince: 99, reason: 'failing' }), false);
      assert.equal((await store.getEndpoint(e.id))!.failingSince, 100);

      assert.equal(await store.recordEndpointFailure(e.id, { at: 300, disableIfFailingSince: null, reason: 'failing' }), false);
      assert.equal(await store.recordEndpointFailure(e.id, { at: 400, disableIfFailingSince: 100, reason: 'failing' }), true);
      const disabled = (await store.getEndpoint(e.id))!;
      assert.equal(disabled.enabled, false);
      assert.equal(disabled.disabledReason, 'failing');

      // Already disabled: not disabled again.
      assert.equal(await store.recordEndpointFailure(e.id, { at: 500, disableIfFailingSince: 500, reason: 'gone' }), false);
      assert.equal((await store.getEndpoint(e.id))!.disabledReason, 'failing');
      assert.equal(await store.recordEndpointFailure('ep_missing', { at: 1, disableIfFailingSince: 1, reason: 'gone' }), false);
    },
  ],
  [
    'disables at once for 410 Gone, and a success clears the failures',
    async (store) => {
      const e = endpoint();
      const gone = endpoint();
      await store.createEndpoint(e);
      await store.createEndpoint(gone);

      await store.recordEndpointFailure(e.id, { at: 100, disableIfFailingSince: null, reason: 'failing' });
      await store.recordEndpointSuccess(e.id);
      assert.equal((await store.getEndpoint(e.id))!.failingSince, null);
      assert.equal((await store.getEndpoint(e.id))!.enabled, true);
      await store.recordEndpointSuccess('ep_missing');

      assert.equal(await store.recordEndpointFailure(gone.id, { at: 100, disableIfFailingSince: 100, reason: 'gone' }), true);
      assert.equal((await store.getEndpoint(gone.id))!.disabledReason, 'gone');
    },
  ],
];

const ENDPOINT_CONCURRENT: Case<WebhookEndpointStore>[] = [
  [
    'rotations at once all land: each keeps the secrets of the ones before it',
    async (store) => {
      const e = endpoint({ secrets: [{ secret: 'whsec_0', createdAt: 0, expiresAt: null }] });
      await store.createEndpoint(e);
      await openConnections(() => store.getEndpoint(e.id));

      // Rotations that read the secrets, then write them back without serializing on the
      // endpoint's row, overwrite each other: every round races RACERS of them.
      const expected = ['whsec_0'];
      for (let round = 1; round <= 3; round++) {
        const added = Array.from({ length: RACERS }, (_, i) => `whsec_${round}_${i}`);
        const rotated = await Promise.all(
          added.map((secret) => store.addEndpointSecret(e.id, { secret, createdAt: round, expiresAt: null }, 1_000_000, round)),
        );
        assert.deepEqual(rotated, Array(RACERS).fill(true));
        expected.push(...added);

        const secrets = (await store.getEndpoint(e.id))!.secrets;
        const kept = secrets.map((s) => s.secret);
        const lost = expected.filter((secret) => !kept.includes(secret));
        assert.deepEqual(lost, [], `a rotation was lost in round ${round}: ${lost.join(', ')} (kept ${kept.length} of ${expected.length})`);
        assert.deepEqual(repeated(kept), [], `a secret was kept twice in round ${round}`);
        assert.equal(kept.length, expected.length);
        assert.equal(secrets.filter((s) => s.expiresAt === null).length, 1, 'only the newest secret never expires');
      }
    },
  ],
  [
    'many failures past the threshold at once disable the endpoint once',
    async (store) => {
      const e = endpoint({ failingSince: 1 });
      await store.createEndpoint(e);
      await openConnections(() => store.getEndpoint(e.id));
      const results = await Promise.all(
        Array.from({ length: RACERS }, (_, i) => store.recordEndpointFailure(e.id, { at: 100 + i, disableIfFailingSince: 50, reason: 'failing' })),
      );
      assert.equal(results.filter(Boolean).length, 1, `disabled by ${results.filter(Boolean).length} calls`);
      assert.equal((await store.getEndpoint(e.id))!.enabled, false);
    },
  ],
];

// ------------------------------------------------------------------ deliveries

const DELIVERY_CASES: Case<WebhookDeliveryStore>[] = [
  [
    'creates a message and its deliveries, and reads them back unchanged',
    async (store) => {
      const m = message();
      const [a, b] = [delivery(m, 'ep_a'), delivery(m, 'ep_b')];
      assert.equal(await store.createDeliveries(m, [a, b]), 2);
      assert.deepEqual(await store.getMessage(m.id), m);
      assert.deepEqual(await store.getDelivery(a.id), a);
      assert.deepEqual(await store.getDelivery(b.id), b);
      assert.equal(await store.getDelivery('dlv_missing'), undefined);
      assert.equal(await store.getMessage('msg_missing'), undefined);

      const untenanted = message({ tenant: null });
      const c = delivery(untenanted, 'ep_a');
      await store.createDeliveries(untenanted, [c]);
      assert.deepEqual(await store.getDelivery(c.id), c);
      assert.deepEqual(await store.getMessage(untenanted.id), untenanted);
    },
  ],
  [
    'creates each (message, endpoint) delivery once, whatever its id: a fan-out can run again',
    async (store) => {
      const m = message();
      const first = delivery(m, 'ep_a');
      assert.equal(await store.createDeliveries(m, [first]), 1);
      assert.equal(await store.createDeliveries(m, [delivery(m, 'ep_a'), delivery(m, 'ep_b')]), 1);
      assert.equal(await store.createDeliveries(m, [delivery(m, 'ep_a'), delivery(m, 'ep_b')]), 0);
      const all = await store.listDeliveries({ messageId: m.id });
      assert.deepEqual(all.map((d) => d.endpointId).sort(), ['ep_a', 'ep_b']);
      assert.ok(all.some((d) => d.id === first.id));
    },
  ],
  [
    'claims due, unleased, pending deliveries, most overdue first, with their message',
    async (store) => {
      const m = message();
      const late = delivery(m, 'ep_a', { nextAttemptAt: 5 });
      const later = delivery(m, 'ep_b', { nextAttemptAt: 8 });
      const future = delivery(m, 'ep_c', { nextAttemptAt: 500 });
      const done = delivery(m, 'ep_d', { status: 'succeeded', nextAttemptAt: null, completedAt: 3 });
      const failed = delivery(m, 'ep_e', { status: 'failed', nextAttemptAt: null, completedAt: 3, failureReason: 'exhausted' });
      await store.createDeliveries(m, [later, future, done, failed, late]);

      const claimed = await store.claimDeliveries({ owner: 'w1', now: 10, leaseMs: 100, limit: 10 });
      assert.deepEqual(claimed.map((c) => c.delivery.id), [late.id, later.id]);
      assert.deepEqual(claimed[0]!.message, m);
      assert.deepEqual(claimed[0]!.delivery, late);

      // Leased until 110.
      assert.deepEqual(await store.claimDeliveries({ owner: 'w2', now: 109, leaseMs: 100, limit: 10 }), []);
      const expired = await store.claimDeliveries({ owner: 'w3', now: 110, leaseMs: 100, limit: 1 });
      assert.deepEqual(expired.map((c) => c.delivery.id), [late.id]);
    },
  ],
  [
    "records an attempt only for the lease's owner, and clears the lease",
    async (store) => {
      const m = message();
      const d = delivery(m, 'ep_a');
      await store.createDeliveries(m, [d]);
      await store.claimDeliveries({ owner: 'w1', now: 1_000, leaseMs: 100, limit: 10 });

      const failure = attempt(d, 1, 1_000, 503);
      const update = { status: 'pending' as const, attempts: 1, nextAttemptAt: 1_050, failureReason: null, completedAt: null, attempt: failure };
      assert.equal(await store.recordDeliveryAttempt(d.id, 'w2', update), false);
      assert.equal(await store.recordDeliveryAttempt(d.id, 'w1', update), true);
      assert.equal(await store.recordDeliveryAttempt(d.id, 'w1', update), false, 'the lease was cleared');
      assert.deepEqual(await store.getDelivery(d.id), {
        ...d,
        attempts: 1,
        nextAttemptAt: 1_050,
        lastAttemptAt: 1_000,
        lastStatusCode: 503,
        lastError: 'failed 1',
      });
      assert.deepEqual(await store.listDeliveryAttempts(d.id), [failure]);

      // Unleased and due again at 1050, not before.
      assert.deepEqual(await store.claimDeliveries({ owner: 'w3', now: 1_049, leaseMs: 100, limit: 10 }), []);
      assert.equal((await store.claimDeliveries({ owner: 'w3', now: 1_050, leaseMs: 100, limit: 10 })).length, 1);

      const success = attempt(d, 2, 1_060, 204);
      await store.recordDeliveryAttempt(d.id, 'w3', {
        status: 'succeeded',
        attempts: 2,
        nextAttemptAt: null,
        failureReason: null,
        completedAt: 1_070,
        attempt: success,
      });

      const delivered = (await store.getDelivery(d.id))!;
      assert.equal(delivered.status, 'succeeded');
      assert.equal(delivered.lastError, null);
      assert.equal(delivered.completedAt, 1_070);
      assert.deepEqual(await store.listDeliveryAttempts(d.id), [failure, success]);
    },
  ],
  [
    'fails a delivery without an attempt, keeping the reason as lastError',
    async (store) => {
      const m = message();
      const d = delivery(m, 'ep_a');
      await store.createDeliveries(m, [d]);
      await store.claimDeliveries({ owner: 'w1', now: 1_000, leaseMs: 100, limit: 10 });

      await store.recordDeliveryAttempt(d.id, 'w1', {
        status: 'failed',
        attempts: 0,
        nextAttemptAt: null,
        failureReason: 'endpoint-deleted',
        completedAt: 1_000,
        error: 'Endpoint ep_a was deleted',
      });

      const failed = (await store.getDelivery(d.id))!;
      assert.equal(failed.status, 'failed');
      assert.equal(failed.failureReason, 'endpoint-deleted');
      assert.equal(failed.lastError, 'Endpoint ep_a was deleted');
      assert.deepEqual(await store.listDeliveryAttempts(d.id), []);
    },
  ],
  [
    "releases only the owner's leases, postponing them when asked",
    async (store) => {
      const m = message();
      const [a, b, c] = [delivery(m, 'ep_a'), delivery(m, 'ep_b'), delivery(m, 'ep_c')];
      await store.createDeliveries(m, [a, b, c]);
      await store.claimDeliveries({ owner: 'w1', now: 2_000, leaseMs: 100, limit: 2 });

      assert.equal(await store.releaseDeliveries([a.id, b.id, c.id], 'w2'), 0);
      assert.equal(await store.releaseDeliveries([a.id], 'w1'), 1);
      assert.equal(await store.releaseDeliveries([b.id], 'w1', 5_000), 1);
      assert.equal((await store.getDelivery(b.id))!.nextAttemptAt, 5_000);
      assert.equal((await store.getDelivery(a.id))!.nextAttemptAt, a.nextAttemptAt);

      const next = await store.claimDeliveries({ owner: 'w3', now: 2_001, leaseMs: 100, limit: 10 });
      assert.deepEqual(next.map((x) => x.delivery.id).sort(), [a.id, c.id].sort());
    },
  ],
  [
    'lists deliveries newest first, by tenant, endpoint, message, status and type',
    async (store) => {
      const m1 = message({ createdAt: 1 });
      const m2 = message({ createdAt: 2, type: 'order.cancelled' });
      const m3 = message({ createdAt: 3, tenant: 'shop-2' });
      const d1 = delivery(m1, 'ep_a');
      const d2 = delivery(m2, 'ep_a', { status: 'failed', nextAttemptAt: null, completedAt: 5, failureReason: 'exhausted' });
      const d3 = delivery(m3, 'ep_b');
      await store.createDeliveries(m1, [d1]);
      await store.createDeliveries(m2, [d2]);
      await store.createDeliveries(m3, [d3]);

      assert.deepEqual(ids(await store.listDeliveries({})), [d3.id, d2.id, d1.id]);
      assert.deepEqual(ids(await store.listDeliveries({ tenant: 'shop-1' })), [d2.id, d1.id]);
      assert.deepEqual(ids(await store.listDeliveries({ tenant: null })), []);
      assert.deepEqual(ids(await store.listDeliveries({ endpointId: 'ep_a', status: 'failed' })), [d2.id]);
      assert.deepEqual(ids(await store.listDeliveries({ type: 'order.shipped' })), [d3.id, d1.id]);
      assert.deepEqual(ids(await store.listDeliveries({ messageId: m1.id })), [d1.id]);
      assert.deepEqual(ids(await store.listDeliveries({ limit: 1, offset: 1 })), [d2.id]);
    },
  ],
  [
    'filters deliveries by failure reason and last status code before pagination, including nulls',
    async (store) => {
      const messages = [1, 2, 3, 4, 5].map((createdAt) => message({ createdAt }));
      const deliveries = [
        delivery(messages[0]!, 'ep_a', { status: 'succeeded', nextAttemptAt: null, completedAt: 1, lastStatusCode: 200 }),
        delivery(messages[1]!, 'ep_a', { status: 'failed', nextAttemptAt: null, completedAt: 2, failureReason: 'exhausted', lastStatusCode: 500 }),
        delivery(messages[2]!, 'ep_a', { status: 'failed', nextAttemptAt: null, completedAt: 3, failureReason: 'exhausted' }),
        delivery(messages[3]!, 'ep_b', { status: 'failed', nextAttemptAt: null, completedAt: 4, failureReason: 'rejected', lastStatusCode: 500 }),
        delivery(messages[4]!, 'ep_a'),
      ];
      const [succeeded, httpFailure, noResponse, rejected, pending] = deliveries;
      for (const [index, m] of messages.entries()) {
        await store.createDeliveries(m, [deliveries[index]!]);
      }

      assert.deepEqual(ids(await store.listDeliveries({ failureReason: 'exhausted' })), [noResponse.id, httpFailure.id]);
      assert.deepEqual(ids(await store.listDeliveries({ failureReason: null })), [pending.id, succeeded.id]);
      assert.deepEqual(ids(await store.listDeliveries({ lastStatusCode: 500 })), [rejected.id, httpFailure.id]);
      assert.deepEqual(ids(await store.listDeliveries({ lastStatusCode: null })), [pending.id, noResponse.id]);
      assert.deepEqual(ids(await store.listDeliveries({ status: 'failed', failureReason: 'exhausted', lastStatusCode: null, endpointId: 'ep_a' })), [noResponse.id]);
      assert.deepEqual(ids(await store.listDeliveries({ tenant: 'shop-1', type: 'order.shipped', failureReason: 'exhausted', lastStatusCode: 500 })), [httpFailure.id]);
      assert.deepEqual(ids(await store.listDeliveries({ failureReason: 'exhausted', limit: 1, offset: 1 })), [httpFailure.id]);
      assert.deepEqual(ids(await store.listDeliveries({ lastStatusCode: null, limit: 1, offset: 1 })), [noResponse.id]);
    },
  ],
  [
    'retries matching deliveries in a new round, keeping the log, never one under a lease',
    async (store) => {
      const m = message();
      const failed = delivery(m, 'ep_a', { status: 'failed', attempts: 10, nextAttemptAt: null, completedAt: 900, failureReason: 'exhausted' });
      const succeeded = delivery(m, 'ep_b', { status: 'succeeded', attempts: 1, nextAttemptAt: null, completedAt: 900 });
      const leased = delivery(m, 'ep_c');
      const otherEndpoint = delivery(m, 'ep_d', { status: 'failed', nextAttemptAt: null, completedAt: 900, failureReason: 'rejected' });
      await store.createDeliveries(m, [failed, succeeded, leased, otherEndpoint]);
      await store.claimDeliveries({ owner: 'w1', now: 1_000, leaseMs: 1_000, limit: 10 });

      await assert.rejects(async () => store.retryDeliveries({}, 1_500), /all: true/);
      assert.equal(await store.retryDeliveries({ ids: [] }, 1_500), 0);

      assert.equal(await store.retryDeliveries({ endpointId: 'ep_a', status: 'failed' }, 1_500), 1);
      assert.deepEqual(await store.getDelivery(failed.id), {
        ...failed,
        status: 'pending',
        attempts: 0,
        nextAttemptAt: 1_500,
        failureReason: null,
        completedAt: null,
      });

      assert.equal(await store.retryDeliveries({ ids: [succeeded.id, leased.id] }, 1_500), 1, 'the leased one stays');
      assert.equal((await store.getDelivery(succeeded.id))!.status, 'pending');

      assert.equal(await store.retryDeliveries({ tenant: 'shop-2' }, 1_500), 0);
      assert.equal(await store.retryDeliveries({ since: 1_001 }, 1_500), 0);
      assert.equal(await store.retryDeliveries({ all: true }, 1_500), 3);
    },
  ],
  [
    'counts pending, due, leased and failed deliveries, and the most overdue',
    async (store) => {
      const m = message();
      await store.createDeliveries(m, [
        delivery(m, 'ep_a', { nextAttemptAt: 100 }),
        delivery(m, 'ep_b', { nextAttemptAt: 300 }),
        delivery(m, 'ep_c', { nextAttemptAt: 9_000 }),
        delivery(m, 'ep_d', { status: 'failed', nextAttemptAt: null, completedAt: 5, failureReason: 'exhausted' }),
        delivery(m, 'ep_e', { status: 'succeeded', nextAttemptAt: null, completedAt: 5 }),
      ]);
      await store.claimDeliveries({ owner: 'w1', now: 200, leaseMs: 1_000, limit: 1 });

      assert.deepEqual(await store.deliveryStats(500), { pending: 3, due: 1, leased: 1, failed: 1, oldestDueAt: 100 });
      assert.deepEqual(await store.deliveryStats(50), { pending: 3, due: 0, leased: 1, failed: 1, oldestDueAt: null });
    },
  ],
  [
    'prunes finished deliveries, their logs and orphaned messages, never pending ones',
    async (store) => {
      const old = message();
      const recent = message();
      const oldDone = delivery(old, 'ep_a', { status: 'succeeded', nextAttemptAt: null, completedAt: 100 });
      const oldFailed = delivery(old, 'ep_b', { status: 'failed', nextAttemptAt: null, completedAt: 100, failureReason: 'rejected' });
      const recentPending = delivery(recent, 'ep_a', { nextAttemptAt: 0 });
      const recentDone = delivery(recent, 'ep_b', { status: 'succeeded', nextAttemptAt: null, completedAt: 100 });
      await store.createDeliveries(old, [oldDone, oldFailed]);
      await store.createDeliveries(recent, [recentPending, recentDone]);
      await store.claimDeliveries({ owner: 'w1', now: 1, leaseMs: 10, limit: 1 });
      await store.recordDeliveryAttempt(recentPending.id, 'w1', {
        status: 'pending',
        attempts: 1,
        nextAttemptAt: 50,
        failureReason: null,
        completedAt: null,
        attempt: attempt(recentPending, 1, 1),
      });

      assert.equal(await store.pruneDeliveries(100), 0, 'completedAt < before, strictly');
      assert.equal(await store.pruneDeliveries(101), 3);
      assert.equal(await store.getMessage(old.id), undefined);
      assert.deepEqual(await store.getMessage(recent.id), recent);
      assert.deepEqual(ids(await store.listDeliveries({})), [recentPending.id]);
      assert.equal((await store.listDeliveryAttempts(recentPending.id)).length, 1);
      assert.deepEqual(await store.listDeliveryAttempts(oldDone.id), []);

      // The pair is free again only because the delivery is gone.
      assert.equal(await store.createDeliveries(old, [delivery(old, 'ep_a')]), 1);
    },
  ],
];

const DELIVERY_CONCURRENT: Case<WebhookDeliveryStore>[] = [
  [
    'workers claiming side by side never get the same delivery, and each holds the lease of all it got',
    async (store) => {
      await openConnections(() => store.getDelivery('dlv_missing'));

      // A claim that doesn't lock the rows it picks hands the same most overdue ones to every
      // worker that picks at the same time: every round races RACERS workers for 40 deliveries.
      for (let round = 1; round <= 3; round++) {
        const m = message();
        const all = Array.from({ length: 40 }, (_, i) => delivery(m, `ep_${i}`));
        await store.createDeliveries(m, all);

        const owners = Array.from({ length: RACERS }, (_, i) => `w${round}_${i}`);
        const batches = await Promise.all(owners.map((owner) => store.claimDeliveries({ owner, now: 5_000, leaseMs: 1_000, limit: 10 })));

        const claimed = batches.flatMap((batch) => batch.map((c) => c.delivery.id));
        assert.deepEqual(repeated(claimed), [], `a delivery was claimed twice in round ${round}`);
        assert.deepEqual([...claimed].sort(), ids(all).sort(), `round ${round} claimed ${claimed.length} of 40`);

        // A lease another worker's claim overwrote fails its owner's write.
        const recorded = await Promise.all(
          batches.flatMap((batch, i) =>
            batch.map(({ delivery: d }) =>
              store.recordDeliveryAttempt(d.id, owners[i]!, {
                status: 'succeeded',
                attempts: 1,
                nextAttemptAt: null,
                failureReason: null,
                completedAt: 5_001,
                attempt: attempt(d, 1, 5_000, 200),
              }),
            ),
          ),
        );
        assert.equal(recorded.filter((ok) => !ok).length, 0, `round ${round}: a worker lost the lease of a delivery it had just claimed`);
      }
    },
  ],
  [
    'fan-outs of one message racing create each delivery once',
    async (store) => {
      await openConnections(() => store.getDelivery('dlv_missing'));
      const m = message();
      const endpoints = ['ep_a', 'ep_b', 'ep_c'];
      const counts = await Promise.all(
        Array.from({ length: 4 }, () => store.createDeliveries(m, endpoints.map((e) => delivery(m, e)))),
      );
      assert.equal(counts.reduce((a, b) => a + b, 0), 3);
      assert.equal((await store.listDeliveries({ messageId: m.id })).length, 3);
    },
  ],
  [
    "a stale worker's write loses to the worker that took the delivery over",
    async (store) => {
      const m = message();
      const d = delivery(m, 'ep_a');
      await store.createDeliveries(m, [d]);
      await store.claimDeliveries({ owner: 'stale', now: 1_000, leaseMs: 100, limit: 1 });
      await store.claimDeliveries({ owner: 'fresh', now: 1_200, leaseMs: 100, limit: 1 });
      await openConnections(() => store.getDelivery(d.id));

      const update = (n: number) => ({
        status: 'pending' as const,
        attempts: n,
        nextAttemptAt: 9_000,
        failureReason: null,
        completedAt: null,
        attempt: attempt(d, n, 1_200),
      });

      const [stale, fresh] = await Promise.all([
        store.recordDeliveryAttempt(d.id, 'stale', update(7)),
        store.recordDeliveryAttempt(d.id, 'fresh', update(1)),
      ]);
      assert.deepEqual([stale, fresh], [false, true]);
      assert.equal((await store.getDelivery(d.id))!.attempts, 1);
      assert.equal((await store.listDeliveryAttempts(d.id)).length, 1);
    },
  ],
  [
    "a manual retry racing a claim never clears the claim's lease",
    async (store) => {
      await openConnections(() => store.getDelivery('dlv_missing'));
      for (let round = 0; round < 10; round++) {
        const m = message();
        const d = delivery(m, 'ep_a', { status: 'failed', nextAttemptAt: null, completedAt: 1, failureReason: 'exhausted' });
        await store.createDeliveries(m, [d]);

        // A pending one to claim, while the failed one is retried.
        const p = delivery(m, 'ep_b');
        await store.createDeliveries(m, [p]);

        const [claimed, retried] = await Promise.all([
          store.claimDeliveries({ owner: `w${round}`, now: 5_000, leaseMs: 1_000, limit: 10 }),
          store.retryDeliveries({ ids: [p.id, d.id] }, 5_000),
        ]);

        for (const { delivery: mine } of claimed) {
          const recorded = await store.recordDeliveryAttempt(mine.id, `w${round}`, {
            status: 'succeeded',
            attempts: 1,
            nextAttemptAt: null,
            failureReason: null,
            completedAt: 5_001,
            attempt: attempt(mine, 1, 5_000, 200),
          });
          assert.equal(recorded, true, `the retry cleared ${mine.id}'s lease (retried ${retried})`);
        }
      }
    },
  ],
];
