/**
 * Options: the defaults the docs page promises, and the startup errors module.spec.ts doesn't
 * list, each naming its option.
 */
import { Injectable } from '@nestjs/common';
import { OutboxModule } from '@nestjs/outbox';
import { Test } from '@nestjs/testing';
import { WEBHOOKS_CONFIG } from '../lib/webhooks.constants.js';
import type { ResolvedWebhooksConfig } from '../lib/interfaces/resolved-webhooks-config.interface.js';
import { WebhooksModule, WebhookWorker } from '../lib/index.js';

async function compile(webhooks: ReturnType<typeof WebhooksModule.forRoot>) {
  const moduleRef = await Test.createTestingModule({ imports: [OutboxModule.forRoot({ relay: { enabled: false } }), webhooks] }).compile();
  const app = moduleRef.createNestApplication({ logger: false });
  await app.init();
  return app;
}

describe('WebhooksModule defaults', () => {
  it('retries 10 times over about two days, and every other default the options document', async () => {
    const app = await compile(WebhooksModule.forRoot({ worker: { enabled: false } }));
    const config = app.get<ResolvedWebhooksConfig>(WEBHOOKS_CONFIG);
    expect(config).toMatchObject({
      eventTypes: null,
      retry: { attempts: 10, backoff: { delay: 5_000, factor: 4, maxDelay: 86_400_000, jitter: 'equal' } },
      timeoutMs: 15_000,
      userAgent: 'NestJS-Webhooks/1.0',
      maxResponseSize: 4_096,
      worker: { enabled: false, pollInterval: 1_000, batchSize: 50, concurrency: 10, lease: 60_000 },
      disableAfterMs: 5 * 86_400_000,
      rotationOverlapMs: 86_400_000,
      allowHttp: false,
    });
    expect(config.receivers.size).toBe(0);
    await app.close();
  });

  it('gives receivers a 5 minute tolerance, deduplication, and a consumer named after them', async () => {
    const app = await compile(
      WebhooksModule.forRoot({
        outgoing: false,
        receivers: { payments: { scheme: 'standard', secret: `whsec_${Buffer.alloc(32).toString('base64')}` }, gh: { scheme: 'github', secret: 's', consumer: 'github-app' } },
      }),
    );
    const { receivers } = app.get<ResolvedWebhooksConfig>(WEBHOOKS_CONFIG);
    expect(receivers.get('payments')).toMatchObject({ toleranceMs: 300_000, dedupe: true, consumer: 'webhooks:payments' });
    expect(receivers.get('gh')).toMatchObject({ consumer: 'github-app' });
    await app.close();
  });

  it('starts the worker at bootstrap unless disabled, and stops it on close', async () => {
    const app = await compile(WebhooksModule.forRoot({ worker: { pollInterval: '1h' } }));
    expect(app.get(WebhookWorker).running).toBe(true);
    await app.close();
    expect(app.get(WebhookWorker).running).toBe(false);
  });

  it('accepts a duration in milliseconds or with a unit, fractions included', async () => {
    const app = await compile(WebhooksModule.forRoot({ worker: { enabled: false, lease: 90_000, pollInterval: '1.5s' }, delivery: { timeout: '1m' }, secretRotationOverlap: '2w' }));
    expect(app.get<ResolvedWebhooksConfig>(WEBHOOKS_CONFIG)).toMatchObject({
      timeoutMs: 60_000,
      worker: { lease: 90_000, pollInterval: 1_500 },
      rotationOverlapMs: 14 * 86_400_000,
    });
    await app.close();
  });
});

describe('WebhooksModule startup errors', () => {
  it.each<[string, () => ReturnType<typeof WebhooksModule.forRoot>, RegExp]>([
    ['a fractional attempt count', () => WebhooksModule.forRoot({ retry: { attempts: 1.5 } }), /retry.attempts must be a whole number of at least 1 \(got 1.5\)/],
    ['a retryIf that is not a function', () => WebhooksModule.forRoot({ retry: { retryIf: true as never } }), /retry.retryIf must be a function \(got boolean\)/],
    ['a backoff given as a number', () => WebhooksModule.forRoot({ retry: { backoff: 1_000 as never } }), /For a fixed wait, use \{ delay: 1000, factor: 1 \}/],
    ['a null backoff', () => WebhooksModule.forRoot({ retry: { backoff: null as never } }), /retry.backoff must be .* \(got null\)/],
    ['a shrinking backoff', () => WebhooksModule.forRoot({ retry: { backoff: { factor: 0.5 } } }), /retry.backoff.factor must be at least 1 \(got 0.5\)/],
    ['a bad backoff delay', () => WebhooksModule.forRoot({ retry: { backoff: { delay: '-1s' as never } } }), /retry.backoff.delay: Invalid duration/],
    ['a bad backoff cap', () => WebhooksModule.forRoot({ retry: { backoff: { maxDelay: -5 } } }), /retry.backoff.maxDelay: Invalid duration -5/],
    ['a zero poll interval', () => WebhooksModule.forRoot({ worker: { pollInterval: 0 } }), /worker.pollInterval must be longer than 0/],
    ['a zero timeout', () => WebhooksModule.forRoot({ delivery: { timeout: '0s' } }), /delivery.timeout must be longer than 0/],
    ['a lease equal to the timeout', () => WebhooksModule.forRoot({ delivery: { timeout: '1m' }, worker: { lease: '60s' } }), /worker.lease must be longer than delivery.timeout/],
    ['a fractional concurrency', () => WebhooksModule.forRoot({ worker: { concurrency: 1.5 } }), /worker.concurrency must be a whole number of at least 1/],
    ['disabling endpoints after zero', () => WebhooksModule.forRoot({ disableEndpointAfter: 0 }), /disableEndpointAfter must be longer than 0/],
    ['a bad rotation overlap', () => WebhooksModule.forRoot({ secretRotationOverlap: '1 day' as never }), /secretRotationOverlap: Invalid duration "1 day"/],
    ['event types that are not an array', () => WebhooksModule.forRoot({ eventTypes: 'order.shipped' as never }), /eventTypes must be an array/],
    ['an event type with an empty segment', () => WebhooksModule.forRoot({ eventTypes: ['order.'] }), /eventTypes: "order." is not a message type/],
    ['no encryption keys', () => WebhooksModule.forRoot({ encryption: { keys: [] } }), /encryption.keys` must list at least one key/],
    ['an encryption key Buffer of the wrong size', () => WebhooksModule.forRoot({ encryption: { keys: [Buffer.alloc(16)] } }), /encryption.keys\[0\]` is a Buffer of 16 bytes; it must be 32 bytes/],
    ['an encryption key with whitespace', () => WebhooksModule.forRoot({ encryption: { keys: ['k'.repeat(40), ` ${'k'.repeat(40)}`] } }), /encryption.keys\[1\]` starts or ends with whitespace/],
    ['a malformed allowed IPv6 range', () => WebhooksModule.forRoot({ delivery: { allowedAddresses: ['fd12::/129'] } }), /delivery.allowedAddresses\[0\]` must be an IP address or a CIDR range/],
    ['a prefix that is not a number', () => WebhooksModule.forRoot({ delivery: { allowedAddresses: ['10.0.0.0/8', '10.0.0.0/x'] } }), /delivery.allowedAddresses\[1\]/],
    ['a user agent with a line break', () => WebhooksModule.forRoot({ delivery: { userAgent: 'CatStore-Webhooks/2.0\n' } }), /delivery.userAgent/],
    ['a user agent HTTP cannot carry', () => WebhooksModule.forRoot({ delivery: { userAgent: 'Cat Store — Webhooks/2.0' } }), /delivery.userAgent/],
  ])('fails on %s, naming the option', async (_name, module, message) => {
    await expect(Promise.resolve().then(() => compile(module()))).rejects.toThrow(message);
  });

  it.each<[string, Record<string, unknown>, RegExp]>([
    ['receivers that are not an object', 'payments' as never, /receivers must be an object of receiver options by name/],
    ['a receiver that is not an object', { payments: null }, /receivers.payments must be an object/],
    ['a secret that is not a string', { payments: { scheme: 'github', secret: 42 } }, /receivers.payments.secret must be a string \(got number\)/],
    ['a listed secret that is not a string', { payments: { scheme: 'github', secret: ['ok', 42] } }, /receivers.payments.secret\[1\] must be a string/],
    ['a GitHub secret with whitespace', { gh: { scheme: 'github', secret: 'secret ' } }, /receivers.gh.secret: the secret starts or ends with whitespace/],
    ['an empty Stripe secret', { stripe: { scheme: 'stripe', secret: '' } }, /receivers.stripe.secret: the secret is empty/],
    ['an id that is not a function', { gh: { scheme: 'github', secret: 's', id: 'x-request-id' } }, /receivers.gh.id must be a function/],
    ['an empty consumer', { gh: { scheme: 'github', secret: 's', consumer: '' } }, /receivers.gh.consumer must be a non-empty string/],
    [
      'a consumer longer than the inbox keeps',
      { gh: { scheme: 'github', secret: 's', consumer: 'c'.repeat(256) } },
      /^WebhooksModule: receivers.gh.consumer is longer than 255 characters: the outbox's inbox keeps consumer names in 255-character columns on MySQL\. Set a shorter one\.$/,
    ],
    [
      'a receiver name too long for its default consumer',
      { ['r'.repeat(247)]: { scheme: 'github', secret: 's' } },
      /receivers.r{247}.consumer \(webhooks:<receiver name>, the default\) is longer than 255 characters/,
    ],
    ['a zero tolerance', { gh: { scheme: 'stripe', secret: 's', tolerance: 0 } }, /receivers.gh.tolerance must be longer than 0/],
    ['a receiver name with a slash', { 'pay/fast': { scheme: 'github', secret: 's' } }, /receivers.pay\/fast: a receiver name is letters, digits/],
  ])('refuses %s', async (_name, receivers, message) => {
    await expect(Promise.resolve().then(() => compile(WebhooksModule.forRoot({ outgoing: false, receivers: receivers as never })))).rejects.toThrow(message);
  });

  it('refuses a transport that cannot send, from the top level at once and from the factory at startup', async () => {
    expect(() => WebhooksModule.forRoot({ transport: { post() {} } as never })).toThrow(
      'WebhooksModule: `transport` must be a WebhookTransport class or an object with send()',
    );
    await expect(compile(WebhooksModule.forRootAsync({ useFactory: () => ({ transport: { post() {} } as never }) }))).rejects.toThrow(
      /`transport` returned by the forRootAsync\(\) factory must be a WebhookTransport instance/,
    );
  });

  it('refuses a transport class at the top level when the factory returns one too', async () => {
    @Injectable()
    class Top {
      async send() {
        return { statusCode: 200, headers: {}, body: '' };
      }
    }
    await expect(
      compile(WebhooksModule.forRootAsync({ transport: Top as never, useFactory: () => ({ transport: new Top() as never }) })),
    ).rejects.toThrow(/set both at the top level and in the options the factory returns/);
  });
});
