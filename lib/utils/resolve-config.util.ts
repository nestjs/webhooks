import { validateHeaderValue } from 'node:http';
import { optionMs, positiveMs, resolveRetry } from './backoff.util.js';
import { plaintextSecrets, SecretCipher } from './encryption.util.js';
import { exceedsInboxKey, MAX_INBOX_KEY_LENGTH } from './inbox-keys.util.js';
import { AddressPolicy } from '../network/address.policy.js';
import { WebhookSignatureScheme } from '../signing/webhook-signature.scheme.js';
import { GitHubScheme } from '../signing/github.scheme.js';
import { StripeScheme } from '../signing/stripe.scheme.js';
import { StandardWebhooksScheme } from '../signing/standard-webhooks.scheme.js';
import type { WebhooksModuleOptions } from '../interfaces/webhooks-module-options.interface.js';
import type { WebhookReceiverOptions } from '../interfaces/webhook-receiver-options.interface.js';
import { EVENT_TYPE } from '../webhooks.constants.js';
import type { CompiledReceiver, ResolvedWebhooksConfig } from '../interfaces/resolved-webhooks-config.interface.js';

const RECEIVER_NAME = /^[A-Za-z0-9_.-]+$/;
const DEFAULT_TOLERANCE = 5 * 60_000;

/** Every option checked, at startup, with the option's name in the error. */
export function resolveConfig(options: WebhooksModuleOptions = {}): ResolvedWebhooksConfig {
  if (options && ('store' in options || 'stores' in options || 'storage' in options)) {
    throw new Error(
      'WebhooksModule: stores are not options. Implement WebhookEndpointStore and WebhookDeliveryStore in a provider ' +
        'that injects WebhooksStorage and calls `storage.registerSource({ endpoints: this, deliveries: this })` in its constructor.',
    );
  }

  const delivery = options.delivery ?? {};
  const workerOptions = options.worker ?? {};
  const timeoutMs = positiveMs(delivery.timeout ?? '15s', 'delivery.timeout');
  const lease = positiveMs(workerOptions.lease ?? '1m', 'worker.lease');
  if (timeoutMs >= lease) {
    throw new TypeError('WebhooksModule: worker.lease must be longer than delivery.timeout (a claim must outlive an attempt)');
  }

  const eventTypes = options.eventTypes === undefined ? null : new Set(checkEventTypes(options.eventTypes));
  const disable = options.disableEndpointAfter ?? '5d';

  return {
    eventTypes,
    retry: resolveRetry(options.retry),
    timeoutMs,
    userAgent: userAgent(delivery.userAgent ?? 'NestJS-Webhooks/1.0'),
    maxResponseSize: wholeNumber(delivery.maxResponseSize ?? 4_096, 'delivery.maxResponseSize', 0),
    worker: {
      enabled: workerOptions.enabled ?? true,
      pollInterval: positiveMs(workerOptions.pollInterval ?? '1s', 'worker.pollInterval'),
      batchSize: wholeNumber(workerOptions.batchSize ?? 50, 'worker.batchSize'),
      concurrency: wholeNumber(workerOptions.concurrency ?? 10, 'worker.concurrency'),
      lease,
    },
    disableAfterMs: disable === false ? null : positiveMs(disable, 'disableEndpointAfter'),
    rotationOverlapMs: optionMs(options.secretRotationOverlap ?? '24h', 'secretRotationOverlap'),
    secrets: options.encryption ? new SecretCipher(options.encryption) : plaintextSecrets,
    addressPolicy: new AddressPolicy(delivery),
    allowHttp: delivery.allowHttp === true,
    receivers: compileReceivers(options.receivers ?? {}),
  };
}

function checkEventTypes(types: readonly string[]): string[] {
  if (!Array.isArray(types)) {
    throw new TypeError('WebhooksModule: eventTypes must be an array of message types');
  }
  for (const type of types) {
    if (typeof type !== 'string' || !EVENT_TYPE.test(type)) {
      throw new TypeError(`WebhooksModule: eventTypes: ${JSON.stringify(type)} is not a message type such as "order.shipped"`);
    }
  }
  return [...types];
}

function compileReceivers(receivers: Record<string, WebhookReceiverOptions>): Map<string, CompiledReceiver> {
  if (receivers === null || typeof receivers !== 'object') {
    throw new TypeError('WebhooksModule: receivers must be an object of receiver options by name');
  }

  const compiled = new Map<string, CompiledReceiver>();
  for (const [name, options] of Object.entries(receivers)) {
    const option = `receivers.${name}`;
    if (!RECEIVER_NAME.test(name)) {
      throw new TypeError(`WebhooksModule: ${option}: a receiver name is letters, digits, "_", "-" and "."`);
    }
    if (!options || typeof options !== 'object') {
      throw new TypeError(`WebhooksModule: ${option} must be an object`);
    }

    const scheme = toScheme(options, option);
    const secrets: unknown[] = Array.isArray(options.secret) ? [...options.secret] : [options.secret];
    if (secrets.length === 0) {
      throw new TypeError(`WebhooksModule: ${option}.secret lists no secret`);
    }

    const keys = secrets.map((secret, index) => {
      const where = Array.isArray(options.secret) ? `${option}.secret[${index}]` : `${option}.secret`;
      if (typeof secret !== 'string') {
        throw new TypeError(`WebhooksModule: ${where} must be a string (got ${typeof secret})`);
      }
      try {
        return scheme.key(secret);
      } catch (error) {
        throw new TypeError(`WebhooksModule: ${where}: ${(error as Error).message}`);
      }
    });

    const tolerance = 'tolerance' in options ? options.tolerance : undefined;
    const id = 'id' in options ? options.id : undefined;
    if (id !== undefined && typeof id !== 'function') {
      throw new TypeError(`WebhooksModule: ${option}.id must be a function`);
    }

    const consumer = options.consumer ?? `webhooks:${name}`;
    if (typeof consumer !== 'string' || consumer === '') {
      throw new TypeError(`WebhooksModule: ${option}.consumer must be a non-empty string`);
    }
    if (exceedsInboxKey(consumer)) {
      throw new TypeError(
        `WebhooksModule: ${option}.consumer${options.consumer === undefined ? ' (webhooks:<receiver name>, the default)' : ''} is longer than ` +
          `${MAX_INBOX_KEY_LENGTH} characters: the outbox's inbox keeps consumer names in ${MAX_INBOX_KEY_LENGTH}-character columns on MySQL. Set a shorter one.`,
      );
    }

    compiled.set(name, {
      name,
      scheme,
      keys,
      toleranceMs: positiveMs(tolerance ?? DEFAULT_TOLERANCE, `${option}.tolerance`),
      dedupe: options.dedupe ?? true,
      consumer,
      id: id as CompiledReceiver['id'],
    });
  }

  return compiled;
}

function toScheme(options: WebhookReceiverOptions, option: string): WebhookSignatureScheme {
  const { scheme } = options;
  if (scheme === 'standard') {
    return new StandardWebhooksScheme();
  }
  if (scheme === 'stripe') {
    const header = 'header' in options && options.header !== undefined ? options.header : 'stripe-signature';
    if (typeof header !== 'string' || !/^[A-Za-z0-9-]+$/.test(header)) {
      throw new TypeError(`WebhooksModule: ${option}.header must be a header name`);
    }
    return new StripeScheme(header.toLowerCase());
  }
  if (scheme === 'github') {
    return new GitHubScheme();
  }
  if (scheme instanceof WebhookSignatureScheme) {
    return scheme;
  }
  if (typeof scheme === 'function') {
    throw new TypeError(`WebhooksModule: ${option}.scheme is a class; pass an instance`);
  }
  throw new TypeError(
    `WebhooksModule: ${option}.scheme must be "standard", "stripe", "github" or a WebhookSignatureScheme instance (got ${JSON.stringify(scheme)})`,
  );
}

/** A user agent HTTP can carry: one that can't fails every attempt of the HTTP transport instead. */
function userAgent(value: string): string {
  try {
    validateHeaderValue('user-agent', value);
  } catch (error) {
    throw new TypeError(`WebhooksModule: delivery.userAgent: ${(error as Error).message}`);
  }
  return value;
}

function wholeNumber(value: number, option: string, min = 1): number {
  if (!Number.isInteger(value) || value < min) {
    throw new TypeError(`WebhooksModule: ${option} must be a whole number of at least ${min} (got ${value})`);
  }
  return value;
}
