import { lookup as dnsLookup } from 'node:dns/promises';
import { request as httpRequest, type IncomingMessage } from 'node:http';
import { request as httpsRequest, type RequestOptions as HttpsRequestOptions } from 'node:https';
import { isIP, type LookupFunction } from 'node:net';
import { StringDecoder } from 'node:string_decoder';
import { createSecureContext, getCACertificates, rootCertificates, type SecureContext } from 'node:tls';
import { WebhookDestinationBlockedError } from '../errors/webhook-destination-blocked.error.js';
import { WebhookTransport } from '../transports/webhook.transport.js';
import type { WebhookRequest, WebhookTransportResponse, WebhookTransportSendOptions } from '../interfaces/webhook-transport.interface.js';
import { AddressPolicy } from './address.policy.js';
import { checkDestination, isLocalhostName, socketHost } from './destination.util.js';
import type { ResolvedAddress, HttpWebhookTransportOptions } from '../interfaces/http-webhook-transport-options.interface.js';

const DEFAULT_MAX_RESPONSE_SIZE = 4_096;

/** Node's default trust store (its bundle, plus `NODE_EXTRA_CA_CERTS`), then the given authorities. */
export function trustedCertificates(ca: NonNullable<HttpWebhookTransportOptions['ca']>): (string | Buffer)[] {
  const defaults: readonly string[] = typeof getCACertificates === 'function' ? getCACertificates('default') : rootCertificates;
  return [...defaults, ...(Array.isArray(ca) ? (ca as readonly (string | Buffer)[]) : [ca as string | Buffer])];
}

/**
 * The default transport: one POST per attempt over `node:http`/`node:https`, guarded
 * against server-side request forgery.
 *
 * - https only (`allowHttp` for development); no credentials in the URL.
 * - The host is resolved here, once, and **every** address it resolves to must be allowed
 *   (see `AddressPolicy`); an IP literal is checked as is.
 * - The socket connects to the address that was checked: the connection's `lookup` returns
 *   it instead of asking DNS again, so a name can't resolve to a public address for the check
 *   and to a private one for the connection (DNS rebinding). TLS still verifies the
 *   certificate against the host name, sent as SNI.
 * - Redirects are never followed: a 3xx is a failed attempt.
 * - At most `maxResponseSize` bytes of the body are read; then the connection is closed.
 * - No connection reuse, and no proxy from the environment: every attempt opens its own socket.
 *
 * `fetch()` can't do this without a third-party dependency: pinning the address takes an
 * undici `Agent` with a custom `connect.lookup`, and Node doesn't expose its bundled undici.
 */
export class HttpWebhookTransport extends WebhookTransport {
  private readonly policy: AddressPolicy;
  private readonly allowHttp: boolean;
  private readonly maxResponseSize: number;
  private readonly resolve: (hostname: string) => Promise<readonly ResolvedAddress[]>;
  /** Built once: a context with a custom trust store costs about 20ms to create. */
  private readonly secureContext: SecureContext | undefined;

  constructor(options: HttpWebhookTransportOptions = {}) {
    super();
    this.policy = new AddressPolicy(options);
    this.allowHttp = options.allowHttp === true;
    this.maxResponseSize = options.maxResponseSize ?? DEFAULT_MAX_RESPONSE_SIZE;

    if (!Number.isInteger(this.maxResponseSize) || this.maxResponseSize < 0) {
      throw new TypeError(`WebhooksModule: delivery.maxResponseSize must be a whole number of bytes (got ${options.maxResponseSize})`);
    }

    this.resolve = options.lookup ?? ((hostname) => dnsLookup(hostname, { all: true, order: 'verbatim' }) as Promise<ResolvedAddress[]>);
    this.secureContext = options.ca === undefined ? undefined : createSecureContext({ ca: trustedCertificates(options.ca) });
  }

  async send(request: WebhookRequest, { signal }: WebhookTransportSendOptions): Promise<WebhookTransportResponse> {
    signal.throwIfAborted();
    const checked = checkDestination(request.url, this.policy, this.allowHttp);
    if (!checked.url) {
      throw new WebhookDestinationBlockedError(request.url, checked.reason);
    }

    const url = checked.url;
    const host = socketHost(url);
    const pinned = await this.pin(request.url, host, signal);

    return this.post(url, host, pinned, request, signal);
  }

  /** The one address the socket may connect to, after every address of the host passed the policy. */
  private async pin(raw: string, host: string, signal: AbortSignal): Promise<ResolvedAddress> {
    const literal = isIP(host);
    if (literal) {
      return { address: host, family: literal as 4 | 6 };
    }
    if (isLocalhostName(host)) {
      // checkDestination() allowed loopback; never ask a resolver what localhost is.
      return { address: '127.0.0.1', family: 4 };
    }

    const addresses = await abortable(this.resolve(host), signal);
    if (!Array.isArray(addresses) || addresses.length === 0) {
      throw Object.assign(new Error(`${host} did not resolve to any address`), { code: 'ENOTFOUND' });
    }

    for (const { address } of addresses) {
      const verdict = this.policy.check(address);
      if (!verdict.allowed) {
        throw new WebhookDestinationBlockedError(raw, `${host} resolves to ${address}, ${verdict.reason}`, address);
      }
    }

    const first = addresses[0]!;
    return { address: first.address, family: isIP(first.address) === 6 ? 6 : 4 };
  }

  private post(
    url: URL,
    host: string,
    pinned: ResolvedAddress,
    request: WebhookRequest,
    signal: AbortSignal,
  ): Promise<WebhookTransportResponse> {
    const body = Buffer.from(request.body, 'utf8');

    // The socket's lookup answers with the checked address, whatever it is asked.
    const lookup: LookupFunction = (_hostname, options, callback) => {
      if ((options as { all?: boolean }).all) {
        (callback as unknown as (error: null, addresses: ResolvedAddress[]) => void)(null, [pinned]);
      } else {
        callback(null, pinned.address, pinned.family);
      }
    };

    const options: HttpsRequestOptions & { secureContext?: SecureContext } = {
      method: 'POST',
      protocol: url.protocol,
      hostname: host,
      port: url.port || undefined,
      path: `${url.pathname}${url.search}`,
      headers: { ...request.headers, host: url.host, 'content-length': String(body.length), connection: 'close' },
      lookup,
      agent: false,
      signal,
      maxHeaderSize: 16_384,
    };

    const secure = url.protocol === 'https:';
    if (secure && !isIP(host)) {
      options.servername = host;
    }
    if (secure && this.secureContext) {
      options.secureContext = this.secureContext;
    }

    return new Promise<WebhookTransportResponse>((resolve, reject) => {
      const req = (secure ? httpsRequest : httpRequest)(options, (res: IncomingMessage) => {
        const chunks: Buffer[] = [];
        let size = 0;
        let settled = false;

        const finish = (cut: boolean) => {
          if (settled) {
            return;
          }
          settled = true;
          const bytes = Buffer.concat(chunks);
          resolve({
            statusCode: res.statusCode ?? 0,
            headers: flatten(res.headers),
            // A cut body may end mid-character: its incomplete tail is dropped rather than decoded to U+FFFD.
            body: cut ? new StringDecoder('utf8').write(bytes) : bytes.toString('utf8'),
          });
          // Nothing more is read: close the connection instead of draining a large body.
          res.destroy();
          req.destroy();
        };

        res.on('data', (chunk: Buffer) => {
          const room = this.maxResponseSize - size;
          if (room > 0) {
            chunks.push(chunk.subarray(0, room));
          }
          size += chunk.length;
          if (size >= this.maxResponseSize) {
            finish(true);
          }
        });
        res.on('end', () => finish(false));
        res.on('error', (error) => {
          if (!settled) {
            settled = true;
            reject(signal.aborted ? signal.reason : error);
          }
        });
        res.on('close', () => {
          if (!settled && !res.complete) {
            settled = true;
            reject(signal.aborted ? signal.reason : Object.assign(new Error('The connection closed mid-response'), { code: 'ECONNRESET' }));
          }
        });
        if (this.maxResponseSize === 0) {
          finish(true);
        }

      });
      req.on('error', (error) => reject(signal.aborted ? signal.reason : error));
      req.end(body);
    });
  }
}

function flatten(headers: IncomingMessage['headers']): Record<string, string | undefined> {
  const out: Record<string, string | undefined> = {};
  for (const [name, value] of Object.entries(headers)) {
    out[name] = Array.isArray(value) ? value.join(', ') : value;
  }
  return out;
}

function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) {
    return Promise.reject(signal.reason);
  }

  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener('abort', abort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener('abort', abort);
        reject(error);
      },
    );
  });
}
