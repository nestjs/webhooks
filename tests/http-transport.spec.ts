/**
 * HttpWebhookTransport against a local server, for what ssrf.spec.ts doesn't cover: the
 * request it writes, the response it returns, resolver answers that aren't usable, IPv6,
 * names it never resolves, a connection that breaks, and what a refusal's message shows.
 */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { HttpWebhookTransport, WebhookDestinationBlockedError, type ResolvedAddress, type WebhookRequest } from '../lib/index.js';

const request = (url: string, body = '{"type":"order.shipped","data":{"note":"ü"}}'): WebhookRequest => ({
  url,
  headers: { 'content-type': 'application/json', 'webhook-id': 'msg_1', 'user-agent': 'NestJS-Webhooks/1.0' },
  body,
  endpointId: 'ep_1',
  deliveryId: 'dlv_1',
  messageId: 'msg_1',
  type: 'order.shipped',
});
const send = (transport: HttpWebhookTransport, url: string, signal = AbortSignal.timeout(5_000)) => transport.send(request(url), { signal, attempt: 1 });
const local = (options: ConstructorParameters<typeof HttpWebhookTransport>[0] = {}) =>
  new HttpWebhookTransport({ allowHttp: true, allowPrivateNetworks: true, ...options });

interface Received {
  method: string;
  url: string;
  headers: IncomingMessage['headers'];
  body: Buffer;
}

async function listen(host: string, handler: (req: IncomingMessage, res: ServerResponse) => void) {
  const received: Received[] = [];
  const server: Server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      received.push({ method: req.method!, url: req.url!, headers: req.headers, body: Buffer.concat(chunks) });
      handler(req, res);
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, host, resolve);
  });
  return {
    port: (server.address() as AddressInfo).port,
    received,
    close() {
      server.closeAllConnections();
      return new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

describe('HttpWebhookTransport', () => {
  let server: Awaited<ReturnType<typeof listen>>;
  let handler: (req: IncomingMessage, res: ServerResponse) => void;

  beforeAll(async () => {
    server = await listen('127.0.0.1', (req, res) => handler(req, res));
  });
  afterAll(() => server.close());
  beforeEach(() => {
    server.received.length = 0;
    handler = (_req, res) => res.writeHead(200).end('ok');
  });

  it('POSTs the body as UTF-8 with its byte length, the given headers, and a fresh connection', async () => {
    await send(local(), `http://127.0.0.1:${server.port}/hooks/store?token=abc`);
    const [received] = server.received;
    expect(received).toMatchObject({ method: 'POST', url: '/hooks/store?token=abc' });
    expect(received!.body.toString('utf8')).toBe('{"type":"order.shipped","data":{"note":"ü"}}');
    expect(received!.headers).toMatchObject({
      host: `127.0.0.1:${server.port}`,
      'content-type': 'application/json',
      'content-length': String(Buffer.byteLength('{"type":"order.shipped","data":{"note":"ü"}}')),
      'webhook-id': 'msg_1',
      'user-agent': 'NestJS-Webhooks/1.0',
      connection: 'close',
    });
  });

  it('returns any status with its headers, repeated ones joined, and the body', async () => {
    handler = (_req, res) => {
      res.setHeader('set-cookie', ['a=1', 'b=2']);
      res.setHeader('retry-after', '30');
      res.writeHead(503).end('overloaded');
    };
    const response = await send(local(), `http://127.0.0.1:${server.port}/`);
    expect(response.statusCode).toBe(503);
    expect(response.body).toBe('overloaded');
    expect(response.headers).toMatchObject({ 'set-cookie': 'a=1, b=2', 'retry-after': '30' });
  });

  it('cuts the body at maxResponseSize bytes without splitting a character', async () => {
    handler = (_req, res) => res.writeHead(200).end('ab🙂c');
    expect((await send(local({ maxResponseSize: 3 }), `http://127.0.0.1:${server.port}/`)).body).toBe('ab');
    expect((await send(local({ maxResponseSize: 5 }), `http://127.0.0.1:${server.port}/`)).body).toBe('ab');
    expect((await send(local({ maxResponseSize: 6 }), `http://127.0.0.1:${server.port}/`)).body).toBe('ab🙂');
  });

  it('reads no body with maxResponseSize: 0, and refuses a size that is not a whole number', async () => {
    handler = (_req, res) => res.writeHead(200).end('a body nobody asked for');
    expect(await send(local({ maxResponseSize: 0 }), `http://127.0.0.1:${server.port}/`)).toMatchObject({ statusCode: 200, body: '' });
    expect(() => new HttpWebhookTransport({ maxResponseSize: 1.5 })).toThrow(/maxResponseSize must be a whole number/);
    expect(() => new HttpWebhookTransport({ maxResponseSize: -1 })).toThrow(TypeError);
  });

  it('fails when the connection closes before the response is complete', async () => {
    handler = (_req, res) => {
      res.writeHead(200, { 'content-length': '100' });
      res.write('partial');
      setTimeout(() => res.socket?.destroy(), 10);
    };
    await expect(send(local(), `http://127.0.0.1:${server.port}/`)).rejects.toMatchObject({ code: 'ECONNRESET' });
  });

  it('does nothing when the signal is already aborted', async () => {
    const lookup = vi.fn(async () => [{ address: '93.184.216.34', family: 4 as const }]);
    const controller = new AbortController();
    controller.abort(new Error('shutting down'));
    await expect(send(new HttpWebhookTransport({ lookup }), 'https://hooks.example.com/', controller.signal)).rejects.toThrow('shutting down');
    expect(lookup).not.toHaveBeenCalled();
  });

  it('fails on a name that resolves to nothing, and passes a resolver error on', async () => {
    const empty = new HttpWebhookTransport({ lookup: async () => [] });
    await expect(send(empty, 'https://nowhere.test/')).rejects.toMatchObject({ code: 'ENOTFOUND', message: 'nowhere.test did not resolve to any address' });
    const broken = new HttpWebhookTransport({
      lookup: async () => {
        throw Object.assign(new Error('queryA ESERVFAIL'), { code: 'ESERVFAIL' });
      },
    });
    await expect(send(broken, 'https://broken.test/')).rejects.toMatchObject({ code: 'ESERVFAIL' });
  });

  it('resolves the name without its trailing dot, and connects to the first address it checked', async () => {
    const asked: string[] = [];
    const lookup = async (hostname: string): Promise<ResolvedAddress[]> => {
      asked.push(hostname);
      return [
        { address: '127.0.0.1', family: 4 },
        { address: '127.0.0.2', family: 4 },
      ];
    };
    const response = await send(local({ lookup }), `http://hooks.partner.test.:${server.port}/x`);
    expect(response.statusCode).toBe(200);
    expect(asked).toEqual(['hooks.partner.test']);
    expect(server.received[0]!.headers.host).toBe(`hooks.partner.test.:${server.port}`);
  });

  it('never asks the resolver about localhost names', async () => {
    const lookup = vi.fn(async (): Promise<ResolvedAddress[]> => [{ address: '10.9.9.9', family: 4 }]);
    const transport = local({ lookup });
    expect((await send(transport, `http://localhost:${server.port}/`)).statusCode).toBe(200);
    expect((await send(transport, `http://api.localhost:${server.port}/`)).statusCode).toBe(200);
    expect(lookup).not.toHaveBeenCalled();
    expect(server.received.map((r) => r.headers.host)).toEqual([`localhost:${server.port}`, `api.localhost:${server.port}`]);
  });

  it('allows a private address only when an allowed range lists it, among blocked ones', async () => {
    const lookup = async (hostname: string): Promise<ResolvedAddress[]> =>
      hostname === 'listed.test' ? [{ address: '127.0.0.1', family: 4 }] : [{ address: '127.0.0.1', family: 4 }, { address: '10.0.0.1', family: 4 }];
    const transport = new HttpWebhookTransport({ allowHttp: true, allowedAddresses: ['127.0.0.0/8'], lookup });
    expect((await send(transport, `http://listed.test:${server.port}/`)).statusCode).toBe(200);
    await expect(send(transport, `http://half.test:${server.port}/`)).rejects.toMatchObject({ address: '10.0.0.1', reason: 'half.test resolves to 10.0.0.1, private' });
    expect(server.received).toHaveLength(1);
  });

  it('refuses IPv6 answers by the same rules', async () => {
    const answers: Record<string, ResolvedAddress[]> = {
      'v6-local.test': [{ address: 'fe80::1', family: 6 }],
      'v6-ula.test': [{ address: 'fd00::5', family: 6 }],
      'v6-nat64.test': [{ address: '64:ff9b::a9fe:a9fe', family: 6 }],
      'v6-teredo.test': [{ address: '2001:0:4136:e378::1', family: 6 }],
    };
    const transport = new HttpWebhookTransport({ lookup: async (hostname) => answers[hostname]! });
    const reasons = await Promise.all(Object.keys(answers).map((host) => send(transport, `https://${host}/`).catch((e: WebhookDestinationBlockedError) => e.reason)));
    expect(reasons).toEqual([
      'v6-local.test resolves to fe80::1, link-local',
      'v6-ula.test resolves to fd00::5, unique local (private)',
      'v6-nat64.test resolves to 64:ff9b::a9fe:a9fe, NAT64 of link-local (cloud metadata)',
      'v6-teredo.test resolves to 2001:0:4136:e378::1, reserved (Teredo)',
    ]);
  });

  it('keeps the path and query of a refused URL out of the error message', async () => {
    const error = (await send(new HttpWebhookTransport(), 'https://10.0.0.1/hooks/store?token=s3cr3t').catch((e: unknown) => e)) as WebhookDestinationBlockedError;
    expect(error).toBeInstanceOf(WebhookDestinationBlockedError);
    expect(error.message).toBe('Refused to deliver to https://10.0.0.1/…: 10.0.0.1 is private');
    expect(error.url).toBe('https://10.0.0.1/hooks/store?token=s3cr3t');
    const bare = (await send(new HttpWebhookTransport(), 'https://10.0.0.1/').catch((e: unknown) => e)) as Error;
    expect(bare.message).toBe('Refused to deliver to https://10.0.0.1/: 10.0.0.1 is private');
    const invalid = (await send(new HttpWebhookTransport(), 'not a url').catch((e: unknown) => e)) as Error;
    expect(invalid.message).toBe('Refused to deliver to (an invalid URL): the URL is not valid');
  });
});

describe('HttpWebhookTransport over IPv6', () => {
  it('connects to an IPv6 literal and to an IPv6 answer', async (context) => {
    const server = await listen('::1', (_req, res) => res.writeHead(204).end()).catch(() => undefined);
    if (!server) {
      context.skip();
      return;
    }

    try {
      expect((await send(local(), `http://[::1]:${server.port}/`)).statusCode).toBe(204);
      const lookup = async (): Promise<ResolvedAddress[]> => [{ address: '::1', family: 6 }];
      expect((await send(local({ lookup }), `http://v6.test:${server.port}/`)).statusCode).toBe(204);
      expect(server.received.map((r) => r.headers.host)).toEqual([`[::1]:${server.port}`, `v6.test:${server.port}`]);
      // Without allowPrivateNetworks, the same literal is refused before connecting.
      await expect(send(new HttpWebhookTransport({ allowHttp: true }), `http://[::1]:${server.port}/`)).rejects.toThrow(/loopback/);
      expect(server.received).toHaveLength(2);
    } finally {
      await server.close();
    }
  });
});
