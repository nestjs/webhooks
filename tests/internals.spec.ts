/**
 * Internal units that hold real logic: the backoff math and Retry-After parsing, durations,
 * error descriptions and URL redaction, id generation, address parsing corners, and the
 * signature schemes' header handling beyond the published vectors in signing.spec.ts.
 */
import { createHmac } from 'node:crypto';
import { computeBackoff, parseRetryAfter, resolveRetry } from '../lib/utils/backoff.util.js';
import { toMs } from '../lib/utils/duration.util.js';
import { describeError, redactUrl } from '../lib/utils/describe-error.util.js';
import { prefixedId, uuidv7 } from '../lib/utils/uuid.util.js';
import { AddressPolicy, parseIPv6 } from '../lib/network/address.policy.js';
import { GitHubScheme } from '../lib/signing/github.scheme.js';
import { StripeScheme } from '../lib/signing/stripe.scheme.js';
import { signStandard, StandardWebhooksScheme } from '../lib/signing/standard-webhooks.scheme.js';
import { generateSecret, standardSecretKey } from '../lib/signing/secrets.util.js';
import { WebhookSignatureScheme, type WebhookDelivery } from '../lib/index.js';

const delivery = {} as WebhookDelivery;

describe('computeBackoff', () => {
  const exponential = { delay: 1_000, factor: 3, maxDelay: 20_000, jitter: 'none' as const };

  it('grows by the factor from the delay, up to the cap', () => {
    expect([1, 2, 3, 4, 5].map((attempt) => computeBackoff(exponential, attempt, null, delivery))).toEqual([1_000, 3_000, 9_000, 20_000, 20_000]);
  });

  it('spreads full jitter over [0, d] and equal jitter over [d/2, d]', () => {
    const at = (jitter: 'full' | 'equal', random: number) => computeBackoff({ ...exponential, jitter }, 2, null, delivery, () => random);
    expect([at('full', 0), at('full', 0.5), at('full', 0.999_999)]).toEqual([0, 1_500, 2_999]);
    expect([at('equal', 0), at('equal', 0.5), at('equal', 0.999_999)]).toEqual([1_500, 2_250, 2_999]);
  });

  it('floors what a function returns, and reads its durations', () => {
    expect(computeBackoff(() => 1_234.9, 1, null, delivery)).toBe(1_234);
    expect(computeBackoff((attempt) => `${attempt}m`, 3, null, delivery)).toBe(180_000);
    expect(() => computeBackoff(() => 'later' as never, 1, null, delivery)).toThrow(/Invalid duration "later"/);
  });

  it('adds up to about two days by default, nine waits for ten attempts', () => {
    const { attempts, backoff } = resolveRetry(undefined);
    const ceilings = Array.from({ length: attempts - 1 }, (_, i) => computeBackoff(backoff, i + 1, null, delivery, () => 1));
    expect(ceilings).toEqual([5_000, 20_000, 80_000, 320_000, 1_280_000, 5_120_000, 20_480_000, 81_920_000, 86_400_000]);
    const hours = ceilings.reduce((sum, ms) => sum + ms, 0) / 3_600_000;
    expect(hours).toBeGreaterThan(53);
    expect(hours).toBeLessThan(55);
  });

  it('resolves the shorthands', () => {
    expect(resolveRetry(false).attempts).toBe(1);
    expect(resolveRetry(3).attempts).toBe(3);
    expect(resolveRetry({ backoff: { delay: '2s' } }).backoff).toEqual({ delay: 2_000, factor: 4, maxDelay: 86_400_000, jitter: 'equal' });
  });
});

describe('parseRetryAfter', () => {
  const now = Date.parse('2026-09-24T12:00:00Z');

  it.each<[string | undefined, number | undefined]>([
    [undefined, undefined],
    ['', undefined],
    ['0', 0],
    ['120', 120_000],
    [' 7 ', 7_000],
    ['Thu, 24 Sep 2026 12:01:30 GMT', 90_000],
    ['Thu, 24 Sep 2026 11:00:00 GMT', 0],
    ['tomorrow', undefined],
    ['Thursday, 24-Sep-26 12:01:30 GMT', 90_000],
    ['Thu Sep 24 12:01:30 2026', 90_000],
    ['Thu Sep  4 12:00:00 2026', 0],
    // Malformed: no HTTP-date form, although Date.parse() reads each of them as some date.
    ['1.5', undefined],
    ['-3', undefined],
    ['2026-09-24T12:01:30Z', undefined],
    ['Thu, 24 Sep 2026 12:01:30', undefined],
    ['24 Sep 2026', undefined],
  ])('reads %j as %j ms', (value, expected) => {
    expect(parseRetryAfter(value, now)).toBe(expected);
  });
});

describe('toMs', () => {
  it.each<[string | number, number]>([
    [0, 0],
    [250, 250],
    ['250ms', 250],
    ['1.5s', 1_500],
    ['2m', 120_000],
    ['3h', 10_800_000],
    ['1d', 86_400_000],
    ['1w', 604_800_000],
  ])('reads %j', (value, ms) => {
    expect(toMs(value as never)).toBe(ms);
  });

  it.each(['', '5', '5 s', '5sec', '-5s', '.5s', '1e3ms'])('refuses %j', (value) => {
    expect(() => toMs(value as never)).toThrow(TypeError);
  });

  it.each([-1, Number.NaN, Number.POSITIVE_INFINITY])('refuses the number %d', (value) => {
    expect(() => toMs(value)).toThrow(/non-negative number of milliseconds/);
  });
});

describe('describeError and redactUrl', () => {
  it('names the error, adds a code the message lacks, and never throws', () => {
    expect(describeError(new TypeError('bad'))).toBe('TypeError: bad');
    expect(describeError(Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }))).toBe('Error: socket hang up (ECONNRESET)');
    expect(describeError(Object.assign(new Error('connect ECONNREFUSED 10.0.0.1:443'), { code: 'ECONNREFUSED' }))).toBe('Error: connect ECONNREFUSED 10.0.0.1:443');
    expect(describeError('plain')).toBe('plain');
    expect(describeError({ status: 1 })).toBe('{"status":1}');
    expect(describeError(undefined)).toBe('undefined');
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(describeError(cyclic)).toBe('[object Object]');
    expect(describeError(new Error('x'.repeat(2_000)))).toHaveLength(1_001);
  });

  it('describes the errors of an AggregateError without a message (a connection that tried several addresses)', () => {
    const refused = (address: string) => Object.assign(new Error(`connect ECONNREFUSED ${address}`), { code: 'ECONNREFUSED' });
    // What node:net rejects with when every address of a host refuses the connection.
    const error = Object.assign(new AggregateError([refused('10.0.0.1:443'), refused('2001:db8::1:443')]), { code: 'ECONNREFUSED' });
    expect(describeError(error)).toBe('AggregateError: connect ECONNREFUSED 10.0.0.1:443; connect ECONNREFUSED 2001:db8::1:443');
    expect(describeError(new AggregateError([new Error('a')], 'all failed'))).toBe('AggregateError: all failed');
  });

  it('keeps the scheme, host and port of a URL, and nothing after', () => {
    expect(redactUrl('https://hooks.example.com:8443/t/abc?k=1')).toBe('https://hooks.example.com:8443/…');
    expect(redactUrl('https://hooks.example.com/?k=1')).toBe('https://hooks.example.com/…');
    expect(redactUrl('https://hooks.example.com')).toBe('https://hooks.example.com/');
    expect(redactUrl('::')).toBe('(an invalid URL)');
  });
});

describe('ids', () => {
  it('generates UUIDv7s in strictly increasing order, even many in one millisecond and with the clock stepping back', () => {
    const now = Date.now();
    const spy = vi.spyOn(Date, 'now');
    spy.mockReturnValue(now + 10_000);
    const ids = Array.from({ length: 5_000 }, () => uuidv7());
    spy.mockReturnValue(now + 5_000);
    ids.push(uuidv7(), uuidv7());
    spy.mockRestore();

    for (let i = 1; i < ids.length; i++) {
      expect(ids[i]! > ids[i - 1]!).toBe(true);
    }
    for (const id of ids.slice(0, 3)) {
      expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    }
    expect(parseInt(ids[0]!.replaceAll('-', '').slice(0, 12), 16)).toBe(now + 10_000);
  });

  it('prefixes an id without dashes or dots', () => {
    expect(prefixedId('dlv')).toMatch(/^dlv_[0-9a-f]{32}$/);
  });
});

describe('address parsing corners', () => {
  const policy = new AddressPolicy();

  it('refuses what is not an IP address, including a scoped IPv6 address', () => {
    expect(policy.check('example.com')).toEqual({ allowed: false, reason: '"example.com" is not an IP address' });
    expect(policy.check('fe80::1%eth0')).toMatchObject({ allowed: false, reason: expect.stringContaining('not an IP address') });
    expect(policy.check('1.2.3')).toMatchObject({ allowed: false });
  });

  it('parses bracketed, compressed and dotted IPv6 forms to the same bytes', () => {
    const expected = parseIPv6('0:0:0:0:0:ffff:7f00:1');
    expect(parseIPv6('[::ffff:127.0.0.1]')).toEqual(expected);
    expect(parseIPv6('::ffff:7f00:1')).toEqual(expected);
    expect(parseIPv6('::FFFF:127.0.0.1')).toEqual(expected);
    expect(parseIPv6('1::2::3')).toBeNull();
  });

  it('treats a bare address as a single-address range, and /0 as everything', () => {
    const single = new AddressPolicy({ allowedAddresses: ['10.0.0.7', '::1'] });
    expect(single.check('10.0.0.7')).toEqual({ allowed: true });
    expect(single.check('10.0.0.8').allowed).toBe(false);
    expect(single.check('::1')).toEqual({ allowed: true });
    const everything = new AddressPolicy({ allowedAddresses: ['0.0.0.0/0'] });
    expect(everything.check('169.254.169.254')).toEqual({ allowed: true });
    // An IPv4 range never matches an IPv6 address, even one carrying that IPv4 address.
    expect(everything.check('::ffff:169.254.169.254').allowed).toBe(false);
  });

  it('matches a range on its prefix bits, not whole bytes', () => {
    const odd = new AddressPolicy({ allowedAddresses: ['10.0.0.0/9'] });
    expect(odd.check('10.127.255.255')).toEqual({ allowed: true });
    expect(odd.check('10.128.0.0').allowed).toBe(false);
    expect(policy.check('172.15.255.255')).toEqual({ allowed: true });
    expect(policy.check('172.32.0.0')).toEqual({ allowed: true });
    expect(policy.check('100.63.255.255')).toEqual({ allowed: true });
    expect(policy.check('100.128.0.0')).toEqual({ allowed: true });
  });
});

describe('signature schemes: header handling', () => {
  const key = standardSecretKey(`whsec_${Buffer.alloc(32, 1).toString('base64')}`);
  const now = Math.floor(Date.now() / 1000);

  it('Standard Webhooks: one-element header arrays are fine; an overlong id is malformed; unknown versions are skipped', () => {
    const scheme = new StandardWebhooksScheme();
    const signature = `v1,${signStandard(key, 'msg_1', now, '{}')}`;
    const verify = (headers: Record<string, string | string[]>) => scheme.verify({ headers, rawBody: Buffer.from('{}') }, [key]);
    expect(verify({ 'webhook-id': ['msg_1'], 'webhook-timestamp': [String(now)], 'webhook-signature': [signature] })).toMatchObject({ valid: true, id: 'msg_1' });
    expect(verify({ 'webhook-id': 'm'.repeat(256), 'webhook-timestamp': String(now), 'webhook-signature': signature })).toMatchObject({
      reason: 'malformed-header',
      detail: 'webhook-id is empty or longer than 255 characters',
    });
    const longest = 'm'.repeat(255);
    const signed = `v1,${signStandard(key, longest, now, '{}')}`;
    expect(verify({ 'webhook-id': longest, 'webhook-timestamp': String(now), 'webhook-signature': signed })).toMatchObject({ valid: true, id: longest });
    expect(verify({ 'webhook-id': '', 'webhook-timestamp': String(now), 'webhook-signature': signature })).toMatchObject({ reason: 'malformed-header' });
    expect(verify({ 'webhook-id': 'msg_1', 'webhook-timestamp': String(now), 'webhook-signature': `v1a,xyz ${signature}` })).toMatchObject({ valid: true });
    expect(verify({ 'webhook-id': 'msg_1', 'webhook-timestamp': '1234567890123', 'webhook-signature': signature })).toMatchObject({ reason: 'malformed-header' });
  });

  it('Stripe: a repeated header is malformed, spaces are trimmed, and non-hex v1 entries are ignored', () => {
    const scheme = new StripeScheme();
    const body = '{"id":"evt_1"}';
    const hex = createHmac('sha256', 'sk').update(`${now}.${body}`).digest('hex');
    const verify = (value: string | string[]) => scheme.verify({ headers: { 'stripe-signature': value }, rawBody: Buffer.from(body) }, [Buffer.from('sk')]);
    expect(verify(`t=${now}, v1=${hex}`)).toEqual({ valid: true, timestamp: now });
    expect(verify(`t=${now},v1=${hex.toUpperCase()}`)).toMatchObject({ valid: true });
    expect(verify([`t=${now},v1=${hex}`, `t=${now},v1=${hex}`])).toMatchObject({ reason: 'malformed-header' });
    expect(verify(`t=${now},v1=not-hex`)).toMatchObject({ reason: 'invalid-signature', detail: 'no v1 signature in stripe-signature' });
    expect(verify(`t=soon,v1=${hex}`)).toMatchObject({ reason: 'malformed-header' });
    expect(verify(`garbage,t=${now},v1=${hex}`)).toMatchObject({ valid: true });
    expect(scheme.idFromPayload({ id: 42 })).toBeUndefined();
    expect(scheme.idFromPayload(null)).toBeUndefined();
  });

  it('GitHub: tries every key, refuses a repeated header, and leaves an empty delivery id out', () => {
    const scheme = new GitHubScheme();
    const body = Buffer.from('{"zen":"x"}');
    const signature = `sha256=${createHmac('sha256', 'new').update(body).digest('hex')}`;
    const verify = (headers: Record<string, string | string[]>) => scheme.verify({ headers, rawBody: body }, [Buffer.from('old'), Buffer.from('new')]);
    expect(verify({ 'x-hub-signature-256': signature, 'x-github-delivery': 'd-1' })).toEqual({ valid: true, id: 'd-1' });
    expect(verify({ 'x-hub-signature-256': signature, 'x-github-delivery': '' })).toEqual({ valid: true, id: undefined });
    expect(verify({ 'x-hub-signature-256': [signature, signature] })).toMatchObject({ reason: 'malformed-header' });
    expect(verify({ 'x-hub-signature-256': `sha256=${'0'.repeat(64)}` })).toMatchObject({ reason: 'invalid-signature' });
    expect(verify({})).toMatchObject({ reason: 'missing-header' });
  });

  it('matches() compares in constant time and never matches a different length', () => {
    const expected = Buffer.from('abcd');
    expect(WebhookSignatureScheme.matches(expected, [Buffer.from('abc'), Buffer.from('abcde')])).toBe(false);
    expect(WebhookSignatureScheme.matches(expected, [Buffer.from('abce'), Buffer.from('abcd')])).toBe(true);
    expect(WebhookSignatureScheme.matches(expected, [])).toBe(false);
  });

  it('generates secrets a Standard Webhooks verifier accepts', () => {
    const secret = generateSecret();
    expect(standardSecretKey(secret)).toHaveLength(32);
    expect(standardSecretKey(secret.slice('whsec_'.length))).toEqual(standardSecretKey(secret));
  });
});
