/**
 * The `nest-webhooks` command on PostgreSQL (lib/cli.ts: the kit's runStoreCli() on both stores' schemas; a
 * postgres:// URL picks PostgresWebhookStore's): `sql` prints what migrationSql() does, `migrate` applies the
 * migrations, `status` exits with 1 while the schema is behind, and every misuse says what to do instead, in webhooks'
 * words. tests/mysql/cli.spec.ts runs it on MySQL.
 */
import { PostgresWebhookStore } from '../../lib/postgres/index.js';
import { runCli as run } from '../support/cli.js';
import { testDatabase } from './support.js';

const { database, reason } = await testDatabase('pgstore_cli');

describe('nest-webhooks', () => {
  it('sql prints the migrations as migrationSql() does, without a database, with statement breakpoints on request', async () => {
    expect(await run(['sql'])).toEqual({ code: 0, out: PostgresWebhookStore.migrationSql(), err: '' });
    expect(await run(['sql', '--schema', 'shop_webhooks', '--from', '0', '--to', '1'])).toEqual({
      code: 0,
      out: PostgresWebhookStore.migrationSql({ schema: 'shop_webhooks', from: 0, to: 1 }),
      err: '',
    });
    expect(await run(['sql', '--dialect', 'postgres', '--statement-breakpoints'])).toEqual({
      code: 0,
      out: PostgresWebhookStore.migrationSql({ statementBreakpoints: true }),
      err: '',
    });
    expect(await run(['sql', '--from', 'one'])).toEqual({ code: 1, out: '', err: '--from takes a version number, not "one".\n' });
    expect((await run(['sql', '--to', '9'])).err).toContain('no migrations lead from version 0 to 9');
    expect((await run(['sql', '--schema', 'bad-name'])).err).toContain('PostgresWebhookStore: invalid schema "bad-name".');
  });

  it('prints its usage for --help, and on stderr, exiting with 1, for no command, an unknown one or an unknown option', async () => {
    const help = await run(['--help']);
    expect(help).toMatchObject({ code: 0, err: '' });
    expect(help.out).toMatch(/^Usage: npx @nestjs\/webhooks <command> \[options\]/);
    expect(help.out).toContain("\n\nPostgresWebhookStore's schema (@nestjs/webhooks/postgres) or MySqlWebhookStore's (@nestjs/webhooks/mysql):\n\n  migrate   ");
    expect(help.out).toContain('--schema <name>    The store\'s schema. Default: nest_webhooks');
    expect(await run([])).toEqual({ code: 1, out: '', err: help.out });
    expect(await run(['upgrade'])).toEqual({ code: 1, out: '', err: `Unknown command "upgrade".\n\n${help.out}` });
    expect((await run(['sql', '--verbose'])).err).toMatch(/^Unknown option '--verbose'/);
  });

  it('needs the database for migrate and status: --url, else DATABASE_URL, of PostgreSQL or MySQL', async () => {
    expect(await run(['migrate'])).toEqual({ code: 1, out: '', err: 'npx @nestjs/webhooks migrate needs the database: pass --url, or set DATABASE_URL.\n' });
    expect((await run(['status'])).err).toBe('npx @nestjs/webhooks status needs the database: pass --url, or set DATABASE_URL.\n');
    expect(await run(['migrate', '--url', 'sqlite:///tmp/shop.db'])).toEqual({
      code: 1,
      out: '',
      err: 'npx @nestjs/webhooks migrate takes a database URL that starts with postgres:// or postgresql:// or mysql://.\n',
    });
  });

  describe('on PostgreSQL', () => {
    beforeEach((context) => {
      if (reason) {
        context.skip(reason);
      }
    });

    it('migrate applies the pending migrations once, and status exits with 1 until they are', async () => {
      const url = database!.url;
      expect(await run(['status', '--url', url, '--schema', 'cli_store'])).toEqual({
        code: 1,
        out: 'Schema "cli_store" is at version 0; this version of @nestjs/webhooks needs version 1.\n',
        err: '',
      });
      expect(await run(['migrate', '--schema', 'cli_store'], { DATABASE_URL: url })).toEqual({
        code: 0,
        out: 'Migrated schema "cli_store" to version 1 (applied 1).\n',
        err: '',
      });
      expect(await run(['migrate', '--url', url, '--schema', 'cli_store'])).toEqual({ code: 0, out: 'Schema "cli_store" is up to date (version 1).\n', err: '' });
      expect(await run(['status', '--url', url, '--schema', 'cli_store'])).toMatchObject({
        code: 0,
        out: 'Schema "cli_store" is at version 1; this version of @nestjs/webhooks needs version 1.\n',
      });
      expect(await database!.admin.query('SELECT version FROM cli_store.migrations')).toMatchObject({ rows: [{ version: 1 }] });
    });

    it('reports a database it cannot migrate, and exits with 1', async () => {
      await database!.admin.query('CREATE SCHEMA cli_taken');
      await database!.admin.query('CREATE TABLE cli_taken.endpoints (id int)');
      expect(await run(['migrate', '--url', database!.url, '--schema', 'cli_taken'])).toEqual({
        code: 1,
        out: '',
        err: 'PostgresWebhookStore: migrating schema "cli_taken" from version 0 to 1 failed, and nothing was applied: relation "endpoints" already exists\n',
      });
    });
  });
});
