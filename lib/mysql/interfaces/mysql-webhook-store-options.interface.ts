import type { SqlExecutor } from '@nestjs/store-kit/mysql';

/**
 * What `new MySqlWebhookStore(options, storage)` takes.
 *
 * ```ts
 * const options: MySqlWebhookStoreOptions = { executor: fromDrizzle(db), schema: 'shop_webhooks', migrate: false };
 * ```
 */
export interface MySqlWebhookStoreOptions {
  /**
   * How the store reaches the database: `fromMysql2(pool)`, `fromSequelize(sequelize)`, `fromDrizzle(db)`,
   * `fromTypeOrm(dataSource)`, `fromPrisma(prisma)` or `fromKysely(db)` from `@nestjs/webhooks/mysql`. The store's
   * tables live in its connections' database (the one the pool or ORM connects to). Every statement and transaction of
   * the store runs on it; none joins the application's transactions (the outbox carries a dispatched message out of
   * them). `fromSequelize()` needs mysql2's `FOUND_ROWS` client flag: Sequelize's MySQL connection manager sets
   * `flags: "-FOUND_ROWS"` unless the instance passes `dialectOptions: { flags: '' }`.
   */
  executor: SqlExecutor<'mysql'>;
  /**
   * The name the store's tables start with, in the connection's database: `<schema>_<table>` (`nest_webhooks`:
   * `nest_webhooks_endpoints`, `nest_webhooks_deliveries`...). Keep it for the store alone. Lowercase letters, digits
   * and underscores, not starting with a digit, at most 40 characters. Default: `'nest_webhooks'`.
   */
  schema?: string;
  /**
   * Apply the store's pending migrations at startup, one statement at a time under a lock (`GET_LOCK()`), so processes
   * that start together migrate once, and a run that failed resumes where it stopped. With `false`, startup fails with
   * a `WebhookSchemaError` while the schema is behind this version of the package: apply them with
   * `npx nest-webhooks migrate`, or with your own migration tool (`MySqlWebhookStore.migrationStatements()`). Default:
   * `true`, except when `NODE_ENV` is `production`.
   */
  migrate?: boolean;
}
