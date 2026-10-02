import type { SqlExecutor } from '@nestjs/store-kit/postgres';

/**
 * What `new PostgresWebhookStore(options, storage)` takes.
 *
 * ```ts
 * const options: PostgresWebhookStoreOptions = { executor: fromDrizzle(db), schema: 'shop_webhooks', migrate: false };
 * ```
 */
export interface PostgresWebhookStoreOptions {
  /**
   * How the store reaches the database: `fromPg(pool)`, `fromSequelize(sequelize)`, `fromDrizzle(db)`,
   * `fromTypeOrm(dataSource)`, `fromPrisma(prisma)` or `fromKysely(db)`. Every statement and transaction of the store
   * runs on it; none joins the application's transactions (the outbox carries a dispatched message out of them).
   */
  executor: SqlExecutor<'postgres'>;
  /**
   * The schema that holds the store's tables, created by its first migration: keep it for the store alone. Letters,
   * digits and underscores, not starting with a digit, at most 63 characters. Default: `'nest_webhooks'`.
   */
  schema?: string;
  /**
   * Apply the store's pending migrations at startup, in one transaction under an advisory lock, so processes that
   * start together migrate once. With `false`, startup fails with a `WebhookSchemaError` while the schema is behind
   * this version of the package: apply them with `npx nest-webhooks migrate`, or with your own migration tool
   * (`PostgresWebhookStore.migrationSql()`). Default: `true`, except when `NODE_ENV` is `production`.
   */
  migrate?: boolean;
}
