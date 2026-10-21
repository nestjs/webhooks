import { WebhooksError } from '../../errors/webhooks.error.js';

/**
 * The schema of `PostgresWebhookStore` (`@nestjs/webhooks/postgres`) or `MySqlWebhookStore` (`@nestjs/webhooks/mysql`)
 * can't serve this version of the package: it is behind the store's migrations (and `migrate` is off), or applying
 * them failed (`cause`). The store refuses every call until it's fixed, and the application fails to start with it.
 *
 * ```ts
 * try {
 *   await app.init();
 * } catch (error) {
 *   if (error instanceof WebhookSchemaError) {
 *     console.error(`Run: npx @nestjs/webhooks migrate --schema ${error.schema} (at ${error.version}, needs ${error.requiredVersion})`);
 *   }
 *   throw error;
 * }
 * ```
 */
export class WebhookSchemaError extends WebhooksError {
  /** The store's `schema`: a PostgreSQL schema, or on MySQL the prefix of the store's tables (`<schema>_<table>`). */
  readonly schema: string;
  /** The schema's version: the last migration applied to it, `0` for none. */
  readonly version: number;
  /** The version this version of the package needs: its last migration. */
  readonly requiredVersion: number;

  constructor(message: string, details: { schema: string; version: number; requiredVersion: number; cause?: unknown }) {
    super(message, details.cause === undefined ? undefined : { cause: details.cause });
    this.schema = details.schema;
    this.version = details.version;
    this.requiredVersion = details.requiredVersion;
  }
}
