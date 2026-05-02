import type { Pool } from 'pg';
import { migrate } from '../src/classes/migrate';
import { getTestPool, createTestSchema, dropTestSchema } from './utils';

/** Shared pool across the test suite (set ELEPHANTMQ_TEST_PG_URL). */
export const testPool: Pool = getTestPool();

/** Default queue prefix used in tests. */
export const TEST_PREFIX = 'emq_test';

export interface PgConnectionOpts {
  connection: Pool;
  schema: string;
  prefix: string;
  /** Avoid re-running migrate() on every client; {@link installTestSchema} already migrates. */
  skipMigrations: true;
}

export function pgConnectionOpts(
  schema: string,
  prefix: string = TEST_PREFIX,
): PgConnectionOpts {
  return {
    connection: testPool,
    schema,
    prefix,
    skipMigrations: true,
  };
}

/**
 * Per-file isolated schema; use in `beforeAll`. Applies SQL migrations.
 */
export async function installTestSchema(): Promise<string> {
  const schema = await createTestSchema(testPool);
  await migrate(testPool, schema);
  return schema;
}

export async function uninstallTestSchema(schema: string): Promise<void> {
  await dropTestSchema(testPool, schema);
}

/**
 * Close a list of closeable objects in parallel without throwing. Use in
 * `afterAll` so a single close failure cannot poison subsequent tests.
 */
export async function closeAll(
  closeables: Array<{ close: () => Promise<void> | void } | undefined | null>,
): Promise<void> {
  const results = await Promise.allSettled(
    closeables
      .filter(
        (c): c is { close: () => Promise<void> | void } =>
          !!c && typeof c.close === 'function',
      )
      .map(async c => c.close()),
  );
  for (const r of results) {
    if (r.status === 'rejected') {
      // eslint-disable-next-line no-console
      console.warn('[elephantmq tests] close() rejected:', r.reason);
    }
  }
}
