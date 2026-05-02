import { randomBytes } from 'crypto';
import type { Pool } from 'pg';
import { Pool as PgPool } from 'pg';
import type { EmqClient } from '../src/interfaces';

let pool: PgPool | undefined;

function quoteIdent(s: string): string {
  return '"' + s.replace(/"/g, '""') + '"';
}

export function getTestPool(): Pool {
  if (!pool) {
    const url = process.env.ELEPHANTMQ_TEST_PG_URL;
    if (!url) {
      throw new Error(
        'Set ELEPHANTMQ_TEST_PG_URL (see .env.test and docker-compose.yml)',
      );
    }
    // Each Worker / QueueEvents holds a dedicated listener PoolClient; tests
    // frequently spin up 5+ workers plus QueueEvents, so cap generously.
    const defaultMax =
      process.env.ELEPHANTMQ_TEST_PARALLEL === '0' ? 64 : 24;
    const max = Math.max(
      2,
      Number.parseInt(
        process.env.ELEPHANTMQ_TEST_PG_POOL_MAX || String(defaultMax),
        10,
      ) || defaultMax,
    );
    pool = new PgPool({
      connectionString: url,
      max,
      idleTimeoutMillis: 10_000,
    });
  }
  return pool;
}

export async function createTestSchema(p: Pool): Promise<string> {
  const schema = 'emq_t_' + randomBytes(8).toString('hex');
  await p.query(`create schema ${quoteIdent(schema)}`);
  return schema;
}

/**
 * Best-effort schema drop with a hard 2s cap: we never want a dangling
 * connection or PG error to fail the whole test file's afterAll.
 */
export async function dropTestSchema(
  p: Pool,
  schema: string | undefined,
  timeoutMs = 2000,
): Promise<void> {
  if (!schema) {
    return;
  }
  const dropPromise = p
    .query(`drop schema if exists ${quoteIdent(schema)} cascade`)
    .then(() => undefined);
  const timeoutPromise = new Promise<void>(resolve => setTimeout(resolve, timeoutMs));
  try {
    await Promise.race([dropPromise, timeoutPromise]);
  } catch (e) {
    // eslint-disable-next-line no-console
    console.warn(`[elephantmq tests] dropTestSchema(${schema}) failed:`, (e as Error).message);
  }
}

/** Shared test pool cast to the library's `queue.client` type (pg query API). */
export function getTestPoolAsEmqClient(): EmqClient {
  return getTestPool() as unknown as EmqClient;
}
