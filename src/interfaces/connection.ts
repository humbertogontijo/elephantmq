import { EventEmitter } from 'events';
import type { Pool, PoolClient } from 'pg';

/** Any pg client that can run parameterized queries (pool or pooled connection). */
export type PgQueryable = Pool | PoolClient;

/**
 * The pg `Pool` exposed as `queue.client` / `connection.client`.
 *
 * elephantmq does not wrap or mutate the pool; consumer applications can
 * reuse the same `pg.Pool` for their own queries.
 */
export type PgClient = Pool;

/** Backwards-compatible alias for the pool handle. */
export type EmqClient = PgClient;

export type ConnectionOptions =
  | string
  | (import('pg').PoolConfig & {
      schema?: string;
      skipVersionCheck?: boolean;
    })
  | Pool;

export interface IConnection extends EventEmitter {
  waitUntilReady(): Promise<boolean>;
  client: Promise<PgClient>;
}
