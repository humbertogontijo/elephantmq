import type { PoolConfig } from 'pg';

export interface BaseOptions {
  skipVersionCheck?: boolean;
  /** Schema for elephantmq tables (default: public) */
  schema?: string;
  url?: string;
}

/** PostgreSQL pool configuration for Queue / Worker `connection` options. */
export type PgConnectionOptions = PoolConfig & BaseOptions;

/** Same as {@link PgConnectionOptions}; cluster mode is not supported. */
export type ClusterOptions = PoolConfig & BaseOptions;

export type { ConnectionOptions } from './connection';
