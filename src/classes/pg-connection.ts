'use strict';

import { EventEmitter } from 'events';
import { Pool, type PoolConfig, type PoolClient } from 'pg';
import type { ConnectionOptions, PgClient } from '../interfaces/connection';
import {
  increaseMaxListeners,
  decreaseMaxListeners,
  isNotConnectionError,
} from '../utils';
import { migrate } from './migrate';
import { resolveSchema } from './queue-identity';
import { version as packageVersion } from '../version';
import { NotificationManager } from './notification-manager';

export interface PgExtraOptions {
  shared?: boolean;
  blocking?: boolean;
  skipVersionCheck?: boolean;
  /** Skip the implicit `migrate()` on init. See {@link QueueBaseOptions.skipMigrations}. */
  skipMigrations?: boolean;
  /** When `connection` is a shared `pg.Pool`, schema must be passed here (Pool has no `schema` field). */
  schema?: string;
  /**
   * Set `application_name` on the dedicated blocking/listener client so it
   * surfaces in `pg_stat_activity`. Only applied when `blocking` is true.
   */
  clientName?: string;
}

function isPoolInstance(opts: ConnectionOptions): opts is Pool {
  return (
    typeof opts === 'object' &&
    opts !== null &&
    'query' in opts &&
    'connect' in opts
  );
}

/**
 * Owns a `pg.Pool`, runs migrations on init, and (when `blocking` is true)
 * holds a dedicated `LISTEN` client for `pg_notify` fan-out via
 * {@link NotificationManager}.
 *
 * Unlike the previous implementation, this class no longer wraps the pool in
 * a Proxy nor attaches Redis-shaped helper methods. Consumer applications
 * receive a plain `pg.Pool` from `connection.client` / `queue.client` and may
 * reuse it freely.
 */
export class PgConnection extends EventEmitter {
  static minimumVersion = '14.0.0';
  static recommendedMinimumVersion = '14.0.0';

  closing = false;
  status: 'initializing' | 'ready' | 'closing' | 'closed' = 'initializing';

  protected _client: PgClient | undefined;
  private readonly opts: PoolConfig & {
    connectionString?: string;
    schema?: string;
    skipVersionCheck?: boolean;
  };
  private readonly initializing: Promise<PgClient>;
  private _postgresVersion = '14.0.0';
  protected packageVersion = packageVersion;
  private skipVersionCheck: boolean;
  private listenerClient: PoolClient | null = null;
  private pool: Pool;
  private closePromise: Promise<void> | null = null;
  private reconnecting = false;
  private poolErrorListener?: (err: Error) => void;
  /** Active reconnect backoff timer, tracked so close() can cancel/unref it. */
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * Present when {@link PgExtraOptions.blocking} is true; multiplexes LISTEN
   * for workers / queue events. Constructed eagerly so consumers can subscribe
   * before `init()` completes — subscriptions are queued and replayed on the
   * first `rebindListenerClient()`.
   */
  notificationManager?: NotificationManager;

  constructor(
    opts: ConnectionOptions,
    private readonly extraOptions: PgExtraOptions = {},
  ) {
    super();

    this.extraOptions = {
      shared: false,
      blocking: false,
      skipVersionCheck: false,
      skipMigrations: false,
      ...extraOptions,
    };

    if (isPoolInstance(opts)) {
      this.pool = opts;
      this.opts = this.extraOptions.schema
        ? { schema: this.extraOptions.schema }
        : {};
    } else if (typeof opts === 'string') {
      this.opts = { connectionString: opts };
      this.pool = new Pool({ connectionString: opts });
    } else {
      this.opts = { ...opts };
      const { schema: _s, ...poolConf } = opts;
      this.pool = new Pool(poolConf as PoolConfig);
    }

    this.skipVersionCheck =
      extraOptions?.skipVersionCheck ||
      !!(this.opts && this.opts.skipVersionCheck);

    if (this.extraOptions.blocking) {
      this.notificationManager = new NotificationManager(async () => {
        if (this.closing) {
          return null;
        }
        if (this.listenerClient) {
          return this.listenerClient;
        }
        try {
          await this.initializing;
        } catch {
          return null;
        }
        return this.listenerClient;
      });
    }

    this.initializing = this.init();
    this.initializing.catch(err => {
      if (this.listenerCount('error') > 0) {
        this.emit('error', err);
      }
    });
  }

  get schema(): string {
    return resolveSchema({ connection: this.opts });
  }

  get client(): Promise<PgClient> {
    return this.initializing;
  }

  /** PostgreSQL server version string e.g. `'16.3'`. */
  get postgresVersion(): string {
    return this._postgresVersion;
  }

  private async init(): Promise<PgClient> {
    increaseMaxListeners(this.pool, 5);
    this.poolErrorListener = (err: Error) => {
      // Pool-level errors (e.g. FATAL 57P01 from pg_terminate_backend on a
      // listener backend) must never escape as an uncaught 'error' event:
      // EventEmitter throws when 'error' is emitted with no listeners.
      if (this.listenerCount('error') > 0) {
        this.emit('error', err);
      }
    };
    this.pool.on('error', this.poolErrorListener);

    if (!this.extraOptions.skipMigrations) {
      await migrate(this.pool, this.schema);
    }

    const c = await this.pool.connect();
    try {
      if (!this.skipVersionCheck) {
        const {
          rows: [row],
        } = await c.query(
          "select current_setting('server_version') as v, current_setting('server_version_num') as n",
        );
        this._postgresVersion = row.v || '14';
        const n = parseInt(row.n, 10);
        if (n < 140000) {
          throw new Error(
            `elephantmq requires PostgreSQL >= 14 (current server_version_num=${row.n})`,
          );
        }
      }
    } finally {
      c.release();
    }

    if (this.extraOptions.blocking) {
      this.listenerClient = await this.pool.connect();
      increaseMaxListeners(this.listenerClient, 20);
      this.listenerClient.on('error', (err: Error) => {
        if (this.listenerCount('error') > 0) {
          this.emit('error', err);
        }
        if (!this.closing) {
          this.reconnect().catch(e => {
            if (this.listenerCount('error') > 0) {
              this.emit('error', e);
            }
          });
        }
      });
      await this.applyListenerAppName();
      if (this.notificationManager) {
        await this.notificationManager.rebindListenerClient();
      }
    }

    this._client = this.pool;
    this.status = 'ready';
    this.emit('ready');
    return this.pool;
  }

  /**
   * Update the `application_name` assigned to the dedicated listener client.
   * Safe to call before `init()` finishes — the name is cached and applied on
   * every bind/rebind of the listener client.
   */
  async setClientName(name: string): Promise<void> {
    this.extraOptions.clientName = name;
    if (this.listenerClient) {
      await this.applyListenerAppName();
    }
  }

  private async applyListenerAppName(): Promise<void> {
    if (!this.listenerClient || !this.extraOptions.clientName) {
      return;
    }
    try {
      // PostgreSQL caps application_name at 63 chars (NAMEDATALEN-1).
      const appName = this.extraOptions.clientName.slice(0, 63);
      await this.listenerClient.query(
        "select set_config('application_name', $1, false)",
        [appName],
      );
    } catch {
      /* best-effort: pgBouncer / pooled deployments may reject SET */
    }
  }

  /**
   * Drop the dedicated listener connection without closing the pool or
   * marking this connection as closing. {@link Worker.pause} and
   * {@link Worker.close} call this on the blocking connection to interrupt a
   * blocked notification wait; callers immediately follow up with
   * {@link reconnect} to re-establish the listener. Using `close()` here
   * would tear everything down and make `worker.resume()` impossible.
   */
  async disconnect(reconnect = true): Promise<void> {
    if (this.listenerClient) {
      const lc = this.listenerClient;
      this.listenerClient = null;
      // The listener attached in init() / reconnect() re-emits errors up to
      // PgConnection (and onward to Worker / QueueEvents). During teardown
      // we are about to destroy the socket on purpose, so swallow any
      // resulting errors instead of letting them propagate to consumers
      // that may have already removed their own 'error' handlers.
      lc.removeAllListeners('error');
      lc.on('error', () => undefined);
      const anyLc = lc as any;
      try {
        anyLc.connection?.stream?.destroy?.();
      } catch {
        /* ignore */
      }
      const backendPid: number | undefined =
        anyLc.processID ??
        anyLc._client?.processID ??
        anyLc.connection?.processID;
      if (backendPid && this.pool) {
        try {
          await this.pool.query('select pg_terminate_backend($1::int)', [
            backendPid,
          ]);
        } catch {
          /* ignore */
        }
      }
      try {
        anyLc.connection?.end?.();
      } catch {
        /* ignore */
      }
      try {
        lc.release(new Error('elephantmq: disconnect'));
      } catch {
        /* ignore */
      }
    }
    if (reconnect) {
      await this.reconnect();
    }
  }

  async reconnect(): Promise<void> {
    if (this.closing || this.reconnecting) {
      return;
    }
    this.reconnecting = true;
    const baseMs = 100;
    const capMs = 30_000;
    let sleepMs = baseMs;
    try {
      while (!this.closing) {
        await new Promise<void>(res => {
          const t = setTimeout(() => {
            this.reconnectTimer = null;
            res();
          }, sleepMs);
          if (typeof (t as any).unref === 'function') {
            (t as any).unref();
          }
          this.reconnectTimer = t;
        });
        if (this.closing) {
          return;
        }
        sleepMs = Math.min(
          capMs,
          Math.floor(
            baseMs + Math.random() * Math.max(0, sleepMs * 3 - baseMs),
          ),
        );
        try {
          if (!this.extraOptions.blocking || !this.pool || this.closing) {
            return;
          }

          if (this.listenerClient) {
            try {
              this.listenerClient.removeAllListeners();
              this.listenerClient.release();
            } catch {
              /* ignore */
            }
            this.listenerClient = null;
          }

          this.listenerClient = await this.pool.connect();
          increaseMaxListeners(this.listenerClient, 20);
          this.listenerClient.on('error', (err: Error) => {
            if (this.listenerCount('error') > 0) {
              this.emit('error', err);
            }
            if (!this.closing) {
              this.reconnect().catch(e => {
                if (this.listenerCount('error') > 0) {
                  this.emit('error', e);
                }
              });
            }
          });
          await this.applyListenerAppName();
          if (this.notificationManager) {
            await this.notificationManager.rebindListenerClient();
          }
          this.emit('ready');
          return;
        } catch (e) {
          if (this.listenerCount('error') > 0) {
            this.emit('error', e as Error);
          }
        }
      }
    } finally {
      this.reconnecting = false;
    }
  }

  async close(_force = false): Promise<void> {
    if (this.closePromise) {
      return this.closePromise;
    }
    this.closing = true;
    this.status = 'closing';
    this.closePromise = (async () => {
      try {
        try {
          await this.initializing;
        } catch {
          /* ignore init failure; we're closing anyway */
        }
        if (this.reconnectTimer) {
          try {
            clearTimeout(this.reconnectTimer);
          } catch {
            /* ignore */
          }
          this.reconnectTimer = null;
        }
        if (this.notificationManager) {
          try {
            this.notificationManager.close();
          } catch {
            /* ignore */
          }
        }
        if (this.listenerClient) {
          const lc = this.listenerClient;
          this.listenerClient = null;
          // Clear pg session-level LISTEN subscriptions so when the
          // PoolClient is recycled by a *later* PgConnection sharing the
          // same Pool we don't leak cross-test NOTIFY events. Keep an
          // 'error' swallow so a connection-end during teardown (e.g. the
          // server closing the backend) cannot turn into an uncaught
          // 'error' event after we have released our handlers.
          try {
            lc.removeAllListeners('notification');
          } catch {
            /* ignore */
          }
          try {
            lc.removeAllListeners('error');
          } catch {
            /* ignore */
          }
          lc.on('error', () => undefined);
          try {
            await lc.query('unlisten *');
          } catch {
            /* ignore: connection may already be dead */
          }
          try {
            lc.release();
          } catch {
            /* ignore */
          }
        }
        if (this.poolErrorListener) {
          try {
            this.pool.off('error', this.poolErrorListener);
          } catch {
            /* ignore */
          }
          this.poolErrorListener = undefined;
        }
        if (!this.extraOptions.shared) {
          try {
            await this.pool.end();
          } catch (error) {
            if (isNotConnectionError(error as Error)) {
              throw error;
            }
          }
        }
      } finally {
        try {
          decreaseMaxListeners(this.pool, 5);
        } catch {
          /* ignore */
        }
        this.removeAllListeners();
        this.status = 'closed';
        this.emit('close');
      }
    })();
    return this.closePromise;
  }

  waitUntilReady(): Promise<boolean> {
    return this.initializing.then(() => true);
  }
}

/** Primary exported connection class (PostgreSQL `Pool` + LISTEN client). */
export { PgConnection as PgPoolConnection };
