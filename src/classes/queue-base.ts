import { createHash } from 'crypto';
import { EventEmitter } from 'events';
import {
  EmqClient,
  MinimalQueue,
  QueueBaseOptions,
  Span,
} from '../interfaces';

import {
  delay,
  DELAY_TIME_5,
  isNotConnectionError,
  isPgPool,
  trace,
} from '../utils';
import type { PoolClient } from 'pg';
import { createScripts } from '../utils/create-scripts';
import { PgPoolConnection } from './pg-connection';
import { Job } from './job';
import { KeysMap, QueueKeys } from './queue-keys';
import { Scripts } from './scripts';
import { SpanKind } from '../enums';
import { ensureQueueRow, resolveSchema } from './queue-identity';

/**
 * Base class for all classes that need to interact with queues.
 * This class is normally not used directly, but extended by the other classes.
 *
 */
export class QueueBase extends EventEmitter implements MinimalQueue {
  toKey: (type: string) => string;
  keys: KeysMap;
  closing: Promise<void> | undefined;

  protected closed = false;
  protected hasBlockingConnection = false;
  protected scripts!: Scripts;
  protected connection: PgPoolConnection;
  public readonly qualifiedName: string;
  public readonly schema: string;
  private queueIdPromise?: Promise<number>;
  /**
   * When set (e.g. during `Queue.prototype.inTransaction`), `client` resolves to
   * this pinned connection so job SQL runs inside the caller's transaction.
   */
  protected transactionClient: PoolClient | null = null;

  /**
   *
   * @param name - The name of the queue.
   * @param opts - Options for the queue.
   * @param Connection - An optional "Connection" class used to instantiate a Connection. This is useful for
   * testing with mockups and/or extending the Connection class and passing an alternate implementation.
   */
  constructor(
    public readonly name: string,
    public opts: QueueBaseOptions = { connection: {} },
    Connection: typeof PgPoolConnection = PgPoolConnection,
    hasBlockingConnection = false,
  ) {
    super();

    this.hasBlockingConnection = hasBlockingConnection;
    this.opts = {
      prefix: 'emq',
      ...opts,
    };

    if (!name) {
      throw new Error('Queue name must be provided');
    }

    if (name.includes(':')) {
      throw new Error('Queue name cannot contain :');
    }

    this.connection = new Connection(opts.connection, {
      shared: isPgPool(opts.connection),
      blocking: hasBlockingConnection,
      skipVersionCheck: opts.skipVersionCheck,
      skipMigrations: opts.skipMigrations,
      schema: opts.schema,
    });

    this.connection.on('error', (error: Error) => this.emit('error', error));
    this.connection.on('close', () => {
      if (!this.closing) {
        this.emit('connection:close');
      }
    });

    const queueKeys = new QueueKeys(opts.prefix);
    this.qualifiedName = queueKeys.getQueueQualifiedName(name);
    this.keys = queueKeys.getKeys(name);
    this.toKey = (type: string) => queueKeys.toKey(name, type);
    this.schema = resolveSchema(opts);
    this.createScripts();
  }

  get queueId(): Promise<number> {
    if (!this.queueIdPromise) {
      this.queueIdPromise = (async () => {
        const client = await this.client;
        return ensureQueueRow(
          client,
          this.schema,
          this.opts.prefix || 'emq',
          this.name,
        );
      })();
    }
    return this.queueIdPromise;
  }

  /**
   * Returns a promise that resolves to the underlying `pg.Pool`. Normally
   * used only by subclasses.
   */
  get client(): Promise<EmqClient> {
    if (this.transactionClient) {
      return Promise.resolve(
        this.transactionClient as unknown as EmqClient,
      );
    }
    return this.connection.client;
  }

  protected createScripts() {
    this.scripts = createScripts(this);
  }

  /** PostgreSQL server version string e.g. `'16.3'`. */
  get postgresVersion(): string {
    return this.connection.postgresVersion;
  }

  /**
   * Helper to easily extend Job class calls.
   */
  protected get Job(): typeof Job {
    return Job;
  }

  /**
   * Emits an event. Normally used by subclasses to emit events.
   *
   * Listener exceptions are routed to the `error` event listener so a single
   * misbehaving consumer does not crash the worker, but they are no longer
   * silently swallowed: if there is no `error` listener the original error
   * propagates per Node.js EventEmitter semantics.
   */
  emit(event: string | symbol, ...args: any[]): boolean {
    try {
      return super.emit(event, ...args);
    } catch (err) {
      if (event === 'error') {
        throw err;
      }
      return super.emit('error', err);
    }
  }

  waitUntilReady(): Promise<EmqClient> {
    return this.client;
  }

  protected base64Name(): string {
    return Buffer.from(this.name).toString('base64');
  }

  protected clientName(suffix = ''): string {
    // PostgreSQL caps `application_name` at NAMEDATALEN-1 (63 chars) and
    // silently truncates anything longer — which would drop our suffix
    // (`":qe"`, `":w:<name>"`) and break `getWorkers` / `getQueueEvents`.
    // Compact the queue portion to a 16-char md5 digest so
    // `${prefix}:${digest}${suffix}` is uniformly short; both producers
    // (PgConnection.applyListenerAppName) and consumers
    // (QueueGetters.baseGetClients) derive the same value from the queue name.
    const digest = createHash('md5')
      .update(`${this.opts.prefix}:${this.name}`)
      .digest('hex')
      .slice(0, 16);
    return `${this.opts.prefix}:${digest}${suffix}`;
  }

  /**
   *
   * Closes the connection and returns a promise that resolves when the connection is closed.
   */
  async close(): Promise<void> {
    if (!this.closing) {
      this.closing = this.connection.close();
    }
    await this.closing;
    this.closed = true;
  }

  /**
   *
   * Force disconnects a connection.
   */
  disconnect(): Promise<void> {
    return this.connection.disconnect();
  }

  protected async checkConnectionError<T>(
    fn: () => Promise<T>,
    delayInMs = DELAY_TIME_5,
  ): Promise<T | undefined> {
    try {
      return await fn();
    } catch (error) {
      if (isNotConnectionError(error as Error)) {
        this.emit('error', <Error>error);
      }

      if (!this.closing && delayInMs) {
        await delay(delayInMs);
      } else {
        return;
      }
    }
  }

  /**
   * Wraps the code with telemetry and provides a span for configuration.
   *
   * @param spanKind - kind of the span: Producer, Consumer, Internal
   * @param operation - operation name (such as add, process, etc)
   * @param destination - destination name (normally the queue name)
   * @param callback - code to wrap with telemetry
   * @param srcPropagationMetadata -
   * @returns
   */
  trace<T>(
    spanKind: SpanKind,
    operation: string,
    destination: string,
    callback: (span?: Span, dstPropagationMetadata?: string) => Promise<T> | T,
    srcPropagationMetadata?: string,
  ) {
    return trace<Promise<T> | T>(
      this.opts.telemetry,
      spanKind,
      this.name,
      operation,
      destination,
      callback,
      srcPropagationMetadata,
    );
  }
}
