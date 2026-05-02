import { AdvancedRepeatOptions } from './advanced-options';
import { DefaultJobOptions } from './base-job-options';
import type { ConnectionOptions } from './connection';
import { Telemetry } from './telemetry';

export enum ClientType {
  blocking = 'blocking',
  normal = 'normal',
}

/**
 * Base Queue options
 */
export interface QueueBaseOptions {
  /**
   * Connection: Postgres URL / `PoolConfig`, or a shared `pg.Pool`.
   */
  connection: ConnectionOptions;
  /**
   * PostgreSQL schema for elephantmq tables (default: public).
   */
  schema?: string;

  /**
   * Prefix for all queue keys.
   */
  prefix?: string;

  /**
   * Skip the runtime PostgreSQL version check (must be 14 or newer).
   * @defaultValue false
   */
  skipVersionCheck?: boolean;

  /**
   * Telemetry client.
   */
  telemetry?: Telemetry;

  /**
   * Skip the implicit `migrate()` that runs on first connect.
   *
   * Production deployments are encouraged to set this to `true` and run
   * {@link migrate} from a dedicated job (CI/CD step, init container, or
   * one-shot script) so application processes do not contend on the
   * advisory migration lock at startup. When `false` (the default), every
   * `Queue` / `Worker` / `FlowProducer` will attempt to migrate on init,
   * which is convenient for local development and tests but does extra
   * work and requires a role with DDL permission.
   *
   * @defaultValue false
   */
  skipMigrations?: boolean;

  /**
   * Use a dedicated connection for blocking LISTEN (workers). Default follows
   * the Worker implementation when not overridden.
   */
  blockingConnection?: boolean;
}

/**
 * Options for the Queue class.
 */
export interface QueueOptions extends QueueBaseOptions {
  defaultJobOptions?: DefaultJobOptions;

  /**
   * Options for the internal events store / trimming.
   */
  streams?: {
    /**
     * Options for the events stream.
     */
    events: {
      /**
       * Max approximated length for streams. Default is 10 000 events.
       */
      maxLen: number;
      /**
       * Optional periodic trim of `emq_events` (milliseconds between runs).
       */
      trim?: {
        every: number;
        /** Overrides `events.maxLen` for trim target when set. */
        maxLen?: number;
      };
    };
  };

  /**
   * Skip Meta update.
   *
   * If true, the queue will not update the metadata of the queue.
   * Useful for read-only systems that do should not update the metadata.
   *
   * @defaultValue false
   */
  skipMetasUpdate?: boolean;

  /**
   * Advanced options for the repeatable jobs.
   */
  settings?: AdvancedRepeatOptions;
}

/**
 * Options for the Repeat class.
 */
export interface RepeatBaseOptions extends QueueBaseOptions {
  settings?: AdvancedRepeatOptions;
}

/**
 * Options for QueueEvents
 */
export interface QueueEventsOptions
  extends Omit<QueueBaseOptions, 'telemetry'> {
  /**
   * Condition to start listening to events at instance creation.
   */
  autorun?: boolean;
  /**
   * Last event Id. If provided it is possible to continue
   * consuming events from a known Id instead of from the last
   * produced event.
   */
  lastEventId?: string;

  /**
   * Timeout for the blocking XREAD call to the events stream.
   */
  blockingTimeout?: number;
}

/**
 * Options for QueueEventsProducer
 */
export type QueueEventsProducerOptions = Omit<QueueBaseOptions, 'telemetry'>;
