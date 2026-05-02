import { JobProgress } from '../types';
import {
  EmqClient,
  EmqConnectionListener,
  QueueEventsOptions,
} from '../interfaces';
import { QUEUE_EVENT_SUFFIX } from '../utils';
import { QueueBase } from './queue-base';
import { PgPoolConnection, type PgConnection } from './pg-connection';
import { escapeSchema } from './queue-identity';
import type { NotificationManager } from './notification-manager';
import { channelForEvents } from './notification-manager';

/** BullMQ stores some payloads as JSON strings; plain strings are valid return values. */
function parseOptionalJsonString(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

export interface QueueEventsListener extends EmqConnectionListener {
  /**
   * Listen to 'active' event.
   *
   * This event is triggered when a job enters the 'active' state, meaning it is being processed.
   *
   * @param args - An object containing details about the job that became active.
   *   - `jobId`: The unique identifier of the job that entered the active state.
   *   - `prev`: The previous state of the job before it became active (e.g., 'waiting'), if applicable.
   *
   * @param id - The identifier of the event.
   */

  active: (args: { jobId: string; prev?: string }, id: string) => void;

  /**
   * Listen to 'added' event.
   *
   * This event is triggered when a job is created and added to the queue.
   *
   * @param args - An object containing details about the newly added job.
   *   - `jobId` - The unique identifier of the job that was added.
   *   - `name` - The name of the job, typically indicating its type or purpose.
   * @param id - The identifier of the event.
   */
  added: (args: { jobId: string; name: string }, id: string) => void;

  /**
   * Listen to 'cleaned' event.
   *
   * This event is triggered when jobs are cleaned (e.g., removed) from the queue, typically via a cleanup method.
   *
   * @param args - An object containing the count of cleaned jobs.
   *   - `count` - The number of jobs that were cleaned, represented as a string due to Redis serialization.
   * @param id - The identifier of the event.
   */
  cleaned: (args: { count: string }, id: string) => void;

  /**
   * Listen to 'completed' event.
   *
   * This event is triggered when a job has successfully completed its execution.
   *
   * @param args - An object containing details about the completed job.
   *   - `jobId` - The unique identifier of the job that completed.
   *   - `returnvalue` - The return value of the job, serialized as a string.
   *   - `prev` - The previous state of the job before completion (e.g., 'active'), if applicable.
   * @param id - The identifier of the event.
   */
  completed: (
    args: { jobId: string; returnvalue: string; prev?: string },
    id: string,
  ) => void;

  /**
   * Listen to 'deduplicated' event.
   *
   * This event is triggered when a job is not added to the queue because a job with the same deduplicationId
   * already exists.
   *
   * @param args - An object containing details about the deduplicated job.
   *  - `jobId` - The unique identifier of the job that was attempted to be added.
   *  - `deduplicationId` - The deduplication identifier that caused the job to be deduplicated.
   *  - `deduplicatedJobId` - The unique identifier of the existing job that caused the deduplication.
   * @param id - The identifier of the event.
   */
  deduplicated: (
    args: { jobId: string; deduplicationId: string; deduplicatedJobId: string },
    id: string,
  ) => void;

  /**
   * Listen to 'delayed' event.
   *
   * This event is triggered when a job is scheduled with a delay before it becomes active.
   *
   * @param args - An object containing details about the delayed job.
   *  - `jobId` - The unique identifier of the job that was delayed.
   *  - `delay` - The delay duration in milliseconds before the job becomes active.
   * @param id - The identifier of the event.
   */
  delayed: (args: { jobId: string; delay: number }, id: string) => void;

  /**
   * Listen to 'drained' event.
   *
   * This event is triggered when the queue has drained its waiting list, meaning there are no jobs
   * in the 'waiting' state.
   * Note that there could still be delayed jobs waiting their timers to expire
   * and this event will still be triggered as long as the waiting list has emptied.
   *
   * @param id - The identifier of the event.
   */
  drained: (id: string) => void;

  /**
   * Listen to 'duplicated' event.
   *
   * This event is triggered when a job is not created because a job with the same identifier already exists.
   *
   * @param args - An object containing the job identifier.
   *  - `jobId` - The unique identifier of the job that was attempted to be added.
   * @param id - The identifier of the event.
   */
  duplicated: (args: { jobId: string }, id: string) => void;

  /**
   * Listen to 'error' event.
   *
   * This event is triggered when an error in the Redis backend is thrown.
   */
  error: (args: Error) => void;

  /**
   * Listen to 'failed' event.
   *
   * This event is triggered when a job fails by throwing an exception during execution.
   *
   * @param args - An object containing details about the failed job.
   *  - `jobId` - The unique identifier of the job that failed.
   *  - `failedReason` - The reason or message describing why the job failed.
   *  - `prev` - The previous state of the job before failure (e.g., 'active'), if applicable.
   * @param id - The identifier of the event.
   */
  failed: (
    args: { jobId: string; failedReason: string; prev?: string },
    id: string,
  ) => void;

  /**
   * Listen to 'paused' event.
   *
   * This event is triggered when the queue is paused, halting the processing of new jobs.
   *
   * @param args - An empty object (no additional data provided).
   * @param id - The identifier of the event.
   */
  paused: (args: object, id: string) => void;

  /**
   * Listen to 'progress' event.
   *
   * This event is triggered when a job updates its progress via the `Job#updateProgress()` method, allowing
   * progress or custom data to be communicated externally.
   *
   * @param args - An object containing the job identifier and progress data.
   *  - `jobId` - The unique identifier of the job reporting progress.
   *  - `data` - The progress data, which can be a number (e.g., percentage) or an object with custom data.
   * @param id - The identifier of the event.
   */
  progress: (args: { jobId: string; data: JobProgress }, id: string) => void;

  /**
   * Listen to 'removed' event.
   *
   * This event is triggered when a job is manually removed from the queue.
   *
   * @param args - An object containing details about the removed job.
   *  - `jobId` - The unique identifier of the job that was removed.
   *  - `prev` - The previous state of the job before removal (e.g., 'active' or 'waiting').
   * @param id - The identifier of the event.
   */
  removed: (args: { jobId: string; prev: string }, id: string) => void;

  /**
   * Listen to 'resumed' event.
   *
   * This event is triggered when the queue is resumed, allowing job processing to continue.
   *
   * @param args - An empty object (no additional data provided).
   * @param id - The identifier of the event.
   */
  resumed: (args: object, id: string) => void;

  /**
   * Listen to 'retries-exhausted' event.
   *
   * This event is triggered when a job has exhausted its maximum retry attempts after repeated failures.
   *
   * @param args - An object containing details about the job that exhausted retries.
   *  - `jobId` - The unique identifier of the job that exhausted its retries.
   *  - `attemptsMade` - The number of retry attempts made, represented as a string
   * (due to Redis serialization).
   * @param id - The identifier of the event.
   */
  'retries-exhausted': (
    args: { jobId: string; attemptsMade: string },
    id: string,
  ) => void;

  /**
   * Listen to 'stalled' event.
   *
   * This event is triggered when a job moves from 'active' back to 'waiting' or
   * 'failed' because the processor could not renew its lock, indicating a
   * potential processing issue.
   *
   * @param args - An object containing the job identifier.
   *  - `jobId` - The unique identifier of the job that stalled.
   * @param id - The identifier of the event.
   */
  stalled: (args: { jobId: string }, id: string) => void;

  /**
   * Listen to 'waiting' event.
   *
   * This event is triggered when a job enters the 'waiting' state, indicating it is queued and
   * awaiting processing.
   *
   * @param args - An object containing details about the job in the waiting state.
   *  - `jobId` - The unique identifier of the job that is waiting.
   *  - `prev` - The previous state of the job before entering 'waiting' (e.g., 'stalled'),
   * if applicable.
   * @param id - The identifier of the event.
   */

  waiting: (args: { jobId: string; prev?: string }, id: string) => void;

  /**
   * Listen to 'waiting-children' event.
   *
   * This event is triggered when a job enters the 'waiting-children' state, indicating it is
   * waiting for its child jobs to complete.
   *
   * @param args - An object containing the job identifier.
   *  - `jobId` - The unique identifier of the job waiting for its children.
   * @param id - The identifier of the event.
   */
  'waiting-children': (args: { jobId: string }, id: string) => void;
}

type CustomParameters<T> = T extends (...args: infer Args) => void
  ? Args
  : never;

type KeyOf<T extends object> = Extract<keyof T, string>;

/**
 * The QueueEvents class is used for listening to the global events
 * emitted by a given queue.
 *
 * This class requires a dedicated redis connection.
 *
 */
export class QueueEvents extends QueueBase {
  private running = false;
  private blocking = false;
  private eventWaitResolvers: Array<() => void> = [];
  private consumerInitSettled = false;
  private readonly consumerInit!: {
    promise: Promise<void>;
    resolve: () => void;
    reject: (reason?: unknown) => void;
  };

  private readonly onEventsChannelNotification = (payload?: string) => {
    const prefix = `${this.qualifiedName}:`;
    if (payload == null || payload === '' || payload.startsWith(prefix)) {
      this.wakeEventWaiters();
    }
  };

  constructor(
    name: string,
    { connection, autorun = true, ...opts }: QueueEventsOptions = {
      connection: {},
    },
    Connection?: typeof PgPoolConnection,
  ) {
    let initResolve!: () => void;
    let initReject!: (reason?: unknown) => void;
    const initPromise = new Promise<void>((resolve, reject) => {
      initResolve = resolve;
      initReject = reject;
    });

    super(
      name,
      {
        ...opts,
        connection,
      },
      Connection,
      true,
    );

    this.consumerInit = {
      promise: initPromise,
      resolve: initResolve,
      reject: initReject,
    };

    this.opts = Object.assign(
      {
        blockingTimeout: 10000,
      },
      this.opts,
    );

    // Advertise ourselves in pg_stat_activity so queue introspection helpers can
    // surface this instance through PgConnection's listener client.
    (this.connection as PgConnection)
      .setClientName(this.clientName(QUEUE_EVENT_SUFFIX))
      .catch(() => {
        /* non-fatal: pg may disallow set on pgBouncer etc. */
      });

    if (autorun) {
      this.run().catch(error => this.emit('error', error));
    }
  }

  /**
   * Resolves when a DB client exists and the event consumer has set its cursor
   * and subscribed on the LISTEN channel so callers do not enqueue work that
   * finishes before `emq_events` polling can observe new rows (which would
   * otherwise permanently skip those events when using the default `$` cursor).
   */
  override async waitUntilReady(): Promise<EmqClient> {
    const client = await super.waitUntilReady();
    await this.consumerInit.promise;
    return client;
  }

  private resolveConsumerInit(): void {
    if (this.consumerInitSettled) {
      return;
    }
    this.consumerInitSettled = true;
    this.consumerInit.resolve();
  }

  private rejectConsumerInit(reason?: unknown): void {
    if (this.consumerInitSettled) {
      return;
    }
    this.consumerInitSettled = true;
    this.consumerInit.reject(reason);
  }

  emit<
    QEL extends QueueEventsListener = QueueEventsListener,
    U extends KeyOf<QEL> = KeyOf<QEL>,
  >(event: U, ...args: CustomParameters<QEL[U]>): boolean {
    return super.emit(event, ...args);
  }

  off<
    QEL extends QueueEventsListener = QueueEventsListener,
    U extends KeyOf<QEL> = KeyOf<QEL>,
  >(eventName: U, listener: QEL[U]): this {
    super.off(eventName, listener as (...args: any[]) => void);
    return this;
  }

  on<
    QEL extends QueueEventsListener = QueueEventsListener,
    U extends KeyOf<QEL> = KeyOf<QEL>,
  >(event: U, listener: QEL[U]): this {
    super.on(event, listener as (...args: any[]) => void);
    return this;
  }

  once<
    QEL extends QueueEventsListener = QueueEventsListener,
    U extends KeyOf<QEL> = KeyOf<QEL>,
  >(event: U, listener: QEL[U]): this {
    super.once(event, listener as (...args: any[]) => void);
    return this;
  }

  /**
   * Manually starts running the event consumming loop. This shall be used if you do not
   * use the default "autorun" option on the constructor.
   */
  async run(): Promise<void> {
    if (!this.running) {
      try {
        this.running = true;
        const client = await this.client;
        // Note: application_name is set on the dedicated listener client by
        // PgConnection.setClientName() (invoked from our constructor). Setting
        // it here would leak onto a random pooled client and produce duplicate
        // rows in `pg_stat_activity`, breaking `getQueueEvents`.
        await this.consumeEvents(client);
      } catch (error) {
        this.running = false;
        this.rejectConsumerInit(error);
        throw error;
      }
    } else {
      throw new Error('Queue Events is already running.');
    }
  }

  private async consumeEvents(client: EmqClient): Promise<void> {
    const opts: QueueEventsOptions = this.opts;
    const S = escapeSchema(this.schema);
    const qid = await this.queueId;
    const ch = channelForEvents(this.qualifiedName);
    const nm = (this.connection as unknown as PgConnection).notificationManager;

    if (!nm) {
      // QueueEvents depends on LISTEN/NOTIFY; without a blocking connection it
      // degrades to polling only, which misses low-latency wakeups. QueueBase
      // always constructs with hasBlockingConnection=true, so this should not
      // normally happen.
      console.warn(
        '[elephantmq] QueueEvents: no NotificationManager on connection; falling back to polling only.',
      );
    }

    let lastId = opts.lastEventId;
    if (!lastId || lastId === '$') {
      const {
        rows: [row],
      } = await client.query<{ m: string }>(
        `select coalesce(max(id), 0)::text as m from ${S}.emq_events where queue_id = $1`,
        [qid],
      );
      lastId = row?.m ?? '0';
    } else {
      // Redis stream IDs look like `ms-seq` (e.g. `0-0`); we store a plain
      // bigint per queue. Strip anything after the dash so `::bigint` casts
      // cleanly — `'0-0'` → `'0'`, `'1734567890123-5'` → `'1734567890123'`.
      lastId = String(lastId).split('-')[0] || '0';
    }

    if (nm) {
      // subscribe() never throws (subscriptions are queued if the listener
      // client is not wired yet), so no try/catch needed.
      await nm.subscribe(ch, this.onEventsChannelNotification);
    }

    this.resolveConsumerInit();

    try {
    while (!this.closing) {
      this.blocking = true;
      const qres = await this.checkConnectionError(() =>
        client.query<{
          id: string;
          event: string;
          args: Record<string, unknown>;
        }>(
          `select id::text, event, args from ${S}.emq_events
           where queue_id = $1 and id > $2::bigint
           order by id asc limit 200`,
          [qid, lastId],
        ),
      );
      this.blocking = false;

      if (!qres) {
        continue;
      }

      const { rows } = qres;

      if (!rows || rows.length === 0) {
        await this.waitForEventNotification(nm, opts.blockingTimeout!);
        continue;
      }

      for (const row of rows) {
        lastId = row.id;
        const payload =
          typeof row.args === 'object' && row.args !== null ? row.args : {};
        const args: Record<string, any> = {
          event: row.event,
          ...payload,
        };

        switch (row.event) {
          case 'progress':
            if (typeof args.data === 'string') {
              args.data = parseOptionalJsonString(args.data);
            }
            break;
          case 'completed':
            if (typeof args.returnvalue === 'string') {
              args.returnvalue = parseOptionalJsonString(args.returnvalue);
            }
            break;
        }

        const { event, ...restArgs } = args;

        if (event === 'drained') {
          this.emit(event, row.id);
        } else {
          this.emit(event as any, restArgs, row.id);
          if (restArgs.jobId) {
            this.emit(`${event}:${restArgs.jobId}` as any, restArgs, row.id);
          }
        }
      }
    }
    } finally {
      if (nm) {
        await nm.unsubscribe(ch, this.onEventsChannelNotification).catch(() => {});
      }
    }
  }

  private wakeEventWaiters(): void {
    const rs = this.eventWaitResolvers;
    this.eventWaitResolvers = [];
    for (const r of rs) {
      try {
        r();
      } catch {
        /* ignore */
      }
    }
  }

  private waitForEventNotification(
    nm: NotificationManager | undefined,
    timeoutMs: number,
  ): Promise<void> {
    if (!nm) {
      return new Promise(r => setTimeout(r, Math.min(timeoutMs, 1000)));
    }
    let settled = false;
    return new Promise(resolve => {
      const timer = setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        this.eventWaitResolvers = this.eventWaitResolvers.filter(x => x !== wrapped);
        resolve();
      }, timeoutMs);
      const wrapped = () => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        resolve();
      };
      this.eventWaitResolvers.push(wrapped);
    });
  }

  /**
   * Stops consuming events and close the underlying Redis connection if necessary.
   *
   * @returns
   */
  async close(): Promise<void> {
    if (!this.closing) {
      this.closing = (async () => {
        try {
          // Wake any pending event waiters so the consumeEvents loop can see
          // this.closing on its next iteration instead of waiting the full
          // blockingTimeout for a timer to fire.
          this.wakeEventWaiters();
          // As the connection has been wrongly markes as "shared" by QueueBase,
          // we need to forcibly close it here. We should fix QueueBase to avoid this in the future.
          await this.connection.close(this.blocking);
        } finally {
          this.closed = true;
        }
      })();
    }
    return this.closing;
  }
}
