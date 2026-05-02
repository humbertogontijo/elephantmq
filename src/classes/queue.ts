import { v4 } from 'uuid';
import type { Pool, PoolClient } from 'pg';
import {
  BaseJobOptions,
  BulkJobOptions,
  EmqClient,
  EmqConnectionListener,
  JobSchedulerJson,
  MinimalQueue,
  QueueOptions,
  RepeatOptions,
} from '../interfaces';
import {
  FinishedStatus,
  JobsOptions,
  JobSchedulerTemplateOptions,
  JobProgress,
} from '../types';
import { Job } from './job';
import { QueueGetters } from './queue-getters';
import { PgPoolConnection } from './pg-connection';
import { SpanKind, TelemetryAttributes } from '../enums';
import { JobScheduler } from './job-scheduler';
import { escapeSchema } from './queue-identity';
import { libName, version } from '../version';
import { schedulePeriodicTrim } from './events-trimmer';
import { mergeBulkJobTelemetry } from './queue/queue-bulk-telemetry';

export interface ObliterateOpts {
  /**
   * Use force = true to force obliteration even with active jobs in the queue
   * @defaultValue false
   */
  force?: boolean;
  /**
   * Use count with the maximum number of deleted keys per iteration
   * @defaultValue 1000
   */
  count?: number;
}

export interface QueueListener<
  JobBase extends Job = Job,
> extends EmqConnectionListener {
  /**
   * Listen to 'cleaned' event.
   *
   * This event is triggered when the queue calls clean method.
   */
  cleaned: (jobs: string[], type: string) => void;

  /**
   * Listen to 'error' event.
   *
   * This event is triggered when an error is thrown.
   */
  error: (err: Error) => void;

  /**
   * Listen to 'paused' event.
   *
   * This event is triggered when the queue is paused.
   */
  paused: () => void;

  /**
   * Listen to 'progress' event.
   *
   * This event is triggered when the job updates its progress.
   */
  progress: (jobId: string, progress: JobProgress) => void;

  /**
   * Listen to 'removed' event.
   *
   * This event is triggered when a job is removed.
   */
  removed: (jobId: string) => void;

  /**
   * Listen to 'resumed' event.
   *
   * This event is triggered when the queue is resumed.
   */
  resumed: () => void;

  /**
   * Listen to 'waiting' event.
   *
   * This event is triggered when the queue creates a new job.
   */
  waiting: (job: JobBase) => void;
}

/**
 * IsAny<T> A type helper to determine if a given type `T` is `any`.
 * This works by using `any` type with the intersection
 * operator (`&`). If `T` is `any`, then `1 & T` resolves to `any`, and since `0`
 * is assignable to `any`, the conditional type returns `true`.
 */
type IsAny<T> = 0 extends 1 & T ? true : false;
// Helper for JobBase type
type JobBase<T, ResultType, NameType extends string> =
  IsAny<T> extends true
    ? Job<T, ResultType, NameType>
    : T extends Job<any, any, any>
      ? T
      : Job<T, ResultType, NameType>;

// Helper types to extract DataType, ResultType, and NameType
type ExtractDataType<DataTypeOrJob, Default> =
  DataTypeOrJob extends Job<infer D, any, any> ? D : Default;

type ExtractResultType<DataTypeOrJob, Default> =
  DataTypeOrJob extends Job<any, infer R, any> ? R : Default;

type ExtractNameType<DataTypeOrJob, Default extends string> =
  DataTypeOrJob extends Job<any, any, infer N> ? N : Default;

/**
 * Queue
 *
 * This class provides methods to add jobs to a queue and some other high-level
 * administration such as pausing or deleting queues.
 *
 * @typeParam DataType - The type of the data that the job will process.
 * @typeParam ResultType - The type of the result of the job.
 * @typeParam NameType - The type of the name of the job.
 *
 * @example
 *
 * ```typescript
 * import { Queue } from 'elephantmq';
 *
 * interface MyDataType {
 *  foo: string;
 * }
 *
 * interface MyResultType {
 *   bar: string;
 * }
 *
 * const queue = new Queue<MyDataType, MyResultType, "blue" | "brown">('myQueue');
 * ```
 */
export class Queue<
  DataTypeOrJob = any,
  DefaultResultType = any,
  DefaultNameType extends string = string,
  DataType = ExtractDataType<DataTypeOrJob, DataTypeOrJob>,
  ResultType = ExtractResultType<DataTypeOrJob, DefaultResultType>,
  NameType extends string = ExtractNameType<DataTypeOrJob, DefaultNameType>,
> extends QueueGetters<JobBase<DataTypeOrJob, ResultType, NameType>> {
  token = v4();
  jobsOpts: BaseJobOptions;
  opts!: QueueOptions;

  protected libName = libName;

  protected _jobScheduler?: JobScheduler;

  private periodicEventsTrim?: { stop: () => void };

  /**
   * Resolved once the queue row has been stamped with `max_len_events` and
   * `settings.version`. `waitUntilReady` folds this in so callers that rely
   * on the metas being persisted (e.g. `events.test.ts` trimming tests) see
   * consistent state before any `add()`/`remove()` mutation. Async BullMQ
   * mirrors this via its `HMSET` inside `addStandardJob-9.lua`; elephantmq
   * stores the config on the queue row, so we need to UPDATE after INSERT.
   */
  private metasUpdate?: Promise<void>;

  constructor(
    name: string,
    opts?: QueueOptions,
    Connection?: typeof PgPoolConnection,
  ) {
    if (!opts?.connection) {
      throw new Error('Queue requires a connection option');
    }

    super(name, opts, Connection);

    this.jobsOpts = opts?.defaultJobOptions ?? {};

    if (!opts?.skipMetasUpdate) {
      this.metasUpdate = (async () => {
        const client = await this.client;
        if (this.closing) {
          return;
        }
        const qid = await this.queueId;
        const S = escapeSchema(this.schema);
        const mv = this.metaValues;
        await client.query(
          `update ${S}.emq_queues set
             max_len_events = $2::int,
             settings = coalesce(settings, '{}'::jsonb) || $3::jsonb,
             updated_at = now()
           where id = $1`,
          [
            qid,
            Number(mv['opts.maxLenEvents'] ?? 10000),
            JSON.stringify({ version: String(mv['version']) }),
          ],
        );
      })().catch(() => {
        // Swallow so `waitUntilReady` rejection isn't surfaced for transient
        // issues; failures still surface via 'error' events if any are set.
      });
    }

    const trimEvery = opts?.streams?.events?.trim?.every;
    if (trimEvery && trimEvery > 0) {
      void this.waitUntilReady().then(() => {
        if (!this.closing) {
          this.periodicEventsTrim = schedulePeriodicTrim(this, trimEvery);
        }
      });
    }
  }

  /**
   * Override base to fold the async metas UPDATE (max_len_events, settings)
   * into the ready barrier. This avoids a race where the first `add` or
   * `addBulk` call inserts events before `max_len_events` is stamped on the
   * queue row (see `tests/events.test.ts`, jobs removal / trim threshold).
   */
  async waitUntilReady(): Promise<EmqClient> {
    const client = await super.waitUntilReady();
    if (this.metasUpdate) {
      await this.metasUpdate;
    }
    return client;
  }

  emit<U extends keyof QueueListener<JobBase<DataType, ResultType, NameType>>>(
    event: U,
    ...args: Parameters<
      QueueListener<JobBase<DataType, ResultType, NameType>>[U]
    >
  ): boolean {
    return super.emit(event, ...args);
  }

  off<U extends keyof QueueListener<JobBase<DataType, ResultType, NameType>>>(
    eventName: U,
    listener: QueueListener<JobBase<DataType, ResultType, NameType>>[U],
  ): this {
    super.off(eventName, listener);
    return this;
  }

  on<U extends keyof QueueListener<JobBase<DataType, ResultType, NameType>>>(
    event: U,
    listener: QueueListener<JobBase<DataType, ResultType, NameType>>[U],
  ): this {
    super.on(event, listener);
    return this;
  }

  once<U extends keyof QueueListener<JobBase<DataType, ResultType, NameType>>>(
    event: U,
    listener: QueueListener<JobBase<DataType, ResultType, NameType>>[U],
  ): this {
    super.once(event, listener);
    return this;
  }

  /**
   * Returns this instance current default job options.
   */
  get defaultJobOptions(): JobsOptions {
    return { ...this.jobsOpts };
  }

  get metaValues(): Record<string, string | number> {
    return {
      'opts.maxLenEvents': this.opts?.streams?.events?.maxLen ?? 10000,
      version: `${this.libName}:${version}`,
    };
  }

  /**
   * Get library version.
   *
   * @returns the content of the meta.library field.
   */
  async getVersion(): Promise<string> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [row],
    } = await client.query<{ v: string | null }>(
      `select settings->>'version' as v from ${S}.emq_queues where id = $1`,
      [qid],
    );
    return row?.v ?? `${this.libName}:${version}`;
  }

  get jobScheduler(): Promise<JobScheduler> {
    return new Promise<JobScheduler>(async resolve => {
      if (!this._jobScheduler) {
        this._jobScheduler = new JobScheduler(this.name, {
          ...this.opts,
          connection: await this.client,
        });
        this._jobScheduler.on('error', this.emit.bind(this, 'error'));
      }
      resolve(this._jobScheduler);
    });
  }

  /**
   * Enable and set global concurrency value.
   * @param concurrency - Maximum number of simultaneous jobs that the workers can handle.
   * For instance, setting this value to 1 ensures that no more than one job
   * is processed at any given time. If this limit is not defined, there will be no
   * restriction on the number of concurrent jobs.
   */
  async setGlobalConcurrency(concurrency: number) {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    await client.query(
      `update ${S}.emq_queues set concurrency = $2, updated_at = now() where id = $1`,
      [qid, concurrency],
    );
  }

  /**
   * Enable and set rate limit.
   * @param max - Max number of jobs to process in the time period specified in `duration`
   * @param duration - Time in milliseconds. During this time, a maximum of `max` jobs will be processed.
   */
  async setGlobalRateLimit(max: number, duration: number) {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    await client.query(
      `update ${S}.emq_queues set rate_limit_max = $2, rate_limit_duration_ms = $3, updated_at = now()
       where id = $1`,
      [qid, max, duration],
    );
  }

  /**
   * Remove global concurrency value.
   */
  async removeGlobalConcurrency() {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    await client.query(
      `update ${S}.emq_queues set concurrency = null, updated_at = now() where id = $1`,
      [qid],
    );
  }

  /**
   * Remove global rate limit values.
   */
  async removeGlobalRateLimit() {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    await client.query(
      `update ${S}.emq_queues set rate_limit_max = null, rate_limit_duration_ms = null, updated_at = now()
       where id = $1`,
      [qid],
    );
  }

  /**
   * Adds a new job to the queue.
   *
   * @param name - Name of the job to be added to the queue.
   * @param data - Arbitrary data to append to the job.
   * @param opts - Job options that affects how the job is going to be processed.
   */
  async add(
    name: NameType,
    data: DataType,
    opts?: JobsOptions,
  ): Promise<Job<DataType, ResultType, NameType>> {
    return this.trace<Job<DataType, ResultType, NameType>>(
      SpanKind.PRODUCER,
      'add',
      `${this.name}.${name}`,
      async (span, srcPropagationMetadata) => {
        if (srcPropagationMetadata && !opts?.telemetry?.omitContext) {
          const telemetry = {
            metadata: srcPropagationMetadata,
          };
          opts = { ...opts, telemetry };
        }

        // Make sure stream-config (max_len_events) lands on the queue
        // row before any event-emitting mutation. Otherwise the first
        // emit_event sees the default 10000 cap and never trims —
        // observed as `events.test.ts > should trim events so its
        // length is at least the threshold` returning 400 events
        // when maxLen is set to 20 by the test.
        if (this.metasUpdate) {
          await this.metasUpdate;
        }

        const job = await this.addJob(name, data, opts);

        span?.setAttributes({
          [TelemetryAttributes.JobName]: name,
          [TelemetryAttributes.JobId]: job.id,
        });

        return job;
      },
    );
  }

  /**
   * addJob is a telemetry free version of the add method, useful in order to wrap it
   * with custom telemetry on subclasses.
   *
   * @param name - Name of the job to be added to the queue.
   * @param data - Arbitrary data to append to the job.
   * @param opts - Job options that affects how the job is going to be processed.
   *
   * @returns Job
   */
  protected async addJob(
    name: NameType,
    data: DataType,
    opts?: JobsOptions,
  ): Promise<Job<DataType, ResultType, NameType>> {
    if (opts?.repeat) {
      throw new Error(
        'add() with repeat options is no longer supported. Use queue.upsertJobScheduler(...) instead.',
      );
    }

    const jobId = opts?.jobId;

    if (jobId == '0' || jobId?.startsWith('0:')) {
      throw new Error("JobId cannot be '0' or start with 0:");
    }

    const mergedOpts = {
      ...this.jobsOpts,
      ...opts,
      jobId,
    };

    const job = await this.Job.create<DataType, ResultType, NameType>(
      this as MinimalQueue,
      name,
      data,
      mergedOpts,
    );
    this.emit('waiting', job as JobBase<DataType, ResultType, NameType>);

    return job;
  }

  /**
   * Adds an array of jobs to the queue. This method may be faster than adding
   * one job at a time in a sequence.
   *
   * @param jobs - The array of jobs to add to the queue. Each job is defined by 3
   * properties, 'name', 'data' and 'opts'. They follow the same signature as 'Queue.add'.
   */
  async addBulk(
    jobs: { name: NameType; data: DataType; opts?: BulkJobOptions }[],
  ): Promise<Job<DataType, ResultType, NameType>[]> {
    return this.trace<Job<DataType, ResultType, NameType>[]>(
      SpanKind.PRODUCER,
      'addBulk',
      this.name,
      async (span, srcPropagationMetadata) => {
        if (span) {
          span.setAttributes({
            [TelemetryAttributes.BulkNames]: jobs.map(job => job.name),
            [TelemetryAttributes.BulkCount]: jobs.length,
          });
        }

        // See `add()` — guarantee max_len_events is stamped before
        // any event-emitting mutation runs.
        if (this.metasUpdate) {
          await this.metasUpdate;
        }

        return await this.Job.createBulk<DataType, ResultType, NameType>(
          this as MinimalQueue,
          jobs.map(job => {
            const telemetry = mergeBulkJobTelemetry(
              job.opts,
              srcPropagationMetadata,
            );

            const mergedOpts = {
              ...this.jobsOpts,
              ...job.opts,
              jobId: job.opts?.jobId,
              telemetry,
            };

            return {
              name: job.name,
              data: job.data,
              opts: mergedOpts,
            };
          }),
        );
      },
    );
  }

  /**
   * Adds jobs in a single PostgreSQL transaction (all-or-nothing).
   * Same behavior as `addBulk`: `Job.createBulk` already wraps inserts in
   * `BEGIN`/`COMMIT` on a pooled connection. This name makes the atomicity
   * guarantee explicit for PostgreSQL callers.
   */
  async addBulkAtomic(
    jobs: { name: NameType; data: DataType; opts?: BulkJobOptions }[],
  ): Promise<Job<DataType, ResultType, NameType>[]> {
    return this.trace<Job<DataType, ResultType, NameType>[]>(
      SpanKind.PRODUCER,
      'addBulkAtomic',
      this.name,
      async (span, srcPropagationMetadata) => {
        if (span) {
          span.setAttributes({
            [TelemetryAttributes.BulkNames]: jobs.map(job => job.name),
            [TelemetryAttributes.BulkCount]: jobs.length,
          });
        }

        return await this.Job.createBulk<DataType, ResultType, NameType>(
          this as MinimalQueue,
          jobs.map(job => {
            const telemetry = mergeBulkJobTelemetry(
              job.opts,
              srcPropagationMetadata,
            );

            const mergedOpts = {
              ...this.jobsOpts,
              ...job.opts,
              jobId: job.opts?.jobId,
              telemetry,
            };

            return {
              name: job.name,
              data: job.data,
              opts: mergedOpts,
            };
          }),
        );
      },
    );
  }

  /**
   * Runs `fn` with this queue's SQL commands pinned to one connection and a
   * single transaction. Use the same `PoolClient` for your own statements so
   * business data and enqueued jobs commit together.
   *
   * Nested `inTransaction` on the same queue instance is not supported.
   *
   * @param fn - Receives this queue and the pinned `PoolClient` for arbitrary SQL.
   */
  async inTransaction<R>(
    fn: (queue: this, sql: PoolClient) => Promise<R>,
  ): Promise<R> {
    if (this.transactionClient) {
      throw new Error('Nested elephantmq Queue.inTransaction is not supported');
    }
    const pool = (await this.client) as Pool;
    const conn = await pool.connect();
    try {
      await conn.query('BEGIN');
      this.transactionClient = conn;
      try {
        const result = await fn(this, conn);
        await conn.query('COMMIT');
        return result;
      } catch (err) {
        try {
          await conn.query('ROLLBACK');
        } catch {
          /* ignore */
        }
        throw err;
      } finally {
        this.transactionClient = null;
      }
    } finally {
      conn.release();
    }
  }

  /**
   * Upserts a scheduler.
   *
   * A scheduler is a job factory that creates jobs at a given interval.
   * Upserting a scheduler will create a new job scheduler or update an existing one.
   * It will also create the first job based on the repeat options and delayed accordingly.
   *
   * @param key - Unique key for the repeatable job meta.
   * @param repeatOpts - Repeat options
   * @param jobTemplate - Job template. If provided it will be used for all the jobs
   * created by the scheduler.
   *
   * @returns The next job to be scheduled (would normally be in delayed state).
   */
  async upsertJobScheduler(
    jobSchedulerId: NameType,
    repeatOpts: Omit<RepeatOptions, 'key'>,
    jobTemplate?: {
      name?: NameType;
      data?: DataType;
      opts?: JobSchedulerTemplateOptions;
    },
  ) {
    if (repeatOpts.endDate) {
      if (+new Date(repeatOpts.endDate) < Date.now()) {
        throw new Error('End date must be greater than current timestamp');
      }
    }

    return (await this.jobScheduler).upsertJobScheduler<
      DataType,
      ResultType,
      NameType
    >(
      jobSchedulerId,
      repeatOpts,
      jobTemplate?.name ?? jobSchedulerId,
      jobTemplate?.data ?? <DataType>{},
      { ...this.jobsOpts, ...jobTemplate?.opts },
      { override: true },
    );
  }

  /**
   * Pauses the processing of this queue globally (`emq_queues.paused`).
   */
  async pause(): Promise<void> {
    await this.trace<void>(SpanKind.INTERNAL, 'pause', this.name, async () => {
      await this.scripts.pause(true);

      this.emit('paused');
    });
  }

  /**
   * Close the queue instance.
   *
   */
  async close(): Promise<void> {
    await this.trace<void>(SpanKind.INTERNAL, 'close', this.name, async () => {
      this.periodicEventsTrim?.stop();
      this.periodicEventsTrim = undefined;
      await super.close();
    });
  }

  /**
   * Overrides the rate limit to be active for the next jobs.
   *
   * @param expireTimeMs - expire time in ms of this rate limit.
   */
  async rateLimit(expireTimeMs: number): Promise<void> {
    await this.trace<void>(
      SpanKind.INTERNAL,
      'rateLimit',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.QueueRateLimit]: expireTimeMs,
        });

        await this.client.then(async client => {
          const qid = await this.queueId;
          const S = escapeSchema(this.schema);
          await client.query(
            `insert into ${S}.emq_rate_limit_state (queue_id, tokens, expires_at)
             values ($1, $2, now() + ($3::bigint * interval '1 millisecond'))
             on conflict (queue_id) do update set
               tokens = $2,
               expires_at = now() + ($3::bigint * interval '1 millisecond')`,
            [qid, Number.MAX_SAFE_INTEGER, expireTimeMs],
          );
        });
      },
    );
  }

  /**
   * Resumes the processing of this queue globally.
   *
   * The method reverses the pause operation by resuming the processing of the
   * queue.
   */
  async resume(): Promise<void> {
    await this.trace<void>(SpanKind.INTERNAL, 'resume', this.name, async () => {
      await this.scripts.pause(false);

      this.emit('resumed');
    });
  }

  /**
   * Returns true if the queue is currently paused.
   */
  async isPaused(): Promise<boolean> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [row],
    } = await client.query<{ p: boolean }>(
      `select paused as p from ${S}.emq_queues where id = $1`,
      [qid],
    );
    return !!row?.p;
  }

  /**
   * Returns true if the queue is currently maxed.
   */
  isMaxed(): Promise<boolean> {
    return this.scripts.isMaxed();
  }

  /**
   * Get Job Scheduler by id
   *
   * @param id - identifier of scheduler.
   */
  async getJobScheduler(
    id: string,
  ): Promise<JobSchedulerJson<DataType> | undefined> {
    return (await this.jobScheduler).getScheduler<DataType>(id);
  }

  /**
   * Get all Job Schedulers
   *
   * @param start - Offset of first scheduler to return.
   * @param end - Offset of last scheduler to return.
   * @param asc - Determine the order in which schedulers are returned based on their
   * next execution time.
   */
  async getJobSchedulers(
    start?: number,
    end?: number,
    asc?: boolean,
  ): Promise<JobSchedulerJson<DataType>[]> {
    return (await this.jobScheduler).getJobSchedulers<DataType>(
      start,
      end,
      asc,
    );
  }

  /**
   *
   * Get the number of job schedulers.
   *
   * @returns The number of job schedulers.
   */
  async getJobSchedulersCount(): Promise<number> {
    return (await this.jobScheduler).getSchedulersCount();
  }

  /**
   *
   * Removes a job scheduler.
   *
   * @param jobSchedulerId - identifier of the job scheduler.
   *
   * @returns
   */
  async removeJobScheduler(jobSchedulerId: string): Promise<boolean> {
    const jobScheduler = await this.jobScheduler;
    const removed = await jobScheduler.removeJobScheduler(jobSchedulerId);

    return !removed;
  }

  /**
   * Removes a deduplication key.
   *
   * @param id - identifier
   */
  async removeDeduplicationKey(id: string): Promise<number> {
    return this.trace<number>(
      SpanKind.INTERNAL,
      'removeDeduplicationKey',
      `${this.name}`,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.DeduplicationKey]: id,
        });

        const client = await this.client;
        const qid = await this.queueId;
        const S = escapeSchema(this.schema);
        const r = await client.query(
          `delete from ${S}.emq_deduplication where queue_id = $1 and dedup_id = $2`,
          [qid, id],
        );
        return r.rowCount ?? 0;
      },
    );
  }

  /**
   * Removes rate limit key.
   */
  async removeRateLimitKey(): Promise<number> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const r = await client.query(
      `delete from ${S}.emq_rate_limit_state where queue_id = $1`,
      [qid],
    );
    return r.rowCount ?? 0;
  }

  /**
   * Removes the given job from the queue as well as all its
   * dependencies.
   *
   * @param jobId - The id of the job to remove
   * @param opts - Options to remove a job
   * @returns 1 if it managed to remove the job or 0 if the job or
   * any of its dependencies were locked.
   */
  async remove(jobId: string, { removeChildren = true } = {}): Promise<number> {
    return this.trace<number>(
      SpanKind.INTERNAL,
      'remove',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.JobId]: jobId,
          [TelemetryAttributes.JobOptions]: JSON.stringify({
            removeChildren,
          }),
        });

        const code = await this.scripts.remove(jobId, removeChildren);

        if (code === 1) {
          this.emit('removed', jobId);
        }

        return code;
      },
    );
  }

  /**
   * Updates the given job's progress.
   *
   * @param jobId - The id of the job to update
   * @param progress - Number or object to be saved as progress.
   */
  async updateJobProgress(jobId: string, progress: JobProgress): Promise<void> {
    await this.trace<void>(
      SpanKind.INTERNAL,
      'updateJobProgress',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.JobId]: jobId,
          [TelemetryAttributes.JobProgress]: JSON.stringify(progress),
        });

        await this.scripts.updateProgress(jobId, progress);

        this.emit('progress', jobId, progress);
      },
    );
  }

  /**
   * Logs one row of job's log data.
   *
   * @param jobId - The job id to log against.
   * @param logRow - String with log data to be logged.
   * @param keepLogs - Max number of log entries to keep (0 for unlimited).
   *
   * @returns The total number of log entries for this job so far.
   */
  async addJobLog(
    jobId: string,
    logRow: string,
    keepLogs?: number,
  ): Promise<number> {
    return Job.addJobLog(this, jobId, logRow, keepLogs);
  }

  /**
   * Drains the queue, i.e., removes all jobs that are waiting
   * or delayed, but not active, completed or failed.
   *
   * @param delayed - Pass true if it should also clean the
   * delayed jobs.
   */
  async drain(delayed = false): Promise<void> {
    await this.trace<void>(
      SpanKind.INTERNAL,
      'drain',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.QueueDrainDelay]: delayed,
        });

        await this.scripts.drain(delayed);
      },
    );
  }

  /**
   * Cleans jobs from a queue. Similar to drain but keeps jobs within a certain
   * grace period.
   *
   * @param grace - The grace period in milliseconds
   * @param limit - Max number of jobs to clean
   * @param type - The type of job to clean
   * Possible values are completed, wait, active, paused, delayed, failed. Defaults to completed.
   * @returns Id jobs from the deleted records
   */
  async clean(
    grace: number,
    limit: number,
    type:
      | 'completed'
      | 'wait'
      | 'waiting'
      | 'active'
      | 'paused'
      | 'prioritized'
      | 'delayed'
      | 'failed' = 'completed',
  ): Promise<string[]> {
    return this.trace<string[]>(
      SpanKind.INTERNAL,
      'clean',
      this.name,
      async span => {
        const maxCount = limit || Infinity;
        const maxCountPerCall = Math.min(10000, maxCount);
        const timestamp = Date.now() - grace;
        let deletedCount = 0;
        const deletedJobsIds: string[] = [];

        // Normalize 'waiting' to 'wait' for consistency with internal Redis keys
        const normalizedType = type === 'waiting' ? 'wait' : type;

        while (deletedCount < maxCount) {
          const jobsIds = await this.scripts.cleanJobsInSet(
            normalizedType,
            timestamp,
            maxCountPerCall,
          );

          this.emit('cleaned', jobsIds, normalizedType);
          deletedCount += jobsIds.length;
          deletedJobsIds.push(...jobsIds);

          if (jobsIds.length < maxCountPerCall) {
            break;
          }
        }

        span?.setAttributes({
          [TelemetryAttributes.QueueGrace]: grace,
          [TelemetryAttributes.JobType]: type,
          [TelemetryAttributes.QueueCleanLimit]: maxCount,
          [TelemetryAttributes.JobIds]: deletedJobsIds,
        });

        return deletedJobsIds;
      },
    );
  }

  /**
   * Completely destroys the queue and all of its contents irreversibly.
   * This method will *pause* the queue and requires that there are no
   * active jobs. It is possible to bypass this requirement, i.e. not
   * having active jobs using the "force" option.
   *
   * Note: This operation requires to iterate on all the jobs stored in the queue
   * and can be slow for very large queues.
   *
   * @param opts - Obliterate options.
   */
  async obliterate(opts?: ObliterateOpts): Promise<void> {
    await this.trace<void>(
      SpanKind.INTERNAL,
      'obliterate',
      this.name,
      async () => {
        await this.pause();

        let cursor = 0;
        do {
          cursor = await this.scripts.obliterate({
            force: false,
            count: 1000,
            ...opts,
          });
        } while (cursor);
      },
    );
  }

  /**
   * Retry all the failed or completed jobs.
   *
   * @param opts - An object with the following properties:
   *   - count  number to limit how many jobs will be moved to wait status per iteration,
   *   - state  failed by default or completed.
   *   - timestamp from which timestamp to start moving jobs to wait status, default Date.now().
   *
   * @returns
   */
  async retryJobs(
    opts: { count?: number; state?: FinishedStatus; timestamp?: number } = {},
  ): Promise<void> {
    await this.trace<void>(
      SpanKind.PRODUCER,
      'retryJobs',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.QueueOptions]: JSON.stringify(opts),
        });

        let cursor = 0;
        do {
          cursor = await this.scripts.retryJobs(
            opts.state,
            opts.count,
            opts.timestamp,
          );
        } while (cursor);
      },
    );
  }

  /**
   * Promote all the delayed jobs.
   *
   * @param opts - An object with the following properties:
   *   - count  number to limit how many jobs will be moved to wait status per iteration
   *
   * @returns
   */
  async promoteJobs(opts: { count?: number } = {}): Promise<void> {
    await this.trace<void>(
      SpanKind.INTERNAL,
      'promoteJobs',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.QueueOptions]: JSON.stringify(opts),
        });

        let cursor = 0;
        do {
          cursor = await this.scripts.promoteJobs(opts.count);
        } while (cursor);
      },
    );
  }

  /**
   * Trim the event stream to an approximately maxLength.
   *
   * @param maxLength -
   */
  async trimEvents(maxLength: number): Promise<number> {
    return this.trace<number>(
      SpanKind.INTERNAL,
      'trimEvents',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.QueueEventMaxLength]: maxLength,
        });

        const client = await this.client;
        const qid = await this.queueId;
        const S = escapeSchema(this.schema);
        const {
          rows: [row],
        } = await client.query<{ m: string | null }>(
          `select max(id)::text as m from ${S}.emq_events where queue_id = $1`,
          [qid],
        );
        if (!row?.m) {
          return 0;
        }
        // Redis XTRIM MAXLEN 0 drops the whole stream, not "keep one"; use
        // `<= maxId` so maxLength=0 wipes everything, matching BullMQ tests.
        const maxId = parseInt(row.m, 10);
        const threshold = maxId - maxLength;
        const r = await client.query(
          `delete from ${S}.emq_events where queue_id = $1 and id <= $2::bigint`,
          [qid, String(threshold)],
        );
        return r.rowCount ?? 0;
      },
    );
  }

  /**
   * Removes **orphan** job rows in this queue: rows where `state` is `NULL`.
   *
   * Under normal operation every job has a non-null state (`wait`, `active`, …).
   * A null state can only appear after manual SQL, a failed migration, or a
   * historic bug — the row is not reachable by workers and should be deleted.
   *
   * BullMQ scanned Redis key names and passed candidates to a Lua script; we
   * select `job_id` batches with `state is null` and delete them atomically via
   * `emq_remove_orphaned_jobs_v1`.
   *
   * @param batchSize - Max job ids per SQL call (default 1000).
   * @param limit - Max orphan job **ids** processed in total (`0` = unlimited).
   *   Matches returned count from the SQL function (one row → at most one id).
   * @returns The number of orphan ids returned (same as rows deleted for these batches).
   */
  async removeOrphanedJobs(batchSize = 1000, limit = 0): Promise<number> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const bs = Math.max(1, Math.floor(batchSize));
    let total = 0;
    let safety = 0;
    const maxIterations = 10_000;

    while (safety++ < maxIterations) {
      const remaining =
        limit > 0 ? Math.max(0, limit - total) : bs;
      if (limit > 0 && remaining <= 0) {
        break;
      }
      const take = limit > 0 ? Math.min(bs, remaining) : bs;

      const { rows } = await client.query<{ job_id: string }>(
        `select job_id from ${S}.emq_jobs
          where queue_id = $1::bigint and state is null
          limit $2::int`,
        [String(qid), take],
      );

      if (rows.length === 0) {
        break;
      }

      const ids = rows.map(r => r.job_id);
      const orphanIds = await this.scripts.removeOrphanedJobs(ids);
      total += orphanIds.length;

      if (limit > 0 && total >= limit) {
        break;
      }
    }

    return total;
  }

  /**
   * Delete old priority helper key.
   */
  async removeDeprecatedPriorityKey(): Promise<number> {
    return 0;
  }
}
