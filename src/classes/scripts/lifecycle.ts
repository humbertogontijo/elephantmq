import type {
  EmqClient,
  MinimalJob,
  MoveToDelayedOpts,
  MoveToWaitingChildrenOpts,
  RetryJobOpts,
  RetryOptions,
  WorkerOptions,
} from '../../interfaces';
import type {
  FinishedPropValAttribute,
  FinishedStatus,
  KeepJobs,
} from '../../types';
import { ErrorCode } from '../../enums';
import {
  jsonJobRowFromDb,
  rowToFlatJobFields,
} from '../emq-mappers';
import { AddJobsScripts } from './add-jobs';
import {
  MoveToFinishedParams,
  extractFailureFields,
  raw2NextJobData,
} from './helpers';

/**
 * Job lifecycle: claim, finish, retry, promote, stalled recovery, lock
 * extension. Methods here transition rows between `wait`, `active`,
 * `delayed`, `waiting-children`, `completed`, and `failed`.
 */
export class LifecycleScripts extends AddJobsScripts {
  async extendLock(
    jobId: string,
    token: string,
    duration: number,
    client?: EmqClient,
  ): Promise<number> {
    const c = client || (await this.queue.client);
    const {
      rows: [r],
    } = await c.query<{ r: number }>(
      `select ${this.S()}.emq_extend_lock_v1($1::bigint, $2::text, $3::text, $4::bigint) as r`,
      [await this.qid(), jobId, token, duration],
    );
    return r?.r ?? -2;
  }

  async extendLocks(
    jobIds: string[],
    tokens: string[],
    duration: number,
  ): Promise<string[]> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ r: string[] }>(
      `select ${this.S()}.emq_extend_locks_v1($1::bigint, $2::text[], $3::text[], $4::bigint) as r`,
      [await this.qid(), jobIds, tokens, duration],
    );
    return r?.r ?? [];
  }

  protected getKeepJobs(
    shouldRemove: undefined | boolean | number | KeepJobs,
    workerKeepJobs: undefined | KeepJobs,
  ): KeepJobs | undefined {
    if (typeof shouldRemove === 'undefined') {
      return workerKeepJobs || { count: shouldRemove ? 0 : -1 };
    }
    return typeof shouldRemove === 'object'
      ? shouldRemove
      : typeof shouldRemove === 'number'
        ? { count: shouldRemove }
        : { count: shouldRemove ? 0 : -1 };
  }

  protected buildMoveToFinishedParams<
    T = any,
    R = any,
    N extends string = string,
  >(
    job: MinimalJob<T, R, N>,
    val: any,
    propVal: FinishedPropValAttribute,
    shouldRemove: undefined | boolean | number | KeepJobs,
    target: FinishedStatus,
    token: string,
    timestamp: number,
    fetchNext = true,
    fieldsToUpdate?: Record<string, any>,
  ): MoveToFinishedParams {
    const opts = this.queue.opts as WorkerOptions;
    const workerKeepJobs =
      target === 'completed' ? opts.removeOnComplete : opts.removeOnFail;
    const k = this.getKeepJobs(shouldRemove, workerKeepJobs);
    let keepJobsCount: number | null = null;
    let keepJobsAgeMs: number | null = null;
    if (k && typeof k === 'object') {
      if (typeof k.count === 'number') {
        keepJobsCount = k.count < 0 ? null : k.count;
      }
      if ('age' in k && typeof (k as { age?: number }).age === 'number') {
        keepJobsAgeMs = (k as { age: number }).age * 1000;
      }
    }
    const failureFields = extractFailureFields(
      fieldsToUpdate as { failedReason?: string; stacktrace?: string },
    );
    // Pull metrics.maxDataPoints from the worker opts so the SQL function can
    // mirror BullMQ's collectMetrics() side-effect inside moveToFinished.
    // Only present on Worker opts; Queue opts won't have it, in which case
    // we pass null and the server skips the collection entirely.
    const metricsMax =
      typeof opts.metrics?.maxDataPoints === 'number'
        ? (opts.metrics?.maxDataPoints ?? null)
        : null;
    return {
      jobId: String(job.id),
      timestamp,
      token,
      target,
      val,
      propVal,
      fetchNext: !!(fetchNext && !this.queue.closing),
      lockDurationMs: opts.lockDuration ?? 30000,
      keepJobsCount,
      keepJobsAgeMs,
      failedReason: failureFields.failedReason,
      stacktrace: failureFields.stacktrace,
      maxMetricsSize: metricsMax,
    };
  }

  async moveToFinished(jobId: string, params: MoveToFinishedParams) {
    const client = await this.queue.client;
    const S = this.S();
    const target = params.target;
    let returnJson: unknown = null;
    if (target === 'completed') {
      const val = params.val;
      if (val !== undefined && val !== null && val !== 'null') {
        returnJson = typeof val === 'string' ? val : JSON.stringify(val);
      }
    }
    const failedReason =
      target === 'failed'
        ? (params.failedReason ?? String(params.val))
        : (params.failedReason ?? null);
    const stacktrace =
      params.stacktrace && params.stacktrace.length > 0
        ? params.stacktrace
        : null;
    const qOpts = this.queue.opts as WorkerOptions;
    const lim = qOpts?.limiter;
    const limMax = lim?.max ?? null;
    const limDur = lim?.duration ?? null;
    const {
      rows: [row],
    } = await client.query<{
      err_code: number;
      finished_job_id: string | null;
      next_job_row: unknown;
      next_job_id: string | null;
      rate_limit_delay_ms: number;
      block_until_ms: string | number;
    }>(
      `select * from ${S}.emq_move_to_finished_v1(
        $1::bigint, $2::text, $3::text, $4::text, $5::jsonb, $6::text, $7::text[], $8::boolean, $9::bigint,
        $10::int, $11::bigint, $12::bigint, $13::bigint, $14::bigint, $15::int
      )`,
      [
        await this.qid(),
        params.jobId,
        params.token,
        target,
        returnJson as unknown as string,
        failedReason,
        stacktrace,
        params.fetchNext,
        params.lockDurationMs,
        params.keepJobsCount,
        params.keepJobsAgeMs,
        limMax,
        limDur,
        // Forward the JS-side timestamp (BullMQ tests install fake timers
        // that only fake Date/setTimeout/clearTimeout on the client; the
        // postgres server has no visibility into those, so age-based trim
        // windows would never fire against fake finished_on values. Passing
        // Date.now() lets the SQL function pick a single consistent "now"
        // for both writes and the keep_age_ms comparison.)
        params.timestamp,
        params.maxMetricsSize ?? null,
      ],
    );

    if (!row || row.err_code < 0) {
      throw this.finishedErrors({
        code: row?.err_code ?? ErrorCode.JobNotExist,
        jobId: params.jobId ?? jobId,
        command: 'moveToFinished',
        state: 'active',
      });
    }
    const rateLimitDelay = row.rate_limit_delay_ms ?? 0;
    const blockUntil = Number(row.block_until_ms ?? 0);
    if (row.next_job_row && row.next_job_id) {
      const flat = rowToFlatJobFields(jsonJobRowFromDb(row.next_job_row));
      return raw2NextJobData([flat, row.next_job_id, rateLimitDelay, blockUntil]);
    }
    return raw2NextJobData([null, row.next_job_id, rateLimitDelay, blockUntil]);
  }

  moveToCompletedArgs<T = any, R = any, N extends string = string>(
    job: MinimalJob<T, R, N>,
    returnvalue: R,
    removeOnComplete: boolean | number | KeepJobs,
    token: string,
    fetchNext = false,
  ): MoveToFinishedParams {
    return this.buildMoveToFinishedParams(
      job,
      returnvalue,
      'returnvalue',
      removeOnComplete,
      'completed',
      token,
      Date.now(),
      fetchNext,
    );
  }

  moveToFailedArgs<T = any, R = any, N extends string = string>(
    job: MinimalJob<T, R, N>,
    failedReason: string,
    removeOnFailed: boolean | number | KeepJobs,
    token: string,
    fetchNext = false,
    fieldsToUpdate?: Record<string, any>,
  ): MoveToFinishedParams {
    return this.buildMoveToFinishedParams(
      job,
      failedReason,
      'failedReason',
      removeOnFailed,
      'failed',
      token,
      Date.now(),
      fetchNext,
      fieldsToUpdate,
    );
  }

  /**
   * Reschedule a delayed job by setting a new delay from the current time.
   * For example, calling `changeDelay(5000)` reschedules execution to 5
   * seconds from now, regardless of the original delay.
   *
   * @throws JobNotExist when the job is missing.
   * @throws JobNotInState when the job is not in the `delayed` state.
   */
  async changeDelay(jobId: string, delay: number): Promise<void> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_change_delay_v1($1::bigint, $2::text, $3::bigint, $4::bigint) as c`,
      [await this.qid(), jobId, delay, Date.now()],
    );
    const result = r?.c ?? -1;
    if (result < 0) {
      throw this.finishedErrors({
        code: result,
        jobId,
        command: 'changeDelay',
        state: 'delayed',
      });
    }
  }

  async changePriority(
    jobId: string,
    priority = 0,
    lifo = false,
  ): Promise<void> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_change_priority_v1($1::bigint, $2::text, $3::int, $4::boolean) as c`,
      [await this.qid(), jobId, priority, lifo],
    );
    const result = r?.c ?? -1;
    if (result < 0) {
      throw this.finishedErrors({
        code: result,
        jobId,
        command: 'changePriority',
      });
    }
  }

  async isMaxed(): Promise<boolean> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ m: boolean }>(
      `select ${this.S()}.emq_is_maxed_v1($1::bigint) as m`,
      [await this.qid()],
    );
    return Boolean(r?.m);
  }

  async moveToDelayed(
    jobId: string,
    timestamp: number,
    delay: number,
    token = '0',
    opts: MoveToDelayedOpts = {},
  ): Promise<void | any[]> {
    const client = await this.queue.client;
    const processAtMs = timestamp + delay;
    const { failedReason, stacktrace } = extractFailureFields(
      opts?.fieldsToUpdate,
    );
    const qOpts = this.queue.opts as WorkerOptions;
    const lim = qOpts?.limiter;
    const limMax = lim?.max ?? null;
    const limDur = lim?.duration ?? null;
    const now = Date.now();
    const lockMs = qOpts.lockDuration ?? 30000;
    const {
      rows: [r],
    } = await client.query<{
      err_code: number;
      next_job_row: unknown;
      next_job_id: string | null;
      rate_limit_delay_ms: number;
      block_until_ms: string | number;
    }>(
      `select * from ${this.S()}.emq_move_to_delayed_v1(
         $1::bigint, $2::text, $3::bigint, $4::text, $5::text, $6::text[], $7::bigint,
         $8::boolean, $9::bigint, $10::text, $11::bigint, $12::bigint, $13::bigint
       )`,
      [
        await this.qid(),
        jobId,
        processAtMs,
        token,
        failedReason,
        stacktrace,
        delay,
        !!opts?.fetchNext,
        lockMs,
        qOpts.name ?? null,
        limMax,
        limDur,
        now,
      ],
    );
    const result = r?.err_code ?? -1;
    if (result < 0) {
      throw this.finishedErrors({
        code: result,
        jobId,
        command: 'moveToDelayed',
        state: 'active',
      });
    }
    if (opts?.fetchNext) {
      if (r?.next_job_row && r?.next_job_id) {
        const flat = rowToFlatJobFields(jsonJobRowFromDb(r.next_job_row));
        return raw2NextJobData([
          flat,
          r.next_job_id,
          r.rate_limit_delay_ms ?? 0,
          Number(r.block_until_ms ?? 0),
        ]);
      }
      return raw2NextJobData([
        null,
        null,
        r?.rate_limit_delay_ms ?? 0,
        Number(r?.block_until_ms ?? 0),
      ]);
    }
    return raw2NextJobData(0 as unknown as any[]);
  }

  /**
   * Move parent job to `waiting-children` state.
   *
   * @returns true if successfully moved, false if there are still pending dependencies.
   * @throws JobNotExist | JobLockNotExist | JobNotInState
   */
  async moveToWaitingChildren(
    jobId: string,
    token: string,
    opts: MoveToWaitingChildrenOpts = {},
  ): Promise<boolean> {
    const client = await this.queue.client;
    // BullMQ's moveToWaitingChildren-7.lua accepts an optional child key
    // (`${prefix}:${queueName}:${jobId}`) so the move only happens when the
    // specific child is still a pending dep of this parent. Pass it through
    // to the SQL function so the "wait for this particular child" semantics
    // line up with the Lua implementation and the `Manually process jobs >
    // when move job to waiting-children` tests.
    const childKey = opts.child
      ? `${opts.child.queue}:${opts.child.id}`
      : null;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_move_to_waiting_children_v1($1::bigint, $2::text, $3::text, $4::text) as c`,
      [await this.qid(), jobId, token, childKey],
    );
    const result = r?.c ?? -1;
    switch (result) {
      case 0:
        return true;
      case 1:
        return false;
      default:
        throw this.finishedErrors({
          code: result,
          jobId,
          command: 'moveToWaitingChildren',
          state: 'active',
        });
    }
  }

  async retryJob(
    jobId: string,
    lifo: boolean,
    token = '0',
    opts: RetryJobOpts = {},
  ): Promise<void> {
    const client = await this.queue.client;
    const { failedReason, stacktrace } = extractFailureFields(
      opts?.fieldsToUpdate,
    );
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_retry_job_v1(
         $1::bigint, $2::text, $3::text, $4::bigint, $5::boolean, $6::text, $7::text[]
       ) as c`,
      [
        await this.qid(),
        jobId,
        token,
        Date.now(),
        lifo,
        failedReason,
        stacktrace,
      ],
    );
    const result = r?.c ?? -1;
    if (result < 0) {
      throw this.finishedErrors({
        code: result,
        jobId,
        command: 'retryJob',
        state: 'active',
      });
    }
  }

  async retryJobs(
    state: FinishedStatus = 'failed',
    count = 1000,
    timestamp = new Date().getTime(),
  ): Promise<number> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_move_jobs_to_wait_v1($1::bigint, $2::text, $3::int, $4::bigint) as c`,
      [await this.qid(), state, count, timestamp],
    );
    return r?.c ?? 0;
  }

  async promoteJobs(count = 1000): Promise<number> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_move_jobs_to_wait_v1($1::bigint, $2::text, $3::int, $4::bigint) as c`,
      [await this.qid(), 'delayed', count, Number.MAX_SAFE_INTEGER],
    );
    return r?.c ?? 0;
  }

  /**
   * Reprocess a `failed` or `completed` job by moving it back to `wait`.
   *
   * @throws JobNotExist | JobLockNotExist | JobNotInState
   */
  async reprocessJob<T = any, R = any, N extends string = string>(
    job: MinimalJob<T, R, N>,
    state: 'failed' | 'completed',
    opts: RetryOptions = {},
  ): Promise<void> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_reprocess_job_v1($1::bigint, $2::text, $3::boolean, $4::boolean, $5::text) as c`,
      [
        await this.qid(),
        job.id,
        !!opts.resetAttemptsMade,
        !!opts.resetAttemptsStarted,
        state,
      ],
    );
    const result = r?.c ?? 0;
    switch (result) {
      case 1:
        return;
      default:
        throw this.finishedErrors({
          code: result,
          jobId: job.id,
          command: 'reprocessJob',
          state,
        });
    }
  }

  async moveToActive(client: EmqClient, token: string, name?: string) {
    const opts = this.queue.opts as WorkerOptions;
    const now = Date.now();
    const lockMs = opts.lockDuration ?? 30000;
    const lim = opts.limiter;
    const limMax = lim?.max ?? null;
    const limDur = lim?.duration ?? null;
    const {
      rows: [r],
    } = await client.query<{
      out_job_row: unknown;
      out_job_id: string | null;
      rate_limit_delay_ms: number;
      block_until_ms: string | number;
    }>(
      `select * from ${this.S()}.emq_move_to_active_v1(
        $1::bigint, $2::bigint, $3::text, $4::bigint, $5::text, true,
        $6::bigint, $7::bigint
      )`,
      [await this.qid(), now, token, lockMs, name ?? null, limMax, limDur],
    );
    if (!r?.out_job_id) {
      // Must use null (not []) so raw2NextJobData does not build an empty
      // job object; [] is truthy and array2obj([]) yields {}, which becomes
      // a Job with no id.
      return raw2NextJobData([
        null,
        null,
        r?.rate_limit_delay_ms ?? 0,
        Number(r?.block_until_ms ?? 0),
      ]);
    }
    const flat = rowToFlatJobFields(jsonJobRowFromDb(r.out_job_row));
    return raw2NextJobData([
      flat,
      r.out_job_id,
      r.rate_limit_delay_ms ?? 0,
      Number(r.block_until_ms ?? 0),
    ]);
  }

  async promote(jobId: string): Promise<void> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_promote_v1($1::bigint, $2::text) as c`,
      [await this.qid(), jobId],
    );
    const code = r?.c ?? -1;
    if (code < 0) {
      throw this.finishedErrors({
        code,
        jobId,
        command: 'promote',
        state: 'delayed',
      });
    }
  }

  /**
   * Looks for unlocked jobs in the active queue.
   *
   * The job was being worked on, but the worker process died and failed to
   * renew the lock. We call these jobs 'stalled'. We resolve these by moving
   * them back to wait to be re-processed.
   */
  async moveStalledJobsToWait(): Promise<[string[], string[]]> {
    const client = await this.queue.client;
    const opts = this.queue.opts as WorkerOptions;
    const {
      rows: [r],
    } = await client.query<{ recovered_ids: string[]; failed_ids: string[] }>(
      `select * from ${this.S()}.emq_move_stalled_jobs_to_wait_v1($1::bigint, $2::int)`,
      [await this.qid(), opts.maxStalledCount],
    );
    return [r?.recovered_ids ?? [], r?.failed_ids ?? []];
  }

  async moveJobFromActiveToWait(jobId: string, token = '0') {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: string | number }>(
      `select ${this.S()}.emq_move_job_from_active_to_wait_v1($1::bigint, $2::text, $3::text) as c`,
      [await this.qid(), jobId, token],
    );
    const result = Number(r?.c ?? -1);
    if (result < 0) {
      throw this.finishedErrors({
        code: result,
        jobId,
        command: 'moveJobFromActiveToWait',
        state: 'active',
      });
    }
    return result;
  }
}
