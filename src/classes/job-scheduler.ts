import { parseExpression } from 'cron-parser';
import {
  JobSchedulerJson,
  JobSchedulerTemplateJson,
  EmqClient,
  RepeatBaseOptions,
  RepeatOptions,
} from '../interfaces';
import {
  JobSchedulerTemplateOptions,
  JobsOptions,
  RepeatStrategy,
} from '../types';
import { Job } from './job';
import { QueueBase } from './queue-base';
import { PgPoolConnection } from './pg-connection';
import { SpanKind, TelemetryAttributes } from '../enums';
import { escapeSchema } from './queue-identity';

export class JobScheduler extends QueueBase {
  private repeatStrategy: RepeatStrategy;

  constructor(
    name: string,
    opts: RepeatBaseOptions,
    Connection?: typeof PgPoolConnection,
  ) {
    super(name, opts, Connection);

    this.repeatStrategy =
      (opts.settings && opts.settings.repeatStrategy) || defaultRepeatStrategy;
  }

  async upsertJobScheduler<T = any, R = any, N extends string = string>(
    jobSchedulerId: string,
    repeatOpts: Omit<RepeatOptions, 'key' | 'prevMillis'>,
    jobName: N,
    jobData: T,
    opts: JobSchedulerTemplateOptions,
    { override, producerId }: { override: boolean; producerId?: string },
  ): Promise<Job<T, R, N> | undefined> {
    const { every, limit, pattern, offset } = repeatOpts;

    if (pattern && every) {
      throw new Error(
        'Both .pattern and .every options are defined for this repeatable job',
      );
    }

    if (!pattern && !every) {
      throw new Error(
        'Either .pattern or .every options must be defined for this repeatable job',
      );
    }

    if (repeatOpts.immediately && repeatOpts.startDate) {
      throw new Error(
        'Both .immediately and .startDate options are defined for this repeatable job',
      );
    }

    if (repeatOpts.immediately && repeatOpts.every) {
      console.warn(
        "Using option immediately with every does not affect the job's schedule. Job will run immediately anyway.",
      );
    }

    // Check if we reached the limit of the repeatable job's iterations
    const iterationCount = repeatOpts.count ? repeatOpts.count + 1 : 1;
    if (
      typeof repeatOpts.limit !== 'undefined' &&
      iterationCount > repeatOpts.limit
    ) {
      return;
    }

    // Check if we reached the end date of the repeatable job
    let now = Date.now();
    const { endDate } = repeatOpts;
    if (endDate && now > new Date(endDate!).getTime()) {
      return;
    }

    const prevMillis = opts.prevMillis || 0;
    now = prevMillis < now ? now : prevMillis;

    // Check if we have a start date for the repeatable job
    const { immediately: _immediately, ...filteredRepeatOpts } = repeatOpts;

    let nextMillis = 0;
    let newOffset: number | null = null;

    if (pattern) {
      nextMillis =
        (await this.repeatStrategy(now, repeatOpts, jobName)) ?? now;
      if (nextMillis < now) {
        nextMillis = now;
      }
      // Mirror BullMQ's `storeJobScheduler.lua`: when `opts['offset']` is
      // present (even 0, which is truthy in Lua), it's written to the hash.
      // The TypeScript side always passes `opts.offset` for pattern mode, so
      // we default to 0 here so `getJobScheduler()` surfaces `offset: 0` (as
      // the ported tests require) instead of omitting it.
      newOffset = 0;
    } else if (every) {
      // Mirror BullMQ's `getJobSchedulerEveryNextMillis.lua`: the offset is
      // computed on first store and then preserved across reschedules
      // (even when `every` changes) unless the caller explicitly supplies a
      // new one. This keeps downstream iterations aligned to the original
      // slot boundary the test suite relies on.
      const startDateMillis = repeatOpts.startDate
        ? new Date(repeatOpts.startDate).getTime()
        : undefined;
      let effectiveOffset: number | null =
        typeof offset === 'number' ? offset : null;
      if (effectiveOffset == null) {
        const storedOffset = await this.readStoredOffset(jobSchedulerId);
        if (typeof storedOffset === 'number') {
          effectiveOffset = storedOffset;
        }
      }
      // Mirror BullMQ's `addJobScheduler-11.lua` (override=true) and
      // `updateJobScheduler-12.lua` (override=false) + shared
      // `getJobSchedulerEveryNextMillis.lua`. `prevMillis` is read from
      // the stored scheduler (equivalent of `ZSCORE repeatKey
      // jobSchedulerId`).
      //
      // * override=false (worker pre-advance): always compute
      //   nextMillis = prevMillis + every so the scheduler moves forward
      //   one iteration. If prevMillis is missing (stale scheduler), use
      //   startDate/now.
      // * override=true (user upsert): compute prevMillis + every up
      //   front. If a previous iteration row is still queued (the SQL
      //   helper will remove it) AND `every` didn't change, BullMQ
      //   reuses the prior `nextMillis` so concurrent re-upserts
      //   collapse onto the same `repeat:<id>:<ms>` slot. We mirror
      //   that by preferring storedNext when every is unchanged.
      const storedEvery = await this.readStoredEvery(jobSchedulerId);
      const storedNext = await this.readStoredNextMillis(jobSchedulerId);
      const updatedEvery = storedEvery != null && storedEvery !== every;
      const effectivePrev = updatedEvery ? null : storedNext;
      if (effectivePrev == null) {
        nextMillis =
          startDateMillis && startDateMillis > now ? startDateMillis : now;
      } else if (override) {
        // User-initiated upsert: reuse the scheduler's existing slot so
        // the delayed iteration just gets re-stamped with new template
        // data/opts rather than scheduling a new future iteration.
        nextMillis = effectivePrev;
      } else {
        // Worker pre-advance: advance by one `every` period, with
        // catch-up if we've missed slots.
        nextMillis = effectivePrev + every;
        if (nextMillis < now) {
          nextMillis =
            Math.floor(now / every) * every + every + (effectiveOffset ?? 0);
        }
      }
      if (!effectiveOffset) {
        const timeSlot = Math.floor(nextMillis / every) * every;
        newOffset = nextMillis - timeSlot;
      } else {
        newOffset = effectiveOffset;
      }
    }

    if (nextMillis || every) {
      return this.trace<Job<T, R, N> | undefined>(
        SpanKind.PRODUCER,
        'add',
        `${this.name}.${jobName}`,
        async (span, srcPropagationMetadata) => {
          let telemetry = opts.telemetry;

          if (srcPropagationMetadata) {
            const omitContext = opts.telemetry?.omitContext;
            const telemetryMetadata =
              opts.telemetry?.metadata ||
              (!omitContext && srcPropagationMetadata);

            if (telemetryMetadata || omitContext) {
              telemetry = {
                ...(typeof telemetryMetadata === 'string'
                  ? { metadata: telemetryMetadata }
                  : {}),
                ...(omitContext !== undefined ? { omitContext } : {}),
              };
            }
          }

          const mergedOpts = this.getNextJobOpts(
            nextMillis,
            jobSchedulerId,
            {
              ...opts,
              repeat: filteredRepeatOpts,
              telemetry,
            },
            iterationCount,
            newOffset ?? undefined,
          );

          if (override) {
            // Clamp nextMillis to now if it's in the past
            if (nextMillis < now) {
              nextMillis = now;
            }

            const [jobId] = await this.scripts.addJobScheduler(
              jobSchedulerId,
              nextMillis,
              JSON.stringify(typeof jobData === 'undefined' ? {} : jobData),
              Job.optsAsJSON(opts),
              {
                name: jobName,
                startDate: repeatOpts.startDate
                  ? new Date(repeatOpts.startDate).getTime()
                  : undefined,
                endDate: endDate ? new Date(endDate).getTime() : undefined,
                tz: repeatOpts.tz,
                pattern,
                every,
                limit,
                offset: newOffset ?? undefined,
              },
              Job.optsAsJSON(mergedOpts),
              producerId,
            );

            // The delayed iteration is created atomically inside
            // `emq_add_job_scheduler_v1`; load the persisted row for the
            // public Job handle.
            const job = (await Job.fromId<T, R, N>(this, jobId))!;

            span?.setAttributes({
              [TelemetryAttributes.JobSchedulerId]: jobSchedulerId,
              [TelemetryAttributes.JobId]: job.id,
            });

            return job;
          } else {
            const schedulerId = await this.scripts.updateJobSchedulerNextMillis(
              jobSchedulerId,
              nextMillis,
              JSON.stringify(typeof jobData === 'undefined' ? {} : jobData),
              Job.optsAsJSON(mergedOpts),
              producerId,
            );
            if (schedulerId) {
              // BullMQ's Lua `updateJobSchedulerNextMillis` both advances the
              // scheduler's next_millis and inserts the corresponding delayed
              // job row. Our SQL function only advances; fall back to
              // `Job.create` here so the delayed iteration actually lands in
              // `emq_jobs` for the next tick.
              const job = await this.Job.create<T, R, N>(
                this,
                jobName,
                jobData,
                mergedOpts,
              );

              span?.setAttributes({
                [TelemetryAttributes.JobSchedulerId]: jobSchedulerId,
                [TelemetryAttributes.JobId]: job.id,
              });

              return job;
            }
            return undefined;
          }
        },
      );
    }
  }

  private getNextJobOpts(
    nextMillis: number,
    jobSchedulerId: string,
    opts: JobsOptions,
    currentCount: number,
    offset?: number,
  ): JobsOptions {
    //
    // Generate unique job id for this iteration.
    //
    const jobId = this.getSchedulerNextJobId({
      jobSchedulerId,
      nextMillis,
    });

    const now = Date.now();
    // Mirrors BullMQ's `addJobScheduler-11.lua` / `updateJobScheduler-12.lua`
    // which compute `delay = nextMillis - now` (ignoring offset) when
    // materialising the delayed job row. The TS wrapper in bullmq leaves
    // `delay` as NaN for `every` mode and relies on the Lua computation.
    // Since our SQL helper doesn't derive the delay itself, we do the
    // Lua-equivalent math here so the first `every` iteration fires at
    // `nextMillis` (which is `now` for the initial insert) instead of
    // `nextMillis + offset`.
    const delay = nextMillis - now;

    const mergedOpts: JobsOptions = {
      ...opts,
      jobId,
      delay: delay < 0 ? 0 : delay,
      timestamp: now,
      prevMillis: nextMillis,
      repeatJobKey: jobSchedulerId,
    };

    mergedOpts.repeat = {
      ...opts.repeat,
      offset,
      count: currentCount,
      startDate: opts.repeat?.startDate
        ? new Date(opts.repeat.startDate).getTime()
        : undefined,
      endDate: opts.repeat?.endDate
        ? new Date(opts.repeat.endDate).getTime()
        : undefined,
    };

    return mergedOpts;
  }

  async removeJobScheduler(jobSchedulerId: string): Promise<number> {
    return this.scripts.removeJobScheduler(jobSchedulerId);
  }

  private async getSchedulerData<D>(
    client: EmqClient,
    key: string,
    next?: number,
  ): Promise<JobSchedulerJson<D> | undefined> {
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [row],
    } = await client.query<{
      name: string;
      data: unknown;
      opts: unknown;
      template: unknown;
      pattern: string | null;
      every_ms: string | null;
      tz: string | null;
      start_date: string | null;
      end_date: string | null;
      next_millis: string | null;
      iteration_count: number;
      offset_ms: string | null;
      limit_count: number | null;
    }>(
      `select name, data, opts, template, pattern, every_ms::text,
              offset_ms::text, limit_count, tz,
              extract(epoch from start_date) * 1000 as start_date,
              extract(epoch from end_date) * 1000 as end_date,
              next_millis::text, iteration_count
       from ${S}.emq_job_schedulers where queue_id = $1 and scheduler_id = $2`,
      [qid, key],
    );

    if (!row) {
      return undefined;
    }

    const jobData: Record<string, string> = {
      name: row.name,
      ic: String(row.iteration_count ?? 0),
    };
    if (row.start_date) {
      jobData.startDate = row.start_date;
    }
    if (row.end_date) {
      jobData.endDate = row.end_date;
    }
    if (row.tz) {
      jobData.tz = row.tz;
    }
    if (row.pattern) {
      jobData.pattern = row.pattern;
    }
    if (row.every_ms) {
      jobData.every = row.every_ms;
    }
    if (row.offset_ms !== null && row.offset_ms !== undefined) {
      jobData.offset = row.offset_ms;
    }
    if (row.limit_count) {
      jobData.limit = String(row.limit_count);
    }
    if (row.data) {
      // BullMQ stores an empty template as the literal string '{}' which
      // `getTemplateFromJSON` then ignores. Match that: skip the data
      // attribute when the template payload is empty so
      // `getJobSchedulers()` does not surface a spurious `template.data`.
      const dataStr =
        typeof row.data === 'string' ? row.data : JSON.stringify(row.data);
      if (dataStr && dataStr !== '{}' && dataStr !== 'null') {
        jobData.data = dataStr;
      }
    }
    if (row.opts) {
      const optsStr =
        typeof row.opts === 'string' ? row.opts : JSON.stringify(row.opts);
      if (optsStr && optsStr !== '{}' && optsStr !== 'null') {
        jobData.opts = optsStr;
      }
    }

    return this.transformSchedulerData<D>(
      key,
      jobData,
      next ?? (row.next_millis ? parseInt(row.next_millis, 10) : undefined),
    );
  }

  private transformSchedulerData<D>(
    key: string,
    jobData: any,
    next?: number,
  ): JobSchedulerJson<D> | undefined {
    if (jobData && Object.keys(jobData).length > 0) {
      const jobSchedulerData: JobSchedulerJson<D> = {
        key,
        name: jobData.name,
        next,
      };

      if (jobData.ic) {
        jobSchedulerData.iterationCount = parseInt(jobData.ic);
      }

      if (jobData.limit) {
        jobSchedulerData.limit = parseInt(jobData.limit);
      }

      if (jobData.startDate) {
        jobSchedulerData.startDate = parseInt(jobData.startDate);
      }

      if (jobData.endDate) {
        jobSchedulerData.endDate = parseInt(jobData.endDate);
      }

      if (jobData.tz) {
        jobSchedulerData.tz = jobData.tz;
      }

      if (jobData.pattern) {
        jobSchedulerData.pattern = jobData.pattern;
      }

      if (jobData.every) {
        jobSchedulerData.every = parseInt(jobData.every);
      }

      if (jobData.offset !== undefined && jobData.offset !== null) {
        jobSchedulerData.offset = parseInt(jobData.offset);
      }

      if (jobData.data || jobData.opts) {
        jobSchedulerData.template = this.getTemplateFromJSON<D>(
          jobData.data,
          jobData.opts,
        );
      }

      return jobSchedulerData;
    }

    return undefined;
  }

  async getScheduler<D = any>(
    id: string,
  ): Promise<JobSchedulerJson<D> | undefined> {
    const client = await this.client;
    return this.getSchedulerData<D>(client, id);
  }

  private getTemplateFromJSON<D = any>(
    rawData?: string,
    rawOpts?: string,
  ): JobSchedulerTemplateJson<D> {
    const template: JobSchedulerTemplateJson<D> = {};
    if (rawData) {
      template.data = JSON.parse(rawData);
    }
    if (rawOpts) {
      template.opts = Job.optsFromJSON(rawOpts);
    }
    return template;
  }

  async getJobSchedulers<D = any>(
    start = 0,
    end = -1,
    asc = false,
  ): Promise<JobSchedulerJson<D>[]> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const order = asc ? 'asc' : 'desc';
    const limit =
      end >= 0 && start >= 0 ? Math.max(0, end - start + 1) : 1073741824;

    const { rows } = await client.query<{
      repeat_key: string;
      next_millis: string;
    }>(
      `select scheduler_id as repeat_key, next_millis::text
       from ${S}.emq_job_schedulers
       where queue_id = $1
       order by next_millis ${order === 'asc' ? 'asc' : 'desc'}
       limit $2 offset $3`,
      [qid, limit, start],
    );

    const jobs = [];
    for (const r of rows) {
      jobs.push(
        this.getSchedulerData<D>(
          client,
          r.repeat_key,
          r.next_millis ? parseInt(r.next_millis, 10) : undefined,
        ),
      );
    }
    const resolved = await Promise.all(jobs);
    return resolved.filter((j): j is JobSchedulerJson<D> => j != null);
  }

  async getSchedulersCount(): Promise<number> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [row],
    } = await client.query<{ c: string }>(
      `select count(*)::text as c from ${S}.emq_job_schedulers where queue_id = $1`,
      [qid],
    );
    return row ? parseInt(row.c, 10) : 0;
  }

  private async readStoredOffset(
    jobSchedulerId: string,
  ): Promise<number | null> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const { rows } = await client.query<{ offset_ms: string | null }>(
      `select offset_ms::text from ${S}.emq_job_schedulers
       where queue_id = $1 and scheduler_id = $2`,
      [qid, jobSchedulerId],
    );
    return rows[0]?.offset_ms ? parseInt(rows[0].offset_ms, 10) : null;
  }

  private async readStoredEvery(
    jobSchedulerId: string,
  ): Promise<number | null> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const { rows } = await client.query<{ every_ms: string | null }>(
      `select every_ms::text from ${S}.emq_job_schedulers
       where queue_id = $1 and scheduler_id = $2`,
      [qid, jobSchedulerId],
    );
    return rows[0]?.every_ms ? parseInt(rows[0].every_ms, 10) : null;
  }

  private async readStoredNextMillis(
    jobSchedulerId: string,
  ): Promise<number | null> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const { rows } = await client.query<{ next_millis: string | null }>(
      `select next_millis::text from ${S}.emq_job_schedulers
       where queue_id = $1 and scheduler_id = $2`,
      [qid, jobSchedulerId],
    );
    return rows[0]?.next_millis ? parseInt(rows[0].next_millis, 10) : null;
  }

  private getSchedulerNextJobId({
    nextMillis,
    jobSchedulerId,
  }: {
    jobSchedulerId: string;
    nextMillis: number | string;
  }) {
    return `repeat:${jobSchedulerId}:${nextMillis}`;
  }
}

export const defaultRepeatStrategy = (
  millis: number,
  opts: RepeatOptions,
): number | undefined => {
  const { pattern } = opts;

  if (!pattern) {
    return undefined;
  }

  const dateFromMillis = new Date(millis);
  const startCandidate = opts.startDate
    ? new Date(opts.startDate)
    : undefined;
  const currentDate =
    startCandidate != null && startCandidate > dateFromMillis
      ? startCandidate
      : dateFromMillis;
  const interval = parseExpression(pattern, {
    ...opts,
    currentDate,
  });

  try {
    if (opts.immediately) {
      return new Date().getTime();
    } else {
      return interval.next().getTime();
    }
  } catch (err) {
    throw new Error(
      `Invalid cron pattern "${pattern}": ${(err as Error).message}`,
    );
  }
};
