import type { RepeatableOptions } from '../../interfaces';
import type { EncodedJobOptions, JobsOptions } from '../../types';
import { ErrorCode } from '../../enums';
import { GettersScripts } from './getters';

/** Job scheduler (cron / every-N-millis) bookkeeping. */
export class SchedulerScripts extends GettersScripts {
  async addJobScheduler(
    jobSchedulerId: string,
    nextMillis: number,
    templateData: string,
    templateOpts: EncodedJobOptions,
    opts: RepeatableOptions,
    _delayedJobOpts: JobsOptions,
    producerId?: string,
  ): Promise<[string, number]> {
    void _delayedJobOpts;
    const client = await this.queue.client;
    const S = this.S();
    const {
      rows: [row],
    } = await client.query<{
      out_scheduler_id: string | null;
      out_next_millis: string | null;
      err_code: number;
    }>(
      `select * from ${S}.emq_add_job_scheduler_v1(
        $1::bigint, $2::text, $3::bigint, $4::text, $5::jsonb, $6::jsonb, $7::jsonb, $8::text,
        $9::text, $10::bigint, $11::bigint, $12::int, $13::text, $14::bigint, $15::bigint
      )`,
      [
        await this.qid(),
        jobSchedulerId,
        nextMillis,
        String(opts.name ?? 'scheduler'),
        JSON.parse(templateData || '{}'),
        // BullMQ stores the template's stringified opts as the `opts`
        // attribute on the scheduler hash. Pass through what the worker
        // persists so downstream `getJobSchedulers()` can reconstruct
        // `template.opts`.
        templateOpts && Object.keys(templateOpts).length > 0
          ? JSON.stringify(templateOpts)
          : null,
        JSON.stringify({ template: true }),
        producerId ?? '',
        opts.pattern ?? null,
        opts.every ?? null,
        (opts as RepeatableOptions & { offset?: number }).offset ?? null,
        opts.limit ?? null,
        opts.tz ?? null,
        opts.startDate
          ? typeof opts.startDate === 'number'
            ? opts.startDate
            : new Date(opts.startDate).getTime()
          : null,
        opts.endDate
          ? typeof opts.endDate === 'number'
            ? opts.endDate
            : new Date(opts.endDate).getTime()
          : null,
      ],
    );
    if (row?.err_code === -10) {
      throw this.finishedErrors({
        code: ErrorCode.SchedulerJobIdCollision,
        command: 'addJobScheduler',
      });
    }
    if (row?.err_code === -11) {
      throw this.finishedErrors({
        code: ErrorCode.SchedulerJobSlotsBusy,
        command: 'addJobScheduler',
      });
    }
    const effectiveNext = Number(row?.out_next_millis ?? nextMillis);
    return [`repeat:${jobSchedulerId}:${effectiveNext}`, effectiveNext];
  }

  async updateJobSchedulerNextMillis(
    jobSchedulerId: string,
    nextMillis: number,
    _templateData: string,
    _delayedJobOpts: JobsOptions,
    producerId?: string,
  ): Promise<string | null> {
    void _templateData;
    void _delayedJobOpts;
    const client = await this.queue.client;
    const { rows } = await client.query<{ id: string | null }>(
      `select ${this.S()}.emq_update_job_scheduler_v1($1::bigint, $2::text, $3::bigint, $4::text) as id`,
      [await this.qid(), jobSchedulerId, nextMillis, producerId ?? null],
    );
    return rows[0]?.id ?? null;
  }

  async removeJobScheduler(jobSchedulerId: string): Promise<number> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_remove_job_scheduler_v1($1::bigint, $2::text) as c`,
      [await this.qid(), jobSchedulerId],
    );
    return Number(r?.c ?? -1);
  }
}
