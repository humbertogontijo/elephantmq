import type { JobProgress } from '../../types';
import type { MinimalJob } from '../../interfaces';
import { SchedulerScripts } from './scheduler';

/**
 * Whole-queue and per-job housekeeping: pause, drain, removal, dependency
 * cleanup, log/data/progress updates, obliteration. These methods do not
 * participate in the per-job lifecycle promotion path.
 */
export class MaintenanceScripts extends SchedulerScripts {
  async pause(pause: boolean): Promise<void> {
    const client = await this.queue.client;
    await client.query(
      `select ${this.S()}.emq_pause_v1($1::bigint, $2::boolean)`,
      [await this.qid(), pause],
    );
  }

  async removeDeduplicationKey(
    deduplicationId: string,
    jobId: string,
  ): Promise<number> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_remove_deduplication_key_v1($1::bigint, $2::text, $3::text) as c`,
      [await this.qid(), deduplicationId, jobId],
    );
    return Number(r?.c ?? 0);
  }

  /**
   * Delete job rows for this queue whose `state` is null among the given
   * ids. Returns job ids that were orphan candidates (null state or missing
   * row).
   */
  async removeOrphanedJobs(candidateJobIds: string[]): Promise<string[]> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: string[] | null }>(
      `select ${this.S()}.emq_remove_orphaned_jobs_v1($1::bigint, $2::text[]) as c`,
      [await this.qid(), candidateJobIds.length ? candidateJobIds : []],
    );
    return Array.isArray(r?.c) ? r!.c : [];
  }

  async remove(jobId: string, removeChildren: boolean): Promise<number> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ r: number }>(
      `select ${this.S()}.emq_remove_job_v1($1::bigint, $2::text, $3::boolean) as r`,
      [await this.qid(), jobId, removeChildren],
    );
    const result = Number(r?.r ?? 0);
    if (result < 0) {
      throw this.finishedErrors({
        code: result,
        jobId,
        command: 'removeJob',
      });
    }
    return result;
  }

  async removeUnprocessedChildren(jobId: string): Promise<void> {
    const client = await this.queue.client;
    await client.query(
      `select ${this.S()}.emq_remove_unprocessed_children_v1($1::bigint, $2::text)`,
      [await this.qid(), jobId],
    );
  }

  async updateData<T = any, R = any, N extends string = string>(
    job: MinimalJob<T, R, N>,
    data: T,
  ): Promise<void> {
    const client = await this.queue.client;
    const dataJson = JSON.stringify(data);
    const {
      rows: [r],
    } = await client.query<{ r: number }>(
      `select ${this.S()}.emq_update_data_v1($1::bigint, $2::text, $3::jsonb) as r`,
      [await this.qid(), job.id, dataJson],
    );
    if ((r?.r ?? -1) < 0) {
      throw this.finishedErrors({
        code: r?.r ?? -1,
        jobId: job.id,
        command: 'updateData',
      });
    }
  }

  async updateProgress(jobId: string, progress: JobProgress): Promise<void> {
    const client = await this.queue.client;
    const progressJson = JSON.stringify(progress);
    const {
      rows: [r],
    } = await client.query<{ r: number }>(
      `select ${this.S()}.emq_update_progress_v1($1::bigint, $2::text, $3::jsonb) as r`,
      [await this.qid(), jobId, progressJson],
    );
    if ((r?.r ?? -1) < 0) {
      throw this.finishedErrors({
        code: r?.r ?? -1,
        jobId,
        command: 'updateProgress',
      });
    }
  }

  async addLog(
    jobId: string,
    logRow: string,
    keepLogs?: number,
  ): Promise<number> {
    const client = await this.queue.client;
    const keep = parseInt(String(keepLogs ?? '0'), 10) || 0;
    const {
      rows: [r],
    } = await client.query<{ r: number }>(
      `select ${this.S()}.emq_add_log_v1($1::bigint, $2::text, $3::text, $4::int) as r`,
      [await this.qid(), jobId, logRow, keep],
    );
    if ((r?.r ?? -1) < 0) {
      throw this.finishedErrors({
        code: r?.r ?? -1,
        jobId,
        command: 'addLog',
      });
    }
    return r?.r ?? 0;
  }

  async drain(delayed: boolean): Promise<void> {
    const client = await this.queue.client;
    await client.query(
      `select ${this.S()}.emq_drain_v1($1::bigint, $2::boolean)`,
      [await this.qid(), delayed],
    );
  }

  async removeChildDependency(
    jobId: string,
    parentKey: string,
  ): Promise<boolean> {
    const client = await this.queue.client;
    const childId = jobId.includes(':') ? jobId.split(':').pop()! : jobId;
    // parentKey layout: `<prefix>:<queueName>:<parentJobId>`. `prefix` can
    // itself contain `:` (e.g. `bull:test`), so peel the last two segments
    // off the right as jobId and queueName, then whatever is left is the
    // prefix.
    const parentParts = parentKey.split(':');
    const parentId = parentParts.pop()!;
    const parentQueueName = parentParts.pop()!;
    const parentPrefix = parentParts.join(':');
    const {
      rows: [r],
    } = await client.query<{ code: number }>(
      `select ${this.S()}.emq_remove_child_dependency_v1(
        $1::bigint, $2::text, $3::text, $4::text, $5::text
      ) as code`,
      [await this.qid(), childId, parentPrefix, parentQueueName, parentId],
    );
    const result = r?.code ?? -1;
    switch (result) {
      case 0:
        return true;
      case 1:
        return false;
      default:
        throw this.finishedErrors({
          code: result,
          jobId,
          parentKey,
          command: 'removeChildDependency',
        });
    }
  }

  /**
   * Remove jobs in a specific state.
   *
   * @returns ids of the deleted records.
   */
  async cleanJobsInSet(
    set: string,
    timestamp: number,
    limit = 0,
  ): Promise<string[]> {
    const client = await this.queue.client;
    // BullMQ cleanJobsInSet-1.lua treats limit <= 0 as "no cap"; pass a
    // large sentinel in that case so we delete everything matching the
    // predicate.
    const lim = limit > 0 ? limit : 10000;
    const {
      rows: [r],
    } = await client.query<{ ids: string[] }>(
      `select ${this.S()}.emq_clean_jobs_in_set_v1($1::bigint, $2::text, $3::bigint, $4::int) as ids`,
      [await this.qid(), set, timestamp, lim],
    );
    return r?.ids ?? [];
  }

  async obliterate(opts: {
    force: boolean;
    count: number;
  }): Promise<number> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number }>(
      `select ${this.S()}.emq_obliterate_v1($1::bigint, $2::boolean, $3::int) as c`,
      [await this.qid(), opts.force, opts.count],
    );
    const result = r?.c ?? 0;
    if (result < 0) {
      switch (result) {
        case -1:
          throw new Error('Cannot obliterate non-paused queue');
        case -2:
          throw new Error('Cannot obliterate queue with active jobs');
      }
    }
    return result;
  }
}
