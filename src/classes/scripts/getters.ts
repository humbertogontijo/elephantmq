import type { JobJsonRaw } from '../../interfaces';
import type { JobState, JobType } from '../../types';
import { rowToJobJsonRaw } from '../emq-mappers';
import { LifecycleScripts } from './lifecycle';
import { mapListKeyToState } from './helpers';

/**
 * Read-only queries: counts, ranges, state lookups, dependency counts,
 * metrics, and pagination over job sets.
 */
export class GettersScripts extends LifecycleScripts {
  async isJobInList(listKey: string, jobId: string): Promise<boolean> {
    const client = await this.queue.client;
    const st = mapListKeyToState(listKey);
    const {
      rows: [r],
    } = await client.query<{ ok: boolean }>(
      `select ${this.S()}.emq_is_job_in_list_v1($1::bigint, $2::text, $3::text) as ok`,
      [await this.qid(), st, jobId],
    );
    return Boolean(r?.ok);
  }

  async getRanges(
    types: JobType[],
    start = 0,
    end = 1,
    asc = false,
  ): Promise<string[][]> {
    const client = await this.queue.client;
    const transformedTypes = types.map(type =>
      type === 'waiting' ? 'wait' : type,
    );
    const lim = end < 0 ? 1_000_000 : Math.max(0, end - start + 1);
    // BullMQ's Scripts.getRanges returns one sub-array per queried type
    // (matches the Lua pipelined lrange/zrange responses). QueueGetters
    // .getRanges then iterates per-type to decide whether to reverse (lrange
    // + asc). We issue one SQL call per type to preserve that shape.
    //
    // Redis quirk: BullMQ stores wait/paused/active with LPUSH
    // (newest-at-head), so LRANGE returns newest-first. When callers ask
    // for asc order, the getter then reverses to put oldest-first. Our SQL
    // returns rows already ordered by wait_seq, so we flip the asc flag for
    // list-backed types and emit rows in the same "newest-first" convention
    // LRANGE would produce — letting the caller's .reverse() do the right
    // thing.
    const listBackedStates = new Set(['wait', 'paused', 'active']);
    const qid = await this.qid();
    const S = this.S();
    return Promise.all(
      (transformedTypes.length ? transformedTypes : ['wait']).map(async set => {
        const effAsc = listBackedStates.has(set) ? !asc : asc;
        const {
          rows: [r],
        } = await client.query<{ ids: string[] }>(
          `select ${S}.emq_get_ranges_v1($1::bigint, $2::text, $3::int, $4::int, $5::boolean) as ids`,
          [qid, set, start, lim, effAsc],
        );
        return (r?.ids ?? []) as string[];
      }),
    );
  }

  async getCounts(types: JobType[]): Promise<number[]> {
    const client = await this.queue.client;
    const transformedTypes = types.map(type =>
      type === 'waiting' ? 'wait' : type,
    );
    const {
      rows: [r],
    } = await client.query<{ c: (number | string | bigint)[] }>(
      `select ${this.S()}.emq_get_counts_v1($1::bigint, $2::text[]) as c`,
      [await this.qid(), transformedTypes],
    );
    const raw = r?.c ?? [];
    return raw.map(v => Number(v));
  }

  async getCountsPerPriority(priorities: number[]): Promise<number[]> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ c: number[] }>(
      `select ${this.S()}.emq_get_counts_per_priority_v1($1::bigint, $2::int[]) as c`,
      [await this.qid(), priorities],
    );
    return r?.c ?? [];
  }

  async getDependencyCounts(jobId: string, types: string[]): Promise<number[]> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{
      processed: string;
      unprocessed: string;
      ignored: string;
      failed: string;
    }>(
      `select * from ${this.S()}.emq_get_dependency_counts_v1($1::bigint, $2::text)`,
      [await this.qid(), jobId],
    );
    const map: Record<string, number> = {
      processed: Number(r?.processed ?? 0),
      unprocessed: Number(r?.unprocessed ?? 0),
      ignored: Number(r?.ignored ?? 0),
      failed: Number(r?.failed ?? 0),
    };
    return types.map(t => map[t] ?? 0);
  }

  async isFinished(
    jobId: string,
    returnValue = false,
  ): Promise<number | [number, string]> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ kind: number; ret: unknown }>(
      `select * from ${this.S()}.emq_is_finished_v1($1::bigint, $2::text)`,
      [await this.qid(), jobId],
    );
    if (!r) {
      return returnValue ? ([0, ''] as [number, string]) : 0;
    }
    // `ret` arrives as jsonb; serialise to the string form BullMQ's Lua
    // variant returned so downstream code (getReturnValue / onFailed) can
    // re-parse it.
    const serialise = (v: unknown): string => {
      if (v == null) {
        return '';
      }
      if (typeof v === 'string') {
        return v;
      }
      return JSON.stringify(v);
    };
    if (r.kind === -2) {
      // Mirror BullMQ's isFinished-3.lua: when the job is missing and
      // caller asked for returnValue, return [-1, "Missing key for job ..."]
      // so waitUntilFinished rejects with the expected message.
      const missing = `Missing key for job ${this.queue.toKey(jobId)}. isFinished`;
      return returnValue ? ([-1, missing] as [number, string]) : -1;
    }
    if (r.kind === 1) {
      return returnValue ? ([1, serialise(r.ret)] as [number, string]) : 1;
    }
    if (r.kind === -1) {
      return [-1, serialise(r.ret)] as [number, string];
    }
    return returnValue ? ([0, ''] as [number, string]) : 0;
  }

  async getState(jobId: string): Promise<JobState | 'unknown'> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query(
      `select ${this.S()}.emq_get_state_v2_v1($1::bigint, $2::text) as s`,
      [await this.qid(), jobId],
    );
    return ((r as { s?: string })?.s ?? 'unknown') as JobState | 'unknown';
  }

  async getRateLimitTtl(maxJobs?: number): Promise<number> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query<{ t: string }>(
      `select ${this.S()}.emq_get_rate_limit_ttl_v1($1::bigint, $2::bigint) as t`,
      [await this.qid(), maxJobs ?? null],
    );
    return Number(r?.t ?? 0);
  }

  async getJobScheduler(id: string): Promise<[any, string | null]> {
    const client = await this.queue.client;
    const {
      rows: [r],
    } = await client.query(
      `select ${this.S()}.emq_get_job_scheduler_v1($1::bigint, $2::text) as j`,
      [await this.qid(), id],
    );
    return [(r as { j?: unknown })?.j ?? null, null];
  }

  async getMetrics(
    type: 'completed' | 'failed',
    start = 0,
    end = -1,
  ): Promise<[string[], string[], number]> {
    const client = await this.queue.client;
    // emq_get_metrics_v1 returns (meta, data, cnt) to mirror BullMQ's
    // getMetrics.lua: meta = [count, prevTS, prevCount], data = per-minute
    // deltas newest-first, cnt = length of the returned slice.
    const { rows } = await client.query<{
      meta: string[];
      data: string[];
      cnt: number;
    }>(
      `select * from ${this.S()}.emq_get_metrics_v1($1::bigint, $2::text, $3::int, $4::int)`,
      [await this.qid(), type, start, end],
    );
    const row = rows[0];
    if (!row) {
      return [['0', '0', '0'], [], 0];
    }
    return [row.meta ?? ['0', '0', '0'], row.data ?? [], row.cnt ?? 0];
  }

  /**
   * Paginate a set or hash of keys.
   */
  async paginate(
    key: string,
    opts: { start: number; end: number; fetchJobs?: boolean },
  ): Promise<{
    cursor: string;
    items: { id: string; v?: any; err?: string }[];
    total: number;
    jobs?: JobJsonRaw[];
  }> {
    const client = await this.queue.client;
    const parts = key.split(':');
    const tail = parts[parts.length - 1] || 'wait';
    const limRaw = opts.end < 0 ? 0 : opts.end - opts.start + 1;
    const qid = await this.qid();

    // Dependencies / processed lookups pass a key like
    // `<prefix>:<queue>:<parentId>:dependencies` or `...:processed`.
    if (tail === 'dependencies' || tail === 'processed') {
      const parentJobId = parts[parts.length - 2];
      const status = tail === 'processed' ? 'processed' : 'pending';
      const { rows } = await client.query<{
        out_job_id: string;
        out_result: string | null;
        out_total: string;
      }>(
        `select * from ${this.S()}.emq_paginate_deps_v1(
           $1::bigint, $2::text, $3::text, $4::int, $5::int
         )`,
        [qid, parentJobId, status, opts.start, limRaw],
      );
      const items = rows.map(x =>
        tail === 'processed'
          ? {
              id: x.out_job_id,
              v: x.out_result === null ? undefined : x.out_result,
            }
          : { id: x.out_job_id },
      );
      const total = rows.length ? Number(rows[0].out_total) : 0;
      const jobs: JobJsonRaw[] = [];
      if (opts.fetchJobs && items.length) {
        const ids = items.map(i => i.id);
        const S = this.S();
        const { rows: jrows } = await client.query(
          `select j.*,
                  case when pq.id is not null
                       then pq.prefix || ':' || pq.name
                       else null end as parent_queue_key
             from ${S}.emq_jobs j
             left join ${S}.emq_queues pq on pq.id = j.parent_queue_id
             where j.queue_id = $1 and j.job_id = any($2::text[])
             order by array_position($2::text[], j.job_id)`,
          [qid, ids],
        );
        for (const r of jrows) {
          jobs.push(rowToJobJsonRaw(r as Parameters<typeof rowToJobJsonRaw>[0]));
        }
      }
      return { cursor: '0', items, total, jobs };
    }

    const set = tail;
    const lim = Math.max(1, limRaw);
    const { rows } = await client.query<{ job_id: string; total: string }>(
      `select * from ${this.S()}.emq_paginate_v1($1::bigint, $2::text, $3::int, $4::int)`,
      [qid, set, opts.start, lim],
    );
    const items = rows.map(x => ({ id: x.job_id }));
    const total = rows.length ? Number(rows[0].total) : 0;
    return {
      cursor: '0',
      items,
      total,
      jobs: [],
    };
  }
}
