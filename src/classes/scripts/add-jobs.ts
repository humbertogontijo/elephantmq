import type {
  JobJson,
  ParentKeyOpts,
  PgQueryable,
  ScriptQueueContext,
} from '../../interfaces';
import type { EncodedJobOptions } from '../../types';
import { ScriptsBase } from './base';
import {
  AddJobSqlPayload,
  encodeEncodedJobOptions,
  normalizeJobDataJson,
} from './helpers';

/**
 * Per-queue serialisation chain for `addJob`.
 *
 * `Job.createScripts` spins up a fresh {@link Scripts} per job, so we cannot
 * key serialisation off `this`. Instead we key off the underlying queue
 * "owner" (set on the `Scripts` instance via `__queueOwner` by
 * `createScripts`), so every concurrent `addJob` for the same queue funnels
 * through the same promise chain.
 *
 * BullMQ's Redis path relies on ioredis pipelining this serialisation
 * implicitly over a single socket; with a `pg.Pool` each call may grab a
 * different client and race, producing a non-deterministic `job_id` →
 * `data` mapping.
 */
const addJobChainByQueue = new WeakMap<object, Promise<unknown>>();

export class AddJobsScripts extends ScriptsBase {
  protected buildAddJobPayload(
    job: JobJson,
    _queueKeys: ScriptQueueContext['keys'],
    parentKeyOpts: ParentKeyOpts,
    jobId: string,
  ): AddJobSqlPayload {
    void _queueKeys;
    // BullMQ lets callers pass `parentKey` directly on the parentOpts bag
    // (e.g. tests that construct Job manually) even when the
    // Job.parent/parentKey fields aren't populated. Mirror that by falling
    // back to the caller-provided parentKey so emq_add_*_v1 can still derive
    // parent id + qualified queue name and call emq_link_child_to_parent_v1.
    const parentQueueKey = job.parentKey || parentKeyOpts.parentKey || '';
    return {
      customId: typeof jobId !== 'undefined' ? jobId : '',
      name: job.name,
      timestamp: job.timestamp,
      parentQueueKey,
      parentDepKey: parentKeyOpts.parentDependenciesKey || '',
      parent: (job.parent || {}) as Record<string, unknown>,
      repeatKey: job.repeatJobKey || null,
      dedupId: job.deduplicationId || null,
    };
  }

  private async addStandardJobPg(
    client: PgQueryable,
    dataJson: string,
    encodedOpts: string,
    p: AddJobSqlPayload,
  ): Promise<string | number> {
    const S = this.S();
    const qid = await this.qid();
    const {
      rows: [r],
    } = await client.query<{ job_id: string }>(
      `select ${S}.emq_add_standard_job_v1(
        $1::bigint, $2::text, $3::text, $4::jsonb, $5::jsonb, $6::bigint,
        $7::text, $8::text, $9::jsonb, $10::text, $11::text
      ) as job_id`,
      [
        qid,
        p.customId,
        p.name,
        dataJson,
        encodedOpts,
        p.timestamp,
        p.parentQueueKey,
        p.parentDepKey,
        JSON.stringify(p.parent),
        p.repeatKey,
        p.dedupId,
      ],
    );
    return r?.job_id ?? '';
  }

  private async addDelayedJobPg(
    client: PgQueryable,
    dataJson: string,
    encodedOpts: string,
    p: AddJobSqlPayload,
  ): Promise<string | number> {
    const S = this.S();
    const {
      rows: [r],
    } = await client.query<{ job_id: string }>(
      `select ${S}.emq_add_delayed_job_v1(
        $1::bigint, $2::text, $3::text, $4::jsonb, $5::jsonb, $6::bigint,
        $7::text, $8::text, $9::jsonb, $10::text, $11::text
      ) as job_id`,
      [
        await this.qid(),
        p.customId,
        p.name,
        dataJson,
        encodedOpts,
        p.timestamp,
        p.parentQueueKey,
        p.parentDepKey,
        JSON.stringify(p.parent),
        p.repeatKey,
        p.dedupId,
      ],
    );
    return r?.job_id ?? '';
  }

  private async addPrioritizedJobPg(
    client: PgQueryable,
    dataJson: string,
    encodedOpts: string,
    p: AddJobSqlPayload,
  ): Promise<string | number> {
    const S = this.S();
    const {
      rows: [r],
    } = await client.query<{ job_id: string }>(
      `select ${S}.emq_add_prioritized_job_v1(
        $1::bigint, $2::text, $3::text, $4::jsonb, $5::jsonb, $6::bigint,
        $7::text, $8::text, $9::jsonb, $10::text, $11::text
      ) as job_id`,
      [
        await this.qid(),
        p.customId,
        p.name,
        dataJson,
        encodedOpts,
        p.timestamp,
        p.parentQueueKey,
        p.parentDepKey,
        JSON.stringify(p.parent),
        p.repeatKey,
        p.dedupId,
      ],
    );
    return r?.job_id ?? '';
  }

  private async addParentJobPg(
    client: PgQueryable,
    job: JobJson,
    encodedOpts: string,
    p: AddJobSqlPayload,
  ): Promise<string | number> {
    const S = this.S();
    const dataJson = normalizeJobDataJson(job.data);
    const {
      rows: [r],
    } = await client.query<{ job_id: string }>(
      `select ${S}.emq_add_parent_job_v1(
        $1::bigint, $2::text, $3::text, $4::jsonb, $5::jsonb, $6::bigint,
        $7::jsonb, $8::text, $9::text
      ) as job_id`,
      [
        await this.qid(),
        p.customId,
        p.name,
        dataJson,
        encodedOpts,
        p.timestamp,
        JSON.stringify(p.parent),
        p.repeatKey,
        p.dedupId,
      ],
    );
    return r?.job_id ?? '';
  }

  /** FlowProducer entry point: runs `emq_add_parent_job_v1` on a transaction client. */
  async addParentJobForFlow(
    client: PgQueryable,
    job: JobJson,
    opts: EncodedJobOptions,
    jobId: string,
    parentKeyOpts: ParentKeyOpts,
  ): Promise<string> {
    const encodedOpts = encodeEncodedJobOptions(opts);
    const p = this.buildAddJobPayload(
      job,
      this.queue.keys,
      parentKeyOpts,
      jobId,
    );
    const result = await this.addParentJobPg(client, job, encodedOpts, p);
    const numeric = Number(result);
    if (Number.isFinite(numeric) && numeric < 0) {
      throw this.finishedErrors({
        code: numeric,
        parentKey: parentKeyOpts.parentKey,
        command: 'addJob',
      });
    }
    return String(result);
  }

  async addJob(
    client: PgQueryable,
    job: JobJson,
    opts: EncodedJobOptions,
    jobId: string,
    parentKeyOpts: ParentKeyOpts = {},
  ): Promise<string> {
    const owner =
      (this as unknown as { __queueOwner?: object }).__queueOwner ?? this;
    const prev = addJobChainByQueue.get(owner) ?? Promise.resolve();
    const chained = (async () => {
      try {
        await prev;
      } catch {
        /* keep chain alive across individual failures */
      }
      return this.runAddJob(client, job, opts, jobId, parentKeyOpts);
    })();
    addJobChainByQueue.set(
      owner,
      chained.catch(() => {
        /* keep chain alive */
      }),
    );
    return chained;
  }

  private async runAddJob(
    client: PgQueryable,
    job: JobJson,
    opts: EncodedJobOptions,
    jobId: string,
    parentKeyOpts: ParentKeyOpts,
  ): Promise<string> {
    const encodedOpts = encodeEncodedJobOptions(opts);
    const payload = this.buildAddJobPayload(
      job,
      this.queue.keys,
      parentKeyOpts,
      jobId,
    );
    const dataJson = normalizeJobDataJson(job.data);

    let result: string | number;

    if (parentKeyOpts.addToWaitingChildren) {
      result = await this.addParentJobPg(client, job, encodedOpts, payload);
    } else if (typeof opts.delay == 'number' && opts.delay > 0) {
      result = await this.addDelayedJobPg(
        client,
        dataJson,
        encodedOpts,
        payload,
      );
    } else if (opts.priority) {
      result = await this.addPrioritizedJobPg(
        client,
        dataJson,
        encodedOpts,
        payload,
      );
    } else {
      result = await this.addStandardJobPg(
        client,
        dataJson,
        encodedOpts,
        payload,
      );
    }

    const numericResult = Number(result);
    if (Number.isFinite(numericResult) && numericResult < 0) {
      throw this.finishedErrors({
        code: numericResult,
        parentKey: parentKeyOpts.parentKey,
        command: 'addJob',
      });
    }

    return <string>result;
  }
}
