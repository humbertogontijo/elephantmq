'use strict';

import { QueueBase } from './queue-base';
import { escapeSchema } from './queue-identity';
import { Job } from './job';
import { JobState, JobType } from '../types';
import { JobJsonRaw, Metrics, QueueMeta } from '../interfaces';
import { MetricNames, TelemetryAttributes } from '../enums';
import { QUEUE_EVENT_SUFFIX } from '../utils';

/**
 * Provides different getters for different aspects of a queue.
 */
export class QueueGetters<JobBase extends Job = Job> extends QueueBase {
  getJob(jobId: string): Promise<JobBase | undefined> {
    return this.Job.fromId(this, jobId) as Promise<JobBase>;
  }

  /**
   * For each requested type, classify whether the underlying state is FIFO
   * (insertion order, e.g. wait/active/paused) or sorted (e.g. delayed by
   * `process_at`, prioritized by `prio_seq`). Used to decide whether asc
   * results need to be reversed when collecting ids across types.
   */
  private classifyJobTypes(
    types: JobType[],
    callback: (key: string, ordering: 'fifo' | 'sorted') => void,
  ) {
    return types.map((type: string) => {
      type = type === 'waiting' ? 'wait' : type;
      const key = this.toKey(type);

      switch (type) {
        case 'completed':
        case 'failed':
        case 'delayed':
        case 'prioritized':
        case 'repeat':
        case 'waiting-children':
          return callback(key, 'sorted');
        case 'active':
        case 'wait':
        case 'paused':
          return callback(key, 'fifo');
      }
    });
  }

  private sanitizeJobTypes(types: JobType[] | JobType | undefined): JobType[] {
    const currentTypes = typeof types === 'string' ? [types] : types;

    if (Array.isArray(currentTypes) && currentTypes.length > 0) {
      const sanitizedTypes = [...currentTypes];

      if (sanitizedTypes.indexOf('waiting') !== -1) {
        sanitizedTypes.push('paused');
      }

      return [...new Set(sanitizedTypes)];
    }

    return [
      'active',
      'completed',
      'delayed',
      'failed',
      'paused',
      'prioritized',
      'waiting',
      'waiting-children',
    ];
  }

  /**
    Returns the number of jobs waiting to be processed. This includes jobs that are
    "waiting" or "delayed" or "prioritized" or "waiting-children".
  */
  async count(): Promise<number> {
    const count = await this.getJobCountByTypes(
      'waiting',
      'paused',
      'delayed',
      'prioritized',
      'waiting-children',
    );

    return count;
  }

  /**
   * Returns the time to live for a rate limited key in milliseconds.
   * @param maxJobs - max jobs to be considered in rate limit state. If not passed
   * it will return the remaining ttl without considering if max jobs is excedeed.
   * @returns -2 if the key does not exist.
   * -1 if the key exists but has no associated expire.
   * @see {@link https://redis.io/commands/pttl/}
   */
  async getRateLimitTtl(maxJobs?: number): Promise<number> {
    return this.scripts.getRateLimitTtl(maxJobs);
  }

  /**
   * Get jobId from deduplicated state.
   *
   * @param id - deduplication identifier
   */
  async getDeduplicationJobId(id: string): Promise<string | null> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [row],
    } = await client.query<{ job_id: string }>(
      `select job_id from ${S}.emq_deduplication
       where queue_id = $1 and dedup_id = $2
         and (expires_at is null or expires_at > now())`,
      [qid, id],
    );
    return row?.job_id ?? null;
  }

  /**
   * Get global concurrency value.
   * Returns null in case no value is set.
   */
  async getGlobalConcurrency(): Promise<number | null> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [row],
    } = await client.query<{ c: number | null }>(
      `select concurrency as c from ${S}.emq_queues where id = $1`,
      [qid],
    );
    return row?.c ?? null;
  }

  /**
   * Get global rate limit values.
   * Returns null in case no value is set.
   */
  async getGlobalRateLimit(): Promise<{
    max: number;
    duration: number;
  } | null> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [row],
    } = await client.query<{ max: number | null; d: number | null }>(
      `select rate_limit_max as max, rate_limit_duration_ms as d from ${S}.emq_queues where id = $1`,
      [qid],
    );
    if (row?.max != null && row?.d != null) {
      return {
        max: Number(row.max),
        duration: Number(row.d),
      };
    }
    return null;
  }

  /**
   * Job counts by type
   *
   * Queue#getJobCountByTypes('completed') =\> completed count
   * Queue#getJobCountByTypes('completed', 'failed') =\> completed + failed count
   * Queue#getJobCountByTypes('completed', 'waiting', 'failed') =\> completed + waiting + failed count
   */
  async getJobCountByTypes(...types: JobType[]): Promise<number> {
    const result = await this.getJobCounts(...types);
    return Object.values(result).reduce((sum, count) => sum + count, 0);
  }

  /**
   * Returns the job counts for each type specified or every list/set in the queue by default.
   * @param types - the types of jobs to count. If not specified, it will return the counts for all types.
   * @returns An object, key (type) and value (count)
   */
  async getJobCounts(...types: JobType[]): Promise<{
    [index: string]: number;
  }> {
    const currentTypes = this.sanitizeJobTypes(types);

    const responses = await this.scripts.getCounts(currentTypes);

    const counts: { [index: string]: number } = {};
    responses.forEach((res, index) => {
      counts[currentTypes[index]] = res || 0;
    });

    return counts;
  }

  /**
   * Records job counts as gauge metrics for telemetry purposes.
   * Each job state count is recorded with the queue name and state as attributes.
   * @param types - the types of jobs to count. If not specified, it will return the counts for all types.
   * @returns An object, key (type) and value (count)
   */
  async recordJobCountsMetric(...types: JobType[]): Promise<{
    [index: string]: number;
  }> {
    const counts = await this.getJobCounts(...types);
    const meter = this.opts.telemetry?.meter;
    if (meter) {
      const gauge = meter.createGauge(MetricNames.QueueJobsCount, {
        description: 'Number of jobs in the queue by state',
        unit: '{jobs}',
      });
      for (const [state, jobCount] of Object.entries(counts)) {
        gauge.record(jobCount, {
          [TelemetryAttributes.QueueName]: this.name,
          [TelemetryAttributes.QueueJobsState]: state,
        });
      }
    }
    return counts;
  }

  /**
   * Get current job state.
   *
   * @param jobId - job identifier.
   * @returns Returns one of these values:
   * 'completed', 'failed', 'delayed', 'active', 'waiting', 'waiting-children', 'unknown'.
   */
  getJobState(jobId: string): Promise<JobState | 'unknown'> {
    return this.scripts.getState(jobId);
  }

  /**
   * Get global queue configuration.
   *
   * @returns Returns the global queue configuration.
   */
  async getMeta(): Promise<QueueMeta> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [row],
    } = await client.query<{
      paused: boolean;
      concurrency: number | null;
      rate_limit_max: number | null;
      rate_limit_duration_ms: number | null;
      max_len_events: number;
      settings: Record<string, unknown>;
    }>(
      `select paused, concurrency, rate_limit_max, rate_limit_duration_ms, max_len_events, settings
       from ${S}.emq_queues where id = $1`,
      [qid],
    );
    if (!row) {
      return {};
    }
    const parsedConfig: QueueMeta = {
      ...(row.settings || {}),
    };
    if (row.concurrency != null) {
      parsedConfig['concurrency'] = row.concurrency;
    }
    parsedConfig['maxLenEvents'] = row.max_len_events;
    if (row.rate_limit_max != null) {
      parsedConfig['max'] = row.rate_limit_max;
    }
    if (row.rate_limit_duration_ms != null) {
      parsedConfig['duration'] = row.rate_limit_duration_ms;
    }
    parsedConfig['paused'] = row.paused;
    return parsedConfig;
  }

  /**
   * @returns Returns the number of jobs in completed status.
   */
  getCompletedCount(): Promise<number> {
    return this.getJobCountByTypes('completed');
  }

  /**
   * Returns the number of jobs in failed status.
   */
  getFailedCount(): Promise<number> {
    return this.getJobCountByTypes('failed');
  }

  /**
   * Returns the number of jobs in delayed status.
   */
  getDelayedCount(): Promise<number> {
    return this.getJobCountByTypes('delayed');
  }

  /**
   * Returns the number of jobs in active status.
   */
  getActiveCount(): Promise<number> {
    return this.getJobCountByTypes('active');
  }

  /**
   * Returns the number of jobs in prioritized status.
   */
  getPrioritizedCount(): Promise<number> {
    return this.getJobCountByTypes('prioritized');
  }

  /**
   * Returns the number of jobs per priority.
   */
  async getCountsPerPriority(priorities: number[]): Promise<{
    [index: string]: number;
  }> {
    const uniquePriorities = [...new Set(priorities)];
    const responses = await this.scripts.getCountsPerPriority(uniquePriorities);

    const counts: { [index: string]: number } = {};
    responses.forEach((res, index) => {
      counts[`${uniquePriorities[index]}`] = res || 0;
    });

    return counts;
  }

  /**
   * Returns the number of jobs in waiting or paused statuses.
   */
  getWaitingCount(): Promise<number> {
    return this.getJobCountByTypes('waiting');
  }

  /**
   * Returns the number of jobs in waiting-children status.
   */
  getWaitingChildrenCount(): Promise<number> {
    return this.getJobCountByTypes('waiting-children');
  }

  /**
   * Returns the jobs that are in the "waiting" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getWaiting(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['waiting'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "waiting-children" status.
   * I.E. parent jobs that have at least one child that has not completed yet.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getWaitingChildren(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['waiting-children'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "active" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getActive(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['active'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "delayed" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getDelayed(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['delayed'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "prioritized" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getPrioritized(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['prioritized'], start, end, true);
  }

  /**
   * Returns the jobs that are in the "completed" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getCompleted(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['completed'], start, end, false);
  }

  /**
   * Returns the jobs that are in the "failed" status.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   */
  getFailed(start = 0, end = -1): Promise<JobBase[]> {
    return this.getJobs(['failed'], start, end, false);
  }

  /**
   * Returns the qualified job ids and the raw job data (if available) of the
   * children jobs of the given parent job.
   * It is possible to get either the already processed children, in this case
   * an array of qualified job ids and their result values will be returned,
   * or the pending children, in this case an array of qualified job ids will
   * be returned.
   * A qualified job id is a string representing the job id in a given queue,
   * for example: "bull:myqueue:jobid".
   *
   * @param parentId - The id of the parent job
   * @param type - "processed" | "pending"
   * @param opts - Options for the query.
   *
   * @returns an object with the following shape:
   * `{ items: { id: string, v?: any, err?: string } [], jobs: JobJsonRaw[], total: number}`
   */
  async getDependencies(
    parentId: string,
    type: 'processed' | 'pending',
    start: number,
    end: number,
  ): Promise<{
    items: { id: string; v?: any; err?: string }[];
    jobs: JobJsonRaw[];
    total: number;
  }> {
    const key = this.toKey(
      type == 'processed'
        ? `${parentId}:processed`
        : `${parentId}:dependencies`,
    );
    const { items, total, jobs } = await this.scripts.paginate(key, {
      start,
      end,
      fetchJobs: true,
    });
    return {
      items,
      jobs: jobs ?? [],
      total,
    };
  }

  async getRanges(
    types: JobType[],
    start = 0,
    end = 1,
    asc = false,
  ): Promise<string[]> {
    const ordering: ('fifo' | 'sorted')[] = [];
    this.classifyJobTypes(types, (_key, kind) => ordering.push(kind));

    const responses = await this.scripts.getRanges(types, start, end, asc);
    let results: string[] = [];

    responses.forEach((response: string[], index: number) => {
      const result = response || [];
      // FIFO states are stored insertion-first; ascending iteration of an
      // existing range needs reversal to match the caller's expectation.
      if (asc && ordering[index] === 'fifo') {
        results = results.concat(result.reverse());
      } else {
        results = results.concat(result);
      }
    });

    return [...new Set(results)];
  }

  /**
   * Returns the jobs that are on the given statuses (note that JobType is synonym for job status)
   * @param types - the statuses of the jobs to return.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   * @param asc - if true, the jobs will be returned in ascending order.
   */
  async getJobs(
    types?: JobType[] | JobType,
    start = 0,
    end = -1,
    asc = false,
  ): Promise<JobBase[]> {
    const currentTypes = this.sanitizeJobTypes(types);

    const jobIds = await this.getRanges(currentTypes, start, end, asc);

    return Promise.all(
      jobIds.map(jobId => this.Job.fromId(this, jobId) as Promise<JobBase>),
    );
  }

  /**
   * Returns the logs for a given Job.
   * @param jobId - the id of the job to get the logs for.
   * @param start - zero based index from where to start returning jobs.
   * @param end - zero based index where to stop returning jobs.
   * @param asc - if true, the jobs will be returned in ascending order.
   */
  async getJobLogs(
    jobId: string,
    start = 0,
    end = -1,
    asc = true,
  ): Promise<{ logs: string[]; count: number }> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const {
      rows: [jp],
    } = await client.query<{ pk: string }>(
      `select pk::text from ${S}.emq_jobs where queue_id = $1 and job_id = $2`,
      [qid, jobId],
    );
    if (!jp) {
      return { logs: [], count: 0 };
    }
    const {
      rows: [{ c }],
    } = await client.query<{ c: string }>(
      `select count(*)::text as c from ${S}.emq_job_logs where job_pk = $1::bigint`,
      [jp.pk],
    );
    const count = Number(c);
    const { rows: allRows } = await client.query<{ line: string }>(
      `select line from ${S}.emq_job_logs where job_pk = $1::bigint order by seq asc`,
      [jp.pk],
    );
    let lines = allRows.map(r => r.line);
    if (!asc) {
      lines = lines.slice().reverse();
    }
    const s = start < 0 ? 0 : start;
    const e = end < 0 ? lines.length : end + 1;
    return { logs: lines.slice(s, e), count };
  }

  private async baseGetClients(matcher: (name: string) => boolean): Promise<
    {
      [index: string]: string;
    }[]
  > {
    const client = await this.client;
    const { rows } = await client.query<{ a: string; p: number }>(
      `select application_name as a, pid as p from pg_stat_activity
       where application_name is not null and application_name <> ''`,
    );
    // The `{ name, rawname }` shape mirrors what consumers reading worker
    // lists expect; here both fields hold the `application_name`.
    return rows
      .map(r => ({ name: r.a, rawname: r.a }))
      .filter(r => matcher(r.name));
  }

  /**
   * Get the QueueEvents instances related to the queue. i.e. all the known
   * QueueEvents listeners that are subscribed to this queue's event stream.
   */
  getQueueEvents(): Promise<
    {
      [index: string]: string;
    }[]
  > {
    const queueEventsClientName = `${this.clientName(QUEUE_EVENT_SUFFIX)}`;

    const matcher = (name: string): boolean =>
      !!(name && name === queueEventsClientName);

    return this.baseGetClients(matcher);
  }

  /**
   * Get the worker list related to the queue. i.e. all the known
   * workers that are available to process jobs for this queue.
   * Note: GCP does not support SETNAME, so this call will not work
   *
   * @returns - Returns an array with workers info.
   */
  getWorkers(): Promise<
    {
      [index: string]: string;
    }[]
  > {
    const unnamedWorkerClientName = `${this.clientName()}`;
    const namedWorkerClientName = `${this.clientName()}:w:`;

    const matcher = (name: string): boolean =>
      !!(
        name &&
        (name === unnamedWorkerClientName ||
          name.startsWith(namedWorkerClientName))
      );

    return this.baseGetClients(matcher);
  }

  /**
   * Returns the current count of workers for the queue.
   *
   * getWorkersCount(): Promise<number>
   *
   */
  async getWorkersCount(): Promise<number> {
    const workers = await this.getWorkers();
    return workers.length;
  }

  /**
   * Get queue metrics related to the queue.
   *
   * This method returns the gathered metrics for the queue.
   * The metrics are represented as an array of job counts
   * per unit of time (1 minute).
   *
   * @param start - Start point of the metrics, where 0
   * is the newest point to be returned.
   * @param end - End point of the metrics, where -1 is the
   * oldest point to be returned.
   *
   * @returns - Returns an object with queue metrics.
   */
  async getMetrics(
    type: 'completed' | 'failed',
    start = 0,
    end = -1,
  ): Promise<Metrics> {
    const [meta, data, count] = await this.scripts.getMetrics(type, start, end);

    return {
      meta: {
        count: parseInt(meta[0] || '0', 10),
        prevTS: parseInt(meta[1] || '0', 10),
        prevCount: parseInt(meta[2] || '0', 10),
      },
      data: data.map(point => +point || 0),
      count,
    };
  }

  private parseClientList(list: string, matcher: (name: string) => boolean) {
    const lines = list.split(/\r?\n/);
    const clients: { [index: string]: string }[] = [];

    lines.forEach((line: string) => {
      const client: { [index: string]: string } = {};
      const keyValues = line.split(' ');
      keyValues.forEach(function (keyValue) {
        const index = keyValue.indexOf('=');
        const key = keyValue.substring(0, index);
        const value = keyValue.substring(index + 1);
        client[key] = value;
      });
      const name = client['name'];
      if (matcher(name)) {
        client['name'] = this.name;
        client['rawname'] = name;
        clients.push(client);
      }
    });
    return clients;
  }

  /**
   * Export the metrics for the queue in the Prometheus format.
   * Automatically exports all the counts returned by getJobCounts().
   *
   * @returns - Returns a string with the metrics in the Prometheus format.
   *
   * @see {@link https://prometheus.io/docs/instrumenting/exposition_formats/}
   *
   **/
  async exportPrometheusMetrics(
    globalVariables?: Record<string, string>,
  ): Promise<string> {
    const counts = await this.getJobCounts();
    const metrics: string[] = [];

    // Match the test's expected HELP text
    metrics.push(
      '# HELP bullmq_job_count Number of jobs in the queue by state',
    );
    metrics.push('# TYPE bullmq_job_count gauge');

    const variables = !globalVariables
      ? ''
      : Object.keys(globalVariables).reduce(
          (acc, curr) => `${acc}, ${curr}="${globalVariables[curr]}"`,
          '',
        );

    for (const [state, count] of Object.entries(counts)) {
      metrics.push(
        `bullmq_job_count{queue="${this.name}", state="${state}"${variables}} ${count}`,
      );
    }

    return metrics.join('\n');
  }
}
