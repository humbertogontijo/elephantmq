import type { JobJsonRaw } from '../../interfaces';
import type {
  EncodedJobOptions,
  FinishedPropValAttribute,
  FinishedStatus,
} from '../../types';
import { array2obj } from '../../utils';

/**
 * Result tuple shape returned to {@link Worker} for the next runnable job.
 * Layout matches what BullMQ's Lua scripts produced: `[flatFields, jobId, rateLimitDelayMs, blockUntilMs]`.
 */
export type JobData = [JobJsonRaw | number, string?];

/** Typed input for `emq_move_to_finished_v1`. */
export interface MoveToFinishedParams {
  jobId: string;
  timestamp: number;
  token: string;
  target: FinishedStatus;
  val: any;
  propVal: FinishedPropValAttribute;
  fetchNext: boolean;
  lockDurationMs: number;
  /** Null = no trim by count (BullMQ unlimited). */
  keepJobsCount: number | null;
  /** Age trim window in milliseconds; null = no age trim. */
  keepJobsAgeMs: number | null;
  /** Optional failure bookkeeping carried through via Job.moveToFailed. */
  failedReason?: string | null;
  stacktrace?: string[] | null;
  /** Metrics rolling-window size (from worker.opts.metrics.maxDataPoints). */
  maxMetricsSize?: number | null;
}

/** Payload shared by emq add-job family. */
export type AddJobSqlPayload = {
  customId: string;
  name: string;
  timestamp: number;
  parentQueueKey: string;
  parentDepKey: string;
  parent: Record<string, unknown>;
  repeatKey: string | null;
  dedupId: string | null;
};

export function mapListKeyToState(listKey: string): string {
  if (listKey.includes(':paused')) {
    return 'paused';
  }
  if (listKey.includes(':delayed')) {
    return 'delayed';
  }
  if (listKey.includes(':prioritized')) {
    return 'prioritized';
  }
  if (listKey.includes(':completed')) {
    return 'completed';
  }
  if (listKey.includes(':failed')) {
    return 'failed';
  }
  if (listKey.includes(':waiting-children')) {
    return 'waiting-children';
  }
  if (listKey.includes(':active')) {
    return 'active';
  }
  return 'wait';
}

export function normalizeJobDataJson(data: unknown): string {
  if (typeof data === 'string') {
    try {
      JSON.parse(data);
      return data;
    } catch {
      return JSON.stringify(data);
    }
  }
  return JSON.stringify(data ?? {});
}

/**
 * Pull the failure bookkeeping fields (stacktrace array + failedReason) out of
 * the shared `fieldsToUpdate` bag passed by `Job.moveToFailed` when it routes
 * into `retryJob` / `moveToDelayed`. Mirrors BullMQ's `updateJobFields` in
 * `retryJob-8.lua` / `moveToDelayed-8.lua` so callers see the latest error
 * even when the job is retried rather than finalised.
 */
export function extractFailureFields(fields?: {
  failedReason?: string;
  stacktrace?: string;
}): { failedReason: string | null; stacktrace: string[] | null } {
  if (!fields) {
    return { failedReason: null, stacktrace: null };
  }
  let trace: string[] | null = null;
  if (typeof fields.stacktrace === 'string' && fields.stacktrace.length > 0) {
    try {
      const parsed = JSON.parse(fields.stacktrace);
      if (Array.isArray(parsed)) {
        trace = parsed.map(String);
      }
    } catch {
      trace = null;
    }
  }
  return {
    failedReason: fields.failedReason ?? null,
    stacktrace: trace,
  };
}

export function encodeEncodedJobOptions(opts: EncodedJobOptions): string {
  if (opts.repeat) {
    const repeat = {
      ...opts.repeat,
    };
    if (repeat.startDate) {
      repeat.startDate = +new Date(repeat.startDate as Date);
    }
    if (repeat.endDate) {
      repeat.endDate = +new Date(repeat.endDate as Date);
    }
    return JSON.stringify({ ...opts, repeat });
  }
  return JSON.stringify(opts);
}

export function raw2NextJobData(raw: any[]) {
  if (raw) {
    const result = [null, raw[1], raw[2], raw[3]];
    const head = raw[0];
    if (head && !(Array.isArray(head) && head.length === 0)) {
      result[0] = array2obj(head);
    }
    return result;
  }
  return [];
}
