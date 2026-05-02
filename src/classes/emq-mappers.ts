import type { JobJsonRaw } from '../interfaces';

/** Prefer column; after TTL cleanup the id may only remain on opts (de / deduplication). */
function deduplicationIdFromOpts(
  opts: Record<string, unknown> | undefined,
): string | undefined {
  if (!opts || typeof opts !== 'object') {
    return undefined;
  }
  const de = opts.de as { id?: string } | undefined;
  const ded = opts.deduplication as { id?: string } | undefined;
  const id = de?.id ?? ded?.id;
  return typeof id === 'string' && id.length > 0 ? id : undefined;
}

/** DB row shape for emq_jobs (subset) */
export interface EmqJobRow {
  pk?: number;
  job_id: string;
  name: string;
  data: unknown;
  opts: Record<string, unknown>;
  state: string;
  priority: number;
  process_at: Date | string | null;
  wait_seq: string | number | null;
  delay_ms: number;
  attempts_made: number;
  attempts_started: number;
  max_attempts: number;
  failed_reason: string | null;
  stacktrace: string[] | null;
  return_value: unknown;
  progress: unknown;
  timestamp: Date | string;
  processed_on: Date | string | null;
  finished_on: Date | string | null;
  locked_by: string | null;
  lock_token: string | null;
  stalled_counter: number;
  processed_by: string | null;
  parent_job_id: string | null;
  repeat_job_key: string | null;
  deduplication_id: string | null;
  deferred_failure?: string | null;
}

export function rowToJobJsonRaw(row: EmqJobRow): JobJsonRaw {
  // `row.data` comes back already jsonb-decoded (so plain strings/numbers stay
  // as JS values, not JSON text). Consumers downstream do `JSON.parse(data)`
  // on the BullMQ side, so always re-encode — wrapping strings like 'delay' in
  // quotes so the parse round-trips.
  const dataStr = JSON.stringify(row.data ?? {});
  const optsStr = JSON.stringify(row.opts ?? {});
  const ts =
    row.timestamp instanceof Date
      ? row.timestamp.getTime()
      : new Date(row.timestamp).getTime();
  return {
    id: row.job_id,
    name: row.name,
    data: dataStr,
    delay: String(row.delay_ms ?? 0),
    opts: optsStr,
    progress: JSON.stringify(row.progress ?? 0),
    priority: String(row.priority ?? 0),
    timestamp: String(ts),
    attemptsMade: String(row.attempts_made ?? 0),
    attemptsStarted: String(row.attempts_started ?? 0),
    stalledCounter: String(row.stalled_counter ?? 0),
    failedReason: row.failed_reason ?? '',
    stacktrace: row.stacktrace?.length
      ? JSON.stringify(row.stacktrace)
      : '[]',
    returnvalue:
      row.return_value !== undefined && row.return_value !== null
        ? JSON.stringify(row.return_value)
        : 'null',
    processedOn: row.processed_on
      ? String(
          row.processed_on instanceof Date
            ? row.processed_on.getTime()
            : new Date(row.processed_on).getTime(),
        )
      : undefined,
    finishedOn: row.finished_on
      ? String(
          row.finished_on instanceof Date
            ? row.finished_on.getTime()
            : new Date(row.finished_on).getTime(),
        )
      : undefined,
    parent: row.parent_job_id
      ? JSON.stringify({
          id: row.parent_job_id,
          queueKey: (row as any).parent_queue_key ?? undefined,
        })
      : undefined,
    parentKey:
      row.parent_job_id && (row as any).parent_queue_key
        ? `${(row as any).parent_queue_key}:${row.parent_job_id}`
        : (row.parent_job_id ?? undefined),
    rjk: row.repeat_job_key ?? undefined,
    deid:
      row.deduplication_id ??
      deduplicationIdFromOpts(row.opts) ??
      undefined,
    pb: row.processed_by ?? undefined,
    defa: row.deferred_failure ?? undefined,
  };
}

/** Flat array for array2obj in moveToActive (HGETALL-like order) */
/** Map jsonb job row from emq_*_v1 into EmqJobRow (moveToActive / moveToFinished). */
export function jsonJobRowFromDb(raw: unknown): EmqJobRow {
  const o = raw as Record<string, unknown>;
  return {
    job_id: String(o.job_id),
    name: String(o.name),
    data: o.data,
    opts: (o.opts as Record<string, unknown>) || {},
    state: String(o.state),
    priority: Number(o.priority ?? 0),
    process_at: o.process_at as Date | string | null,
    wait_seq: o.wait_seq as number | string | null,
    delay_ms: Number(o.delay_ms ?? 0),
    attempts_made: Number(o.attempts_made ?? 0),
    attempts_started: Number(o.attempts_started ?? 0),
    max_attempts: Number(o.max_attempts ?? 1),
    failed_reason: (o.failed_reason as string) ?? null,
    stacktrace: (o.stacktrace as string[]) ?? null,
    return_value: o.return_value,
    progress: o.progress,
    timestamp: o.timestamp as Date | string,
    processed_on: o.processed_on as Date | string | null,
    finished_on: o.finished_on as Date | string | null,
    locked_by: (o.locked_by as string) ?? null,
    lock_token: (o.lock_token as string) ?? null,
    stalled_counter: Number(o.stalled_counter ?? 0),
    processed_by: (o.processed_by as string) ?? null,
    parent_job_id: (o.parent_job_id as string) ?? null,
    repeat_job_key: (o.repeat_job_key as string) ?? null,
    deduplication_id:
      (o.deduplication_id as string | null) ??
      deduplicationIdFromOpts((o.opts as Record<string, unknown>) || {}) ??
      null,
    deferred_failure: (o.deferred_failure as string | null) ?? null,
  };
}

export function rowToFlatJobFields(row: EmqJobRow): string[] {
  const raw = rowToJobJsonRaw(row);
  const out: string[] = [];
  const push = (k: string, v: string) => {
    out.push(k, v);
  };
  push('id', raw.id);
  push('name', raw.name);
  push('data', raw.data);
  push('opts', raw.opts);
  push('delay', raw.delay);
  push('progress', raw.progress);
  push('priority', raw.priority);
  push('timestamp', raw.timestamp);
  push('attemptsMade', raw.attemptsMade || '0');
  push('atm', raw.attemptsMade || '0');
  push('ats', raw.attemptsStarted || '0');
  push('stc', raw.stalledCounter || '0');
  push('failedReason', raw.failedReason || '');
  push('stacktrace', raw.stacktrace || '[]');
  push('returnvalue', raw.returnvalue || '');
  if (raw.processedOn) {
    push('processedOn', raw.processedOn);
  }
  if (raw.finishedOn) {
    push('finishedOn', raw.finishedOn);
  }
  if (raw.parentKey) {
    push('parentKey', raw.parentKey);
  }
  if (raw.rjk) {
    push('rjk', raw.rjk);
  }
  if (raw.deid) {
    push('deid', raw.deid);
  }
  if (raw.pb) {
    push('pb', raw.pb);
  }
  if ((raw as any).defa) {
    push('defa', (raw as any).defa);
  }
  return out;
}
