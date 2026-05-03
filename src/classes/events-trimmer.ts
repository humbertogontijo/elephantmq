import type { PgQueryable } from '../interfaces';
import { escapeSchema } from './queue-identity';

/** Partial queue opts for event stream trim (matches optional `streams.events` fields). */
export type EventsTrimQueueOpts = {
  streams?: {
    events?: { maxLen?: number; trim?: { every: number; maxLen?: number } };
  };
};

/** Resolve trim target length: `streams.events.trim.maxLen` overrides `streams.events.maxLen`. */
export function resolveEventsTrimMaxLen(
  opts: EventsTrimQueueOpts | undefined,
): number | undefined {
  const ev = opts?.streams?.events;
  const m = ev?.trim?.maxLen ?? ev?.maxLen;
  return typeof m === 'number' && m > 0 ? m : undefined;
}

/**
 * Best-effort trim of `emq_events` to approximately `max_len_events` rows per queue.
 */
export async function trimEventsForQueue(
  client: PgQueryable,
  schema: string,
  queueId: number,
  maxLen: number,
): Promise<number> {
  if (maxLen <= 0) {
    return 0;
  }
  const S = escapeSchema(schema);
  const {
    rows: [row],
  } = await client.query<{ m: string | null }>(
    `select max(id)::text as m from ${S}.emq_events where queue_id = $1`,
    [queueId],
  );
  if (!row?.m) {
    return 0;
  }
  const maxId = parseInt(row.m, 10);
  const threshold = Math.max(0, maxId - maxLen);
  const r = await client.query(
    `delete from ${S}.emq_events e
     where e.id in (
       select e2.id from ${S}.emq_events e2
       where e2.queue_id = $1 and e2.id < $2::bigint
       order by e2.id
       for update
     )`,
    [queueId, String(threshold)],
  );
  return r.rowCount ?? 0;
}

/**
 * Periodically trim `emq_events` for a queue (best-effort).
 * Call `stop()` on the returned handle to clear the interval.
 */
export function schedulePeriodicTrim(
  queue: {
    client: Promise<PgQueryable>;
    queueId: Promise<number>;
    schema: string;
    opts: EventsTrimQueueOpts;
  },
  intervalMs: number,
): { stop: () => void } {
  const id = setInterval(() => {
    void queue.client.then(async client => {
      const qid = await queue.queueId;
      const maxLen = resolveEventsTrimMaxLen(queue.opts);
      if (maxLen && maxLen > 0) {
        await trimEventsForQueue(client, queue.schema, qid, maxLen);
      }
    });
  }, Math.max(1000, intervalMs));
  return {
    stop: () => clearInterval(id),
  };
}
