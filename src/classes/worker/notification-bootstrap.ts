import type { EmqClient } from '../../interfaces';
import { escapeSchema } from '../queue-identity';

export interface NotificationBootstrapCtx {
  getBlockUntil: () => number;
  setBlockUntil: (next: number) => void;
  setPendingMarker: () => void;
  wakeupJobWaiters: (v: number) => void;
}

/**
 * Probe DB for waits/delayed rows after subscribing so NOTIFYs lost before LISTEN are not deadly.
 */
export async function bootstrapNotificationMarkersFromRows(
  client: EmqClient,
  queueId: number,
  schema: string,
  ctx: NotificationBootstrapCtx,
): Promise<void> {
  const S = escapeSchema(schema);
  const res = await client.query(
    `select 1
       from ${S}.emq_jobs
      where queue_id = $1
        and state in ('wait', 'prioritized', 'paused')
      limit 1`,
    [queueId],
  );
  if (res.rowCount && res.rowCount > 0) {
    ctx.setPendingMarker();
  }

  const {
    rows: [delayedRow],
  } = await client.query<{ next_ms: string | null }>(
    `select min(extract(epoch from process_at) * 1000)::text as next_ms
       from ${S}.emq_jobs
      where queue_id = $1 and state = 'delayed'`,
    [queueId],
  );
  const nextMs = delayedRow?.next_ms
    ? Math.floor(parseFloat(delayedRow.next_ms))
    : 0;
  if (nextMs > 0) {
    const nowMs = Date.now();
    if (nextMs <= nowMs) {
      ctx.setPendingMarker();
    } else {
      const blockUntil = ctx.getBlockUntil();
      if (!blockUntil || nextMs < blockUntil) {
        ctx.setBlockUntil(nextMs);
        ctx.wakeupJobWaiters(nextMs);
      }
    }
  }
}
