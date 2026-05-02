import type { JobJsonRaw, MinimalQueue } from '../interfaces';
import { escapeSchema } from './queue-identity';
import { rowToJobJsonRaw } from './emq-mappers';

export async function fetchJobRowAsRaw(
  queue: MinimalQueue,
  jobId: string,
): Promise<JobJsonRaw | null> {
  const qid = await queue.queueId;
  const client = await queue.client;
  const S = escapeSchema(queue.schema);
  const {
    rows: [row],
  } = await client.query(
    `select j.*,
            case when pq.id is not null
                 then pq.prefix || ':' || pq.name
                 else null end as parent_queue_key
       from ${S}.emq_jobs j
       left join ${S}.emq_queues pq on pq.id = j.parent_queue_id
      where j.queue_id=$1 and j.job_id=$2`,
    [qid, jobId],
  );
  if (!row) {
    return null;
  }
  return rowToJobJsonRaw(row);
}
