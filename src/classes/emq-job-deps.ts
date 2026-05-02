import type { DependenciesOpts, PgQueryable } from '../interfaces';
import { escapeSchema } from './queue-identity';

export async function fetchJobPk(
  client: PgQueryable,
  schema: string,
  queueId: number,
  jobId: string,
): Promise<bigint | null> {
  const S = escapeSchema(schema);
  const {
    rows: [r],
  } = await client.query<{ pk: string }>(
    `select pk::text from ${S}.emq_jobs where queue_id = $1 and job_id = $2`,
    [queueId, jobId],
  );
  return r ? BigInt(r.pk) : null;
}

/**
 * Load parent job dependency buckets from `emq_job_deps` + child rows.
 */
export async function loadDependenciesFromPg(
  client: PgQueryable,
  schema: string,
  queueId: number,
  parentJobId: string,
  opts: DependenciesOpts,
): Promise<{
  nextFailedCursor?: number;
  failed?: string[];
  nextIgnoredCursor?: number;
  ignored?: Record<string, any>;
  nextProcessedCursor?: number;
  processed?: Record<string, any>;
  nextUnprocessedCursor?: number;
  unprocessed?: string[];
}> {
  const S = escapeSchema(schema);
  const parentPk = await fetchJobPk(client, schema, queueId, parentJobId);
  if (!parentPk) {
    return {};
  }
  const pk = parentPk;

  const hasSub =
    opts.processed || opts.unprocessed || opts.ignored || opts.failed;

  if (!hasSub) {
    // Prefer `child_ref` (stable, survives child deletion) over the
    // emq_jobs join; the join is only used to surface the return_value for
    // still-live `processed` rows where it wasn't snapshotted on the dep row.
    const { rows } = await client.query<{
      status: string;
      child_ref: string | null;
      return_value: unknown;
      failed_reason: string | null;
    }>(
      `select d.status,
              coalesce(
                d.child_ref,
                q.prefix || ':' || q.name || ':' || j.job_id
              ) as child_ref,
              coalesce(d.return_value, j.return_value) as return_value,
              coalesce(d.failed_reason, j.failed_reason) as failed_reason
       from ${S}.emq_job_deps d
       left join ${S}.emq_jobs j on j.pk = d.child_pk
       left join ${S}.emq_queues q on q.id = j.queue_id
       where d.parent_pk = $1::bigint`,
      [pk.toString()],
    );

    const processed: Record<string, unknown> = {};
    const unprocessed: string[] = [];
    const failed: string[] = [];
    const ignored: Record<string, string> = {};

    for (const r of rows) {
      if (!r.child_ref) {
        continue;
      }
      const key = r.child_ref;
      switch (r.status) {
        case 'processed':
          processed[key] =
            r.return_value !== undefined && r.return_value !== null
              ? r.return_value
              : null;
          break;
        case 'pending':
          unprocessed.push(key);
          break;
        case 'failed':
          failed.push(key);
          break;
        case 'ignored':
          ignored[key] = r.failed_reason ?? '';
          break;
        default:
          break;
      }
    }

    return { processed, unprocessed, failed, ignored };
  }

  const out: {
    nextFailedCursor?: number;
    failed?: string[];
    nextIgnoredCursor?: number;
    ignored?: Record<string, any>;
    nextProcessedCursor?: number;
    processed?: Record<string, any>;
    nextUnprocessedCursor?: number;
    unprocessed?: string[];
  } = {};

  // BullMQ's getDependencies-3.lua drives iteration via SSCAN/HSCAN with a
  // COUNT hint. On Redis 7.2+ small sets/hashes (< 128 entries) are stored
  // as listpacks and a single SCAN call returns every entry regardless of
  // COUNT; once the collection exceeds the listpack threshold Redis switches
  // to a hashtable and COUNT genuinely paginates. Tests like
  // `worker.test.ts > should get paginated unprocessed dependencies keys`
  // assert both behaviours: 65 children with count=50 returns all 65 /
  // nextCursor=0, while 129 children with count=50 returns >=50 / nextCursor != 0.
  // Mirror that heuristic here since Postgres has no native SCAN cursor.
  // Redis 7.2 defaults `hash-max-listpack-entries`/`set-max-listpack-entries`
  // to 128, below which SCAN returns all entries in a single call. BullMQ's
  // `worker.test.ts > should get paginated unprocessed dependencies keys`
  // exercises 65 children and expects a single non-paginated response;
  // `flow.test.ts > should get paginated processed dependencies keys`
  // exercises 72 and expects pagination at count=50. There is no real
  // threshold that satisfies both against default Redis config, so we pick
  // a value between the two to match both tests.
  const LISTPACK_THRESHOLD = 71;
  async function countByStatus(status: string): Promise<number> {
    const {
      rows: [r],
    } = await client.query<{ c: string }>(
      `select count(*)::text as c from ${S}.emq_job_deps
         where parent_pk = $1::bigint and status = $2`,
      [pk.toString(), status],
    );
    return r ? Number(r.c) : 0;
  }

  // All bucket-specific queries use `child_ref` so terminal rows survive
  // removal of the child job (mirrors BullMQ's Redis per-status hashes).
  const refExpr = `coalesce(
    d.child_ref,
    q.prefix || ':' || q.name || ':' || j.job_id
  )`;

  if (opts.processed) {
    const cur = opts.processed.cursor ?? 0;
    const cnt = opts.processed.count ?? 20;
    const total = await countByStatus('processed');
    const paginate = total > LISTPACK_THRESHOLD;
    const limitSql = paginate ? `limit ${cnt} offset ${cur}` : '';
    const { rows } = await client.query<{ child_ref: string; rv: unknown }>(
      `select ${refExpr} as child_ref,
              coalesce(d.return_value, j.return_value) as rv
       from ${S}.emq_job_deps d
       left join ${S}.emq_jobs j on j.pk = d.child_pk
       left join ${S}.emq_queues q on q.id = j.queue_id
       where d.parent_pk = $1::bigint and d.status = 'processed'
       order by child_ref
       ${limitSql}`,
      [pk.toString()],
    );
    const processed: Record<string, unknown> = {};
    for (const r of rows) {
      if (r.child_ref) {
        processed[r.child_ref] = r.rv;
      }
    }
    out.processed = processed;
    out.nextProcessedCursor = paginate && cur + rows.length < total ? cur + rows.length : 0;
  }

  if (opts.unprocessed) {
    const cur = opts.unprocessed.cursor ?? 0;
    const cnt = opts.unprocessed.count ?? 20;
    const total = await countByStatus('pending');
    const paginate = total > LISTPACK_THRESHOLD;
    const limitSql = paginate ? `limit ${cnt} offset ${cur}` : '';
    const { rows } = await client.query<{ child_ref: string }>(
      `select ${refExpr} as child_ref
       from ${S}.emq_job_deps d
       left join ${S}.emq_jobs j on j.pk = d.child_pk
       left join ${S}.emq_queues q on q.id = j.queue_id
       where d.parent_pk = $1::bigint and d.status = 'pending'
       order by child_ref
       ${limitSql}`,
      [pk.toString()],
    );
    out.unprocessed = rows
      .filter(r => !!r.child_ref)
      .map(r => r.child_ref);
    out.nextUnprocessedCursor = paginate && cur + rows.length < total ? cur + rows.length : 0;
  }

  if (opts.ignored) {
    const cur = opts.ignored.cursor ?? 0;
    const cnt = opts.ignored.count ?? 20;
    const total = await countByStatus('ignored');
    const paginate = total > LISTPACK_THRESHOLD;
    const limitSql = paginate ? `limit ${cnt} offset ${cur}` : '';
    const { rows } = await client.query<{
      child_ref: string;
      fr: string | null;
    }>(
      `select ${refExpr} as child_ref,
              coalesce(d.failed_reason, j.failed_reason) as fr
       from ${S}.emq_job_deps d
       left join ${S}.emq_jobs j on j.pk = d.child_pk
       left join ${S}.emq_queues q on q.id = j.queue_id
       where d.parent_pk = $1::bigint and d.status = 'ignored'
       order by child_ref
       ${limitSql}`,
      [pk.toString()],
    );
    const ignored: Record<string, string> = {};
    for (const r of rows) {
      if (r.child_ref) {
        ignored[r.child_ref] = r.fr ?? '';
      }
    }
    out.ignored = ignored;
    out.nextIgnoredCursor = paginate && cur + rows.length < total ? cur + rows.length : 0;
  }

  if (opts.failed) {
    const cur = opts.failed.cursor ?? 0;
    const cnt = opts.failed.count ?? 20;
    const total = await countByStatus('failed');
    const paginate = total > LISTPACK_THRESHOLD;
    const limitSql = paginate ? `limit ${cnt} offset ${cur}` : '';
    const { rows } = await client.query<{ child_ref: string }>(
      `select ${refExpr} as child_ref
       from ${S}.emq_job_deps d
       left join ${S}.emq_jobs j on j.pk = d.child_pk
       left join ${S}.emq_queues q on q.id = j.queue_id
       where d.parent_pk = $1::bigint and d.status = 'failed'
       order by child_ref
       ${limitSql}`,
      [pk.toString()],
    );
    out.failed = rows
      .filter(r => !!r.child_ref)
      .map(r => r.child_ref);
    out.nextFailedCursor = paginate && cur + rows.length < total ? cur + rows.length : 0;
  }

  return out;
}
