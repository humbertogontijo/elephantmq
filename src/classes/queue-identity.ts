import type { Pool, PoolClient } from 'pg';

export function escapeSchema(schema: string): string {
  return '"' + schema.replace(/"/g, '""') + '"';
}

/** Resolve PostgreSQL schema from queue/connection options (default `public`). */
export function resolveSchema(opts: {
  schema?: string;
  connection?: unknown;
}): string {
  return (
    opts.schema ||
    (typeof opts.connection === 'object' &&
      opts.connection !== null &&
      'schema' in opts.connection &&
      typeof (opts.connection as { schema?: string }).schema === 'string' &&
      (opts.connection as { schema?: string }).schema) ||
    'public'
  );
}

function isRetryablePgConflict(err: unknown): boolean {
  const code =
    typeof err === 'object' &&
    err !== null &&
    'code' in err &&
    typeof (err as { code: unknown }).code === 'string'
      ? (err as { code: string }).code
      : '';
  return code === '40P01' || code === '40001';
}

/**
 * Ensure a row exists in emq_queues and return its id.
 */
export async function ensureQueueRow(
  client: Pool | PoolClient,
  schema: string,
  prefix: string,
  name: string,
): Promise<number> {
  const s = escapeSchema(schema);
  const sql = `insert into ${s}.emq_queues (prefix, name) values ($1, $2)
     on conflict (prefix, name) do update set updated_at = now()
     returning id`;
  const params = [prefix, name];
  let lastErr: unknown;
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const {
        rows: [row],
      } = await client.query<{ id: string }>(sql, params);
      return Number(row.id);
    } catch (e) {
      if (!isRetryablePgConflict(e)) {
        throw e;
      }
      lastErr = e;
      await new Promise<void>(resolve => {
        setTimeout(resolve, 15 + attempt * 25);
      });
    }
  }
  throw lastErr;
}
