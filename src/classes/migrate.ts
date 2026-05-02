import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';
import type { Pool, PoolClient } from 'pg';

const MIGRATE_LOCK_KEY = 'elephantmq_schema_migrate';

// Capture real timers at module load time so the advisory-lock retry loop is
// unaffected by tests that fake `setTimeout` via `vi.useFakeTimers()` etc.
const realSetTimeout: typeof setTimeout = (globalThis as any).setTimeout;

function escapeIdent(schema: string): string {
  return '"' + schema.replace(/"/g, '""') + '"';
}

function substituteSchema(sql: string, schema: string): string {
  const q = escapeIdent(schema);
  // `:EMQ_SCHEMA_NAME_LIT` substitutes the schema name as a SQL string
  // literal (used by catalog lookups that key on the text name).
  const qLit = "'" + schema.replace(/'/g, "''") + "'";
  return sql
    .replace(/:EMQ_SCHEMA_NAME_LIT\b/g, qLit)
    .replace(/:EMQ_SCHEMA\b/g, q);
}

function migrationFiles(): string[] {
  const dir = join(__dirname, '..', 'sql', 'migrations');
  return readdirSync(dir)
    .filter(f => f.endsWith('.sql'))
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
}

/** Sorted SQL files under `src/sql/functions`; reapplied on every migrate. */
export function functionSqlFiles(): string[] {
  const dir = join(__dirname, '..', 'sql', 'functions');
  try {
    return readdirSync(dir)
      .filter(f => f.endsWith('.sql'))
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  } catch {
    return [];
  }
}

function migrationNumber(filename: string): number {
  const m = filename.match(/^(\d+)/);
  return m ? parseInt(m[1], 10) : 0;
}

export async function checkPostgresVersion(client: PoolClient): Promise<void> {
  const {
    rows: [row],
  } = await client.query(
    "select current_setting('server_version_num') as server_version_num",
  );
  const n = parseInt(row.server_version_num, 10);
  if (n < 140000) {
    throw new Error(
      `elephantmq requires PostgreSQL >= 14 (server_version_num=${row.server_version_num})`,
    );
  }
}

async function isApplied(
  client: PoolClient,
  qSchema: string,
  id: number,
): Promise<boolean> {
  try {
    const r = await client.query(
      `select 1 as ok from ${qSchema}.emq_migrations where id = $1 limit 1`,
      [id],
    );
    return r.rows.length > 0;
  } catch (e: any) {
    if (e.code === '42P01') {
      return false;
    }
    throw e;
  }
}

async function tryAdvisoryLockWithRetries(
  client: PoolClient,
  schema: string,
  maxAttempts = 50,
  delayMs = 100,
): Promise<boolean> {
  for (let i = 0; i < maxAttempts; i++) {
    const {
      rows: [r],
    } = await client.query(
      `select pg_try_advisory_lock(hashtext($1::text), hashtext($2::text)) as ok`,
      [MIGRATE_LOCK_KEY, schema],
    );
    if (r?.ok) {
      return true;
    }
    await new Promise(res => realSetTimeout(res, delayMs));
  }
  return false;
}

/**
 * Apply pending DDL migrations under `src/sql/migrations` (advisory-locked
 * per-schema), then unconditionally reapply every PL/pgSQL function file
 * under `src/sql/functions` so consumers always get the latest function
 * definitions without needing to bump migration numbers.
 *
 * @param pool - `pg.Pool` to migrate against.
 * @param schema - Schema to install elephantmq into. Created if it does not
 *   already exist. Defaults to `'public'`.
 */
export async function migrate(pool: Pool, schema = 'public'): Promise<void> {
  const client = await pool.connect();
  const qSchema = escapeIdent(schema);
  try {
    await checkPostgresVersion(client);
    const locked = await tryAdvisoryLockWithRetries(client, schema);
    if (!locked) {
      throw new Error('elephantmq migrate: could not acquire advisory lock');
    }
    try {
      await client.query(`create schema if not exists ${qSchema}`);

      const files = migrationFiles();
      const seenIds = new Set<number>();
      for (const file of files) {
        const id = migrationNumber(file);
        if (!id) {
          continue;
        }
        if (seenIds.has(id)) {
          throw new Error(
            `elephantmq migrate: duplicate migration id ${id} in "${file}"`,
          );
        }
        seenIds.add(id);
      }

      for (const file of files) {
        const id = migrationNumber(file);
        if (!id) {
          continue;
        }
        if (await isApplied(client, qSchema, id)) {
          continue;
        }
        await client.query('begin');
        try {
          const raw = readFileSync(
            join(__dirname, '..', 'sql', 'migrations', file),
            'utf8',
          );
          await client.query(substituteSchema(raw, schema));
          await client.query(
            `insert into ${qSchema}.emq_migrations (id) values ($1) on conflict do nothing`,
            [id],
          );
          await client.query('commit');
        } catch (e) {
          await client.query('rollback');
          throw e;
        }
      }

      // Functions are idempotent (CREATE OR REPLACE) so they can be applied
      // outside the migration tracker. Running them every migrate keeps
      // existing schemas in sync with the current source tree without bumping
      // migration ids.
      const funcDir = join(__dirname, '..', 'sql', 'functions');
      for (const ff of functionSqlFiles()) {
        const raw = readFileSync(join(funcDir, ff), 'utf8');
        await client.query(substituteSchema(raw, schema));
      }
    } finally {
      await client.query(
        `select pg_advisory_unlock(hashtext($1::text), hashtext($2::text))`,
        [MIGRATE_LOCK_KEY, schema],
      );
    }
  } finally {
    client.release();
  }
}
