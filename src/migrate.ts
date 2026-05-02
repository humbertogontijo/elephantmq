/**
 * Schema migration entry point.
 *
 * Use this from CLI scripts or one-shot tasks:
 *
 * ```ts
 * import { migrate } from 'elephantmq/migrate';
 * import { Pool } from 'pg';
 *
 * const pool = new Pool({ connectionString: process.env.DATABASE_URL });
 * await migrate(pool, 'public');
 * await pool.end();
 * ```
 */
export { migrate, checkPostgresVersion } from './classes/migrate';
