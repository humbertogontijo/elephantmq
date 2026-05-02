import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { randomBytes } from 'crypto';
import { migrate } from '../../src/classes/migrate';
import { testPool } from '../test_context';

describe('migrate', () => {
  let schema: string;

  beforeAll(async () => {
    schema = 'emq_mig_' + randomBytes(8).toString('hex');
  });

  afterAll(async () => {
    await testPool.query(`drop schema if exists "${schema}" cascade`);
  });

  it('creates emq_* tables and is idempotent', async () => {
    await migrate(testPool, schema);

    const tablesQuery = `
      select table_name
      from information_schema.tables
      where table_schema = $1
      order by table_name`;
    const { rows } = await testPool.query(tablesQuery, [schema]);
    const names = rows.map(r => r.table_name);

    expect(names).toContain('emq_jobs');
    expect(names).toContain('emq_queues');
    expect(names).toContain('emq_events');
    expect(names).toContain('emq_migrations');

    // Re-run should be a no-op.
    await migrate(testPool, schema);
    const { rows: rows2 } = await testPool.query(tablesQuery, [schema]);
    expect(rows2.length).toBe(rows.length);
  });
});
