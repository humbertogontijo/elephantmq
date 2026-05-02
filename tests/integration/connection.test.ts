import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  testPool,
  uninstallTestSchema,
} from '../test_context';
import { delay, newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Postgres connection recovery', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it(
    'worker survives pg_terminate_backend on its LISTEN session',
    async () => {
      const queue = newQueue('pg-term', schema);
      const worker = newWorker('pg-term', async () => 'survived', schema, {
        name: 'pgterm-worker',
        concurrency: 1,
      });
      worker.on('error', () => undefined);

      try {
        await worker.waitUntilReady();

        const { rows: pids } = await testPool.query<{ pid: number }>(
          `select pid from pg_stat_activity
          where datname = current_database()
            and application_name like $1`,
          ['%w:pgterm-worker%'],
        );

        expect(pids.length).toBeGreaterThanOrEqual(1);
        const target = pids[0]!.pid;

        await testPool.query('select pg_terminate_backend($1::int)', [target]);

        await queue.add('after-term', {});
        await waitUntil(
          async () => (await queue.getCompletedCount()) >= 1,
          20_000,
        );
        await delay(500);
      } finally {
        await closeAll([worker, queue]);
      }
    },
    // Inner waitUntil may use the full 20s; with setup + cleanup this exceeds the default 20s test cap.
    60_000,
  );
});
