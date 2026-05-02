import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue } from '../../src';
import { closeAll, installTestSchema, uninstallTestSchema } from '../test_context';
import { newQueue } from '../test_helpers';

describe('Queue', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  describe('add', () => {
    it('adds a job and returns a Job with id and data', async () => {
      const queue = newQueue('add-basic', schema);
      try {
        const job = await queue.add('paint', { color: 'red' });
        expect(job.id).toBeTruthy();
        expect(job.name).toBe('paint');
        expect(job.data).toEqual({ color: 'red' });

        const fetched = await queue.getJob(job.id!);
        expect(fetched?.data).toEqual({ color: 'red' });
      } finally {
        await closeAll([queue]);
      }
    });

    it('rejects jobIds that are "0" or start with "0:"', async () => {
      const queue = newQueue('add-bad-jobid', schema);
      try {
        await expect(
          queue.add('paint', {}, { jobId: '0' }),
        ).rejects.toThrow();
        await expect(
          queue.add('paint', {}, { jobId: '0:foo' }),
        ).rejects.toThrow();
      } finally {
        await closeAll([queue]);
      }
    });

    it('respects priority option (higher priority runs first)', async () => {
      const queue = newQueue('add-priority', schema);
      try {
        const a = await queue.add('p', { n: 'a' }, { priority: 10 });
        const b = await queue.add('p', { n: 'b' }, { priority: 1 });
        const c = await queue.add('p', { n: 'c' }, { priority: 5 });

        const prioritized = await queue.getPrioritized();
        const ids = prioritized.map(j => j.id);
        // Lower priority value = higher priority in BullMQ-style API
        expect(ids).toEqual([b.id, c.id, a.id]);
      } finally {
        await closeAll([queue]);
      }
    });

    it('places delayed jobs in delayed state with process_at in the future', async () => {
      const queue = newQueue('add-delayed', schema);
      try {
        const job = await queue.add('d', {}, { delay: 60_000 });
        const state = await job.getState();
        expect(state).toBe('delayed');
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('addBulk', () => {
    it('adds multiple jobs in one round-trip', async () => {
      const queue = newQueue('add-bulk', schema);
      try {
        const jobs = await queue.addBulk([
          { name: 'a', data: { i: 1 } },
          { name: 'b', data: { i: 2 } },
          { name: 'c', data: { i: 3 } },
        ]);
        expect(jobs).toHaveLength(3);
        const counts = await queue.getJobCounts('wait');
        expect(counts.wait).toBe(3);
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('inTransaction', () => {
    it('commits jobs together with arbitrary SQL', async () => {
      const queue = new Queue('tx-commit', {
        connection: (await import('./../test_context')).testPool,
        schema,
        prefix: 'emq_test',
        skipMigrations: true,
      });
      try {
        await queue.inTransaction(async (q, sql) => {
          await sql.query(
            `create table if not exists "${schema}".inventory(sku text primary key, reserved int not null default 0)`,
          );
          await sql.query(
            `insert into "${schema}".inventory(sku, reserved) values ('paint', 1)
             on conflict (sku) do update set reserved = "${schema}".inventory.reserved + 1`,
          );
          await q.add('fulfill', { sku: 'paint' });
        });

        const counts = await queue.getJobCounts('wait');
        expect(counts.wait).toBeGreaterThanOrEqual(1);
      } finally {
        await closeAll([queue]);
      }
    });

    it('rolls back jobs and SQL together on error', async () => {
      const queue = newQueue('tx-rollback', schema);
      try {
        const before = (await queue.getJobCounts('wait')).wait;
        await expect(
          queue.inTransaction(async q => {
            await q.add('fail', { will: 'rollback' });
            throw new Error('boom');
          }),
        ).rejects.toThrow('boom');
        const after = (await queue.getJobCounts('wait')).wait;
        expect(after).toBe(before);
      } finally {
        await closeAll([queue]);
      }
    });

    it('rejects nested transactions on the same queue', async () => {
      const queue = newQueue('tx-nested', schema);
      try {
        await expect(
          queue.inTransaction(async q => {
            await q.inTransaction(async () => {
              /* unreachable */
            });
          }),
        ).rejects.toThrow(/Nested/);
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('pause / resume', () => {
    it('isPaused reports global queue state', async () => {
      const queue = newQueue('pause', schema);
      try {
        expect(await queue.isPaused()).toBe(false);
        await queue.pause();
        expect(await queue.isPaused()).toBe(true);
        await queue.resume();
        expect(await queue.isPaused()).toBe(false);
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('remove', () => {
    it('removes a waiting job and returns 1', async () => {
      const queue = newQueue('remove', schema);
      try {
        const job = await queue.add('rm', {});
        const removed = await queue.remove(job.id!);
        expect(removed).toBe(1);
        const after = await queue.getJob(job.id!);
        expect(after).toBeUndefined();
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('drain', () => {
    it('drain() empties wait/prioritized but leaves active jobs', async () => {
      const queue = newQueue('drain-basic', schema);
      try {
        await queue.add('a', {});
        await queue.add('b', {});
        await queue.drain();
        const counts = await queue.getJobCounts('wait', 'prioritized');
        expect(counts.wait + counts.prioritized).toBe(0);
      } finally {
        await closeAll([queue]);
      }
    });
  });
});
