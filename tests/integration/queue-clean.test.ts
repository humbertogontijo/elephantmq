import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  testPool,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Queue.clean / drain', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('clean(grace, limit, "completed") removes completed jobs older than the grace window', async () => {
    const queue = newQueue('clean-completed', schema);
    const worker = newWorker('clean-completed', async () => 'ok', schema);
    try {
      for (let i = 0; i < 5; i++) {
        await queue.add('a', { i });
      }
      await waitUntil(
        async () => (await queue.getCompletedCount()) === 5,
        10_000,
      );
      // grace=0 removes everything, limit=10 caps the batch.
      const removed = await queue.clean(0, 10, 'completed');
      expect(removed.length).toBe(5);
      expect(await queue.getCompletedCount()).toBe(0);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('clean(grace, limit, "failed") removes failed jobs', async () => {
    const queue = newQueue('clean-failed', schema);
    const worker = newWorker(
      'clean-failed',
      async () => {
        throw new Error('nope');
      },
      schema,
    );
    try {
      for (let i = 0; i < 3; i++) {
        await queue.add('a', { i });
      }
      await waitUntil(
        async () => (await queue.getFailedCount()) === 3,
        10_000,
      );
      const removed = await queue.clean(0, 10, 'failed');
      expect(removed.length).toBe(3);
      expect(await queue.getFailedCount()).toBe(0);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('clean respects the grace period', async () => {
    const queue = newQueue('clean-grace', schema);
    const worker = newWorker('clean-grace', async () => 'ok', schema);
    try {
      await queue.add('a', {});
      await waitUntil(
        async () => (await queue.getCompletedCount()) === 1,
        10_000,
      );
      const removed = await queue.clean(60_000, 10, 'completed');
      expect(removed.length).toBe(0);
      expect(await queue.getCompletedCount()).toBe(1);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('drain(true) removes wait, prioritized AND delayed jobs', async () => {
    const queue = newQueue('drain-with-delayed', schema);
    try {
      await queue.add('w', {});
      await queue.add('p', {}, { priority: 1 });
      await queue.add('d', {}, { delay: 60_000 });

      expect(await queue.count()).toBe(3);
      await queue.drain(true);
      expect(await queue.getJobCountByTypes('wait', 'prioritized', 'delayed')).toBe(
        0,
      );
    } finally {
      await closeAll([queue]);
    }
  });

  it('removeOrphanedJobs deletes rows with null state for this queue', async () => {
    const queue = newQueue('orphan-null-state', schema);
    try {
      await queue.waitUntilReady();
      const qid = await (
        queue as unknown as { queueId: Promise<number> }
      ).queueId;

      await testPool.query(
        `insert into "${schema}".emq_jobs (queue_id, job_id, name, data, opts, state)
         values ($1, $2, 'x', '{}'::jsonb, '{}'::jsonb, null)
         on conflict (queue_id, job_id) do nothing`,
        [qid, 'injected-orphan'],
      );

      const removed = await queue.removeOrphanedJobs(100, 0);
      expect(removed).toBeGreaterThanOrEqual(1);
      expect(await queue.getJob('injected-orphan')).toBeUndefined();

      const {
        rows: [{ n }],
      } = await testPool.query<{ n: string }>(
        `select count(*)::text as n from "${schema}".emq_jobs
         where queue_id = $1 and job_id = $2`,
        [qid, 'injected-orphan'],
      );
      expect(Number.parseInt(n, 10)).toBe(0);
    } finally {
      await closeAll([queue]);
    }
  });

  it('removeOrphanedJobs respects limit across batches', async () => {
    const queue = newQueue('orphan-limit', schema);
    try {
      await queue.waitUntilReady();
      const qid = await (
        queue as unknown as { queueId: Promise<number> }
      ).queueId;

      for (let i = 0; i < 5; i++) {
        await testPool.query(
          `insert into "${schema}".emq_jobs (queue_id, job_id, name, data, opts, state)
           values ($1, $2, 'x', '{}'::jsonb, '{}'::jsonb, null)`,
          [qid, `orph-${i}`],
        );
      }

      const removed = await queue.removeOrphanedJobs(2, 3);
      expect(removed).toBe(3);

      const {
        rows: [{ n }],
      } = await testPool.query<{ n: string }>(
        `select count(*)::text as n from "${schema}".emq_jobs
         where queue_id = $1 and state is null`,
        [qid],
      );
      expect(Number.parseInt(n, 10)).toBe(2);
    } finally {
      await closeAll([queue]);
    }
  });
});
