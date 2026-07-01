import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Queue bulk operations', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('retryJobs moves failed jobs back to wait in batches', async () => {
    const queue = newQueue('bulk-retry', schema);
    const worker = newWorker(
      'bulk-retry',
      async () => {
        throw new Error('fail');
      },
      schema,
      { concurrency: 1 },
    );

    try {
      await queue.addBulk([
        { name: 'a', data: {}, opts: { attempts: 1 } },
        { name: 'b', data: {}, opts: { attempts: 1 } },
      ]);

      await waitUntil(async () => (await queue.getFailedCount()) >= 2, 8_000);
      await worker.pause(true);

      // retryJobs batches until the failed set is empty (BullMQ semantics).
      await queue.retryJobs({ count: 1 });
      expect(await queue.getWaitingCount()).toBe(2);
      expect(await queue.getFailedCount()).toBe(0);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('promoteJobs moves delayed jobs to wait respecting priority', async () => {
    const queue = newQueue('bulk-promote', schema);
    try {
      await queue.add('low', {}, { delay: 60_000, priority: 0 });
      await queue.add('high', {}, { delay: 60_000, priority: 5 });

      expect(await queue.getDelayedCount()).toBe(2);

      await queue.promoteJobs({ count: 10 });

      await waitUntil(async () => (await queue.getPrioritizedCount()) >= 1, 3_000);
      expect(await queue.getWaitingCount()).toBeGreaterThanOrEqual(1);
      expect(await queue.getDelayedCount()).toBe(0);
    } finally {
      await closeAll([queue]);
    }
  });

  it('promoteJobs respects paused queue routing', async () => {
    const queue = newQueue('bulk-promote-paused', schema);
    try {
      await queue.add('d', {}, { delay: 1000 });
      await queue.pause();
      await queue.promoteJobs({ count: 10 });
      const counts = await queue.getJobCounts();
      expect(counts.paused).toBe(1);
      expect(await queue.getDelayedCount()).toBe(0);
    } finally {
      await closeAll([queue]);
    }
  });
});
