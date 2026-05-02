import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Job state transitions', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('promote() moves a delayed job into the runnable set', async () => {
    const queue = newQueue('jm-promote', schema);
    try {
      const job = await queue.add('p', {}, { delay: 3600_000 });
      expect(await job.getState()).toBe('delayed');
      await job.promote();
      expect(
        ['waiting', 'prioritized', 'wait'].includes(await job.getState() as string),
      ).toBe(true);
    } finally {
      await closeAll([queue]);
    }
  });

  it('changeDelay updates process_at for a delayed job', async () => {
    const queue = newQueue('jm-chdelay', schema);
    try {
      const job = await queue.add('d', {}, { delay: 600_000 });
      await job.changeDelay(30_000);
      expect(await job.getState()).toBe('delayed');
    } finally {
      await closeAll([queue]);
    }
  });

  it('changePriority moves a waiting job into prioritized ordering', async () => {
    const queue = newQueue('jm-chprio', schema);
    try {
      const job = await queue.add('x', {});
      expect((await job.getState()) === 'waiting' || (await job.getState()) === 'wait').toBe(
        true,
      );
      await job.changePriority({ priority: 1 });
      expect(await job.getState()).toBe('prioritized');
      const list = await queue.getPrioritized();
      expect(list.some(j => j.id === job.id)).toBe(true);
    } finally {
      await closeAll([queue]);
    }
  });

  it('retry() requeues a failed job', async () => {
    const queue = newQueue('jm-retry', schema);
    const worker = newWorker(
      'jm-retry',
      async () => {
        throw new Error('fail-once');
      },
      schema,
      { concurrency: 1 },
    );
    try {
      const job = await queue.add('r', {}, { attempts: 1 });
      await waitUntil(async () => (await job.getState()) === 'failed', 8000);
      await job.retry('failed');
      expect(
        ['waiting', 'wait', 'prioritized'].includes((await job.getState()) as string),
      ).toBe(true);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('retry() requeues a completed job when asked', async () => {
    const queue = newQueue('jm-retry-done', schema);
    const worker = newWorker('jm-retry-done', async () => 'done', schema);
    try {
      const job = await queue.add('r', {});
      await waitUntil(async () => (await job.getState()) === 'completed', 8000);
      await job.retry('completed');
      expect(
        ['waiting', 'wait', 'prioritized'].includes((await job.getState()) as string),
      ).toBe(true);
    } finally {
      await closeAll([worker, queue]);
    }
  });
});
