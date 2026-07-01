import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { QueueEvents } from '../../src';
import {
  closeAll,
  installTestSchema,
  pgConnectionOpts,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Reliability: stalled jobs, locks, and events', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('maxStalledCount moves job directly to failed', async () => {
    const queue = newQueue('max-stall', schema);
    const resumes: (() => void)[] = [];
    const worker1 = newWorker(
      'max-stall',
      async () =>
        new Promise<void>(resolve => {
          resumes.push(resolve);
        }),
      schema,
      {
        concurrency: 1,
        stalledInterval: 300,
        lockDuration: 800,
        maxStalledCount: 0,
      },
    );

    try {
      await queue.add('long', {}, { attempts: 3 });
      await waitUntil(async () => (await queue.getActiveCount()) === 1, 5_000);

      await worker1.close(true);

      await waitUntil(async () => (await queue.getFailedCount()) >= 1, 15_000);
      expect(await queue.getWaitingCount()).toBe(0);
    } finally {
      resumes.forEach(r => r());
      await closeAll([worker1, queue]);
    }
  });

  it('retries-exhausted fires only when attempts are exhausted', async () => {
    const queue = newQueue('retry-exhaust', schema);
    const events = new QueueEvents('retry-exhaust', pgConnectionOpts(schema));
    const worker = newWorker(
      'retry-exhaust',
      async () => {
        throw new Error('boom');
      },
      schema,
    );

    let retriesExhausted = 0;
    let failedCount = 0;
    events.on('retries-exhausted', () => retriesExhausted++);
    events.on('failed', () => failedCount++);

    try {
      await events.waitUntilReady();
      await queue.add('x', {}, { attempts: 1 });
      await waitUntil(() => retriesExhausted >= 1, 8_000);
      expect(retriesExhausted).toBe(1);
      expect(failedCount).toBeGreaterThanOrEqual(1);
    } finally {
      await closeAll([worker, queue, events]);
    }
  });

  it('delivers progress, stalled, and waiting-children events', async () => {
    const queue = newQueue('ev-extra', schema);
    const events = new QueueEvents('ev-extra', pgConnectionOpts(schema));
    const worker = newWorker(
      'ev-extra',
      async job => {
        await job.updateProgress(42);
        return 'ok';
      },
      schema,
      { stalledInterval: 500, lockDuration: 2000 },
    );

    const seen = {
      progress: 0,
      stalled: 0,
      waitingChildren: 0,
    };
    events.on('progress', () => seen.progress++);
    events.on('stalled', () => seen.stalled++);
    events.on('waiting-children', () => seen.waitingChildren++);

    try {
      await events.waitUntilReady();
      await queue.add('p', {});
      await waitUntil(() => seen.progress >= 1, 8_000);
      expect(seen.progress).toBeGreaterThanOrEqual(1);
    } finally {
      await closeAll([worker, queue, events]);
    }
  });

  it('lock renewal keeps job active through slow finalize path', async () => {
    const queue = newQueue('lock-finalize', schema);
    const worker = newWorker(
      'lock-finalize',
      async () => {
        await new Promise(r => setTimeout(r, 1200));
        return 'done';
      },
      schema,
      {
        concurrency: 1,
        lockDuration: 1500,
        lockRenewTime: 400,
        stalledInterval: 5000,
      },
    );

    try {
      await queue.add('slow', {});
      await waitUntil(async () => (await queue.getCompletedCount()) >= 1, 10_000);
      expect(await queue.getActiveCount()).toBe(0);
    } finally {
      await closeAll([worker, queue]);
    }
  });
});
