import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Worker stalled jobs', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('moves a stalled job back to wait when the worker is force-closed mid-job', async () => {
    const queue = newQueue('stall-1', schema);
    const resumes: (() => void)[] = [];
    const worker1 = newWorker(
      'stall-1',
      async () =>
        new Promise<void>(resolve => {
          resumes.push(resolve);
        }),
      schema,
      {
        concurrency: 1,
        stalledInterval: 300,
        lockDuration: 1500,
        maxStalledCount: 1,
      },
    );

    try {
      await queue.add('long', {});
      await waitUntil(async () => (await queue.getActiveCount()) === 1, 5000);

      await worker1.close(true);

      const worker2 = newWorker('stall-1', async () => 'recovered', schema, {
        stalledInterval: 300,
        lockDuration: 30_000,
      });

      try {
        await waitUntil(
          async () => (await queue.getCompletedCount()) >= 1,
          12_000,
        );
        expect(await queue.getWaitingCount()).toBe(0);
      } finally {
        resumes.forEach(r => r());
        await closeAll([worker2]);
      }
    } finally {
      resumes.forEach(r => r());
      await closeAll([worker1, queue]);
    }
  });

  it('extendLock extends the lease while the job runs (no background renewal)', async () => {
    const queue = newQueue('stall-lock', schema);
    const worker = newWorker(
      'stall-lock',
      async job => {
        const token = job.token!;
        expect(token).toBeTruthy();
        const until = await job.extendLock(token, 15_000);
        expect(typeof until).toBe('number');
        return 'done';
      },
      schema,
      {
        concurrency: 1,
        skipLockRenewal: true,
        lockDuration: 2000,
        stalledInterval: 2000,
      },
    );
    try {
      await queue.add('x', {});
      await waitUntil(async () => (await queue.getCompletedCount()) >= 1, 10_000);
    } finally {
      await closeAll([worker, queue]);
    }
  });
});
