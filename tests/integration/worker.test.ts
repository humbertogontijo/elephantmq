import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { delay, newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Worker', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('processes a single job', async () => {
    const queue = newQueue('w-basic', schema);
    let received: any = null;
    const worker = newWorker(
      'w-basic',
      async job => {
        received = job.data;
        return 42;
      },
      schema,
    );

    try {
      await queue.add('paint', { color: 'red' });
      await waitUntil(() => received !== null, 5000);
      expect(received).toEqual({ color: 'red' });
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('emits completed event with return value', async () => {
    const queue = newQueue('w-completed', schema);
    const worker = newWorker(
      'w-completed',
      async () => 'done',
      schema,
    );

    try {
      const completed = new Promise<{ id: string; ret: any }>(resolve => {
        worker.on('completed', (job, ret) => resolve({ id: job.id!, ret }));
      });
      const job = await queue.add('a', {});
      const result = await completed;
      expect(result.id).toBe(job.id);
      expect(result.ret).toBe('done');
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('processes jobs concurrently up to the configured limit', async () => {
    const queue = newQueue('w-concurrency', schema);
    let active = 0;
    let peak = 0;
    let processed = 0;

    const worker = newWorker(
      'w-concurrency',
      async () => {
        active++;
        peak = Math.max(peak, active);
        await delay(50);
        active--;
        processed++;
      },
      schema,
      { concurrency: 3 },
    );

    try {
      const jobs = Array.from({ length: 6 }, (_, i) => ({
        name: 'job',
        data: { i },
      }));
      await queue.addBulk(jobs);
      await waitUntil(() => processed === 6, 10_000);
      expect(processed).toBe(6);
      expect(peak).toBe(3);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('retries failed jobs with backoff', async () => {
    const queue = newQueue('w-retry', schema);
    const attemptsByJob = new Map<string, number>();

    const worker = newWorker(
      'w-retry',
      async job => {
        const n = (attemptsByJob.get(job.id!) ?? 0) + 1;
        attemptsByJob.set(job.id!, n);
        if (n < 3) {
          throw new Error(`fail attempt ${n}`);
        }
        return n;
      },
      schema,
    );

    try {
      const job = await queue.add(
        'r',
        {},
        { attempts: 3, backoff: { type: 'fixed', delay: 50 } },
      );
      await waitUntil(
        async () => (await job.getState()) === 'completed',
        10_000,
      );
      expect(attemptsByJob.get(job.id!)).toBe(3);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('respects worker rateLimit (jobs per duration)', async () => {
    const queue = newQueue('w-rate', schema);
    const startTimes: number[] = [];

    const worker = newWorker(
      'w-rate',
      async () => {
        startTimes.push(Date.now());
      },
      schema,
      { limiter: { max: 2, duration: 500 } },
    );

    try {
      await queue.addBulk(
        Array.from({ length: 4 }, (_, i) => ({ name: 'r', data: { i } })),
      );
      await waitUntil(() => startTimes.length === 4, 15_000);
      const elapsed = startTimes[3] - startTimes[0];
      // 4 jobs at 2/500ms => at least 1 full window of throttling.
      expect(elapsed).toBeGreaterThanOrEqual(400);
    } finally {
      await closeAll([worker, queue]);
    }
  });
});
