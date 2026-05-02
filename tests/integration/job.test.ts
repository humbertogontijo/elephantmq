import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Job methods', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  describe('updateData', () => {
    it('persists new data and is observable through getJob', async () => {
      const queue = newQueue('j-update', schema);
      try {
        const job = await queue.add('a', { v: 1 });
        await job.updateData({ v: 2 });
        const refetched = await queue.getJob(job.id!);
        expect(refetched?.data).toEqual({ v: 2 });
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('updateProgress', () => {
    it('persists numeric progress and emits a queue event', async () => {
      const queue = newQueue('j-progress', schema);
      const seen: number[] = [];
      const handler = (_job: unknown, p: unknown) => {
        if (typeof p === 'number') {
          seen.push(p);
        }
      };
      queue.on('progress' as never, handler as never);
      try {
        const job = await queue.add('a', {});
        await job.updateProgress(25);
        await job.updateProgress(75);
        const refetched = await queue.getJob(job.id!);
        expect(refetched?.progress).toBe(75);
        expect(seen).toEqual([25, 75]);
      } finally {
        queue.off('progress' as never, handler as never);
        await closeAll([queue]);
      }
    });

    it('persists object progress', async () => {
      const queue = newQueue('j-progress-obj', schema);
      try {
        const job = await queue.add('a', {});
        await job.updateProgress({ done: 3, total: 10 });
        const refetched = await queue.getJob(job.id!);
        expect(refetched?.progress).toEqual({ done: 3, total: 10 });
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('logs', () => {
    it('log() appends rows; clearLogs() clears them', async () => {
      const queue = newQueue('j-logs', schema);
      try {
        const job = await queue.add('a', {});
        await job.log('first');
        await job.log('second');
        const before = await queue.getJobLogs(job.id!);
        expect(before.count).toBe(2);
        expect(before.logs).toEqual(['first', 'second']);
        await job.clearLogs();
        const after = await queue.getJobLogs(job.id!);
        expect(after.count).toBe(0);
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('state predicates', () => {
    it('isCompleted / isFailed / isDelayed / isWaiting', async () => {
      const queue = newQueue('j-states', schema);
      try {
        const completed = await queue.add('c', {});
        const failed = await queue.add('f', {});
        const delayed = await queue.add('d', {}, { delay: 60_000 });

        let runs = 0;
        const worker = newWorker(
          'j-states',
          async () => {
            runs++;
            if (runs === 2) {
              throw new Error('expected');
            }
          },
          schema,
          { concurrency: 1 },
        );
        try {
          await waitUntil(
            async () =>
              (await completed.getState()) === 'completed' &&
              (await failed.getState()) === 'failed',
            10_000,
          );
        } finally {
          await closeAll([worker]);
        }

        // Add the waiting job AFTER the worker has been closed so it cannot
        // be claimed during the assertion.
        const waiting = await queue.add('w', {});

        expect(await completed.isCompleted()).toBe(true);
        expect(await failed.isFailed()).toBe(true);
        expect(await delayed.isDelayed()).toBe(true);
        expect(await waiting.isWaiting()).toBe(true);
      } finally {
        await closeAll([queue]);
      }
    });
  });

  describe('remove', () => {
    it('Job.remove deletes a waiting job', async () => {
      const queue = newQueue('j-remove', schema);
      try {
        const job = await queue.add('a', {});
        await job.remove();
        expect(await queue.getJob(job.id!)).toBeUndefined();
      } finally {
        await closeAll([queue]);
      }
    });
  });
});
