import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Chaos: multi-worker reliability', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('processes every job exactly once under random worker kills', async () => {
    const queue = newQueue('chaos', schema);
    const jobCount = 30;
    const seen = new Set<number>();

    const makeWorker = () =>
      newWorker(
        'chaos',
        async job => {
          seen.add(Number((job.data as { i: number }).i));
          await new Promise(r => setTimeout(r, 20 + Math.random() * 80));
          return 'ok';
        },
        schema,
        {
          concurrency: 2,
          lockDuration: 8000,
          stalledInterval: 500,
          maxStalledCount: 3,
        },
      );

    const workers = [makeWorker(), makeWorker()];

    try {
      for (let i = 0; i < jobCount; i++) {
        await queue.add(`job-${i}`, { i });
        if (i > 0 && i % 7 === 0) {
          const victim = workers.shift();
          await victim?.close(true);
          workers.push(makeWorker());
        }
      }

      const settled = await waitUntil(
        async () => (await queue.getCompletedCount()) >= jobCount,
        60_000,
      );
      expect(settled).toBeTruthy();
      expect(seen.size).toBe(jobCount);
      expect(await queue.getFailedCount()).toBe(0);
    } finally {
      await closeAll([...workers, queue]);
    }
  }, 90_000);
});
