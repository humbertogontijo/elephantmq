import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { delay, newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Queue global rate limit', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('limits throughput across two workers on the same queue', async () => {
    const queueName = 'qrate-2w';
    const queue = newQueue(queueName, schema);
    await queue.setGlobalRateLimit(2, 500);
    const starts: number[] = [];

    const w1 = newWorker(
      queueName,
      async () => {
        starts.push(Date.now());
      },
      schema,
      { concurrency: 1 },
    );
    const w2 = newWorker(
      queueName,
      async () => {
        starts.push(Date.now());
      },
      schema,
      { concurrency: 1 },
    );

    try {
      await queue.addBulk(
        Array.from({ length: 6 }, (_, i) => ({
          name: 'r',
          data: { i },
        })),
      );
      await waitUntil(() => starts.length === 6, 25_000);
      starts.sort((a, b) => a - b);
      const elapsed = starts[5]! - starts[0]!;
      expect(elapsed).toBeGreaterThanOrEqual(400);
    } finally {
      await queue.removeGlobalRateLimit();
      await closeAll([w1, w2, queue]);
    }
  });

  it('getRateLimitTtl reflects an active window after global limit is hit', async () => {
    const queueName = 'qrate-ttl';
    const queue = newQueue(queueName, schema);
    await queue.setGlobalRateLimit(1, 800);

    const worker = newWorker(
      queueName,
      async () => 'ok',
      schema,
      { concurrency: 1 },
    );

    try {
      await queue.add('one', {});
      await waitUntil(async () => (await queue.getCompletedCount()) >= 1, 10_000);

      const ttl1 = await queue.getRateLimitTtl(1);
      expect(ttl1).toBeGreaterThan(0);

      await delay(ttl1 + 50);
      const ttl2 = await queue.getRateLimitTtl(1);
      expect(ttl2).toBe(0);
    } finally {
      await queue.removeGlobalRateLimit();
      await closeAll([worker, queue]);
    }
  });
});
