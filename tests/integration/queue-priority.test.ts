import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('priority and FIFO', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('lower priority value is claimed first', async () => {
    const queue = newQueue('p-order', schema);
    const order: string[] = [];
    const worker = newWorker(
      'p-order',
      async job => {
        order.push(job.data.tag as string);
      },
      schema,
      { concurrency: 1 },
    );
    try {
      await queue.add('p', { tag: 'low' }, { priority: 10 });
      await queue.add('p', { tag: 'high' }, { priority: 1 });
      await queue.add('p', { tag: 'mid' }, { priority: 5 });

      await waitUntil(() => order.length === 3, 5000);
      expect(order).toEqual(['high', 'mid', 'low']);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('within the same priority, jobs run in FIFO order', async () => {
    const queue = newQueue('p-fifo', schema);
    const order: number[] = [];
    const worker = newWorker(
      'p-fifo',
      async job => {
        order.push(job.data.i as number);
      },
      schema,
      { concurrency: 1 },
    );
    try {
      for (let i = 0; i < 5; i++) {
        await queue.add('p', { i }, { priority: 5 });
      }
      await waitUntil(() => order.length === 5, 5000);
      expect(order).toEqual([0, 1, 2, 3, 4]);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('the wait list drains before prioritized jobs run', async () => {
    const queue = newQueue('p-wait-first', schema);
    const order: string[] = [];
    const worker = newWorker(
      'p-wait-first',
      async job => {
        order.push(job.data.tag as string);
      },
      schema,
      { concurrency: 1 },
    );
    try {
      await queue.add('w', { tag: 'wait1' });
      await queue.add('w', { tag: 'wait2' });
      await queue.add('p', { tag: 'prio' }, { priority: 1 });

      await waitUntil(() => order.length === 3, 5000);
      // wait list (FIFO) drains first, then prioritized jobs run.
      expect(order).toEqual(['wait1', 'wait2', 'prio']);
    } finally {
      await closeAll([worker, queue]);
    }
  });
});
