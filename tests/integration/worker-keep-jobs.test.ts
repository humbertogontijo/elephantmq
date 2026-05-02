import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('removeOnComplete / removeOnFail', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('removeOnComplete:true deletes the job row when it completes', async () => {
    const queue = newQueue('koc-true', schema);
    const worker = newWorker('koc-true', async () => 'ok', schema);
    try {
      const job = await queue.add('a', {}, { removeOnComplete: true });
      await waitUntil(
        async () => (await queue.getJob(job.id!)) === undefined,
        5_000,
      );
      expect(await queue.getCompletedCount()).toBe(0);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('removeOnComplete:N keeps at most N completed jobs', async () => {
    const queue = newQueue('koc-n', schema);
    const worker = newWorker('koc-n', async () => 'ok', schema, {
      concurrency: 1,
    });
    try {
      for (let i = 0; i < 5; i++) {
        await queue.add('a', { i }, { removeOnComplete: { count: 2 } });
      }
      await waitUntil(async () => {
        const completed = await queue.getCompletedCount();
        return completed === 2;
      }, 10_000);
      expect(await queue.getCompletedCount()).toBe(2);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('removeOnFail:true deletes the job row when it exhausts attempts', async () => {
    const queue = newQueue('kof-true', schema);
    const worker = newWorker(
      'kof-true',
      async () => {
        throw new Error('nope');
      },
      schema,
    );
    try {
      const job = await queue.add(
        'a',
        {},
        { attempts: 1, removeOnFail: true },
      );
      await waitUntil(
        async () => (await queue.getJob(job.id!)) === undefined,
        5_000,
      );
      expect(await queue.getFailedCount()).toBe(0);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('removeOnFail:N keeps at most N failed jobs', async () => {
    const queue = newQueue('kof-n', schema);
    const worker = newWorker(
      'kof-n',
      async () => {
        throw new Error('nope');
      },
      schema,
      { concurrency: 1 },
    );
    try {
      for (let i = 0; i < 4; i++) {
        await queue.add(
          'a',
          { i },
          { attempts: 1, removeOnFail: { count: 2 } },
        );
      }
      await waitUntil(async () => {
        const failed = await queue.getFailedCount();
        return failed === 2;
      }, 10_000);
      expect(await queue.getFailedCount()).toBe(2);
    } finally {
      await closeAll([worker, queue]);
    }
  });
});
