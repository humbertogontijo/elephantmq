import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  pgConnectionOpts,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Job scheduler edge cases', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('respects startDate for cron schedulers', async () => {
    const queue = newQueue('sched-start', schema);
    const start = Date.now() + 60_000;
    try {
      await queue.upsertJobScheduler(
        'cron-start',
        { pattern: '*/5 * * * *', startDate: start },
        'tick',
        {},
        {},
      );
      expect(await queue.getDelayedCount()).toBe(1);
    } finally {
      await closeAll([queue]);
    }
  });

  it('immediately schedules first cron iteration when requested', async () => {
    const queue = newQueue('sched-imm', schema);
    const worker = newWorker('sched-imm', async () => 'ok', schema);
    try {
      await queue.upsertJobScheduler(
        'cron-imm',
        { pattern: '0 0 1 1 *', immediately: true },
        'tick',
        {},
        {},
      );
      await waitUntil(async () => (await queue.getCompletedCount()) >= 1, 10_000);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('rejects invalid cron patterns', async () => {
    const queue = newQueue('sched-bad-cron', schema);
    try {
      await expect(
        queue.upsertJobScheduler(
          'bad',
          { pattern: 'not a cron' },
          'tick',
          {},
          {},
        ),
      ).rejects.toThrow();
    } finally {
      await closeAll([queue]);
    }
  });

  it('upsert over existing scheduler replaces delayed iteration atomically', async () => {
    const queue = newQueue('sched-upsert', schema);
    try {
      await queue.upsertJobScheduler(
        'every-key',
        { every: 60_000 },
        'v1',
        { n: 1 },
        {},
      );
      expect(await queue.getDelayedCount()).toBe(1);

      await queue.upsertJobScheduler(
        'every-key',
        { every: 60_000 },
        'v2',
        { n: 2 },
        {},
      );
      expect(await queue.getDelayedCount()).toBe(1);
      const schedulers = await queue.getJobSchedulers();
      expect(schedulers).toHaveLength(1);
      expect(schedulers[0]?.key).toBe('every-key');
    } finally {
      await closeAll([queue]);
    }
  });
});
