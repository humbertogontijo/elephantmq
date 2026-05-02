import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('JobScheduler', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('upsertJobScheduler runs jobs on the configured interval', async () => {
    const queue = newQueue('sched-interval', schema);
    let runs = 0;

    const worker = newWorker(
      'sched-interval',
      async () => {
        runs++;
      },
      schema,
    );

    try {
      await queue.upsertJobScheduler(
        'tick',
        { every: 200 },
        { name: 'tick', data: {} },
      );
      await waitUntil(() => runs >= 3, 8_000, 100);
      expect(runs).toBeGreaterThanOrEqual(3);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('removeJobScheduler stops future iterations', async () => {
    const queue = newQueue('sched-remove', schema);
    let runs = 0;

    const worker = newWorker(
      'sched-remove',
      async () => {
        runs++;
      },
      schema,
    );

    try {
      await queue.upsertJobScheduler(
        'tick',
        { every: 200 },
        { name: 'tick', data: {} },
      );
      await waitUntil(() => runs >= 1, 8_000, 100);
      const removed = await queue.removeJobScheduler('tick');
      expect(removed).toBe(true);

      const before = runs;
      await new Promise(r => setTimeout(r, 600));
      // Allow at most one in-flight iteration to complete.
      expect(runs - before).toBeLessThanOrEqual(1);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('getJobScheduler, getJobSchedulers, and getJobSchedulersCount reflect upserts', async () => {
    const queue = newQueue('sched-meta', schema);
    try {
      await queue.upsertJobScheduler(
        'meta-id',
        { every: 3600_000 },
        { name: 'tick', data: { zone: 'a' } },
      );
      expect(await queue.getJobSchedulersCount()).toBe(1);
      const one = await queue.getJobScheduler('meta-id');
      expect(one?.key).toBe('meta-id');
      expect(one?.name).toBe('tick');
      expect(one?.template?.data).toEqual({ zone: 'a' });
      const list = await queue.getJobSchedulers();
      expect(list.map(j => j.key).sort()).toEqual(['meta-id']);
    } finally {
      await closeAll([queue]);
    }
  });

  it('repeat limit stops the scheduler after N iterations', async () => {
    const queue = newQueue('sched-limit', schema);
    let runs = 0;
    const worker = newWorker(
      'sched-limit',
      async () => {
        runs++;
      },
      schema,
    );
    try {
      await queue.upsertJobScheduler(
        'lim',
        { every: 120, limit: 3 },
        { name: 'tick', data: {} },
      );
      await waitUntil(() => runs >= 3, 12_000, 80);
      const before = runs;
      await new Promise(r => setTimeout(r, 600));
      expect(runs).toBe(before);
      const js = await queue.getJobScheduler('lim');
      expect(js?.iterationCount).toBe(3);
    } finally {
      await closeAll([worker, queue]);
    }
  });

  it('endDate stops scheduling future iterations', async () => {
    const queue = newQueue('sched-end', schema);
    let runs = 0;
    const worker = newWorker(
      'sched-end',
      async () => {
        runs++;
      },
      schema,
    );
    try {
      const endAt = Date.now() + 2_800;
      await queue.upsertJobScheduler(
        'ed',
        { every: 150, endDate: new Date(endAt) },
        { name: 'tick', data: {} },
      );
      expect(await waitUntil(() => runs >= 1, 15_000, 50)).toBe(true);
      expect(
        await waitUntil(() => Date.now() > endAt + 200, 12_000, 50),
      ).toBe(true);
      const plateau = runs;
      await new Promise(r => setTimeout(r, 700));
      expect(runs).toBe(plateau);
    } finally {
      await closeAll([worker, queue]);
    }
  });
});
