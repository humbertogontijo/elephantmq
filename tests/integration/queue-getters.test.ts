import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Queue getters', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('getJob returns undefined for an unknown id', async () => {
    const queue = newQueue('g-getjob-unknown', schema);
    try {
      const job = await queue.getJob('does-not-exist');
      expect(job).toBeUndefined();
    } finally {
      await closeAll([queue]);
    }
  });

  it('getJob returns a job that was just enqueued', async () => {
    const queue = newQueue('g-getjob', schema);
    try {
      const added = await queue.add('n', { v: 1 });
      const fetched = await queue.getJob(added.id!);
      expect(fetched?.id).toBe(added.id);
      expect(fetched?.name).toBe('n');
      expect(fetched?.data).toEqual({ v: 1 });
    } finally {
      await closeAll([queue]);
    }
  });

  it('getJobState returns "waiting"/"delayed"/"unknown"', async () => {
    const queue = newQueue('g-state', schema);
    try {
      const w = await queue.add('w', {});
      const d = await queue.add('d', {}, { delay: 60_000 });
      expect(await queue.getJobState(w.id!)).toBe('waiting');
      expect(await queue.getJobState(d.id!)).toBe('delayed');
      expect(await queue.getJobState('nope')).toBe('unknown');
    } finally {
      await closeAll([queue]);
    }
  });

  it('count() and getJobCounts() reflect waiting jobs', async () => {
    const queue = newQueue('g-counts', schema);
    try {
      await queue.addBulk([
        { name: 'a', data: {} },
        { name: 'a', data: {} },
        { name: 'a', data: {} },
      ]);
      expect(await queue.count()).toBe(3);
      const counts = await queue.getJobCounts('wait', 'completed', 'failed');
      expect(counts.wait).toBe(3);
      expect(counts.completed ?? 0).toBe(0);
      expect(counts.failed ?? 0).toBe(0);
    } finally {
      await closeAll([queue]);
    }
  });

  it('getActive/Completed/Delayed/Prioritized/Waiting return the right rows', async () => {
    const queue = newQueue('g-buckets', schema);
    let resumeJob: (() => void) | undefined;
    const worker = newWorker(
      'g-buckets',
      async () =>
        new Promise<void>(resolve => {
          resumeJob = resolve;
        }),
      schema,
      { concurrency: 1 },
    );
    try {
      const willBeActive = await queue.add('a', { kind: 'active' });
      const delayed = await queue.add(
        'd',
        { kind: 'delayed' },
        { delay: 60_000 },
      );
      const prioritized = await queue.add(
        'p',
        { kind: 'prio' },
        { priority: 1 },
      );

      await waitUntil(async () => (await queue.getActiveCount()) === 1, 5000);

      const activeJobs = await queue.getActive();
      expect(activeJobs.map(j => j.id)).toEqual([willBeActive.id]);

      const delayedJobs = await queue.getDelayed();
      expect(delayedJobs.map(j => j.id)).toEqual([delayed.id]);

      const prioritizedJobs = await queue.getPrioritized();
      expect(prioritizedJobs.map(j => j.id)).toEqual([prioritized.id]);

      expect(await queue.getActiveCount()).toBe(1);
      expect(await queue.getDelayedCount()).toBe(1);
      expect(await queue.getPrioritizedCount()).toBe(1);

      resumeJob?.();
      await waitUntil(async () => (await queue.getActiveCount()) === 0, 5000);
      expect(await queue.getCompletedCount()).toBeGreaterThanOrEqual(1);
    } finally {
      resumeJob?.();
      await closeAll([worker, queue]);
    }
  });

  it('getJobs returns jobs across requested types', async () => {
    const queue = newQueue('g-getjobs', schema);
    try {
      const j1 = await queue.add('a', { i: 1 });
      const j2 = await queue.add('a', { i: 2 });
      const j3 = await queue.add('a', { i: 3 }, { delay: 60_000 });

      const wait = await queue.getJobs(['wait']);
      expect(wait.map(j => j.id).sort()).toEqual([j1.id, j2.id].sort());

      const both = await queue.getJobs(['wait', 'delayed']);
      expect(both.map(j => j.id).sort()).toEqual(
        [j1.id, j2.id, j3.id].sort(),
      );
    } finally {
      await closeAll([queue]);
    }
  });

  it('getJobCountByTypes sums across multiple buckets', async () => {
    const queue = newQueue('g-countby', schema);
    try {
      await queue.add('a', {});
      await queue.add('a', {});
      await queue.add('a', {}, { delay: 60_000 });

      expect(await queue.getJobCountByTypes('wait')).toBe(2);
      expect(await queue.getJobCountByTypes('delayed')).toBe(1);
      expect(await queue.getJobCountByTypes('wait', 'delayed')).toBe(3);
    } finally {
      await closeAll([queue]);
    }
  });
});
