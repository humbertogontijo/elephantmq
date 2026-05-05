import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { delay, newQueue } from '../test_helpers';

describe('Deduplication', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('returns the existing job id when the same dedup id is added twice', async () => {
    const queue = newQueue('dedup', schema);
    try {
      const a = await queue.add(
        'd',
        { v: 1 },
        { deduplication: { id: 'unique-key' } },
      );
      const b = await queue.add(
        'd',
        { v: 2 },
        { deduplication: { id: 'unique-key' } },
      );
      expect(a.id).toBe(b.id);

      const counts = await queue.getJobCounts('wait');
      expect(counts.wait).toBe(1);
    } finally {
      await closeAll([queue]);
    }
  });

  it('getDeduplicationJobId returns the canonical job id for an active dedup key', async () => {
    const queue = newQueue('dedup-lookup', schema);
    try {
      const job = await queue.add(
        'd',
        {},
        { deduplication: { id: 'lookup-me' } },
      );
      const fromTable = await queue.getDeduplicationJobId('lookup-me');
      expect(fromTable).toBe(job.id);
    } finally {
      await closeAll([queue]);
    }
  });

  it('queue.removeDeduplicationKey allows a new job with the same id', async () => {
    const queue = newQueue('dedup-rdk-q', schema);
    try {
      const a = await queue.add(
        'd',
        { n: 1 },
        { deduplication: { id: 'reusable' } },
      );
      const n = await queue.removeDeduplicationKey('reusable');
      expect(n).toBe(1);
      expect(await queue.getDeduplicationJobId('reusable')).toBeNull();
      const b = await queue.add(
        'd',
        { n: 2 },
        { deduplication: { id: 'reusable' } },
      );
      expect(b.id).not.toBe(a.id);
      expect(await queue.getDeduplicationJobId('reusable')).toBe(b.id);
    } finally {
      await closeAll([queue]);
    }
  });

  it('Job.removeDeduplicationKey removes the mapping when this job owns it', async () => {
    const queue = newQueue('dedup-rdk-j', schema);
    try {
      const job = await queue.add(
        'd',
        {},
        { deduplication: { id: 'owned-by-job' } },
      );
      const ok = await job.removeDeduplicationKey();
      expect(ok).toBe(true);
      expect(await queue.getDeduplicationJobId('owned-by-job')).toBeNull();
    } finally {
      await closeAll([queue]);
    }
  });

  it('replace: true on a wait dedup swaps in a new job', async () => {
    const queue = newQueue('dedup-replace-wait', schema);
    try {
      const first = await queue.add(
        'd',
        { v: 1 },
        { deduplication: { id: 'rep-wait-key' } },
      );
      const second = await queue.add(
        'd',
        { v: 2 },
        { deduplication: { id: 'rep-wait-key', replace: true } },
      );
      expect(second.id).not.toBe(first.id);
      const counts = await queue.getJobCounts('wait');
      expect(counts.wait).toBe(1);
      const waiting = await queue.getWaiting(0, 10);
      expect(waiting).toHaveLength(1);
      expect(waiting[0]!.data).toEqual({ v: 2 });
    } finally {
      await closeAll([queue]);
    }
  });

  it('replace: true on a prioritized dedup swaps in a new job', async () => {
    const queue = newQueue('dedup-replace-prio', schema);
    try {
      const first = await queue.add(
        'd',
        { v: 1 },
        { priority: 1, deduplication: { id: 'rep-prio-key' } },
      );
      const second = await queue.add(
        'd',
        { v: 2 },
        {
          priority: 1,
          deduplication: { id: 'rep-prio-key', replace: true },
        },
      );
      expect(second.id).not.toBe(first.id);
      const counts = await queue.getJobCounts('prioritized');
      expect(counts.prioritized).toBe(1);
      const jobs = await queue.getPrioritized();
      expect(jobs).toHaveLength(1);
      expect(jobs[0]!.data).toEqual({ v: 2 });
    } finally {
      await closeAll([queue]);
    }
  });

  it('replace: true on a delayed dedup swaps in a new delayed job', async () => {
    const queue = newQueue('dedup-replace', schema);
    try {
      const first = await queue.add(
        'd',
        { v: 1 },
        { delay: 60_000, deduplication: { id: 'rep-key' } },
      );
      const second = await queue.add(
        'd',
        { v: 2 },
        {
          delay: 60_000,
          deduplication: { id: 'rep-key', replace: true },
        },
      );
      expect(second.id).not.toBe(first.id);
      const counts = await queue.getJobCounts('delayed');
      expect(counts.delayed).toBe(1);
      const cur = await queue.getDelayed();
      expect(cur).toHaveLength(1);
      expect(cur[0]!.data).toEqual({ v: 2 });
    } finally {
      await closeAll([queue]);
    }
  });

  it('deduplication ttl expires so a later add creates a new job', async () => {
    const queue = newQueue('dedup-ttl', schema);
    try {
      const a = await queue.add(
        'd',
        { pass: 1 },
        { deduplication: { id: 'short-ttl', ttl: 80 } },
      );
      await delay(200);
      const b = await queue.add(
        'd',
        { pass: 2 },
        { deduplication: { id: 'short-ttl' } },
      );
      expect(b.id).not.toBe(a.id);
    } finally {
      await closeAll([queue]);
    }
  });
});
