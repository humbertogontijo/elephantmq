import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { FlowProducer } from '../../src';
import {
  closeAll,
  installTestSchema,
  pgConnectionOpts,
  testPool,
  uninstallTestSchema,
} from '../test_context';
import { delay, newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Queue.obliterate', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('removes the queue row and all its jobs', async () => {
    const queue = newQueue('oblit', schema);
    try {
      await queue.add('a', {});
      await queue.add('b', {});
      let counts = await queue.getJobCounts('wait');
      expect(counts.wait).toBe(2);

      await queue.obliterate();

      const { rows } = await testPool.query(
        `select count(*)::int as n from "${schema}".emq_queues where name = $1`,
        ['oblit'],
      );
      expect(rows[0].n).toBe(0);
    } finally {
      await closeAll([queue]);
    }
  });

  it('throws when jobs are still active unless force is true', async () => {
    const queue = newQueue('oblit-act', schema);
    let resume: (() => void) | undefined;
    const hold = new Promise<void>(r => {
      resume = r;
    });

    const worker = newWorker(
      'oblit-act',
      async () => {
        await hold;
        return 'done';
      },
      schema,
      { concurrency: 1 },
    );

    try {
      await queue.add('slow', {});
      await waitUntil(
        async () => (await queue.getActiveCount()) >= 1,
        8_000,
      );

      await expect(queue.obliterate()).rejects.toThrow(/active jobs/i);

      resume!();
      await waitUntil(
        async () => (await queue.getActiveCount()) === 0,
        8_000,
      );

      await queue.obliterate();
      const { rows } = await testPool.query(
        `select count(*)::int as n from "${schema}".emq_queues where name = $1`,
        ['oblit-act'],
      );
      expect(rows[0].n).toBe(0);
    } finally {
      resume?.();
      await closeAll([worker, queue]);
    }
  });

  it('force: true obliterates while a job is still active', async () => {
    const queue = newQueue('oblit-f', schema);
    const worker = newWorker(
      'oblit-f',
      async () => {
        await delay(30_000);
        return 'nope';
      },
      schema,
      { concurrency: 1 },
    );

    try {
      await queue.add('long', {});
      await waitUntil(
        async () => (await queue.getActiveCount()) >= 1,
        8_000,
      );

      await queue.obliterate({ force: true });

      const { rows } = await testPool.query(
        `select count(*)::int as n from "${schema}".emq_queues where name = $1`,
        ['oblit-f'],
      );
      expect(rows[0].n).toBe(0);
    } finally {
      await worker.close(true);
      await closeAll([queue]);
    }
  });

  it('obliterating the child queue after children finish still lets the parent complete', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const parentQ = newQueue('obf-p', schema);
    const childQ = newQueue('obf-c', schema);

    const childWorker = newWorker('obf-c', async () => 'c', schema);
    const parentWorker = newWorker('obf-p', async () => 'p', schema);

    try {
      const tree = await flow.add({
        name: 'par',
        queueName: 'obf-p',
        data: {},
        children: [
          { name: 'c1', queueName: 'obf-c', data: {} },
          { name: 'c2', queueName: 'obf-c', data: {} },
        ],
      });
      await waitUntil(async () => (await childQ.getCompletedCount()) >= 2, 12_000);
      await waitUntil(async () => (await parentQ.getWaitingCount()) >= 1, 12_000);

      await childWorker.close(true);
      await childQ.obliterate({ force: true });

      const tail = await childQ.getJobCounts(
        'wait',
        'active',
        'completed',
        'failed',
        'delayed',
      );
      expect(tail.wait).toBe(0);
      expect(tail.active).toBe(0);
      expect(tail.completed).toBe(0);

      const parentId = tree.job.id!;

      await waitUntil(async () => {
        const { rows } = await testPool.query<{ s: string | null }>(
          `select j.state::text as s
             from "${schema}".emq_jobs j
             join "${schema}".emq_queues q on q.id = j.queue_id
            where q.name = 'obf-p' and j.job_id = $1`,
          [parentId],
        );
        return rows[0]?.s === 'completed';
      }, 12_000);
    } finally {
      await closeAll([parentWorker, flow, parentQ, childQ]);
    }
  });
});
