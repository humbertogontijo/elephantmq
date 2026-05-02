import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { FlowProducer } from '../../src';
import {
  closeAll,
  installTestSchema,
  pgConnectionOpts,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Flow dependencies API', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('getChildrenValues returns completed child returnvalues for the parent', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const parentQ = newQueue('fd-parent', schema);

    const childWorker = newWorker(
      'fd-child',
      async job => ({ child: job.name }),
      schema,
    );
    const parentWorker = newWorker(
      'fd-parent',
      async job => {
        const values = await job.getChildrenValues();
        expect(Object.keys(values).length).toBeGreaterThanOrEqual(1);
        return values;
      },
      schema,
    );

    try {
      await flow.add({
        name: 'parent',
        queueName: 'fd-parent',
        data: {},
        children: [
          { name: 'only', queueName: 'fd-child', data: { n: 1 } },
        ],
      });

      await waitUntil(async () => {
        const counts = await parentQ.getJobCounts('completed');
        return counts.completed >= 1;
      }, 12_000);
    } finally {
      await closeAll([childWorker, parentWorker, flow, parentQ]);
    }
  });

  it('getDependenciesCount returns expected buckets for a parent', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const childWorker = newWorker(
      'fdc-child',
      async () => 'ok',
      schema,
    );
    const parentWorker = newWorker(
      'fdc-parent',
      async job => {
        const d = await job.getDependenciesCount();
        expect(d.unprocessed).toBe(0);
        return d;
      },
      schema,
    );

    try {
      const tree = await flow.add({
        name: 'parent',
        queueName: 'fdc-parent',
        data: {},
        children: [
          { name: 'a', queueName: 'fdc-child', data: {} },
          { name: 'b', queueName: 'fdc-child', data: {} },
        ],
      });

      const parent = tree.job;
      await waitUntil(async () => (await parent.getState()) === 'completed', 12_000);
    } finally {
      await closeAll([childWorker, parentWorker, flow]);
    }
  });
});
