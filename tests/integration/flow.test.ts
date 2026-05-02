import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { FlowProducer } from '../../src';
import {
  closeAll,
  installTestSchema,
  pgConnectionOpts,
  uninstallTestSchema,
} from '../test_context';
import { delay, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('FlowProducer', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('parent runs only after all children complete', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const order: string[] = [];

    const childWorker = newWorker(
      'flow-children',
      async job => {
        await delay(20);
        order.push(`child:${job.name}`);
        return job.name;
      },
      schema,
    );
    const parentWorker = newWorker(
      'flow-parent',
      async () => {
        order.push('parent');
        return 'parent-done';
      },
      schema,
    );

    try {
      await flow.add({
        name: 'parent',
        queueName: 'flow-parent',
        data: {},
        children: [
          { name: 'a', queueName: 'flow-children', data: {} },
          { name: 'b', queueName: 'flow-children', data: {} },
          { name: 'c', queueName: 'flow-children', data: {} },
        ],
      });

      await waitUntil(() => order.includes('parent'), 10_000);
      const childIdx = order
        .map((s, i) => (s.startsWith('child:') ? i : -1))
        .filter(i => i >= 0);
      const parentIdx = order.indexOf('parent');
      expect(childIdx.length).toBe(3);
      for (const i of childIdx) {
        expect(i).toBeLessThan(parentIdx);
      }
    } finally {
      await closeAll([childWorker, parentWorker, flow]);
    }
  });
});
