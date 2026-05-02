import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { FlowProducer } from '../../src';
import {
  closeAll,
  installTestSchema,
  pgConnectionOpts,
  uninstallTestSchema,
} from '../test_context';
import { newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Flow failure propagation', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('failParentOnFailure: child failure fails the parent', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const childWorker = newWorker(
      'ff-child',
      async () => {
        throw new Error('child boom');
      },
      schema,
    );
    const parentWorker = newWorker(
      'ff-parent',
      async () => 'should-not-run',
      schema,
    );

    try {
      const tree = await flow.add({
        name: 'parent',
        queueName: 'ff-parent',
        data: {},
        children: [
          {
            name: 'c',
            queueName: 'ff-child',
            data: {},
            opts: { failParentOnFailure: true, attempts: 1 },
          },
        ],
      });

      const parentJob = tree.job;

      await waitUntil(
        async () => (await parentJob.getState()) === 'failed',
        12_000,
      );

      expect(await parentJob.getState()).toBe('failed');
    } finally {
      await closeAll([childWorker, parentWorker, flow]);
    }
  });
});
