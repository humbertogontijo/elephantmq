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

describe('Flow failure modes', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('continueParentOnFailure: parent completes when child fails', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const childWorker = newWorker(
      'cpof-child',
      async () => {
        throw new Error('child failed');
      },
      schema,
    );
    const parentWorker = newWorker(
      'cpof-parent',
      async () => 'parent ok',
      schema,
    );

    try {
      const tree = await flow.add({
        name: 'parent',
        queueName: 'cpof-parent',
        data: {},
        children: [
          {
            name: 'c',
            queueName: 'cpof-child',
            data: {},
            opts: { continueParentOnFailure: true, attempts: 1 },
          },
        ],
      });

      await waitUntil(
        async () => (await tree.job.getState()) === 'completed',
        12_000,
      );
      expect(await tree.job.getState()).toBe('completed');
    } finally {
      await closeAll([childWorker, parentWorker, flow]);
    }
  });

  it('ignoreDependencyOnFailure: parent completes when child fails', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const childWorker = newWorker(
      'idof-child',
      async () => {
        throw new Error('ignored');
      },
      schema,
    );
    const parentWorker = newWorker(
      'idof-parent',
      async () => 'parent ok',
      schema,
    );

    try {
      const tree = await flow.add({
        name: 'parent',
        queueName: 'idof-parent',
        data: {},
        children: [
          {
            name: 'c',
            queueName: 'idof-child',
            data: {},
            opts: { ignoreDependencyOnFailure: true, attempts: 1 },
          },
        ],
      });

      await waitUntil(
        async () => (await tree.job.getState()) === 'completed',
        12_000,
      );
      expect(await tree.job.getState()).toBe('completed');
    } finally {
      await closeAll([childWorker, parentWorker, flow]);
    }
  });

  it('removeDependencyOnFailure: parent completes when child fails', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const childWorker = newWorker(
      'rdof-child',
      async () => {
        throw new Error('removed dep');
      },
      schema,
    );
    const parentWorker = newWorker(
      'rdof-parent',
      async () => 'parent ok',
      schema,
    );

    try {
      const tree = await flow.add({
        name: 'parent',
        queueName: 'rdof-parent',
        data: {},
        children: [
          {
            name: 'c',
            queueName: 'rdof-child',
            data: {},
            opts: { removeDependencyOnFailure: true, attempts: 1 },
          },
        ],
      });

      await waitUntil(
        async () => (await tree.job.getState()) === 'completed',
        12_000,
      );
      expect(await tree.job.getState()).toBe('completed');
    } finally {
      await closeAll([childWorker, parentWorker, flow]);
    }
  });

  it('flow parent emits waiting-children on creation', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    const events = new (await import('../../src')).QueueEvents(
      'wc-ev',
      pgConnectionOpts(schema),
    );
    let waitingChildren = 0;
    events.on('waiting-children', () => waitingChildren++);

    try {
      await events.waitUntilReady();
      await flow.add({
        name: 'parent',
        queueName: 'wc-ev',
        data: {},
        children: [{ name: 'c', queueName: 'wc-ev-child', data: {} }],
      });
      await waitUntil(() => waitingChildren >= 1, 5_000);
    } finally {
      await closeAll([events, flow]);
    }
  });

  it('getFlow returns nested job tree', async () => {
    const flow = new FlowProducer(pgConnectionOpts(schema));
    try {
      const tree = await flow.add({
        name: 'root',
        queueName: 'gf-root',
        data: { level: 0 },
        children: [
          {
            name: 'leaf',
            queueName: 'gf-leaf',
            data: { level: 1 },
          },
        ],
      });

      const loaded = await flow.getFlow({
        queueName: 'gf-root',
        id: tree.job.id!,
        depth: 2,
      });

      expect(loaded.job.id).toBe(tree.job.id);
      expect(loaded.children?.length).toBe(1);
      expect(loaded.children?.[0]?.job.name).toBe('leaf');
    } finally {
      await closeAll([flow]);
    }
  });
});
