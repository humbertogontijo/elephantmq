import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { QueueEvents } from '../../src';
import {
  closeAll,
  installTestSchema,
  pgConnectionOpts,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';

describe('Job.waitUntilFinished', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('resolves with the return value when the job completes', async () => {
    const queue = newQueue('wuf-ok', schema);
    const events = new QueueEvents('wuf-ok', pgConnectionOpts(schema));
    const worker = newWorker('wuf-ok', async () => ({ ok: true }), schema);
    try {
      await events.waitUntilReady();
      const job = await queue.add('x', {});
      const ret = await job.waitUntilFinished(events, 15000);
      expect(ret).toEqual({ ok: true });
    } finally {
      await closeAll([worker, events, queue]);
    }
  });

  it('rejects when the job fails', async () => {
    const queue = newQueue('wuf-fail', schema);
    const events = new QueueEvents('wuf-fail', pgConnectionOpts(schema));
    const worker = newWorker(
      'wuf-fail',
      async () => {
        throw new Error('bad');
      },
      schema,
    );
    try {
      await events.waitUntilReady();
      const job = await queue.add('x', {}, { attempts: 1 });
      await expect(job.waitUntilFinished(events, 15000)).rejects.toThrow(/bad/);
    } finally {
      await closeAll([worker, events, queue]);
    }
  });

  it('rejects on timeout when the job never runs (queue paused)', async () => {
    const queue = newQueue('wuf-timeout', schema);
    const events = new QueueEvents('wuf-timeout', pgConnectionOpts(schema));
    try {
      await events.waitUntilReady();
      await queue.pause();
      const job = await queue.add('x', {});
      await expect(job.waitUntilFinished(events, 400)).rejects.toThrow(
        /timed out/i,
      );
    } finally {
      await closeAll([events, queue]);
    }
  });
});
