import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { QueueEvents, QueueEventsProducer } from '../../src';
import {
  closeAll,
  installTestSchema,
  pgConnectionOpts,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('QueueEvents', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('delivers waiting/active/completed events for a processed job', async () => {
    const queue = newQueue('ev-basic', schema);
    const events = new QueueEvents('ev-basic', pgConnectionOpts(schema));
    const worker = newWorker(
      'ev-basic',
      async () => 'ok',
      schema,
    );

    const seen: Record<string, number> = {
      waiting: 0,
      active: 0,
      completed: 0,
    };

    events.on('waiting', () => seen.waiting++);
    events.on('active', () => seen.active++);
    events.on('completed', () => seen.completed++);

    try {
      await events.waitUntilReady();
      await queue.add('e', {});
      await waitUntil(() => seen.completed >= 1, 5_000);
      expect(seen.waiting).toBeGreaterThanOrEqual(1);
      expect(seen.active).toBeGreaterThanOrEqual(1);
      expect(seen.completed).toBeGreaterThanOrEqual(1);
    } finally {
      await closeAll([worker, queue, events]);
    }
  });

  it('delivers failed event with reason', async () => {
    const queue = newQueue('ev-fail', schema);
    const events = new QueueEvents('ev-fail', pgConnectionOpts(schema));
    const worker = newWorker(
      'ev-fail',
      async () => {
        throw new Error('nope');
      },
      schema,
    );

    let failedReason: string | undefined;
    events.on('failed', ({ failedReason: reason }) => {
      failedReason = reason;
    });

    try {
      await events.waitUntilReady();
      await queue.add('e', {});
      await waitUntil(() => failedReason !== undefined, 5_000);
      expect(failedReason).toBe('nope');
    } finally {
      await closeAll([worker, queue, events]);
    }
  });

  it('delays a job emits delayed', async () => {
    const queue = newQueue('ev-delayed', schema);
    const events = new QueueEvents('ev-delayed', pgConnectionOpts(schema));
    let delayMs = 0;
    events.on('delayed', args => {
      delayMs = args.delay;
    });
    try {
      await events.waitUntilReady();
      await queue.add('e', {}, { delay: 5_000 });
      await waitUntil(() => delayMs === 5_000, 3_000);
    } finally {
      await closeAll([queue, events]);
    }
  });

  it('remove emits removed', async () => {
    const queue = newQueue('ev-removed', schema);
    const events = new QueueEvents('ev-removed', pgConnectionOpts(schema));
    let removedId: string | undefined;
    events.on('removed', args => {
      removedId = args.jobId;
    });
    try {
      await events.waitUntilReady();
      const job = await queue.add('e', {});
      await queue.remove(job.id!);
      await waitUntil(() => removedId === job.id, 3_000);
    } finally {
      await closeAll([queue, events]);
    }
  });

  it('pause and resume emit paused / resumed', async () => {
    const queue = newQueue('ev-pause', schema);
    const events = new QueueEvents('ev-pause', pgConnectionOpts(schema));
    const seen: string[] = [];
    events.on('paused', () => seen.push('paused'));
    events.on('resumed', () => seen.push('resumed'));
    try {
      await events.waitUntilReady();
      await queue.pause();
      await queue.resume();
      await waitUntil(() => seen.includes('paused') && seen.includes('resumed'), 5_000);
    } finally {
      await closeAll([queue, events]);
    }
  });

  it('emits drained when the last runnable job finishes', async () => {
    const queue = newQueue('ev-drained', schema);
    const events = new QueueEvents('ev-drained', pgConnectionOpts(schema));
    const worker = newWorker('ev-drained', async () => 'ok', schema);
    let drained = false;
    events.on('drained', () => {
      drained = true;
    });
    try {
      await events.waitUntilReady();
      await queue.add('e', {});
      await waitUntil(() => drained, 8_000);
    } finally {
      await closeAll([worker, queue, events]);
    }
  });

  it('emits retries-exhausted when the job fails for good', async () => {
    const queue = newQueue('ev-rex', schema);
    const events = new QueueEvents('ev-rex', pgConnectionOpts(schema));
    const worker = newWorker(
      'ev-rex',
      async () => {
        throw new Error('final');
      },
      schema,
    );
    let jobId: string | undefined;
    events.on('retries-exhausted', args => {
      jobId = args.jobId;
    });
    try {
      await events.waitUntilReady();
      const job = await queue.add('e', {}, { attempts: 1 });
      await waitUntil(() => jobId === job.id, 8_000);
    } finally {
      await closeAll([worker, queue, events]);
    }
  });

  it('QueueEventsProducer.publishEvent delivers a custom payload', async () => {
    const queue = newQueue('ev-custom', schema);
    const events = new QueueEvents('ev-custom', pgConnectionOpts(schema));
    const producer = new QueueEventsProducer('ev-custom', pgConnectionOpts(schema));
    let payload: { k?: string } | undefined;
    (events as { on: (e: string, fn: (a: { k?: string }) => void) => void }).on(
      'my.custom',
      (args: { k?: string }) => {
        payload = args;
      },
    );
    try {
      await events.waitUntilReady();
      await producer.publishEvent({ eventName: 'my.custom', k: 'v' });
      await waitUntil(() => payload?.k === 'v', 5_000);
    } finally {
      await closeAll([producer, queue, events]);
    }
  });
});
