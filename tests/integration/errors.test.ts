import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { UnrecoverableError } from '../../src';
import {
  closeAll,
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Processor errors', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('UnrecoverableError moves the job to failed without further attempts', async () => {
    const queue = newQueue('unrec', schema);
    let n = 0;
    const worker = newWorker(
      'unrec',
      async () => {
        n++;
        throw new UnrecoverableError('no-retry');
      },
      schema,
    );
    try {
      const job = await queue.add('x', {}, { attempts: 5 });
      await waitUntil(async () => (await job.getState()) === 'failed', 8000);
      expect(n).toBe(1);
    } finally {
      await closeAll([worker, queue]);
    }
  });
});
