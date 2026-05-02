import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  installTestSchema,
  uninstallTestSchema,
} from '../test_context';
import { newQueue, newWorker } from '../test_helpers';
import { waitUntil } from '../utils/wait-until';

describe('Multiple workers', () => {
  let schema: string;

  beforeAll(async () => {
    schema = await installTestSchema();
  });

  afterAll(async () => {
    await uninstallTestSchema(schema);
  });

  it('two workers on the same queue both process jobs', async () => {
    const queue = newQueue('mw2', schema);
    const seen = new Set<string>();
    const proc = async (job: { id?: string }) => {
      seen.add(job.id!);
    };
    const w1 = newWorker('mw2', proc, schema, { concurrency: 1 });
    const w2 = newWorker('mw2', proc, schema, { concurrency: 1 });
    try {
      await queue.addBulk([
        { name: 'a', data: {} },
        { name: 'a', data: {} },
        { name: 'a', data: {} },
        { name: 'a', data: {} },
      ]);
      await waitUntil(
        async () => (await queue.getCompletedCount()) >= 4,
        12_000,
      );
      expect(seen.size).toBe(4);
    } finally {
      await w1.close(true);
      await w2.close(true);
      await queue.close();
    }
  });
});
