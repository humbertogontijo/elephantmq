import { describe, expect, it } from 'vitest';
import { createScripts } from '../../src/utils/create-scripts';
import type { MinimalQueue } from '../../src/interfaces';

describe('createScripts', () => {
  it('builds Scripts with queue context including postgresVersion getter', async () => {
    const queue = {
      keys: {} as MinimalQueue['keys'],
      client: Promise.resolve({} as never),
      postgresVersion: '16.0',
      toKey: (t: string) => t,
      opts: {} as MinimalQueue['opts'],
      closing: undefined,
      schema: 'public',
      queueId: Promise.resolve(7),
    } as MinimalQueue;

    const scripts = createScripts(queue);
    const base = scripts as unknown as { queue: { postgresVersion: string } };
    expect(base.queue.postgresVersion).toBe('16.0');
    await expect(queue.queueId).resolves.toBe(7);
  });

  it('tags scripts with queue owner reference', () => {
    const queue = {
      keys: {},
      client: Promise.resolve({}),
      postgresVersion: '15',
      toKey: (t: string) => t,
      opts: {},
      closing: undefined,
      schema: 'public',
      queueId: Promise.resolve(1),
    } as unknown as MinimalQueue;

    const scripts = createScripts(queue);
    expect(
      (scripts as unknown as { __queueOwner?: object }).__queueOwner,
    ).toBe(queue as object);
  });
});
