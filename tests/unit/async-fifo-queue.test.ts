import { describe, it, expect } from 'vitest';
import { AsyncFifoQueue } from '../../src/classes/async-fifo-queue';

describe('AsyncFifoQueue', () => {
  it('returns values in resolution order', async () => {
    const q = new AsyncFifoQueue<number>();
    q.add(new Promise(r => setTimeout(() => r(1), 10)));
    q.add(new Promise(r => setTimeout(() => r(2), 30)));
    q.add(new Promise(r => setTimeout(() => r(3), 5)));

    const a = await q.fetch();
    const b = await q.fetch();
    const c = await q.fetch();

    expect([a, b, c]).toEqual([3, 1, 2]);
  });

  it('numTotal/Pending/Queued reflect state', async () => {
    const q = new AsyncFifoQueue<string>();
    expect(q.numTotal()).toBe(0);

    q.add(Promise.resolve('a'));
    q.add(new Promise(r => setTimeout(() => r('b'), 20)));

    await new Promise(r => setTimeout(r, 5));
    expect(q.numQueued() + q.numPending()).toBe(2);

    await q.waitAll();
    expect(q.numPending()).toBe(0);
    expect(q.numQueued()).toBe(2);
  });

  it('fetch returns undefined when nothing pending or queued', async () => {
    const q = new AsyncFifoQueue<number>();
    const result = await q.fetch();
    expect(result).toBeUndefined();
  });

  it('ignoreErrors swallows rejected promises', async () => {
    const q = new AsyncFifoQueue<number>(true);
    q.add(Promise.reject(new Error('boom')));
    q.add(Promise.resolve(42));

    const a = await q.fetch();
    const b = await q.fetch();

    const seen = new Set([a, b]);
    expect(seen.has(undefined)).toBe(true);
    expect(seen.has(42)).toBe(true);
  });
});
