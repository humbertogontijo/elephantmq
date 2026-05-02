import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import {
  resolveEventsTrimMaxLen,
  trimEventsForQueue,
  schedulePeriodicTrim,
} from '../../src/classes/events-trimmer';

describe('events-trimmer', () => {
  describe('resolveEventsTrimMaxLen', () => {
    it('returns undefined when unset or non-positive', () => {
      expect(resolveEventsTrimMaxLen(undefined)).toBeUndefined();
      expect(resolveEventsTrimMaxLen({})).toBeUndefined();
      expect(
        resolveEventsTrimMaxLen({
          streams: { events: { maxLen: 0 } },
        }),
      ).toBeUndefined();
      expect(
        resolveEventsTrimMaxLen({
          streams: { events: { maxLen: -1 } },
        }),
      ).toBeUndefined();
    });

    it('uses streams.events.maxLen', () => {
      expect(
        resolveEventsTrimMaxLen({
          streams: { events: { maxLen: 100 } },
        }),
      ).toBe(100);
    });

    it('prefers trim.maxLen over maxLen', () => {
      expect(
        resolveEventsTrimMaxLen({
          streams: {
            events: { maxLen: 10, trim: { every: 1, maxLen: 500 } },
          },
        }),
      ).toBe(500);
    });
  });

  describe('trimEventsForQueue', () => {
    it('returns 0 when maxLen <= 0', async () => {
      const client = { query: vi.fn() };
      expect(
        await trimEventsForQueue(client as never, 'public', 1, 0),
      ).toBe(0);
      expect(client.query).not.toHaveBeenCalled();
    });

    it('returns 0 when queue has no events', async () => {
      const client = {
        query: vi
          .fn()
          .mockResolvedValueOnce({ rows: [{ m: null }] })
          .mockResolvedValueOnce({ rowCount: 0 }),
      };
      expect(
        await trimEventsForQueue(client as never, 'public', 7, 50),
      ).toBe(0);
      expect(client.query).toHaveBeenCalledTimes(1);
    });

    it('deletes old rows and returns rowCount', async () => {
      const client = {
        query: vi
          .fn()
          .mockResolvedValueOnce({ rows: [{ m: '1000' }] })
          .mockResolvedValueOnce({ rowCount: 12 }),
      };
      const n = await trimEventsForQueue(client as never, 'public', 3, 100);
      expect(n).toBe(12);
      expect(client.query).toHaveBeenCalledTimes(2);
      const delArgs = client.query.mock.calls[1];
      expect(delArgs[1]).toEqual([3, String(1000 - 100)]);
    });

    it('defaults rowCount to 0 when null', async () => {
      const client = {
        query: vi
          .fn()
          .mockResolvedValueOnce({ rows: [{ m: '10' }] })
          .mockResolvedValueOnce({ rowCount: null as unknown as number }),
      };
      expect(
        await trimEventsForQueue(client as never, 'public', 1, 5),
      ).toBe(0);
    });
  });

  describe('schedulePeriodicTrim', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });
    afterEach(() => {
      vi.useRealTimers();
    });

    it('runs trim on interval and stops', async () => {
      const query = vi.fn().mockResolvedValue({ rows: [{ m: null }] });
      const queue = {
        client: Promise.resolve({ query } as never),
        queueId: Promise.resolve(9),
        schema: 'public',
        opts: {
          streams: { events: { maxLen: 10, trim: { every: 1000 } } },
        },
      };
      const handle = schedulePeriodicTrim(queue, 1000);
      expect(query).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1000);
      expect(query).toHaveBeenCalled();
      const n = query.mock.calls.length;
      handle.stop();
      await vi.advanceTimersByTimeAsync(2000);
      expect(query.mock.calls.length).toBe(n);
    });

    it('clamps interval to at least 1000ms', () => {
      const setIntervalSpy = vi.spyOn(globalThis, 'setInterval');
      const query = vi.fn().mockResolvedValue({ rows: [{ m: null }] });
      const queue = {
        client: Promise.resolve({ query } as never),
        queueId: Promise.resolve(1),
        schema: 'public',
        opts: { streams: { events: { maxLen: 1 } } },
      };
      const handle = schedulePeriodicTrim(queue, 100);
      expect(setIntervalSpy.mock.calls[0][1]).toBe(1000);
      handle.stop();
      setIntervalSpy.mockRestore();
    });

    it('skips trim when maxLen is unset', async () => {
      const query = vi.fn();
      const queue = {
        client: Promise.resolve({ query } as never),
        queueId: Promise.resolve(1),
        schema: 'public',
        opts: {},
      };
      const handle = schedulePeriodicTrim(queue, 1000);
      await vi.advanceTimersByTimeAsync(1000);
      expect(query).not.toHaveBeenCalled();
      handle.stop();
    });
  });
});
