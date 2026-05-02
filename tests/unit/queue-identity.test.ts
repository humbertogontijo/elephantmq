import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import {
  escapeSchema,
  resolveSchema,
  ensureQueueRow,
} from '../../src/classes/queue-identity';

describe('queue-identity', () => {
  describe('escapeSchema', () => {
    it('quotes identifiers and escapes embedded quotes', () => {
      expect(escapeSchema('public')).toBe('"public"');
      expect(escapeSchema('a"b')).toBe('"a""b"');
    });
  });

  describe('resolveSchema', () => {
    it('prefers opts.schema', () => {
      expect(
        resolveSchema({
          schema: 'one',
          connection: { schema: 'two' } as never,
        }),
      ).toBe('one');
    });

    it('reads schema from connection when opts.schema is missing', () => {
      expect(
        resolveSchema({
          connection: { schema: 'app' } as never,
        }),
      ).toBe('app');
    });

    it('ignores connection schema when not a string', () => {
      expect(
        resolveSchema({
          connection: { schema: 123 } as never,
        }),
      ).toBe('public');
    });

    it('defaults to public', () => {
      expect(resolveSchema({})).toBe('public');
      expect(resolveSchema({ connection: undefined })).toBe('public');
      expect(resolveSchema({ connection: null as never })).toBe('public');
      expect(resolveSchema({ connection: { foo: 1 } as never })).toBe(
        'public',
      );
    });
  });

  describe('ensureQueueRow', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });
    afterEach(() => {
      vi.useRealTimers();
    });

    it('returns id on first successful insert', async () => {
      const client = {
        query: vi.fn().mockResolvedValue({ rows: [{ id: '42' }] }),
      };
      const id = await ensureQueueRow(client as never, 'public', 'emq', 'q1');
      expect(id).toBe(42);
      expect(client.query).toHaveBeenCalledTimes(1);
    });

    it('retries on serialization conflict 40P01 then succeeds', async () => {
      const err = { code: '40P01' };
      const client = {
        query: vi
          .fn()
          .mockRejectedValueOnce(err)
          .mockResolvedValueOnce({ rows: [{ id: '3' }] }),
      };
      const p = ensureQueueRow(client as never, 's', 'p', 'n');
      await vi.runAllTimersAsync();
      expect(await p).toBe(3);
      expect(client.query).toHaveBeenCalledTimes(2);
    });

    it('retries on rollback 40001 then succeeds', async () => {
      const client = {
        query: vi
          .fn()
          .mockRejectedValueOnce({ code: '40001' })
          .mockResolvedValueOnce({ rows: [{ id: '1' }] }),
      };
      const p = ensureQueueRow(client as never, 's', 'p', 'n');
      await vi.runAllTimersAsync();
      expect(await p).toBe(1);
    });

    it('rethrows non-retryable errors immediately', async () => {
      const boom = { code: '23505' };
      const client = {
        query: vi.fn().mockRejectedValueOnce(boom),
      };
      await expect(
        ensureQueueRow(client as never, 's', 'p', 'n'),
      ).rejects.toBe(boom);
      expect(client.query).toHaveBeenCalledTimes(1);
    });

    it('throws last error after exhausting retries', async () => {
      const err = { code: '40P01' };
      const client = {
        query: vi.fn().mockRejectedValue(err),
      };
      const p = ensureQueueRow(client as never, 's', 'p', 'n');
      const assertion = expect(p).rejects.toBe(err);
      await vi.runAllTimersAsync();
      await assertion;
      expect(client.query).toHaveBeenCalledTimes(5);
    });
  });
});
