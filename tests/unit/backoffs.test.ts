import { describe, it, expect } from 'vitest';
import { Backoffs } from '../../src/classes/backoffs';

describe('Backoffs', () => {
  describe('normalize', () => {
    it('wraps a numeric delay as a fixed strategy', () => {
      expect(Backoffs.normalize(1000)).toEqual({ type: 'fixed', delay: 1000 });
    });

    it('passes BackoffOptions through unchanged', () => {
      const opts = { type: 'exponential', delay: 500 };
      expect(Backoffs.normalize(opts)).toEqual(opts);
    });

    it('returns undefined for falsy input', () => {
      expect(Backoffs.normalize(0)).toEqual({ type: 'fixed', delay: 0 });
      expect(Backoffs.normalize(undefined as any)).toBeUndefined();
    });
  });

  describe('calculate', () => {
    const fakeJob = { id: '1' } as any;
    const err = new Error('test');

    it('fixed strategy returns the configured delay', async () => {
      const out = await Backoffs.calculate(
        { type: 'fixed', delay: 250 },
        3,
        err,
        fakeJob,
      );
      expect(out).toBe(250);
    });

    it('exponential strategy returns delay * 2^(attempts-1)', async () => {
      const out = await Backoffs.calculate(
        { type: 'exponential', delay: 100 },
        4,
        err,
        fakeJob,
      );
      expect(out).toBe(800);
    });

    it('exponential with jitter stays within bounds', async () => {
      for (let i = 0; i < 10; i++) {
        const out = (await Backoffs.calculate(
          { type: 'exponential', delay: 100, jitter: 0.5 },
          3,
          err,
          fakeJob,
        )) as number;
        expect(out).toBeGreaterThanOrEqual(100);
        expect(out).toBeLessThanOrEqual(400);
      }
    });

    it('routes unknown strategies through customStrategy', async () => {
      const out = await Backoffs.calculate(
        { type: 'custom', delay: 0 },
        2,
        err,
        fakeJob,
        () => 999,
      );
      expect(out).toBe(999);
    });

    it('throws when an unknown strategy has no customStrategy', () => {
      expect(() =>
        Backoffs.calculate({ type: 'nope', delay: 0 }, 1, err, fakeJob),
      ).toThrow(/Unknown backoff strategy/);
    });
  });
});
