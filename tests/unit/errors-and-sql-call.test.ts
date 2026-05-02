import { describe, expect, it, vi } from 'vitest';
import {
  DelayedError,
  DELAYED_ERROR,
  RateLimitError,
  RATE_LIMIT_ERROR,
  WaitingError,
  WAITING_ERROR,
  WaitingChildrenError,
  WAITING_CHILDREN_ERROR,
  UnrecoverableError,
} from '../../src/classes/errors';
import { ErrorCode } from '../../src/enums';
import { ScriptsBase } from '../../src/classes/scripts/base';
import {
  emqQualifiedFn,
  queryOne,
} from '../../src/classes/sql-call';

describe('BullMQ-style control flow errors', () => {
  it('DelayedError uses default and custom message', () => {
    const d = new DelayedError();
    expect(d).toBeInstanceOf(Error);
    expect(d.message).toBe(DELAYED_ERROR);
    const d2 = new DelayedError('x');
    expect(d2.message).toBe('x');
  });

  it('RateLimitError uses default and custom message', () => {
    const e = new RateLimitError();
    expect(e.message).toBe(RATE_LIMIT_ERROR);
    expect(new RateLimitError('cap').message).toBe('cap');
  });

  it('WaitingError uses default and custom message', () => {
    const e = new WaitingError();
    expect(e.message).toBe(WAITING_ERROR);
    expect(new WaitingError('w').message).toBe('w');
  });

  it('WaitingChildrenError uses default and custom message', () => {
    const e = new WaitingChildrenError();
    expect(e.message).toBe(WAITING_CHILDREN_ERROR);
    expect(new WaitingChildrenError('c').message).toBe('c');
  });
});

describe('ScriptsBase.finishedErrors', () => {
  class T extends ScriptsBase {
    expose(p: Parameters<ScriptsBase['finishedErrors']>[0]) {
      return this.finishedErrors(p);
    }
  }

  const t = new T({ schema: 'public' } as never);

  const cases: Array<{ code: number; expectMsg: (string | RegExp)[]; kind?: 'unrecoverable' }> =
    [
      {
        code: ErrorCode.JobNotExist,
        expectMsg: [/Missing key for job j1/, /cmd/],
      },
      {
        code: ErrorCode.JobLockNotExist,
        expectMsg: [/Missing lock for job j1/, /cmd/],
      },
      {
        code: ErrorCode.JobNotInState,
        expectMsg: [/Job j1 is not in the active state/, /cmd/],
      },
      {
        code: ErrorCode.JobPendingChildren,
        expectMsg: [/Job j1 has pending dependencies/, /cmd/],
      },
      {
        code: ErrorCode.ParentJobNotExist,
        expectMsg: [/Missing key for parent job p1/, /cmd/],
      },
      {
        code: ErrorCode.JobLockMismatch,
        expectMsg: [/Lock mismatch for job j1/, /active/, /cmd/],
      },
      {
        code: ErrorCode.ParentJobCannotBeReplaced,
        expectMsg: [/The parent job p1 cannot be replaced/, /cmd/],
      },
      {
        code: ErrorCode.JobBelongsToJobScheduler,
        expectMsg: [
          /Job j1 belongs to a job scheduler and cannot be removed directly/,
          /cmd/,
        ],
      },
      {
        code: ErrorCode.JobHasFailedChildren,
        expectMsg: [/Cannot complete job j1/, /failed child/, /cmd/],
        kind: 'unrecoverable',
      },
      {
        code: ErrorCode.SchedulerJobIdCollision,
        expectMsg: [/job ID already exists/, /cmd/],
      },
      {
        code: ErrorCode.SchedulerJobSlotsBusy,
        expectMsg: [/time slots already have jobs/, /cmd/],
      },
    ];

  it.each(cases)('maps error code $code', ({ code, expectMsg, kind }) => {
    const err = t.expose({
      code,
      jobId: 'j1',
      parentKey: 'p1',
      command: 'cmd',
      state: 'active',
    });
    expect((err as Error & { code?: number }).code).toBe(code);
    for (const m of expectMsg) {
      expect(err.message).toMatch(m);
    }
    if (kind === 'unrecoverable') {
      expect(err).toBeInstanceOf(UnrecoverableError);
    }
  });

  it('maps unknown codes to a generic error', () => {
    const err = t.expose({
      code: -999,
      jobId: 'j1',
      command: 'cmd',
    });
    expect((err as Error & { code?: number }).code).toBe(-999);
    expect(err.message).toMatch(/Unknown code -999/);
  });
});

describe('sql-call helpers', () => {
  it('emqQualifiedFn quotes schema and appends function name', () => {
    expect(emqQualifiedFn('public', 'emq_foo_v1')).toBe('"public".emq_foo_v1');
  });

  it('queryOne returns first row or undefined', async () => {
    const client = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [{ a: 1 }] })
        .mockResolvedValueOnce({ rows: [] }),
    };
    const r1 = await queryOne(client as never, 'select 1', []);
    expect(r1).toEqual({ a: 1 });
    const r2 = await queryOne(client as never, 'select 1', []);
    expect(r2).toBeUndefined();
    expect(client.query).toHaveBeenCalledTimes(2);
  });
});
