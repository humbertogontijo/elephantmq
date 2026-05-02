import { describe, expect, it } from 'vitest';
import { mergeBulkJobTelemetry } from '../../src/classes/queue/queue-bulk-telemetry';

describe('mergeBulkJobTelemetry', () => {
  it('returns job opts telemetry when no propagation metadata', () => {
    expect(
      mergeBulkJobTelemetry({ telemetry: { metadata: 'm' } }, undefined),
    ).toEqual({ metadata: 'm' });
    expect(mergeBulkJobTelemetry(undefined, undefined)).toBeUndefined();
  });

  it('merges propagated metadata when omitContext is false', () => {
    expect(
      mergeBulkJobTelemetry(
        { telemetry: { metadata: 'job-meta' } },
        'trace-ctx',
      ),
    ).toEqual({ metadata: 'job-meta', omitContext: undefined });

    expect(
      mergeBulkJobTelemetry(
        {},
        'trace-ctx',
      ),
    ).toEqual({ metadata: 'trace-ctx', omitContext: undefined });
  });

  it('clears propagated metadata when omitContext is true', () => {
    expect(
      mergeBulkJobTelemetry(
        { telemetry: { omitContext: true } },
        'trace-ctx',
      ),
    ).toEqual({ metadata: undefined, omitContext: true });
  });

  it('keeps explicit metadata with omitContext', () => {
    expect(
      mergeBulkJobTelemetry(
        { telemetry: { metadata: 'x', omitContext: true } },
        'trace-ctx',
      ),
    ).toEqual({ metadata: 'x', omitContext: true });
  });

  it('uses propagation metadata when job opts have no telemetry', () => {
    expect(mergeBulkJobTelemetry(undefined, 'trace')).toEqual({
      metadata: 'trace',
      omitContext: undefined,
    });
  });
});
