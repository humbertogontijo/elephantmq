import type { BulkJobOptions } from '../../interfaces';
import type { JobsOptions } from '../../types';

/**
 * Enriches per-job opts with distributed trace metadata when addBulk runs
 * under an active trace context.
 */
export function mergeBulkJobTelemetry(
  jobOpts: BulkJobOptions | undefined,
  srcPropagationMetadata: string | undefined,
): JobsOptions['telemetry'] | undefined {
  if (!srcPropagationMetadata) {
    return jobOpts?.telemetry;
  }

  let telemetry = jobOpts?.telemetry;
  const omitContext = jobOpts?.telemetry?.omitContext;
  const propagated = !omitContext ? srcPropagationMetadata : undefined;
  const telemetryMetadata = jobOpts?.telemetry?.metadata ?? propagated;

  if (telemetryMetadata || omitContext) {
    telemetry = {
      metadata: telemetryMetadata,
      omitContext,
    };
  }

  return telemetry;
}
