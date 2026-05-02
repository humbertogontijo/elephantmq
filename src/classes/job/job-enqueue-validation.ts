import type { JobJson } from '../../interfaces';
import type { JobsOptions } from '../../types';
import { lengthInUtf8Bytes } from '../../utils';
import { PRIORITY_LIMIT } from './job-constants';

export function validateEnqueueJobOptions(
  opts: JobsOptions,
  jobName: string,
  jobData: JobJson,
  parentKey?: string,
): void {
  const exclusiveOptions: (keyof JobsOptions)[] = [
    'removeDependencyOnFailure',
    'failParentOnFailure',
    'continueParentOnFailure',
    'ignoreDependencyOnFailure',
  ];

  const exceedLimit =
    opts.sizeLimit &&
    lengthInUtf8Bytes(jobData.data) > opts.sizeLimit;

  if (exceedLimit) {
    throw new Error(
      `The size of job ${jobName} exceeds the limit ${opts.sizeLimit} bytes`,
    );
  }

  if (opts.delay && opts.repeat && !opts.repeat?.count) {
    throw new Error(`Delay and repeat options cannot be used together`);
  }

  const enabledExclusiveOptions = exclusiveOptions.filter(opt => opts[opt]);

  if (enabledExclusiveOptions.length > 1) {
    const optionsList = enabledExclusiveOptions.join(', ');
    throw new Error(
      `The following options cannot be used together: ${optionsList}`,
    );
  }

  if (opts?.jobId) {
    if (`${parseInt(opts.jobId, 10)}` === opts?.jobId) {
      throw new Error('Custom Id cannot be integers');
    }

    if (
      opts?.jobId.includes(':') &&
      opts?.jobId?.split(':').length !== 3
    ) {
      throw new Error('Custom Id cannot contain :');
    }
  }

  if (opts.priority) {
    if (Math.trunc(opts.priority) !== opts.priority) {
      throw new Error(`Priority should not be float`);
    }

    if (opts.priority > PRIORITY_LIMIT) {
      throw new Error(`Priority should be between 0 and ${PRIORITY_LIMIT}`);
    }
  }

  if (opts.deduplication) {
    if (!opts.deduplication?.id) {
      throw new Error('Deduplication id must be provided');
    }

    if (parentKey) {
      throw new Error(
        'Deduplication and parent options cannot be used together',
      );
    }
  }

  if (
    typeof opts.backoff === 'object' &&
    typeof opts.backoff.jitter === 'number'
  ) {
    if (opts.backoff.jitter < 0 || opts.backoff.jitter > 1) {
      throw new Error(`Jitter should be between 0 and 1`);
    }
  }
}
