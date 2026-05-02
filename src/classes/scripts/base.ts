import type { ScriptQueueContext } from '../../interfaces';
import { ErrorCode } from '../../enums';
import { version as packageVersion } from '../../version';
import { UnrecoverableError } from '../errors';
import { escapeSchema } from '../sql-call';

/**
 * Shared per-queue context and helpers for the {@link Scripts} family.
 *
 * Concrete behaviour lives in domain-focused subclasses (`AddJobsScripts`,
 * `LifecycleScripts`, `GettersScripts`, `SchedulerScripts`,
 * `MaintenanceScripts`) which extend this base in a single linear chain.
 * Callers only ever import the final {@link Scripts} facade; the chain
 * exists to keep each file under a few hundred lines and grouped by intent.
 */
export class ScriptsBase {
  protected version = packageVersion;

  constructor(protected queue: ScriptQueueContext) {}

  /** Numeric primary key of the queue row in `emq_queues`. */
  protected async qid(): Promise<number> {
    return this.queue.queueId;
  }

  /** Quoted schema identifier used to qualify every SQL function call. */
  protected S(): string {
    return escapeSchema(this.queue.schema);
  }

  /**
   * Translate the negative error codes returned by `emq_*` SQL functions into
   * a typed JS error. Adds `code` to the resulting object so callers can
   * branch on the original error code without parsing strings.
   */
  finishedErrors({
    code,
    jobId,
    parentKey,
    command,
    state,
  }: {
    code: number;
    jobId?: string;
    parentKey?: string;
    command: string;
    state?: string;
  }): Error {
    let error: Error;
    switch (code) {
      case ErrorCode.JobNotExist:
        error = new Error(`Missing key for job ${jobId}. ${command}`);
        break;
      case ErrorCode.JobLockNotExist:
        error = new Error(`Missing lock for job ${jobId}. ${command}`);
        break;
      case ErrorCode.JobNotInState:
        error = new Error(
          `Job ${jobId} is not in the ${state} state. ${command}`,
        );
        break;
      case ErrorCode.JobPendingChildren:
        error = new Error(`Job ${jobId} has pending dependencies. ${command}`);
        break;
      case ErrorCode.ParentJobNotExist:
        error = new Error(
          `Missing key for parent job ${parentKey}. ${command}`,
        );
        break;
      case ErrorCode.JobLockMismatch:
        error = new Error(
          `Lock mismatch for job ${jobId}. Cmd ${command} from ${state}`,
        );
        break;
      case ErrorCode.ParentJobCannotBeReplaced:
        error = new Error(
          `The parent job ${parentKey} cannot be replaced. ${command}`,
        );
        break;
      case ErrorCode.JobBelongsToJobScheduler:
        error = new Error(
          `Job ${jobId} belongs to a job scheduler and cannot be removed directly. ${command}`,
        );
        break;
      case ErrorCode.JobHasFailedChildren:
        error = new UnrecoverableError(
          `Cannot complete job ${jobId} because it has at least one failed child. ${command}`,
        );
        break;
      case ErrorCode.SchedulerJobIdCollision:
        error = new Error(
          `Cannot create job scheduler iteration - job ID already exists. ${command}`,
        );
        break;
      case ErrorCode.SchedulerJobSlotsBusy:
        error = new Error(
          `Cannot create job scheduler iteration - current and next time slots already have jobs. ${command}`,
        );
        break;
      default:
        error = new Error(
          `Unknown code ${code} error for ${jobId}. ${command}`,
        );
    }

    (error as Error & { code?: number }).code = code;
    return error;
  }
}
