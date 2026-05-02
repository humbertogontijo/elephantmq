import type { EmqConnectionListener } from '../../interfaces';
import type { Job } from '../job';
import type { JobProgress } from '../../types';

export interface WorkerListener<
  DataType = any,
  ResultType = any,
  NameType extends string = string,
> extends EmqConnectionListener {
  /**
   * Listen to 'active' event.
   *
   * This event is triggered when a job enters the 'active' state.
   */
  active: (job: Job<DataType, ResultType, NameType>, prev: string) => void;

  /**
   * Listen to 'closed' event.
   *
   * This event is triggered when the worker is closed.
   */
  closed: () => void;

  /**
   * Listen to 'closing' event.
   *
   * This event is triggered when the worker is closing.
   */
  closing: (msg: string) => void;

  /**
   * Listen to 'completed' event.
   *
   * This event is triggered when a job has successfully completed.
   */
  completed: (
    job: Job<DataType, ResultType, NameType>,
    result: ResultType,
    prev: string,
  ) => void;

  /**
   * Listen to 'drained' event.
   *
   * This event is triggered when the queue has drained the waiting list.
   * Note that there could still be delayed jobs waiting their timers to expire
   * and this event will still be triggered as long as the waiting list has emptied.
   */
  drained: () => void;

  /**
   * Listen to 'error' event.
   *
   * This event is triggered when an error is throw.
   */
  error: (failedReason: Error) => void;

  /**
   * Listen to 'failed' event.
   *
   * This event is triggered when a job has thrown an exception.
   * Note: job parameter could be received as undefined when an stalled job
   * reaches the stalled limit and it is deleted by the removeOnFail option.
   */
  failed: (
    job: Job<DataType, ResultType, NameType> | undefined,
    error: Error,
    prev: string,
  ) => void;

  /**
   * Listen to 'paused' event.
   *
   * This event is triggered when the queue is paused.
   */
  paused: () => void;

  /**
   * Listen to 'progress' event.
   *
   * This event is triggered when a job updates it progress, i.e. the
   * Job##updateProgress() method is called. This is useful to notify
   * progress or any other data from within a processor to the rest of the
   * world.
   */
  progress: (
    job: Job<DataType, ResultType, NameType>,
    progress: JobProgress,
  ) => void;

  /**
   * Listen to 'ready' event.
   *
   * This event is triggered when blockingConnection is ready.
   */
  ready: () => void;

  /**
   * Listen to 'resumed' event.
   *
   * This event is triggered when the queue is resumed.
   */
  resumed: () => void;

  /**
   * Listen to 'stalled' event.
   *
   * This event is triggered when a job has stalled and
   * has been moved back to the wait list.
   */
  stalled: (jobId: string, prev: string) => void;

  /**
   * Listen to 'lockRenewalFailed' event.
   *
   * This event is triggered when lock renewal fails for one or more jobs.
   */
  lockRenewalFailed: (jobIds: string[]) => void;

  /**
   * Listen to 'locksRenewed' event.
   *
   * This event is triggered when locks are successfully renewed.
   */
  locksRenewed: (data: { count: number; jobIds: string[] }) => void;
}
