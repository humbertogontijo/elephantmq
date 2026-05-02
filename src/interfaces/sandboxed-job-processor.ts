import { SandboxedJob } from './sandboxed-job';

/**
 * @see {@link https://docs.bullmq.io/guide/workers/sandboxed-processors}
 */
export type SandboxedJobProcessor<T = unknown, R = unknown> =
  | ((job: SandboxedJob<T, R>) => R | PromiseLike<R>)
  | ((
      job: SandboxedJob<T, R>,
      callback: (error: unknown, result: R) => void,
    ) => void);
