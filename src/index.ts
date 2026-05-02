/**
 * elephantmq — PostgreSQL-native job queue.
 *
 * The default entry point exposes the public producer/consumer API:
 *   {@link Queue}, {@link Worker}, {@link Job}, {@link JobScheduler},
 *   {@link FlowProducer}, {@link QueueEvents}, {@link QueueEventsProducer}
 *
 * Schema management lives behind a separate import path
 * (`elephantmq/migrate`) so apps that only enqueue jobs do not pull migration
 * code into their bundle.
 */

export { Queue } from './classes/queue';
export type { ObliterateOpts, QueueListener } from './classes/queue';

export { Worker } from './classes/worker';
export type { WorkerListener } from './classes/worker';

export { Job } from './classes/job';
export { JobScheduler } from './classes/job-scheduler';

export { FlowProducer } from './classes/flow-producer';
export type {
  AddNodeOpts,
  AddChildrenOpts,
  NodeOpts,
  JobNode,
  FlowProducerListener,
} from './classes/flow-producer';

export { QueueEvents } from './classes/queue-events';
export type { QueueEventsListener } from './classes/queue-events';
export { QueueEventsProducer } from './classes/queue-events-producer';

export {
  DelayedError,
  RateLimitError,
  WaitingError,
  WaitingChildrenError,
  UnrecoverableError,
} from './classes/errors';

export type {
  EmqClient,
  EmqConnectionListener,
  PgClient,
  PgQueryable,
  ConnectionOptions,
  QueueOptions,
  QueueBaseOptions,
  QueueEventsOptions,
  WorkerOptions,
  RateLimiterOptions,
  AdvancedOptions,
  BaseJobOptions,
  BulkJobOptions,
  RepeatOptions,
  RepeatableOptions,
  RepeatBaseOptions,
  RetryOptions,
  MetricsOptions,
  Metrics,
  QueueMeta,
  Parent,
  ParentKeys,
  ParentOptions,
  FlowJob,
  FlowOpts,
  FlowQueuesOpts,
  ChildMessage,
  ParentMessage,
  Receiver,
  Tracer,
  ContextManager,
  Span,
  SandboxedJob,
  SandboxedJobProcessor,
  SandboxedOptions,
  Telemetry,
  JobJson,
  JobJsonRaw,
  JobSchedulerJson,
  JobSchedulerTemplateJson,
  LockManagerWorkerContext,
  MinimalJob,
  MinimalQueue,
  PgConnectionOptions,
} from './interfaces';

export type {
  JobsOptions,
  CompressableJobOptions,
  EncodedJobOptions,
  DeduplicationOptions,
  FinishedStatus,
  JobType,
  JobState,
  JobProgress,
  JobSchedulerTemplateOptions,
  KeepJobs,
  RepeatStrategy,
  BackoffStrategy,
} from './types';

export type { Processor } from './types/processor';
