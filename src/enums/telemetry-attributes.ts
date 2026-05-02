/**
 * OpenTelemetry-compatible attribute keys emitted by elephantmq spans and
 * metrics. Renamed from the historical `bullmq.*` prefix used by the BullMQ
 * fork these names originated from; consumers wiring up custom telemetry
 * should look for `elephantmq.*` keys.
 */
export enum TelemetryAttributes {
  QueueName = 'elephantmq.queue.name',
  QueueOperation = 'elephantmq.queue.operation',
  BulkCount = 'elephantmq.job.bulk.count',
  BulkNames = 'elephantmq.job.bulk.names',
  JobName = 'elephantmq.job.name',
  JobId = 'elephantmq.job.id',
  JobKey = 'elephantmq.job.key',
  JobIds = 'elephantmq.job.ids',
  JobAttemptsMade = 'elephantmq.job.attempts.made',
  DeduplicationKey = 'elephantmq.job.deduplication.key',
  JobOptions = 'elephantmq.job.options',
  JobProgress = 'elephantmq.job.progress',
  QueueDrainDelay = 'elephantmq.queue.drain.delay',
  QueueGrace = 'elephantmq.queue.grace',
  QueueCleanLimit = 'elephantmq.queue.clean.limit',
  QueueRateLimit = 'elephantmq.queue.rate.limit',
  JobType = 'elephantmq.job.type',
  QueueOptions = 'elephantmq.queue.options',
  QueueEventMaxLength = 'elephantmq.queue.event.max.length',
  QueueJobsState = 'elephantmq.queue.jobs.state',
  WorkerOptions = 'elephantmq.worker.options',
  WorkerName = 'elephantmq.worker.name',
  WorkerId = 'elephantmq.worker.id',
  WorkerRateLimit = 'elephantmq.worker.rate.limit',
  WorkerDoNotWaitActive = 'elephantmq.worker.do.not.wait.active',
  WorkerForceClose = 'elephantmq.worker.force.close',
  WorkerStalledJobs = 'elephantmq.worker.stalled.jobs',
  WorkerFailedJobs = 'elephantmq.worker.failed.jobs',
  WorkerJobsToExtendLocks = 'elephantmq.worker.jobs.to.extend.locks',
  JobFinishedTimestamp = 'elephantmq.job.finished.timestamp',
  JobAttemptFinishedTimestamp = 'elephantmq.job.attempt_finished_timestamp',
  JobProcessedTimestamp = 'elephantmq.job.processed.timestamp',
  JobResult = 'elephantmq.job.result',
  JobFailedReason = 'elephantmq.job.failed.reason',
  FlowName = 'elephantmq.flow.name',
  JobSchedulerId = 'elephantmq.job.scheduler.id',
  JobState = 'elephantmq.job.state',
}

/**
 * Standard metric names for elephantmq telemetry.
 */
export enum MetricNames {
  QueueJobsCount = 'elephantmq.queue.jobs',
  JobsCompleted = 'elephantmq.jobs.completed',
  JobsFailed = 'elephantmq.jobs.failed',
  JobsDelayed = 'elephantmq.jobs.delayed',
  JobsRetried = 'elephantmq.jobs.retried',
  JobsWaiting = 'elephantmq.jobs.waiting',
  JobsWaitingChildren = 'elephantmq.jobs.waiting_children',
  JobDuration = 'elephantmq.job.duration',
}

export enum SpanKind {
  INTERNAL = 0,
  SERVER = 1,
  CLIENT = 2,
  PRODUCER = 3,
  CONSUMER = 4,
}
