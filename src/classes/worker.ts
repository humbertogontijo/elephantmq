import * as fs from 'fs';
import { URL } from 'url';
import * as path from 'path';
import { v4 } from 'uuid';

import {
  EmqClient,
  GetNextJobOptions,
  JobJsonRaw,
  LockManagerWorkerContext,
  MinimalQueue,
  Span,
  WorkerOptions,
} from '../interfaces';
import { Processor } from '../types/processor';
import {
  delay,
  DELAY_TIME_1,
  isNotConnectionError,
  isPgPool,
} from '../utils';
import { QueueBase } from './queue-base';
import { escapeSchema } from './queue-identity';
import { ChildPool } from './child-pool';
import { Job } from './job';
import { PgPoolConnection, type PgConnection } from './pg-connection';
import sandbox from './sandbox';
import { AsyncFifoQueue } from './async-fifo-queue';
import {
  DelayedError,
  RateLimitError,
  RATE_LIMIT_ERROR,
  WaitingChildrenError,
  WaitingError,
  UnrecoverableError,
} from './errors';
import { SpanKind, TelemetryAttributes } from '../enums';
import { JobScheduler } from './job-scheduler';
import { LockManager } from './lock-manager';
import { channelForDelayed, channelForMarker } from './notification-manager';
import type { WorkerListener } from './worker/worker-listener';
import { maximumBlockTimeout } from './worker/constants';
import { execMoveStalledJobsToWait } from './worker/move-stalled-jobs';
import { runStalledCheckerLoop } from './worker/stalled-checker-loop';
import { waitForWorkerRateLimit } from './worker/rate-limit-wait';
import { bootstrapNotificationMarkersFromRows } from './worker/notification-bootstrap';


// note: sandboxed processors would also like to define concurrency per process
// for better resource utilization.

export type { WorkerListener };

/**
 * A worker that processes jobs from a queue.
 *
 * As soon as the class is instantiated and a connection to PostgreSQL is
 * established the worker starts pulling and running jobs.
 */
export class Worker<
  DataType = any,
  ResultType = any,
  NameType extends string = string,
> extends QueueBase {
  readonly opts!: WorkerOptions;
  readonly id: string;

  private abortDelayController: AbortController | null = null;
  private blockingConnection!: PgPoolConnection;
  private blockUntil = 0;
  private _concurrency!: number;
  private childPool?: ChildPool;
  private drained = false;
  private limitUntil = 0;
  protected lockManager!: LockManager;
  private processorAcceptsSignal = false;

  private stalledCheckerRunning = false;
  private stalledCheckStopper?: () => void;
  private waiting: Promise<number> | null = null;
  /** Resolvers for {@link waitForJob} when using NotificationManager wakeups */
  private pendingJobResolvers: Array<(v: number) => void> = [];
  // BullMQ's marker zset accumulates at most one element per addJob; its
  // bzpopmin returns immediately when a marker is present (with score 0) or
  // blocks otherwise. We emulate that with a small latch: any marker/delayed
  // NOTIFY that arrives while no `waitForJob` is pending sets
  // `pendingMarker`, which the next `waitForJob` consumes synchronously and
  // returns 0 from without blocking — matching the extra moveToActive call
  // that the "do not call moveToActive more than ..." worker tests assert.
  private pendingMarker = false;
  // Tracks whether we have fetched at least one real job since the last
  // drain transition. Used to only latch `pendingMarker` on the "active
  // → drained" edge rather than on the "startup with empty queue"
  // edge (which would add a spurious extra moveToActive call).
  private fetchedSinceDrain = false;
  private notificationSubscribed = false;

  protected _jobScheduler?: JobScheduler;

  protected paused = false;
  protected processFn?: Processor<DataType, ResultType, NameType>;
  protected running = false;
  protected mainLoopRunning: Promise<void> | null = null;

  static RateLimitError(): Error {
    return new RateLimitError();
  }

  constructor(
    name: string,
    processor?: string | URL | null | Processor<DataType, ResultType, NameType>,
    opts?: WorkerOptions,
    Connection?: typeof PgPoolConnection,
  ) {
    if (!opts?.connection) {
      throw new Error('Worker requires a connection');
    }
    super(
      name,
      {
        drainDelay: 5,
        concurrency: 1,
        lockDuration: 30000,
        maximumRateLimitDelay: 30000,
        maxStalledCount: 1,
        stalledInterval: 30000,
        autorun: true,
        runRetryDelay: 15000,
        ...opts,
        blockingConnection: true,
      },
      Connection,
    );

    if (
      typeof this.opts.maxStalledCount !== 'number' ||
      this.opts.maxStalledCount < 0
    ) {
      throw new Error('maxStalledCount must be greater or equal than 0');
    }

    if (
      typeof this.opts.maxStartedAttempts === 'number' &&
      this.opts.maxStartedAttempts < 0
    ) {
      throw new Error('maxStartedAttempts must be greater or equal than 0');
    }

    if (
      typeof this.opts.stalledInterval !== 'number' ||
      this.opts.stalledInterval <= 0
    ) {
      throw new Error('stalledInterval must be greater than 0');
    }

    if (typeof this.opts.drainDelay !== 'number' || this.opts.drainDelay <= 0) {
      throw new Error('drainDelay must be greater than 0');
    }

    this.concurrency =
      typeof this.opts.concurrency === 'number' && isFinite(this.opts.concurrency)
        ? this.opts.concurrency
        : 1;

    this.opts.lockRenewTime =
      this.opts.lockRenewTime ?? (this.opts.lockDuration ?? 30000) / 2;

    this.id = v4();

    this.createLockManager();

    if (processor) {
      if (typeof processor === 'function') {
        this.processFn = processor;
        // Check if processor accepts signal parameter (3rd parameter)
        this.processorAcceptsSignal = processor.length >= 3;
      } else {
        // SANDBOXED
        if (processor instanceof URL) {
          if (!fs.existsSync(processor)) {
            throw new Error(
              `URL ${processor} does not exist in the local file system`,
            );
          }
          processor = processor.href;
        } else {
          const supportedFileTypes = ['.js', '.ts', '.flow', '.cjs', '.mjs'];
          const processorFile =
            processor +
            (supportedFileTypes.includes(path.extname(processor)) ? '' : '.js');

          if (!fs.existsSync(processorFile)) {
            throw new Error(`File ${processorFile} does not exist`);
          }
        }

        // Separate paths so that bundling tools can resolve dependencies easier
        const dirname = path.dirname(module.filename || __filename);
        const workerThreadsMainFile = path.join(dirname, 'main-worker.js');
        const spawnProcessMainFile = path.join(dirname, 'main.js');

        let mainFilePath = this.opts.useWorkerThreads
          ? workerThreadsMainFile
          : spawnProcessMainFile;

        try {
          fs.statSync(mainFilePath); // would throw if file not exists
        } catch (_) {
          const mainFile = this.opts.useWorkerThreads
            ? 'main-worker.js'
            : 'main.js';
          mainFilePath = path.join(
            process.cwd(),
            `dist/cjs/classes/${mainFile}`,
          );
          fs.statSync(mainFilePath);
        }

        this.childPool = new ChildPool({
          mainFile: mainFilePath,
          useWorkerThreads: this.opts.useWorkerThreads,
          workerForkOptions: this.opts.workerForkOptions,
          workerThreadsOptions: this.opts.workerThreadsOptions,
        });

        this.createSandbox(processor);
        this.processorAcceptsSignal = true;
      }

      if (this.opts.autorun) {
        void this.run().catch(error => this.emit('error', error));
      }
    }

    const workerClientName = opts!.name
      ? `${this.clientName()}:w:${opts.name}`
      : `${this.clientName()}`;

    this.blockingConnection = new PgPoolConnection(
      isPgPool(opts!.connection)
        ? opts!.connection
        : { ...(opts!.connection as Record<string, unknown>) },
      {
        shared: isPgPool(opts!.connection),
        blocking: true,
        skipVersionCheck: opts!.skipVersionCheck,
        skipMigrations: opts!.skipMigrations,
        schema: opts!.schema,
        clientName: workerClientName,
      },
    );
    this.blockingConnection.on('error', error => this.emit('error', error));
    this.blockingConnection.on('ready', () =>
      setTimeout(() => this.emit('ready'), 0),
    );
  }

  /**
   * Creates and configures the lock manager for processing jobs.
   * This method can be overridden in subclasses to customize lock manager behavior.
   */
  protected createLockManager() {
    this.lockManager = new LockManager(this as LockManagerWorkerContext, {
      lockRenewTime: this.opts.lockRenewTime ?? 15000,
      lockDuration: this.opts.lockDuration ?? 30000,
      workerId: this.id,
      workerName: this.opts.name,
    });
  }

  /**
   * Creates and configures the sandbox for processing jobs.
   * This method can be overridden in subclasses to customize sandbox behavior.
   *
   * @param processor - The processor file path, URL, or function to be sandboxed
   */
  protected createSandbox(
    processor: string | URL | null | Processor<DataType, ResultType, NameType>,
  ) {
    this.processFn = sandbox<DataType, ResultType, NameType>(
      processor,
      this.childPool!,
    ).bind(this);
  }

  /**
   * Public accessor method for LockManager to extend locks.
   * This delegates to the protected scripts object.
   */
  async extendJobLocks(
    jobIds: string[],
    tokens: string[],
    duration: number,
  ): Promise<string[]> {
    return this.scripts.extendLocks(jobIds, tokens, duration);
  }

  emit<U extends keyof WorkerListener<DataType, ResultType, NameType>>(
    event: U,
    ...args: Parameters<WorkerListener<DataType, ResultType, NameType>[U]>
  ): boolean {
    return super.emit(event, ...args);
  }

  off<U extends keyof WorkerListener<DataType, ResultType, NameType>>(
    eventName: U,
    listener: WorkerListener<DataType, ResultType, NameType>[U],
  ): this {
    super.off(eventName, listener);
    return this;
  }

  on<U extends keyof WorkerListener<DataType, ResultType, NameType>>(
    event: U,
    listener: WorkerListener<DataType, ResultType, NameType>[U],
  ): this {
    super.on(event, listener);
    return this;
  }

  once<U extends keyof WorkerListener<DataType, ResultType, NameType>>(
    event: U,
    listener: WorkerListener<DataType, ResultType, NameType>[U],
  ): this {
    super.once(event, listener);
    return this;
  }

  protected callProcessJob(
    job: Job<DataType, ResultType, NameType>,
    token: string,
    signal?: AbortSignal,
  ): Promise<ResultType> {
    return this.processFn!(job, token, signal);
  }

  protected createJob(
    data: JobJsonRaw,
    jobId: string,
  ): Job<DataType, ResultType, NameType> {
    return this.Job.fromJSON(this as MinimalQueue, data, jobId) as Job<
      DataType,
      ResultType,
      NameType
    >;
  }

  /**
   *
   * Waits until the worker is ready to start processing jobs.
   * In general only useful when writing tests.
   *
   */
  async waitUntilReady(): Promise<EmqClient> {
    await super.waitUntilReady();
    return this.blockingConnection.client;
  }

  /**
   * Cancels a specific job currently being processed by this worker.
   * The job's processor function will receive an abort signal.
   *
   * @param jobId - The ID of the job to cancel
   * @param reason - Optional reason for the cancellation
   * @returns true if the job was found and cancelled, false otherwise
   */
  cancelJob(jobId: string, reason?: string): boolean {
    return this.lockManager.cancelJob(jobId, reason);
  }

  /**
   * Cancels all jobs currently being processed by this worker.
   * All active job processor functions will receive abort signals.
   *
   * @param reason - Optional reason for the cancellation
   */
  cancelAllJobs(reason?: string): void {
    this.lockManager.cancelAllJobs(reason);
  }

  set concurrency(concurrency: number) {
    if (
      typeof concurrency !== 'number' ||
      concurrency < 1 ||
      !isFinite(concurrency)
    ) {
      throw new Error('concurrency must be a finite number greater than 0');
    }
    this._concurrency = concurrency;
  }

  get concurrency() {
    return this._concurrency;
  }

  get jobScheduler(): Promise<JobScheduler> {
    return new Promise<JobScheduler>(async resolve => {
      if (!this._jobScheduler) {
        const connection = await this.client;
        this._jobScheduler = new JobScheduler(this.name, {
          ...this.opts,
          connection,
        });
        this._jobScheduler.on('error', this.emit.bind(this, 'error'));
      }
      resolve(this._jobScheduler);
    });
  }

  async run() {
    if (!this.processFn) {
      throw new Error('No process function is defined.');
    }

    if (this.running) {
      throw new Error('Worker is already running.');
    }

    try {
      this.running = true;

      if (this.closing || this.paused) {
        return;
      }

      await this.startStalledCheckTimer();

      if (!this.opts.skipLockRenewal) {
        this.lockManager.start();
      }

      await this.blockingConnection.waitUntilReady();
      await this.ensureNotificationSubscriptions();

      const client = await this.client;
      const bclient = await this.blockingConnection.client;

      this.mainLoopRunning = this.mainLoop(client, bclient);

      // We must await here or finally will be called too early.
      await this.mainLoopRunning;
    } finally {
      this.running = false;
    }
  }

  private async waitForRateLimit(): Promise<void> {
    await waitForWorkerRateLimit({
      getLimitUntil: () => this.limitUntil,
      computeRateLimitDelay: ms => this.getRateLimitDelay(ms),
      delay: (ms, ctrl) => this.delay(ms, ctrl),
      resetAbortDelayForRateLimitSleep: () => {
        this.abortDelayController?.abort();
        const c = new AbortController();
        this.abortDelayController = c;
        return c;
      },
      setDrained: v => {
        this.drained = v;
      },
      clearRateLimitExpiry: () => {
        this.limitUntil = 0;
      },
    });
  }

  /**
   * This is the main loop in BullMQ. Its goals are to fetch jobs from the queue
   * as efficiently as possible, providing concurrency and minimal unnecessary calls
   * to Redis.
   */
  private async mainLoop(client: EmqClient, bclient: EmqClient) {
    const asyncFifoQueue = new AsyncFifoQueue<void | Job<
      DataType,
      ResultType,
      NameType
    >>();

    let tokenPostfix = 0;
    // Start in the "drained" state and rely on the marker latch seeded
    // by `ensureNotificationSubscriptions` to drive the first
    // `moveToActive` call. This avoids the extra "initial empty
    // queue" moveToActive call that BullMQ hides behind its blocking
    // `bzpopmin` and keeps the worker call-count tests deterministic.
    this.drained = true;
    this.fetchedSinceDrain = false;

    while ((!this.closing && !this.paused) || asyncFifoQueue.numTotal() > 0) {
      /**
       * This inner loop tries to fetch jobs concurrently, but if we are waiting for a job
       * to arrive at the queue we should not try to fetch more jobs (as it would be pointless)
       */
      while (
        !this.closing &&
        !this.paused &&
        !this.waiting &&
        asyncFifoQueue.numTotal() < this._concurrency &&
        !this.isRateLimited()
      ) {
        const token = `${this.id}:${tokenPostfix++}`;

        const fetchedJob = this.retryIfFailed<void | Job<
          DataType,
          ResultType,
          NameType
        >>(() => this._getNextJob(client, bclient, token, { block: true }), {
          delayInMs: this.opts.runRetryDelay ?? 15000,
          onlyEmitError: true,
        });
        asyncFifoQueue.add(fetchedJob);

        if (this.waiting && asyncFifoQueue.numTotal() > 1) {
          // We are waiting for jobs but we have others that we could start processing already
          break;
        }

        // We await here so that we fetch jobs in sequence, this is important to avoid unnecessary calls
        // to Redis in high concurrency scenarios.
        const job = await fetchedJob;

        // No more jobs waiting but we have others that could start processing already
        if (!job && asyncFifoQueue.numTotal() > 1) {
          break;
        }

        // If there are potential jobs to be processed and blockUntil is set, we should exit to avoid waiting
        // for processing this job.
        if (this.blockUntil) {
          break;
        }
      }

      // Since there can be undefined jobs in the queue (when a job fails or queue is empty)
      // we iterate until we find a job.
      let job: Job<DataType, ResultType, NameType> | void;
      do {
        job = await asyncFifoQueue.fetch();
      } while (!job && asyncFifoQueue.numQueued() > 0);

      if (job) {
        const jobToken = job.token ?? '';
        asyncFifoQueue.add(
          this.processJob(
            <Job<DataType, ResultType, NameType>>job,
            jobToken,
            () => asyncFifoQueue.numTotal() <= this._concurrency,
          ),
        );
      } else if (asyncFifoQueue.numQueued() === 0) {
        await this.waitForRateLimit();
      }
    }
  }

  /**
   * Returns a promise that resolves to the next job in queue.
   * @param token - worker token to be assigned to retrieved job
   * @returns a Job or undefined if no job was available in the queue.
   */
  async getNextJob(token: string, { block = true }: GetNextJobOptions = {}) {
    const nextJob = await this._getNextJob(
      await this.client,
      await this.blockingConnection.client,
      token,
      { block },
    );

    return this.trace<Job<DataType, ResultType, NameType> | undefined>(
      SpanKind.INTERNAL,
      'getNextJob',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.WorkerId]: this.id,
          [TelemetryAttributes.QueueName]: this.name,
          [TelemetryAttributes.WorkerName]: this.opts.name,
          [TelemetryAttributes.WorkerOptions]: JSON.stringify({ block }),
          [TelemetryAttributes.JobId]: nextJob?.id,
        });

        return nextJob;
      },
      nextJob?.opts?.telemetry?.metadata,
    );
  }

  private async _getNextJob(
    client: EmqClient,
    bclient: EmqClient,
    token: string,
    { block = true }: GetNextJobOptions = {},
  ): Promise<Job<DataType, ResultType, NameType> | undefined> {
    if (this.paused) {
      return;
    }

    if (this.closing) {
      return;
    }

    let job: Job<DataType, ResultType, NameType> | undefined;
    if (this.drained && block && !this.limitUntil && !this.waiting) {
      this.waiting = this.waitForJob(bclient, this.blockUntil);
      try {
        this.blockUntil = await this.waiting;

        if (this.blockUntil <= 0 || this.blockUntil - Date.now() < 1) {
          job = await this.moveToActive(
            client,
            token,
            this.opts.name ?? this.id,
          );
        }
      } finally {
        this.waiting = null;
      }
    } else {
      if (!this.isRateLimited()) {
        job = await this.moveToActive(
          client,
          token,
          this.opts.name ?? this.id,
        );
        if (process.env.EMQ_DBG_WORKER) {
          try {
            require('fs').appendFileSync(
              '/tmp/emq-dbg.log',
              `[worker ${this.id}] nowait moveToActive job=${job?.id}` +
                ` drained=${this.drained} blockUntil=${this.blockUntil}\n`,
            );
          } catch {
            /* ignore */
          }
        }
      }
    }

    if (job) {
      // Track the job (and create its abort controller, if the processor
      // accepts one) BEFORE emitting 'active'. Listeners frequently use
      // 'active' as the moment to call `worker.cancelJob(...)`, and
      // without registering the abort controller first, that call
      // silently returns false because the lock manager hasn't seen the
      // job yet.
      this.lockManager.trackJob(
        job.id,
        token,
        job.processedOn ?? 0,
        this.processorAcceptsSignal,
      );
      this.emit('active', job, 'waiting');
    }

    return job;
  }

  /**
   * Overrides the rate limit to be active for the next jobs (same effect as {@link Queue.rateLimit}).
   * @param expireTimeMs - expire time in ms of this rate limit.
   */
  async rateLimit(expireTimeMs: number): Promise<void> {
    await this.trace<void>(
      SpanKind.INTERNAL,
      'rateLimit',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.WorkerId]: this.id,
          [TelemetryAttributes.WorkerRateLimit]: expireTimeMs,
        });

        await this.client.then(async client => {
          const qid = await this.queueId;
          const S = escapeSchema(this.schema);
          await client.query(
            `insert into ${S}.emq_rate_limit_state (queue_id, tokens, expires_at)
             values ($1, $2, now() + ($3::bigint * interval '1 millisecond'))
             on conflict (queue_id) do update set
               tokens = $2,
               expires_at = now() + ($3::bigint * interval '1 millisecond')`,
            [qid, Number.MAX_SAFE_INTEGER, expireTimeMs],
          );
        });
      },
    );
  }

  get minimumBlockTimeout(): number {
    // 1 ms — the granularity of our process_at timestamps. Delays and rate
    // limits do not exceed this resolution.
    return 0.001;
  }

  private isRateLimited(): boolean {
    return this.limitUntil > Date.now();
  }

  protected async moveToActive(
    client: EmqClient,
    token: string,
    name?: string,
  ): Promise<Job<DataType, ResultType, NameType> | undefined> {
    const [jobData, id, rateLimitDelay, delayUntil] =
      await this.scripts.moveToActive(client, token, name);
    this.updateDelays(rateLimitDelay, delayUntil);

    // Only the mainLoop's moveToActive (not completion-chain fetches
    // inside `handleCompleted`/`handleFailed`) is responsible for the
    // marker latch that drives the drain-detection call. BullMQ's Lua
    // `moveToActive` re-adds the base marker when the queue becomes
    // empty so the blocking `bzpopmin` wakes once more. We emulate
    // that here: if the result is empty and we were processing jobs,
    // latch `pendingMarker` once so the next waitForJob returns
    // immediately; on a cold queue (fetchedSinceDrain=false) we leave
    // the latch false so the worker blocks.
    if (!jobData) {
      if (!this.drained) {
        this.emit('drained');
        this.drained = true;
        if (this.fetchedSinceDrain) {
          this.pendingMarker = true;
        }
        this.fetchedSinceDrain = false;
      }
    }

    return this.nextJobFromJobData(jobData, id, token);
  }

  private wakeupJobWaiters(value: number): void {
    const rs = this.pendingJobResolvers;
    this.pendingJobResolvers = [];
    for (const r of rs) {
      try {
        r(value);
      } catch {
        /* ignore */
      }
    }
  }

  private onQueueMarkerNotify = (_payload?: string) => {
    if (process.env.EMQ_DBG_WORKER) {
      try {
        require('fs').appendFileSync(
          '/tmp/emq-dbg.log',
          `[worker ${this.id}] marker payload=${_payload}` +
            ` blockUntil=${this.blockUntil} pending=${this.pendingJobResolvers.length}\n`,
        );
      } catch {
        /* ignore */
      }
    }
    // BullMQ's `bzpopmin(markerKey)` returns immediately when the marker
    // zset is non-empty. We don't store markers server-side, so latch the
    // fact that a wake was requested here and consume it in `waitForJob` on
    // the next call when no waiter is currently parked.
    if (this.pendingJobResolvers.length === 0) {
      this.pendingMarker = true;
    }
    this.wakeupJobWaiters(0);
  };

  private onQueueDelayedNotify = (payload?: string) => {
    if (process.env.EMQ_DBG_WORKER) {
      try {
        require('fs').appendFileSync(
          '/tmp/emq-dbg.log',
          `[worker ${this.id}] delayed payload=${payload} blockUntil=${this.blockUntil}\n`,
        );
      } catch {
        /* ignore */
      }
    }
    if (payload == null || payload === '') {
      this.wakeupJobWaiters(0);
      return;
    }
    const newBlockUntil = parseInt(payload, 10);
    let v = newBlockUntil;
    if (this.blockUntil && newBlockUntil > this.blockUntil) {
      v = this.blockUntil;
    }
    // If the worker parks in `waitForJob` after this NOTIFY arrives (e.g. the
    // main loop transitioned to the blocking path between the processor
    // emitting 'delayed' and the next iteration), wakeupJobWaiters will be a
    // no-op and the `blockUntil` value we just learned about would be lost.
    // Persist it on the worker so the next `waitForJob` sees a concrete
    // block_until and sizes its timeout accordingly (otherwise it would use
    // `drainDelay` and miss the fake-timer tick performed by the caller that
    // was waiting on the `delayed` event).
    if (this.pendingJobResolvers.length === 0) {
      if (!this.blockUntil || newBlockUntil < this.blockUntil) {
        this.blockUntil = newBlockUntil;
      }
    }
    this.wakeupJobWaiters(v);
    // If the newly-delayed job's process_at is earlier than our active
    // rate-limit sleep, abort the sleep at that time so moveToActive can
    // promote the row as soon as it's ready. Without this, workers honour
    // the full maximumRateLimitDelay even when a delayed job would have
    // been available much earlier, which tests like
    // `rate_limiter.test.ts > should promote jobs after maximumRateLimitDelay`
    // rely on (delay=1500ms with maxRLDelay=3000ms must still see the job
    // promoted before the cap elapses).
    if (newBlockUntil > 0 && this.abortDelayController) {
      const msUntil = newBlockUntil - Date.now();
      if (msUntil <= 0) {
        this.abortDelayController.abort();
      } else if (msUntil < this.limitUntil - Date.now()) {
        const ctrl = this.abortDelayController;
        setTimeout(() => ctrl.abort(), msUntil).unref();
      }
    }
  };

  private async ensureNotificationSubscriptions(): Promise<void> {
    if (this.notificationSubscribed) {
      return;
    }
    const nm = (this.blockingConnection as PgConnection).notificationManager;
    if (!nm) {
      return;
    }
    const qn = this.qualifiedName;
    await nm.subscribeToQueueMarker(qn, this.onQueueMarkerNotify);
    await nm.subscribeToDelayed(qn, this.onQueueDelayedNotify);
    this.notificationSubscribed = true;
    // Seed the marker latch only if the wait/prioritized/delayed lists
    // already have entries committed before we started subscribing;
    // those NOTIFYs were lost because no listener existed yet. This
    // mirrors BullMQ's Redis behaviour where the marker zset survives
    // across worker restarts. For workers that start on a truly empty
    // queue we leave `pendingMarker` false, so the test harness won't
    // see a bonus `moveToActive` before the first `addBulk` arrives.
    try {
      const client = await this.client;
      const qid = await this.queueId;
      await bootstrapNotificationMarkersFromRows(client, qid, this.schema, {
        getBlockUntil: () => this.blockUntil,
        setBlockUntil: v => {
          this.blockUntil = v;
        },
        setPendingMarker: () => {
          this.pendingMarker = true;
        },
        wakeupJobWaiters: v => this.wakeupJobWaiters(v),
      });
    } catch {
      // If the probe fails we fall back to the conservative latch so we
      // don't deadlock a worker that inherits a non-empty queue.
      this.pendingMarker = true;
    }
  }

  private async unsubscribeQueueNotifications(): Promise<void> {
    const nm = (this.blockingConnection as PgConnection).notificationManager;
    if (!nm || !this.notificationSubscribed) {
      return;
    }
    const qn = this.qualifiedName;
    await nm.unsubscribe(channelForMarker(qn), this.onQueueMarkerNotify).catch(() => {});
    await nm.unsubscribe(channelForDelayed(qn), this.onQueueDelayedNotify).catch(() => {});
    this.notificationSubscribed = false;
  }

  private async waitForJob(
    _bclient: EmqClient,
    blockUntil: number,
  ): Promise<number> {
    if (this.paused) {
      return Infinity;
    }

    try {
      if (!this.closing && !this.isRateLimited()) {
        const blockTimeout = this.getBlockTimeout(blockUntil);

        if (blockTimeout > 0) {
          this.updateDelays();

          // Consume the queued-marker latch before taking out a real
          // blocking wait. Mirrors BullMQ's `bzpopmin` returning
          // immediately when the marker zset is non-empty.
          if (this.pendingMarker) {
            this.pendingMarker = false;
            return 0;
          }

          const nm = (this.blockingConnection as PgConnection).notificationManager;
          if (nm) {
            await this.ensureNotificationSubscriptions();
            let settled = false;
            return await new Promise<number>(resolve => {
              const timer = setTimeout(() => {
                if (settled) {
                  return;
                }
                settled = true;
                this.pendingJobResolvers = this.pendingJobResolvers.filter(
                  x => x !== resolver,
                );
                resolve(0);
              }, blockTimeout * 1000);

              const resolver = (v: number) => {
                if (settled) {
                  return;
                }
                settled = true;
                clearTimeout(timer);
                this.pendingJobResolvers = this.pendingJobResolvers.filter(
                  x => x !== resolver,
                );
                resolve(v);
              };

              this.pendingJobResolvers.push(resolver);
            });
          }

          await this.delay(blockTimeout * 1000);
          return 0;
        }

        return 0;
      }
    } catch (error) {
      if (isNotConnectionError(<Error>error)) {
        this.emit('error', <Error>error);
      }
      if (!this.closing) {
        await this.delay();
      }
    }
    return Infinity;
  }

  protected getBlockTimeout(blockUntil: number): number {
    const opts: WorkerOptions = <WorkerOptions>this.opts;

    // when there are delayed jobs
    if (blockUntil) {
      const blockDelay = blockUntil - Date.now();
      // when we reach the time to get new jobs
      if (blockDelay <= 0) {
        return blockDelay;
      } else if (blockDelay < this.minimumBlockTimeout * 1000) {
        return this.minimumBlockTimeout;
      } else {
        // We restrict the maximum block timeout to 10 second to avoid
        // blocking the connection for too long in the case of reconnections
        // reference: https://github.com/taskforcesh/bullmq/issues/1658
        return Math.min(blockDelay / 1000, maximumBlockTimeout);
      }
    } else {
      return Math.max(opts.drainDelay ?? 5, this.minimumBlockTimeout);
    }
  }

  protected getRateLimitDelay(delay: number): number {
    // We restrict the maximum limit delay to the configured maximumRateLimitDelay
    // to be able to promote delayed jobs while the queue is rate limited
    return Math.min(delay, this.opts.maximumRateLimitDelay ?? 30000);
  }

  /**
   *
   * This function is exposed only for testing purposes.
   */
  async delay(
    milliseconds?: number,
    abortController?: AbortController,
  ): Promise<void> {
    await delay(milliseconds || DELAY_TIME_1, abortController);
  }

  private updateDelays(limitDelay = 0, delayUntil = 0) {
    const clampedLimit = Math.max(limitDelay, 0);
    if (clampedLimit > 0) {
      this.limitUntil = Date.now() + clampedLimit;
    } else {
      this.limitUntil = 0;
    }
    this.blockUntil = Math.max(delayUntil, 0) || 0;
  }

  protected async nextJobFromJobData(
    jobData?: JobJsonRaw,
    jobId?: string,
    token?: string,
  ): Promise<Job<DataType, ResultType, NameType> | undefined> {
    if (!jobData) {
      // Drain-detection latching is handled in `moveToActive` so that
      // concurrent completion-chain fetches (via `handleCompleted` /
      // `handleFailed` → `nextJobFromJobData`) don't re-arm
      // `pendingMarker` after the mainLoop has already consumed it.
      // We still emit `drained` here for compatibility with callers
      // that drive completion-chain fetches (BullMQ emits `drained`
      // from the same path).
      if (!this.drained) {
        this.emit('drained');
        this.drained = true;
        this.fetchedSinceDrain = false;
      }
    } else {
      this.drained = false;
      this.fetchedSinceDrain = true;
      const job = this.createJob(jobData, jobId!);
      job.token = token;
      if (this.opts.name) {
        job.processedBy = job.processedBy ?? this.opts.name;
      }

      if (job.repeatJobKey && job.opts.repeat) {
        const repeatJobKey = job.repeatJobKey;
        const repeatOpts = job.opts.repeat;
        try {
          await this.retryIfFailed(
            async () => {
              const jobScheduler = await this.jobScheduler;
              // emq_update_job_scheduler_v1 validates producerId against
              // repeat:<schedulerId>:<scheduler.next_millis>. That millis must match the iteration slot;
              // prefer opts.prevMillis (set when the delayed iteration was materialised) over job.id in case
              // the id string ever diverges from the stored scheduler timestamp after PG round-trips.
              const advanceOpts: { override: false; producerId?: string } = {
                override: false,
              };
              const slotMillis = job.opts.prevMillis;
              if (
                typeof slotMillis === 'number' &&
                Number.isFinite(slotMillis) &&
                slotMillis > 0
              ) {
                advanceOpts.producerId = `repeat:${repeatJobKey}:${slotMillis}`;
              }
              await jobScheduler.upsertJobScheduler(
                repeatJobKey,
                repeatOpts,
                job.name,
                job.data,
                job.opts,
                advanceOpts,
              );
            },
            { delayInMs: this.opts.runRetryDelay ?? 15000 },
          );
        } catch (err) {
          // The current job is already claimed in `active` state and must
          // be processed. Surface the scheduling failure on `error` so it
          // is observable, but still hand the job to the processor — the
          // worst case is that the *next* iteration is missed, not that
          // the current one is left in a stuck state.
          const errorMessage = err instanceof Error ? err.message : String(err);
          this.emit(
            'error',
            new Error(
              `Failed to schedule next iteration of "${job.name}": ${errorMessage}`,
            ),
          );
        }
      }
      return job;
    }
    return undefined;
  }

  async processJob(
    job: Job<DataType, ResultType, NameType>,
    token: string,
    fetchNextCallback = () => true,
  ): Promise<void | Job<DataType, ResultType, NameType>> {
    const srcPropagationMetadata = job.opts?.telemetry?.metadata;

    return this.trace<void | Job<DataType, ResultType, NameType>>(
      SpanKind.CONSUMER,
      'process',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.WorkerId]: this.id,
          [TelemetryAttributes.WorkerName]: this.opts.name,
          [TelemetryAttributes.JobId]: job.id,
          [TelemetryAttributes.JobName]: job.name,
        });

        // _getNextJob already registered this job with the lock manager
        // (see comment there). Re-registering would clobber any signal
        // already aborted via worker.cancelJob() between 'active' and
        // here, so just look up the existing tracking entry.
        const abortController =
          this.lockManager.getAbortController(job.id) ??
          this.lockManager.trackJob(
            job.id,
            token,
            job.processedOn ?? Date.now(),
            this.processorAcceptsSignal,
          );

        try {
          const unrecoverableErrorMessage =
            this.getUnrecoverableErrorMessage(job);
          if (unrecoverableErrorMessage) {
            const failed = await this.retryIfFailed<void | Job<
              DataType,
              ResultType,
              NameType
            >>(
              () => {
                this.lockManager.untrackJob(job.id);
                return this.handleFailed(
                  new UnrecoverableError(unrecoverableErrorMessage),
                  job,
                  token,
                  fetchNextCallback,
                  span,
                );
              },
              { delayInMs: this.opts.runRetryDelay ?? 15000, span },
            );
            return failed;
          }

          const result = await this.callProcessJob(
            job,
            token,
            abortController
              ? (abortController.signal as AbortSignal)
              : undefined,
          );
          return await this.retryIfFailed<void | Job<
            DataType,
            ResultType,
            NameType
          >>(
            () => {
              this.lockManager.untrackJob(job.id);
              return this.handleCompleted(
                result,
                job,
                token,
                fetchNextCallback,
                span,
              );
            },
            { delayInMs: this.opts.runRetryDelay ?? 15000, span },
          );
        } catch (err) {
          const failed = await this.retryIfFailed<void | Job<
            DataType,
            ResultType,
            NameType
          >>(
            () => {
              this.lockManager.untrackJob(job.id);
              return this.handleFailed(
                <Error>err,
                job,
                token,
                fetchNextCallback,
                span,
              );
            },
            {
              delayInMs: this.opts.runRetryDelay ?? 15000,
              span,
              onlyEmitError: true,
            },
          );
          return failed;
        } finally {
          this.lockManager.untrackJob(job.id);
          const now = Date.now();

          span?.setAttributes({
            [TelemetryAttributes.JobFinishedTimestamp]: now,
            [TelemetryAttributes.JobAttemptFinishedTimestamp]:
              job.finishedOn ?? now,
            [TelemetryAttributes.JobProcessedTimestamp]: job.processedOn,
          });
        }
      },
      srcPropagationMetadata,
    );
  }

  private getUnrecoverableErrorMessage(
    job: Job<DataType, ResultType, NameType>,
  ) {
    if (job.deferredFailure) {
      return job.deferredFailure;
    }
    if (
      this.opts.maxStartedAttempts &&
      this.opts.maxStartedAttempts < job.attemptsStarted
    ) {
      return 'job started more than allowable limit';
    }
  }

  protected async handleCompleted(
    result: ResultType,
    job: Job<DataType, ResultType, NameType>,
    token: string,
    fetchNextCallback = () => true,
    span?: Span,
  ) {
    if (!this.connection.closing) {
      const completed = await job.moveToCompleted(
        result,
        token,
        fetchNextCallback() && !(this.closing || this.paused),
      );
      if (process.env.EMQ_DBG_WORKER) {
        try {
          require('fs').appendFileSync(
            '/tmp/emq-dbg.log',
            `[worker ${this.id}] handleCompleted job=${job.id}` +
              ` nextRaw=${Array.isArray(completed) ? 'array' : typeof completed}\n`,
          );
        } catch {
          /* ignore */
        }
      }

      span?.addEvent('job completed', {
        [TelemetryAttributes.JobResult]: JSON.stringify(result),
      });

      span?.setAttributes({
        [TelemetryAttributes.JobAttemptsMade]: job.attemptsMade,
      });

      // Advance repeatable schedulers BEFORE emitting `completed`, so
      // listeners that inspect queue state on `completed` see the next
      // delayed iteration already present. BullMQ's `moveToFinished-14.lua`
      // schedules the next iteration atomically inside the same Lua call;
      // our SQL path computes cron `nextMillis` in JS, so the advance must
      // happen here. Not doing it pre-emit produces a timing race where
      // `queueEvents.on('completed')` fires before the next delayed row
      // exists and tests see `delayedCount=0` when BullMQ sees `1`.
      let nextJob: Job<DataType, ResultType, NameType> | undefined;
      if (Array.isArray(completed)) {
        const [jobData, jobId, rateLimitDelay, delayUntil] = completed;
        this.updateDelays(rateLimitDelay, delayUntil);
        nextJob = await this.nextJobFromJobData(jobData, jobId, token);
      }

      this.emit('completed', job, result, 'active');

      return nextJob;
    }
  }

  protected async handleFailed(
    err: Error,
    job: Job<DataType, ResultType, NameType>,
    token: string,
    fetchNextCallback = () => true,
    span?: Span,
  ) {
    if (!this.connection.closing) {
      // Check if the job was manually rate-limited
      if (err.message === RATE_LIMIT_ERROR) {
        const rateLimitTtl = await this.moveLimitedBackToWait(job, token);
        this.limitUntil = rateLimitTtl > 0 ? Date.now() + rateLimitTtl : 0;
        return;
      }

      if (
        err instanceof DelayedError ||
        err.name == 'DelayedError' ||
        err instanceof WaitingError ||
        err.name == 'WaitingError' ||
        err instanceof WaitingChildrenError ||
        err.name == 'WaitingChildrenError'
      ) {
        const client = await this.client;
        return this.moveToActive(client, token, this.opts.name ?? this.id);
      }

      const result = await job.moveToFailed(
        err,
        token,
        fetchNextCallback() && !(this.closing || this.paused),
      );

      this.emit('failed', job, err, 'active');

      span?.addEvent('job failed', {
        [TelemetryAttributes.JobFailedReason]: err.message,
      });
      span?.setAttributes({
        [TelemetryAttributes.JobAttemptsMade]: job.attemptsMade,
      });

      // Note: result can be undefined if moveToFailed fails (e.g., lock was lost)
      if (Array.isArray(result)) {
        const [jobData, jobId, rateLimitDelay, delayUntil] = result;
        this.updateDelays(rateLimitDelay, delayUntil);
        return this.nextJobFromJobData(jobData, jobId, token);
      }
    }
  }

  /**
   *
   * Pauses the processing of this queue only for this worker.
   */
  async pause(doNotWaitActive?: boolean): Promise<void> {
    await this.trace<void>(
      SpanKind.INTERNAL,
      'pause',
      this.name,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.WorkerId]: this.id,
          [TelemetryAttributes.WorkerName]: this.opts.name,
          [TelemetryAttributes.WorkerDoNotWaitActive]: doNotWaitActive,
        });

        if (!this.paused) {
          this.paused = true;
          // Wake in-flight `waitForJob` promises so the main loop can observe
          // `this.paused` and exit. Without this, `whenCurrentJobsFinished`
          // would block until the next NOTIFY or until the block timeout.
          this.wakeupJobWaiters(0);
          if (!doNotWaitActive) {
            await this.whenCurrentJobsFinished();
          }
          this.stalledCheckStopper?.();
          this.emit('paused');
        }
      },
    );
  }

  /**
   * Resumes processing of this worker (if paused). Resolves once the
   * stalled-check timer has been re-armed (when applicable); the `resumed`
   * event is emitted before the promise resolves.
   */
  async resume(): Promise<void> {
    if (this.running && !this.paused) {
      return;
    }
    await this.trace<void>(SpanKind.INTERNAL, 'resume', this.name, async span => {
      span?.setAttributes({
        [TelemetryAttributes.WorkerId]: this.id,
        [TelemetryAttributes.WorkerName]: this.opts.name,
      });

      this.paused = false;

      if (!this.running) {
        if (this.processFn != null) {
          void this.run();
        }
      } else {
        // Main loop is still running (pause was called with
        // doNotWaitActive=true). Restart the stalled checker since pause()
        // stopped it.
        await this.startStalledCheckTimer();
      }
      this.emit('resumed');
    });
  }

  /**
   *
   * Checks if worker is paused.
   *
   * @returns true if worker is paused, false otherwise.
   */
  isPaused(): boolean {
    return !!this.paused;
  }

  /**
   *
   * Checks if worker is currently running.
   *
   * @returns true if worker is running, false otherwise.
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   *
   * Closes the worker and related redis connections.
   *
   * This method waits for current jobs to finalize before returning.
   *
   * @param force - Use force boolean parameter if you do not want to wait for
   * current jobs to be processed. When using telemetry, be mindful that it can
   * interfere with the proper closure of spans, potentially preventing them from being exported.
   *
   * @returns Promise that resolves when the worker has been closed.
   */
  async close(force = false): Promise<void> {
    if (this.closing) {
      return this.closing;
    }

    this.closing = (async () => {
      await this.trace<void>(
        SpanKind.INTERNAL,
        'close',
        this.name,
        async span => {
          span?.setAttributes({
            [TelemetryAttributes.WorkerId]: this.id,
            [TelemetryAttributes.WorkerName]: this.opts.name,
            [TelemetryAttributes.WorkerForceClose]: force,
          });
          this.emit('closing', 'closing queue');
          this.abortDelayController?.abort();

          // Wake any in-flight `waitForJob` promise immediately so the
          // main loop exits without waiting out the full drainDelay.
          this.wakeupJobWaiters(0);
          // Stop the stalled-check timer up front so it cannot fire a
          // query after `connection.close()` and hit "relation does not exist".
          this.stalledCheckStopper?.();

          // Define the async cleanup functions
          const asyncCleanups = [
            () => {
              return force || this.whenCurrentJobsFinished(false);
            },
            () => this.unsubscribeQueueNotifications(),
            () => this.lockManager.close(),
            () => this.childPool?.clean(),
            () => this.blockingConnection.close(force),
            () => this.connection.close(force),
          ];

          // Run cleanup functions sequentially and make sure all are run despite any errors
          for (const cleanup of asyncCleanups) {
            try {
              await cleanup();
            } catch (err) {
              this.emit('error', <Error>err);
            }
          }

          this.closed = true;
          this.emit('closed');
        },
      );
    })();

    return await this.closing;
  }

  /**
   *
   * Manually starts the stalled checker.
   * The check will run once as soon as this method is called, and
   * then every opts.stalledInterval milliseconds until the worker is closed.
   * Note: Normally you do not need to call this method, since the stalled checker
   * is automatically started when the worker starts processing jobs after
   * calling run. However if you want to process the jobs manually you need
   * to call this method to start the stalled checker.
   *
   * @see {@link https://docs.bullmq.io/patterns/manually-fetching-jobs}
   */
  async startStalledCheckTimer(): Promise<void> {
    if (!this.opts.skipStalledCheck) {
      if (!this.closing && !this.stalledCheckerRunning) {
        await this.trace<void>(
          SpanKind.INTERNAL,
          'startStalledCheckTimer',
          this.name,
          async span => {
            span?.setAttributes({
              [TelemetryAttributes.WorkerId]: this.id,
              [TelemetryAttributes.WorkerName]: this.opts.name,
            });

            this.stalledCheckerRunning = true;
            this.stalledChecker()
              .catch(err => {
                this.emit('error', <Error>err);
              })
              .finally(() => {
                this.stalledCheckerRunning = false;
              });
          },
        );
      }
    }
  }

  private async stalledChecker() {
    await runStalledCheckerLoop({
      stalledIntervalMs: this.opts.stalledInterval ?? 30000,
      shouldStop: () => !!(this.closing || this.paused),
      onTick: () =>
        this.checkConnectionError(() =>
          execMoveStalledJobsToWait(this),
        ),
      setStopper: fn => {
        this.stalledCheckStopper = fn;
      },
    });
  }

  /**
   * Returns a promise that resolves when active jobs are cleared
   *
   * @returns
   */
  private async whenCurrentJobsFinished(reconnect = true) {
    //
    // Force reconnection of blocking connection to abort blocking redis call immediately.
    //
    if (this.waiting) {
      // If we are not going to reconnect, we will not wait for the disconnection.
      await this.blockingConnection.disconnect(reconnect);
    } else {
      reconnect = false;
    }

    if (this.mainLoopRunning) {
      await this.mainLoopRunning;
    }

    reconnect && (await this.blockingConnection.reconnect());
  }

  private async retryIfFailed<T>(
    fn: () => Promise<T>,
    opts: {
      delayInMs: number;
      span?: Span;
      maxRetries?: number;
      onlyEmitError?: boolean;
    },
  ): Promise<T | undefined> {
    let retry = 0;
    const maxRetries = opts.maxRetries || Infinity;

    do {
      try {
        return await fn();
      } catch (err) {
        opts.span?.recordException((<Error>err).message);

        if (isNotConnectionError(<Error>err)) {
          // Emit error when not paused or closing; optionally swallow (no throw) when opts.onlyEmitError is set.
          if (!this.paused && !this.closing) {
            this.emit('error', <Error>err);
          }

          if (opts.onlyEmitError) {
            return undefined;
          } else {
            throw err;
          }
        } else {
          if (opts.delayInMs && !this.closing && !this.closed) {
            await this.delay(
              opts.delayInMs,
              this.abortDelayController ?? undefined,
            );
          }

          if (retry + 1 >= maxRetries) {
            // If we've reached max retries, throw the last error
            throw err;
          }
        }
      }
    } while (++retry < maxRetries);
  }

  private async moveStalledJobsToWait() {
    return execMoveStalledJobsToWait(this);
  }

  private moveLimitedBackToWait(
    job: Job<DataType, ResultType, NameType>,
    token: string,
  ) {
    return job.moveToWait(token);
  }
}
