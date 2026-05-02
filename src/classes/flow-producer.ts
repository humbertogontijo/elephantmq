import { EventEmitter } from 'events';
import type { PoolClient } from 'pg';
import { v4 } from 'uuid';
import {
  FlowJob,
  FlowQueuesOpts,
  FlowOpts,
  EmqConnectionListener,
  MinimalQueue,
  ParentOptions,
  PgQueryable,
  QueueBaseOptions,
  EmqClient,
  Tracer,
  ContextManager,
} from '../interfaces';
import { getParentKey, isPgPool, trace } from '../utils';
import { Job } from './job';
import { KeysMap, QueueKeys } from './queue-keys';
import { PgPoolConnection } from './pg-connection';
import { SpanKind, TelemetryAttributes } from '../enums';
import { ensureQueueRow, escapeSchema, resolveSchema } from './queue-identity';
import type { EncodedJobOptions } from '../types';
import { createScripts } from '../utils/create-scripts';

export interface AddNodeOpts {
  txClient: PoolClient;
  node: FlowJob;
  parent?: {
    parentOpts?: ParentOptions;
    parentDependenciesKey?: string;
  };
  /**
   * Queues options that will be applied in each node depending on queue name presence.
   */
  queuesOpts?: FlowQueuesOpts;
}

export interface AddChildrenOpts {
  txClient: PoolClient;
  nodes: FlowJob[];
  parent: {
    parentOpts: ParentOptions;
    parentDependenciesKey: string;
  };
  queuesOpts?: FlowQueuesOpts;
}

export interface NodeOpts {
  /**
   * Root job queue name.
   */
  queueName: string;
  /**
   * Prefix included in job key.
   */
  prefix?: string;
  /**
   * Root job id.
   */
  id: string;
  /**
   * Maximum depth or levels to visit in the tree.
   */
  depth?: number;
  /**
   * Maximum quantity of children per type (processed, unprocessed).
   */
  maxChildren?: number;
}

export interface JobNode {
  job: Job;
  children?: JobNode[];
}

export interface FlowProducerListener extends EmqConnectionListener {
  /**
   * Listen to 'error' event.
   *
   * This event is triggered when an error is throw.
   */
  error: (failedReason: Error) => void;
}

/**
 * This class allows to add jobs with dependencies between them in such
 * a way that it is possible to build complex flows.
 * Note: A flow is a tree-like structure of jobs that depend on each other.
 * Whenever the children of a given parent are completed, the parent
 * will be processed, being able to access the children's result data.
 * All Jobs can be in different queues, either children or parents,
 */
export class FlowProducer extends EventEmitter {
  toKey: (name: string, type: string) => string;
  keys: KeysMap;
  closing: Promise<void> | undefined;
  queueKeys: QueueKeys;

  protected connection: PgPoolConnection;
  protected telemetry: {
    tracer: Tracer | undefined;
    contextManager: ContextManager | undefined;
  } = {
    tracer: undefined,
    contextManager: undefined,
  };

  constructor(
    public opts: QueueBaseOptions,
    Connection: typeof PgPoolConnection = PgPoolConnection,
  ) {
    super();

    if (!opts.connection) {
      throw new Error('FlowProducer requires a connection');
    }

    this.opts = {
      prefix: 'emq',
      ...opts,
    };

    this.connection = new Connection(opts.connection, {
      shared: isPgPool(opts.connection),
      blocking: false,
      skipVersionCheck: opts.skipVersionCheck,
      skipMigrations: opts.skipMigrations,
      schema: opts.schema,
    });

    this.connection.on('error', (error: Error) => this.emit('error', error));
    this.connection.on('close', () => {
      if (!this.closing) {
        this.emit('connection:close');
      }
    });

    this.queueKeys = new QueueKeys(this.opts.prefix);
    this.toKey = (name: string, type: string) => this.queueKeys.toKey(name, type);
    this.keys = {};

    if (opts.telemetry) {
      this.telemetry = opts.telemetry;
    }
  }

  private telemetryForTrace():
    | { tracer: Tracer; contextManager: ContextManager }
    | undefined {
    const t = this.telemetry;
    if (t.tracer != null && t.contextManager != null) {
      return { tracer: t.tracer, contextManager: t.contextManager };
    }
    return undefined;
  }

  emit<U extends keyof FlowProducerListener>(
    event: U,
    ...args: Parameters<FlowProducerListener[U]>
  ): boolean {
    return super.emit(event, ...args);
  }

  off<U extends keyof FlowProducerListener>(
    eventName: U,
    listener: FlowProducerListener[U],
  ): this {
    super.off(eventName, listener);
    return this;
  }

  on<U extends keyof FlowProducerListener>(
    event: U,
    listener: FlowProducerListener[U],
  ): this {
    super.on(event, listener);
    return this;
  }

  once<U extends keyof FlowProducerListener>(
    event: U,
    listener: FlowProducerListener[U],
  ): this {
    super.once(event, listener);
    return this;
  }

  /**
   * Returns a promise that resolves to a redis client. Normally used only by subclasses.
   */
  get client(): Promise<EmqClient> {
    return this.connection.client;
  }

  /**
   * Helper to easily extend Job class calls.
   */
  protected get Job(): typeof Job {
    return Job;
  }

  waitUntilReady(): Promise<EmqClient> {
    return this.client;
  }

  /**
   * Adds a flow.
   *
   * This call would be atomic, either it fails and no jobs will
   * be added to the queues, or it succeeds and all jobs will be added.
   *
   * @param flow - an object with a tree-like structure where children jobs
   * will be processed before their parents.
   * @param opts - options that will be applied to the flow object.
   */
  async add(flow: FlowJob, opts?: FlowOpts): Promise<JobNode> {
    if (this.closing) {
      throw new Error('FlowProducer is closing');
    }
    const pool = await this.connection.client;

    const parentOpts = flow?.opts?.parent;
    const parentKey = getParentKey(parentOpts);
    const parentDependenciesKey = parentKey
      ? `${parentKey}:dependencies`
      : undefined;

    return trace<Promise<JobNode>>(
      this.telemetryForTrace(),
      SpanKind.PRODUCER,
      flow.queueName,
      'addFlow',
      flow.queueName,
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.FlowName]: flow.name,
        });

        const tx = await pool.connect();
        try {
          await tx.query('BEGIN');
          const jobsTree = await this.addNode({
            txClient: tx,
            node: flow,
            queuesOpts: opts?.queuesOptions,
            parent: {
              parentOpts,
              parentDependenciesKey,
            },
          });
          await tx.query('COMMIT');
          return jobsTree;
        } catch (e) {
          await tx.query('ROLLBACK');
          throw e;
        } finally {
          tx.release();
        }
      },
    );
  }

  /**
   * Get a flow.
   *
   * @param opts - an object with options for getting a JobNode.
   */
  async getFlow(opts: NodeOpts): Promise<JobNode> {
    if (this.closing) {
      throw new Error('FlowProducer is closing');
    }
    const client = await this.connection.client;

    const updatedOpts = Object.assign(
      {
        depth: 10,
        maxChildren: 20,
        prefix: this.opts.prefix,
      },
      opts,
    );
    const jobsTree = await this.getNode(client, updatedOpts);

    return jobsTree;
  }

  /**
   * Adds multiple flows.
   *
   * A flow is a tree-like structure of jobs that depend on each other.
   * Whenever the children of a given parent are completed, the parent
   * will be processed, being able to access the children's result data.
   *
   * All Jobs can be in different queues, either children or parents,
   * however this call would be atomic, either it fails and no jobs will
   * be added to the queues, or it succeeds and all jobs will be added.
   *
   * @param flows - an array of objects with a tree-like structure where children jobs
   * will be processed before their parents.
   */
  async addBulk(flows: FlowJob[]): Promise<JobNode[]> {
    if (this.closing) {
      throw new Error('FlowProducer is closing');
    }
    const pool = await this.connection.client;

    return trace<Promise<JobNode[]>>(
      this.telemetryForTrace(),
      SpanKind.PRODUCER,
      '',
      'addBulkFlows',
      '',
      async span => {
        span?.setAttributes({
          [TelemetryAttributes.BulkCount]: flows.length,
          [TelemetryAttributes.BulkNames]: flows
            .map(flow => flow.name)
            .join(','),
        });

        // BullMQ pipelines all operations through a single `multi.exec()`;
        // individual failures surface as error entries in the result array
        // but do not rollback the successful ones. Mirror that by running
        // each root flow in its own transaction and swallowing per-flow
        // errors so that one bad flow cannot corrupt the rest.
        const jobsTrees: JobNode[] = [];
        for (const flow of flows) {
          const parentOpts = flow?.opts?.parent;
          const parentKey = getParentKey(parentOpts);
          const parentDependenciesKey = parentKey
            ? `${parentKey}:dependencies`
            : undefined;

          const tx = await pool.connect();
          let tree: JobNode | undefined;
          try {
            await tx.query('BEGIN');
            tree = await this.addNode({
              txClient: tx,
              node: flow,
              parent: { parentOpts, parentDependenciesKey },
            });
            await tx.query('COMMIT');
          } catch {
            try {
              await tx.query('ROLLBACK');
            } catch {
              /* ignore rollback errors */
            }
            tree = undefined;
          } finally {
            tx.release();
          }

          if (!tree) {
            // Preserve positional alignment with input flows; synthesize a
            // placeholder node with a generated id (BullMQ returns the node
            // with its v4 id even when the pipelined exec entry errored).
            const prefix = flow.prefix ?? this.opts.prefix ?? 'emq';
            const placeholderQueue = this.queueFromNode(
              flow,
              new QueueKeys(prefix),
              prefix,
              pool as unknown as PgQueryable,
            );
            const placeholderJob = new this.Job(
              placeholderQueue,
              flow.name,
              flow.data,
              { ...flow.opts, parent: flow?.opts?.parent },
              flow.opts?.jobId || v4(),
            );
            placeholderJob.id = flow.opts?.jobId || v4();
            tree = { job: placeholderJob };
          }
          jobsTrees.push(tree);
        }
        return jobsTrees;
      },
    );
  }

  /**
   * Add a node (job) of a flow to the queue. This method will recursively
   * add all its children as well. Note that a given job can potentially be
   * a parent and a child job at the same time depending on where it is located
   * in the tree hierarchy.
   *
   * @param txClient - PostgreSQL client (transaction)
   * @param node - the node representing a job to be added to some queue
   * @param parent - parent data sent to children to create the "links" to their parent
   * @returns
   */
  protected async addNode({
    txClient,
    node,
    parent,
    queuesOpts,
  }: AddNodeOpts): Promise<JobNode> {
    const prefix = node.prefix ?? this.opts.prefix ?? 'emq';
    const queue = this.queueFromNode(node, new QueueKeys(prefix), prefix, txClient);
    const queueOpts = queuesOpts && queuesOpts[node.queueName];

    const jobsOpts = queueOpts?.defaultJobOptions ?? {};
    const jobId = node.opts?.jobId || v4();

    return trace<Promise<JobNode>>(
      this.telemetryForTrace(),
      SpanKind.PRODUCER,
      node.queueName,
      'addNode',
      node.queueName,
      async (span, srcPropagationMetadata) => {
        span?.setAttributes({
          [TelemetryAttributes.JobName]: node.name,
          [TelemetryAttributes.JobId]: jobId,
        });
        const opts = node.opts;
        let telemetry = opts?.telemetry;

        if (srcPropagationMetadata && opts) {
          const omitContext = opts.telemetry?.omitContext;
          const telemetryMetadata =
            opts.telemetry?.metadata ||
            (!omitContext && srcPropagationMetadata);

          if (telemetryMetadata || omitContext) {
            telemetry = {
              ...(typeof telemetryMetadata === 'string'
                ? { metadata: telemetryMetadata }
                : {}),
              ...(omitContext !== undefined ? { omitContext } : {}),
            };
          }
        }

        const job = new this.Job(
          queue,
          node.name,
          node.data,
          {
            ...jobsOpts,
            ...opts,
            parent: parent?.parentOpts,
            telemetry,
          },
          jobId,
        );

        const parentKey = getParentKey(parent?.parentOpts);

        if (node.children && node.children.length > 0) {
          // Create the parent job, it will be a job in status "waiting-children".
          const parentId = jobId;
          const queueKeysParent = new QueueKeys(
            node.prefix || this.opts.prefix,
          );

          const jobJson = job.asJSON();
          const scripts = createScripts(queue);
          const addedId = await scripts.addParentJobForFlow(
            txClient,
            jobJson,
            jobJson.opts as EncodedJobOptions,
            jobId,
            {
              parentDependenciesKey: parent?.parentDependenciesKey,
              addToWaitingChildren: true,
              parentKey,
            },
          );
          job.id = addedId;

          // If the parent job was deduplicated (or matched an existing job
          // under a different id), the requested parentId was never stored.
          // BullMQ's pipelined `add` silently no-ops children that reference
          // the non-existent parent key; mirror that by skipping children
          // altogether in this transactional path.
          if (addedId !== parentId) {
            return { job };
          }

          const parentDependenciesKey = `${queueKeysParent.toKey(
            node.queueName,
            parentId,
          )}:dependencies`;

          const children = await this.addChildren({
            txClient,
            nodes: node.children,
            parent: {
              parentOpts: {
                id: parentId,
                queue: queueKeysParent.getQueueQualifiedName(node.queueName),
              },
              parentDependenciesKey,
            },
            queuesOpts,
          });

          const schema = resolveSchema(this.opts);
          const S = escapeSchema(schema);
          const parentQid = await ensureQueueRow(
            txClient,
            schema,
            prefix,
            node.queueName,
          );
          for (const ch of children) {
            const childQid = await ensureQueueRow(
              txClient,
              schema,
              ch.job.prefix,
              ch.job.queueName,
            );
            await txClient.query(
              `select ${S}.emq_link_child_to_parent_v1($1::bigint, $2::text, $3::bigint, $4::text)`,
              [parentQid, String(parentId), childQid, String(ch.job.id)],
            );
          }

          return { job, children };
        } else {
          const addedId = await job.addJob(txClient as unknown as EmqClient, {
            parentDependenciesKey: parent?.parentDependenciesKey,
            parentKey,
          });
          job.id = addedId;

          return { job };
        }
      },
    );
  }

  /**
   * Adds nodes (jobs) of multiple flows to the queue. This method will recursively
   * add all its children as well. Note that a given job can potentially be
   * a parent and a child job at the same time depending on where it is located
   * in the tree hierarchy.
   *
   * @param txClient - PostgreSQL client (transaction)
   * @param nodes - the nodes representing jobs to be added to some queue
   * @returns
   */
  protected addNodes(
    txClient: PoolClient,
    nodes: FlowJob[],
  ): Promise<JobNode[]> {
    return Promise.all(
      nodes.map(node => {
        const parentOpts = node?.opts?.parent;
        const parentKey = getParentKey(parentOpts);
        const parentDependenciesKey = parentKey
          ? `${parentKey}:dependencies`
          : undefined;

        return this.addNode({
          txClient,
          node,
          parent: {
            parentOpts,
            parentDependenciesKey,
          },
        });
      }),
    );
  }

  private async getNode(client: EmqClient, node: NodeOpts): Promise<JobNode> {
    const pfx = node.prefix ?? this.opts.prefix ?? 'emq';
    const queue = this.queueFromNode(
      node,
      new QueueKeys(pfx),
      pfx,
      client as unknown as PgQueryable,
    );

    const job = await this.Job.fromId(queue, node.id);

    if (job) {
      const maxC = node.maxChildren ?? 20;
      const {
        processed = {},
        unprocessed = [],
        failed = [],
        ignored = {},
      } = await job.getDependencies({
        failed: {
          count: maxC,
        },
        processed: {
          count: maxC,
        },
        unprocessed: {
          count: maxC,
        },
        ignored: {
          count: maxC,
        },
      });
      const processedKeys = Object.keys(processed);
      const ignoredKeys = Object.keys(ignored);

      const childrenCount =
        processedKeys.length +
        unprocessed.length +
        ignoredKeys.length +
        failed.length;
      const depthVal = node.depth ?? 10;
      const newDepth = depthVal - 1;
      if (childrenCount > 0 && newDepth > 0) {
        const children = await this.getChildren(
          client,
          [...processedKeys, ...unprocessed, ...failed, ...ignoredKeys],
          newDepth,
          maxC,
        );

        return { job, children };
      } else {
        return { job };
      }
    }
    throw new Error(`Job "${node.id}" not found`);
  }

  private addChildren({ txClient, nodes, parent, queuesOpts }: AddChildrenOpts) {
    return Promise.all(
      nodes.map(node => this.addNode({ txClient, node, parent, queuesOpts })),
    );
  }

  private getChildren(
    client: EmqClient,
    childrenKeys: string[],
    depth: number,
    maxChildren: number,
  ) {
    const getChild = (key: string) => {
      const [prefix, queueName, id] = key.split(':');

      return this.getNode(client, {
        id,
        queueName,
        prefix,
        depth,
        maxChildren,
      });
    };

    return Promise.all([...childrenKeys.map(getChild)]);
  }

  /**
   * Helper factory method that creates a queue-like object
   * required to create jobs in any queue.
   *
   * @param node -
   * @param queueKeys -
   * @returns
   */
  private queueFromNode(
    node: Omit<NodeOpts, 'id' | 'depth' | 'maxChildren'>,
    queueKeys: QueueKeys,
    prefix: string,
    db: PgQueryable,
  ): MinimalQueue {
    const schema = resolveSchema(this.opts);
    const qidPromise = ensureQueueRow(db, schema, prefix, node.queueName);
    return {
      client: Promise.resolve(db as unknown as EmqClient),
      name: node.queueName,
      keys: queueKeys.getKeys(node.queueName),
      toKey: (type: string) => queueKeys.toKey(node.queueName, type),
      opts: { prefix, connection: this.opts.connection, schema },
      qualifiedName: queueKeys.getQueueQualifiedName(node.queueName),
      closing: this.closing,
      waitUntilReady: async () => db as unknown as EmqClient,
      removeListener: this.removeListener.bind(this) as any,
      emit: this.emit.bind(this) as any,
      on: this.on.bind(this) as any,
      postgresVersion: this.connection.postgresVersion,
      trace: async (): Promise<any> => {},
      schema,
      get queueId() {
        return qidPromise;
      },
    };
  }

  /**
   *
   * Closes the connection and returns a promise that resolves when the connection is closed.
   */
  async close(): Promise<void> {
    if (!this.closing) {
      this.closing = this.connection.close();
    }
    await this.closing;
  }

  /**
   *
   * Force disconnects a connection.
   */
  disconnect(): Promise<void> {
    return this.connection.disconnect();
  }
}
