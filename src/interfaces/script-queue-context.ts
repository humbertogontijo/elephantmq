import { EmqClient } from './connection';
import { QueueBaseOptions } from './queue-options';
import { KeysMap } from '../classes/queue-keys';

export interface ScriptQueueContext {
  opts: QueueBaseOptions;
  toKey: (type: string) => string;
  keys: KeysMap;
  closing: Promise<void> | undefined;
  /**
   * Returns a promise that resolves to the pg pool handle. Normally used only by subclasses.
   */
  get client(): Promise<EmqClient>;
  /** Postgres server version string e.g. `'16.3'`. */
  get postgresVersion(): string;
  /** Postgres schema for elephantmq tables */
  readonly schema: string;
  /** Resolved emq_queues.id for this queue */
  get queueId(): Promise<number>;
}
