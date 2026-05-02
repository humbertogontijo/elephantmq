import { MinimalQueue } from '../interfaces';
import { Scripts } from '../classes/scripts';

/**
 * Factory method to create a {@link Scripts} instance bound to a queue.
 */
export const createScripts = (queue: MinimalQueue) => {
  const scripts = new Scripts({
    keys: queue.keys,
    client: queue.client,
    get postgresVersion() {
      return queue.postgresVersion;
    },
    toKey: queue.toKey,
    opts: queue.opts,
    closing: queue.closing,
    schema: queue.schema,
    get queueId() {
      return queue.queueId;
    },
  });
  // Tag the Scripts with a stable reference to the owning queue so
  // `Scripts.addJob` can share a per-queue serialization chain across every
  // Scripts instance that gets created for the same Queue.
  (scripts as unknown as { __queueOwner?: object }).__queueOwner =
    queue as unknown as object;
  return scripts;
};
