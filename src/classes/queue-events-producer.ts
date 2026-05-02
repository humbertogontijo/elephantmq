import { QueueEventsProducerOptions } from '../interfaces';
import { QueueBase } from './queue-base';
import { PgPoolConnection } from './pg-connection';
import { escapeSchema } from './queue-identity';
import { trimEventsForQueue } from './events-trimmer';

/**
 * The QueueEventsProducer class is used for publishing custom events.
 */
export class QueueEventsProducer extends QueueBase {
  constructor(
    name: string,
    opts: QueueEventsProducerOptions = {
      connection: {},
    },
    Connection?: typeof PgPoolConnection,
  ) {
    super(name, opts, Connection);

    this.opts = opts;
  }

  /**
   * Publish custom event to be processed in QueueEvents.
   * @param argsObj - Event payload
   * @param maxEvents - Max quantity of events to be saved
   */
  async publishEvent<T extends { eventName: string }>(
    argsObj: T,
    maxEvents = 1000,
  ): Promise<string> {
    const client = await this.client;
    const qid = await this.queueId;
    const S = escapeSchema(this.schema);
    const { eventName, ...restArgs } = argsObj;
    const {
      rows: [row],
    } = await client.query<{ id: string }>(
      `insert into ${S}.emq_events (queue_id, event, args) values ($1, $2, $3::jsonb)
       returning id::text as id`,
      [qid, eventName, JSON.stringify(restArgs)],
    );
    const id = row?.id ?? '0';
    if (maxEvents > 0) {
      void trimEventsForQueue(client, this.schema, qid, maxEvents);
    }
    return id;
  }

  /**
   * Closes the connection and returns a promise that resolves when the connection is closed.
   */
  async close(): Promise<void> {
    if (!this.closing) {
      this.closing = this.connection.close();
    }
    await this.closing;
  }
}
