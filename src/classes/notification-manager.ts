import type { Notification, PoolClient } from 'pg';
import { EventEmitter } from 'events';
import { createHash } from 'crypto';

export type NotificationHandler = (payload: string | undefined) => void;

/**
 * PostgreSQL NOTIFY channel names are limited to NAMEDATALEN-1 (63 bytes).
 * Qualified queue names (`<prefix>:<queueName>`) can easily exceed this when
 * callers use UUID-suffixed prefixes. We therefore hash the qualified name with
 * md5 and prefix a short 6-char tag so every channel is 38 bytes regardless of
 * how long the queue name is. Kept in sync with `tg_emq_*_notify` in
 * `src/sql/migrations/0002_triggers.sql`.
 */
export function channelForMarker(qualifiedQueueName: string): string {
  return 'emq_m_' + createHash('md5').update(qualifiedQueueName).digest('hex');
}

export function channelForDelayed(qualifiedQueueName: string): string {
  return 'emq_d_' + createHash('md5').update(qualifiedQueueName).digest('hex');
}

export function channelForEvents(qualifiedQueueName: string): string {
  return 'emq_e_' + createHash('md5').update(qualifiedQueueName).digest('hex');
}

/**
 * Multiplexes LISTEN on a dedicated listener {@link PoolClient}, tolerating
 * subscribe-before-init and reconnect by queueing LISTENs until a client is bound.
 *
 * Contract:
 * - `subscribe` never throws because the listener PoolClient is not ready yet;
 *   the channel is recorded and LISTEN is replayed on the next successful
 *   `rebindListenerClient()`.
 * - After `close()`, subscribe/unsubscribe are no-ops so teardown races are harmless.
 */
export class NotificationManager extends EventEmitter {
  private channels = new Map<string, Set<NotificationHandler>>();
  /** Channels where the server has an active LISTEN on the current connection. */
  private listening = new Set<string>();
  private wiredClient: PoolClient | null = null;
  private closed = false;

  private readonly onNotification = (msg: Notification) => {
    const hs = this.channels.get(msg.channel);
    if (!hs) {
      return;
    }
    for (const h of hs) {
      try {
        h(msg.payload ?? undefined);
      } catch (e) {
        this.emit('error', e);
      }
    }
  };

  constructor(private getListenerClient: () => Promise<PoolClient | null>) {
    super();
    this.setMaxListeners(100);
  }

  private quoteIdent(channel: string): string {
    return '"' + channel.replace(/"/g, '""') + '"';
  }

  /**
   * Wire the notification fan-out to the current listener client and replay LISTENs.
   * Invoked by `PgConnection` after (re)establishing the listener PoolClient.
   */
  async rebindListenerClient(): Promise<void> {
    if (this.closed) {
      return;
    }
    if (this.wiredClient) {
      this.wiredClient.removeListener('notification', this.onNotification);
      this.wiredClient = null;
    }
    const client = await this.getListenerClient();
    if (!client) {
      return;
    }
    this.wiredClient = client;
    client.on('notification', this.onNotification);
    this.listening.clear();
    for (const ch of this.channels.keys()) {
      try {
        await client.query(`LISTEN ${this.quoteIdent(ch)}`);
        this.listening.add(ch);
      } catch (e) {
        this.emit('error', e);
      }
    }
  }

  /**
   * Try to obtain the wired listener client without throwing.
   * Returns null during init (before first rebind) or after close.
   */
  private async tryWire(): Promise<PoolClient | null> {
    if (this.closed) {
      return null;
    }
    if (this.wiredClient) {
      return this.wiredClient;
    }
    const client = await this.getListenerClient().catch((): PoolClient | null => null);
    if (!client) {
      return null;
    }
    if (this.wiredClient !== client) {
      await this.rebindListenerClient();
    }
    return this.wiredClient;
  }

  async subscribe(channel: string, handler: NotificationHandler): Promise<void> {
    if (this.closed) {
      return;
    }
    let set = this.channels.get(channel);
    if (!set) {
      set = new Set();
      this.channels.set(channel, set);
    }
    const firstHandlerForChannel = set.size === 0;
    set.add(handler);
    if (!firstHandlerForChannel) {
      return;
    }
    const client = await this.tryWire();
    if (!client) {
      return;
    }
    if (this.listening.has(channel)) {
      return;
    }
    try {
      await client.query(`LISTEN ${this.quoteIdent(channel)}`);
      this.listening.add(channel);
    } catch (e) {
      this.emit('error', e);
    }
  }

  async unsubscribe(channel: string, handler: NotificationHandler): Promise<void> {
    const set = this.channels.get(channel);
    if (!set) {
      return;
    }
    set.delete(handler);
    if (set.size > 0) {
      return;
    }
    this.channels.delete(channel);
    this.listening.delete(channel);
    if (this.closed) {
      return;
    }
    const client =
      this.wiredClient ??
      (await this.getListenerClient().catch((): null => null));
    if (client) {
      await client.query(`UNLISTEN ${this.quoteIdent(channel)}`).catch(() => {});
    }
  }

  /**
   * Drop all subscriptions whose channel name matches any of the given prefixes
   * (e.g. after `obliterate` removes a queue row).
   */
  dropChannelsMatching(prefixes: string[]): void {
    if (prefixes.length === 0) {
      return;
    }
    for (const ch of Array.from(this.channels.keys())) {
      if (prefixes.some(p => ch.startsWith(p))) {
        this.channels.delete(ch);
        this.listening.delete(ch);
      }
    }
  }

  /**
   * Stop accepting subscriptions and clear state. Idempotent.
   * The underlying PoolClient is owned by {@link PgConnection}, not released here.
   */
  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    if (this.wiredClient) {
      this.wiredClient.removeListener('notification', this.onNotification);
      this.wiredClient = null;
    }
    this.channels.clear();
    this.listening.clear();
    this.removeAllListeners();
  }

  /** `emq_m_<md5(prefix:name)>` */
  subscribeToQueueMarker(
    qualifiedQueueName: string,
    handler: NotificationHandler,
  ): Promise<void> {
    return this.subscribe(channelForMarker(qualifiedQueueName), handler);
  }

  /** `emq_d_<md5(prefix:name)>` */
  subscribeToDelayed(
    qualifiedQueueName: string,
    handler: NotificationHandler,
  ): Promise<void> {
    return this.subscribe(channelForDelayed(qualifiedQueueName), handler);
  }

  /** `emq_e_<md5(prefix:name)>` */
  subscribeToEvents(
    qualifiedQueueName: string,
    handler: NotificationHandler,
  ): Promise<void> {
    return this.subscribe(channelForEvents(qualifiedQueueName), handler);
  }
}
