# Operations

This document covers running elephantmq in production: connection tuning, schema migrations, monitoring, and failure modes.

## Connection pool

- Pass a `pg.Pool` (or a `PoolConfig` / connection string) as `connection` on `Queue`, `Worker`, `QueueEvents`, and `FlowProducer`.
- **Pool size**: size the pool to your concurrent workers plus API traffic. Each `Worker` instance holds at least one connection for the `LISTEN` channel plus one per concurrent job for processing. As a rule of thumb, start with `2 + concurrency` connections per worker process.
- **Sharing**: pass `shared: true` on Worker / QueueEvents options to reuse an externally managed `pg.Pool`. Otherwise elephantmq calls `pool.end()` when the instance is closed.
- **Timeouts**: tune `idleTimeoutMillis`, `connectionTimeoutMillis`, and server-side `idle_in_transaction_session_timeout` to match your deployment.
- **Reconnection**: the internal listener reconnects on errors with decorrelated jitter backoff. Treat transient errors at call sites as retriable where appropriate.

## Schema and migrations

elephantmq stores everything in regular PostgreSQL tables under a single schema you choose (default `public`).

By default, every `Queue`, `Worker`, and `FlowProducer` runs `migrate()` on first connect (guarded by a per-schema advisory lock). This is convenient for local development and tests but is **not** what you want at production scale: every process pays the migration cost on startup, and the connecting role needs DDL permission. Set `skipMigrations: true` on each instance and run `migrate()` once from a deploy job, init container, or one-shot script:

```ts
import { migrate } from 'elephantmq/migrate';
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool, 'public');
await pool.end();
```

For the bundled test/dev script, run `npm run migrate:test` against `ELEPHANTMQ_TEST_PG_URL`.

- **Schemas as namespaces**: pass a different schema name to `migrate()` and the matching `prefix` (queue prefix) on the client side to isolate environments or tenants without separate databases.
- **Forward-only**: schema is applied via `src/sql/migrations/0001_init.sql`. SQL functions in `src/sql/functions/` are reapplied unconditionally on every `migrate()` call, so upgrading just means redeploying and rerunning `migrate()`.
- **Upgrades**: apply new SQL migrations during deploy; workers tolerate brief gaps in function availability while migration is in flight, but you should redeploy workers afterward.
- **Restricted database roles**: if your application connects with a role that lacks `CREATE` privileges, you must use `skipMigrations: true` and have a separate, more privileged role run `migrate()` ahead of time.

## LISTEN / NOTIFY

Event delivery uses `LISTEN` on channels prefixed with the queue's `prefix.namespace.queueName`:

- `emq_events:*` — job lifecycle events for `QueueEvents` consumers.
- `emq_marker:*` — wakeups for waiting workers.
- `emq_delayed:*` — promotions out of the delayed set.

Each `Worker` and `QueueEvents` instance holds one PostgreSQL connection in `LISTEN` mode. If you run many workers, size `max_connections` (and any pgbouncer in front) accordingly. Note that pgbouncer's **transaction pooling** mode does not support session-level `LISTEN`/`NOTIFY`; use **session pooling** (or a direct connection) for the listener path.

`NOTIFY` payloads are small (qualified name + event id) — large job data lives in regular tables, never in notifications.

## Monitoring

- **Backlog**: `SELECT state, count(*) FROM emq_jobs WHERE queue_pk = ... GROUP BY state;`
- **Rate-limit state**: `SELECT * FROM emq_rate_limit_state WHERE queue_pk = ...;`
- **Active jobs**: `SELECT id, locked_until, lock_token FROM emq_jobs WHERE state = 'active';` — `locked_until` advancing means the worker is renewing the lease.
- **Stalled detection**: workers run a periodic sweep (`stalledInterval` option, default 30s) that requeues jobs whose `locked_until` has passed without renewal.
- **Event log size**: `emq_events` accumulates with activity; `events.maxLen` (Worker / Queue option, default `10000`) trims it on a best-effort basis at insertion time.

## Backups and replication

- Use your standard PostgreSQL backup and PITR procedures — elephantmq has no out-of-band state. Backups are **fully consistent** with the rest of your application's data, which is one of the main reasons to use elephantmq.
- **Logical replication** works for read replicas. Writers must still target the primary; do not point a producer or worker at a replica.

## Failure modes

- **Database unavailable**: producers and workers surface connection errors. The listener reconnects automatically; in-flight job handlers should be idempotent where possible because we cannot guarantee that an `ack` write made it to disk before the connection dropped.
- **Long handlers**: keep business logic outside any transaction you opened in `Queue.add({ inTransaction: ... })`. Holding a transaction across slow work blocks vacuum and replication.
- **Lock loss**: if a worker pauses long enough for `lockDuration` to elapse without renewal, the stalled checker on another worker will pick the job up. Make handlers idempotent or use deduplication keys.

## Required PostgreSQL version

elephantmq requires **PostgreSQL 14 or newer**. We rely on `hashtextextended()` (PG 14+) for advisory-lock keying and on partial / unique indexes that have been stable since PG 11.
