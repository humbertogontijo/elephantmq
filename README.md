# elephantmq

[![npm version](https://img.shields.io/npm/v/elephantmq.svg)](https://www.npmjs.com/package/elephantmq)
[![CI](https://github.com/humbertogontijo/elephantmq/actions/workflows/ci.yml/badge.svg)](https://github.com/humbertogontijo/elephantmq/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Node](https://img.shields.io/node/v/elephantmq.svg)](https://nodejs.org/)

A **PostgreSQL-native job queue** for Node.js. Producers and workers talk to the same Postgres you already run; there is no separate broker, no in-memory state, and no second datastore to back up.

```ts
import { Queue, Worker } from 'elephantmq';
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const queue = new Queue('emails', { connection: pool });

await queue.add('welcome', { userId: 42 });

new Worker('emails', async job => {
  await sendEmail(job.data.userId);
}, { connection: pool });
```

## Why elephantmq

- **Transactional enqueue.** `await queue.inTransaction(async (q, sql) => { ... })` runs your block on one pinned connection so business writes and `q.add(...)` commit atomically — or roll back together. No outbox table, no two-phase commit.
- **One datastore.** Backups, PITR, replication, monitoring, IAM — whatever you already do for Postgres now covers your queue.
- **Real SQL visibility.** Want to know how many emails are stuck retrying? `SELECT count(*) FROM emq_jobs WHERE state = 'delayed' AND queue_pk = ...`. No Redis-shaped abstractions in the way.
- **Familiar API.** The producer/worker shape mirrors BullMQ on purpose, so existing patterns and tutorials still apply. If you're coming from BullMQ, see [docs/MIGRATING_FROM_BULLMQ.md](./docs/MIGRATING_FROM_BULLMQ.md).

## Install

```bash
npm install elephantmq pg
```

elephantmq targets:

- **Node.js 18+**
- **PostgreSQL 14+**

## Quick start

### 1. Run migrations once

```ts
import { migrate } from 'elephantmq/migrate';
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool, 'public');     // schema name
await pool.end();
```

The migration creates a small set of `emq_*` tables and stored functions in the schema you choose. By default each `Queue`/`Worker` will also lazily run `migrate()` on first connect; in production set `skipMigrations: true` and run the snippet above from a deploy job. See [docs/OPERATIONS.md](./docs/OPERATIONS.md#schema-and-migrations) for the recommended setup.

### 2. Enqueue jobs

```ts
import { Queue } from 'elephantmq';
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const queue = new Queue('orders', { connection: pool });

// fire-and-forget
await queue.add('place', { orderId: 'ord_123' });

// delayed
await queue.add('reminder', { orderId: 'ord_123' }, { delay: 60_000 });

// priority (lower number = sooner)
await queue.add('rush', { orderId: 'ord_124' }, { priority: 1 });

// bulk, atomically
await queue.addBulk([
  { name: 'place', data: { orderId: 'ord_125' } },
  { name: 'place', data: { orderId: 'ord_126' } },
]);
```

### 3. Process jobs

```ts
import { Worker } from 'elephantmq';

const worker = new Worker('orders', async job => {
  await placeOrder(job.data.orderId);
  return { ok: true };
}, {
  connection: pool,
  concurrency: 8,
  limiter: { max: 100, duration: 1000 },   // 100 jobs/sec ceiling
});

worker.on('failed', (job, err) => {
  console.error('order failed', job?.id, err);
});
```

### 4. Transactional enqueue

The headline feature. `Queue.inTransaction` opens a transaction on a pinned `PoolClient`, hands it to your block, and commits when the block resolves (or rolls back if it throws):

```ts
await queue.inTransaction(async (q, sql) => {
  await sql.query(
    'UPDATE inventory SET reserved = reserved + 1 WHERE sku = $1',
    [sku],
  );

  await q.add('ship', { sku });
});
```

If the block throws, the inventory update *and* the job row are rolled back together. Workers never see a "ship" job for an order that didn't actually get reserved.

## API surface

The default entry point exposes the producer/worker API:

| Class | Purpose |
| ----- | ------- |
| `Queue` | Enqueue jobs, manage a queue (pause, drain, obliterate). |
| `Worker` | Pull and process jobs with concurrency and rate limiting. |
| `Job` | Job row read/update API. |
| `JobScheduler` | Cron / interval schedules. |
| `FlowProducer` | Parent / child job graphs. |
| `QueueEvents` | Subscribe to lifecycle events (`waiting`, `active`, `completed`, `failed`, ...). |
| `QueueEventsProducer` | Emit custom events on a queue. |

Schema management lives behind a separate path (`elephantmq/migrate`) so apps that only enqueue jobs don't pull migration code into their bundle.

Re-exported error classes (`DelayedError`, `RateLimitError`, `WaitingError`, `WaitingChildrenError`, `UnrecoverableError`) let processors signal control flow back to the worker.

## Operations

- **Pool sizing**, `LISTEN`/`NOTIFY`, monitoring, and failure modes: see [docs/OPERATIONS.md](./docs/OPERATIONS.md).
- **How it works under the hood** (data model, claim path, retries, flows): see [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md).
- **Coming from BullMQ?** [docs/MIGRATING_FROM_BULLMQ.md](./docs/MIGRATING_FROM_BULLMQ.md).

## Trade-offs

| | |
| - | - |
| **Throughput** | Lower than Redis-backed queues for raw enqueue/claim. Most product workloads are bottlenecked on the handler, not the queue, so this rarely matters in practice. If you're sustainably pushing very high job rates, measure on your own hardware and Postgres tuning. |
| **Latency** | Enqueue → claim is dominated by Postgres round-trip + `pg_notify`. Co-located DB: a couple of ms. WAN: more. |
| **Storage** | Every job is a row. Set `removeOnComplete` / `removeOnFail` to keep the table bounded. |
| **Operational simplicity** | One database to back up, restore, replicate, and monitor. No separate broker. |

## Contributing

Bug reports, feature requests, and PRs are very welcome. Start with [CONTRIBUTING.md](./CONTRIBUTING.md). For security issues, follow [SECURITY.md](./SECURITY.md) instead of opening a public issue.

## License

[MIT](./LICENSE)
