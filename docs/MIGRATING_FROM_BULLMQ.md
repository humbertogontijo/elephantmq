# Migrating from BullMQ

elephantmq's API was modelled on [BullMQ](https://github.com/taskforcesh/bullmq) so that teams already running BullMQ on Redis can move to PostgreSQL without rewriting the producer/worker layer. This document lists the differences you'll actually encounter when porting code.

If you're new to elephantmq and just want to know how it works, read [ARCHITECTURE.md](./ARCHITECTURE.md) instead.

## Connection

| BullMQ | elephantmq |
| ------ | ---------- |
| `connection: new IORedis(...)` or `{ host, port }` | `connection: new Pool(...)` or `{ connectionString }` (`pg.PoolConfig`) |
| Redis Sentinel / Cluster | Not applicable. Use PostgreSQL replication / failover. |
| `skipVersionCheck: true` skips Redis version probe | Same option name; enforces **PostgreSQL ≥ 14** instead of Redis ≥ 5. |

```ts
// before
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
const queue = new Queue('emails', { connection: new IORedis() });

// after
import { Queue } from 'elephantmq';
import { Pool } from 'pg';
const queue = new Queue('emails', { connection: new Pool() });
```

## Class names that stayed

`Queue`, `Worker`, `Job`, `JobScheduler`, `FlowProducer`, `QueueEvents`, `QueueEventsProducer`, plus the option types (`QueueOptions`, `WorkerOptions`, `JobsOptions`, etc.) all kept the same name and roughly the same shape. Most BullMQ tutorials work with elephantmq if you just swap the import and pass a `pg.Pool`.

## Class names that changed

| BullMQ | elephantmq | Why |
| ------ | ---------- | --- |
| `redisVersion` (getter) | `postgresVersion` | We're on Postgres. |
| `databaseType` (getter) | *(removed)* | Not meaningful when there is only one storage backend. |
| `'ioredis:close'` event | `'connection:close'` | Same idea, neutral name. |
| `Repeat` (legacy repeatable jobs) | *(removed)* | Use `JobScheduler`, which BullMQ also recommends as the canonical API. |

## What's not supported

- **Redis-only features**: `bzpopmin`, `XREAD`/`XADD`, Lua scripts, `CLIENT LIST`, `SCAN`-based maintenance — none of these exist in elephantmq because they don't exist in PostgreSQL. The functionality they provide in BullMQ is implemented differently (see [ARCHITECTURE.md](./ARCHITECTURE.md)).
- **`SandboxedJob` IPC channels backed by Redis pub/sub**: child processors must receive a `connectionString` (and optional `schema`) via job data and create their own `pg.Pool`. A parent-process `pg.Pool` cannot cross the fork / worker-thread boundary.
- **`extendJobLocks` as a public method on `Worker`**: it's now package-internal and is invoked by the lock manager directly. If you were calling it manually, you probably don't need to.
- **The compatibility shim** that exposed `hgetall`, `zrange`, `keys`, etc. on the pool. Inspect queue state with regular SQL instead — every queue object is a row.

## Behaviour you should re-check

- **Queue prefix default** is now `'emq'` (was `'bull'`). Set `prefix: 'bull'` on `Queue` / `Worker` to keep the old key shape if you're staging side-by-side, or migrate the prefix when you cut over.
- **Queue events delivery** uses `LISTEN` / `NOTIFY` plus an event-id watermark, not Redis streams. `QueueEventsProducer.publishEvent` returns the `emq_events.id` cast to string, which serves the same purpose as the BullMQ stream id.
- **Stalled detection and lock extension** match BullMQ's contract on the API surface, but the underlying mechanism is `lock_expires_at` columns and a `pg_advisory_lock`-protected sweep instead of Redis hash TTLs.
- **Rate limiting** is enforced at *claim* time inside `emq_move_to_active_v1`. The worker is told `rate_limit_delay_ms` and parks for that long. End-to-end behaviour is the same; the place where the limit is checked is different.
- **`removeOrphanedJobs`** returns the number of rows actually removed, not the size of the input list.

## Patterns that get *better*

- **Transactional enqueue.** `await queue.add('email', data, { inTransaction: client })` enqueues the job on your own `pg.Client`. Commit your business write and the job atomically. There is no equivalent in Redis-backed BullMQ.
- **SQL introspection.** `SELECT state, count(*) FROM emq_jobs WHERE queue_pk = ... GROUP BY state` is your monitoring dashboard. No `KEYS bull:emails:*` slow scans.
- **Backups.** Your queue is part of `pg_dump`. PITR works. Replication works.

## What if I want both running side-by-side?

Use different connections and different prefixes. There is no shared state, so a BullMQ instance and an elephantmq instance can coexist indefinitely, and you can drain one queue before retiring it.
