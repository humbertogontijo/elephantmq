# Architecture

elephantmq stores all queue state in PostgreSQL. There is no separate broker, no in-memory ledger, no sidecar. A producer writes a row, a worker reads (and updates) a row, and `LISTEN` / `NOTIFY` keeps idle workers responsive without polling.

This document explains the data model, how a job becomes runnable, how delays and retries work, and how parent/child flows are represented. It assumes basic PostgreSQL familiarity; nothing here requires Redis knowledge.

---

## 1. Data model

Everything lives under a single PostgreSQL schema (default `public`). The interesting tables:

- **`emq_queues`** — One row per logical queue (`prefix` + `name`), plus `paused`, optional concurrency cap, rate-limit columns, and a settings JSON blob.
- **`emq_jobs`** — The job row. Key columns:
  - `state` — one of `wait`, `paused`, `prioritized`, `delayed`, `active`, `waiting-children`, `completed`, `failed`.
  - `process_at` — when a `delayed` job is eligible to run (used for user-supplied delays, scheduler promotions, and retry backoff).
  - `wait_seq` / `prio_seq` — FIFO ordering inside `wait` and priority ordering inside `prioritized`.
  - `lock_token`, `locked_by`, `lock_expires_at` — the active worker's lease.
  - `attempts_made`, `max_attempts`, `backoff` (JSON) — retry policy state.
  - `parent_queue_id`, `parent_job_id`, `parent_fail_strategy` — flow links.
- **`emq_job_deps`** — Edges from parent to child with a `status` (`pending` → `processed` / `failed` / `ignored`) so a parent can wait for its children.
- **`emq_job_schedulers`** — Repeat-job templates with `next_millis`, limits, etc. Each cron / interval schedule is one row; firing a schedule produces a regular row in `emq_jobs`.
- **`emq_events`** — Append-only event log consumed by `QueueEvents`. Trimmed to a configurable upper bound on insert.
- **`emq_rate_limit_state`** — Counter + window for queue-level rate limiting.

Indexes on `emq_jobs` cover the hot paths: a partial index on `(queue_pk, process_at)` for delayed promotion, `(queue_pk, wait_seq)` for FIFO claiming, `(queue_pk, prio_seq)` for priority claiming, and `(queue_pk, locked_until)` for the stalled sweep.

---

## 2. How a job is created

`Queue.add` (and `addBulk`) call SQL functions like `emq_add_job_v1`. Depending on the options:

- **No delay, default priority** — inserts as `state = 'wait'` with a fresh `wait_seq`.
- **Priority** — inserts as `state = 'prioritized'` with `prio_seq` set.
- **Delay** — inserts as `state = 'delayed'` with `process_at = now + delay_ms`.
- **Inside a flow** — inserts the parent as `waiting-children` and links each child via `emq_job_deps`. See §6.

The same call writes to `emq_events` and fires `pg_notify` so listening workers wake up.

### Transactional enqueue

`Queue.inTransaction(async (q, sql) => { ... })` opens a transaction on a `PoolClient` and pins both your queries (the `sql` argument) and the queue's SQL calls (the `q` argument) to that connection. The job becomes visible to workers only when the transaction commits, and disappears if it rolls back. This is how elephantmq's enqueue is "exactly once with your data": there is no two-phase commit because there is no second system.

---

## 3. How a worker picks up a job

The worker loop calls `emq_move_to_active_v1`. That function does three things in one transaction:

1. **Promotes due delayed jobs.** Any rows with `state = 'delayed' AND process_at <= now_ms` move to `wait` (or `prioritized`, or `paused` if the queue is paused), with `delay_ms` cleared. A `waiting` event is emitted with `prev = 'delayed'`.
2. **Applies rate limit and concurrency.** Reads `emq_rate_limit_state` for the queue; counts existing `active` rows; bails out (returning a hint about when to wake again) if either limit is hit.
3. **Claims the next job.** Under a narrow `pg_advisory_lock` it picks one row in this order: oldest `wait` (FIFO by `wait_seq`), then highest-priority `prioritized` (by `prio_seq`). The chosen row flips to `active`, gets a fresh `lock_token` / `lock_expires_at`, and is returned to the caller.

The worker runs the user-supplied `Processor` against that row, then calls `emq_move_to_finished_v1` (success) or `emq_move_to_delayed_v1` / `emq_move_to_finished_v1` (failure / no-more-attempts).

### When the queue is empty

The worker does **not** poll in a hot loop. When there is no work to claim, it enters `waitForJob`, which:

- Computes the next interesting time — the earliest `process_at` of a delayed row, or a configured `drainDelay`, capped to a maximum.
- Sleeps until that time **or** until a `pg_notify` arrives, whichever comes first.

Two lightweight triggers on `emq_jobs` fire `pg_notify`:

- **`emq_m_<hash>`** — a row entered `wait` or `prioritized` (new work).
- **`emq_d_<hash>`** — a row's `process_at` changed or it became `delayed` (the next-run time may be earlier than the worker thought).

`NotificationManager` runs one dedicated `LISTEN` connection per worker process and routes wakeups to the workers that care. The timer-based fallback ensures that a missed or delayed `NOTIFY` cannot leave a worker stuck.

The hash is needed because PostgreSQL channel names are limited to 63 bytes, and `prefix.namespace.queueName` can easily be longer.

---

## 4. Retries and backoff

When a handler throws, `Job.moveToFailed` decides what to do:

- **No more attempts** — `emq_move_to_finished_v1` records `state = 'failed'`, persists `failed_reason` / `stacktrace`, and (if configured) removes the row per `removeOnFail`.
- **Should retry with delay** — `emq_move_to_delayed_v1` sets `state = 'delayed'`, sets `process_at = now + computed_backoff`, increments `attempts_made`, clears the lock, and emits a `delayed` event.

`backoff` strategies are computed in JS (`src/classes/backoffs.ts`): `'fixed'`, `'exponential'`, or a custom function. The result is a millisecond delay, which becomes `process_at`. The same delayed-promotion path that handles user-supplied delays handles retries — there is exactly one mechanism.

Manual retries (`Queue.retryJobs`, `Job.reprocess`) call dedicated SQL functions to move `failed` / `delayed` rows back into the runnable set.

---

## 5. Stalled detection

If a worker process dies between claiming and finishing a job, the row stays in `state = 'active'` with a `lock_expires_at` in the past. Each worker periodically (every `stalledInterval`, default 30s) calls `emq_move_stalled_jobs_to_wait_v1`, which:

- Finds active rows whose lease has expired.
- If `stalled_counter + 1` exceeds `maxStalledCount`, moves them directly to `failed` with a "job stalled more than allowable limit" reason.
- Otherwise moves them back to `wait` (or `paused` when the queue is paused) and increments the stalled counter.

Workers renew their lease via `emq_extend_locks_v1` on a timer at half the lock duration, so the only way a job stalls is if the worker process is genuinely gone.

---

## 6. Flows (parent and child)

`FlowProducer.add` walks the tree depth-first inside a single transaction:

1. The root parent is inserted via `emq_add_parent_job_v1` in `state = 'waiting-children'`.
2. For every child, `emq_link_child_to_parent_v1` inserts the child row and a corresponding edge in `emq_job_deps` with `status = 'pending'`.
3. The child rows themselves enter `wait` / `prioritized` / `delayed` exactly like any other job and get picked up normally.

When a child finishes, `emq_move_to_finished_v1` updates the matching `emq_job_deps.status`. If the parent has no more `pending` children, the same call promotes the parent from `waiting-children` into `wait` / `prioritized` / `delayed` (or fails it, depending on `parent_fail_strategy`).

`getChildrenValues()` reads from `emq_job_deps` joined to the child rows — there is no separate "child results" structure.

---

## 7. Job schedulers (cron / interval)

`JobScheduler.upsertJobScheduler(key, repeatOpts, template)` upserts a row in `emq_job_schedulers` and enqueues the *next* occurrence (typically as a delayed job).

When a worker takes a scheduler-produced job, it calls `JobScheduler.upsertJobScheduler` again to enqueue the *following* occurrence. So scheduling is driven from the worker side, not by a database cron — there is no separate scheduler process. If `upsertJobScheduler` fails, the current job still runs (the error is logged via the worker's `error` event); the next scheduled occurrence will be reattempted on the following tick.

---

## 8. Queue events

`emq_emit_event_v1` inserts into `emq_events` and fires `pg_notify('emq_e_<hash>', '<event_id>')`. `QueueEvents` keeps a watermark and:

- On notify, fetches rows past the watermark.
- On a timer (fallback), does the same fetch.

This gives at-most-one-delivery to each `QueueEvents` instance (you must drive your own idempotency for downstream effects) with low latency on the happy path.

---

## 9. Where to look in the repo

| Area | File |
| ---- | ---- |
| Public API entry | `src/classes/queue.ts`, `src/classes/worker.ts`, `src/classes/job.ts` |
| SQL invocation layer | `src/classes/scripts.ts` |
| Worker loop, wait + claim | `src/classes/worker.ts` |
| LISTEN / NOTIFY | `src/classes/notification-manager.ts`, `src/classes/pg-connection.ts` |
| Schema | `src/sql/migrations/0001_init.sql` |
| SQL functions | `src/sql/functions/` |
| Migration runner | `src/classes/migrate.ts`, exposed via `elephantmq/migrate` |

---

## 10. Trade-offs

- **Throughput**: a Postgres-backed queue caps out lower than an in-memory one (Redis, RabbitMQ). For most product workloads the bottleneck is the worker handler, not the queue, but if you are pushing well past 10k jobs/sec sustained, look at horizontal sharding (multiple queues / databases) before assuming elephantmq is the limit.
- **Latency**: enqueue → claim is dominated by network round-trips to Postgres plus `pg_notify` propagation. On a co-located DB this is typically a couple of milliseconds; over WAN it grows accordingly.
- **Storage**: every job is a row. Set `removeOnComplete` and `removeOnFail` appropriately; the default keeps the last 1000 of each.
- **Operational simplicity**: you back up the queue when you back up the database. You audit the queue with the same SQL tools you already have. You don't run a separate broker.
