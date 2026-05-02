# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking**: telemetry attribute keys renamed from `bullmq.*` to
  `elephantmq.*` (e.g. `bullmq.job.id` → `elephantmq.job.id`); `JobStatus`
  attribute renamed to `JobState` (`elephantmq.job.state`). Update any custom
  spans/dashboards that filter on these keys.
- **Breaking**: `QueueBaseOptions.skipWaitingForReady` renamed to
  `skipMigrations`. The behaviour (skip the implicit `migrate()` on first
  connect) is unchanged.
- **Breaking**: `Worker.resume()` is now `async`. Awaiting it ensures the
  stalled-check timer is re-armed before the call resolves.
- **Breaking**: `Telemetry.Meter.createGauge` is now required. Custom
  telemetry adapters must implement it.
- Minimum supported PostgreSQL bumped from 12 to **14** in the runtime
  version check (already required by the SQL function set; the gate now
  matches the docs).
- `Scripts` implementation reorganised into focused modules under
  `src/classes/scripts/` (no public API change).

### Removed

- **Breaking**: deprecated `RepeatableJob` type. Use `JobSchedulerJson`.
- Unused dependencies `msgpackr` and `node-abort-controller`. The latter
  was a polyfill for Node < 15.4 and is no longer needed under the Node 18+
  engines requirement.
- Maintainer-only tooling: BullMQ vs Postgres benchmark (`npm run bench`),
  SQL hot-path profiler (`npm run profile:sql`), `npm run test:fast`, and the
  Postgres connection-budget probe that set `ELEPHANTMQ_TEST_MAX_FORKS` before
  Vitest. Tune parallel workers with `ELEPHANTMQ_TEST_MAX_FORKS` and
  `ELEPHANTMQ_TEST_PARALLEL` instead.

## [1.0.0] - 2026-05-02

First public release.

### Added

- Postgres-native producer/worker API: `Queue`, `Worker`, `Job`, `JobScheduler`,
  `FlowProducer`, `QueueEvents`, `QueueEventsProducer`.
- Atomic enqueue and state transitions implemented as PostgreSQL functions
  (`emq_*_v1`).
- Transactional `Queue.add()` / `Queue.addBulk()` so jobs and your own writes
  commit (or roll back) together.
- `JobScheduler` for cron and every-N-millis schedules, with deduplication on
  the scheduler key.
- `pg_notify` / `LISTEN` / `NOTIFY` based wakeup for workers and queue events.
- Single-file initial migration (`0001_init.sql`) plus self-applying SQL
  function bundle on every `migrate()` call.
- Separate package entry point for migrations: `import { migrate } from
  'elephantmq/migrate'`.

### Changed

- Public surface narrowed to the producer/consumer API; previously exported
  internal modules (`Scripts`, `MainBase`, mappers, etc.) are no longer part of
  the published API.
- Default queue prefix changed from `bull` to `emq`.

### Removed

- The legacy `Repeat` class. `JobScheduler` is the canonical scheduler API.
- Redis/BullMQ compatibility shim that re-implemented Redis commands on top of
  `pg.Pool`.
- Redis-flavoured terminology (`redisVersion`, `databaseType`, `ioredis:close`)
  from the public API.

[Unreleased]: https://github.com/humbertogontijo/elephantmq/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/humbertogontijo/elephantmq/releases/tag/v1.0.0
