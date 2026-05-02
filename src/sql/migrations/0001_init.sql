-- elephantmq schema bootstrap.
-- All DDL lives in this single, forward-only file. Functions live under
-- src/sql/functions/*.sql and are reapplied (CREATE OR REPLACE) on every
-- migrate() call, so changes there take effect without a new migration id.

create schema if not exists :EMQ_SCHEMA;

create table if not exists :EMQ_SCHEMA.emq_migrations (
  id int primary key,
  applied_at timestamptz not null default now()
);

create table if not exists :EMQ_SCHEMA.emq_queues (
  id bigserial primary key,
  prefix text not null default 'emq',
  name text not null,
  paused boolean not null default false,
  concurrency int,
  rate_limit_max int,
  rate_limit_duration_ms int,
  max_len_events int not null default 10000,
  settings jsonb not null default '{}'::jsonb,
  worker_seen boolean not null default false,
  job_added_seen boolean not null default false,
  obliterated_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (prefix, name)
);

create table if not exists :EMQ_SCHEMA.emq_queue_counters (
  queue_id bigint primary key references :EMQ_SCHEMA.emq_queues(id) on delete cascade,
  job_num bigint not null default 0,
  priority_num bigint not null default 0,
  wait_num bigint not null default 0,
  drained boolean not null default false
);

do $$ begin
  create type :EMQ_SCHEMA.emq_job_state as enum (
    'wait',
    'paused',
    'active',
    'delayed',
    'prioritized',
    'waiting-children',
    'completed',
    'failed'
  );
exception
  when duplicate_object then null;
end $$;

create table if not exists :EMQ_SCHEMA.emq_jobs (
  pk bigserial primary key,
  queue_id bigint not null references :EMQ_SCHEMA.emq_queues(id) on delete cascade,
  job_id text not null,
  name text not null,
  data jsonb not null default '{}'::jsonb,
  opts jsonb not null default '{}'::jsonb,
  state :EMQ_SCHEMA.emq_job_state,
  priority int not null default 0,
  prio_seq bigint,
  process_at timestamptz,
  wait_seq bigint,
  delay_ms bigint not null default 0,
  attempts_made int not null default 0,
  attempts_started int not null default 0,
  max_attempts int not null default 1,
  backoff jsonb,
  failed_reason text,
  stacktrace text[] not null default '{}',
  return_value jsonb,
  progress jsonb,
  timestamp timestamptz default now(),
  processed_on timestamptz,
  finished_on timestamptz,
  locked_by text,
  lock_token text,
  locked_at timestamptz,
  lock_expires_at timestamptz,
  stalled_counter int not null default 0,
  processed_by text,
  parent_pk bigint references :EMQ_SCHEMA.emq_jobs(pk) on delete set null,
  parent_queue_id bigint,
  parent_job_id text,
  parent_fail_strategy text,
  repeat_job_key text,
  next_repeatable_job_key text,
  deduplication_id text,
  deferred_failure text,
  unique (queue_id, job_id)
);

create index if not exists emq_jobs_wait_idx on :EMQ_SCHEMA.emq_jobs (queue_id, wait_seq) where state = 'wait';
create index if not exists emq_jobs_paused_idx on :EMQ_SCHEMA.emq_jobs (queue_id, wait_seq) where state = 'paused';
create index if not exists emq_jobs_delayed_idx on :EMQ_SCHEMA.emq_jobs (queue_id, process_at) where state = 'delayed';
create index if not exists emq_jobs_prio_idx on :EMQ_SCHEMA.emq_jobs (queue_id, priority, prio_seq) where state = 'prioritized';
create index if not exists emq_jobs_active_idx on :EMQ_SCHEMA.emq_jobs (queue_id, locked_at) where state = 'active';
create index if not exists emq_jobs_finished_idx on :EMQ_SCHEMA.emq_jobs (queue_id, finished_on desc) where state in ('completed','failed');
create index if not exists emq_jobs_wc_idx on :EMQ_SCHEMA.emq_jobs (queue_id) where state = 'waiting-children';

create unique index if not exists emq_jobs_dedup_uniq on :EMQ_SCHEMA.emq_jobs (queue_id, deduplication_id)
  where deduplication_id is not null
    and state not in (
      'completed':: :EMQ_SCHEMA.emq_job_state,
      'failed':: :EMQ_SCHEMA.emq_job_state
    );

create table if not exists :EMQ_SCHEMA.emq_job_logs (
  job_pk bigint not null references :EMQ_SCHEMA.emq_jobs(pk) on delete cascade,
  seq bigint not null,
  line text not null,
  created_at timestamptz not null default now(),
  primary key (job_pk, seq)
);

-- Dependency graph: parent waits until every dependency reaches a terminal
-- status. `child_pk` may be null after the child row is hard-removed via
-- `removeOnComplete` / `removeOnFail` / obliterate; in that case `child_ref`
-- (`<prefix>:<queue>:<job_id>`) is retained so callers can still surface
-- which child the dep referred to.
create table if not exists :EMQ_SCHEMA.emq_job_deps (
  id bigserial primary key,
  parent_pk bigint not null references :EMQ_SCHEMA.emq_jobs(pk) on delete cascade,
  child_pk bigint references :EMQ_SCHEMA.emq_jobs(pk) on delete set null,
  child_ref text,
  status text not null check (status in ('pending','processed','failed','ignored')) default 'pending',
  resolved_at timestamptz,
  failed_reason text,
  return_value jsonb
);
create unique index if not exists emq_job_deps_parent_child_uq
  on :EMQ_SCHEMA.emq_job_deps (parent_pk, child_pk)
  where child_pk is not null;
create index if not exists emq_job_deps_parent_idx on :EMQ_SCHEMA.emq_job_deps (parent_pk);
create index if not exists emq_job_deps_child_idx on :EMQ_SCHEMA.emq_job_deps (child_pk);

create table if not exists :EMQ_SCHEMA.emq_events (
  id bigserial primary key,
  queue_id bigint not null references :EMQ_SCHEMA.emq_queues(id) on delete cascade,
  event text not null,
  args jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);
create index if not exists emq_events_queue_idx on :EMQ_SCHEMA.emq_events (queue_id, id);

create table if not exists :EMQ_SCHEMA.emq_rate_limit_state (
  queue_id bigint primary key references :EMQ_SCHEMA.emq_queues(id) on delete cascade,
  tokens bigint not null default 0,
  expires_at timestamptz
);

create table if not exists :EMQ_SCHEMA.emq_deduplication (
  queue_id bigint not null references :EMQ_SCHEMA.emq_queues(id) on delete cascade,
  dedup_id text not null,
  job_id text not null,
  expires_at timestamptz,
  keep_last_if_active boolean not null default false,
  pending_name text,
  pending_data jsonb,
  pending_opts jsonb,
  primary key (queue_id, dedup_id)
);

create table if not exists :EMQ_SCHEMA.emq_job_schedulers (
  queue_id bigint not null references :EMQ_SCHEMA.emq_queues(id) on delete cascade,
  scheduler_id text not null,
  name text not null,
  data jsonb,
  opts jsonb,
  template jsonb,
  pattern text,
  every_ms bigint,
  offset_ms bigint,
  limit_count int,
  tz text,
  start_date timestamptz,
  end_date timestamptz,
  next_millis bigint,
  iteration_count int not null default 0,
  producer_id text,
  primary key (queue_id, scheduler_id)
);

create table if not exists :EMQ_SCHEMA.emq_metrics (
  queue_id bigint not null references :EMQ_SCHEMA.emq_queues(id) on delete cascade,
  metric text not null check (metric in ('completed','failed')),
  count bigint not null default 0,
  prev_ts bigint,
  prev_count bigint not null default 0,
  data jsonb not null default '[]'::jsonb,
  primary key (queue_id, metric)
);

-- pg_notify triggers used by Worker / QueueEvents to wake LISTEN clients.
-- NOTIFY channel names are limited to NAMEDATALEN-1 (63) bytes, so we hash
-- the qualified queue name and prefix a short tag:
--   emq_m_<md5>  wait/prioritized job markers
--   emq_d_<md5>  delayed-processing-time markers
--   emq_e_<md5>  queue event stream notifications
create or replace function :EMQ_SCHEMA.tg_emq_events_notify() returns trigger
language plpgsql as $$
declare v_qn text; v_ch text;
begin
  select prefix || ':' || name into v_qn from :EMQ_SCHEMA.emq_queues where id = NEW.queue_id;
  v_ch := 'emq_e_' || md5(v_qn);
  perform pg_notify(v_ch, v_qn || ':' || NEW.id::text);
  return NEW;
end;
$$;

drop trigger if exists emq_events_after_insert on :EMQ_SCHEMA.emq_events;
create trigger emq_events_after_insert
  after insert on :EMQ_SCHEMA.emq_events
  for each row execute procedure :EMQ_SCHEMA.tg_emq_events_notify();

create or replace function :EMQ_SCHEMA.tg_emq_jobs_wait_notify() returns trigger
language plpgsql as $$
declare v_qn text; v_ch text;
begin
  if NEW.state in ('wait','prioritized') and (TG_OP = 'INSERT' or OLD.state is distinct from NEW.state) then
    select prefix || ':' || name into v_qn from :EMQ_SCHEMA.emq_queues where id = NEW.queue_id;
    v_ch := 'emq_m_' || md5(v_qn);
    perform pg_notify(v_ch, coalesce(NEW.job_id,''));
  elsif NEW.state = 'delayed' and (
    TG_OP = 'INSERT' or OLD.state is distinct from NEW.state or OLD.process_at is distinct from NEW.process_at
  ) then
    select prefix || ':' || name into v_qn from :EMQ_SCHEMA.emq_queues where id = NEW.queue_id;
    v_ch := 'emq_d_' || md5(v_qn);
    perform pg_notify(v_ch, (extract(epoch from NEW.process_at) * 1000)::bigint::text);
  end if;
  return NEW;
end;
$$;

drop trigger if exists emq_jobs_after_change on :EMQ_SCHEMA.emq_jobs;
create trigger emq_jobs_after_change
  after insert or update of state, process_at on :EMQ_SCHEMA.emq_jobs
  for each row execute procedure :EMQ_SCHEMA.tg_emq_jobs_wait_notify();

-- Maintain `emq_queues.job_added_seen` / clear `obliterated_at` when a queue
-- receives its next job after an obliterate.
create or replace function :EMQ_SCHEMA.tg_emq_jobs_added_seen() returns trigger
language plpgsql as $$
begin
  update :EMQ_SCHEMA.emq_queues
     set job_added_seen = true,
         obliterated_at = null
   where id = NEW.queue_id
     and (job_added_seen = false or obliterated_at is not null);
  return NEW;
end;
$$;

drop trigger if exists emq_jobs_added_seen_insert on :EMQ_SCHEMA.emq_jobs;
drop trigger if exists emq_jobs_added_seen_update on :EMQ_SCHEMA.emq_jobs;
create trigger emq_jobs_added_seen_insert
  after insert on :EMQ_SCHEMA.emq_jobs
  for each row
  when (NEW.state in ('wait','paused','prioritized','delayed'))
  execute procedure :EMQ_SCHEMA.tg_emq_jobs_added_seen();
create trigger emq_jobs_added_seen_update
  after update of state on :EMQ_SCHEMA.emq_jobs
  for each row
  when (
    NEW.state in ('wait','paused','prioritized','delayed')
    and OLD.state is distinct from NEW.state
  )
  execute procedure :EMQ_SCHEMA.tg_emq_jobs_added_seen();
