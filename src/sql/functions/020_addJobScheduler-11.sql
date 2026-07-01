-- Mirrors ref/bullmq/src/commands/addJobScheduler-11.lua

drop function if exists :EMQ_SCHEMA.emq_add_job_scheduler_v1(bigint, text, bigint, text, jsonb, jsonb, jsonb, text);

create or replace function :EMQ_SCHEMA.emq_add_job_scheduler_v1(
  p_queue_id bigint,
  p_scheduler_id text,
  p_next_millis bigint,
  p_name text,
  p_data jsonb,
  p_opts jsonb,
  p_template jsonb,
  p_producer_id text,
  p_pattern text default null,
  p_every_ms bigint default null,
  p_offset_ms bigint default null,
  p_limit_count int default null,
  p_tz text default null,
  p_start_date bigint default null,
  p_end_date bigint default null,
  p_delayed_opts jsonb default null
) returns table (out_scheduler_id text, out_next_millis bigint, err_code int)
language plpgsql
as $fn$
declare
  v_prev_ic int;
  v_prev_offset bigint;
  v_prev_next bigint;
  v_prev_job_id text;
  v_new_job_id text;
  v_next_slot_millis bigint;
  v_next_slot_job_id text;
  v_effective_next bigint;
  v_removed_prev_job boolean := false;
begin
  -- Mirrors BullMQ's `storeJobScheduler` include (addJobScheduler-11.lua):
  -- persist all scheduler attributes, carry `iteration_count` across upserts
  -- (defaulting to 1 on first write), and preserve the previous `offset_ms`
  -- when the caller did not supply a new one (every-based reschedules read
  -- the stored offset from previous attributes).
  v_effective_next := p_next_millis;

  select iteration_count, offset_ms, next_millis
    into v_prev_ic, v_prev_offset, v_prev_next
    from :EMQ_SCHEMA.emq_job_schedulers
    where queue_id = p_queue_id and scheduler_id = p_scheduler_id;

  -- BullMQ's addJobScheduler Lua calls `removeJobFromScheduler` for the
  -- previous iteration (delayed/prioritized/wait/paused states) before
  -- writing the new one. Track whether we actually removed such a row:
  -- BullMQ uses this flag (`removedPrevJob`) to decide whether to bypass
  -- the fatal collision branch below. If we did remove the previous
  -- delayed-job row, we proceed with the upsert even when the target
  -- `nextMillis` still has a job (e.g. still-active iteration) at the
  -- same job id; the insert/on-conflict-do-update will overwrite the
  -- hash equivalent (data/opts/template) for the next materialisation.
  if v_prev_next is not null then
    v_prev_job_id := 'repeat:' || p_scheduler_id || ':' || v_prev_next::text;
    with removed as (
      delete from :EMQ_SCHEMA.emq_jobs
      where queue_id = p_queue_id
        and job_id = v_prev_job_id
        and state in ('delayed':: :EMQ_SCHEMA.emq_job_state,
                      'prioritized':: :EMQ_SCHEMA.emq_job_state,
                      'wait':: :EMQ_SCHEMA.emq_job_state,
                      'paused':: :EMQ_SCHEMA.emq_job_state)
      returning 1
    )
    select exists (select 1 from removed) into v_removed_prev_job;
  end if;

  -- BullMQ's `addJobScheduler-11.lua` collision branch: if a job for the
  -- target iteration slot already exists in a non-removable state (active,
  -- completed, failed, ...) we either advance one slot (every mode, error
  -- -11 `SchedulerJobSlotsBusy` when both collide) or bail out (pattern
  -- mode, error -10 `SchedulerJobIdCollision`). However, when we already
  -- removed the previous iteration's delayed-job row above
  -- (`removedPrevJob = true` in Lua), BullMQ bypasses the fatal branch
  -- via `if not hasCollision or removedPrevJob then ... elseif hasCollision
  -- then return -10 end` and proceeds with the upsert. This is what lets
  -- a processor call `queue.upsertJobScheduler` while its own iteration is
  -- still `active`: the worker's pre-advance (via `nextJobFromJobData ->
  -- updateJobSchedulerNextMillis`) seeds a fresh delayed row at
  -- `prev_next`; `addJobScheduler` then removes that row, detects the
  -- "collision" on the active slot's id, and still upserts over the
  -- existing hash (BullMQ's `HMSET jobIdKey` equivalent).
  v_new_job_id := 'repeat:' || p_scheduler_id || ':' || v_effective_next::text;
  if exists (
    select 1 from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_queue_id and job_id = v_new_job_id
  ) then
    if p_every_ms is not null then
      v_next_slot_millis := v_effective_next + p_every_ms;
      v_next_slot_job_id := 'repeat:' || p_scheduler_id || ':' || v_next_slot_millis::text;
      if exists (
        select 1 from :EMQ_SCHEMA.emq_jobs
        where queue_id = p_queue_id and job_id = v_next_slot_job_id
      ) then
        return query select null::text, null::bigint, -11;
        return;
      end if;
      v_effective_next := v_next_slot_millis;
    elsif not v_removed_prev_job then
      return query select null::text, null::bigint, -10;
      return;
    end if;
  end if;

  insert into :EMQ_SCHEMA.emq_job_schedulers (
    queue_id, scheduler_id, name, next_millis, data, opts, template, producer_id,
    pattern, every_ms, offset_ms, limit_count, tz,
    start_date, end_date, iteration_count
  )
  values (
    p_queue_id, p_scheduler_id, p_name, v_effective_next, p_data, p_opts, p_template, p_producer_id,
    p_pattern, p_every_ms, coalesce(p_offset_ms, v_prev_offset), p_limit_count, p_tz,
    case when p_start_date is not null then to_timestamp(p_start_date / 1000.0) else null end,
    case when p_end_date is not null then to_timestamp(p_end_date / 1000.0) else null end,
    coalesce(v_prev_ic, 1)
  )
  on conflict (queue_id, scheduler_id) do update set
    name = excluded.name,
    next_millis = excluded.next_millis,
    data = excluded.data,
    opts = excluded.opts,
    template = excluded.template,
    producer_id = excluded.producer_id,
    pattern = excluded.pattern,
    every_ms = excluded.every_ms,
    offset_ms = coalesce(excluded.offset_ms, :EMQ_SCHEMA.emq_job_schedulers.offset_ms),
    limit_count = excluded.limit_count,
    tz = excluded.tz,
    start_date = excluded.start_date,
    end_date = excluded.end_date,
    iteration_count = coalesce(:EMQ_SCHEMA.emq_job_schedulers.iteration_count, 1);

  -- Materialise the first delayed iteration in the same transaction as the
  -- scheduler upsert (mirrors addJobScheduler-11.lua).
  v_new_job_id := 'repeat:' || p_scheduler_id || ':' || v_effective_next::text;
  perform :EMQ_SCHEMA.emq_add_delayed_job_v1(
    p_queue_id,
    v_new_job_id,
    p_name,
    coalesce(p_data, '{}'::jsonb),
    coalesce(p_delayed_opts, p_opts, '{}'::jsonb),
    (extract(epoch from now()) * 1000)::bigint,
    null,
    null,
    null,
    p_scheduler_id,
    null
  );

  return query select p_scheduler_id, v_effective_next, 0;
end;
$fn$;
