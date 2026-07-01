-- Mirrors ref/bullmq/src/commands/moveToActive-11.lua

drop function if exists :EMQ_SCHEMA.emq_move_to_active_v1(bigint, bigint, text, bigint, text);
drop function if exists :EMQ_SCHEMA.emq_move_to_active_v1(bigint, bigint, text, bigint, text, boolean);
drop function if exists :EMQ_SCHEMA.emq_move_to_active_v1(bigint, bigint, text, bigint, text, boolean, bigint, bigint);

create or replace function :EMQ_SCHEMA.emq_move_to_active_v1(
  p_queue_id bigint,
  p_now_ms bigint,
  p_token text,
  p_lock_ms bigint,
  p_worker_name text default null,
  p_emit_drained boolean default true,
  p_limiter_max bigint default null,
  p_limiter_duration_ms bigint default null
) returns table (
  out_job_row jsonb,
  out_job_id text,
  rate_limit_delay_ms int,
  block_until_ms bigint
)
language plpgsql
as $fn$
declare
  v_paused boolean;
  v_concurrency int;
  v_active int;
  v_pick :EMQ_SCHEMA.emq_jobs;
  v_next_delayed double precision;
  v_delay_ms int;
  v_json jsonb;
  v_tok bigint;
  v_exp timestamptz;
  v_rl_max bigint;
  v_rl_dur bigint;
  -- Align with job timestamps from addJob (Node Date.now): process_at uses client ms; promotion must not use PG now() alone or Docker/host clock skew breaks delay/replace semantics.
  v_now timestamptz;
begin
  v_now := to_timestamp(p_now_ms::double precision / 1000.0);

  if not exists (select 1 from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id) then
    return query select null::jsonb, null::text, 0, 0::bigint;
    return;
  end if;

  select q.paused, q.concurrency, q.rate_limit_max, q.rate_limit_duration_ms
  into v_paused, v_concurrency, v_rl_max, v_rl_dur
  from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id;

  -- BullMQ's WorkerOptions.limiter is applied per worker (not stored in the
  -- queue meta). When passed, it overrides the queue's limiter config for this
  -- moveToActive call so the dynamic-limiter worker path still rate-limits.
  if p_limiter_max is not null and p_limiter_duration_ms is not null then
    v_rl_max := p_limiter_max;
    v_rl_dur := p_limiter_duration_ms;
  end if;

  -- Expired rate-limit window: Redis deletes the key; we drop the row.
  -- Safe to do outside the advisory lock; concurrent deletes of the same
  -- expired row are idempotent.
  delete from :EMQ_SCHEMA.emq_rate_limit_state r
  where r.queue_id = p_queue_id
    and r.expires_at is not null
    and r.expires_at <= now();

  -- Transaction-scoped advisory lock around delayed promotion, the
  -- rate-limit gate, pause/concurrency gate, wait/prioritized pick,
  -- activate, AND limiter INCR. BullMQ's Lua runs single-threaded inside
  -- Redis so the read-modify-write of the limiter token is atomic. On PG
  -- we MUST hold this lock across both the read of `v_tok` and the INCR;
  -- otherwise N concurrent workers on a fresh queue all observe
  -- `v_tok IS NULL`, all sneak past the gate, and the limiter
  -- overshoots (regression seen as
  -- `Rate Limiter > when queue is paused between rate limit > should
  -- add active jobs to paused` finishing all 4 jobs before pause lands).
  --
  -- Delayed promotion MUST run inside this lock so concurrent workers
  -- cannot double-promote the same delayed rows or duplicate `waiting`
  -- events.
  perform pg_advisory_xact_lock(
    hashtextextended(:EMQ_SCHEMA_NAME_LIT, 2024000001::bigint) # p_queue_id
  );

  -- Match BullMQ v5 `promoteDelayedJobs`: when the queue is paused, ready
  -- delayed jobs promote into the `paused` list rather than `wait`.
  declare
    v_promoted_id text;
  begin
    declare
      v_pri int;
      v_new_prio_seq bigint;
    begin
      for v_promoted_id, v_pri in
        select j.job_id, coalesce(j.priority, 0)
        from :EMQ_SCHEMA.emq_jobs j
        where j.queue_id = p_queue_id
          and j.state = 'delayed'
          and j.process_at <= v_now
        order by j.process_at asc, j.priority asc, j.pk asc
      loop
        if v_pri > 0 then
          update :EMQ_SCHEMA.emq_queue_counters
          set priority_num = priority_num + 1
          where queue_id = p_queue_id
          returning priority_num into v_new_prio_seq;
          if v_new_prio_seq is null then
            insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, priority_num)
            values (p_queue_id, 1)
            on conflict (queue_id) do update
              set priority_num = :EMQ_SCHEMA.emq_queue_counters.priority_num + 1
            returning priority_num into v_new_prio_seq;
          end if;

          update :EMQ_SCHEMA.emq_jobs j
          set state = case when coalesce(v_paused, false)
                           then 'paused':: :EMQ_SCHEMA.emq_job_state
                           else 'prioritized':: :EMQ_SCHEMA.emq_job_state
                      end,
              prio_seq = v_new_prio_seq,
              delay_ms = 0
          where j.queue_id = p_queue_id and j.job_id = v_promoted_id;
        else
          update :EMQ_SCHEMA.emq_jobs j
          set state = case when coalesce(v_paused, false)
                           then 'paused':: :EMQ_SCHEMA.emq_job_state
                           else 'wait':: :EMQ_SCHEMA.emq_job_state
                      end,
              wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id),
              delay_ms = 0
          where j.queue_id = p_queue_id and j.job_id = v_promoted_id;
        end if;
        perform :EMQ_SCHEMA.emq_emit_event_v1(
          p_queue_id,
          'waiting',
          jsonb_build_object('jobId', v_promoted_id, 'prev', 'delayed')
        );
      end loop;
    end;
  end;

  select rls.tokens, rls.expires_at into v_tok, v_exp
  from :EMQ_SCHEMA.emq_rate_limit_state rls
  where rls.queue_id = p_queue_id;

  -- Same order as moveToActive-11.lua: getRateLimitTTL before paused / maxed.
  if v_rl_max is not null
     and v_exp is not null
     and v_exp > now()
     and coalesce(v_tok, 0) >= v_rl_max then
    v_delay_ms := greatest(0, ceil(extract(epoch from (v_exp - now())) * 1000)::int);
    -- When we're rate-limited but a delayed job will become ready before
    -- the window expires, cap `rate_limit_delay_ms` at that time so the
    -- worker wakes up, re-enters this function, and promotes the delayed
    -- row instead of sleeping out the full limiter window. Mirrors
    -- BullMQ's `getRateLimitTtl` clamp in moveToActive-11.lua (which
    -- factors in `delayed` zset head).
    select min(extract(epoch from process_at) * 1000) into v_next_delayed
    from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_queue_id and state = 'delayed';
    if v_next_delayed is not null then
      v_delay_ms := least(
        v_delay_ms,
        greatest(0, ceil(v_next_delayed - p_now_ms::double precision)::int)
      );
    end if;
    return query select null::jsonb, null::text, v_delay_ms, 0::bigint;
    return;
  end if;

  if coalesce(v_paused, false) then
    select min(extract(epoch from process_at) * 1000) into v_next_delayed
    from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_queue_id and state = 'delayed';
    return query select null::jsonb, null::text, 0, coalesce(ceil(v_next_delayed)::bigint, 0);
    return;
  end if;

  if v_concurrency is not null then
    -- Only count jobs with a valid (non-expired) lock. Expired actives are
    -- invisible to concurrency until the stalled sweep moves them back.
    select count(*)::int into v_active
    from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_queue_id
      and state = 'active'
      and lock_expires_at is not null
      and lock_expires_at > now();
    if v_active >= v_concurrency then
      return query select null::jsonb, null::text, 0, 0::bigint;
      return;
    end if;
  end if;

  -- Match BullMQ v5 (moveToActive-11.lua): drain the `wait` list (FIFO by
  -- wait_seq) before considering the prioritized zset. Tests depend on this
  -- ordering (e.g. `.getJobCounts` with a late-added priority:5 job).
  select * into v_pick
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and state = 'wait'
  order by wait_seq asc nulls last
  limit 1
  for update skip locked;

  if not found then
    select * into v_pick
    from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_queue_id and state = 'prioritized'
    order by priority asc, prio_seq asc nulls last
    limit 1
    for update skip locked;
  end if;

  if not found then
    if not exists (
      select 1 from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id
        -- Include `active`: BullMQ v5 only emits `drained` once the queue is
        -- truly idle. With concurrency > 1, multiple workers complete back to
        -- back and each one calls moveToActive; without the active check the
        -- latch would flip on the first empty pick while others are still
        -- running, yielding `drained` before the final completion.
        and j.state in ('wait', 'prioritized', 'delayed', 'active')
    ) then
      -- BullMQ emits `drained` exactly once per transition from "has jobs"
      -- to "empty". Latch via `emq_queue_counters.drained`: emit only when
      -- not already latched; new enqueues reset the flag via
      -- `emq_clear_drained_latch_v1`.
      -- BullMQ only emits `drained` from the top-level moveToActive (worker
      -- main loop), not from the internal fetch-next path in moveToFinished.
      -- `p_emit_drained = false` matches that path; we also skip flipping the
      -- latch there so the next top-level moveToActive still transitions.
      if p_emit_drained then
        update :EMQ_SCHEMA.emq_queue_counters qc
        set drained = true
        where qc.queue_id = p_queue_id and qc.drained = false;
        if found then
          perform :EMQ_SCHEMA.emq_emit_event_v1(p_queue_id, 'drained', '{}'::jsonb);
        end if;
      end if;
    end if;
    select min(extract(epoch from process_at) * 1000) into v_next_delayed
    from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_queue_id and state = 'delayed';
    return query select null::jsonb, null::text, 0, coalesce(ceil(v_next_delayed)::bigint, 0);
    return;
  end if;

  update :EMQ_SCHEMA.emq_jobs
  set state = 'active',
      locked_by = p_token,
      lock_token = p_token,
      locked_at = now(),
      lock_expires_at = now() + (p_lock_ms::text || ' milliseconds')::interval,
      attempts_started = attempts_started + 1,
      processed_on = to_timestamp(p_now_ms::double precision / 1000.0),
      processed_by = case
        when p_worker_name is not null and length(trim(p_worker_name)) > 0
        then trim(p_worker_name)
        else processed_by
      end
  where pk = v_pick.pk;

  -- Redis INCR limiter (prepareJobForProcessing); new window gets
  -- `expires_at` like PEXPIRE on first job. The xact advisory lock above
  -- makes this read-modify-write atomic across concurrent workers.
  if v_rl_max is not null then
    insert into :EMQ_SCHEMA.emq_rate_limit_state (queue_id, tokens, expires_at)
    values (
      p_queue_id,
      1,
      case
        when v_rl_dur is not null then now() + (v_rl_dur::text || ' milliseconds')::interval
        else null
      end
    )
    on conflict (queue_id) do update set
      tokens = :EMQ_SCHEMA.emq_rate_limit_state.tokens + 1;
  end if;

  -- Mark the queue as having observed a worker. Mirrors the `stalled-check`
  -- Redis key created by BullMQ's worker loop; our compat shim surfaces it
  -- once this flag is set even after all processed jobs have been cleaned.
  -- Short-circuit the UPDATE once the flag is set so hot-path job pickup
  -- doesn't pay the full planner cost on every call.
  if not exists (
    select 1 from :EMQ_SCHEMA.emq_queues
    where id = p_queue_id and worker_seen = true
  ) then
    update :EMQ_SCHEMA.emq_queues
       set worker_seen = true
     where id = p_queue_id and worker_seen = false;
  end if;

  select to_jsonb(j.*) into v_json
  from :EMQ_SCHEMA.emq_jobs j where j.pk = v_pick.pk;

  perform :EMQ_SCHEMA.emq_emit_event_v1(
    p_queue_id,
    'active',
    jsonb_build_object('jobId', v_pick.job_id, 'prev', 'waiting')
  );

  return query select v_json, v_pick.job_id, 0, 0::bigint;
end;
$fn$;
