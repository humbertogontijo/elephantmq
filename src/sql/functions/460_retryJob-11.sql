-- Mirrors ref/bullmq/src/commands/retryJob-11.lua

drop function if exists :EMQ_SCHEMA.emq_retry_job_v1(
  bigint, text, text, boolean, text, text[]
);

create or replace function :EMQ_SCHEMA.emq_retry_job_v1(
  p_queue_id bigint,
  p_job_id text,
  p_token text,
  p_now_ms bigint,
  p_lifo boolean default false,
  p_failed_reason text default null,
  p_stacktrace text[] default null
) returns int
language plpgsql
as $fn$
declare
  n int;
  v_prio int;
  v_paused boolean;
  v_ws bigint;
  v_ps bigint;
begin
  -- Token '0' bypasses the lock check to match BullMQ's removeLock semantics.
  if p_token = '0' then
    select j.priority, q.paused
    into v_prio, v_paused
    from :EMQ_SCHEMA.emq_jobs j
    join :EMQ_SCHEMA.emq_queues q on q.id = j.queue_id
    where j.queue_id = p_queue_id and j.job_id = p_job_id and j.state = 'active';
  else
    select j.priority, q.paused
    into v_prio, v_paused
    from :EMQ_SCHEMA.emq_jobs j
    join :EMQ_SCHEMA.emq_queues q on q.id = j.queue_id
    where j.queue_id = p_queue_id and j.job_id = p_job_id and j.lock_token = p_token and j.state = 'active';
  end if;

  IF NOT FOUND THEN
    -- Distinguish "missing job" from "job is not active" so callers can raise
    -- the correct BullMQ error (ErrorCode.JobNotInState for wait/paused/...
    -- where retry makes no sense) — ref: retryJob-8.lua `not-in-state` branch.
    if exists (
      select 1 from :EMQ_SCHEMA.emq_jobs
      where queue_id = p_queue_id and job_id = p_job_id
    ) then
      return -3;
    end if;
    return -1;
  END IF;

  -- Mirror retryJob-11.lua: promote any ready delayed jobs BEFORE we push
  -- the retried job back to wait, so delayed jobs keep their temporal order
  -- and don't get jumped by a just-failed retry. Required by the
  -- "when there are delayed jobs between retries" worker tests.
  update :EMQ_SCHEMA.emq_jobs j
  set state = case when coalesce(v_paused, false)
      then 'paused':: :EMQ_SCHEMA.emq_job_state
      else 'wait':: :EMQ_SCHEMA.emq_job_state
    end,
      wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id),
      delay_ms = 0
  where j.queue_id = p_queue_id
    and j.state = 'delayed'
    and j.process_at <= to_timestamp(p_now_ms / 1000.0);

  -- Persist failure bookkeeping (stacktrace, failedReason) on retry so callers
  -- like Job.moveToFailed(err, token) that route into retry still surface the
  -- last error on the job — matches BullMQ's updateJobFieldsIfNeeded in
  -- src/commands/retryJob-8.lua.
  if p_failed_reason is not null or p_stacktrace is not null then
    update :EMQ_SCHEMA.emq_jobs j
    set failed_reason = coalesce(p_failed_reason, j.failed_reason),
        stacktrace = coalesce(p_stacktrace, j.stacktrace)
    where j.queue_id = p_queue_id and j.job_id = p_job_id;
  end if;

  if coalesce(v_prio, 0) > 0 then
    if p_lifo then
      -- LIFO retry into the prioritized zset: head of the same-priority
      -- bucket. moveToActive orders `prio_seq asc`, so the head sits at
      -- `min(prio_seq) - 1`.
      select coalesce(min(j.prio_seq), 1) - 1 into v_ps
      from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id
        and j.state = 'prioritized'
        and j.priority = v_prio
        and j.job_id <> p_job_id;
    else
      update :EMQ_SCHEMA.emq_queue_counters
      set priority_num = priority_num + 1
      where queue_id = p_queue_id
      returning priority_num into v_ps;
      if v_ps is null then
        insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, priority_num)
        values (p_queue_id, 1)
        on conflict (queue_id) do update
          set priority_num = :EMQ_SCHEMA.emq_queue_counters.priority_num + 1
        returning priority_num into v_ps;
      end if;
    end if;

    update :EMQ_SCHEMA.emq_jobs j
    set state = 'prioritized':: :EMQ_SCHEMA.emq_job_state,
        prio_seq = v_ps,
        wait_seq = null,
        attempts_made = j.attempts_made + 1,
        locked_by = null,
        lock_token = null,
        locked_at = null,
        lock_expires_at = null
    where j.queue_id = p_queue_id and j.job_id = p_job_id
      and (p_token = '0' or j.lock_token = p_token) and j.state = 'active';
  else
    if p_lifo then
      select coalesce(min(wait_seq), 1) - 1 into v_ws
      from :EMQ_SCHEMA.emq_jobs
      where queue_id = p_queue_id and state = 'wait';
    else
      v_ws := :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id);
    end if;

    update :EMQ_SCHEMA.emq_jobs j
    set state = case when coalesce(v_paused, false)
        then 'paused':: :EMQ_SCHEMA.emq_job_state
        else 'wait':: :EMQ_SCHEMA.emq_job_state
      end,
        wait_seq = v_ws,
        prio_seq = null,
        attempts_made = j.attempts_made + 1,
        locked_by = null,
        lock_token = null,
        locked_at = null,
        lock_expires_at = null
    where j.queue_id = p_queue_id and j.job_id = p_job_id
      and (p_token = '0' or j.lock_token = p_token) and j.state = 'active';
  end if;

  get diagnostics n = row_count;
  if n > 0 then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'waiting',
      jsonb_build_object('jobId', p_job_id, 'prev', 'active')
    );
    return 1;
  end if;
  return -1;
end;
$fn$;
