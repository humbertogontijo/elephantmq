-- Mirrors ref/bullmq/src/commands/moveStalledJobsToWait-8.lua

create or replace function :EMQ_SCHEMA.emq_move_stalled_jobs_to_wait_v1(
  p_queue_id bigint,
  p_max_stalled int
) returns table (recovered_ids text[], failed_ids text[])
language plpgsql
as $fn$
declare
  r1 text[];
  r2 text[];
  v_jid text;
  v_paused boolean;
  v_count int;
  v_base bigint;
  v_fail_reason constant text := 'job stalled more than allowable limit';
begin
  -- Serialize with moveToActive/moveToFinished per queue so our state changes
  -- don't race (mirrors BullMQ's single-threaded Lua stalled sweep).
  perform pg_advisory_xact_lock(
    hashtextextended(:EMQ_SCHEMA_NAME_LIT, 2024000001::bigint) # p_queue_id
  );

  select q.paused into v_paused
  from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id;

  -- Fail jobs that exceeded maxStalledCount directly (BullMQ moves them to
  -- failed in the stalled sweep, not via deferred re-queue).
  with failed as (
    update :EMQ_SCHEMA.emq_jobs j
    set state = 'failed',
        finished_on = now(),
        failed_reason = v_fail_reason,
        delay_ms = 0,
        deferred_failure = null,
        lock_token = null,
        locked_by = null,
        locked_at = null,
        lock_expires_at = null,
        stalled_counter = j.stalled_counter + 1
    where j.queue_id = p_queue_id
      and j.state = 'active'
      and j.lock_expires_at < now()
      and j.stalled_counter + 1 > p_max_stalled
    returning j.job_id
  )
  select coalesce(array_agg(job_id), array[]::text[]) into r2 from failed;

  if r2 is not null then
    foreach v_jid in array r2 loop
      perform :EMQ_SCHEMA.emq_emit_event_v1(
        p_queue_id, 'failed',
        jsonb_build_object(
          'jobId', v_jid,
          'failedReason', v_fail_reason,
          'prev', 'active'
        )
      );
    end loop;
  end if;

  select count(*)
  into v_count
  from :EMQ_SCHEMA.emq_jobs j
  where j.queue_id = p_queue_id
    and j.state = 'active'
    and j.lock_expires_at < now();

  if v_count = 0 then
    return query select coalesce(r1, array[]::text[]), coalesce(r2, array[]::text[]);
    return;
  end if;

  update :EMQ_SCHEMA.emq_queue_counters
  set wait_num = wait_num + v_count
  where queue_id = p_queue_id
  returning wait_num - v_count into v_base;

  with ranked as (
    select j.pk, j.job_id,
           row_number() over (order by j.pk desc) as rn
    from :EMQ_SCHEMA.emq_jobs j
    where j.queue_id = p_queue_id
      and j.state = 'active'
      and j.lock_expires_at < now()
  ),
  u as (
    update :EMQ_SCHEMA.emq_jobs j
    set state = case when coalesce(v_paused, false)
          then 'paused':: :EMQ_SCHEMA.emq_job_state
          else 'wait':: :EMQ_SCHEMA.emq_job_state
        end,
        wait_seq = v_base + r.rn,
        stalled_counter = j.stalled_counter + 1,
        deferred_failure = null,
        lock_token = null,
        locked_by = null,
        locked_at = null,
        lock_expires_at = null
    from ranked r
    where j.pk = r.pk
    returning j.job_id, r.rn
  )
  select coalesce(array_agg(job_id order by rn), array[]::text[]) into r1 from u;

  if r1 is not null then
    foreach v_jid in array r1 loop
      perform :EMQ_SCHEMA.emq_emit_event_v1(
        p_queue_id, 'stalled',
        jsonb_build_object('jobId', v_jid)
      );
    end loop;
  end if;

  return query select coalesce(r1, array[]::text[]), coalesce(r2, array[]::text[]);
end;
$fn$;
