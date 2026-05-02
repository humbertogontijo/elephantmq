drop function if exists :EMQ_SCHEMA.emq_update_job_scheduler_v1(bigint, text, bigint);

create or replace function :EMQ_SCHEMA.emq_update_job_scheduler_v1(
  p_queue_id bigint,
  p_scheduler_id text,
  p_next_millis bigint,
  p_producer_id text default null
) returns text
language plpgsql
as $fn$
declare
  v_prev bigint;
  v_expected_producer text;
  v_next_job_id text;
  v_id text;
begin
  -- Advance the scheduler only when the caller's `producerId` matches the
  -- current delayed-job id (`repeat:<key>:<prev_next_millis>`). This guards
  -- against double-scheduling when both `Job.moveToCompleted` and
  -- `Worker.nextJobFromJobData` trigger `upsertJobScheduler` for the same
  -- completed iteration: only the first call advances; the second finds the
  -- scheduler already advanced and becomes a no-op.
  --
  -- Also skip the advance if a delayed job at the new `next_millis` already
  -- exists, so we never overwrite a freshly-inserted iteration. Emit a
  -- `duplicated` event so subscribers can observe the collision.
  select next_millis into v_prev
    from :EMQ_SCHEMA.emq_job_schedulers
    where queue_id = p_queue_id and scheduler_id = p_scheduler_id;
  if v_prev is null then
    return null;
  end if;

  if p_producer_id is not null then
    v_expected_producer := 'repeat:' || p_scheduler_id || ':' || v_prev::text;
    if p_producer_id <> v_expected_producer then
      return null;
    end if;
  end if;

  v_next_job_id := 'repeat:' || p_scheduler_id || ':' || p_next_millis::text;
  if exists (
    select 1 from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_queue_id and job_id = v_next_job_id
  ) then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'duplicated',
      jsonb_build_object('jobId', v_next_job_id)
    );
    return null;
  end if;

  update :EMQ_SCHEMA.emq_job_schedulers
     set next_millis = p_next_millis,
         iteration_count = iteration_count + 1
   where queue_id = p_queue_id and scheduler_id = p_scheduler_id
   returning scheduler_id into v_id;

  return v_id;
end;
$fn$;
