-- Mirrors ref/bullmq/src/commands/reprocessJob-8.lua

drop function if exists :EMQ_SCHEMA.emq_reprocess_job_v1(bigint, text, boolean, boolean);

create or replace function :EMQ_SCHEMA.emq_reprocess_job_v1(
  p_queue_id bigint,
  p_job_id text,
  p_reset_attempts_made boolean default false,
  p_reset_attempts_started boolean default false,
  p_target_state text default null
)
returns int
language plpgsql
as $fn$
declare
  n int;
  v_current text;
  v_paused boolean;
  v_target_state :EMQ_SCHEMA.emq_job_state;
begin
  -- When caller passes a target state (Job.retry('failed' | 'completed')), we
  -- must only reprocess if the job is actually in that state. BullMQ's
  -- reprocessJob-4.lua returns `-3` (JobNotInState) when the requested
  -- src state doesn't match the row, so tests like `worker.test.ts > should
  -- not retry a job that has been retried already` assert on the resulting
  -- "Job <id> is not in the <state> state. reprocessJob" message.
  -- Mirror reprocessJob-4.lua: when the queue is paused, the reprocessed job
  -- lands on the `paused` list instead of `wait`, so callers that retry a
  -- failed/completed job while the queue is paused see the row counted under
  -- `paused` (worker.test.ts > when queue is paused and retry a job).
  select paused into v_paused from :EMQ_SCHEMA.emq_queues where id = p_queue_id;
  if coalesce(v_paused, false) then
    v_target_state := 'paused';
  else
    v_target_state := 'wait';
  end if;
  update :EMQ_SCHEMA.emq_jobs
  set state = v_target_state,
      wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id),
      lock_token = null,
      locked_by = null,
      locked_at = null,
      lock_expires_at = null,
      attempts_made = case when p_reset_attempts_made then 0 else attempts_made end,
      attempts_started = case when p_reset_attempts_started then 0 else attempts_started end,
      processed_by = null,
      processed_on = null,
      finished_on = null,
      return_value = null,
      failed_reason = null,
      stacktrace = '{}'::text[]
  where queue_id = p_queue_id
    and job_id = p_job_id
    and (
      (p_target_state is null and state in ('failed', 'completed'))
      or (p_target_state is not null and state::text = p_target_state)
    )
    and lock_token is null;
  get diagnostics n = row_count;
  if n > 0 then
    -- reprocessJob-8.lua flips the parent dep back to pending so the parent
    -- sees the child as unresolved again. We mirror that here; if the parent
    -- was already completed and moved on, the dep flip is a no-op for state
    -- transitions but keeps `getDependencies` consistent.
    update :EMQ_SCHEMA.emq_job_deps d
    set status = 'pending',
        return_value = null,
        failed_reason = null,
        resolved_at = null
    from :EMQ_SCHEMA.emq_jobs j
    where j.queue_id = p_queue_id
      and j.job_id = p_job_id
      and d.child_pk = j.pk
      and d.status in ('processed', 'failed', 'ignored');
    return 1;
  end if;

  -- Distinguish "job missing" from "job not in requested state" so the JS
  -- wrapper can raise the right BullMQ error (`JobNotExist` vs
  -- `JobNotInState`). Mirrors reprocessJob-4.lua.
  select state::text into v_current
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and job_id = p_job_id;

  if not found then
    return -1;
  end if;
  return -3;
end;
$fn$;
