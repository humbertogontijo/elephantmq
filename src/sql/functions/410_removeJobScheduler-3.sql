create or replace function :EMQ_SCHEMA.emq_remove_job_scheduler_v1(p_queue_id bigint, p_scheduler_id text)
returns int
language plpgsql
as $fn$
declare
  v_next bigint;
  v_job_id text;
  v_prev_state text;
  v_removed boolean := false;
begin
  -- Returns 0 on success and -1 when the scheduler does not exist.
  -- Removes the scheduler row plus any still-pending delayed job created
  -- for the upcoming iteration. Rows already promoted to wait / prioritized
  -- are intentionally preserved so the already-enqueued job still runs.
  select next_millis into v_next from :EMQ_SCHEMA.emq_job_schedulers
    where queue_id = p_queue_id and scheduler_id = p_scheduler_id;
  if FOUND then
    v_removed := true;
    delete from :EMQ_SCHEMA.emq_job_schedulers
      where queue_id = p_queue_id and scheduler_id = p_scheduler_id;
  end if;

  if v_next is not null then
    v_job_id := 'repeat:' || p_scheduler_id || ':' || v_next::text;
    delete from :EMQ_SCHEMA.emq_jobs
     where queue_id = p_queue_id
       and job_id = v_job_id
       and state = 'delayed':: :EMQ_SCHEMA.emq_job_state
    returning state::text into v_prev_state;
    if v_prev_state is not null then
      perform :EMQ_SCHEMA.emq_emit_event_v1(
        p_queue_id,
        'removed',
        jsonb_build_object('jobId', v_job_id, 'prev', v_prev_state)
      );
    end if;
  end if;

  if v_removed then
    return 0;
  end if;
  return -1;
end;
$fn$;
