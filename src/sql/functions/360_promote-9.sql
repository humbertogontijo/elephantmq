-- Mirrors ref/bullmq/src/commands/promote-9.lua

create or replace function :EMQ_SCHEMA.emq_promote_v1(p_queue_id bigint, p_job_id text)
returns int
language plpgsql
as $fn$
declare
  n int;
  v_prio int;
  v_prio_seq bigint;
  v_paused boolean;
begin
  -- Determine current priority (set at add-time); a promoted prioritized job
  -- must re-enter the 'prioritized' state with a fresh prio_seq so the
  -- moveToActive priority index can order it correctly.
  select priority into v_prio
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and job_id = p_job_id and state = 'delayed';

  if v_prio is null then
    -- Distinguish "no such job" (-1) from "job is not delayed" (-3) to mirror
    -- BullMQ's promote.lua return codes (keep finishedErrors in sync).
    if exists (
      select 1 from :EMQ_SCHEMA.emq_jobs
      where queue_id = p_queue_id and job_id = p_job_id
    ) then
      return -3;
    end if;
    return -1;
  end if;

  select paused into v_paused from :EMQ_SCHEMA.emq_queues where id = p_queue_id;

  if v_prio > 0 then
    update :EMQ_SCHEMA.emq_queue_counters
    set priority_num = priority_num + 1
    where queue_id = p_queue_id
    returning priority_num into v_prio_seq;
    if v_prio_seq is null then
      insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, priority_num)
      values (p_queue_id, 1)
      on conflict (queue_id) do update
        set priority_num = :EMQ_SCHEMA.emq_queue_counters.priority_num + 1
      returning priority_num into v_prio_seq;
    end if;

    update :EMQ_SCHEMA.emq_jobs
    set state = case when coalesce(v_paused, false)
                    then 'paused':: :EMQ_SCHEMA.emq_job_state
                    else 'prioritized':: :EMQ_SCHEMA.emq_job_state end,
        prio_seq = v_prio_seq,
        process_at = null
    where queue_id = p_queue_id and job_id = p_job_id and state = 'delayed';
  else
    update :EMQ_SCHEMA.emq_jobs
    set state = case when coalesce(v_paused, false)
                    then 'paused':: :EMQ_SCHEMA.emq_job_state
                    else 'wait':: :EMQ_SCHEMA.emq_job_state end,
        wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id),
        process_at = null
    where queue_id = p_queue_id and job_id = p_job_id and state = 'delayed';
  end if;

  get diagnostics n = row_count;
  if n > 0 then
    perform :EMQ_SCHEMA.emq_emit_event_v1(p_queue_id, 'waiting', jsonb_build_object('jobId', p_job_id));
    return 1;
  end if;
  return -1;
end;
$fn$;
