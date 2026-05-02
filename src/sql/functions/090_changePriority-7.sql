-- Mirrors ref/bullmq/src/commands/changePriority-7.lua

create or replace function :EMQ_SCHEMA.emq_change_priority_v1(
  p_queue_id bigint,
  p_job_id text,
  p_priority int,
  p_lifo boolean default false
) returns int
language plpgsql
as $fn$
declare
  n int;
  v_st :EMQ_SCHEMA.emq_job_state;
  v_paused boolean;
  v_ps bigint;
  v_ws bigint;
begin
  select j.state, q.paused
  into v_st, v_paused
  from :EMQ_SCHEMA.emq_jobs j
  join :EMQ_SCHEMA.emq_queues q on q.id = j.queue_id
  where j.queue_id = p_queue_id and j.job_id = p_job_id;

  IF NOT FOUND THEN
    return -1;
  END IF;

  -- BullMQ's changePriority-5.lua updates the priority hash field regardless of
  -- state, but only manipulates list/zset membership for wait/paused/prioritized.
  -- Delayed jobs: just update the priority field and the "priority" on delayed
  -- jobs is consulted at promote time (see emq_promote_v1).
  if v_st::text not in ('wait', 'paused', 'prioritized') then
    update :EMQ_SCHEMA.emq_jobs j
    set priority = p_priority
    where j.queue_id = p_queue_id and j.job_id = p_job_id;
    return 1;
  end if;

  if p_priority > 0 then
    if p_lifo then
      -- LIFO inside the prioritized zset: move this job in front of all peers
      -- with the same priority. moveToActive picks `prio_seq asc`, so the
      -- head is `min(prio_seq) - 1`.
      select coalesce(min(j.prio_seq), 1) - 1 into v_ps
      from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id
        and j.state = 'prioritized'
        and j.priority = p_priority
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
    set priority = p_priority,
        state = 'prioritized':: :EMQ_SCHEMA.emq_job_state,
        prio_seq = v_ps,
        wait_seq = null
    where j.queue_id = p_queue_id and j.job_id = p_job_id;
  else
    -- Priority 0 transition: mirror BullMQ's changePriority-7.lua which does
    -- RPUSH (lifo) -> head of wait / LPUSH (non-lifo) -> tail of wait. Head
    -- means the job is consumed next, which corresponds to `min(wait_seq) - 1`.
    if p_lifo then
      -- Schema-scoped key (see emq_move_to_active_v1) — must match the
      -- key used by emq_add_standard_job_v1's LIFO insert path so a
      -- concurrent add and changePriority on the same wait list
      -- properly serialize on `min(wait_seq) - 1`.
      perform pg_advisory_xact_lock(
        hashtextextended(
          'emq_lifo_wait_seq:' || :EMQ_SCHEMA_NAME_LIT || ':' || p_queue_id::text,
          0::bigint
        )
      );
      select min(j.wait_seq) into v_ws
      from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id
        and j.state in ('wait':: :EMQ_SCHEMA.emq_job_state,
                        'paused':: :EMQ_SCHEMA.emq_job_state)
        and j.job_id <> p_job_id;
      if v_ws is null then
        v_ws := :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id);
      else
        v_ws := v_ws - 1;
      end if;
    else
      v_ws := :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id);
    end if;

    update :EMQ_SCHEMA.emq_jobs j
    set priority = 0,
        state = case when coalesce(v_paused, false)
            then 'paused':: :EMQ_SCHEMA.emq_job_state
            else 'wait':: :EMQ_SCHEMA.emq_job_state
          end,
        prio_seq = null,
        wait_seq = v_ws
    where j.queue_id = p_queue_id and j.job_id = p_job_id;
  end if;

  get diagnostics n = row_count;
  if n > 0 then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'waiting',
      jsonb_build_object('jobId', p_job_id)
    );
    return 1;
  end if;
  return -1;
end;
$fn$;
