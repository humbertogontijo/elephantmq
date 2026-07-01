-- Mirrors ref/bullmq/src/commands/moveJobsToWait-8.lua

drop function if exists :EMQ_SCHEMA.emq_move_jobs_to_wait_v1(bigint, text);
drop function if exists :EMQ_SCHEMA.emq_move_jobs_to_wait_v1(bigint, text, int, bigint);

create or replace function :EMQ_SCHEMA.emq_move_jobs_to_wait_v1(
  p_queue_id bigint,
  p_from_state text,
  p_count int default 1000,
  p_timestamp_ms bigint default null
) returns int
language plpgsql
as $fn$
declare
  n int;
  st text;
  v_paused boolean;
  v_ts timestamptz;
  v_job record;
  v_prio_seq bigint;
  v_target :EMQ_SCHEMA.emq_job_state;
  v_prev text;
begin
  st := case
    when p_from_state in ('failed', 'completed', 'delayed') then p_from_state
    else 'failed'
  end;
  v_prev := st;
  v_ts := to_timestamp(coalesce(p_timestamp_ms, (extract(epoch from now()) * 1000)::bigint) / 1000.0);

  select q.paused into v_paused
  from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id;

  if coalesce(v_paused, false) then
    v_target := 'paused';
  else
    v_target := 'wait';
  end if;

  n := 0;

  for v_job in
    select j.job_id, j.priority, j.pk
    from :EMQ_SCHEMA.emq_jobs j
    where j.queue_id = p_queue_id
      and j.state::text = st
      and (
        st = 'delayed'
          and j.process_at <= v_ts
        or st in ('failed', 'completed')
          and coalesce(j.finished_on, to_timestamp(0)) <= v_ts
      )
    order by
      case when st = 'delayed' then extract(epoch from j.process_at) else extract(epoch from j.finished_on) end asc,
      j.pk asc
    limit greatest(coalesce(p_count, 1000), 0)
  loop
    if st = 'delayed' and coalesce(v_job.priority, 0) > 0 then
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

      update :EMQ_SCHEMA.emq_jobs j
      set state = case when coalesce(v_paused, false)
                      then 'paused':: :EMQ_SCHEMA.emq_job_state
                      else 'prioritized':: :EMQ_SCHEMA.emq_job_state end,
          prio_seq = v_prio_seq,
          process_at = null,
          delay_ms = 0,
          finished_on = null,
          processed_on = null,
          failed_reason = case when st = 'failed' then null else j.failed_reason end,
          return_value = case when st = 'completed' then null else j.return_value end
      where j.pk = v_job.pk;
    else
      update :EMQ_SCHEMA.emq_jobs j
      set state = v_target,
          wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id),
          prio_seq = null,
          process_at = null,
          delay_ms = 0,
          finished_on = null,
          processed_on = null,
          failed_reason = case when st = 'failed' then null else j.failed_reason end,
          return_value = case when st = 'completed' then null else j.return_value end
      where j.pk = v_job.pk;
    end if;

    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'waiting',
      jsonb_build_object('jobId', v_job.job_id, 'prev', v_prev)
    );

    n := n + 1;
  end loop;

  -- BullMQ moveJobsToWait-8.lua: return 1 when the batch limit was hit
  -- AND more matching jobs may remain; 0 when done.
  if n >= greatest(coalesce(p_count, 1000), 0) and n > 0 then
    if exists (
      select 1
      from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id
        and j.state::text = st
        and (
          st = 'delayed'
            and j.process_at <= v_ts
          or st in ('failed', 'completed')
            and coalesce(j.finished_on, to_timestamp(0)) <= v_ts
        )
    ) then
      return 1;
    end if;
  end if;
  return 0;
end;
$fn$;
