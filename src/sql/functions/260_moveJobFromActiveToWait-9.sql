-- Mirrors ref/bullmq/src/commands/moveJobFromActiveToWait-9.lua

drop function if exists :EMQ_SCHEMA.emq_move_job_from_active_to_wait_v1(bigint, text, text);

create or replace function :EMQ_SCHEMA.emq_move_job_from_active_to_wait_v1(
  p_queue_id bigint,
  p_job_id text,
  p_token text
) returns bigint
language plpgsql
as $fn$
declare
  n int;
  v_prio int;
  v_ps bigint;
  v_paused boolean;
  v_pttl bigint;
begin
  -- Preserve priority: if the job had priority > 0, put it back at the head of
  -- its priority bucket in the `prioritized` zset (moveLimitedBackToWait on
  -- rate-limited priority jobs); else it lands on the head of `wait`. When the
  -- queue is paused, rate-limited jobs must land on the `paused` list instead
  -- (BullMQ moveToWait-8.lua).
  select priority into v_prio
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and job_id = p_job_id and lock_token = p_token and state = 'active';

  if v_prio is null then
    return -1;
  end if;

  select coalesce(q.paused, false) into v_paused
  from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id;

  if coalesce(v_paused, false) then
    update :EMQ_SCHEMA.emq_jobs
    set state = 'paused',
        wait_seq = coalesce(
          (select min(j2.wait_seq) - 1
             from :EMQ_SCHEMA.emq_jobs j2
            where j2.queue_id = p_queue_id
              and j2.state = 'paused':: :EMQ_SCHEMA.emq_job_state),
          :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id)
        ),
        locked_by = null,
        lock_token = null,
        locked_at = null,
        lock_expires_at = null
    where queue_id = p_queue_id and job_id = p_job_id
      and lock_token = p_token and state = 'active';
    get diagnostics n = row_count;
    if n > 0 then
      perform :EMQ_SCHEMA.emq_emit_event_v1(p_queue_id, 'waiting', jsonb_build_object('jobId', p_job_id, 'prev', 'active'));
    end if;
    return n;
  end if;

  if v_prio > 0 then
    select coalesce(min(j.prio_seq), 1) - 1 into v_ps
    from :EMQ_SCHEMA.emq_jobs j
    where j.queue_id = p_queue_id
      and j.state = 'prioritized'
      and j.priority = v_prio
      and j.job_id <> p_job_id;

    update :EMQ_SCHEMA.emq_jobs
    set state = 'prioritized':: :EMQ_SCHEMA.emq_job_state,
        prio_seq = v_ps,
        wait_seq = null,
        locked_by = null,
        lock_token = null,
        locked_at = null,
        lock_expires_at = null
    where queue_id = p_queue_id and job_id = p_job_id
      and lock_token = p_token and state = 'active';
  else
    update :EMQ_SCHEMA.emq_jobs
    set state = 'wait',
        wait_seq = coalesce(
          (select min(j2.wait_seq) - 1
             from :EMQ_SCHEMA.emq_jobs j2
            where j2.queue_id = p_queue_id
              and j2.state = 'wait':: :EMQ_SCHEMA.emq_job_state),
          :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id)
        ),
        locked_by = null,
        lock_token = null,
        locked_at = null,
        lock_expires_at = null
    where queue_id = p_queue_id and job_id = p_job_id
      and lock_token = p_token and state = 'active';
  end if;
  get diagnostics n = row_count;
  if n = 0 then
    return -1;
  end if;
  perform :EMQ_SCHEMA.emq_emit_event_v1(p_queue_id, 'waiting', jsonb_build_object('jobId', p_job_id, 'prev', 'active'));

  -- BullMQ moveJobFromActiveToWait-9.lua ends with `PTTL` on the limiter key
  -- (used by `moveLimitedBackToWait` to set Worker.limitUntil). Return the
  -- remaining window in ms (0 when no active limiter).
  select greatest(0::numeric, ceil(extract(epoch from (r.expires_at - now())) * 1000))::bigint
  into v_pttl
  from :EMQ_SCHEMA.emq_rate_limit_state r
  where r.queue_id = p_queue_id and r.expires_at is not null and r.expires_at > now();
  return coalesce(v_pttl, 0::bigint);
end;
$fn$;
