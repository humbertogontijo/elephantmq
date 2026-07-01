-- Mirrors ref/bullmq/src/commands/moveToDelayed-12.lua

drop function if exists :EMQ_SCHEMA.emq_move_to_delayed_v1(bigint, text, bigint, text);
drop function if exists :EMQ_SCHEMA.emq_move_to_delayed_v1(bigint, text, bigint, text, text, text[]);
drop function if exists :EMQ_SCHEMA.emq_move_to_delayed_v1(bigint, text, bigint, text, text, text[], bigint);

create or replace function :EMQ_SCHEMA.emq_move_to_delayed_v1(
  p_queue_id bigint,
  p_job_id text,
  p_process_at_ms bigint,
  p_token text,
  p_failed_reason text default null,
  p_stacktrace text[] default null,
  p_delay_ms bigint default null,
  p_fetch_next boolean default false,
  p_lock_ms bigint default null,
  p_worker_name text default null,
  p_limiter_max bigint default null,
  p_limiter_duration_ms bigint default null,
  p_now_ms bigint default null
) returns table (
  err_code int,
  next_job_row jsonb,
  next_job_id text,
  rate_limit_delay_ms int,
  block_until_ms bigint
)
language plpgsql
as $fn$
declare
  n int;
  j :EMQ_SCHEMA.emq_jobs;
  v_now_ms bigint;
  v_active record;
begin
  select * into j
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and job_id = p_job_id;

  if not found then
    return query select -1, null::jsonb, null::text, 0, 0::bigint;
    return;
  end if;

  if j.state::text <> 'active' then
    return query select -3, null::jsonb, null::text, 0, 0::bigint;
    return;
  end if;

  if p_token is distinct from '0' then
    if j.lock_token is null or j.lock_expires_at is null or j.lock_expires_at <= now() then
      return query select -2, null::jsonb, null::text, 0, 0::bigint;
      return;
    end if;
    if j.lock_token is distinct from p_token then
      return query select -6, null::jsonb, null::text, 0, 0::bigint;
      return;
    end if;
  end if;

  update :EMQ_SCHEMA.emq_jobs
  set state = 'delayed',
      process_at = to_timestamp(p_process_at_ms / 1000.0),
      delay_ms = greatest(
        coalesce(
          p_delay_ms,
          p_process_at_ms - (extract(epoch from now()) * 1000)::bigint
        ),
        0
      ),
      lock_token = null,
      locked_by = null,
      locked_at = null,
      lock_expires_at = null,
      failed_reason = coalesce(p_failed_reason, failed_reason),
      stacktrace = coalesce(p_stacktrace, stacktrace),
      attempts_made = case
        when p_failed_reason is not null then attempts_made + 1
        else attempts_made
      end
  where pk = j.pk and state = 'active';

  get diagnostics n = row_count;
  if n = 0 then
    return query select -1, null::jsonb, null::text, 0, 0::bigint;
    return;
  end if;

  perform :EMQ_SCHEMA.emq_emit_event_v1(
    p_queue_id,
    'delayed',
    jsonb_build_object('jobId', p_job_id, 'delay', p_process_at_ms)
  );

  if not p_fetch_next then
    return query select 0, null::jsonb, null::text, 0, 0::bigint;
    return;
  end if;

  v_now_ms := coalesce(p_now_ms, (extract(epoch from now()) * 1000)::bigint);

  select * into v_active
  from :EMQ_SCHEMA.emq_move_to_active_v1(
    p_queue_id,
    v_now_ms,
    p_token,
    coalesce(p_lock_ms, 30000),
    p_worker_name,
    true,
    p_limiter_max,
    p_limiter_duration_ms
  );

  return query select
    0,
    v_active.out_job_row,
    v_active.out_job_id,
    coalesce(v_active.rate_limit_delay_ms, 0),
    coalesce(v_active.block_until_ms, 0::bigint);
end;
$fn$;
