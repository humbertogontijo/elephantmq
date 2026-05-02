-- Mirrors ref/bullmq/src/commands/extendLock-2.lua

create or replace function :EMQ_SCHEMA.emq_extend_lock_v1(
  p_queue_id bigint,
  p_job_id text,
  p_token text,
  p_duration_ms bigint
) returns int
language plpgsql
as $fn$
declare n int;
begin
  update :EMQ_SCHEMA.emq_jobs
  set lock_expires_at = now() + (p_duration_ms::text || ' milliseconds')::interval
  where queue_id = p_queue_id
    and job_id = p_job_id
    and lock_token = p_token
    and state = 'active'
    and lock_expires_at is not null
    and lock_expires_at > now();
  get diagnostics n = row_count;
  if n > 0 then return 1; end if;
  return -2;
end;
$fn$;
