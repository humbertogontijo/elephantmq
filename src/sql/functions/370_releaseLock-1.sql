-- Mirrors ref/bullmq/src/commands/releaseLock-1.lua

create or replace function :EMQ_SCHEMA.emq_release_lock_v1(
  p_queue_id bigint,
  p_job_id text,
  p_token text
) returns int
language sql
as $fn$
  with u as (
    update :EMQ_SCHEMA.emq_jobs
    set lock_token = null, lock_expires_at = null, locked_by = null, locked_at = null
    where queue_id = p_queue_id and job_id = p_job_id and lock_token = p_token
    returning 1
  )
  select count(*)::int from u;
$fn$;
