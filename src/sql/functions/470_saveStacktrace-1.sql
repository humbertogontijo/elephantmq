-- Mirrors ref/bullmq/src/commands/saveStacktrace-1.lua

create or replace function :EMQ_SCHEMA.emq_save_stacktrace_v1(
  p_queue_id bigint,
  p_job_id text,
  p_stacktrace text[],
  p_failed_reason text
) returns int
language sql
as $fn$
  with u as (
    update :EMQ_SCHEMA.emq_jobs
    set stacktrace = coalesce(p_stacktrace, stacktrace),
        failed_reason = coalesce(p_failed_reason, failed_reason)
    where queue_id = p_queue_id and job_id = p_job_id
    returning 1
  )
  select count(*)::int from u;
$fn$;
