-- Mirrors ref/bullmq/src/commands/getJobScheduler-1.lua

create or replace function :EMQ_SCHEMA.emq_get_job_scheduler_v1(p_queue_id bigint, p_scheduler_id text)
returns jsonb
language sql
stable
as $fn$
  select to_jsonb(s.*) from :EMQ_SCHEMA.emq_job_schedulers s
  where s.queue_id = p_queue_id and s.scheduler_id = p_scheduler_id;
$fn$;
