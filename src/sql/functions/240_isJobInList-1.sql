-- Mirrors ref/bullmq/src/commands/isJobInList-1.lua

create or replace function :EMQ_SCHEMA.emq_is_job_in_list_v1(
  p_queue_id bigint,
  p_state text,
  p_job_id text
) returns boolean
language sql
stable
as $fn$
  select exists(
    select 1 from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_queue_id and job_id = p_job_id and state::text = p_state
  );
$fn$;
