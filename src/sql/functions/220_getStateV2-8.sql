-- Mirrors ref/bullmq/src/commands/getStateV2-8.lua

-- Raw job state (no folding prioritized → waiting); used when Redis >= 6.0.6 getStateV2 path.
create or replace function :EMQ_SCHEMA.emq_get_state_v2_v1(p_queue_id bigint, p_job_id text)
returns text
language plpgsql
stable
as $fn$
declare st text;
begin
  select state::text into st from :EMQ_SCHEMA.emq_jobs where queue_id = p_queue_id and job_id = p_job_id;
  if st is null then return 'unknown'; end if;
  return case st
    when 'wait' then 'waiting'
    when 'waiting-children' then 'waiting-children'
    else st
  end;
end;
$fn$;
