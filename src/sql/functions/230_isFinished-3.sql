-- Mirrors ref/bullmq/src/commands/isFinished-3.lua

create or replace function :EMQ_SCHEMA.emq_is_finished_v1(p_queue_id bigint, p_job_id text)
returns table (kind int, ret jsonb)
language plpgsql
stable
as $fn$
declare j :EMQ_SCHEMA.emq_jobs;
begin
  select * into j from :EMQ_SCHEMA.emq_jobs where queue_id = p_queue_id and job_id = p_job_id;
  -- kind -2: job row is missing entirely (e.g. removeOnComplete trimmed it).
  -- Callers surface this as ErrorCode.JobNotExist to match BullMQ's isFinished
  -- lua which `return -1` when exists(jobKey) is false.
  if not found then return query select -2, null::jsonb; return; end if;
  if j.state = 'completed' then return query select 1, coalesce(j.return_value, 'null'::jsonb); return; end if;
  if j.state = 'failed' then return query select -1, to_jsonb(coalesce(j.failed_reason, '')); return; end if;
  return query select 0, null::jsonb;
end;
$fn$;
