-- Mirrors ref/bullmq/src/commands/addLog-2.lua

create or replace function :EMQ_SCHEMA.emq_add_log_v1(
  p_queue_id bigint,
  p_job_id text,
  p_line text,
  p_keep_logs int
) returns int
language plpgsql
as $fn$
declare
  jpk bigint;
  s bigint;
begin
  select pk into jpk from :EMQ_SCHEMA.emq_jobs where queue_id = p_queue_id and job_id = p_job_id;
  if jpk is null then return -1; end if;
  select coalesce(max(seq), 0) + 1 into s from :EMQ_SCHEMA.emq_job_logs where job_pk = jpk;
  insert into :EMQ_SCHEMA.emq_job_logs (job_pk, seq, line) values (jpk, s, p_line);
  if p_keep_logs > 0 then
    delete from :EMQ_SCHEMA.emq_job_logs l
    where l.job_pk = jpk
      and l.seq <= s - p_keep_logs;
  end if;
  perform :EMQ_SCHEMA.emq_emit_event_v1(p_queue_id, 'logs', jsonb_build_object('jobId', p_job_id));
  -- BullMQ's addLog-2.lua returns `min(keepLogs, logCount)` so callers see
  -- the current retained-log count rather than the raw sequence id.
  if p_keep_logs > 0 then
    return least(p_keep_logs, s)::int;
  end if;
  return s::int;
end;
$fn$;
