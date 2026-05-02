-- Mirrors ref/bullmq/src/commands/updateProgress-3.lua

create or replace function :EMQ_SCHEMA.emq_update_progress_v1(
  p_queue_id bigint,
  p_job_id text,
  p_progress jsonb
) returns int
language plpgsql
as $fn$
declare n int;
begin
  update :EMQ_SCHEMA.emq_jobs set progress = p_progress
  where queue_id = p_queue_id and job_id = p_job_id;
  get diagnostics n = row_count;
  if n > 0 then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'progress',
      jsonb_build_object('jobId', p_job_id, 'data', p_progress)
    );
  end if;
  return case when n > 0 then 1 else -1 end;
end;
$fn$;
