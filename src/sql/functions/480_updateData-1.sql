-- Mirrors ref/bullmq/src/commands/updateData-1.lua

create or replace function :EMQ_SCHEMA.emq_update_data_v1(
  p_queue_id bigint,
  p_job_id text,
  p_data jsonb
) returns int
language plpgsql
as $fn$
declare n int;
begin
  update :EMQ_SCHEMA.emq_jobs set data = p_data
  where queue_id = p_queue_id and job_id = p_job_id;
  get diagnostics n = row_count;
  return case when n > 0 then 1 else -1 end;
end;
$fn$;
