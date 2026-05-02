-- Mirrors ref/bullmq/src/commands/moveJobsToWait-8.lua

create or replace function :EMQ_SCHEMA.emq_move_jobs_to_wait_v1(
  p_queue_id bigint,
  p_from_state text
) returns int
language plpgsql
as $fn$
declare
  n int;
  st text;
begin
  st := case
    when p_from_state in ('failed', 'completed') then p_from_state
    else 'delayed'
  end;
  update :EMQ_SCHEMA.emq_jobs j
  set state = 'wait',
      wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id)
  where j.queue_id = p_queue_id
    and j.state::text = st;
  get diagnostics n = row_count;
  return n;
end;
$fn$;
