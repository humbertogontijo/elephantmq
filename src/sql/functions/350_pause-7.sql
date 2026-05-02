-- Mirrors ref/bullmq/src/commands/pause-7.lua

create or replace function :EMQ_SCHEMA.emq_pause_v1(p_queue_id bigint, p_paused boolean)
returns void
language plpgsql
as $fn$
begin
  update :EMQ_SCHEMA.emq_queues set paused = p_paused where id = p_queue_id;
  if p_paused then
    update :EMQ_SCHEMA.emq_jobs set state = 'paused' where queue_id = p_queue_id and state = 'wait';
  else
    update :EMQ_SCHEMA.emq_jobs set state = 'wait' where queue_id = p_queue_id and state = 'paused';
  end if;
  perform :EMQ_SCHEMA.emq_emit_event_v1(p_queue_id, case when p_paused then 'paused' else 'resumed' end, '{}'::jsonb);
end;
$fn$;
