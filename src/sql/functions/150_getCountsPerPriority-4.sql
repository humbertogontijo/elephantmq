-- Mirrors ref/bullmq/src/commands/getCountsPerPriority-4.lua

create or replace function :EMQ_SCHEMA.emq_get_counts_per_priority_v1(
  p_queue_id bigint,
  p_priorities int[]
) returns int[]
language plpgsql
stable
as $fn$
declare
  r int[] := array[]::int[];
  i int;
  pr int;
  c bigint;
begin
  for i in 1 .. coalesce(array_length(p_priorities, 1), 0) loop
    pr := p_priorities[i];
    select count(*)::bigint into c
    from :EMQ_SCHEMA.emq_jobs j
    where j.queue_id = p_queue_id
      and j.priority = pr
      and j.state::text in ('wait', 'paused', 'prioritized');
    r := r || c::int;
  end loop;
  return r;
end;
$fn$;
