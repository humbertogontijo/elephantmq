-- Mirrors ref/bullmq/src/commands/getCounts-1.lua

create or replace function :EMQ_SCHEMA.emq_get_counts_v1(p_queue_id bigint, p_types text[])
returns bigint[]
language plpgsql
stable
as $fn$
declare
  r bigint[] := array[]::bigint[];
  i int;
  typ text;
  st text;
  c bigint;
begin
  for i in 1 .. coalesce(array_length(p_types, 1), 0) loop
    typ := p_types[i];
    st := case typ
      when 'waiting' then 'wait'
      when 'wait' then 'wait'
      else typ
    end;
    -- BullMQ: "waiting" counts only the wait list (LLEN). Prioritized jobs are
    -- counted separately via the 'prioritized' ZCARD, and higher-level helpers
    -- (Queue.count / getJobCountByTypes) sum the requested types themselves.
    select count(*)::bigint into c
    from :EMQ_SCHEMA.emq_jobs j
    where j.queue_id = p_queue_id and j.state::text = st;
    r := r || c;
  end loop;
  return r;
end;
$fn$;
