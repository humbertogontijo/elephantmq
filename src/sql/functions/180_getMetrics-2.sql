-- Mirrors ref/bullmq/src/commands/getMetrics-2.lua

-- emq_get_metrics_v1 mirrors BullMQ's getMetrics.lua: returns a (meta, data,
-- count) triple. `meta` is a 3-element text[] [count, prevTS, prevCount],
-- `data` is a text[] slice of the per-minute delta list (newest first), and
-- `count` is the slice length. Using arrays as out columns keeps the client
-- script's destructuring simple and matches the Redis command's shape.
drop function if exists :EMQ_SCHEMA.emq_get_metrics_v1(bigint, text, int, int);
create or replace function :EMQ_SCHEMA.emq_get_metrics_v1(
  p_queue_id bigint,
  p_metric text,
  p_start int,
  p_end int
) returns table (meta text[], data text[], cnt int)
language plpgsql
stable
as $fn$
declare
  v_meta text[];
  v_arr jsonb;
  v_len int;
  v_start int;
  v_end int;
  v_sliced jsonb;
  v_data text[];
begin
  select array[coalesce(m.count, 0)::text,
               coalesce(m.prev_ts, 0)::text,
               coalesce(m.prev_count, 0)::text],
         coalesce(m.data, '[]'::jsonb)
  into v_meta, v_arr
  from :EMQ_SCHEMA.emq_metrics m
  where m.queue_id = p_queue_id and m.metric = p_metric;

  if v_meta is null then
    -- No metrics row yet: behave like Redis (empty hash → all zeros).
    v_meta := array['0', '0', '0']::text[];
    v_arr := '[]'::jsonb;
  end if;

  v_len := jsonb_array_length(v_arr);

  -- Normalize negative indices (Redis LRANGE semantics).
  v_start := case when p_start < 0 then greatest(v_len + p_start, 0) else p_start end;
  v_end := case when p_end < 0 then v_len + p_end else p_end end;

  if v_len = 0 or v_start > v_end or v_start >= v_len then
    v_data := array[]::text[];
  else
    if v_end >= v_len then
      v_end := v_len - 1;
    end if;
    select coalesce(array_agg(value order by idx), array[]::text[])
    into v_data
    from (
      select (v_arr->>i)::text as value, i as idx
      from generate_series(v_start, v_end) as i
    ) s;
  end if;

  return query select v_meta, v_data, coalesce(array_length(v_data, 1), 0);
end;
$fn$;
