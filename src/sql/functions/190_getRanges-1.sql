-- Mirrors ref/bullmq/src/commands/getRanges-1.lua

create or replace function :EMQ_SCHEMA.emq_get_ranges_v1(
  p_queue_id bigint,
  p_state text,
  p_offset int,
  p_limit int,
  p_asc boolean default true
) returns text[]
language plpgsql
stable
as $fn$
declare
  st text;
  ids text[];
begin
  st := case
    when p_state in ('waiting', 'wait') then 'wait'
    when p_state = 'waiting-children' then 'waiting-children'
    else p_state
  end;

  if st = 'wait' then
    -- Match Redis LRANGE on the `wait` list. Prioritized jobs live in their
    -- own zset (see state='prioritized'); they must NOT be folded into the
    -- wait list or getJobs(['waiting']) would double-count them alongside a
    -- separate getJobs(['prioritized']) call.
    if p_asc then
      select coalesce(
        array(
          select j.job_id
          from :EMQ_SCHEMA.emq_jobs j
          where j.queue_id = p_queue_id and j.state::text = 'wait'
          order by j.wait_seq asc nulls last
          offset greatest(p_offset, 0)
          limit greatest(p_limit, 0)
        ),
        array[]::text[]
      ) into ids;
    else
      select coalesce(
        array(
          select j.job_id
          from :EMQ_SCHEMA.emq_jobs j
          where j.queue_id = p_queue_id and j.state::text = 'wait'
          order by j.wait_seq desc nulls last
          offset greatest(p_offset, 0)
          limit greatest(p_limit, 0)
        ),
        array[]::text[]
      ) into ids;
    end if;
  elsif st = 'prioritized' then
    -- Redis prioritized zset is scored by (priority << 32) + counter; ZRANGE
    -- therefore surfaces lower-priority buckets first, then FIFO inside each
    -- bucket. Mirror that with (priority, wait_seq) ordering.
    if p_asc then
      select coalesce(
        array(
          select j.job_id
          from :EMQ_SCHEMA.emq_jobs j
          where j.queue_id = p_queue_id and j.state::text = 'prioritized'
          order by j.priority asc nulls last, j.wait_seq asc nulls last, j.pk asc
          offset greatest(p_offset, 0)
          limit greatest(p_limit, 0)
        ),
        array[]::text[]
      ) into ids;
    else
      select coalesce(
        array(
          select j.job_id
          from :EMQ_SCHEMA.emq_jobs j
          where j.queue_id = p_queue_id and j.state::text = 'prioritized'
          order by j.priority desc nulls last, j.wait_seq desc nulls last, j.pk desc
          offset greatest(p_offset, 0)
          limit greatest(p_limit, 0)
        ),
        array[]::text[]
      ) into ids;
    end if;
  elsif p_asc then
    select coalesce(
      array(
        select j.job_id
        from :EMQ_SCHEMA.emq_jobs j
        where j.queue_id = p_queue_id and j.state::text = st
        order by j.pk asc
        offset greatest(p_offset, 0)
        limit greatest(p_limit, 0)
      ),
      array[]::text[]
    ) into ids;
  else
    select coalesce(
      array(
        select j.job_id
        from :EMQ_SCHEMA.emq_jobs j
        where j.queue_id = p_queue_id and j.state::text = st
        order by j.pk desc
        offset greatest(p_offset, 0)
        limit greatest(p_limit, 0)
      ),
      array[]::text[]
    ) into ids;
  end if;

  return coalesce(ids, array[]::text[]);
end;
$fn$;
