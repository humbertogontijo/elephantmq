-- Mirrors ref/bullmq/src/commands/extendLocks-1.lua

create or replace function :EMQ_SCHEMA.emq_extend_locks_v1(
  p_queue_id bigint,
  p_job_ids text[],
  p_tokens text[],
  p_duration_ms bigint
) returns text[]
language sql
as $fn$
  with inp as (
    select * from unnest(p_job_ids, p_tokens) as t(job_id, tok)
  ),
  upd as (
    update :EMQ_SCHEMA.emq_jobs j
    set lock_expires_at = now() + (p_duration_ms::text || ' milliseconds')::interval
    from inp
    where j.queue_id = p_queue_id
      and j.job_id = inp.job_id
      and j.lock_token = inp.tok
      and j.state = 'active'
      and j.lock_expires_at is not null
    returning j.job_id
  )
  select coalesce(array_agg(inp.job_id), '{}')
  from inp
  left join upd u on u.job_id = inp.job_id
  where u.job_id is null;
$fn$;
