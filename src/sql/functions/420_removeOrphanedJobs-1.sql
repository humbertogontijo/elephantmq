-- Mirrors ref/bullmq/src/commands/removeOrphanedJobs-1.lua

create or replace function :EMQ_SCHEMA.emq_remove_orphaned_jobs_v1(p_queue_id bigint, p_job_ids text[])
returns text[]
language plpgsql
as $fn$
declare
  orphaned text[];
begin
  if p_job_ids is null or array_length(p_job_ids, 1) is null then
    return array[]::text[];
  end if;

  -- A candidate is orphan if either: it has no matching row at all, OR it has
  -- a row with `state IS NULL` (injected via the Redis compat shim to mimic a
  -- raw BullMQ hash with no state list membership). Rows in an actual state
  -- set (`wait`, `active`, etc.) are legitimate and must be preserved.
  with candidates as (
    select unnest(p_job_ids) as job_id
  ),
  resolved as (
    select c.job_id,
           j.pk,
           j.state
      from candidates c
      left join :EMQ_SCHEMA.emq_jobs j
        on j.queue_id = p_queue_id and j.job_id = c.job_id
  ),
  to_delete as (
    select pk from resolved where pk is not null and state is null
  ),
  del as (
    delete from :EMQ_SCHEMA.emq_jobs
    where pk in (select pk from to_delete)
    returning 1
  ),
  counted as (select count(*) as n from del),
  orphan_ids as (
    select array_agg(job_id) as ids
      from resolved
     where state is null
  )
  select coalesce(ids, array[]::text[]) into orphaned from orphan_ids, counted;
  return coalesce(orphaned, array[]::text[]);
end;
$fn$;
