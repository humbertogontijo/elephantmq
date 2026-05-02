-- Mirrors ref/bullmq/src/commands/getDependencyCounts-4.lua

create or replace function :EMQ_SCHEMA.emq_get_dependency_counts_v1(p_queue_id bigint, p_job_id text)
returns table (
  processed bigint,
  unprocessed bigint,
  ignored bigint,
  failed bigint
)
language plpgsql
stable
as $fn$
declare
  jpk bigint;
  pr bigint;
  u bigint;
  ig bigint;
  fa bigint;
begin
  select pk into jpk from :EMQ_SCHEMA.emq_jobs where queue_id = p_queue_id and job_id = p_job_id;
  if jpk is null then
    return query select 0::bigint, 0::bigint, 0::bigint, 0::bigint;
    return;
  end if;
  select
    count(*) filter (where d.status = 'processed')::bigint,
    count(*) filter (where d.status = 'pending')::bigint,
    count(*) filter (where d.status = 'ignored')::bigint,
    count(*) filter (where d.status = 'failed')::bigint
  into pr, u, ig, fa
  from :EMQ_SCHEMA.emq_job_deps d
  where d.parent_pk = jpk;
  return query select coalesce(pr, 0), coalesce(u, 0), coalesce(ig, 0), coalesce(fa, 0);
end;
$fn$;
