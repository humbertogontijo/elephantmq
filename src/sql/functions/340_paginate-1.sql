-- Mirrors ref/bullmq/src/commands/paginate-1.lua

create or replace function :EMQ_SCHEMA.emq_paginate_v1(
  p_queue_id bigint,
  p_state text,
  p_offset int,
  p_limit int
) returns table (job_id text, total bigint)
language plpgsql
stable
as $fn$
declare st text;
  tot bigint;
begin
  st := case when p_state in ('waiting','wait') then 'wait' else p_state end;
  select count(*) into tot from :EMQ_SCHEMA.emq_jobs where queue_id = p_queue_id and state::text = st;
  return query
  select j.job_id, tot
  from :EMQ_SCHEMA.emq_jobs j
  where j.queue_id = p_queue_id and j.state::text = st
  order by j.pk
  offset greatest(p_offset, 0)
  limit greatest(p_limit, 1);
end;
$fn$;

drop function if exists :EMQ_SCHEMA.emq_paginate_deps_v1(bigint, text, text, int, int);
create or replace function :EMQ_SCHEMA.emq_paginate_deps_v1(
  p_queue_id bigint,
  p_parent_job_id text,
  p_status text,
  p_offset int,
  p_limit int
) returns table (out_job_id text, out_result text, out_total bigint)
language plpgsql
stable
as $fn$
declare
  ppk bigint;
  tot bigint;
  effective_limit int;
begin
  select j.pk into ppk
  from :EMQ_SCHEMA.emq_jobs j
  where j.queue_id = p_queue_id and j.job_id = p_parent_job_id;
  if ppk is null then
    return;
  end if;

  select count(*) into tot
  from :EMQ_SCHEMA.emq_job_deps d
  where d.parent_pk = ppk and d.status = p_status;

  effective_limit := case when p_limit <= 0 then null::int else p_limit end;

  return query
  select cj.job_id::text,
         cj.return_value::text,
         tot
  from :EMQ_SCHEMA.emq_job_deps d
  join :EMQ_SCHEMA.emq_jobs cj on cj.pk = d.child_pk
  where d.parent_pk = ppk and d.status = p_status
  order by d.child_pk
  offset greatest(p_offset, 0)
  limit effective_limit;
end;
$fn$;
