-- Mirrors ref/bullmq/src/commands/removeChildDependency-1.lua

drop function if exists :EMQ_SCHEMA.emq_remove_child_dependency_v1(bigint, text, text);
drop function if exists :EMQ_SCHEMA.emq_remove_child_dependency_v1(bigint, text, text, text);
create or replace function :EMQ_SCHEMA.emq_remove_child_dependency_v1(
  p_child_queue_id bigint,
  p_child_job_id text,
  p_parent_prefix text,
  p_parent_queue_name text,
  p_parent_job_id text
) returns int
language plpgsql
as $fn$
declare
  ppk bigint;
  cpk bigint;
  parent_qid bigint;
  deleted_count int;
  remaining int;
  v_parent_paused boolean;
  v_parent_state text;
begin
  -- Mirror BullMQ's removeChildDependency-1.lua return codes:
  --   -1 : child job missing
  --   -5 : parent job missing
  --    1 : there was no relationship to break
  --    0 : relationship removed; parent may move to wait if it was last child.
  select pk into cpk from :EMQ_SCHEMA.emq_jobs
    where queue_id = p_child_queue_id and job_id = p_child_job_id;
  if cpk is null then return -1; end if;

  select id into parent_qid from :EMQ_SCHEMA.emq_queues
    where prefix = p_parent_prefix and name = p_parent_queue_name;
  if parent_qid is null then return -5; end if;

  select pk, state::text into ppk, v_parent_state from :EMQ_SCHEMA.emq_jobs
    where queue_id = parent_qid and job_id = p_parent_job_id;
  if ppk is null then return -5; end if;

  delete from :EMQ_SCHEMA.emq_job_deps
    where parent_pk = ppk and child_pk = cpk;
  get diagnostics deleted_count = row_count;
  if deleted_count = 0 then return 1; end if;

  update :EMQ_SCHEMA.emq_jobs
     set parent_pk = null
   where pk = cpk;

  select count(*)::int into remaining from :EMQ_SCHEMA.emq_job_deps
    where parent_pk = ppk and status = 'pending';

  if remaining = 0 and v_parent_state = 'waiting-children' then
    select q.paused into v_parent_paused from :EMQ_SCHEMA.emq_queues q
      where q.id = parent_qid;
    update :EMQ_SCHEMA.emq_jobs
       set state = (case when coalesce(v_parent_paused, false) then 'paused' else 'wait' end):: :EMQ_SCHEMA.emq_job_state,
           wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(parent_qid)
     where pk = ppk;
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      parent_qid, 'waiting',
      jsonb_build_object('jobId', p_parent_job_id, 'prev', 'waiting-children')
    );
  end if;

  return 0;
end;
$fn$;
