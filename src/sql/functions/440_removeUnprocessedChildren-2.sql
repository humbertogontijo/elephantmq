-- Mirrors ref/bullmq/src/commands/removeUnprocessedChildren-2.lua

-- BullMQ removeUnprocessedChildren-2.lua delegates to removeJobWithChildren
-- with ignoreProcessed=true and ignoreLocked=true. It walks the parent's
-- `:dependencies` set (pending children) depth-first; for each child that is
-- not locked, it recurses into that child's pending deps and then deletes the
-- child job, emitting a `removed` event. Locked children are skipped entirely.
create or replace function :EMQ_SCHEMA.emq_remove_unprocessed_children_by_pk_v1(
  p_parent_pk bigint
) returns void
language plpgsql
as $fn$
declare
  dep record;
  v_prev text;
begin
  for dep in
    select d.child_pk,
           j.queue_id as child_qid,
           j.job_id   as child_jid,
           j.state::text as child_state,
           j.lock_token,
           j.lock_expires_at
    from :EMQ_SCHEMA.emq_job_deps d
    join :EMQ_SCHEMA.emq_jobs j on j.pk = d.child_pk
    where d.parent_pk = p_parent_pk
      and d.status = 'pending'
  loop
    -- Skip locked children (ignoreLocked=true in removeJobWithChildren).
    if dep.lock_token is not null
       and dep.lock_expires_at is not null
       and dep.lock_expires_at > now() then
      continue;
    end if;

    -- Depth-first: strip grandchildren first so parent cascades are clean.
    perform :EMQ_SCHEMA.emq_remove_unprocessed_children_by_pk_v1(dep.child_pk);

    v_prev := dep.child_state;
    delete from :EMQ_SCHEMA.emq_jobs where pk = dep.child_pk;

    perform :EMQ_SCHEMA.emq_emit_event_v1(
      dep.child_qid,
      'removed',
      jsonb_build_object('jobId', dep.child_jid, 'prev', coalesce(v_prev, 'wait'))
    );
  end loop;
end;
$fn$;

create or replace function :EMQ_SCHEMA.emq_remove_unprocessed_children_v1(p_queue_id bigint, p_job_id text)
returns void
language plpgsql
as $fn$
declare jpk bigint;
begin
  select pk into jpk from :EMQ_SCHEMA.emq_jobs where queue_id = p_queue_id and job_id = p_job_id;
  if jpk is null then
    return;
  end if;

  perform :EMQ_SCHEMA.emq_remove_unprocessed_children_by_pk_v1(jpk);
end;
$fn$;
