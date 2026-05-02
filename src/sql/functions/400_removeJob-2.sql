-- Mirrors ref/bullmq/src/commands/removeJob-2.lua

create or replace function :EMQ_SCHEMA.emq_remove_job_v1(
  p_queue_id bigint,
  p_job_id text,
  p_remove_children boolean
) returns int
language plpgsql
as $fn$
declare
  n int;
  v_locked boolean;
  v_root_pk bigint;
  v_rjk text;
  v_next_millis bigint;
begin
  select pk, repeat_job_key into v_root_pk, v_rjk
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and job_id = p_job_id;
  if v_root_pk is null then
    return 0;
  end if;

  -- BullMQ removeJob-2.lua rejects (-8) removing the current "next iteration"
  -- delayed job spawned by a job scheduler. Mirror that: if this job carries
  -- a repeat_job_key and its id matches `repeat:<rjk>:<nextMillis>` from the
  -- active scheduler entry, bail with JobBelongsToJobScheduler (-8). The
  -- `removeJobScheduler` / `emq_remove_job_scheduler_v1` is the supported way
  -- to delete that row.
  if v_rjk is not null then
    select next_millis into v_next_millis
    from :EMQ_SCHEMA.emq_job_schedulers
    where queue_id = p_queue_id and scheduler_id = v_rjk;
    if v_next_millis is not null
       and p_job_id = 'repeat:' || v_rjk || ':' || v_next_millis::text then
      return -8;
    end if;
  end if;

  -- BullMQ removeJob-2.lua refuses to remove a locked root. When
  -- removeChildren=true, it also refuses if any descendant (reachable via
  -- pending deps) is locked. Mirror that check; active jobs with live locks
  -- must not be silently dropped from under a running worker.
  if p_remove_children then
    with recursive subtree as (
      select j.pk, j.lock_token, j.lock_expires_at
      from :EMQ_SCHEMA.emq_jobs j
      where j.pk = v_root_pk
      union
      select j2.pk, j2.lock_token, j2.lock_expires_at
      from :EMQ_SCHEMA.emq_jobs j2
      inner join :EMQ_SCHEMA.emq_job_deps d on d.child_pk = j2.pk
      inner join subtree s on s.pk = d.parent_pk
      where d.status = 'pending'
    )
    select exists (
      select 1 from subtree s
      where s.lock_token is not null
        and s.lock_expires_at is not null
        and s.lock_expires_at > now()
    ) into v_locked;
  else
    select (lock_token is not null
            and lock_expires_at is not null
            and lock_expires_at > now())
    into v_locked
    from :EMQ_SCHEMA.emq_jobs
    where pk = v_root_pk;
  end if;

  if v_locked then
    return 0;
  end if;

  declare
    v_affected_parents bigint[];
    v_ppk bigint;
    v_pqid bigint;
    v_pjid text;
    v_pprio int;
    v_pdelay int;
    v_pstate text;
    v_paused boolean;
    v_ppriosq bigint;
  begin
    if p_remove_children then
      with recursive subtree as (
        select pk from :EMQ_SCHEMA.emq_jobs where pk = v_root_pk
        union all
        select j.pk from :EMQ_SCHEMA.emq_jobs j
        inner join :EMQ_SCHEMA.emq_job_deps d on d.child_pk = j.pk
        inner join subtree s on s.pk = d.parent_pk
      )
      select array_agg(distinct d.parent_pk)
      into v_affected_parents
      from :EMQ_SCHEMA.emq_job_deps d
      where d.child_pk in (select pk from subtree)
        and d.parent_pk not in (select pk from subtree);

      delete from :EMQ_SCHEMA.emq_jobs
      where pk in (
        with recursive subtree as (
          select pk from :EMQ_SCHEMA.emq_jobs where pk = v_root_pk
          union all
          select j.pk from :EMQ_SCHEMA.emq_jobs j
          inner join :EMQ_SCHEMA.emq_job_deps d on d.child_pk = j.pk
          inner join subtree s on s.pk = d.parent_pk
        )
        select pk from subtree
      );
    else
      select array_agg(distinct d.parent_pk)
      into v_affected_parents
      from :EMQ_SCHEMA.emq_job_deps d
      where d.child_pk = v_root_pk;

      delete from :EMQ_SCHEMA.emq_jobs where pk = v_root_pk;
    end if;
    get diagnostics n = row_count;

    -- Drop dep rows that now point at deleted children. FK is ON DELETE SET
    -- NULL, so the rows survive as dangling entries; the parent-promotion
    -- check below must not count them as pending.
    delete from :EMQ_SCHEMA.emq_job_deps where child_pk is null;

    -- BullMQ removeParentDependencyKey: every parent that had a pending dep
    -- on a removed child must be promoted out of `waiting-children` if no
    -- pending deps remain (respecting its own delay/priority/paused state).
    if v_affected_parents is not null then
      foreach v_ppk in array v_affected_parents loop
        if v_ppk is null then
          continue;
        end if;
        select p.state::text, p.priority, p.delay_ms, p.queue_id, p.job_id
        into v_pstate, v_pprio, v_pdelay, v_pqid, v_pjid
        from :EMQ_SCHEMA.emq_jobs p where p.pk = v_ppk;
        if v_pstate is null then
          continue;
        end if;
        if v_pstate <> 'waiting-children' then
          continue;
        end if;
        if exists (
          select 1 from :EMQ_SCHEMA.emq_job_deps dd
          where dd.parent_pk = v_ppk
            and dd.status not in ('processed', 'ignored')
        ) then
          continue;
        end if;

        if v_pdelay is not null and v_pdelay > 0 then
          update :EMQ_SCHEMA.emq_jobs p
          set state = 'delayed':: :EMQ_SCHEMA.emq_job_state,
              process_at = now() + (v_pdelay::text || ' milliseconds')::interval,
              wait_seq = null,
              prio_seq = null
          where p.pk = v_ppk and p.state = 'waiting-children';
          perform :EMQ_SCHEMA.emq_emit_event_v1(
            v_pqid,
            'delayed',
            jsonb_build_object(
              'jobId', v_pjid,
              'delay', (extract(epoch from now()) * 1000)::bigint + v_pdelay
            )
          );
        elsif coalesce(v_pprio, 0) > 0 then
          update :EMQ_SCHEMA.emq_queue_counters qc
          set priority_num = qc.priority_num + 1
          where qc.queue_id = v_pqid
          returning priority_num into v_ppriosq;
          if v_ppriosq is null then
            insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, priority_num)
            values (v_pqid, 1)
            on conflict (queue_id) do update
              set priority_num = :EMQ_SCHEMA.emq_queue_counters.priority_num + 1
            returning priority_num into v_ppriosq;
          end if;
          update :EMQ_SCHEMA.emq_jobs p
          set state = 'prioritized':: :EMQ_SCHEMA.emq_job_state,
              prio_seq = v_ppriosq,
              wait_seq = null
          where p.pk = v_ppk and p.state = 'waiting-children';
          perform :EMQ_SCHEMA.emq_emit_event_v1(
            v_pqid,
            'waiting',
            jsonb_build_object('jobId', v_pjid, 'prev', 'waiting-children')
          );
        else
          select coalesce(q.paused, false) into v_paused
          from :EMQ_SCHEMA.emq_queues q where q.id = v_pqid;
          update :EMQ_SCHEMA.emq_jobs p
          set state = (case when coalesce(v_paused, false)
                            then 'paused' else 'wait' end)
                      :: :EMQ_SCHEMA.emq_job_state,
              wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p.queue_id)
          where p.pk = v_ppk and p.state = 'waiting-children';
          perform :EMQ_SCHEMA.emq_emit_event_v1(
            v_pqid,
            'waiting',
            jsonb_build_object('jobId', v_pjid, 'prev', 'waiting-children')
          );
        end if;
      end loop;
    end if;
  end;

  return n;
end;
$fn$;
