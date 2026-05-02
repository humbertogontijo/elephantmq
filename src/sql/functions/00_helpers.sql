-- Shared helpers for command functions

-- Clear the `drained` latch (see emq_move_to_active_v1). Called by the
-- add-job paths; keeps `drained` from re-emitting until the queue empties
-- again.
create or replace function :EMQ_SCHEMA.emq_clear_drained_latch_v1(
  p_queue_id bigint
) returns void
language sql
as $fn$
  update :EMQ_SCHEMA.emq_queue_counters
  set drained = false
  where queue_id = p_queue_id and drained = true;
$fn$;

create or replace function :EMQ_SCHEMA.emq_emit_event_v1(
  p_queue_id bigint,
  p_event text,
  p_args jsonb
) returns void
language plpgsql
as $fn$
declare
  v_max_len int;
  v_batch int;
  v_cnt int;
  v_mx bigint;
begin
  -- Skip if the queue row is gone (e.g. obliterated / removeAllQueueData) while a worker
  -- still calls moveToActive with a stale queue_id — avoids FK violation on emq_events.
  insert into :EMQ_SCHEMA.emq_events (queue_id, event, args)
  select p_queue_id, p_event, coalesce(p_args, '{}'::jsonb)
  where exists (select 1 from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id);

  -- Approximate BullMQ's `XADD MAXLEN ~ N`: trim lazily in batches so callers
  -- that are mid-poll still see a contiguous window of events. The final
  -- stream can overshoot `max_len_events` by up to `v_batch` rows — matching
  -- Redis' radix-tree approximation (`~`) tolerance.
  select q.max_len_events into v_max_len
  from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id;

  if v_max_len is not null and v_max_len >= 0 then
    select count(*)::int, max(id) into v_cnt, v_mx
    from :EMQ_SCHEMA.emq_events where queue_id = p_queue_id;
    v_batch := greatest(2, v_max_len / 2);
    if v_cnt > v_max_len + v_batch then
      delete from :EMQ_SCHEMA.emq_events e
      where e.queue_id = p_queue_id
        and e.id <= v_mx - (v_max_len + v_batch);
    end if;
  end if;

  -- Any event other than `drained` means the queue is active again; clear
  -- the latch so the *next* empty poll re-emits `drained`. This matches
  -- BullMQ's semantics of emitting `drained` once per transition.
  if p_event <> 'drained' then
    update :EMQ_SCHEMA.emq_queue_counters
    set drained = false
    where queue_id = p_queue_id and drained = true;
  end if;
end;
$fn$;

-- Allocate the next per-queue wait sequence (LIFO / FIFO ordering key).
-- Replaces a schema-global sequence to avoid cross-queue contention.
create or replace function :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id bigint)
returns bigint
language plpgsql
as $fn$
declare
  v bigint;
begin
  update :EMQ_SCHEMA.emq_queue_counters
  set wait_num = wait_num + 1
  where queue_id = p_queue_id
  returning wait_num into v;

  if v is null then
    insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, job_num, priority_num, wait_num)
    values (p_queue_id, 0, 0, 1)
    on conflict (queue_id) do update
      set wait_num = :EMQ_SCHEMA.emq_queue_counters.wait_num + 1
    returning wait_num into v;
  end if;

  return v;
end;
$fn$;

-- BullMQ include parity: child→parent dependency edges (used by all emq_add_* paths).
create or replace function :EMQ_SCHEMA.emq_link_child_to_parent_v1(
  p_parent_queue_id bigint,
  p_parent_job_id text,
  p_child_queue_id bigint,
  p_child_job_id text
) returns void
language plpgsql
as $fn$
declare
  v_parent_pk bigint;
  v_child_pk bigint;
  v_child_ref text;
  v_child_state text;
  v_child_rv jsonb;
  v_child_fr text;
  v_status text;
  v_parent_prio int;
  v_parent_delay bigint;
  v_parent_state text;
  v_parent_paused boolean;
  v_parent_ps bigint;
  v_parent_qid bigint;
  v_parent_jid text;
begin
  select pk into v_parent_pk from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_parent_queue_id and job_id = p_parent_job_id;
  select pk, state::text, return_value, failed_reason
    into v_child_pk, v_child_state, v_child_rv, v_child_fr
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_child_queue_id and job_id = p_child_job_id;
  if v_parent_pk is null or v_child_pk is null then
    return;
  end if;

  -- Mirror BullMQ's Redis format `<prefix>:<queueName>:<jobId>` so callers
  -- that later inspect getFailedChildrenValues / getIgnoredChildrenFailures
  -- can still identify the child after its job row has been removed (the FK
  -- nulls `child_pk` but `child_ref` is preserved).
  select q.prefix || ':' || q.name || ':' || p_child_job_id
    into v_child_ref
  from :EMQ_SCHEMA.emq_queues q
  where q.id = p_child_queue_id;

  -- Mirror BullMQ's handleDuplicatedJob/updateExistingJobsParent: when a
  -- child is re-attached to a new parent after it has already finished, the
  -- dep row must reflect the terminal outcome so the parent's promotion
  -- check (`status not in ('processed','ignored')`) doesn't leave it stuck.
  if v_child_state = 'completed' then
    v_status := 'processed';
  elsif v_child_state = 'failed' then
    v_status := 'failed';
  else
    v_status := 'pending';
  end if;

  insert into :EMQ_SCHEMA.emq_job_deps (
    parent_pk, child_pk, child_ref, status, return_value, failed_reason,
    resolved_at
  )
  values (
    v_parent_pk, v_child_pk, v_child_ref, v_status,
    case when v_status = 'processed' then v_child_rv else null end,
    case when v_status = 'failed' then v_child_fr else null end,
    case when v_status <> 'pending' then now() else null end
  )
  on conflict (parent_pk, child_pk) where child_pk is not null do nothing;

  -- If the parent is already `waiting-children` and all its dependencies are
  -- resolved (the newly-linked terminal child was the last blocker), promote
  -- it now — mirrors moveParentToWaitIfNoPendingDependencies in Redis.
  if v_status <> 'pending' then
    select p.state::text, p.priority, p.delay_ms
    into v_parent_state, v_parent_prio, v_parent_delay
    from :EMQ_SCHEMA.emq_jobs p
    where p.pk = v_parent_pk;

    if v_parent_state = 'waiting-children'
       and not exists (
         select 1 from :EMQ_SCHEMA.emq_job_deps dd
         where dd.parent_pk = v_parent_pk
           and dd.status not in ('processed', 'ignored')
       )
    then
      if v_parent_delay is not null and v_parent_delay > 0 then
        update :EMQ_SCHEMA.emq_jobs p
        set state = 'delayed':: :EMQ_SCHEMA.emq_job_state,
            process_at = now() + (v_parent_delay::text || ' milliseconds')::interval,
            wait_seq = null,
            prio_seq = null
        where p.pk = v_parent_pk and p.state = 'waiting-children'
        returning p.queue_id, p.job_id into v_parent_qid, v_parent_jid;
        if v_parent_qid is not null then
          perform :EMQ_SCHEMA.emq_emit_event_v1(
            v_parent_qid,
            'delayed',
            jsonb_build_object(
              'jobId', v_parent_jid,
              'delay', (extract(epoch from now()) * 1000)::bigint + v_parent_delay
            )
          );
          -- Skip the generic `waiting` event below for the delayed path.
          v_parent_qid := null;
        end if;
      elsif coalesce(v_parent_prio, 0) > 0 then
        update :EMQ_SCHEMA.emq_queue_counters qc
        set priority_num = qc.priority_num + 1
        where qc.queue_id = p_parent_queue_id
        returning priority_num into v_parent_ps;
        if v_parent_ps is null then
          insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, priority_num)
          values (p_parent_queue_id, 1)
          on conflict (queue_id) do update
            set priority_num = :EMQ_SCHEMA.emq_queue_counters.priority_num + 1
          returning priority_num into v_parent_ps;
        end if;
        update :EMQ_SCHEMA.emq_jobs p
        set state = 'prioritized':: :EMQ_SCHEMA.emq_job_state,
            prio_seq = v_parent_ps,
            wait_seq = null
        where p.pk = v_parent_pk and p.state = 'waiting-children'
        returning p.queue_id, p.job_id into v_parent_qid, v_parent_jid;
      else
        select coalesce(q.paused, false) into v_parent_paused
        from :EMQ_SCHEMA.emq_queues q where q.id = p_parent_queue_id;
        update :EMQ_SCHEMA.emq_jobs p
        set state = (case when coalesce(v_parent_paused, false)
                          then 'paused' else 'wait' end)
                    :: :EMQ_SCHEMA.emq_job_state,
            wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p.queue_id)
        where p.pk = v_parent_pk and p.state = 'waiting-children'
        returning p.queue_id, p.job_id into v_parent_qid, v_parent_jid;
      end if;

      if v_parent_qid is not null then
        perform :EMQ_SCHEMA.emq_emit_event_v1(
          v_parent_qid,
          'waiting',
          jsonb_build_object('jobId', v_parent_jid, 'prev', 'waiting-children')
        );
      end if;
    end if;
  end if;
end;
$fn$;

-- BullMQ include parity: removeParentDependencyKey cascade after hard child removal.
-- Shared helper: given a set of just-deleted child pks, cascade the parent
-- dependency bookkeeping BullMQ performs in `removeParentDependencyKey.lua`.
-- * Same-queue parents whose deps are now empty get deleted too (recursive).
-- * Cross-queue parents whose deps are empty get moved back to `wait`.
drop function if exists :EMQ_SCHEMA.emq_cascade_parent_cleanup_v1(bigint, bigint[]);
drop function if exists :EMQ_SCHEMA.emq_cascade_parent_cleanup_v1(bigint, bigint[], boolean);
create or replace function :EMQ_SCHEMA.emq_cascade_parent_cleanup_v1(
  p_queue_id bigint,
  p_parent_pks bigint[],
  p_emit_waiting_event boolean default true
) returns void
language plpgsql
as $fn$
declare
  rec record;
  remaining int;
  next_parents bigint[] := '{}';
  next_picked_pks bigint[] := '{}';
  v_parent_paused boolean;
begin
  if p_parent_pks is null or array_length(p_parent_pks, 1) is null then
    return;
  end if;

  -- The emq_job_deps.child_pk FK uses `on delete set null`, so rows whose
  -- children have just been hard-removed still linger with status='pending'
  -- and a null child_pk. BullMQ's removeParentDependencyKey.lua treats those
  -- as already resolved (the SREM/SADD of the dependencies set only tracks
  -- live children). Purge them here so the `remaining` count below reflects
  -- only live, outstanding dependencies.
  delete from :EMQ_SCHEMA.emq_job_deps d
   where d.parent_pk = any(p_parent_pks)
     and d.child_pk is null;

  for rec in
    select p.pk, p.queue_id, p.job_id, p.state::text as state
      from :EMQ_SCHEMA.emq_jobs p
     where p.pk = any(p_parent_pks)
  loop
    -- BullMQ's `dependencies` set only contains *unprocessed* children
    -- (successful completion moves them into the `processedSet` hash).
    -- Mirror that by ignoring rows whose status has already graduated to
    -- `processed`; failed/pending rows still represent outstanding deps.
    select count(*) into remaining
      from :EMQ_SCHEMA.emq_job_deps d
     where d.parent_pk = rec.pk
       and d.status <> 'processed';

    if remaining = 0 and rec.state = 'waiting-children' then
      if rec.queue_id = p_queue_id then
        -- Capture this parent's own parents *before* we delete the row so
        -- we can recurse through grandparent chains.
        next_parents := next_parents
          || coalesce(
               (select array_agg(distinct d.parent_pk)
                  from :EMQ_SCHEMA.emq_job_deps d
                 where d.child_pk = rec.pk),
               array[]::bigint[]);
        delete from :EMQ_SCHEMA.emq_jobs where pk = rec.pk;
        next_picked_pks := next_picked_pks || rec.pk;
      else
        -- Respect the parent queue's paused flag: BullMQ's
        -- getTargetQueueList(meta, active, wait, paused) returns the paused
        -- list when meta.paused is set.
        select q.paused into v_parent_paused
          from :EMQ_SCHEMA.emq_queues q
         where q.id = rec.queue_id;
        update :EMQ_SCHEMA.emq_jobs
           set state = case when coalesce(v_parent_paused, false)
                         then 'paused':: :EMQ_SCHEMA.emq_job_state
                         else 'wait':: :EMQ_SCHEMA.emq_job_state
                       end,
               wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(rec.queue_id)
         where pk = rec.pk;
        if p_emit_waiting_event then
          perform :EMQ_SCHEMA.emq_emit_event_v1(
            rec.queue_id,
            'waiting',
            jsonb_build_object('jobId', rec.job_id, 'prev', 'waiting-children')
          );
        end if;
      end if;
    end if;
  end loop;

  if array_length(next_parents, 1) is not null then
    perform :EMQ_SCHEMA.emq_cascade_parent_cleanup_v1(
      p_queue_id, next_parents, p_emit_waiting_event
    );
  end if;
end;
$fn$;

