-- Mirrors ref/bullmq/src/commands/obliterate-2.lua

drop function if exists :EMQ_SCHEMA.emq_obliterate_v1(bigint, boolean, int);
create or replace function :EMQ_SCHEMA.emq_obliterate_v1(
  p_queue_id bigint,
  p_force boolean,
  p_limit int
) returns int
language plpgsql
as $fn$
declare
  v_paused boolean;
  v_active int;
  v_left int;
  lim int;
  picked_pks bigint[];
  parents_touched bigint[];
begin
  select paused into v_paused from :EMQ_SCHEMA.emq_queues where id = p_queue_id;
  if not coalesce(v_paused, false) then
    return -1;
  end if;
  if not p_force then
    select count(*)::int into v_active from :EMQ_SCHEMA.emq_jobs where queue_id = p_queue_id and state = 'active';
    if v_active > 0 then return -2; end if;
  end if;

  lim := greatest(coalesce(p_limit, 1000), 1);

  -- Mirror BullMQ's obliterate-2.lua: iterate the paused/active/delayed/
  -- completed/prioritized/failed lists and remove those jobs. `waiting-children`
  -- parents are intentionally skipped — if their deps live in other queues the
  -- parent row must survive so the child queue's cleanup can still promote it.
  select coalesce(array_agg(pk order by pk), array[]::bigint[])
    into picked_pks
    from (
      select j.pk
      from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id
        and (p_force or j.state <> 'active')
        and j.state <> 'waiting-children'
      order by j.pk
      limit lim
    ) s;

  if array_length(picked_pks, 1) is not null then
    -- Capture cross-queue parents before FK-cascade removes the dep rows.
    select coalesce(array_agg(distinct d.parent_pk), array[]::bigint[])
      into parents_touched
      from :EMQ_SCHEMA.emq_job_deps d
      where d.child_pk = any(picked_pks);

    delete from :EMQ_SCHEMA.emq_jobs where pk = any(picked_pks);

    -- BullMQ's hard-remove path (removeParentDependencyKey with hard=true) does
    -- NOT emit a `waiting` event when it promotes a cross-queue parent, so
    -- pass `false` to suppress the event here. Same-queue parents that lose
    -- their last dep are cascade-deleted by this helper.
    perform :EMQ_SCHEMA.emq_cascade_parent_cleanup_v1(p_queue_id, parents_touched, false);
  end if;

  -- If we hit the per-call limit and there's potentially more work, ask the
  -- caller (Queue.obliterate's do/while loop) to call us again before we do
  -- the final queue-level key cleanup. Matches BullMQ's obliterate-2.lua
  -- `return 1` short-circuit when maxCount reaches zero.
  if array_length(picked_pks, 1) = lim then
    select count(*)::int into v_left
      from :EMQ_SCHEMA.emq_jobs
     where queue_id = p_queue_id
       and (p_force or state <> 'active')
       and state <> 'waiting-children';
    if v_left > 0 then
      return 1;
    end if;
  end if;

  select count(*)::int into v_left from :EMQ_SCHEMA.emq_jobs where queue_id = p_queue_id;

  delete from :EMQ_SCHEMA.emq_deduplication where queue_id = p_queue_id;
  delete from :EMQ_SCHEMA.emq_events where queue_id = p_queue_id;
  delete from :EMQ_SCHEMA.emq_job_schedulers where queue_id = p_queue_id;
  delete from :EMQ_SCHEMA.emq_metrics where queue_id = p_queue_id;
  delete from :EMQ_SCHEMA.emq_rate_limit_state where queue_id = p_queue_id;
  delete from :EMQ_SCHEMA.emq_queue_counters where queue_id = p_queue_id;

  if v_left = 0 then
    -- BullMQ obliterate removes the queue's meta/id/events keys entirely so
    -- subsequent `queue.keys('<prefix>:<name>:*')` returns zero keys. Mirror
    -- that by dropping the queue row itself; it will be re-created lazily on
    -- the next `queue.add()` (ensureQueueRow) with a fresh id.
    delete from :EMQ_SCHEMA.emq_queues where id = p_queue_id;
    return 0;
  end if;

  -- Residual waiting-children parent(s). Keep the queue row (jobs FK requires
  -- it) but flag the queue as obliterated so the Redis compat shim suppresses
  -- meta/id/events. Cleared by the tg_emq_jobs_added_seen trigger as soon as
  -- the queue starts receiving new jobs.
  update :EMQ_SCHEMA.emq_queues
     set obliterated_at = now(),
         job_added_seen = false,
         worker_seen = false
   where id = p_queue_id;

  return 0;
end;
$fn$;
