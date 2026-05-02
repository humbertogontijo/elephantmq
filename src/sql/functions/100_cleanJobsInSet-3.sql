-- Mirrors ref/bullmq/src/commands/cleanJobsInSet-3.lua

create or replace function :EMQ_SCHEMA.emq_clean_jobs_in_set_v1(
  p_queue_id bigint,
  p_state text,
  p_before_ms bigint,
  p_limit int
) returns text[]
language plpgsql
as $fn$
declare
  ids text[];
  picked_pks bigint[];
  parents_touched bigint[];
begin
  -- Step 1: collect candidate pks (and surface job_ids) limited by grace period.
  -- BullMQ's `cleanJobsInSet-1.lua` passes the `repeat` ZSET and skips jobs
  -- whose id matches `repeat:<schedulerId>:<next_millis>` for live
  -- schedulers, so `clean` doesn't sweep away the next-iteration row of a
  -- currently-registered job scheduler. Mirror by excluding matching rows
  -- from the candidate set.
  select
    coalesce(array_agg(job_id order by pk), array[]::text[]),
    coalesce(array_agg(pk order by pk), array[]::bigint[])
    into ids, picked_pks
  from (
    select j2.pk, j2.job_id
      from :EMQ_SCHEMA.emq_jobs j2
     where j2.queue_id = p_queue_id
       and j2.state::text = p_state
       and not exists (
         select 1 from :EMQ_SCHEMA.emq_job_schedulers s
         where s.queue_id = p_queue_id
           and j2.job_id = 'repeat:' || s.scheduler_id || ':' || s.next_millis::text
       )
       -- BullMQ cleanJobsInSet: finalized states (`completed`/`failed`) use
       -- `finishedOn`; in-queue states fall back to the job's `timestamp`
       -- (added-at). `delayed` uses `delay`+`timestamp` but we approximate
       -- with the added-at timestamp, which matches BullMQ for gracePeriod=0.
       and (case
              when p_state in ('completed', 'failed')
                then coalesce(j2.finished_on, j2.timestamp, to_timestamp(0))
              else coalesce(j2.timestamp, to_timestamp(0))
            end) <= to_timestamp(p_before_ms / 1000.0)
     order by j2.pk
     limit greatest(coalesce(p_limit, 1000), 1)
  ) s;

  if array_length(picked_pks, 1) is null then
    return coalesce(ids, array[]::text[]);
  end if;

  -- Capture parent pks *before* deleting the children, since FK cascade
  -- removes the `emq_job_deps` rows at the same time.
  select coalesce(array_agg(distinct d.parent_pk), array[]::bigint[])
    into parents_touched
    from :EMQ_SCHEMA.emq_job_deps d
   where d.child_pk = any(picked_pks);

  delete from :EMQ_SCHEMA.emq_jobs
   where pk = any(picked_pks);

  -- Step 2: mirror BullMQ `removeJob(..., hard=true)`. For every parent whose
  -- dependency set just emptied as a consequence of deleting the children
  -- above, drop the parent too (when the parent is in the *same* queue and
  -- currently in `waiting-children`). Otherwise move the parent to `wait`
  -- so that BullMQ's cross-queue semantics are preserved.
  --
  -- BullMQ's `removeParentDependencyKey.lua` with `hard=true` takes the
  -- `_moveParentToWait(parentPrefix, parentId)` branch WITHOUT the third
  -- arg, so `emitEvent` is nil and no `waiting` event is published on the
  -- parent queue (see `tests/clean.test.ts > deletes each children until
  -- trying to move parent to wait` which asserts exactly 2 events: `added`
  -- and `waiting-children`). Pass `false` to match.
  perform :EMQ_SCHEMA.emq_cascade_parent_cleanup_v1(p_queue_id, parents_touched, false);

  -- BullMQ cleanJobsInSet-1.lua emits `cleaned` with {count = <n>} (count is
  -- stringified by XADD). Match that so QueueEvents delivers a stringified
  -- count like Redis does.
  if cardinality(ids) > 0 then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'cleaned',
      jsonb_build_object('count', cardinality(ids)::text)
    );
  end if;
  return coalesce(ids, array[]::text[]);
end;
$fn$;
