-- Mirrors ref/bullmq/src/commands/moveStalledJobsToWait-8.lua

create or replace function :EMQ_SCHEMA.emq_move_stalled_jobs_to_wait_v1(
  p_queue_id bigint,
  p_max_stalled int
) returns table (recovered_ids text[], failed_ids text[])
language plpgsql
as $fn$
declare
  r1 text[];
  v_jid text;
  v_paused boolean;
  v_count int;
  v_base bigint;
begin
  -- Serialize with moveToActive/moveToFinished per queue so our state changes
  -- don't race (mirrors BullMQ's single-threaded Lua stalled sweep).
  -- Schema-scoped key (see emq_move_to_active_v1 for rationale) so two
  -- parallel test schemas with overlapping queue_ids do not serialize
  -- their stalled sweeps globally.
  perform pg_advisory_xact_lock(
    hashtextextended(:EMQ_SCHEMA_NAME_LIT, 2024000001::bigint) # p_queue_id
  );

  select q.paused into v_paused
  from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id;

  -- Count stalled candidates up-front and reserve a contiguous range of
  -- wait_seq values so every row gets a distinct, increasing sequence. We
  -- cannot call emq_next_wait_seq_v1() per-row inside the UPDATE because
  -- PostgreSQL evaluates it independently for each target row and the
  -- evaluation order of the CTE is not guaranteed to match the rn order,
  -- which previously produced ties and non-deterministic pickup.
  select count(*)
  into v_count
  from :EMQ_SCHEMA.emq_jobs j
  where j.queue_id = p_queue_id
    and j.state = 'active'
    and j.lock_expires_at < now();

  if v_count = 0 then
    return query select array[]::text[], array[]::text[];
    return;
  end if;

  update :EMQ_SCHEMA.emq_queue_counters
  set wait_num = wait_num + v_count
  where queue_id = p_queue_id
  returning wait_num - v_count into v_base;

  -- Assign new wait_seqs in reverse-pk order so the most-recently-active
  -- stalled job is picked first next round. BullMQ's moveStalledJobsToWait-8
  -- pushes recovered jobs back with `RPUSH` and moveToActive uses
  -- `RPOPLPUSH`, yielding LIFO-of-stall-batch ordering. Tests (removeOnFail
  -- trimming / parent-child propagation) depend on this ordering.
  with ranked as (
    select j.pk, j.job_id,
           row_number() over (order by j.pk desc) as rn
    from :EMQ_SCHEMA.emq_jobs j
    where j.queue_id = p_queue_id
      and j.state = 'active'
      and j.lock_expires_at < now()
  ),
  u as (
    update :EMQ_SCHEMA.emq_jobs j
    set state = case when coalesce(v_paused, false)
          then 'paused':: :EMQ_SCHEMA.emq_job_state
          else 'wait':: :EMQ_SCHEMA.emq_job_state
        end,
        wait_seq = v_base + r.rn,
        stalled_counter = j.stalled_counter + 1,
        deferred_failure = case
          when j.stalled_counter + 1 > p_max_stalled
               and (j.opts->'repeat') is null
          then 'job stalled more than allowable limit'
          else j.deferred_failure
        end,
        lock_token = null,
        locked_by = null,
        locked_at = null,
        lock_expires_at = null
    from ranked r
    where j.pk = r.pk
    returning j.job_id, r.rn
  )
  select coalesce(array_agg(job_id order by rn), array[]::text[]) into r1 from u;

  -- Emit queue-events stream rows for each recovered job so QueueEvents
  -- subscribers receive `stalled` events like BullMQ's Lua does via XADD.
  if r1 is not null then
    foreach v_jid in array r1 loop
      perform :EMQ_SCHEMA.emq_emit_event_v1(
        p_queue_id, 'stalled',
        jsonb_build_object('jobId', v_jid)
      );
    end loop;
  end if;

  -- For API parity we still return a split (recovered / failed) tuple, but
  -- the deferred-failure path means the actual `failed` event is emitted
  -- later from the worker when it next picks the job up. Callers today only
  -- use the first element, so an empty failed array is safe.
  return query select coalesce(r1, array[]::text[]), array[]::text[];
end;
$fn$;
