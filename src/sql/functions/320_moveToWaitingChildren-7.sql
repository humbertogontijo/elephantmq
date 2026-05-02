-- Mirrors ref/bullmq/src/commands/moveToWaitingChildren-7.lua

drop function if exists :EMQ_SCHEMA.emq_move_to_waiting_children_v1(bigint, text, text);
drop function if exists :EMQ_SCHEMA.emq_move_to_waiting_children_v1(bigint, text, text, text);

-- Mirror moveToWaitingChildren-7.lua return codes. The Lua script first
-- checks for failed children (-9), then either targets a specific child via
-- `child` or requires at least one pending dep; if the move is eligible it
-- calls `removeLock` + `LREM active`. Matching error codes let
-- `scripts.moveToWaitingChildren` raise the right BullMQ error:
--   0  → moved to waiting-children
--   1  → not moved (no pending deps / specified child not in deps)
--  -1  → missing job
--  -2  → missing lock  (JobLockNotExist)
--  -3  → not in active state  (JobNotInState)
--  -6  → lock exists but token mismatch  (JobLockMismatch)
--  -9  → job has failed children  (JobHasFailedChildren)
create or replace function :EMQ_SCHEMA.emq_move_to_waiting_children_v1(
  p_queue_id bigint,
  p_job_id text,
  p_token text,
  p_child_key text default null
) returns int
language plpgsql
as $fn$
declare
  jpk bigint;
  v_state text;
  v_lock text;
  v_failed int;
  v_pending int;
  v_specific int;
  v_child_job_id text;
  v_child_prefix text;
  v_child_qname text;
  v_child_qid bigint;
  v_child_pk bigint;
begin
  select pk, state::text, lock_token
    into jpk, v_state, v_lock
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and job_id = p_job_id;
  if jpk is null then
    return -1;
  end if;

  select count(*)::int into v_failed
  from :EMQ_SCHEMA.emq_job_deps d
  where d.parent_pk = jpk and d.status = 'failed';
  if v_failed > 0 then
    return -9;
  end if;

  -- Parse the optional `child` key (format: `<prefix>:<queueName>:<jobId>`)
  -- and require it to live in this parent's pending deps, mirroring the
  -- `SISMEMBER jobDependenciesKey <childKey>` branch.
  if p_child_key is not null and p_child_key <> '' then
    declare
      v_last_colon int;
      v_without_jid text;
      v_last_colon2 int;
    begin
      v_last_colon := position(':' in reverse(p_child_key));
      if v_last_colon = 0 then
        return 1;
      end if;
      v_child_job_id := substring(p_child_key from length(p_child_key) - v_last_colon + 2);
      v_without_jid := substring(p_child_key from 1 for length(p_child_key) - v_last_colon);
      v_last_colon2 := position(':' in reverse(v_without_jid));
      if v_last_colon2 = 0 then
        return 1;
      end if;
      v_child_qname := substring(v_without_jid from length(v_without_jid) - v_last_colon2 + 2);
      v_child_prefix := substring(v_without_jid from 1 for length(v_without_jid) - v_last_colon2);
    end;

    select id into v_child_qid from :EMQ_SCHEMA.emq_queues
    where prefix = v_child_prefix and name = v_child_qname;
    if v_child_qid is null then
      return 1;
    end if;
    select pk into v_child_pk from :EMQ_SCHEMA.emq_jobs
    where queue_id = v_child_qid and job_id = v_child_job_id;
    if v_child_pk is null then
      return 1;
    end if;
    select count(*)::int into v_specific
    from :EMQ_SCHEMA.emq_job_deps d
    where d.parent_pk = jpk and d.child_pk = v_child_pk and d.status = 'pending';
    if v_specific = 0 then
      return 1;
    end if;
  else
    select count(*)::int into v_pending
    from :EMQ_SCHEMA.emq_job_deps d
    where d.parent_pk = jpk and d.status = 'pending';
    if v_pending = 0 then
      return 1;
    end if;
  end if;

  -- removeLock branch: when token is "0" BullMQ skips the lock check but
  -- still requires the job to be in the active list.
  if p_token <> '0' then
    if v_lock is null then
      return -2;
    end if;
    if v_lock <> p_token then
      return -6;
    end if;
  end if;

  if v_state <> 'active' then
    return -3;
  end if;

  update :EMQ_SCHEMA.emq_jobs
  set state = 'waiting-children',
      lock_token = null,
      locked_by = null,
      locked_at = null,
      lock_expires_at = null
  where pk = jpk;

  -- BullMQ moveToWaitingChildren-7.lua emits a `waiting-children` event with
  -- `prev = 'active'` after the state transition so QueueEvents subscribers
  -- can observe parent jobs pausing for their dependencies.
  perform :EMQ_SCHEMA.emq_emit_event_v1(
    p_queue_id,
    'waiting-children',
    jsonb_build_object('jobId', p_job_id, 'prev', 'active')
  );

  return 0;
end;
$fn$;
