-- Mirrors ref/bullmq/src/commands/addParentJob-6.lua

create or replace function :EMQ_SCHEMA.emq_add_parent_job_v1(
  p_queue_id bigint,
  p_custom_job_id text,
  p_name text,
  p_data jsonb,
  p_opts jsonb,
  p_timestamp_ms bigint,
  p_parent jsonb,
  p_repeat_job_key text,
  p_dedup_id text
) returns text
language plpgsql
as $fn$
declare
  v_job_id text;
  v_max_attempts int;
  v_parent_qid bigint;
  v_dedup_existing text;
  v_de_opts jsonb;
  v_ttl_ms bigint;
  v_inserted_id text;
  v_qk text;
  v_pid text;
begin
  v_job_id := nullif(trim(p_custom_job_id), '');
  if v_job_id is null or v_job_id = '' then
    insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, job_num)
    values (p_queue_id, 1)
    on conflict (queue_id) do update
      set job_num = :EMQ_SCHEMA.emq_queue_counters.job_num + 1
    returning job_num::text into v_job_id;
  end if;

  v_qk := nullif(trim(p_parent->>'queueKey'), '');
  v_pid := nullif(trim(p_parent->>'id'), '');

  if v_qk is not null and v_pid is not null then
    if not exists (
      select 1
      from :EMQ_SCHEMA.emq_jobs j
      join :EMQ_SCHEMA.emq_queues q on q.id = j.queue_id
      where j.job_id = v_pid
        and (q.prefix || ':' || q.name) = v_qk
    ) then
      return '-5';
    end if;
  end if;

  if v_qk is not null then
    select q.id into v_parent_qid
    from :EMQ_SCHEMA.emq_queues q
    where (q.prefix || ':' || q.name) = v_qk
    limit 1;
  end if;

  if nullif(trim(p_custom_job_id), '') is not null then
    declare
      v_existing_parent_qid bigint;
      v_existing_parent_jid text;
    begin
      select j.parent_queue_id, j.parent_job_id
      into v_existing_parent_qid, v_existing_parent_jid
      from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id and j.job_id = v_job_id;

      if FOUND then
        if v_pid is not null
           and v_existing_parent_jid is not null
           and (v_existing_parent_jid is distinct from v_pid
                or v_existing_parent_qid is distinct from v_parent_qid)
           and exists (
             select 1 from :EMQ_SCHEMA.emq_jobs p
             where p.queue_id = v_existing_parent_qid
               and p.job_id = v_existing_parent_jid
           )
        then
          return '-7';
        end if;

        if v_parent_qid is not null and v_pid is not null then
          perform :EMQ_SCHEMA.emq_link_child_to_parent_v1(
            v_parent_qid, v_pid, p_queue_id, v_job_id
          );
          update :EMQ_SCHEMA.emq_jobs
          set parent_queue_id = v_parent_qid,
              parent_job_id = v_pid,
              parent_fail_strategy = coalesce(
                p_parent->>'failParentOnFailure',
                parent_fail_strategy
              )
          where queue_id = p_queue_id and job_id = v_job_id;
        end if;

        perform :EMQ_SCHEMA.emq_emit_event_v1(
          p_queue_id,
          'duplicated',
          jsonb_build_object('jobId', v_job_id)
        );
        return v_job_id;
      end if;
    end;
  end if;

  v_de_opts := coalesce(p_opts->'deduplication', p_opts->'de');

  if nullif(trim(p_dedup_id), '') is not null
     and (v_de_opts is null or (v_de_opts->>'replace') is distinct from 'true') then
    select d.job_id into v_dedup_existing
    from :EMQ_SCHEMA.emq_deduplication d
    where d.queue_id = p_queue_id
      and d.dedup_id = p_dedup_id
      and (d.expires_at is null or d.expires_at > now());

    if v_dedup_existing is not null
       and exists (
         select 1 from :EMQ_SCHEMA.emq_jobs j
         where j.queue_id = p_queue_id and j.job_id = v_dedup_existing
       ) then
      v_ttl_ms := coalesce(nullif(trim(v_de_opts->>'ttl'), '')::bigint, 0);
      if (v_de_opts->>'extend') = 'true' and v_ttl_ms > 0 then
        update :EMQ_SCHEMA.emq_deduplication d
        set expires_at = now() + (v_ttl_ms::text || ' milliseconds')::interval
        where d.queue_id = p_queue_id
          and d.dedup_id = p_dedup_id
          and d.job_id = v_dedup_existing;
      end if;
      perform :EMQ_SCHEMA.emq_emit_event_v1(
        p_queue_id,
        'deduplicated',
        jsonb_build_object(
          'jobId', v_dedup_existing,
          'deduplicationId', p_dedup_id,
          'deduplicatedJobId', v_job_id
        )
      );
      return v_dedup_existing;
    end if;
  end if;

  if nullif(trim(p_dedup_id), '') is not null then
    update :EMQ_SCHEMA.emq_jobs j
    set deduplication_id = null
    where j.queue_id = p_queue_id
      and j.deduplication_id = p_dedup_id
      and not exists (
        select 1
        from :EMQ_SCHEMA.emq_deduplication d
        where d.queue_id = p_queue_id
          and d.dedup_id = p_dedup_id
          and d.job_id = j.job_id
          and (d.expires_at is null or d.expires_at > now())
      );
  end if;

  v_max_attempts := coalesce((p_opts->>'attempts')::int, 3);

  -- Preserve the caller-supplied `priority`/`delay` on the parent-for-flow
  -- row so it is honoured when moveParentToWait promotes the job out of
  -- `waiting-children` once its own children finish (BullMQ's
  -- moveParentToWait reads `HMGET priority delay`).
  insert into :EMQ_SCHEMA.emq_jobs (
    queue_id, job_id, name, data, opts, state, priority, delay_ms, timestamp, max_attempts,
    repeat_job_key, deduplication_id, parent_job_id, parent_fail_strategy, parent_queue_id
  )
  values (
    p_queue_id, v_job_id, p_name, coalesce(p_data, '{}'::jsonb), coalesce(p_opts, '{}'::jsonb),
    'waiting-children':: :EMQ_SCHEMA.emq_job_state,
    coalesce((p_opts->>'priority')::int, 0),
    coalesce((p_opts->>'delay')::int, 0),
    to_timestamp(p_timestamp_ms / 1000.0), v_max_attempts,
    p_repeat_job_key, nullif(p_dedup_id, ''),
    v_pid, p_parent->>'failParentOnFailure', v_parent_qid
  )
  on conflict (queue_id, job_id) do nothing
  returning job_id into v_inserted_id;

  if v_inserted_id is null then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'duplicated',
      jsonb_build_object('jobId', v_job_id)
    );
    return v_job_id;
  end if;

  if nullif(trim(p_dedup_id), '') is not null then
    v_ttl_ms := coalesce(nullif(trim(v_de_opts->>'ttl'), '')::bigint, 0);
    insert into :EMQ_SCHEMA.emq_deduplication (queue_id, dedup_id, job_id, expires_at)
    values (
      p_queue_id,
      p_dedup_id,
      v_job_id,
      case
        when v_ttl_ms > 0 then now() + (v_ttl_ms::text || ' milliseconds')::interval
        else null
      end
    )
    on conflict (queue_id, dedup_id) do update set
      job_id = excluded.job_id,
      expires_at = excluded.expires_at;
  end if;

  -- Mirror BullMQ's addParentJob-6.lua: storeJob emits `added` and the
  -- parent-specific path then emits `waiting-children`. Both events need to
  -- land on this queue's stream so QueueEvents subscribers (and tests) see
  -- the same footprint as BullMQ.
  perform :EMQ_SCHEMA.emq_emit_event_v1(
    p_queue_id,
    'added',
    jsonb_build_object('jobId', v_job_id, 'name', p_name)
  );
  perform :EMQ_SCHEMA.emq_emit_event_v1(
    p_queue_id,
    'waiting-children',
    jsonb_build_object('jobId', v_job_id)
  );

  -- Mirror the addJob path (emq_add_job_v1): when this job itself has a
  -- parent reference (opts.parent.id/queue), register the dependency so the
  -- `v_child_parents` lookup in emq_move_to_finished_v1 can cascade failures
  -- (e.g. fpof / cpof / idof / rdof) up the chain. Previously only direct
  -- children added via emq_add_job_v1 created the emq_job_deps edge, so
  -- multi-level flows (grandchild → child → root) lost fpof propagation when
  -- the middle node was created as a parent-job-for-flow.
  if v_parent_qid is not null and v_pid is not null then
    perform :EMQ_SCHEMA.emq_link_child_to_parent_v1(
      v_parent_qid,
      v_pid,
      p_queue_id,
      v_job_id
    );
  end if;

  return v_job_id;
end;
$fn$;
