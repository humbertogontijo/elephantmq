-- Mirrors ref/bullmq/src/commands/addStandardJob-9.lua

create or replace function :EMQ_SCHEMA.emq_add_standard_job_v1(
  p_queue_id bigint,
  p_custom_job_id text,
  p_name text,
  p_data jsonb,
  p_opts jsonb,
  p_timestamp_ms bigint,
  p_parent_key text,
  p_parent_dep_key text,
  p_parent jsonb,
  p_repeat_job_key text,
  p_dedup_id text
) returns text
language plpgsql
as $fn$
declare
  v_job_id text;
  v_paused boolean;
  v_wait_seq bigint;
  v_min_ws bigint;
  v_prio int;
  v_delay bigint;
  v_max_attempts int;
  v_parent_qid bigint;
  v_dedup_existing text;
  v_de_opts jsonb;
  v_ttl_ms bigint;
  v_inserted_id text;
  v_qk text;
  v_pid text;
begin
  select paused into v_paused from :EMQ_SCHEMA.emq_queues where id = p_queue_id;
  if v_paused is null then
    raise exception 'queue not found';
  end if;

  v_job_id := nullif(trim(p_custom_job_id), '');
  if v_job_id is null or v_job_id = '' then
    insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, job_num)
    values (p_queue_id, 1)
    on conflict (queue_id) do update
      set job_num = :EMQ_SCHEMA.emq_queue_counters.job_num + 1
    returning job_num::text into v_job_id;
  end if;

  -- Parent must exist (BullMQ EXISTS(parentKey)).
  v_qk := nullif(trim(p_parent->>'queueKey'), '');
  v_pid := nullif(trim(p_parent->>'id'), '');
  if v_qk is null and v_pid is null and nullif(trim(p_parent_key), '') is not null then
    v_pid := split_part(p_parent_key, ':', array_length(string_to_array(p_parent_key, ':'), 1));
    v_qk := array_to_string(
      (string_to_array(p_parent_key, ':'))[1:array_length(string_to_array(p_parent_key, ':'), 1) - 1],
      ':'
    );
  end if;

  if nullif(trim(p_parent_key), '') is not null then
    if v_qk is null or v_pid is null then
      return '-5';
    end if;
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

  -- Custom job id already exists (handleDuplicatedJob).
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
        -- BullMQ handleDuplicatedJob: if a different parent is specified and
        -- the existing parent still exists in Redis, reject with -7
        -- ("parent cannot be replaced"). If same parent (or no old parent),
        -- re-link transparently.
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

  -- Deduplication short-circuit (non-replace path).
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
      -- BullMQ setDeduplicationKey on extend: refresh PX TTL so later adds still hit GET.
      v_ttl_ms := coalesce(nullif(trim(v_de_opts->>'ttl'), '')::bigint, 0);
      if (v_de_opts->>'extend') = 'true' and v_ttl_ms > 0 then
        update :EMQ_SCHEMA.emq_deduplication d
        set expires_at = now() + (v_ttl_ms::text || ' milliseconds')::interval
        where d.queue_id = p_queue_id
          and d.dedup_id = p_dedup_id
          and d.job_id = v_dedup_existing;
      end if;
      -- keepLastIfActive: stash the new job's (name, data, opts) so
      -- emq_move_to_finished_v1 can requeue it once the active holder
      -- completes or fails permanently. Only one pending slot — last add wins.
      if (v_de_opts->>'keepLastIfActive') = 'true'
         and exists (
           select 1 from :EMQ_SCHEMA.emq_jobs j
           where j.queue_id = p_queue_id
             and j.job_id = v_dedup_existing
             and j.state = 'active':: :EMQ_SCHEMA.emq_job_state
         ) then
        update :EMQ_SCHEMA.emq_deduplication d
        set keep_last_if_active = true,
            pending_name = p_name,
            pending_data = coalesce(p_data, '{}'::jsonb),
            pending_opts = coalesce(p_opts, '{}'::jsonb),
            expires_at = null
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

  -- replace: remove prior wait/paused job for this dedup id so the new job can be inserted
  -- (mirror addDelayedJob replace path; avoids emq_jobs_dedup_uniq when job_id changes).
  if nullif(trim(p_dedup_id), '') is not null
     and v_de_opts is not null
     and (v_de_opts->>'replace') = 'true' then
    select d.job_id into v_dedup_existing
    from :EMQ_SCHEMA.emq_deduplication d
    where d.queue_id = p_queue_id
      and d.dedup_id = p_dedup_id;

    if v_dedup_existing is not null then
      delete from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id
        and j.job_id = v_dedup_existing
        and j.state in (
          'wait':: :EMQ_SCHEMA.emq_job_state,
          'paused':: :EMQ_SCHEMA.emq_job_state
        );

      if found then
        delete from :EMQ_SCHEMA.emq_deduplication d
        where d.queue_id = p_queue_id
          and d.dedup_id = p_dedup_id
          and d.job_id = v_dedup_existing;
        perform :EMQ_SCHEMA.emq_emit_event_v1(
          p_queue_id,
          'deduplicated',
          jsonb_build_object(
            'jobId', v_job_id,
            'deduplicationId', p_dedup_id,
            'deduplicatedJobId', v_dedup_existing
          )
        );
      else
        return v_dedup_existing;
      end if;
    end if;
  end if;

  -- Debounce TTL expiry: emq_deduplication no longer counts as active, but emq_jobs may
  -- still hold deduplication_id — clear it so a new job with the same id can be inserted
  -- without violating emq_jobs_dedup_uniq (matches Redis debounce key expiry).
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

  -- LIFO: new jobs jump to the head of the wait queue. Serialize via advisory lock
  -- to avoid two inserts picking the same "min - 1" value.
  -- Schema-scoped key: see the rationale comment in
  -- emq_move_to_active_v1. Without including the schema in the hash,
  -- two parallel test schemas with overlapping queue_ids serialize
  -- their LIFO inserts globally.
  if coalesce((p_opts->>'lifo')::boolean, false) then
    perform pg_advisory_xact_lock(
      hashtextextended(
        'emq_lifo_wait_seq:' || :EMQ_SCHEMA_NAME_LIT || ':' || p_queue_id::text,
        0::bigint
      )
    );
    select min(j.wait_seq) into v_min_ws
    from :EMQ_SCHEMA.emq_jobs j
    where j.queue_id = p_queue_id
      and j.state in ('wait':: :EMQ_SCHEMA.emq_job_state, 'paused':: :EMQ_SCHEMA.emq_job_state);
    if v_min_ws is null then
      v_wait_seq := :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id);
    else
      v_wait_seq := v_min_ws - 1;
    end if;
  else
    v_wait_seq := :EMQ_SCHEMA.emq_next_wait_seq_v1(p_queue_id);
  end if;
  v_prio := coalesce((p_opts->>'priority')::int, 0);
  v_delay := coalesce((p_opts->>'delay')::bigint, 0);
  v_max_attempts := coalesce((p_opts->>'attempts')::int, 3);

  insert into :EMQ_SCHEMA.emq_jobs (
    queue_id, job_id, name, data, opts, state, priority, wait_seq, delay_ms,
    timestamp, max_attempts, repeat_job_key, deduplication_id,
    parent_job_id, parent_fail_strategy, parent_queue_id
  )
  values (
    p_queue_id, v_job_id, p_name, coalesce(p_data, '{}'::jsonb), coalesce(p_opts, '{}'::jsonb),
    case when v_paused
      then 'paused':: :EMQ_SCHEMA.emq_job_state
      else 'wait':: :EMQ_SCHEMA.emq_job_state
    end,
    v_prio, v_wait_seq, v_delay,
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
    v_ttl_ms := coalesce(
      nullif(trim(v_de_opts->>'ttl'), '')::bigint,
      0
    );
    insert into :EMQ_SCHEMA.emq_deduplication (
      queue_id, dedup_id, job_id, expires_at, keep_last_if_active
    )
    values (
      p_queue_id,
      p_dedup_id,
      v_job_id,
      case
        when (v_de_opts->>'keepLastIfActive') = 'true' then null
        when v_ttl_ms > 0 then now() + (v_ttl_ms::text || ' milliseconds')::interval
        else null
      end,
      coalesce((v_de_opts->>'keepLastIfActive')::boolean, false)
    )
    on conflict (queue_id, dedup_id) do update set
      job_id = excluded.job_id,
      expires_at = excluded.expires_at,
      keep_last_if_active = excluded.keep_last_if_active,
      pending_name = null,
      pending_data = null,
      pending_opts = null;
  end if;

  perform :EMQ_SCHEMA.emq_emit_event_v1(
    p_queue_id,
    'added',
    jsonb_build_object('jobId', v_job_id, 'name', p_name)
  );
  perform :EMQ_SCHEMA.emq_emit_event_v1(
    p_queue_id,
    'waiting',
    jsonb_build_object('jobId', v_job_id)
  );

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
