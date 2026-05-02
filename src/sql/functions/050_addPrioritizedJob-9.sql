-- Mirrors ref/bullmq/src/commands/addPrioritizedJob-9.lua

create or replace function :EMQ_SCHEMA.emq_add_prioritized_job_v1(
  p_queue_id bigint,
  p_custom_job_id text,
  p_name text,
  p_data jsonb,
  p_opts jsonb,
  p_timestamp_ms bigint,
  p_parent_key text default null,
  p_parent_dep_key text default null,
  p_parent jsonb default null,
  p_repeat_job_key text default null,
  p_dedup_id text default null
) returns text
language plpgsql
as $fn$
declare
  v_job_id text;
  v_paused boolean;
  v_prio_seq bigint;
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

  update :EMQ_SCHEMA.emq_queue_counters
  set priority_num = priority_num + 1
  where queue_id = p_queue_id
  returning priority_num into v_prio_seq;

  if v_prio_seq is null then
    insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, priority_num)
    values (p_queue_id, 1)
    on conflict (queue_id) do update
      set priority_num = :EMQ_SCHEMA.emq_queue_counters.priority_num + 1
    returning priority_num into v_prio_seq;
  end if;

  v_prio := coalesce((p_opts->>'priority')::int, 0);
  v_delay := coalesce((p_opts->>'delay')::bigint, 0);
  v_max_attempts := coalesce((p_opts->>'attempts')::int, 3);

  insert into :EMQ_SCHEMA.emq_jobs (
    queue_id, job_id, name, data, opts, state, priority, prio_seq, delay_ms, timestamp, max_attempts,
    repeat_job_key, deduplication_id, parent_job_id, parent_fail_strategy, parent_queue_id
  )
  values (
    p_queue_id, v_job_id, p_name, coalesce(p_data, '{}'::jsonb), coalesce(p_opts, '{}'::jsonb),
    -- BullMQ keeps prioritized jobs in the `prioritized` zset even while the
    -- queue is paused (pause only migrates the `wait` list). moveToActive
    -- gates on `q.paused`, not on job state, so the zset stays intact for
    -- resume.
    'prioritized':: :EMQ_SCHEMA.emq_job_state,
    v_prio, v_prio_seq, v_delay, to_timestamp(p_timestamp_ms / 1000.0), v_max_attempts,
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
