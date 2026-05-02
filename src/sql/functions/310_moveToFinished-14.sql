-- Mirrors ref/bullmq/src/commands/moveToFinished-14.lua

drop function if exists :EMQ_SCHEMA.emq_move_to_finished_v1(bigint, text, text, text, jsonb, text, text[], boolean, bigint, int, bigint);
drop function if exists :EMQ_SCHEMA.emq_move_to_finished_v1(bigint, text, text, text, jsonb, text, text[], boolean, bigint, int, bigint, bigint, bigint);
drop function if exists :EMQ_SCHEMA.emq_move_to_finished_v1(bigint, text, text, text, jsonb, text, text[], boolean, bigint, int, bigint, bigint, bigint, bigint);

-- emq_collect_metrics_v1 mirrors BullMQ's collectMetrics.lua:
--   * cumulative count is incremented by 1 on every call
--   * per-minute deltas are prepended to `data` when we cross a minute
--     boundary (relative to prev_ts)
--   * the list is truncated to p_max_data_points
--   * prev_ts/prev_count are bumped so the next invocation can compute the
--     correct delta
-- Returning `void` keeps the caller (moveToFinished) unburdened; callers that
-- want to inspect the state should use emq_get_metrics_v1 directly.
create or replace function :EMQ_SCHEMA.emq_collect_metrics_v1(
  p_queue_id bigint,
  p_metric text,
  p_timestamp bigint,
  p_max_data_points int
) returns void
language plpgsql
as $fn$
declare
  v_count bigint;
  v_prev_ts bigint;
  v_prev_count bigint;
  v_data jsonb;
  v_n int;
  v_delta bigint;
  v_zeros jsonb;
  i int;
begin
  -- Upsert + HINCRBY semantics: first call inserts a row with count=1.
  insert into :EMQ_SCHEMA.emq_metrics (queue_id, metric, count)
    values (p_queue_id, p_metric, 1)
  on conflict (queue_id, metric) do update
    set count = :EMQ_SCHEMA.emq_metrics.count + 1
  returning count - 1, prev_ts, prev_count, data
  into v_count, v_prev_ts, v_prev_count, v_data;

  if v_prev_ts is null then
    -- First observation: seed prev_ts/prev_count like BullMQ's first-run path.
    update :EMQ_SCHEMA.emq_metrics
    set prev_ts = p_timestamp, prev_count = 0
    where queue_id = p_queue_id and metric = p_metric;
    return;
  end if;

  v_n := least(
    floor(p_timestamp::numeric / 60000) - floor(v_prev_ts::numeric / 60000),
    p_max_data_points
  )::int;

  if v_n > 0 then
    v_delta := v_count - v_prev_count;

    if v_n > 1 then
      -- BullMQ does a single LPUSH(dataPointsList, delta, 0, 0, ...0): Redis
      -- places the last arg at the head, so the resulting prefix ends up as
      -- `[0, 0, ..., delta, ...previous]`. Preserve that exact layout by
      -- prepending the zeros _first_ and then the delta.
      v_zeros := '[]'::jsonb;
      for i in 1..(v_n - 1) loop
        v_zeros := v_zeros || to_jsonb(0);
      end loop;
      v_data := v_zeros || (to_jsonb(v_delta) || v_data);
    else
      v_data := to_jsonb(v_delta) || v_data;
    end if;

    -- LTRIM 0, maxDataPoints - 1
    if jsonb_array_length(v_data) > p_max_data_points then
      select jsonb_agg(value)
      into v_data
      from (
        select value
        from jsonb_array_elements(v_data) with ordinality as t(value, idx)
        where idx <= p_max_data_points
      ) s;
    end if;

    update :EMQ_SCHEMA.emq_metrics
    set data = coalesce(v_data, '[]'::jsonb),
        prev_count = v_count,
        prev_ts = p_timestamp
    where queue_id = p_queue_id and metric = p_metric;
  end if;
end;
$fn$;

create or replace function :EMQ_SCHEMA.emq_move_to_finished_v1(
  p_queue_id bigint,
  p_job_id text,
  p_token text,
  p_target text,
  p_return_value jsonb,
  p_failed_reason text,
  p_stacktrace text[],
  p_fetch_next boolean,
  p_lock_duration_ms bigint,
  p_keep_jobs_count int,
  p_keep_age_ms bigint,
  p_limiter_max bigint default null,
  p_limiter_duration_ms bigint default null,
  -- BullMQ passes `timestamp` (usually Date.now()) in moveToFinished-14.lua;
  -- tests that install `sinon.useFakeTimers({ toFake: ['Date', ...] })` rely on
  -- that fake clock for both `finished_on` and age-based trim windows.
  -- Postgres `now()` always returns the real wall clock, so keep_age_ms windows
  -- would never match against fake-Date finished_on values. When callers pass
  -- `p_now_ms`, we use it consistently for `finished_on` writes and the
  -- age-trim comparison; otherwise we fall back to `now()`.
  p_now_ms bigint default null,
  -- Mirrors the `maxMetricsSize` argv in moveToFinished-14.lua: when set the
  -- function increments the completed/failed metrics rolling window for this
  -- queue (matching BullMQ's collectMetrics). A null/0 value skips collection.
  p_max_metrics_size int default null
) returns table (
  err_code int,
  finished_job_id text,
  next_job_row jsonb,
  next_job_id text,
  rate_limit_delay_ms int,
  block_until_ms bigint
)
language plpgsql
as $fn$
declare
  j :EMQ_SCHEMA.emq_jobs;
  v_parent bigint;
  v_fin text;
  v_now timestamptz;
  v_child_parents bigint[];
begin
  v_now := case
    when p_now_ms is not null then to_timestamp(p_now_ms / 1000.0)
    else now()
  end;

  -- Serialize with concurrent moveToActive/moveToFinished calls on the
  -- same queue so the fetch-next step picks jobs in strict wait_seq
  -- order (mirrors BullMQ's single-threaded Lua semantics). See the
  -- matching comment in emq_move_to_active_v1.
  -- IMPORTANT: must use the SAME schema-scoped lock key as
  -- emq_move_to_active_v1 — otherwise (a) parallel-test schemas with
  -- overlapping queue_ids serialize globally on the old constant key
  -- pair, and (b) within a single session the keys would diverge,
  -- breaking the intended "fetch-next inherits the lock" semantics.
  perform pg_advisory_xact_lock(
    hashtextextended(:EMQ_SCHEMA_NAME_LIT, 2024000001::bigint) # p_queue_id
  );

  select * into j from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and job_id = p_job_id;
  if not found then
    -- Row gone (e.g. removeOnComplete trim): idempotent completed ack; failed still errors.
    if p_target = 'completed' then
      return query select 0, p_job_id, null::jsonb, null::text, 0, 0::bigint;
    else
      return query select -1, null::text, null::jsonb, null::text, 0, 0::bigint;
    end if;
    return;
  end if;

  v_fin := case when p_target = 'completed' then 'completed' else 'failed' end;

  -- Idempotent: job already finalized with the same outcome (double moveToCompleted/moveToFailed).
  -- Do not run move_to_active again here (avoids duplicate next-job fetch).
  if j.state::text = v_fin then
    return query select 0, p_job_id, null::jsonb, null::text, 0, 0::bigint;
    return;
  end if;

  -- BullMQ special-case: token '0' bypasses the lock check entirely (see
  -- ref/bullmq/src/commands/includes/removeLock.lua). This lets callers like
  -- Job.moveToFailed(err, '0') finalize a job without holding the worker's
  -- active lock (e.g. for direct failure bookkeeping in tests and stalled
  -- retry flows).
  if p_token is distinct from '0' then
    -- BullMQ's removeLock.lua distinguishes "lock is missing" (-2) from "lock
    -- token does not match" (-6). When the lock row has been cleared (by a
    -- stalled-check / expiry) we fall into the first case; when another
    -- worker still holds it under a different token we fall into the second.
    if j.lock_token is null or j.lock_expires_at is null or j.lock_expires_at <= now() then
      return query select -2, null::text, null::jsonb, null::text, 0, 0::bigint;
      return;
    end if;
    if j.lock_token is distinct from p_token then
      return query select -6, null::text, null::jsonb, null::text, 0, 0::bigint;
      return;
    end if;
  end if;

  if p_target = 'completed' then
    if exists (
      select 1 from :EMQ_SCHEMA.emq_job_deps d
      where d.parent_pk = j.pk and d.status = 'pending'
    ) then
      return query select -4, null::text, null::jsonb, null::text, 0, 0::bigint;
      return;
    end if;
    if exists (
      select 1 from :EMQ_SCHEMA.emq_job_deps d
      where d.parent_pk = j.pk and d.status = 'failed'
    ) then
      return query select -9, null::text, null::jsonb, null::text, 0, 0::bigint;
      return;
    end if;
  end if;

  if p_target = 'completed' then
    update :EMQ_SCHEMA.emq_jobs
    set state = 'completed',
        finished_on = v_now,
        return_value = p_return_value,
        lock_token = null,
        locked_by = null,
        locked_at = null,
        lock_expires_at = null,
        attempts_made = attempts_made + 1
    where pk = j.pk;
  else
    update :EMQ_SCHEMA.emq_jobs
    set state = 'failed',
        finished_on = v_now,
        failed_reason = p_failed_reason,
        stacktrace = coalesce(p_stacktrace, stacktrace),
        lock_token = null,
        locked_by = null,
        locked_at = null,
        lock_expires_at = null,
        attempts_made = attempts_made + 1,
        -- Clear any backoff delay left over from the previous retry pass so
        -- `job.delay` reports 0 on the final, permanent failure. Matches
        -- BullMQ moveToFailed-6.lua behaviour (`HSET delay 0` on terminal
        -- failure in v5).
        delay_ms = 0,
        -- Clear the deferred-failure marker so this row is no longer treated
        -- as "unrecoverable" on any subsequent reprocess (mirrors the
        -- `HDEL ... defa` in moveToFinished-14.lua).
        deferred_failure = null
    where pk = j.pk;
  end if;

  -- keepLastIfActive: if a pending next job was stashed while this job was
  -- active, requeue it now (name/data/opts from the dedup row). The add
  -- function will overwrite the dedup row to point to the new job.
  declare
    v_pend_dedup text;
    v_pend_name text;
    v_pend_data jsonb;
    v_pend_opts jsonb;
    v_pend_delay int;
    v_pend_prio int;
    v_pend_ts bigint;
  begin
    select d.dedup_id, d.pending_name, d.pending_data, d.pending_opts
    into v_pend_dedup, v_pend_name, v_pend_data, v_pend_opts
    from :EMQ_SCHEMA.emq_deduplication d
    where d.queue_id = p_queue_id
      and d.job_id = p_job_id
      and d.keep_last_if_active = true
      and d.pending_data is not null;

    if v_pend_dedup is not null then
      -- Drop the old row so the add helper can re-insert a fresh one pointing
      -- at the newly-created job (with cleared pending_* fields).
      delete from :EMQ_SCHEMA.emq_deduplication d
      where d.queue_id = p_queue_id
        and d.dedup_id = v_pend_dedup
        and d.job_id = p_job_id;

      v_pend_delay := coalesce((v_pend_opts->>'delay')::int, 0);
      v_pend_prio := coalesce((v_pend_opts->>'priority')::int, 0);
      v_pend_ts := (extract(epoch from now()) * 1000)::bigint;

      if v_pend_delay > 0 then
        perform :EMQ_SCHEMA.emq_add_delayed_job_v1(
          p_queue_id, '', coalesce(v_pend_name, ''),
          coalesce(v_pend_data, '{}'::jsonb),
          coalesce(v_pend_opts, '{}'::jsonb),
          v_pend_ts, null, null, null, null, v_pend_dedup
        );
      elsif v_pend_prio > 0 then
        perform :EMQ_SCHEMA.emq_add_prioritized_job_v1(
          p_queue_id, '', coalesce(v_pend_name, ''),
          coalesce(v_pend_data, '{}'::jsonb),
          coalesce(v_pend_opts, '{}'::jsonb),
          v_pend_ts, null, null, null, null, v_pend_dedup
        );
      else
        perform :EMQ_SCHEMA.emq_add_standard_job_v1(
          p_queue_id, '', coalesce(v_pend_name, ''),
          coalesce(v_pend_data, '{}'::jsonb),
          coalesce(v_pend_opts, '{}'::jsonb),
          v_pend_ts, null, null, null::jsonb, null, v_pend_dedup
        );
      end if;
    else
      -- Debounce/dedupe without TTL: remove table row when the job finishes.
      -- Keep rows with expires_at set (BullMQ: finishing does not clear the
      -- TTL'd debounce key).
      delete from :EMQ_SCHEMA.emq_deduplication d
      where d.queue_id = p_queue_id
        and d.job_id = p_job_id
        and d.expires_at is null;
    end if;
  end;

  -- Capture the parent PKs BEFORE mutating deps so the subsequent
  -- promotion loop still has a list to walk (rdof deletes the edge).
  -- Using DISTINCT: in practice a child has a single parent but the schema
  -- allows N:M, so this keeps semantics correct.
  select coalesce(array_agg(distinct d.parent_pk), array[]::bigint[])
  into v_child_parents
  from :EMQ_SCHEMA.emq_job_deps d
  where d.child_pk = j.pk;

  -- Apply per-option parent-reaction when the child fails, mirroring
  -- BullMQ moveChildFromDependenciesIfNeeded.lua:
  --   * fpof: parent fails (we set `deferred_failure` so the next pickup
  --           funnels through handleFailed)
  --   * cpof: parent unconditionally moves to wait; this failed child is
  --           recorded so Job#getFailedChildrenValues returns it
  --   * idof: failed dep is treated as 'ignored' (does NOT block parent
  --           promotion) and the failure reason is stored so
  --           getIgnoredChildrenFailures surfaces it
  --   * rdof: dep row is deleted outright
  -- Completed always → 'processed'. Failed without any option → 'failed'
  -- (parent remains blocked, same as the prior behaviour).
  if p_target = 'completed' then
    -- Store the return_value on the dep row so getDependencies().processed
    -- survives `removeOnComplete` (mirrors Redis <parent>:processed hash).
    update :EMQ_SCHEMA.emq_job_deps d
    set status = 'processed',
        resolved_at = now(),
        return_value = p_return_value
    where d.child_pk = j.pk;
  else
    -- rdof: delete the edge entirely.
    delete from :EMQ_SCHEMA.emq_job_deps d
    where d.child_pk = j.pk
      and coalesce((j.opts->>'rdof')::boolean, false) = true;

    -- idof: mark as ignored, save failedReason.
    update :EMQ_SCHEMA.emq_job_deps d
    set status = 'ignored',
        failed_reason = p_failed_reason,
        resolved_at = now()
    where d.child_pk = j.pk
      and coalesce((j.opts->>'idof')::boolean, false) = true;

    -- cpof: mark as ignored (so it does not block promotion), save
    -- failedReason for getFailedChildrenValues.
    update :EMQ_SCHEMA.emq_job_deps d
    set status = 'ignored',
        failed_reason = p_failed_reason,
        resolved_at = now()
    where d.child_pk = j.pk
      and coalesce((j.opts->>'cpof')::boolean, false) = true;

    -- When the child job row is about to be removed (`removeOnFail: true`,
    -- i.e. keepJobsCount = 0) and no fpof/cpof/idof/rdof option steered the
    -- dep, detach the edge entirely. BullMQ's moveToFinished calls
    -- `removeParentDependencyKey` in this branch, which SREMs the child from
    -- the parent's `:dependencies` set and promotes the parent to wait once
    -- the set is empty. Deleting the dep row mirrors that behaviour under
    -- our `emq_job_deps` model.
    if p_keep_jobs_count is not null and p_keep_jobs_count = 0 then
      delete from :EMQ_SCHEMA.emq_job_deps d
      using :EMQ_SCHEMA.emq_jobs jj
      where d.child_pk = j.pk
        and d.resolved_at is null
        and jj.pk = j.pk
        and not coalesce((jj.opts->>'fpof')::boolean, false);
    end if;

    -- Everything else (including fpof, which we handle below) → 'failed'.
    update :EMQ_SCHEMA.emq_job_deps d
    set status = 'failed',
        failed_reason = p_failed_reason,
        resolved_at = now()
    where d.child_pk = j.pk
      and d.resolved_at is null;
  end if;

  if p_target = 'completed' then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id, 'completed',
      jsonb_build_object(
        'jobId', p_job_id,
        'returnvalue', coalesce(p_return_value, 'null'::jsonb),
        'prev', 'active'
      )
    );
  else
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id, 'failed',
      jsonb_build_object(
        'jobId', p_job_id,
        'failedReason', p_failed_reason,
        'prev', 'active'
      )
    );
  end if;

  -- Mirror BullMQ's collectMetrics side-effect: when the worker is configured
  -- with `metrics.maxDataPoints`, bump the completed/failed rolling-window
  -- metrics state using the caller's timestamp. Must happen here rather than
  -- in a follow-up query so the metrics are atomic with the terminal state
  -- transition (mirrors moveToFinished-14.lua).
  if p_max_metrics_size is not null and p_max_metrics_size > 0 then
    perform :EMQ_SCHEMA.emq_collect_metrics_v1(
      p_queue_id,
      case when p_target = 'completed' then 'completed' else 'failed' end,
      coalesce(p_now_ms, (extract(epoch from now()) * 1000)::bigint),
      p_max_metrics_size
    );
  end if;

  -- BullMQ emits `retries-exhausted` when the job reaches the failed terminal
  -- state after having attempted at least once and burned through all retries.
  if p_target <> 'completed' and j.max_attempts > 0 then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'retries-exhausted',
      jsonb_build_object(
        'jobId', p_job_id,
        'attemptsMade', j.attempts_made + 1
      )
    );
  end if;

  -- fpof: arm the parent with a deferred-failure marker so its next pick
  -- is routed through handleFailed. This mirrors BullMQ's `HSET defa =
  -- "child <key> failed"` and the subsequent moveParentToWait.
  if p_target <> 'completed'
     and coalesce((j.opts->>'fpof')::boolean, false)
     and array_length(v_child_parents, 1) is not null then
    declare
      v_parent_pk bigint;
      v_parent_qid bigint;
      v_parent_jid text;
      v_child_qname text;
      v_child_prefix text;
    begin
      select q.prefix, q.name into v_child_prefix, v_child_qname
      from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id;

      foreach v_parent_pk in array v_child_parents loop
        declare
          v_parent_prev_state text;
          v_parent_paused boolean;
        begin
          -- Mirror BullMQ's handleChildFailureAndMoveParentToWait:
          --   * If parent is `waiting-children` or `delayed`, move it to
          --     `wait` (or `paused` if the queue is paused) and emit a
          --     `waiting` event so the worker picks it up and the
          --     deferred-failure path funnels the job through handleFailed.
          --   * Otherwise (prioritized/wait/active/…): only arm the
          --     deferred-failure marker. The job will be caught on next pick.
          select p.state::text into v_parent_prev_state
            from :EMQ_SCHEMA.emq_jobs p where p.pk = v_parent_pk;
          select q.paused into v_parent_paused
            from :EMQ_SCHEMA.emq_queues q,
                 :EMQ_SCHEMA.emq_jobs p
           where p.pk = v_parent_pk and q.id = p.queue_id;

          if v_parent_prev_state in ('waiting-children', 'delayed') then
            update :EMQ_SCHEMA.emq_jobs p
            set deferred_failure = 'child ' || v_child_prefix || ':' || v_child_qname || ':' || p_job_id || ' failed',
                state = (case when coalesce(v_parent_paused, false)
                              then 'paused' else 'wait' end)
                        :: :EMQ_SCHEMA.emq_job_state,
                wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p.queue_id),
                delay_ms = case when v_parent_prev_state = 'delayed'
                                then 0 else p.delay_ms end,
                process_at = case when v_parent_prev_state = 'delayed'
                                  then null else p.process_at end
            where p.pk = v_parent_pk
            returning p.queue_id, p.job_id into v_parent_qid, v_parent_jid;

            if v_parent_qid is not null then
              perform :EMQ_SCHEMA.emq_emit_event_v1(
                v_parent_qid,
                'waiting',
                jsonb_build_object('jobId', v_parent_jid, 'prev', v_parent_prev_state)
              );
            end if;
          else
            update :EMQ_SCHEMA.emq_jobs p
            set deferred_failure = 'child ' || v_child_prefix || ':' || v_child_qname || ':' || p_job_id || ' failed'
            where p.pk = v_parent_pk;
          end if;
        end;
      end loop;
    end;
  end if;

  if array_length(v_child_parents, 1) is not null then
    foreach v_parent in array v_child_parents loop
      declare
        v_parent_qid bigint;
        v_parent_jid text;
        v_cpof boolean;
        v_parent_prio int;
        v_parent_delay int;
        v_parent_paused boolean;
        v_parent_ps bigint;
        v_should_move boolean;
        v_cur_state text;
      begin
        v_cpof := (p_target <> 'completed')
                and coalesce((j.opts->>'cpof')::boolean, false);

        -- Snapshot the parent row before we change its state so we can
        -- apply the priority/delay-aware promotion below.
        select p.priority, p.delay_ms, p.state::text
        into v_parent_prio, v_parent_delay, v_cur_state
        from :EMQ_SCHEMA.emq_jobs p where p.pk = v_parent;

        if v_cur_state = 'waiting-children' then
          v_should_move := v_cpof
            or not exists (
              select 1 from :EMQ_SCHEMA.emq_job_deps dd
              where dd.parent_pk = v_parent
                and dd.status not in ('processed', 'ignored')
            );
        else
          v_should_move := false;
        end if;

        if v_should_move then
          -- Mirror BullMQ moveParentToWait: respect the parent's own
          -- priority/delay when choosing the destination state.
          if v_parent_delay is not null and v_parent_delay > 0 then
            update :EMQ_SCHEMA.emq_jobs p
            set state = 'delayed':: :EMQ_SCHEMA.emq_job_state,
                process_at = v_now + (v_parent_delay::text || ' milliseconds')::interval,
                wait_seq = null,
                prio_seq = null
            where p.pk = v_parent
              and p.state = 'waiting-children'
            returning p.queue_id, p.job_id into v_parent_qid, v_parent_jid;
            if v_parent_qid is not null then
              perform :EMQ_SCHEMA.emq_emit_event_v1(
                v_parent_qid,
                'delayed',
                jsonb_build_object(
                  'jobId', v_parent_jid,
                  'delay', (extract(epoch from v_now) * 1000)::bigint + v_parent_delay
                )
              );
              v_parent_qid := null;
            end if;
          elsif coalesce(v_parent_prio, 0) > 0 then
            -- Bump the queue's priority counter to assign a strictly
            -- increasing prio_seq (head-of-same-priority -> FIFO per prio).
            update :EMQ_SCHEMA.emq_queue_counters qc
            set priority_num = qc.priority_num + 1
            where qc.queue_id = (select p.queue_id from :EMQ_SCHEMA.emq_jobs p where p.pk = v_parent)
            returning priority_num into v_parent_ps;
            if v_parent_ps is null then
              insert into :EMQ_SCHEMA.emq_queue_counters (queue_id, priority_num)
              select p.queue_id, 1 from :EMQ_SCHEMA.emq_jobs p where p.pk = v_parent
              on conflict (queue_id) do update
                set priority_num = :EMQ_SCHEMA.emq_queue_counters.priority_num + 1
              returning priority_num into v_parent_ps;
            end if;
            update :EMQ_SCHEMA.emq_jobs p
            set state = 'prioritized':: :EMQ_SCHEMA.emq_job_state,
                prio_seq = v_parent_ps,
                wait_seq = null
            where p.pk = v_parent
              and p.state = 'waiting-children'
            returning p.queue_id, p.job_id into v_parent_qid, v_parent_jid;
          else
            select coalesce(q.paused, false) into v_parent_paused
            from :EMQ_SCHEMA.emq_queues q,
                 :EMQ_SCHEMA.emq_jobs p
            where p.pk = v_parent and q.id = p.queue_id;

            update :EMQ_SCHEMA.emq_jobs p
            set state = (case when coalesce(v_parent_paused, false)
                              then 'paused' else 'wait' end)
                        :: :EMQ_SCHEMA.emq_job_state,
                wait_seq = :EMQ_SCHEMA.emq_next_wait_seq_v1(p.queue_id)
            where p.pk = v_parent
              and p.state = 'waiting-children'
            returning p.queue_id, p.job_id into v_parent_qid, v_parent_jid;
          end if;
        end if;

        if v_parent_qid is not null then
          -- Parent transitioned from `waiting-children` → `wait`; BullMQ emits
          -- `waiting` on the parent's queue with `prev = 'waiting-children'`.
          perform :EMQ_SCHEMA.emq_emit_event_v1(
            v_parent_qid,
            'waiting',
            jsonb_build_object('jobId', v_parent_jid, 'prev', 'waiting-children')
          );
        end if;
      end;
    end loop;
  end if;

  -- BullMQ: keepJobs.count == 0 removes the finalized job row (removeOnComplete / removeOnFail true).
  -- CASCADE deletes dependency edges so parents are not blocked by failed-child checks (-9).
  if p_keep_jobs_count is not null and p_keep_jobs_count = 0 then
    delete from :EMQ_SCHEMA.emq_jobs where pk = j.pk;
  elsif (p_keep_jobs_count is not null and p_keep_jobs_count > 0)
     or (p_keep_age_ms is not null and p_keep_age_ms > 0) then
    with ranked as (
      select j2.pk,
             -- Deterministic tiebreak by pk DESC: when several
             -- moveToFinished calls share the same `p_now_ms` (4
             -- concurrent stalled-then-failed workers calling
             -- `Date.now()` inside the same JS event-loop tick is the
             -- typical case), all finalized rows end up with the same
             -- `finished_on` value. Without a tiebreak `row_number()`
             -- ordering on ties is implementation-defined, so the
             -- just-finalized row can land in *any* rank position.
             -- Sorting by pk DESC as the secondary key matches Redis'
             -- ZSET tiebreak behavior (lex DESC of the integer-string
             -- jobId, where higher pk == higher jobId) and keeps the
             -- newest insertion at rn=1.
             row_number() over (
               order by j2.finished_on desc nulls last, j2.pk desc
             ) as rn,
             j2.finished_on
      from :EMQ_SCHEMA.emq_jobs j2
      where j2.queue_id = p_queue_id
        and j2.state::text = v_fin
    ),
    doomed as (
      select r.pk
      from ranked r
      where (p_keep_jobs_count is not null and p_keep_jobs_count >= 0 and r.rn > p_keep_jobs_count)
         or (p_keep_age_ms is not null and p_keep_age_ms > 0
             and r.finished_on is not null
             -- BullMQ removeJobsByMaxAge uses ZREVRANGEBYSCORE with inclusive
             -- bounds (`start -inf`), so jobs whose score equals exactly
             -- `timestamp - maxAge*1000` get removed. Use `<=` here too,
             -- otherwise the boundary job is kept and tests like
             -- `worker.test.ts > should keep of jobs newer than specified
             -- after completed with removeOnComplete` see one extra row.
             and r.finished_on <= v_now - (p_keep_age_ms::text || ' milliseconds')::interval)
    )
    -- NOTE: the just-finalized row IS a trim candidate (no `j3.pk <>
    -- j.pk` exclusion). BullMQ's `removeJobsByMaxCount` ZREVRANGEs
    -- the target set AFTER `ZADD`-ing the just-finalized job, so it
    -- can — and will — trim that job if its (score, lex) position
    -- puts it at the bottom. Excluding j here was a deviation that
    -- broke `keeps the specified number of jobs in failed`: 4
    -- concurrent stalled finalizes share `finished_on`, the j-row
    -- can land at `rn = keepCount + 1`, and the exclusion would skip
    -- the only `DELETE`, leaving an extra row. The plpgsql `j`
    -- variable is a snapshot copy, and the function returns
    -- `p_job_id` (the input), so deleting the underlying row is
    -- safe for all later code paths.
    delete from :EMQ_SCHEMA.emq_jobs j3
    using doomed d0
    where j3.pk = d0.pk;
  end if;

  if p_fetch_next then
    -- Fetch next atomically; internal call does not emit `drained`.
    declare
      v_next_row jsonb;
      v_next_id text;
      v_rl_delay int;
      v_blk_until bigint;
    begin
      select m.out_job_row, m.out_job_id, m.rate_limit_delay_ms, m.block_until_ms
      into v_next_row, v_next_id, v_rl_delay, v_blk_until
      from :EMQ_SCHEMA.emq_move_to_active_v1(
        p_queue_id,
        -- Must use the caller's timestamp (BullMQ moveToFinished-14.lua ARGV[10])
        -- not `now()`. Tests using `sinon.useFakeTimers` fake JS time but
        -- Postgres keeps its real clock, so `now()` would treat every
        -- JS-fake-dated delayed job as instantly due and promote jobs that
        -- should remain delayed (e.g. repeatable-job next-iteration rows).
        coalesce(p_now_ms, (extract(epoch from now()) * 1000)::bigint),
        p_token,
        coalesce(p_lock_duration_ms, 30000),
        null::text,
        false,
        p_limiter_max,
        p_limiter_duration_ms
      ) m
      limit 1;

      -- BullMQ moveToFinished-14.lua: after the next-job fetch, if wait +
      -- active + prioritized are all empty the queue is truly idle and we
      -- emit `drained` once (latched via `emq_queue_counters.drained`).
      if v_next_id is null
         and not exists (
           select 1 from :EMQ_SCHEMA.emq_jobs jx
           where jx.queue_id = p_queue_id
             and jx.state in ('wait', 'prioritized', 'active')
         )
      then
        update :EMQ_SCHEMA.emq_queue_counters qc
        set drained = true
        where qc.queue_id = p_queue_id and qc.drained = false;
        if found then
          perform :EMQ_SCHEMA.emq_emit_event_v1(p_queue_id, 'drained', '{}'::jsonb);
        end if;
      end if;

      return query select 0::int, p_job_id, v_next_row, v_next_id,
        coalesce(v_rl_delay, 0), coalesce(v_blk_until, 0::bigint);
      return;
    end;
  end if;

  return query select 0, p_job_id, null::jsonb, null::text, 0, 0::bigint;
end;
$fn$;
