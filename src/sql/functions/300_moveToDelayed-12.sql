-- Mirrors ref/bullmq/src/commands/moveToDelayed-12.lua

drop function if exists :EMQ_SCHEMA.emq_move_to_delayed_v1(bigint, text, bigint, text);
drop function if exists :EMQ_SCHEMA.emq_move_to_delayed_v1(bigint, text, bigint, text, text, text[]);

create or replace function :EMQ_SCHEMA.emq_move_to_delayed_v1(
  p_queue_id bigint,
  p_job_id text,
  p_process_at_ms bigint,
  p_token text,
  p_failed_reason text default null,
  p_stacktrace text[] default null,
  p_delay_ms bigint default null
) returns int
language plpgsql
as $fn$
declare n int;
begin
  update :EMQ_SCHEMA.emq_jobs
  set state = 'delayed',
      process_at = to_timestamp(p_process_at_ms / 1000.0),
      -- BullMQ's moveToDelayed-12.lua does `HSET job delay ARGV[5]` — it
      -- persists the caller's delay value verbatim. Use the explicit
      -- `p_delay_ms` when supplied so `job.delay` mirrors BullMQ's stored
      -- value exactly; fall back to the derived `process_at - now()` for
      -- legacy callers that don't pass the delay.
      delay_ms = greatest(
        coalesce(
          p_delay_ms,
          p_process_at_ms - (extract(epoch from now()) * 1000)::bigint
        ),
        0
      ),
      lock_token = null,
      locked_by = null,
      locked_at = null,
      lock_expires_at = null,
      -- Route "retry with backoff" failures from moveToFailed through this path
      -- while still persisting the last failure details, matching BullMQ's
      -- Job.moveToFailed -> moveToDelayed-8.lua path.
      failed_reason = coalesce(p_failed_reason, failed_reason),
      stacktrace = coalesce(p_stacktrace, stacktrace),
      -- Bump attempts_made when coming from `active` so retry-with-backoff
      -- consumes one of the configured attempts (BullMQ's moveToDelayed-8.lua
      -- path). Job.moveToDelayed (user-initiated) passes lock_token '0' and
      -- p_failed_reason null — skip the bump there to match Redis semantics.
      attempts_made = case
        when state = 'active' and p_failed_reason is not null then attempts_made + 1
        else attempts_made
      end
  where queue_id = p_queue_id and job_id = p_job_id
    and (p_token = '0' or lock_token = p_token);
  get diagnostics n = row_count;
  if n > 0 then
    perform :EMQ_SCHEMA.emq_emit_event_v1(
      p_queue_id,
      'delayed',
      jsonb_build_object('jobId', p_job_id, 'delay', p_process_at_ms)
    );
    return 0;
  end if;
  return -1;
end;
$fn$;
