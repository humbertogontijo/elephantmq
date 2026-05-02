-- Mirrors ref/bullmq/src/commands/changeDelay-4.lua

-- Drop legacy signature so create-or-replace doesn't reject the new arg list.
drop function if exists :EMQ_SCHEMA.emq_change_delay_v1(bigint, text, bigint);

create or replace function :EMQ_SCHEMA.emq_change_delay_v1(
  p_queue_id bigint,
  p_job_id text,
  p_delay_ms bigint,
  p_now_ms bigint default null
) returns int
language plpgsql
as $fn$
declare
  n int;
  v_exists boolean;
  v_base timestamptz;
begin
  -- BullMQ's changeDelay-4.lua computes the new score as
  -- `clientTimestamp + delay` so all other moveToActive comparisons (which
  -- also use the client-supplied `Date.now()` via `p_now_ms`) stay
  -- consistent. Falling back to PG `now()` here introduced a clock-skew
  -- gap between the Docker Postgres clock and the JS host clock, causing
  -- `tests/job.test.ts > .changeDelay > can change delay of a delayed job`
  -- to flake (JS-side timeDiff observed ~1970ms when test asserted >=2000).
  if p_now_ms is not null then
    v_base := to_timestamp(p_now_ms::double precision / 1000.0);
  else
    v_base := now();
  end if;
  update :EMQ_SCHEMA.emq_jobs
  set process_at = v_base + (p_delay_ms::text || ' milliseconds')::interval,
      delay_ms = p_delay_ms::int
  where queue_id = p_queue_id and job_id = p_job_id and state = 'delayed';
  get diagnostics n = row_count;
  if n > 0 then
    return 1;
  end if;
  -- Distinguish "missing job" (-1) from "not in delayed state" (-3) so callers
  -- can raise the matching BullMQ error (mirrors Redis changeDelay-3.lua).
  select true into v_exists
  from :EMQ_SCHEMA.emq_jobs
  where queue_id = p_queue_id and job_id = p_job_id
  limit 1;
  if v_exists then
    return -3;
  end if;
  return -1;
end;
$fn$;
