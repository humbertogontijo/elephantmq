-- Mirrors ref/bullmq/src/commands/drain-5.lua

create or replace function :EMQ_SCHEMA.emq_drain_v1(p_queue_id bigint, p_include_delayed boolean)
returns int
language plpgsql
as $fn$
declare n int;
begin
  -- Preserve jobs that belong to a current scheduler iteration
  -- (id `repeat:<schedulerId>:<next_millis>`) so drain/clean does not leave
  -- the scheduler without a pending fire.
  delete from :EMQ_SCHEMA.emq_jobs j
  where j.queue_id = p_queue_id
    and (
      j.state in ('wait', 'paused', 'prioritized')
      or (p_include_delayed and j.state = 'delayed')
    )
    and not exists (
      select 1 from :EMQ_SCHEMA.emq_job_schedulers s
      where s.queue_id = p_queue_id
        and j.job_id = 'repeat:' || s.scheduler_id || ':' || s.next_millis::text
    );
  get diagnostics n = row_count;

  -- Orphan dedup rows (job removed; logs/deps cascade from emq_jobs FKs).
  delete from :EMQ_SCHEMA.emq_deduplication d
  where d.queue_id = p_queue_id
    and not exists (
      select 1 from :EMQ_SCHEMA.emq_jobs j
      where j.queue_id = p_queue_id and j.job_id = d.job_id
    );

  perform :EMQ_SCHEMA.emq_emit_event_v1(p_queue_id, 'drained', jsonb_build_object('count', n));
  return n;
end;
$fn$;
