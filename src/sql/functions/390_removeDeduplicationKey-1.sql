-- Mirrors ref/bullmq/src/commands/removeDeduplicationKey-1.lua

-- Matches Redis removeDeduplicationKey-1.lua: delete only if GET would return this job id (key exists and matches).
-- Expired TTL means no key — same as Redis GET returning nil.
create or replace function :EMQ_SCHEMA.emq_remove_deduplication_key_v1(
  p_queue_id bigint,
  p_dedup_id text,
  p_job_id text
) returns int
language sql
as $fn$
  with d as (
    delete from :EMQ_SCHEMA.emq_deduplication
    where queue_id = p_queue_id
      and dedup_id = p_dedup_id
      and job_id = p_job_id
      and (expires_at is null or expires_at > now())
    returning 1
  )
  select count(*)::int from d;
$fn$;
