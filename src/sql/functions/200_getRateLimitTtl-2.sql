-- Mirrors ref/bullmq/src/commands/getRateLimitTtl-2.lua

drop function if exists :EMQ_SCHEMA.emq_get_rate_limit_ttl_v1(bigint);

create or replace function :EMQ_SCHEMA.emq_get_rate_limit_ttl_v1(
  p_queue_id bigint,
  p_max_jobs bigint default null
)
returns bigint
language sql
stable
as $fn$
  -- BullMQ `getRateLimitTtl(maxJobs?)` semantics (getRateLimitTTL.lua):
  -- * No maxJobs: return PTTL of rate limiter key (-2 when absent).
  -- * With maxJobs: return 0 when counter < maxJobs (or key absent), else PTTL.
  select case
    when p_max_jobs is null then coalesce(
      (select greatest(0::numeric, ceil(extract(epoch from (r.expires_at - now())) * 1000))::bigint
       from :EMQ_SCHEMA.emq_rate_limit_state r
       where r.queue_id = p_queue_id and r.expires_at is not null and r.expires_at > now()),
      (-2)::bigint
    )
    else coalesce(
      (select case
                when r.tokens >= p_max_jobs and r.expires_at is not null and r.expires_at > now()
                  then greatest(0::numeric, ceil(extract(epoch from (r.expires_at - now())) * 1000))::bigint
                else 0::bigint
              end
       from :EMQ_SCHEMA.emq_rate_limit_state r
       where r.queue_id = p_queue_id),
      0::bigint
    )
  end;
$fn$;
