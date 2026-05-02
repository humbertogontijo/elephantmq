-- Mirrors ref/bullmq/src/commands/isMaxed-2.lua

create or replace function :EMQ_SCHEMA.emq_is_maxed_v1(p_queue_id bigint)
returns boolean
language sql
stable
as $fn$
  select coalesce(
    (select (select count(*)::int from :EMQ_SCHEMA.emq_jobs j where j.queue_id = p_queue_id and j.state = 'active')
     >= q.concurrency
     from :EMQ_SCHEMA.emq_queues q where q.id = p_queue_id and q.concurrency is not null),
    false
  );
$fn$;
