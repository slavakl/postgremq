# PostgreMQ SQL

PostgreMQ stores JSONB payloads once in `postgremq.messages` and tracks each queue's delivery in `postgremq.queue_messages`. Topics and queues use names as primary keys. Message IDs are `BIGINT`; every delivery has a UUID consumer token. Every queue incarnation also has a UUID generation.

Install into a database without an existing PostgreMQ installation (PostgreSQL 15 is exercised by the test suite). Application data can already exist in other schemas:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f mq/sql/latest.sql
```

The initial migration and `sql/latest.sql` are identical. This is a fresh-install schema; loading it over an existing installation is not an upgrade procedure.

## Schema and application transactions

Installation creates the fixed `postgremq` schema. All queue tables, sequences, functions, indexes and triggers belong to it. SQL object references and client queries are schema-qualified, so neither installation nor runtime requires changing the application's `search_path`.

Keep application data in `public`, `app`, or another schema in the same database. Use the same transaction/connection for application writes and queue calls:

```sql
BEGIN;
UPDATE app.orders SET status = 'paid' WHERE id = 123;
SELECT postgremq.publish_message('order_paid', '{"order_id":123}'::jsonb);
COMMIT;
```

The Go `PublishWithTx`/`AckWithTx` and TypeScript `publishWithTransaction`/`ackWithTransaction` methods execute qualified calls through the supplied transaction. Their API signatures are unchanged. Queue triggers run on queue tables; application tables need no queue triggers.

The migration CLI/Go API stores its version table at `postgremq.postgremq_migrations`. A status check is read-only, including before installation. Runtime roles distinct from the installer need `USAGE` on the schema and the table, sequence and function privileges required by the operations they use; functions retain caller privileges (`SECURITY INVOKER`).

## Delivery contract

`postgremq.publish_message(topic, jsonb, deliver_after DEFAULT clock_timestamp(), group_key DEFAULT NULL)` returns a BIGINT ID and distributes the message transactionally to all live queues currently subscribed to that topic. Queues do not receive publications from before their creation. A publication with no live subscribers creates a payload with no delivery references.

`postgremq.create_queue(name, topic, max_attempts DEFAULT 0, exclusive DEFAULT false, keep_alive_interval DEFAULT '5 minutes')` returns the queue's UUID generation. Redeclaration requires matching configuration. An expired exclusive queue cannot be revived: explicitly delete it and create a new generation. Exclusive means lease-expiring, not ownership restricted to one connection. Only keepalive renews queue lifetime; consuming messages does not.

`postgremq.consume_message(queue, vt_seconds, batch_size DEFAULT 1, generation DEFAULT NULL)` atomically claims available messages using `FOR UPDATE SKIP LOCKED`, increments attempts, and generates an ownership token. Pass the generation to bind a consumer to one queue incarnation. Omit it only when intentionally addressing whichever incarnation currently exists. Missing, expired, or mismatched queues raise `PMQ02`.

- `postgremq.ack_message(queue, message_id, token)` marks a delivery completed.
- `postgremq.nack_message(queue, message_id, token, delay_until DEFAULT clock_timestamp())` returns attempted work for retry or retires its final attempt to the DLQ.
- `postgremq.release_message(queue, message_id, token)` returns unattempted work and decrements the claim's attempt count.
- `postgremq.set_vt(queue, message_id, token, seconds)` extends a live lease and returns its deadline. Contention raises retryable SQLSTATE `55P03`; the function never waits on the delivery row.

A token fences a superseded delivery. Ack can succeed after the visibility deadline if another consumer has not claimed the message yet. Extension requires a live lease. External side effects remain at least once: applications must handle redelivery and use application idempotency keys where necessary. An ambiguous publish response is not proof of failure; clients retry publication automatically only for SQLSTATE `40001` and `40P01`, which establish transaction abortion.

## Message groups

- A message MAY carry a `group_key` (text, ≤255). `NULL` means ungrouped,
  and ungrouped behaviour is byte-for-byte today's.
- **Within one queue, deliveries of one group are claimed in group order.**
  A row is claimable only if no row of the same `(queue, group_key)` with a
  lower `group_seq` is `pending` or `processing`. Completed, dead-lettered
  and deleted rows are settled.
- **Group order is publish commit order.** `publish_message` serialises
  publishers of the same `(topic, group_key)` and allocates a dense
  `group_seq` under a row lock held to commit, so a lower sequence can never
  become visible after a higher one. Consequence to document: publishers of
  one group are serialised at the database; a group is a session, an order,
  an account — never a hot shared key.
- **Head-of-line blocking is by design** (the SQS FIFO model): a head that is
  leased, or nacked with a delay, blocks its group until it is settled or
  visible again. A poison head is retired to the DLQ on its final attempt,
  which unblocks the group. Redelivery after lease expiry is the same head.
- A batch claim (`p_limit > 1`) returns at most one row per group.
- Fan-out: `group_seq` is a property of the topic message, copied to every
  queue row; each queue orders its own copy independently.
- Not promised: ordering across queues; ordering across a DLQ detour (a
  requeued row re-enters as the head of its group although later rows may
  have run); exactly-once external effects; per-group priority.

Operational notes:

- The empty string is not a group key (`PMQ03`); omit the key to publish ungrouped.
- A head delayed by `deliver_after` blocks its group like a nack delay does.
- A publish holds its group's `postgremq.message_groups` row lock until the
  caller's transaction ends. A transaction publishing to several groups can
  deadlock (`40P01`) with another taking the same groups in a different order;
  take groups in a consistent order, and retry the whole transaction on
  `40P01`. The clients retry `40P01` only for their own non-transactional publish.
  Under `REPEATABLE READ`/`SERIALIZABLE`, a grouped publish fails with `40001`
  if another publisher of the same group committed after the transaction's
  snapshot was taken; retry the whole transaction.
- `ack_message`, a final-attempt `nack_message`, `pmq_maintenance_fast` and
  `delete_queue_message` NOTIFY `pmq:q:<queue>` when they settle or remove a
  grouped row, because its successor has just become claimable.
- `get_next_visible_time` skips blocked successors, so a consumer waiting on a
  leased head polls at the head's lease end rather than spinning.
- Claim cost: the consume scan walks the queue's visible rows in `vt` order and
  checks each grouped row against `idx_queue_messages_group_head` until it has
  the batch, so every visible row queued behind a leased or delayed head is
  stepped over on every claim (and again by `get_next_visible_time` after an
  empty claim) — roughly 5 µs per blocked row, e.g. ~50 ms per claim with 10k
  rows waiting behind in-flight heads. Free heads are found without scanning
  the backlog. A group should therefore not accumulate a deep backlog behind a
  long-running or repeatedly failing head: use `max_delivery_attempts > 0` and
  nack delays so a poison head reaches the DLQ. With `max_delivery_attempts = 0`
  a head that always fails blocks its group indefinitely. A consumer that crashes
  on a final attempt blocks the group until the lease expires and
  `pmq_maintenance_fast` retires the row.
  Potential future optimization: *park* successors — distribute a grouped row
  whose group already has an unsettled row with a far-future `vt`, and restore
  its `deliver_after` when the head settles (ack, final-attempt nack,
  maintenance retirement, `delete_queue_message`). The claim's `vt` range would
  then never reach blocked rows, making claim cost independent of the blocked
  backlog; ungrouped traffic would be unaffected. The open design point is the
  race between a publish's "park?" check and a concurrent settle's release
  (both may run in caller-owned transactions): it needs either a
  `message_groups` row lock taken by grouped settles (coupling acks to that
  group's in-flight publishers) or a maintenance sweep that releases stranded
  parked rows (bounded stalls, liveness depends on maintenance).
- `queue_metrics()` counts a blocked successor as `ready` (it is visible and
  within its attempt limit), so `ready`/`oldest_ready_age_seconds` include work
  waiting behind a group head.
- `postgremq.message_groups` keeps one row per group that still has messages.
  `cleanup_unreferenced_messages` prunes the group rows of groups whose last
  payload it deleted (and `clean_up_topic`/`purge_all_messages` prune theirs);
  a pruned group's next publish starts again at `group_seq` 1. A group row
  whose in-flight publisher rolls back while cleanup is pruning it is left
  behind (harmless; it is reused by the next publish to that group).

## Heartbeats

`postgremq.set_vt_batch_multi(queue_names[], message_ids[], tokens[], seconds[])` returns `(queue_name, message_id, vt, consumer_token, outcome)`.

`postgremq.extend_queue_keep_alive_multi(queue_names[], intervals_ms[], generations[] DEFAULT NULL)` returns `(queue_name, keep_alive_until, outcome)`.

Both acquire row locks without waiting. `outcome='extended'` returns a confirmed deadline; `outcome='busy'` has a NULL deadline and means retry within the last confirmed lease budget. An omitted request has lost its lease. Correlate message results by queue, message ID **and token**. Pass queue generations to prevent an old keepalive from renewing a replacement queue. Wall-clock time after locking determines expiry and renewal; transaction start time cannot revive an expired lease. DDL/table locks and network failure can still block a statement, so clients also bound heartbeat I/O.

## Required maintenance

Run these from an external scheduler; installing SQL does not start a scheduler:

```sql
-- For example, every second: final-attempt crash retirement and queue reaping.
SELECT * FROM postgremq.pmq_maintenance_fast();

-- For example, every 10 seconds: 24-hour retention, at most 1000 delivery
-- rows and 1000 orphan payloads per call.
SELECT postgremq.cleanup_completed_messages(24, 1000);
```

`postgremq.cleanup_unreferenced_messages(24, 1000)` is also available separately. It collects only old payloads with **no references in any queue or the DLQ**, including publications without subscribers and payloads left by queue deletion/DLQ purge. Both cleanup functions return deleted-row counts; `postgremq.cleanup_completed_messages` returns the delivery count. They use bounded batches and skip locked candidates. At 10 publications/sec and five queues per topic, fan-out creates about 3000 delivery rows/minute. A 1000-row batch every 10 seconds provides capacity for 6000 delivery rows/minute and 6000 orphan payloads/minute, assuming calls complete on schedule and rows are eligible/unlocked. Increase the batch or frequency for higher fan-out, retries, or a cleanup backlog.

Lagging queues and retained DLQ entries intentionally retain their payloads. Monitor maintenance counts, oldest pending/DLQ age, table size and autovacuum. Run maintenance often enough for the chosen retry latency. Reaping never defines the expiry boundary: expired queues already reject consumption and miss new publications before physical deletion.

`postgremq.list_topics`, `postgremq.list_queues`, `postgremq.get_queue_statistics`, `postgremq.list_messages`, `postgremq.get_message`, and `postgremq.list_dlq_messages` expose state. `postgremq.requeue_dlq_messages(queue)` resets attempts and makes DLQ entries available again; `postgremq.purge_dlq()` deletes DLQ references. DLQ foreign keys restrict destructive queue/payload deletion. `postgremq.delete_queue` refuses a queue with DLQ entries; `postgremq.delete_topic` refuses a topic that still has messages.

`postgremq.clean_up_queue`, `postgremq.clean_up_topic`, and `postgremq.purge_all_messages` are destructive purges, not retention maintenance: they can remove active work.

## Notifications and pooling

Publications emit `NOTIFY` on `pmq:t:<topic>`. Nack, release and DLQ requeue emit on `pmq:q:<queue>`, as do settlements and removals of grouped rows (see Message groups). Payloads are empty; notifications are hints to fetch, with polling as fallback. Topic/queue names are limited to 57 ASCII bytes because PostgreSQL channel names are limited to 63 bytes including the prefix.

Each client Connection shares a dedicated LISTEN session across its consumers. LISTEN requires session affinity; transaction pooling cannot carry that session. Allow additional pool connections for publish, consume, settlement and heartbeats.

## Validation

```sh
cd mq
python3 -m pytest tests/tests.py -q
```

Docker is required. Tests cover fan-out, ownership, delayed delivery, DLQ, queue expiry/recreation, row-lock contention, fresh lease clocks, bounded payload collection, and message-group ordering (including concurrent publishers/consumers and a consume plan guard).


## Observability

Queue-state metrics are available through `postgremq.queue_metrics()`. Both clients
support opt-in OpenTelemetry metrics for operations, handler execution, received
deliveries and renewal loss. See the [observability guide](../docs/observability.md) for the
tested Collector configuration, metric definitions, and runnable Go/TypeScript
examples.
