# PostgreMQ SQL

PostgreMQ stores JSONB payloads once in `messages` and tracks each queue's delivery in `queue_messages`. Topics and queues use names as primary keys. Message IDs are `BIGINT`; every delivery has a UUID consumer token. Every queue incarnation also has a UUID generation.

Install into an empty database (PostgreSQL 15 is exercised by the test suite):

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f mq/sql/latest.sql
```

The initial migration and `sql/latest.sql` are identical. This is a fresh-install schema; loading it over an existing installation is not an upgrade procedure.

## Delivery contract

`publish_message(topic, jsonb, deliver_after DEFAULT clock_timestamp())` returns a BIGINT ID and distributes the message transactionally to all live queues currently subscribed to that topic. Queues do not receive publications from before their creation. A publication with no live subscribers creates a payload with no delivery references.

`create_queue(name, topic, max_attempts DEFAULT 0, exclusive DEFAULT false, keep_alive_interval DEFAULT '5 minutes')` returns the queue's UUID generation. Redeclaration requires matching configuration. An expired exclusive queue cannot be revived: explicitly delete it and create a new generation. Exclusive means lease-expiring, not ownership restricted to one connection. Only keepalive renews queue lifetime; consuming messages does not.

`consume_message(queue, vt_seconds, batch_size DEFAULT 1, generation DEFAULT NULL)` atomically claims available messages using `FOR UPDATE SKIP LOCKED`, increments attempts, and generates an ownership token. Pass the generation to bind a consumer to one queue incarnation. Omit it only when intentionally addressing whichever incarnation currently exists. Missing, expired, or mismatched queues raise `PMQ02`.

- `ack_message(queue, message_id, token)` marks a delivery completed.
- `nack_message(queue, message_id, token, delay_until DEFAULT clock_timestamp())` returns attempted work for retry or retires its final attempt to the DLQ.
- `release_message(queue, message_id, token)` returns unattempted work and decrements the claim's attempt count.
- `set_vt(queue, message_id, token, seconds)` extends a live lease and returns its deadline. Contention raises retryable SQLSTATE `55P03`; the function never waits on the delivery row.

A token fences a superseded delivery. Ack can succeed after the visibility deadline if another consumer has not claimed the message yet. Extension requires a live lease. External side effects remain at least once: applications must handle redelivery and use application idempotency keys where necessary. An ambiguous publish response is not proof of failure; clients retry publication automatically only for SQLSTATE `40001` and `40P01`, which establish transaction abortion.

## Heartbeats

`set_vt_batch_multi(queue_names[], message_ids[], tokens[], seconds[])` returns `(queue_name, message_id, vt, consumer_token, outcome)`.

`extend_queue_keep_alive_multi(queue_names[], intervals_ms[], generations[] DEFAULT NULL)` returns `(queue_name, keep_alive_until, outcome)`.

Both acquire row locks without waiting. `outcome='extended'` returns a confirmed deadline; `outcome='busy'` has a NULL deadline and means retry within the last confirmed lease budget. An omitted request has lost its lease. Correlate message results by queue, message ID **and token**. Pass queue generations to prevent an old keepalive from renewing a replacement queue. Wall-clock time after locking determines expiry and renewal; transaction start time cannot revive an expired lease. DDL/table locks and network failure can still block a statement, so clients also bound heartbeat I/O.

## Required maintenance

Run these from an external scheduler; installing SQL does not start a scheduler:

```sql
-- For example, every second: final-attempt crash retirement and queue reaping.
SELECT * FROM pmq_maintenance_fast();

-- For example, every 10 seconds: 24-hour retention, at most 1000 delivery
-- rows and 1000 orphan payloads per call.
SELECT cleanup_completed_messages(24, 1000);
```

`cleanup_unreferenced_messages(24, 1000)` is also available separately. It collects only old payloads with **no references in any queue or the DLQ**, including publications without subscribers and payloads left by queue deletion/DLQ purge. Both cleanup functions return deleted-row counts; `cleanup_completed_messages` returns the delivery count. They use bounded batches and skip locked candidates. At 10 publications/sec and five queues per topic, fan-out creates about 3000 delivery rows/minute. A 1000-row batch every 10 seconds provides capacity for 6000 delivery rows/minute and 6000 orphan payloads/minute, assuming calls complete on schedule and rows are eligible/unlocked. Increase the batch or frequency for higher fan-out, retries, or a cleanup backlog.

Lagging queues and retained DLQ entries intentionally retain their payloads. Monitor maintenance counts, oldest pending/DLQ age, table size and autovacuum. Run maintenance often enough for the chosen retry latency. Reaping never defines the expiry boundary: expired queues already reject consumption and miss new publications before physical deletion.

`list_topics`, `list_queues`, `get_queue_statistics`, `list_messages`, `get_message`, and `list_dlq_messages` expose state. `requeue_dlq_messages(queue)` resets attempts and makes DLQ entries available again; `purge_dlq()` deletes DLQ references. DLQ foreign keys restrict destructive queue/payload deletion. `delete_queue` refuses a queue with DLQ entries; `delete_topic` refuses a topic that still has messages.

`clean_up_queue`, `clean_up_topic`, and `purge_all_messages` are destructive purges, not retention maintenance: they can remove active work.

## Notifications and pooling

Publications emit `NOTIFY` on `pmq:t:<topic>`. Nack, release and DLQ requeue emit on `pmq:q:<queue>`. Payloads are empty; notifications are hints to fetch, with polling as fallback. Topic/queue names are limited to 57 ASCII bytes because PostgreSQL channel names are limited to 63 bytes including the prefix.

Each client Connection shares a dedicated LISTEN session across its consumers. LISTEN requires session affinity; transaction pooling cannot carry that session. Allow additional pool connections for publish, consume, settlement and heartbeats.

## Validation

```sh
cd mq
python3 -m pytest tests/tests.py -q
```

Docker is required. Tests cover fan-out, ownership, delayed delivery, DLQ, queue expiry/recreation, row-lock contention, fresh lease clocks and bounded payload collection.
