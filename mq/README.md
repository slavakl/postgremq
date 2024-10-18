# PostgreMQ - SQL Implementation

This directory contains the core PostgreSQL implementation of PostgreMQ - the SQL schema, functions, and triggers that power the message queue system.

## Overview

The SQL implementation provides:

- **Database schema** for topics, queues, messages, and queue management
- **Stored functions** for message publishing, consuming, and lifecycle management
- **Triggers** for automatic message distribution
- **Dead Letter Queue** for handling failed messages
- **Visibility timeout mechanism** instead of traditional locks

## Database Schema

### Tables

#### `topics`
The root table for message topics. Topics are logical channels where messages are published.

```sql
CREATE TABLE topics (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

#### `queues`
Queues subscribe to topics and receive copies of all messages published to those topics.

```sql
CREATE TABLE queues (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    topic_id INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    exclusive BOOLEAN NOT NULL DEFAULT FALSE,
    max_delivery_attempts INTEGER NOT NULL DEFAULT 3,
    keep_alive_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- `exclusive`: If true, queue is temporary and expires when keep_alive_until passes
- `max_delivery_attempts`: After this many failed attempts, messages move to DLQ
- `keep_alive_until`: For exclusive queues, the expiration time

#### `messages`
Stores the actual message payloads.

```sql
CREATE TABLE messages (
    id SERIAL PRIMARY KEY,
    topic_id INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    publish_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deliver_after TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- `deliver_after`: Optional delayed delivery time

#### `queue_messages`
Junction table tracking message state per queue. This is where the visibility timeout is managed.

```sql
CREATE TABLE queue_messages (
    id SERIAL PRIMARY KEY,
    queue_id INTEGER NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'processing', 'completed')),
    vt TIMESTAMPTZ, -- visibility timeout
    consumer_token VARCHAR(255),
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
```

- `status`:
  - `pending`: Ready to be consumed
  - `processing`: Currently being processed (invisible)
  - `completed`: Successfully processed
- `vt`: Visibility timeout - message is invisible until this time
- `consumer_token`: Random token identifying which consumer has the message
- `delivery_attempts`: How many times this message has been delivered

#### `dead_letter_queue`
Messages that exceeded max_delivery_attempts are moved here.

```sql
CREATE TABLE dead_letter_queue (
    id SERIAL PRIMARY KEY,
    queue_name VARCHAR(255) NOT NULL,
    message_id INTEGER NOT NULL,
    payload JSONB NOT NULL,
    delivery_attempts INTEGER NOT NULL,
    last_error TEXT,
    moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Cascade Behavior

All tables use `ON DELETE CASCADE` from topics down:
- Deleting a topic deletes all its queues, messages, and queue_messages
- Deleting a queue deletes all its queue_messages
- Deleting a message deletes all its queue_messages

This makes cleanup operations simple and safe.

## Core Functions

### Topic Management

#### `create_topic(topic_name TEXT) RETURNS INTEGER`
Creates a new topic and returns its ID.

#### `delete_topic(topic_name TEXT) RETURNS VOID`
Deletes a topic and all associated queues and messages (via CASCADE).

#### `list_topics() RETURNS TABLE(...)`
Lists all topics with their metadata.

### Queue Management

#### `create_queue(queue_name TEXT, topic_name TEXT, exclusive BOOLEAN, max_delivery_attempts INTEGER, keep_alive_seconds INTEGER) RETURNS INTEGER`
Creates a new queue subscribed to a topic.

- `exclusive`: If true, creates a temporary queue
- `max_delivery_attempts`: Number of retries before moving to DLQ
- `keep_alive_seconds`: For exclusive queues, initial keep-alive duration

#### `delete_queue(queue_name TEXT) RETURNS VOID`
Deletes a queue and all its queue_messages.

#### `extend_queue_keep_alive(queue_name TEXT, keep_alive_seconds INTEGER) RETURNS TIMESTAMPTZ`
Extends the keep-alive timeout for an exclusive queue.

#### `list_queues() RETURNS TABLE(...)`
Lists all queues with their configuration.

#### `get_queue_statistics(queue_name TEXT) RETURNS TABLE(...)`
Returns statistics for a queue:
- Pending messages count
- Processing messages count
- Completed messages count
- Total messages

### Message Publishing

#### `publish_message(topic_name TEXT, payload JSONB, deliver_after TIMESTAMPTZ) RETURNS INTEGER`
Publishes a message to a topic. Returns the message ID.

The `distribute_message()` trigger automatically creates queue_messages entries for all queues subscribed to the topic.

- `deliver_after`: Optional - delays delivery until specified time

### Message Consumption

#### `consume_message(queue_name TEXT, consumer_token TEXT, vt_seconds INTEGER) RETURNS TABLE(...)`
Fetches the next available message from a queue.

- Sets status to 'processing'
- Sets visibility timeout (vt)
- Stores consumer_token
- Increments delivery_attempts
- Returns message details

Uses `FOR UPDATE SKIP LOCKED` for concurrent consumer safety.

#### `consume_messages_batch(queue_name TEXT, consumer_token TEXT, vt_seconds INTEGER, batch_size INTEGER) RETURNS TABLE(...)`
Batch version of consume_message - fetches multiple messages at once.

### Message Acknowledgment

#### `ack_message(queue_name TEXT, message_id INTEGER, token TEXT) RETURNS BOOLEAN`
Marks a message as successfully processed.

- Verifies consumer_token matches
- Sets status to 'completed'
- Records completion time
- Returns true if successful

#### `nack_message(queue_name TEXT, message_id INTEGER, token TEXT, delay_seconds INTEGER) RETURNS BOOLEAN`
Negatively acknowledges a message - returns it to the queue for retry.

- Verifies consumer_token matches
- Resets status to 'pending'
- Clears vt and consumer_token
- Optionally sets deliver_after for delayed retry
- Returns true if successful

If delivery_attempts exceeds max_delivery_attempts, message moves to DLQ instead.

#### `release_message(queue_name TEXT, message_id INTEGER, token TEXT) RETURNS BOOLEAN`
Releases a message back to the queue without incrementing delivery_attempts.

- Used when consumer fetched but never attempted processing
- Resets to 'pending' state
- Does not count against max_delivery_attempts

### Visibility Timeout Extension

#### `extend_message_vt(queue_name TEXT, message_id INTEGER, token TEXT, extension_seconds INTEGER) RETURNS TIMESTAMPTZ`
Extends the visibility timeout for a message being processed.

- Verifies consumer_token matches
- Extends vt by extension_seconds
- Returns new vt timestamp

Used by auto-extension features in client libraries.

#### `batch_extend_message_vt(queue_name TEXT, message_ids INTEGER[], token TEXT, extension_seconds INTEGER) RETURNS TABLE(...)`
Batch version of extend_message_vt for extending multiple messages at once.

### Dead Letter Queue

#### `list_dlq_messages() RETURNS TABLE(...)`
Lists all messages in the dead letter queue.

#### `requeue_dlq_messages(queue_name TEXT) RETURNS INTEGER`
Moves messages from DLQ back to their original queue for retry.

Returns count of requeued messages.

#### `purge_dlq() RETURNS INTEGER`
Deletes all messages from the dead letter queue.

Returns count of deleted messages.

### Cleanup Operations

#### `cleanup_queue_messages(queue_name TEXT) RETURNS INTEGER`
Deletes all completed messages from a queue.

Returns count of deleted messages.

#### `cleanup_topic_messages(topic_name TEXT) RETURNS INTEGER`
Deletes all completed messages from all queues of a topic.

#### `cleanup_expired_exclusive_queues() RETURNS INTEGER`
Deletes exclusive queues where keep_alive_until has expired.

Should be called periodically (e.g., via cron job).

#### `purge_all_messages() RETURNS VOID`
Deletes ALL messages and queue_messages (dangerous - for testing only).

## Triggers

### `distribute_message_trigger`
Automatically fires after INSERT on messages table.

Creates queue_message entries for all queues subscribed to the message's topic.

Sets initial status to 'pending' and uses deliver_after if specified.

## Event Notifications

PostgreMQ uses PostgreSQL's LISTEN/NOTIFY for real-time message delivery.

When a message is distributed to a queue, a NOTIFY is sent on channel `queue:{queue_name}` with the message ID as payload.

Client libraries LISTEN on these channels to receive instant notifications instead of polling.

## Installation

### Load the Schema

```bash
psql -U postgres -d your_database -f sql/latest.sql
```

Or from within PostgreSQL:

```sql
\i sql/latest.sql
```

### Verify Installation

```sql
-- Check tables exist
SELECT tablename FROM pg_tables WHERE schemaname = 'public'
  AND tablename IN ('topics', 'queues', 'messages', 'queue_messages', 'dead_letter_queue');

-- Check functions exist
SELECT proname FROM pg_proc WHERE proname LIKE '%message%' OR proname LIKE '%queue%';
```

## Testing

The SQL implementation includes comprehensive tests using pytest and testcontainers.

### Setup

```bash
# Install dependencies
pip install -r tests/requirements.txt
```

### Run Tests

```bash
# All tests
pytest tests/tests.py -v

# With coverage
pytest tests/tests.py -v --cov

# Specific test
pytest tests/tests.py::TestBasicOperations::test_create_topic -v
```

Tests cover:
- Topic/queue creation and deletion
- Message publishing and distribution
- Message consumption (single and batch)
- Acknowledgment (ack, nack, release)
- Visibility timeout extension
- Dead letter queue behavior
- Concurrent consumer scenarios
- Delayed delivery
- Exclusive queue expiration
- Transaction behavior

## Performance Considerations

### Indexes

The schema includes indexes on:
- `queue_messages(queue_id, status, vt)` - Critical for consume_message performance
- `queue_messages(message_id)` - For message lookups
- `messages(topic_id)` - For topic-based queries
- Unique constraints on names

### Partitioning

For high-volume deployments, consider partitioning:
- `messages` by created_at (time-based partitioning)
- `queue_messages` by queue_id (hash partitioning)

### Vacuum

Ensure autovacuum is configured properly, especially for:
- `queue_messages` - high churn from status updates
- `messages` - regular cleanup of completed messages

### Connection Pooling

Use connection pooling (pgBouncer, pgxpool, pg pool) to minimize connection overhead.

## Best Practices

### Regular Cleanup

Periodically clean up completed messages:

```sql
-- Cleanup completed messages older than 7 days
DELETE FROM queue_messages
WHERE status = 'completed'
  AND completed_at < NOW() - INTERVAL '7 days';
```

### Monitor Queue Depth

```sql
-- Check pending message counts
SELECT q.name, COUNT(*) as pending_count
FROM queues q
JOIN queue_messages qm ON qm.queue_id = q.id
WHERE qm.status = 'pending'
GROUP BY q.name
ORDER BY pending_count DESC;
```

### Monitor DLQ

```sql
-- Check dead letter queue
SELECT queue_name, COUNT(*) as failed_count
FROM dead_letter_queue
GROUP BY queue_name
ORDER BY failed_count DESC;
```

### Cleanup Exclusive Queues

Run periodically:

```sql
SELECT cleanup_expired_exclusive_queues();
```

## Migration

When updating the schema:

1. Test changes in a development environment
2. Use transactions for schema changes
3. Consider backward compatibility
4. Update client libraries if function signatures change
5. Document breaking changes in CHANGELOG.md

## Troubleshooting

### Messages Not Being Consumed

Check:
- Queue exists and is subscribed to the correct topic
- Messages have passed their deliver_after time
- Messages haven't exceeded max_delivery_attempts
- Exclusive queues haven't expired

### Stuck Messages

```sql
-- Find messages stuck in 'processing' with expired vt
SELECT qm.*, q.name
FROM queue_messages qm
JOIN queues q ON q.id = qm.queue_id
WHERE qm.status = 'processing'
  AND qm.vt < NOW();
```

These will automatically become available again for consumption.

### Performance Issues

- Check `pg_stat_user_tables` for table statistics
- Review slow query log
- Analyze query plans with EXPLAIN
- Consider adding indexes for custom queries
- Review vacuum/analyze statistics

## Security

- Use least-privilege database roles
- Don't allow untrusted user input as queue/topic names
- Use SSL/TLS for database connections
- Consider row-level security for multi-tenant deployments
- Audit critical operations

## License

MIT License - See [LICENSE](../LICENSE) for details.
