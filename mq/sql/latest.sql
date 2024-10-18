/*
 * PostgreSQL Message Queue System Implementation
 *
 * REQUIREMENTS: PostgreSQL 15 or later
 *
 * Overview
 * --------
 * This file contains the SQL schema and runtime API for PostgreMQ – a
 * PostgreSQL‑backed message queue. It implements message publishing and
 * fan‑out to queues, consumption with "visibility timeout" (vt), positive
 * and negative acknowledgments, visibility extension, dead‑letter queue
 * (DLQ) movement and restoration, queue/topic management, and list/inspect
 * helpers.
 *
 * Core Concepts
 * -------------
 * - Visibility Timeout (vt): When a consumer fetches a message it becomes
 *   invisible to other consumers until `vt`. Extending vt keeps the message
 *   with the current consumer without taking a heavyweight lock.
 * - Queue Types: Queues can be:
 *   - Non‑exclusive (persistent): never expire.
 *   - Exclusive (temporary): expire unless a client keeps them alive by
 *     periodically extending `keep_alive_until`.
 * - Keep‑Alive: Clients of exclusive queues should periodically call
 *   `extend_queue_keep_alive()` (both clients implement automatic keep‑alive)
 *   otherwise the queue is eligible for deletion by `delete_inactive_queues()`.
 * - Delivery Attempts: Each time a message is consumed its
 *   `delivery_attempts` is incremented. When a queue has
 *   `max_delivery_attempts > 0` and a message reaches the limit it is
 *   moved to the DLQ by `move_messages_to_dlq()`.
 * - DLQ (Dead Letter Queue): Failed messages are copied to
 *   `dead_letter_queue`. Use `list_dlq_messages()`, `requeue_dlq_messages()`
 *   and `purge_dlq()` to manage.
 *
 * Event Notifications
 * -------------------
 * Several operations emit LISTEN/NOTIFY events on channel `postgremq_events`.
 * The payload is a JSON object with shape:
 *   { "event": <text>, "queues": <text[]>, "vt": <timestamptz|null> }
 * Emitted events:
 *   - "message_published" by the distribution trigger after a publish
 *   - "message_nacked" when `nack_message` resets a message
 *   - "message_released" when `release_message` returns a message to pending
 * Clients can LISTEN to this channel to promptly react to new availability.
 *
 * Table Relationships and Cascade Behavior:
 * - topics: The root table containing topic names
 * - queues: References topics.name with ON DELETE CASCADE
 * - messages: References topics.name with ON DELETE CASCADE
 * - queue_messages: References both queues.name and messages.id with ON DELETE CASCADE
 * - dead_letter_queue: References both queues.name and messages.id with ON DELETE CASCADE
 *
 * This cascade behavior ensures that:
 * 1. When a topic is deleted, all related queues, messages, and queue entries are automatically removed
 * 2. When a message is deleted (e.g., via clean_up_topic), all related queue entries are automatically removed
 * 3. When a queue is deleted, all its message entries are automatically removed
 *
 * Functions (high level):
 *   - publish_message: Insert into messages and trigger distribution to queues.
 *   - consume_message: Retrieve and mark messages as processing; sets vt.
 *   - ack_message: Mark as completed; clears consumer token; sets processed_at.
 *   - nack_message: Return to pending with optional delay; clears token; NOTIFY.
 *   - release_message: Return to pending without incrementing attempts; NOTIFY.
 *   - set_vt / set_vt_batch: Extend visibility time for one or many messages.
 *   - move_messages_to_dlq / requeue_dlq_messages / purge_dlq.
 *   - extend_queue_keep_alive / delete_inactive_queues for exclusive queues.
 *   - Management: create_topic, create_queue, delete_topic, delete_queue,
 *     list_topics, list_queues, get_queue_statistics, clean_up_* helpers,
 *     list_messages, get_message, get_next_visible_time, cleanup_completed_messages.
 */


-- Topics table.
CREATE TABLE topics (
  name VARCHAR(255) PRIMARY KEY
);

-- Queues table.
CREATE TABLE queues (
  name VARCHAR(255) PRIMARY KEY,
  topic_name VARCHAR(255) REFERENCES topics(name) ON DELETE CASCADE,
  max_delivery_attempts INT NOT NULL DEFAULT 0,
  exclusive BOOLEAN NOT NULL DEFAULT false,  -- Changed from durable
  keep_alive_until TIMESTAMPTZ
);

-- Messages table: payload stored as JSONB.
CREATE TABLE messages (
  id SERIAL PRIMARY KEY,
  topic_name VARCHAR(255) REFERENCES topics(name) ON DELETE CASCADE,
  payload JSONB NOT NULL,
  published_at TIMESTAMPTZ DEFAULT NOW(),
  deliver_after TIMESTAMPTZ DEFAULT NOW()  -- New column with default NOW()
);

-- Queue Messages table.
-- Composite primary key: (queue_name, message_id).
CREATE TABLE queue_messages (
  queue_name VARCHAR(255) REFERENCES queues(name) ON DELETE CASCADE,
  message_id INT REFERENCES messages(id) ON DELETE CASCADE,
  status VARCHAR(16) DEFAULT 'pending',  -- Allowed: 'pending', 'processing', 'completed'
  published_at TIMESTAMPTZ DEFAULT NOW(),
  vt TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- Renamed from locked_until
  delivery_attempts INT DEFAULT 0,
  consumer_token VARCHAR(64),
  processed_at TIMESTAMPTZ,
  PRIMARY KEY (queue_name, message_id)
);

-- Dead Letter Queue table.
-- Composite primary key: (queue_name, message_id).
CREATE TABLE dead_letter_queue (
  queue_name VARCHAR(255) REFERENCES queues(name) ON DELETE CASCADE,
  message_id INT REFERENCES messages(id) ON DELETE CASCADE,
  retry_count INT,
  published_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (queue_name, message_id)
);

---------------------------
-- Performance Indexes
---------------------------

-- Index for consume_message query: filters by queue, status, and vt
-- Partial index excludes completed messages to keep it small
CREATE INDEX idx_queue_messages_consume
ON queue_messages(queue_name, vt, published_at)
WHERE status IN ('pending', 'processing');

-- Index for get_next_visible_time query
CREATE INDEX idx_queue_messages_next_visible
ON queue_messages(queue_name, vt)
WHERE status IN ('pending', 'processing');

/* Function: distribute_message
 *
 * Description:
 *   Trigger function that fans a newly published message (row in `messages`)
 *   out to all queues subscribed to the message's topic. For each matching
 *   queue, it inserts a row into `queue_messages` with initial vt set to the
 *   message's `deliver_after`. It also NOTIFY-s on channel `postgremq_events`
 *   with an event payload `{event:"message_published", queues:[...], vt: NEW.deliver_after}`.
 *
 * Trigger:
 *   - Installed as AFTER INSERT trigger `after_message_insert` on `messages`.
 *
 * Side Effects:
 *   - Inserts multiple rows into `queue_messages`.
 *   - Emits a NOTIFY event used by clients for push-based wakeups.
 *
 * Returns:
 *   The unmodified NEW row for the `messages` table.
 */
CREATE OR REPLACE FUNCTION distribute_message() 
RETURNS trigger AS $$
DECLARE
   published_queues TEXT[];
BEGIN
   WITH ins AS (
     INSERT INTO queue_messages(queue_name, message_id, vt)
     SELECT q.name, NEW.id, NEW.deliver_after
     FROM queues q
     WHERE q.topic_name = NEW.topic_name
       AND (NOT q.exclusive OR q.keep_alive_until > NOW())
     RETURNING queue_name
   )
   SELECT array_agg(queue_name) INTO published_queues FROM ins;
   
   PERFORM pg_notify('postgremq_events', 
      json_build_object(
         'event', 'message_published',
         'queues', published_queues,
         'vt', NEW.deliver_after
      )::text);
      
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

/* Trigger: after_message_insert
 *
 * Description:
 *   Automatically distributes newly published messages to all queues subscribed to the message's topic.
 *   This trigger fires after each message insert and calls distribute_message() to handle the fan-out.
 *
 * Timing: AFTER INSERT
 * Granularity: FOR EACH ROW
 * Target Table: messages
 *
 * Behavior:
 *   For each newly inserted message, this trigger:
 *   1. Finds all active queues subscribed to the message's topic
 *   2. Creates queue_message entries for each queue via distribute_message()
 *   3. Emits NOTIFY event to wake up waiting consumers
 *
 * Side Effects:
 *   - Multiple inserts into queue_messages (one per subscribed queue)
 *   - NOTIFY on 'postgremq_events' channel with payload containing affected queue names
 *   - Exclusive queues with expired keep_alive_until are excluded from distribution
 */
CREATE TRIGGER after_message_insert
AFTER INSERT ON messages
FOR EACH ROW
EXECUTE FUNCTION distribute_message();

---------------------------
-- Runtime API Functions
---------------------------

/* Function: create_topic
 *
 * Description:
 *   Creates a topic if it does not exist (idempotent).
 *
 * Parameters:
 *   - p_topic (VARCHAR): Name of the topic to create.
 *
 * Returns:
 *   VARCHAR: The topic name (for convenience/chaining).
 */
CREATE OR REPLACE FUNCTION create_topic(p_topic VARCHAR(255))
RETURNS VARCHAR(255) AS $$
BEGIN
  INSERT INTO topics(name) VALUES (p_topic)
  ON CONFLICT (name) DO NOTHING;
  RETURN p_topic;
END;
$$ LANGUAGE plpgsql;

/* Function: create_queue
 *
 * Description:
 *   Creates a new queue that subscribes to a topic. If the queue is exclusive,
 *   it will be automatically deleted when its keep_alive_until timestamp expires.
 *   Only one exclusive queue with a given name can exist at a time.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue to create.
 *   - p_topic_name (VARCHAR): Name of the topic to subscribe to.
 *   - p_max_attempts (INTEGER): Maximum delivery attempts before moving to DLQ.
 *   - p_exclusive (BOOLEAN): If true, queue will be deleted when keep_alive expires.
 *   - p_keep_alive_sec (INTEGER): Seconds to keep the queue alive (only used for exclusive queues,
 *                                defaults to 30 seconds).
 *
 * Returns: VOID.
 *
 * Raises:
 *   - Exception if attempting to create a duplicate exclusive queue.
 */
CREATE OR REPLACE FUNCTION create_queue(
    p_queue_name VARCHAR(255),
    p_topic_name VARCHAR(255),
    p_max_attempts INTEGER DEFAULT 0,  -- Changed to 0 for unlimited retries
    p_exclusive BOOLEAN DEFAULT false,
    p_keep_alive_sec INTEGER DEFAULT 30
) RETURNS VOID AS $$
BEGIN
    INSERT INTO queues (
        name,
        topic_name,
        max_delivery_attempts,
        exclusive,
        keep_alive_until
    ) VALUES (
        p_queue_name,
        p_topic_name,
        p_max_attempts,
        p_exclusive,
        CASE 
            WHEN p_exclusive THEN NOW() + make_interval(secs => p_keep_alive_sec)
            ELSE NULL
        END
    );
EXCEPTION
    WHEN unique_violation THEN
        IF p_exclusive THEN
            RAISE EXCEPTION 'An exclusive queue with name "%" already exists', p_queue_name;
        ELSE
            RAISE;
        END IF;
END;
$$ LANGUAGE plpgsql;

/* Function: publish_message
 *
 * Description:
 *   Publishes a message into `messages` for the specified topic. The
 *   distribution to queues is performed by the `after_message_insert` trigger
 *   via `distribute_message()`. If `p_deliver_after` is specified, message will
 *   be invisible to consumers until that timestamp; otherwise it is visible
 *   immediately.
 *
 * Parameters:
 *   - p_topic (VARCHAR): Topic name (must exist).
 *   - p_payload (JSONB): Arbitrary JSON payload stored in `messages.payload`.
 *   - p_deliver_after (TIMESTAMPTZ, default NOW()): First visibility time.
 *
 * Returns:
 *   INT: The generated message id.
 *
 * Side Effects:
 *   - Triggers `distribute_message()` which inserts into `queue_messages` and
 *     emits NOTIFY on `postgremq_events`.
 */
CREATE OR REPLACE FUNCTION publish_message(
    p_topic VARCHAR(255), 
    p_payload JSONB,
    p_deliver_after TIMESTAMPTZ DEFAULT NOW()
) RETURNS INT AS $$
DECLARE
    v_message_id INT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM topics WHERE name = p_topic) THEN
        RAISE EXCEPTION 'Topic "%" does not exist', p_topic;
    END IF;
    
    INSERT INTO messages(topic_name, payload, deliver_after)
    VALUES (p_topic, p_payload, p_deliver_after)
    RETURNING id INTO v_message_id;
    
    RETURN v_message_id;
END;
$$ LANGUAGE plpgsql;

/* Function: consume_message
 *
 * Description:
 *   Retrieves one or more messages from the specified queue. For each message, it increments the 
 *   delivery_attempts field and locks the message for the provided duration. Only messages that
 *   haven't exceeded their max_delivery_attempts limit are returned.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_vt (INTEGER): The duration for which the message is locked in seconds.
 *   - p_limit (INT DEFAULT 1): Maximum number of messages to retrieve.
 *
 * Returns:
 *   A table of records with fields: queue_name, message_id, payload, consumer_token, delivery_attempts.
 */
CREATE OR REPLACE FUNCTION consume_message(
    p_queue_name VARCHAR(255),
    p_vt INTEGER,
    p_limit INT DEFAULT 1
) RETURNS TABLE(
    queue_name VARCHAR(255), 
    message_id INT,
    payload JSONB, 
    consumer_token VARCHAR(64),
    delivery_attempts INT,
    vt TIMESTAMPTZ,
    published_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    WITH target_queue AS (
        SELECT name, max_delivery_attempts
        FROM queues
        WHERE name = p_queue_name
            AND (NOT exclusive OR keep_alive_until > NOW())
    ),
    next_msg AS (
        SELECT qm.queue_name,
               qm.message_id,
               qm.status,
               qm.delivery_attempts,
               qm.published_at
        FROM queue_messages qm
        CROSS JOIN target_queue tq
        WHERE qm.queue_name = tq.name
            AND (tq.max_delivery_attempts = 0 OR qm.delivery_attempts < tq.max_delivery_attempts)
            AND (qm.status = 'pending' OR qm.status = 'processing' )
            AND qm.vt <= NOW()
        ORDER BY qm.published_at
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    )
    UPDATE queue_messages
    SET status = 'processing',
        vt = NOW() + make_interval(secs => p_vt),
        delivery_attempts = qm.delivery_attempts + 1,
        -- Generate unique consumer token using microsecond precision and transaction ID
        consumer_token = to_char(NOW(), 'YYYYMMDDHH24MISS.US') || '-' || substr(md5(random()::text || txid_current()::text), 1, 12)
    FROM next_msg qm
    WHERE queue_messages.queue_name = qm.queue_name
        AND queue_messages.message_id = qm.message_id
    RETURNING queue_messages.queue_name,
              queue_messages.message_id,
              (SELECT m.payload FROM messages m WHERE m.id = queue_messages.message_id) AS payload,
              queue_messages.consumer_token,
              queue_messages.delivery_attempts,
              queue_messages.vt,
              queue_messages.published_at;
END;
$$ LANGUAGE plpgsql;

/* Function: ack_message
 *
 * Description:
 *   Acknowledges a message by marking its status as 'completed'. This function ensures that only the
 *   correct consumer (verified via consumer_token) can acknowledge the intended message.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (INT): Identifier of the message.
 *   - p_consumer_token (VARCHAR): The consumer token generated via consume_message.
 *
 * Returns: VOID.
 *
 * Note: The actual implementation is assumed to exist elsewhere if not defined here.
 */
CREATE OR REPLACE FUNCTION ack_message(p_queue_name VARCHAR(255), p_message_id INT, p_consumer_token VARCHAR(64))
RETURNS VOID AS $$
BEGIN
  UPDATE queue_messages
  SET status = 'completed',
      processed_at = NOW(),
      consumer_token = NULL
  WHERE queue_name = p_queue_name
    AND message_id = p_message_id
    AND status = 'processing'
    AND consumer_token = p_consumer_token;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Ack failed: message not found, not in processing state, or token mismatch';
  END IF;
END;
$$ LANGUAGE plpgsql;

/* Function: nack_message
 *
 * Description:
 *   Performs a negative acknowledgment (nack) on a message. The function resets the message status
 *   to 'pending', clears the lock and consumer token, and sends a notification event. The topic
 *   is determined by looking up the corresponding queue.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (INT): Identifier of the message.
 *   - p_consumer_token (VARCHAR): The consumer token to verify the consumer.
 *   - p_delay_until (TIMESTAMPTZ): The timestamp until which the message should be delayed for redelivery.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION nack_message(
    p_queue_name VARCHAR(255),
    p_message_id INT,
    p_consumer_token VARCHAR(64),
    p_delay_until TIMESTAMPTZ DEFAULT NOW()
) RETURNS VOID AS $$
DECLARE
    published_queues TEXT[];
BEGIN
    UPDATE queue_messages
    SET status = 'pending',
        vt = p_delay_until,
        consumer_token = NULL
    WHERE queue_name = p_queue_name
        AND message_id = p_message_id
        AND status = 'processing'
        AND consumer_token = p_consumer_token;
        
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Nack failed: message not in processing state or token mismatch';
    END IF;
    
    published_queues := ARRAY[p_queue_name];
    
    PERFORM pg_notify('postgremq_events',
        json_build_object(
            'event', 'message_nacked',
            'queues', published_queues,
            'vt', p_delay_until
        )::text);
        
    RETURN;
END;
$$ LANGUAGE plpgsql;

/* Function: release_message
 *
 * Description:
 *   Releases the message back for delivery without increasing retry count. Should be used when client fetched the
 *   message but didn't make an attempt to process it, like for example buffered consumption.
 *   The function resets the message status
 *   to 'pending', clears the lock and consumer token, and sends a notification event.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (INT): Identifier of the message.
 *   - p_consumer_token (VARCHAR): The consumer token to verify the consumer.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION release_message(
    p_queue_name VARCHAR(255),
    p_message_id INT,
    p_consumer_token VARCHAR(64)
)
    RETURNS VOID AS $$
DECLARE
    published_queues TEXT[];
BEGIN
    UPDATE queue_messages
    SET status = 'pending',
        vt = NOW(),  -- Renamed from locked_until
        consumer_token = NULL,
        delivery_attempts = delivery_attempts - 1
    WHERE queue_name = p_queue_name
      AND message_id = p_message_id
      AND status = 'processing'
      AND consumer_token = p_consumer_token;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Release message failed: message not in processing state or token mismatch';
    END IF;

    published_queues := ARRAY[p_queue_name];

    PERFORM pg_notify('postgremq_events',
                      json_build_object('event', 'message_released', 'queues', published_queues)::text);

    RETURN;
END;
$$ LANGUAGE plpgsql;

/* Function: set_vt
 *
 * Description:
 *   Extends the lock time on a particular message, giving the consumer more time to process it.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (INT): Identifier of the message.
 *   - p_consumer_token (VARCHAR): The consumer token to verify the consumer.
 *   - p_vt (INT): Additional seconds to add to the current lock duration.
 *
 * Returns:
 *   TIMESTAMPTZ indicating new lock time.
 *
 * Note:
 *   The visibility timeout (p_vt) parameter is not bounded by this function.
 *   Callers should ensure reasonable values are used to prevent messages from
 *   being locked for excessive periods. Recommended maximum: 43200 seconds (12 hours).
 */
CREATE OR REPLACE FUNCTION set_vt(
    p_queue_name VARCHAR(255),
    p_message_id INTEGER,
    p_consumer_token VARCHAR(64),
    p_vt INTEGER
) RETURNS TIMESTAMPTZ AS $$
DECLARE
    v_vt TIMESTAMPTZ;
BEGIN
    UPDATE queue_messages
    SET vt = NOW() + make_interval(secs => p_vt)
    WHERE queue_name = p_queue_name
      AND message_id = p_message_id
      AND consumer_token = p_consumer_token
      AND status = 'processing'
      AND vt > NOW()
    RETURNING vt INTO v_vt;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Extend lock failed: message not in processing state or token mismatch';
    END IF;

    RETURN v_vt;
END;
$$ LANGUAGE plpgsql;

/* Function: move_messages_to_dlq
 *
 * Description:
 *   Moves messages from the active queue (queue_messages) into the dead letter queue if their
 *   delivery_attempts have reached/exceeded the maximum allowed by the corresponding queue.
 *   Only moves messages from queues with max_delivery_attempts > 0.
 *   Uses a single atomic CTE operation for efficiency.
 *
 * Parameters: None.
 *
 * Returns: INTEGER - number of messages moved to DLQ.
 */
CREATE OR REPLACE FUNCTION move_messages_to_dlq()
RETURNS INTEGER AS $$
DECLARE
    v_moved_count INTEGER;
BEGIN
    WITH deleted_messages AS (
        DELETE FROM queue_messages qm
        USING queues q
        WHERE qm.queue_name = q.name
          AND q.max_delivery_attempts > 0
          AND qm.delivery_attempts >= q.max_delivery_attempts
        RETURNING qm.queue_name, qm.message_id, qm.delivery_attempts
    )
    INSERT INTO dead_letter_queue(queue_name, message_id, retry_count)
    SELECT queue_name, message_id, delivery_attempts
    FROM deleted_messages;

    GET DIAGNOSTICS v_moved_count = ROW_COUNT;
    RETURN v_moved_count;
END;
$$ LANGUAGE plpgsql;

/* Function: extend_queue_keep_alive
 *
 * Description:
 *   Extends the keep-alive time for a non-durable queue by setting its expiration to
 *   NOW() plus the provided extension interval.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_interval (INTERVAL): The interval to add to NOW() for the new keep-alive timestamp.
 *
 * Returns:
 *   BOOLEAN indicating whether the update was successful.
 *
 * Note:
 *   Negative or zero intervals are not allowed.
 */
CREATE OR REPLACE FUNCTION extend_queue_keep_alive(
    p_queue_name VARCHAR(255),
    p_interval INTERVAL
) RETURNS BOOLEAN AS $$
BEGIN
    UPDATE queues
    SET keep_alive_until = NOW() + p_interval
    WHERE name = p_queue_name
      AND exclusive = true;  -- Changed from NOT durable
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

---------------------------
-- Management Functions
---------------------------

/* ---------------------------------------------------------------------
 * Management Functions
 * ---------------------------------------------------------------------
 * The following functions provide administrative and utility operations
 * for the message queue system. They allow you to list topics and queues,
 * obtain queue statistics, manage the dead letter queue (DLQ), and perform
 * cleanup or deletion of topics and queues.
 */

/* Function: list_topics
 *
 * Description:
 *   Retrieves a list of all topics in the system.
 *
 * Returns:
 *   A TABLE with one column:
 *     - topic (VARCHAR): The name of the topic.
 */
CREATE OR REPLACE FUNCTION list_topics()
RETURNS TABLE(topic VARCHAR(255)) AS $$
BEGIN
  RETURN QUERY
    SELECT topics.name AS topic
    FROM topics
    ORDER BY topics.name;
END;
$$ LANGUAGE plpgsql;

/* Function: list_queues
 *
 * Description:
 *   Retrieves a list of all queues in the system with details including the
 *   associated topic, maximum delivery attempts (max_delivery_attempts), durability,
 *   and the keep-alive expiration time.
 *
 * Returns:
 *   A TABLE with the following columns:
 *     - queue_name (VARCHAR): The name of the queue.
 *     - topic_name (VARCHAR): The associated topic name.
 *     - max_delivery_attempts (INT): Maximum delivery attempts (-1 indicates unlimited).
 *     - durable (BOOLEAN): Indicates if the queue is durable.
 *     - keep_alive_until (TIMESTAMPTZ): Expiration timestamp for non-durable queues.
 */
CREATE OR REPLACE FUNCTION list_queues()
RETURNS TABLE(
  queue_name VARCHAR(255),
  topic_name VARCHAR(255),
  max_delivery_attempts INT,
  exclusive BOOLEAN,  -- Changed from durable
  keep_alive_until TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
    SELECT 
      queues.name AS queue_name,
      queues.topic_name,
      queues.max_delivery_attempts,
      queues.exclusive,  -- Changed from durable
      queues.keep_alive_until
    FROM queues
    ORDER BY queues.name;
END;
$$ LANGUAGE plpgsql;

/* Function: get_queue_statistics
 *
 * Description:
 *   Provides message statistics for a specified queue or for all queues if none is specified.
 *
 * Parameters:
 *   - p_queue (VARCHAR, optional): The name of the queue to gather statistics for.
 *
 * Returns:
 *   A TABLE with the following columns:
 *     - pending_count (BIGINT): Number of messages with status 'pending'.
 *     - processing_count (BIGINT): Number of messages with status 'processing'.
 *     - completed_count (BIGINT): Number of messages with status 'completed'.
 *     - total_count (BIGINT): Total number of messages in the queue.
 */
CREATE OR REPLACE FUNCTION get_queue_statistics(p_queue VARCHAR(255) DEFAULT NULL)
RETURNS TABLE(
  pending_count BIGINT,
  processing_count BIGINT,
  completed_count BIGINT,
  total_count BIGINT
) AS $$
BEGIN
  RETURN QUERY
    SELECT 
      count(*) FILTER (WHERE qm.status = 'pending'),
      count(*) FILTER (WHERE qm.status = 'processing'),
      count(*) FILTER (WHERE qm.status = 'completed'),
      count(*)
    FROM queue_messages qm
    WHERE (p_queue IS NULL OR qm.queue_name = p_queue);
END;
$$ LANGUAGE plpgsql;

/* Function: list_dlq_messages
 *
 * Description:
 *   Retrieves messages that have been moved to the Dead Letter Queue (DLQ).
 *
 * Returns:
 *   A TABLE with the following columns:
 *     - queue_name (VARCHAR): Name of the queue from which the message was moved.
 *     - message_id (INT): Identifier of the message.
 *     - retry_count (INT): Number of delivery attempts made (as stored in DLQ).
 *     - published_at (TIMESTAMPTZ): Timestamp when the message was moved into the DLQ.
 */
CREATE OR REPLACE FUNCTION list_dlq_messages()
RETURNS TABLE(
  queue_name VARCHAR(255),
  message_id INT,
  retry_count INT,
  published_at TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
    SELECT dl.queue_name, dl.message_id, dl.retry_count, dl.published_at
    FROM dead_letter_queue dl
    ORDER BY dl.published_at;
END;
$$ LANGUAGE plpgsql;

/* Function: requeue_dlq_messages
 *
 * Description:
 *   Moves messages from the dead letter queue back to their original queues.
 *   The delivery_attempts counter is reset to 0 for these messages.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue to requeue messages for.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION requeue_dlq_messages(p_queue_name VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  WITH moved_messages AS (
    DELETE FROM dead_letter_queue dlq
    WHERE dlq.queue_name = p_queue_name
    RETURNING dlq.queue_name, dlq.message_id
  )
  INSERT INTO queue_messages(queue_name, message_id, status, delivery_attempts)
  SELECT queue_name, message_id, 'pending', 0
  FROM moved_messages;
END;
$$ LANGUAGE plpgsql;

/* Function: purge_dlq
 *
 * Description:
 *   Deletes all messages from the Dead Letter Queue (DLQ).
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION purge_dlq()
RETURNS VOID AS $$
BEGIN
  DELETE FROM dead_letter_queue;
END;
$$ LANGUAGE plpgsql;

/* Function: purge_all_messages
 *
 * Description:
 *   Deletes all messages from the system. This includes messages in both the DLQ and
 *   the primary messages table.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION purge_all_messages()
RETURNS VOID AS $$
BEGIN
  DELETE FROM dead_letter_queue;
  DELETE FROM queue_messages;
  DELETE FROM messages;
END;
$$ LANGUAGE plpgsql;

/* Function: delete_topic
 *
 * Description:
 *   Deletes a topic from the system. The topic cannot be deleted if any messages are associated
 *   with it. Use clean_up_topic to remove messages first if necessary.
 *
 * Parameters:
 *   - p_topic (VARCHAR): The name of the topic to be deleted.
 *
 * Returns: VOID.
 *
 * Raises:
 *   Exception if messages exist for the topic.
 */
CREATE OR REPLACE FUNCTION delete_topic(p_topic VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM messages WHERE topic_name = p_topic) THEN
    RAISE EXCEPTION 'Cannot delete topic "%" because messages exist. Clean up the topic first.', p_topic;
  END IF;
  DELETE FROM topics WHERE name = p_topic;
END;
$$ LANGUAGE plpgsql;

/* Function: delete_queue
 *
 * Description:
 *   Deletes a queue from the system.
 *
 * Parameters:
 *   - p_queue (VARCHAR): The name of the queue to be deleted.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION delete_queue(p_queue VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  DELETE FROM queues WHERE name = p_queue;
END;
$$ LANGUAGE plpgsql;

/* Function: delete_queue_message
 *
 * Description:
 *   Deletes a specific message from an active queue.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): The name of the queue.
 *   - p_message_id (INT): The identifier of the message to be deleted.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION delete_queue_message(p_queue_name VARCHAR(255), p_message_id INT)
RETURNS VOID AS $$
BEGIN
  DELETE FROM queue_messages
  WHERE queue_name = p_queue_name
    AND message_id = p_message_id;
END;
$$ LANGUAGE plpgsql;

/* Function: clean_up_queue
 *
 * Description:
 *   Removes all messages from a specified queue.
 *
 * Parameters:
 *   - p_queue (VARCHAR): The name of the queue to clean up.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION clean_up_queue(p_queue VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  DELETE FROM queue_messages WHERE queue_name = p_queue;
END;
$$ LANGUAGE plpgsql;

/* Function: clean_up_topic
 *
 * Description:
 *   Deletes all messages associated with a specified topic.
 *
 * Parameters:
 *   - p_topic (VARCHAR): The topic whose messages are to be removed.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION clean_up_topic(p_topic VARCHAR(255))
RETURNS VOID AS $$
BEGIN
  DELETE FROM messages WHERE topic_name = p_topic;
END;
$$ LANGUAGE plpgsql;

/* Function: delete_inactive_queues
 *
 * Description:
 *   Deletes non-durable queues that are inactive. A queue is considered inactive if it
 *   is non-durable and its keep_alive_until timestamp is either NULL or has already expired.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION delete_inactive_queues()
RETURNS VOID AS $$
BEGIN
  DELETE FROM queues
  WHERE exclusive = true  -- Changed from durable = false
    AND (keep_alive_until IS NULL OR keep_alive_until <= NOW());
END;
$$ LANGUAGE plpgsql;

---------------------------
-- End of Implementation Script
-- ============================================================

/* Function: set_vt_batch
 *
 * Description:
 *   Extends the visibility timeout for multiple messages in a single operation.
 *   Orders updates by message_id to prevent deadlocks when multiple consumers
 *   extend overlapping sets of messages.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_ids (INTEGER[]): Array of message IDs to extend.
 *   - p_consumer_tokens (VARCHAR[]): Array of consumer tokens (must match array order).
 *   - p_vt (INTEGER): New visibility timeout in seconds.
 *
 * Returns:
 *   TABLE of (message_id, vt) for successfully extended messages.
 */
CREATE OR REPLACE FUNCTION set_vt_batch(
    p_queue_name VARCHAR(255),
    p_message_ids INTEGER[],
    p_consumer_tokens VARCHAR[],
    p_vt INTEGER
) RETURNS TABLE (message_id INTEGER, vt TIMESTAMPTZ) AS $$
BEGIN
    RETURN QUERY
    WITH to_update AS (
        SELECT unnest(p_message_ids) AS msg_id,
               unnest(p_consumer_tokens) AS token
    ),
    -- Lock rows in consistent order to prevent deadlocks
    locked_rows AS (
        SELECT qm.queue_name, qm.message_id, qm.consumer_token
        FROM queue_messages qm
        JOIN to_update tu ON qm.message_id = tu.msg_id AND qm.consumer_token = tu.token
        WHERE qm.queue_name = p_queue_name
          AND qm.status = 'processing'
          AND qm.vt > NOW()
        ORDER BY qm.message_id
        FOR UPDATE
    )
    UPDATE queue_messages qm
    SET vt = NOW() + make_interval(secs => p_vt)
    FROM locked_rows lr
    WHERE qm.queue_name = lr.queue_name
      AND qm.message_id = lr.message_id
      AND qm.consumer_token = lr.consumer_token
    RETURNING qm.message_id, qm.vt;
END;
$$ LANGUAGE plpgsql;

/* Function: list_messages
 *
 * Description:
 *   Lists all messages in a queue without consuming them.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *
 * Returns:
 *   A TABLE with message details (excluding payload).
 */
CREATE OR REPLACE FUNCTION list_messages(p_queue_name VARCHAR(255))
RETURNS TABLE(
    message_id INT,
    status VARCHAR(16),
    published_at TIMESTAMPTZ,
    delivery_attempts INT,
    vt TIMESTAMPTZ,
    processed_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        qm.message_id,
        qm.status,
        qm.published_at,
        qm.delivery_attempts,
        qm.vt,
        qm.processed_at
    FROM queue_messages qm
    WHERE qm.queue_name = p_queue_name
    ORDER BY qm.published_at;
END;
$$ LANGUAGE plpgsql;

/* Function: get_message
 *
 * Description:
 *   Retrieves a single message by ID, including its payload.
 *
 * Parameters:
 *   - p_message_id (INT): ID of the message.
 *
 * Returns:
 *   A TABLE with message details and payload.
 */
CREATE OR REPLACE FUNCTION get_message(p_message_id INT)
RETURNS TABLE(
    message_id INT,
    topic_name VARCHAR(255),
    payload JSONB,
    published_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.id,
        m.topic_name,
        m.payload,
        m.published_at
    FROM messages m
    WHERE m.id = p_message_id;
END;
$$ LANGUAGE plpgsql;

/* Function: get_next_visible_time
 *
 * Description:
 *   Returns the timestamp when the next message will become visible for delivery
 *   in the specified queue. Only considers messages in 'pending' or 'processing' state
 *   that haven't exceeded their max delivery attempts.
 *   Optimized to use an index-only scan with LIMIT 1 instead of MIN() aggregation.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *
 * Returns:
 *   TIMESTAMPTZ indicating when the next message will be visible, or NULL if no messages.
 */
CREATE OR REPLACE FUNCTION get_next_visible_time(p_queue_name VARCHAR(255))
RETURNS TIMESTAMPTZ AS $$
DECLARE
    v_next_vt TIMESTAMPTZ;
BEGIN
    SELECT qm.vt INTO v_next_vt
    FROM queue_messages qm
    JOIN queues q ON q.name = qm.queue_name
    WHERE qm.queue_name = p_queue_name
      AND (qm.status = 'pending' OR qm.status = 'processing')
      AND (q.max_delivery_attempts = 0 OR qm.delivery_attempts < q.max_delivery_attempts)
    ORDER BY qm.vt ASC
    LIMIT 1;

    RETURN v_next_vt;
END;
$$ LANGUAGE plpgsql;

/* Function: cleanup_completed_messages
 *
 * Description:
 *   Removes completed messages older than the specified retention period.
 *   This function should be called periodically (e.g., via cron or scheduled task)
 *   to prevent unbounded growth of the queue_messages table.
 *
 * Parameters:
 *   - p_older_than_hours (INTEGER): Delete completed messages processed more than this many hours ago.
 *                                   Defaults to 24 hours.
 *
 * Returns:
 *   INTEGER - number of completed messages deleted.
 *
 * Example:
 *   -- Delete messages completed more than 24 hours ago
 *   SELECT cleanup_completed_messages();
 *
 *   -- Delete messages completed more than 7 days ago
 *   SELECT cleanup_completed_messages(168);
 */
CREATE OR REPLACE FUNCTION cleanup_completed_messages(
    p_older_than_hours INTEGER DEFAULT 24
) RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    DELETE FROM queue_messages
    WHERE status = 'completed'
      AND processed_at < NOW() - make_interval(hours => p_older_than_hours);

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;
