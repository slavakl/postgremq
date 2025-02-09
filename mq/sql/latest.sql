/*
 * PostgreSQL Message Queue System Implementation
 *
 * This file contains the implementation of the core functions for the message queue system.
 * Functions include message consumption, acknowledgment, negative acknowledgment (nack),
 * lock extension, and moving messages to a dead letter queue (DLQ).
 *
 * Functions:
 *   - consume_message: Retrieve and lock pending messages, incrementing delivery attempts.
 *   - ack_message: Acknowledge a message as completed.
 *   - nack_message: Negatively acknowledge a message, resetting its status for redelivery.
 *   - extend_lock_time: Extend the lock duration of a message.
 *   - move_messages_to_dlq: Move messages that exceeded max retries/delivery attempts to the DLQ.
 *   - extend_queue_keep_alive: Extend the keep-alive time for non-durable queues.
 *
 * Other topic and queue management functions (e.g., create_topic, create_queue, publish_message,
 * delete_topic, clean_up_topic, list_queues) are assumed to exist elsewhere.
 *
 * Author: [Your Name/Your Organization]
 * License: MIT License
 */


-- Topics table.
CREATE TABLE topics (
  name VARCHAR(255) PRIMARY KEY
);

-- Queues table.
CREATE TABLE queues (
  name VARCHAR(255) PRIMARY KEY,
  topic_name VARCHAR(255) REFERENCES topics(name) ON DELETE CASCADE,
  max_delivery_attempts INT DEFAULT 0,  -- 0 means unlimited attempts
  durable BOOLEAN DEFAULT true,
  keep_alive_until TIMESTAMPTZ  -- For non-durable queues; e.g., NOW() + interval '10 minutes'
);

-- Messages table: payload stored as JSONB.
CREATE TABLE messages (
  id SERIAL PRIMARY KEY,
  topic_name VARCHAR(255) REFERENCES topics(name),
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Queue Messages table.
-- Composite primary key: (queue_name, message_id).
CREATE TABLE queue_messages (
  queue_name VARCHAR(255) NOT NULL,
  message_id INT NOT NULL,  -- References messages(id)
  status VARCHAR(16) DEFAULT 'pending',  -- Allowed: 'pending', 'processing', 'completed'
  created_at TIMESTAMPTZ DEFAULT NOW(),
  locked_until TIMESTAMPTZ,
  delivery_attempts INT DEFAULT 0,  -- Changed from retry_count to delivery_attempts (default 0)
  consumer_token VARCHAR(64),
  processed_at TIMESTAMPTZ,
  PRIMARY KEY (queue_name, message_id)
);

-- Dead Letter Queue table.
-- Composite primary key: (queue_name, message_id).
CREATE TABLE dead_letter_queue (
  queue_name VARCHAR(255) NOT NULL,
  message_id INT NOT NULL,
  retry_count INT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (queue_name, message_id)
);

---------------------------
-- Trigger for Message Distribution
---------------------------
CREATE OR REPLACE FUNCTION distribute_message() 
RETURNS trigger AS $$
DECLARE
   published_queues TEXT[];
BEGIN
   WITH ins AS (
     INSERT INTO queue_messages(queue_name, message_id)
     SELECT q.name, NEW.id
     FROM queues q
     WHERE q.topic_name = NEW.topic_name
       AND (q.durable OR q.keep_alive_until > NOW())
     RETURNING queue_name
   )
   SELECT array_agg(queue_name) INTO published_queues FROM ins;
   
   PERFORM pg_notify('postgremq_events', 
      json_build_object('event','message_published','queues', published_queues)::text);
      
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_message_insert
AFTER INSERT ON messages
FOR EACH ROW
EXECUTE FUNCTION distribute_message();

---------------------------
-- Runtime API Functions
---------------------------

-- Create a topic.
CREATE OR REPLACE FUNCTION create_topic(p_topic VARCHAR(255))
RETURNS VARCHAR(255) AS $$
BEGIN
  INSERT INTO topics(name) VALUES (p_topic)
  ON CONFLICT (name) DO NOTHING;
  RETURN p_topic;
END;
$$ LANGUAGE plpgsql;

-- Create a queue.
CREATE OR REPLACE FUNCTION create_queue(
  p_queue VARCHAR(255), 
  p_topic VARCHAR(255),
  p_max_delivery_attempts INT DEFAULT 0,  -- Changed from max_retries
  p_durable BOOLEAN DEFAULT true,
  p_keep_alive_interval INTERVAL DEFAULT interval '10 minutes'
)
RETURNS VARCHAR(255) AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM topics WHERE name = p_topic) THEN
    RAISE EXCEPTION 'Topic "%" does not exist', p_topic;
  END IF;
  
  INSERT INTO queues(name, topic_name, max_delivery_attempts, durable, keep_alive_until)
  VALUES (p_queue, p_topic, p_max_delivery_attempts, p_durable,
          CASE WHEN p_durable THEN NULL ELSE NOW() + p_keep_alive_interval END)
  ON CONFLICT (name) DO UPDATE
       SET topic_name = EXCLUDED.topic_name,
           max_delivery_attempts = EXCLUDED.max_delivery_attempts,
           durable = EXCLUDED.durable,
           keep_alive_until = CASE 
                               WHEN EXCLUDED.durable THEN NULL 
                               ELSE NOW() + p_keep_alive_interval 
                             END;
  RETURN p_queue;
END;
$$ LANGUAGE plpgsql;

-- Publish a message.
CREATE OR REPLACE FUNCTION publish_message(p_topic VARCHAR(255), p_payload JSONB)
RETURNS VOID AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM topics WHERE name = p_topic) THEN
    RAISE EXCEPTION 'Topic "%" does not exist', p_topic;
  END IF;
  INSERT INTO messages(topic_name, payload)
  VALUES (p_topic, p_payload);  
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
 *   - p_lock_timeout (INTERVAL): The duration for which the message is locked.
 *   - p_limit (INT DEFAULT 1): Maximum number of messages to retrieve.
 *
 * Returns:
 *   A table of records with fields: queue_name, message_id, payload, consumer_token, delivery_attempts.
 */
CREATE OR REPLACE FUNCTION consume_message(
  p_queue_name VARCHAR(255),
  p_lock_timeout INTERVAL,
  p_limit INT DEFAULT 1
)
RETURNS TABLE(
  queue_name VARCHAR(255), 
  message_id INT, 
  payload JSONB, 
  consumer_token VARCHAR(64),
  delivery_attempts INT
) AS $$
BEGIN
  RETURN QUERY
  WITH target_queue AS (
    SELECT name, max_delivery_attempts
    FROM queues
    WHERE name = p_queue_name
      AND (durable OR keep_alive_until > NOW())
  ),
  next_msg AS (
    SELECT qm.queue_name,
           qm.message_id,
           qm.status,
           qm.delivery_attempts,
           qm.created_at
    FROM queue_messages qm
    CROSS JOIN target_queue tq
    WHERE qm.queue_name = tq.name
      /* Allow consumption if max_delivery_attempts is 0 (unlimited) or not exceeded */
      AND (tq.max_delivery_attempts = 0 OR qm.delivery_attempts < tq.max_delivery_attempts)
      /* Message must be pending or processing with an expired lock */
      AND (qm.status = 'pending' OR (qm.status = 'processing' AND qm.locked_until < NOW()))
    ORDER BY qm.created_at
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  )
  UPDATE queue_messages
  SET status = 'processing',
      locked_until = NOW() + p_lock_timeout,
      delivery_attempts = qm.delivery_attempts + 1,
      consumer_token = to_char(NOW(), 'YYYYMMDDHH24MISS.MS') || '-' || substr(md5(random()::text), 1, 6)
  FROM next_msg qm
  WHERE queue_messages.queue_name = qm.queue_name
    AND queue_messages.message_id = qm.message_id
  RETURNING queue_messages.queue_name,
            queue_messages.message_id,
            (SELECT m.payload FROM messages m WHERE m.id = queue_messages.message_id) AS payload,
            queue_messages.consumer_token,
            queue_messages.delivery_attempts;
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
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION nack_message(
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
      locked_until = NULL,
      consumer_token = NULL
  WHERE queue_name = p_queue_name
    AND message_id = p_message_id
    AND status = 'processing'
    AND consumer_token = p_consumer_token;
    
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Nack failed: message not in processing state or token mismatch';
  END IF;
  
  -- Since the nack is only applied to a single queue, use that queue name.
  published_queues := ARRAY[p_queue_name];

  PERFORM pg_notify('postgremq_events',
     json_build_object('event', 'message_nacked', 'queues', published_queues)::text);
     
  RETURN;
END;
$$ LANGUAGE plpgsql;

/* Function: extend_lock_time
 *
 * Description:
 *   Extends the lock time on a particular message, giving the consumer more time to process it.
 *
 * Parameters:
 *   - p_queue_name (VARCHAR): Name of the queue.
 *   - p_message_id (INT): Identifier of the message.
 *   - p_extra_seconds (INT): Additional seconds to add to the current lock duration.
 *
 * Returns:
 *   BOOLEAN indicating if the lock was successfully extended.
 */
CREATE OR REPLACE FUNCTION extend_lock_time(p_queue_name VARCHAR(255), p_message_id INT, p_extra_seconds INT)
RETURNS BOOLEAN AS $$
BEGIN
  UPDATE queue_messages
  SET locked_until = locked_until + make_interval(secs => p_extra_seconds)
  WHERE queue_name = p_queue_name
    AND message_id = p_message_id
    AND locked_until > NOW();
  RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

/* Function: move_messages_to_dlq
 *
 * Description:
 *   Moves messages from the active queue (queue_messages) into the dead letter queue if their
 *   delivery_attempts have reached/exceeded the maximum allowed by the corresponding queue.
 *   Only moves messages from queues with max_delivery_attempts > 0.
 *
 * Parameters: None.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION move_messages_to_dlq()
RETURNS VOID AS $$
BEGIN
   INSERT INTO dead_letter_queue(queue_name, message_id, retry_count)
   SELECT qm.queue_name, qm.message_id, qm.delivery_attempts
   FROM queue_messages qm
   JOIN queues q ON qm.queue_name = q.name
   WHERE q.max_delivery_attempts > 0 
     AND qm.delivery_attempts >= q.max_delivery_attempts;

   DELETE FROM queue_messages 
   WHERE (queue_name, message_id) IN (
           SELECT queue_name, message_id FROM dead_letter_queue
         );
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
 *   - p_extension (INTERVAL): The interval to add to NOW() for the new keep-alive timestamp.
 *
 * Returns:
 *   BOOLEAN indicating whether the update was successful.
 *
 * Note:
 *   Negative or zero intervals are not allowed.
 */
CREATE OR REPLACE FUNCTION extend_queue_keep_alive(p_queue_name VARCHAR(255), p_extension INTERVAL)
RETURNS BOOLEAN AS $$
BEGIN
   IF p_extension <= interval '0' THEN
     RAISE EXCEPTION 'Negative or zero interval not allowed.';
   END IF;

   UPDATE queues 
   SET keep_alive_until = NOW() + p_extension
   WHERE name = p_queue_name AND durable = FALSE;

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
  durable BOOLEAN,
  keep_alive_until TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
    SELECT 
      queues.name AS queue_name,
      queues.topic_name,
      queues.max_delivery_attempts,
      queues.durable,
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
 *     - created_at (TIMESTAMPTZ): Timestamp when the message was moved into the DLQ.
 */
CREATE OR REPLACE FUNCTION list_dlq_messages()
RETURNS TABLE(
  queue_name VARCHAR(255),
  message_id INT,
  retry_count INT,
  created_at TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
    SELECT dl.queue_name, dl.message_id, dl.retry_count, dl.created_at
    FROM dead_letter_queue dl
    ORDER BY dl.created_at;
END;
$$ LANGUAGE plpgsql;

/* Function: requeue_dlq_messages
 *
 * Description:
 *   Requeues messages from the Dead Letter Queue back to their active queues
 *   (i.e., marks them as 'pending') and then removes them from the DLQ.
 *
 * Parameters:
 *   - p_queue (VARCHAR, optional): Limit the requeue operation to a specific queue.
 *
 * Returns: VOID.
 */
CREATE OR REPLACE FUNCTION requeue_dlq_messages(p_queue VARCHAR(255) DEFAULT NULL)
RETURNS VOID AS $$
BEGIN
  INSERT INTO queue_messages(queue_name, message_id, status, created_at, retry_count)
  SELECT queue_name, message_id, 'pending', NOW(), retry_count
  FROM dead_letter_queue
  WHERE (p_queue IS NULL OR queue_name = p_queue);
  
  DELETE FROM dead_letter_queue
  WHERE (p_queue IS NULL OR queue_name = p_queue);
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
  WHERE durable = false AND (keep_alive_until IS NULL OR keep_alive_until <= NOW());
END;
$$ LANGUAGE plpgsql;

---------------------------
-- End of Implementation Script
-- ============================================================
