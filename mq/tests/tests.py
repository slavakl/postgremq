import os
from pathlib import Path
import pytest
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from typing import Generator, Any
import uuid
import pytz
from testcontainers.postgres import PostgresContainer
import time
from datetime import timezone
import json

# Get the path to latest.sql relative to this test file
SQL_FILE = Path(__file__).parent.parent / 'sql' / 'latest.sql'

@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Create a PostgreSQL container for testing."""
    postgres = PostgresContainer(
        "postgres:15"
    )
    
    postgres.start()
    yield postgres
    postgres.stop()

@pytest.fixture(scope="session")
def db_config(postgres_container: PostgresContainer) -> dict[str, str]:
    """Get database configuration from the test container."""
    return {
        'dbname': postgres_container.dbname,
        'user': postgres_container.username,
        'password': postgres_container.password,
        'host': postgres_container.get_container_host_ip(),
        'port': postgres_container.get_exposed_port(5432),
    }

@pytest.fixture(scope="session")
def test_db_name() -> str:
    """Generate a unique test database name."""
    return f"mq_test_{uuid.uuid4().hex[:8]}"

@pytest.fixture(scope="session")
def admin_conn(db_config: dict[str, str]) -> Generator[psycopg2.extensions.connection, None, None]:
    """Create a connection with admin privileges for database creation/deletion."""
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    yield conn
    conn.close()

@pytest.fixture(scope="session")
def test_db(admin_conn: psycopg2.extensions.connection, test_db_name: str) -> Generator[str, None, None]:
    """Create and drop the test database."""
    # Drop database if it exists
    cur = admin_conn.cursor()
    try:
        cur.execute(f"DROP DATABASE IF EXISTS {test_db_name} WITH (FORCE)")
        
        # Create fresh database
        cur.execute(f"CREATE DATABASE {test_db_name}")
        cur.close()
        
        yield test_db_name
        
    finally:
        # Cleanup - will run even if tests fail
        cur = admin_conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {test_db_name} WITH (FORCE)")
        cur.close()

@pytest.fixture(scope="function")
def conn(test_db: str, db_config: dict[str, str]) -> Generator[psycopg2.extensions.connection, None, None]:
    """Create a connection to the test database for each test."""
    config = db_config.copy()
    config['dbname'] = test_db
    conn = psycopg2.connect(**config)
    conn.autocommit = True
    
    # Initialize schema and functions
    with conn.cursor() as cur:
        # First drop existing objects if they exist
        cur.execute("""
            DROP TABLE IF EXISTS dead_letter_queue CASCADE;
            DROP TABLE IF EXISTS queue_messages CASCADE;
            DROP TABLE IF EXISTS queues CASCADE;
            DROP TABLE IF EXISTS messages CASCADE;
            DROP TABLE IF EXISTS topics CASCADE;
        """)
        
        # Load MQ implementation
        with open(SQL_FILE, 'r') as f:
            cur.execute(f.read())
    
    yield conn
    conn.close()

@pytest.fixture(scope="function")
def cur(conn: psycopg2.extensions.connection) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Create a cursor for each test."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield cur
    cur.close()

def test_topic_and_queue_creation(cur: psycopg2.extensions.cursor) -> None:
    """Test basic topic and queue creation functionality."""
    cur.execute("""
        SELECT create_topic('TestTopic');
        SELECT create_queue('TestQueue', 'TestTopic', 3, false);
        SELECT create_queue('TestQueue_Ex', 'TestTopic', 2, true, 300);
    """)
    
    # Verify queues were created
    cur.execute("SELECT count(*) FROM queues WHERE topic_name = 'TestTopic'")
    assert cur.fetchone()[0] == 2

    # Verify keep-alive for exclusive queue
    cur.execute("""
        SELECT keep_alive_until 
        FROM queues 
        WHERE name = 'TestQueue_Ex'
    """)
    keep_alive = cur.fetchone()[0]
    now = datetime.now(pytz.UTC)
    assert keep_alive > now + timedelta(minutes=4)
    assert keep_alive < now + timedelta(minutes=6)

    cur.execute("""
        SELECT * FROM consume_message('TestQueue', 30)
    """)

def test_queue_keep_alive_extension(cur: psycopg2.extensions.cursor) -> None:
    """Test queue keep-alive extension functionality."""
    # Setup
    cur.execute("""
        SELECT create_topic('TestTopic');
        SELECT create_queue('TestQueue_Ex', 'TestTopic', 2, true, 300);
        SELECT create_queue('TestQueue_NonEx', 'TestTopic', 2, false);
    """)

    # Get initial keep-alive and extend it
    cur.execute("""
        SELECT keep_alive_until FROM queues WHERE name = 'TestQueue_Ex';
        SELECT extend_queue_keep_alive('TestQueue_Ex', interval '15 minutes');
    """)
    assert cur.fetchone()[0] is True

    # Verify keep-alive was extended
    cur.execute("""
        SELECT keep_alive_until 
        FROM queues 
        WHERE name = 'TestQueue_Ex'
    """)
    new_keep_alive = cur.fetchone()[0]
    now = datetime.now(pytz.UTC)
    assert new_keep_alive > now + timedelta(minutes=14)
    assert new_keep_alive < now + timedelta(minutes=16)

def test_exclusive_queue_uniqueness(cur: psycopg2.extensions.cursor) -> None:
    """Test that exclusive queues must have unique names."""
    cur.execute("SELECT create_topic('TestTopic')")
    
    # Create first exclusive queue
    cur.execute("""
        SELECT create_queue('ExQueue1', 'TestTopic', 2, true, 300)
    """)
    
    # Attempt to create second exclusive queue with same name
    with pytest.raises(Exception) as exc_info:
        cur.execute("""
            SELECT create_queue('ExQueue1', 'TestTopic', 2, true, 300)
        """)
    assert "already exists" in str(exc_info.value)
    
    # Verify we can call create twice for non-exclusive queues
    cur.execute("""
        SELECT create_queue('NonExQueue1', 'TestTopic', 2, false);
        SELECT create_queue('NonExQueue2', 'TestTopic', 2, false);
    """)

def test_unlimited_delivery_attempts(cur: psycopg2.extensions.cursor) -> None:
    """Test queue with unlimited delivery attempts (max_delivery_attempts = 0)."""
    # Setup
    cur.execute("""
        SELECT create_topic('UnlimitedTopic');
        SELECT create_queue('UnlimitedQueue', 'UnlimitedTopic', 0, false);
        SELECT publish_message('UnlimitedTopic', '{"test": "unlimited"}'::jsonb);
    """)

    # Try multiple delivery attempts
    for i in range(5):
        cur.execute("""
            SELECT message_id, consumer_token, delivery_attempts
            FROM consume_message('UnlimitedQueue', 60, 1)
        """)
        msg_id, token, attempts = cur.fetchone()
        assert msg_id is not None, f"Message should be available for consumption on attempt {i+1}"
        assert attempts == i + 1, f"Delivery attempts should be {i+1}"
        
        cur.execute("SELECT nack_message('UnlimitedQueue', %s, %s)", (msg_id, token))

    # Verify message wasn't moved to DLQ
    cur.execute("""
        SELECT move_messages_to_dlq();
        SELECT count(*) 
        FROM dead_letter_queue dlq 
        WHERE dlq.queue_name = 'UnlimitedQueue';
    """)
    assert cur.fetchone()[0] == 0

def test_default_keep_alive_for_exclusive_queue(cur: psycopg2.extensions.cursor) -> None:
    """Test that exclusive queues get default 30-second keep-alive."""
    cur.execute("""
        SELECT create_topic('TestTopic');
        SELECT create_queue('ExQueue', 'TestTopic', 2, true);
    """)
    
    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'ExQueue'")
    keep_alive = cur.fetchone()[0]
    now = datetime.now(pytz.UTC)
    assert keep_alive > now + timedelta(seconds=25)
    assert keep_alive < now + timedelta(seconds=35)

def test_non_exclusive_queue_keep_alive_ignored(cur: psycopg2.extensions.cursor) -> None:
    """Test that keep_alive is ignored for non-exclusive queues."""
    cur.execute("""
        SELECT create_topic('TestTopic');
        SELECT create_queue('NonExQueue', 'TestTopic', 2, false, 300);
    """)
    
    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'NonExQueue'")
    assert cur.fetchone()[0] is None

def test_message_delivery_and_acknowledgment(cur: psycopg2.extensions.cursor) -> None:
    """Test message publishing, consumption, and acknowledgment."""
    # Setup
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 3, true)")
    
    # Publish message
    cur.execute(
        "SELECT publish_message('TestTopic', '{\"test\": \"ack_nack\"}'::jsonb)"
    )

    # Consume message
    cur.execute("""
        SELECT message_id, consumer_token, payload 
        FROM consume_message('TestQueue', 300, 1)
    """)
    result = cur.fetchone()
    assert result is not None
    msg_id, consumer_token = result['message_id'], result['consumer_token']

    # Verify message is locked
    cur.execute("""
        SELECT status 
        FROM queue_messages qm 
        WHERE queue_name = 'TestQueue' AND message_id = %s
    """, (msg_id,))
    assert cur.fetchone()[0] == 'processing'

    # Acknowledge message
    cur.execute(
        "SELECT ack_message('TestQueue', %s, %s)",
        (msg_id, consumer_token)
    )

    # Verify message is completed
    cur.execute("""
        SELECT status 
        FROM queue_messages qm 
        WHERE queue_name = 'TestQueue' AND message_id = %s
    """, (msg_id,))
    assert cur.fetchone()[0] == 'completed'

def test_message_retry_behavior(cur: psycopg2.extensions.cursor) -> None:
    """Test message delivery attempts behavior and dead letter queue functionality."""
    # Setup - queue with max 2 delivery attempts (initial + 1 retry)
    cur.execute("""
        SELECT create_topic('RetryTopic');
        SELECT create_queue('RetryQueue', 'RetryTopic', 2, true);
        SELECT publish_message('RetryTopic', '{"test": "retry"}'::jsonb);
    """)

    # First delivery attempt
    cur.execute("""
        SELECT message_id, consumer_token 
        FROM consume_message('RetryQueue', 60, 1)
    """)
    msg_id, token = cur.fetchone()
    cur.execute("SELECT nack_message('RetryQueue', %s, %s)", (msg_id, token))

    # Second delivery attempt
    cur.execute("""
        SELECT message_id, consumer_token 
        FROM consume_message('RetryQueue', 60, 1)
    """)
    msg_id, token = cur.fetchone()
    cur.execute("SELECT nack_message('RetryQueue', %s, %s)", (msg_id, token))

    # Verify no more attempts available
    cur.execute("""
        SELECT count(*) 
        FROM consume_message('RetryQueue', 60, 1)
    """)
    assert cur.fetchone()[0] == 0

    # Move to DLQ and verify
    cur.execute("""
        SELECT move_messages_to_dlq();
        SELECT count(*), MAX(retry_count) 
        FROM dead_letter_queue dlq 
        WHERE dlq.queue_name = 'RetryQueue';
    """)
    count, retries = cur.fetchone()
    assert count == 1
    assert retries == 2

def test_queue_creation_with_delivery_attempts(cur: psycopg2.extensions.cursor) -> None:
    """Test queue creation with different max_delivery_attempts values."""
    cur.execute("""
        SELECT create_topic('DeliveryTopic');
        SELECT create_queue('UnlimitedQueue', 'DeliveryTopic', 0, true);
        SELECT create_queue('LimitedQueue', 'DeliveryTopic', 3, true);
        
        SELECT name, max_delivery_attempts 
        FROM queues 
        WHERE topic_name = 'DeliveryTopic' 
        ORDER BY name;
    """)
    
    results = cur.fetchall()
    assert len(results) == 2
    assert results[0]['name'] == 'LimitedQueue'
    assert results[0]['max_delivery_attempts'] == 3
    assert results[1]['name'] == 'UnlimitedQueue'
    assert results[1]['max_delivery_attempts'] == 0

def test_concurrent_message_access(cur: psycopg2.extensions.cursor) -> None:
    """Test concurrent access to messages."""
    # Setup
    cur.execute("SELECT create_topic('ConcurrentTopic')")
    cur.execute("SELECT create_queue('ConcurrentQueue', 'ConcurrentTopic', 3, true)")
    cur.execute(
        "SELECT publish_message('ConcurrentTopic', '{\"test\": \"concurrent\"}'::jsonb)"
    )

    # First consumer gets the message
    cur.execute("""
        SELECT message_id, consumer_token 
        FROM consume_message('ConcurrentQueue', 60, 1)
    """)
    assert cur.fetchone() is not None

    # Second consumer should get nothing
    cur.execute("""
        SELECT count(*) 
        FROM consume_message('ConcurrentQueue', 60, 1)
    """)
    assert cur.fetchone()[0] == 0

def test_queue_cleanup(cur: psycopg2.extensions.cursor) -> None:
    """Test queue cleanup functionality."""
    # Setup
    cur.execute("SELECT create_topic('CleanupTopic')")
    cur.execute("SELECT create_queue('CleanupQueue', 'CleanupTopic', 3, true)")
    cur.execute(
        "SELECT publish_message('CleanupTopic', '{\"test\": \"cleanup\"}'::jsonb)"
    )

    # Clean up topic
    cur.execute("SELECT clean_up_topic('CleanupTopic')")
    cur.execute("SELECT delete_topic('CleanupTopic')")

    # Verify cleanup
    cur.execute(
        "SELECT count(*) FROM queues WHERE topic_name = 'CleanupTopic'"
    )
    assert cur.fetchone()[0] == 0
    cur.execute(
        "SELECT count(*) FROM messages WHERE topic_name = 'CleanupTopic'"
    )
    assert cur.fetchone()[0] == 0

def test_set_vt(cur: psycopg2.extensions.cursor) -> None:
    """Test setting visibility timeout for a single message."""
    # Create test topic and queue
    cur.execute("SELECT create_topic('test_topic')")
    cur.execute("SELECT create_queue('test_queue', 'test_topic')")
    
    # Publish a test message
    cur.execute("SELECT publish_message('test_topic', '{\"test\": \"data\"}'::jsonb)")
    
    # Consume the message to get it into processing state
    cur.execute("""
        SELECT queue_name, message_id, consumer_token 
        FROM consume_message('test_queue', 30)
    """)
    message = cur.fetchone()
    queue_name, message_id, consumer_token = message
    
    # Test valid extension
    cur.execute("""
        SELECT set_vt(%s, %s, %s, 60)
    """, (queue_name, message_id, consumer_token))
    new_vt = cur.fetchone()[0]
    assert new_vt > datetime.now(pytz.UTC)
    assert new_vt < datetime.now(pytz.UTC) + timedelta(seconds=61)
    
    # Test wrong consumer token
    with pytest.raises(Exception):
        cur.execute("""
            SELECT set_vt(%s, %s, %s, 60)
        """, (queue_name, message_id, 'wrong-token'))
    
    # Test wrong message ID
    with pytest.raises(Exception):
        cur.execute("""
            SELECT set_vt(%s, %s, %s, 60)
        """, (queue_name, message_id + 1, consumer_token))

def test_set_vt_batch(cur: psycopg2.extensions.cursor) -> None:
    """Test batch extension of message visibility timeouts."""
    # Setup
    cur.execute("SELECT create_topic('BatchTopic')")
    cur.execute("SELECT create_queue('BatchQueue', 'BatchTopic', 3, true)")
    
    # Publish messages
    for _ in range(3):
        cur.execute(
            "SELECT publish_message('BatchTopic', '{\"test\": \"batch\"}'::jsonb)"
        )
    
    # Consume messages
    cur.execute("""
        SELECT message_id, consumer_token
        FROM consume_message('BatchQueue', 300, 3)
    """)
    messages = cur.fetchall()
    
    msg_ids = [m['message_id'] for m in messages]
    tokens = [m['consumer_token'] for m in messages]
    
    # Test valid extension
    cur.execute("""
        SELECT message_id 
        FROM set_vt_batch('BatchQueue', %s, %s, 60)
    """, (msg_ids, tokens))
    extended_ids = [r[0] for r in cur.fetchall()]
    assert len(extended_ids) == len(msg_ids)
    
    # Test with wrong tokens
    wrong_tokens = ['wrong-token' for _ in tokens]
    cur.execute("""
        SELECT COUNT(*) 
        FROM set_vt_batch('BatchQueue', %s, %s, 60)
    """, (msg_ids, wrong_tokens))
    assert cur.fetchone()[0] == 0

def test_set_vt_batch_comprehensive(cur: psycopg2.extensions.cursor) -> None:
    """Test batch extension of message visibility timeouts with more edge cases."""
    # Setup
    cur.execute("SELECT create_topic('BatchCompTopic')")
    cur.execute("SELECT create_queue('BatchCompQueue', 'BatchCompTopic', 3, true)")
    
    # Publish messages
    for i in range(5):
        cur.execute(
            "SELECT publish_message('BatchCompTopic', %s::jsonb)",
            (json.dumps({"test": f"batch-{i}"}),)
        )
    
    # Consume messages
    cur.execute("""
        SELECT message_id, consumer_token
        FROM consume_message('BatchCompQueue', 300, 5)
    """)
    messages = cur.fetchall()
    
    msg_ids = [m['message_id'] for m in messages]
    tokens = [m['consumer_token'] for m in messages]
    
    # Test 1: Empty arrays
    cur.execute("""
        SELECT COUNT(*) 
        FROM set_vt_batch('BatchCompQueue', %s, %s, 60)
    """, ([], []))
    assert cur.fetchone()[0] == 0
    
    # Test 2: Subset of messages
    subset_ids = msg_ids[0:2]
    subset_tokens = tokens[0:2]
    cur.execute("""
        SELECT COUNT(*) 
        FROM set_vt_batch('BatchCompQueue', %s, %s, 60)
    """, (subset_ids, subset_tokens))
    assert cur.fetchone()[0] == 2
    
    # Test 3: Mismatched array lengths - should return correct number of matches
    truncated_tokens = tokens[:-1]
    cur.execute("""
        SELECT COUNT(*) 
        FROM set_vt_batch('BatchCompQueue', %s, %s, 60)
    """, (msg_ids, truncated_tokens))
    # The actual implementation appears to match as many as it can
    assert cur.fetchone()[0] == 4
    
    # Test 4: Non-existent queue
    cur.execute("""
        SELECT COUNT(*) 
        FROM set_vt_batch('NonExistentQueue', %s, %s, 60)
    """, (msg_ids, tokens))
    
    # Test 5: Negative VT value
    cur.execute("""
        SELECT COUNT(*) 
        FROM set_vt_batch('BatchCompQueue', %s, %s, -60)
    """, (msg_ids, tokens))
    
    # After setting negative VT, they should all be expired
    time.sleep(0.1)  # Small delay to ensure they expire
    
    # Messages should be visible again
    cur.execute("""
        SELECT COUNT(*) 
        FROM consume_message('BatchCompQueue', 30, 5)
    """)
    assert cur.fetchone()[0] == 5
    
    # Clean up
    cur.execute("DELETE FROM queues WHERE name = 'BatchCompQueue'")
    cur.execute("DELETE FROM topics WHERE name = 'BatchCompTopic'")

def test_requeue_dlq_messages_resets_delivery_attempts(cur):
    # Create topic and queue
    topic = "test_requeue_dlq_topic"
    queue = "test_requeue_dlq_queue"
    cur.execute("SELECT create_topic(%s)", (topic,))
    cur.execute("SELECT create_queue(%s, %s, %s, %s, %s)",
                  (queue, topic, 2, True, 60))  # Changed from '60 seconds' to 60

    # Publish a message
    cur.execute("SELECT publish_message(%s, %s)", 
                  (topic, '{"test":"requeue"}'))
    cur.execute("SELECT id FROM messages ORDER BY id DESC LIMIT 1")
    msg_id = cur.fetchone()[0]

    # Consume and fail the message until it reaches max attempts
    for _ in range(2):  # max_delivery_attempts = 2
        cur.execute("""
            SELECT queue_name, message_id, payload, consumer_token 
            FROM consume_message(%s, 30)
        """, (queue,))
        result = cur.fetchone()
        assert result is not None, "Should get a message"
        
        cur.execute("""
            SELECT nack_message(%s, %s, %s)
        """, (queue, msg_id, result['consumer_token']))  # Use dictionary access for DictCursor

    # Move messages to DLQ
    cur.execute("SELECT move_messages_to_dlq()")

    # Verify message moved to DLQ
    cur.execute("SELECT message_id FROM dead_letter_queue WHERE queue_name = %s", (queue,))
    dlq_messages = cur.fetchall()
    assert len(dlq_messages) == 1, "Message should be in DLQ"
    assert dlq_messages[0][0] == msg_id, "DLQ message ID mismatch"

    # Verify message is no longer in queue_messages
    cur.execute("""
        SELECT COUNT(*) FROM queue_messages 
        WHERE queue_name = %s AND message_id = %s
    """, (queue, msg_id))
    count = cur.fetchone()[0]
    assert count == 0, "Message should not be in queue_messages"

    # Requeue the message
    cur.execute("SELECT requeue_dlq_messages(%s)", (queue,))

    # Verify message is back in queue with reset delivery attempts
    cur.execute("""
        SELECT message_id, delivery_attempts 
        FROM queue_messages 
        WHERE queue_name = %s AND message_id = %s
    """, (queue, msg_id))
    result = cur.fetchone()
    assert result is not None, "Message should be back in queue"
    assert result[0] == msg_id, "Message ID mismatch"
    assert result[1] == 0, "Delivery attempts should be reset to 0"

    # Verify DLQ is empty
    cur.execute("SELECT COUNT(*) FROM dead_letter_queue WHERE queue_name = %s", (queue,))
    dlq_count = cur.fetchone()[0]
    assert dlq_count == 0, "DLQ should be empty after requeue"

def test_delayed_message_delivery_notifications(cur: psycopg2.extensions.cursor) -> None:
    """Test that notifications include correct VT for delayed messages."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 2, false)")
    
    # Listen for notifications
    cur.execute("LISTEN postgremq_events")
    
    # Publish message with 2 second delay
    delay_time = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT publish_message('TestTopic', '{"test":"data"}'::jsonb, %s)
    """, (delay_time,))
    
    # Get notification
    conn = cur.connection
    conn.poll()
    notify = conn.notifies.pop(0)
    
    notification = json.loads(notify.payload)
    assert notification['event'] == 'message_published'
    assert 'TestQueue' in notification['queues']
    assert abs(parse_timestamp(notification['vt']) - delay_time) < timedelta(milliseconds=100)
    
    # Try to consume immediately - should get no messages
    cur.execute("""
        SELECT * FROM consume_message('TestQueue', 30)
    """)
    assert cur.fetchone() is None
    
    # Wait for delay
    time.sleep(2)
    
    # Now should get the message
    cur.execute("""
        SELECT * FROM consume_message('TestQueue', 30)
    """)
    assert cur.fetchone() is not None

def test_delayed_nack_notifications(cur: psycopg2.extensions.cursor) -> None:
    """Test that notifications include correct VT for nacked messages."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 2, false)")
    
    # Publish and consume message
    cur.execute("SELECT publish_message('TestTopic', '{\"test\":\"data\"}'::jsonb)")
    cur.execute("""
        SELECT message_id, consumer_token 
        FROM consume_message('TestQueue', 30)
    """)
    msg_id, token = cur.fetchone()
    
    # Listen for notifications
    cur.execute("LISTEN postgremq_events")
    
    # Nack with 2 second delay
    delay_time = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT nack_message('TestQueue', %s, %s, %s)
    """, (msg_id, token, delay_time))
    
    # Get notification
    conn = cur.connection
    conn.poll()
    notify = conn.notifies.pop(0)
    
    notification = json.loads(notify.payload)
    assert notification['event'] == 'message_nacked'
    assert 'TestQueue' in notification['queues']
    assert abs(parse_timestamp(notification['vt']) - delay_time) < timedelta(milliseconds=100)
    
    # Try to consume immediately - should get no messages
    cur.execute("""
        SELECT * FROM consume_message('TestQueue', 30)
    """)
    assert cur.fetchone() is None
    
    # Wait for delay
    time.sleep(2)
    
    # Now should get the message
    cur.execute("""
        SELECT * FROM consume_message('TestQueue', 30)
    """)
    msg = cur.fetchone()
    assert msg is not None
    assert msg[1] == msg_id  # message_id matches

def parse_timestamp(ts_str: str) -> datetime:
    """Parse timestamp from notification."""
    return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))

def test_get_next_visible_time(cur: psycopg2.extensions.cursor) -> None:
    """Test getting next message visibility time."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 2, false)")
    
    # When no messages, should return NULL
    cur.execute("SELECT get_next_visible_time('TestQueue')")
    assert cur.fetchone()[0] is None
    
    # Publish message with 2 second delay
    delay_time = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT publish_message('TestTopic', '{"test":"data"}'::jsonb, %s)
    """, (delay_time,))
    
    # Should return the delay time
    cur.execute("SELECT get_next_visible_time('TestQueue')")
    next_time = cur.fetchone()[0]
    assert abs(next_time - delay_time) < timedelta(milliseconds=100)
    
    # Publish immediate message
    cur.execute("""
        SELECT publish_message('TestTopic', '{"test":"immediate"}'::jsonb)
    """)
    
    # Should return current time (immediate message)
    cur.execute("SELECT get_next_visible_time('TestQueue')")
    next_time = cur.fetchone()[0]
    assert abs(next_time - datetime.now(pytz.UTC)) < timedelta(seconds=1)
    
    # Consume one message
    cur.execute("SELECT * FROM consume_message('TestQueue', 30)")
    msg = cur.fetchone()
    
    # Should still return delay_time for the delayed message
    cur.execute("SELECT get_next_visible_time('TestQueue')")
    next_time = cur.fetchone()[0]
    assert abs(next_time - delay_time) < timedelta(milliseconds=100)

def test_consume_message_published_at(cur: psycopg2.extensions.cursor) -> None:
    """Test that consume_message returns published_at timestamp."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 2, false)")
    
    # Publish a message and record approximate time
    before_publish = datetime.now(pytz.UTC)
    cur.execute("SELECT publish_message('TestTopic', '{\"test\":\"data\"}'::jsonb)")
    after_publish = datetime.now(pytz.UTC)
    
    # Consume the message
    cur.execute("SELECT * FROM consume_message('TestQueue', 30)")
    msg = cur.fetchone()
    
    # Verify published_at is set and within the expected timeframe
    assert msg['published_at'] is not None
    assert abs(msg['published_at'] - before_publish) < timedelta(milliseconds=100)
    assert abs(msg['published_at'] - after_publish) < timedelta(milliseconds=100)

def test_cascade_behavior(cur: psycopg2.extensions.cursor) -> None:
    """Test that CASCADE relationships work correctly between tables."""
    # Setup - Create test data
    cur.execute("SELECT create_topic('test_cascade_topic')")
    cur.execute("SELECT create_queue('test_cascade_queue', 'test_cascade_topic', 3, false)")
    
    # Publish 3 test messages
    for i in range(1, 4):
        query = f"SELECT publish_message('test_cascade_topic', '{{\"test\": \"cascade{i}\"}}'::jsonb)"
        cur.execute(query)
    
    # Verify initial state - should be 3 messages in the queue
    cur.execute("SELECT COUNT(*) FROM list_messages('test_cascade_queue')")
    assert cur.fetchone()[0] == 3, "Should have 3 messages initially"
    
    # Get one message ID to delete directly
    cur.execute("SELECT message_id FROM queue_messages WHERE queue_name = 'test_cascade_queue' LIMIT 1")
    message_id = cur.fetchone()[0]
    
    # Test 1: Delete a message directly - should cascade to queue_messages
    cur.execute("DELETE FROM messages WHERE id = %s", (message_id,))
    
    # Verify message is gone from queue_messages too
    cur.execute("SELECT COUNT(*) FROM queue_messages WHERE message_id = %s", (message_id,))
    assert cur.fetchone()[0] == 0, "Message reference should be deleted from queue_messages"
    
    # Verify we now have 2 messages in the queue
    cur.execute("SELECT COUNT(*) FROM list_messages('test_cascade_queue')")
    assert cur.fetchone()[0] == 2, "Should have 2 messages after direct deletion"
    
    # Test 2: Clean up topic - should cascade to queue_messages
    cur.execute("SELECT clean_up_topic('test_cascade_topic')")
    
    # Verify all messages are gone
    cur.execute("SELECT COUNT(*) FROM list_messages('test_cascade_queue')")
    assert cur.fetchone()[0] == 0, "No messages should remain after topic cleanup"
    
    # Create new messages for test 3
    for i in range(1, 4):
        query = f"SELECT publish_message('test_cascade_topic', '{{\"test\": \"cascade_test3_{i}\"}}'::jsonb)"
        cur.execute(query)
    
    # Test 3: Delete topic - should cascade to queues, messages, and queue_messages
    cur.execute("DELETE FROM topics WHERE name = 'test_cascade_topic'")
    
    # Verify queue is gone
    cur.execute("SELECT COUNT(*) FROM queues WHERE name = 'test_cascade_queue'")
    assert cur.fetchone()[0] == 0, "Queue should be deleted when topic is deleted"
    
    # Verify no messages remain for this topic
    cur.execute("SELECT COUNT(*) FROM messages WHERE topic_name = 'test_cascade_topic'")
    assert cur.fetchone()[0] == 0, "Messages should be deleted when topic is deleted"
    
    # Verify no queue_messages entries remain
    cur.execute("SELECT COUNT(*) FROM queue_messages WHERE queue_name = 'test_cascade_queue'")
    assert cur.fetchone()[0] == 0, "Queue message entries should be deleted when topic is deleted"

def test_delete_inactive_queues_edge_cases(cur: psycopg2.extensions.cursor) -> None:
    """Test edge cases for delete_inactive_queues function."""
    # Test 1: Empty database
    cur.execute("SELECT delete_inactive_queues()")
    # Should complete without error
    
    # Test 2: Queue with NULL keep_alive_until
    cur.execute("SELECT create_topic('null_ka_topic')")
    cur.execute("SELECT create_queue('null_ka_queue', 'null_ka_topic', 3, true)")
    
    # Manually set keep_alive_until to NULL
    cur.execute("UPDATE queues SET keep_alive_until = NULL WHERE name = 'null_ka_queue'")
    
    # Verify it's NULL
    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'null_ka_queue'")
    assert cur.fetchone()[0] is None
    
    # Delete inactive queues
    cur.execute("SELECT delete_inactive_queues()")
    
    # Verify the queue was deleted (should be, as it's exclusive with NULL keep_alive_until)
    cur.execute("SELECT COUNT(*) FROM queues WHERE name = 'null_ka_queue'")
    assert cur.fetchone()[0] == 0
    
    # Clean up
    cur.execute("DELETE FROM topics WHERE name = 'null_ka_topic'")

def test_get_next_visible_time_multiple_queues(cur: psycopg2.extensions.cursor) -> None:
    """Test get_next_visible_time with multiple queues."""
    # Create separate topics for each queue to avoid cross-routing of messages
    cur.execute("SELECT create_topic('MultiQTopic1')")
    cur.execute("SELECT create_topic('MultiQTopic2')")
    cur.execute("SELECT create_queue('Queue1', 'MultiQTopic1', 2, false)")
    cur.execute("SELECT create_queue('Queue2', 'MultiQTopic2', 2, false)")
    
    # Add a message to Queue1 with a 5-second delay
    delay1 = datetime.now(pytz.UTC) + timedelta(seconds=5)
    cur.execute("""
        SELECT publish_message('MultiQTopic1', '{"test":"queue1"}'::jsonb, %s)
    """, (delay1,))
    
    # Add a message to Queue2 with a 2-second delay
    delay2 = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT publish_message('MultiQTopic2', '{"test":"queue2"}'::jsonb, %s)
    """, (delay2,))
    
    # Check both queues
    cur.execute("SELECT get_next_visible_time('Queue1')")
    next_time1 = cur.fetchone()[0]
    
    cur.execute("SELECT get_next_visible_time('Queue2')")
    next_time2 = cur.fetchone()[0]
    
    # Check that next_time2 is earlier than next_time1
    # We don't compare to the exact delays to avoid test flakiness
    assert next_time2 < next_time1, "Queue2 should have an earlier next visible time than Queue1"
    
    # Clean up
    cur.execute("DELETE FROM queues WHERE name IN ('Queue1', 'Queue2')")
    cur.execute("DELETE FROM topics WHERE name IN ('MultiQTopic1', 'MultiQTopic2')")

def test_cleanup_functions_edge_cases(cur: psycopg2.extensions.cursor) -> None:
    """Test edge cases for cleanup functions."""
    # Setup
    cur.execute("SELECT create_topic('CleanupTopic')")
    cur.execute("SELECT create_queue('CleanupQueue', 'CleanupTopic', 3, false)")
    
    # Test 1: Clean up empty topic/queue
    cur.execute("SELECT clean_up_queue('CleanupQueue')")
    cur.execute("SELECT clean_up_topic('CleanupTopic')")
    
    # Test 2: Clean up non-existent queue/topic - should execute without errors
    cur.execute("SELECT clean_up_queue('NonExistentQueue')")
    cur.execute("SELECT clean_up_topic('NonExistentTopic')")
    
    # Test 3: Publish messages, then clean up
    for i in range(3):
        cur.execute(
            "SELECT publish_message('CleanupTopic', %s::jsonb)",
            (json.dumps({"test": f"cleanup-{i}"}),)
        )
    
    # Consume one message to have mixed statuses
    cur.execute("SELECT * FROM consume_message('CleanupQueue', 30, 1)")
    
    # Clean up the queue
    cur.execute("SELECT clean_up_queue('CleanupQueue')")
    
    # Verify all messages are gone from queue
    cur.execute("SELECT COUNT(*) FROM queue_messages WHERE queue_name = 'CleanupQueue'")
    assert cur.fetchone()[0] == 0
    
    # Clean up the topic
    cur.execute("SELECT clean_up_topic('CleanupTopic')")
    
    # Verify all messages are gone from the topic
    cur.execute("SELECT COUNT(*) FROM messages WHERE topic_name = 'CleanupTopic'")
    assert cur.fetchone()[0] == 0
    
    # Clean up
    cur.execute("DELETE FROM queues WHERE name = 'CleanupQueue'")
    cur.execute("DELETE FROM topics WHERE name = 'CleanupTopic'")

def test_cleanup_completed_messages(cur: psycopg2.extensions.cursor) -> None:
    """Test cleanup_completed_messages removes only stale completed entries."""
    cur.execute("SELECT create_topic('CleanupRetentionTopic')")
    cur.execute("SELECT create_queue('CleanupRetentionQueue', 'CleanupRetentionTopic', 3, false)")
    
    for i in range(2):
        cur.execute(
            "SELECT publish_message('CleanupRetentionTopic', %s::jsonb)",
            (json.dumps({"test": f"cleanup-retention-{i}"}),)
        )
    
    cur.execute("""
        SELECT message_id, consumer_token
        FROM consume_message('CleanupRetentionQueue', 30, 2)
    """)
    messages = cur.fetchall()
    assert len(messages) == 2, "Should consume both messages"
    
    for msg in messages:
        cur.execute(
            "SELECT ack_message('CleanupRetentionQueue', %s, %s)",
            (msg['message_id'], msg['consumer_token'])
        )
    
    older_id = messages[0]['message_id']
    newer_id = messages[1]['message_id']
    
    cur.execute("""
        UPDATE queue_messages
        SET processed_at = NOW() - interval '2 hours'
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (older_id,))
    cur.execute("""
        UPDATE queue_messages
        SET processed_at = NOW() - interval '30 minutes'
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (newer_id,))
    
    cur.execute("SELECT cleanup_completed_messages(1)")
    deleted = cur.fetchone()[0]
    assert deleted == 1, "Exactly one completed message should be removed"
    
    cur.execute("""
        SELECT COUNT(*)
        FROM queue_messages
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (older_id,))
    assert cur.fetchone()[0] == 0, "Old completed message should be deleted"
    
    cur.execute("""
        SELECT status
        FROM queue_messages
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (newer_id,))
    assert cur.fetchone()[0] == 'completed', "Recent completed message should remain"
    
    cur.execute("""
        UPDATE queue_messages
        SET processed_at = NOW() - interval '2 days'
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (newer_id,))
    
    cur.execute("SELECT cleanup_completed_messages()")
    deleted_second = cur.fetchone()[0]
    assert deleted_second == 1, "Default retention should remove stale completed entries"
    
    cur.execute("""
        SELECT COUNT(*)
        FROM queue_messages
        WHERE queue_name = 'CleanupRetentionQueue'
    """)
    assert cur.fetchone()[0] == 0, "No queue entries should remain after cleanup"
    
    cur.execute("SELECT clean_up_topic('CleanupRetentionTopic')")
    cur.execute("SELECT delete_queue('CleanupRetentionQueue')")
    cur.execute("SELECT delete_topic('CleanupRetentionTopic')")

def test_purge_all_messages_states(cur: psycopg2.extensions.cursor) -> None:
    """Test purge_all_messages with messages in different states."""
    # Setup multiple topics and queues
    cur.execute("SELECT create_topic('PurgeTopic1')")
    cur.execute("SELECT create_topic('PurgeTopic2')")
    cur.execute("SELECT create_queue('PurgeQueue1', 'PurgeTopic1', 3, false)")
    cur.execute("SELECT create_queue('PurgeQueue2', 'PurgeTopic2', 3, false)")
    
    # Publish messages to both topics
    for i in range(3):
        cur.execute(
            "SELECT publish_message('PurgeTopic1', %s::jsonb)",
            (json.dumps({"test": f"purge1-{i}"}),)
        )
        cur.execute(
            "SELECT publish_message('PurgeTopic2', %s::jsonb)",
            (json.dumps({"test": f"purge2-{i}"}),)
        )
    
    # Consume some messages to get them in different states
    cur.execute("SELECT * FROM consume_message('PurgeQueue1', 30, 2)")
    msg = cur.fetchone()
    if msg:
        # Complete one message
        cur.execute("""
            SELECT ack_message('PurgeQueue1', %s, %s)
        """, (msg['message_id'], msg['consumer_token']))
    
    # Nack one message from queue2 with delay
    cur.execute("SELECT * FROM consume_message('PurgeQueue2', 30, 1)")
    msg = cur.fetchone()
    if msg:
        delay_time = datetime.now(pytz.UTC) + timedelta(seconds=10)
        cur.execute("""
            SELECT nack_message('PurgeQueue2', %s, %s, %s)
        """, (msg['message_id'], msg['consumer_token'], delay_time))
    
    # Verify initial message counts
    cur.execute("SELECT COUNT(*) FROM messages")
    initial_msg_count = cur.fetchone()[0]
    assert initial_msg_count > 0
    
    # Purge all messages
    cur.execute("SELECT purge_all_messages()")
    
    # Verify all messages are gone
    cur.execute("SELECT COUNT(*) FROM messages")
    assert cur.fetchone()[0] == 0
    
    cur.execute("SELECT COUNT(*) FROM queue_messages")
    assert cur.fetchone()[0] == 0
    
    # Clean up
    cur.execute("DELETE FROM queues WHERE name IN ('PurgeQueue1', 'PurgeQueue2')")
    cur.execute("DELETE FROM topics WHERE name IN ('PurgeTopic1', 'PurgeTopic2')")

def test_complex_cascade_behavior(cur: psycopg2.extensions.cursor) -> None:
    """Test cascade behavior with complex relationships between topics, queues, and messages."""
    # Setup - create multiple topics and queues
    topics = ['CascadeTopic1', 'CascadeTopic2']
    for topic in topics:
        cur.execute(f"SELECT create_topic('{topic}')")
    
    # Create 2 queues per topic (4 total)
    queues = []
    for topic in topics:
        for i in range(1, 3):
            queue_name = f"{topic}_Queue{i}"
            cur.execute(f"SELECT create_queue('{queue_name}', '{topic}', 3, false)")
            queues.append(queue_name)
    
    # Publish multiple messages to each topic
    msg_ids = []
    for topic in topics:
        for i in range(3):
            cur.execute(
                "SELECT publish_message(%s, %s::jsonb)",
                (topic, json.dumps({"test": f"{topic}-msg{i}"})))
            cur.execute("SELECT lastval()")
            msg_ids.append(cur.fetchone()[0])
    
    # Consume some messages from each queue
    for queue in queues:
        cur.execute(f"SELECT * FROM consume_message('{queue}', 30, 1)")
        msg = cur.fetchone()
        if msg:
            # Complete some, leave others in processing state
            if queue.endswith("Queue1"):
                cur.execute(
                    "SELECT ack_message(%s, %s, %s)",
                    (queue, msg['message_id'], msg['consumer_token'])
                )
    
    # Test 1: Delete one queue and verify its queue_messages are gone
    test_queue = queues[0]
    cur.execute(f"SELECT delete_queue('{test_queue}')")
    
    cur.execute(f"SELECT COUNT(*) FROM queue_messages WHERE queue_name = '{test_queue}'")
    assert cur.fetchone()[0] == 0
    
    # Test 2: Delete one topic and verify cascade effects
    test_topic = topics[0]
    
    # Get counts before
    cur.execute(f"SELECT COUNT(*) FROM messages WHERE topic_name = '{test_topic}'")
    topic_msg_count = cur.fetchone()[0]
    assert topic_msg_count > 0
    
    topic_queues = [q for q in queues if q.startswith(test_topic)]
    queue_msg_count = 0
    for queue in topic_queues:
        cur.execute(f"SELECT COUNT(*) FROM queue_messages WHERE queue_name = '{queue}'")
        queue_msg_count += cur.fetchone()[0]
    
    # Delete the topic
    # First, clean up all messages for this topic 
    cur.execute(f"DELETE FROM messages WHERE topic_name = '{test_topic}'")
    
    # Now we can delete the topic
    cur.execute(f"SELECT delete_topic('{test_topic}')")
    
    # Verify topic is gone
    cur.execute(f"SELECT COUNT(*) FROM messages WHERE topic_name = '{test_topic}'")
    assert cur.fetchone()[0] == 0
    
    for queue in topic_queues:
        cur.execute(f"SELECT COUNT(*) FROM queues WHERE name = '{queue}'")
        assert cur.fetchone()[0] == 0
        
        cur.execute(f"SELECT COUNT(*) FROM queue_messages WHERE queue_name = '{queue}'")
        assert cur.fetchone()[0] == 0
    
    # Clean up
    # Delete messages for remaining topics
    for topic in topics:
        if topic != test_topic:  # We already deleted messages for test_topic
            cur.execute(f"DELETE FROM messages WHERE topic_name = '{topic}'")
            cur.execute(f"SELECT delete_topic('{topic}')")

def test_nack_message_delays(cur: psycopg2.extensions.cursor) -> None:
    """Test nack_message with different delay strategies."""
    # Setup
    cur.execute("SELECT create_topic('NackTopic')")
    cur.execute("SELECT create_queue('NackQueue', 'NackTopic', 3, false)")
    
    # Publish a series of messages
    for i in range(4):
        cur.execute(
            "SELECT publish_message('NackTopic', %s::jsonb)",
            (json.dumps({"test": f"nack-{i}"}),)
        )
    
    # Consume all messages
    cur.execute("SELECT * FROM consume_message('NackQueue', 30, 4)")
    messages = cur.fetchall()
    
    # Test different nack delay strategies
    # 1. Immediate nack
    msg1 = messages[0]
    cur.execute("""
        SELECT nack_message('NackQueue', %s, %s)
    """, (msg1['message_id'], msg1['consumer_token']))
    
    # 2. Short delay (1 second)
    msg2 = messages[1]
    short_delay = datetime.now(pytz.UTC) + timedelta(seconds=1)
    cur.execute("""
        SELECT nack_message('NackQueue', %s, %s, %s)
    """, (msg2['message_id'], msg2['consumer_token'], short_delay))
    
    # 3. Medium delay (3 seconds)
    msg3 = messages[2]
    medium_delay = datetime.now(pytz.UTC) + timedelta(seconds=3)
    cur.execute("""
        SELECT nack_message('NackQueue', %s, %s, %s)
    """, (msg3['message_id'], msg3['consumer_token'], medium_delay))
    
    # 4. Long delay (5 seconds)
    msg4 = messages[3]
    long_delay = datetime.now(pytz.UTC) + timedelta(seconds=5)
    cur.execute("""
        SELECT nack_message('NackQueue', %s, %s, %s)
    """, (msg4['message_id'], msg4['consumer_token'], long_delay))
    
    # Test immediate message availability
    cur.execute("SELECT * FROM consume_message('NackQueue', 30, 1)")
    immediately_available = cur.fetchone()
    assert immediately_available is not None
    assert immediately_available['message_id'] == msg1['message_id']
    
    # Wait for 1.1 seconds and check for next message
    time.sleep(1.1)
    cur.execute("SELECT * FROM consume_message('NackQueue', 30, 1)")
    short_delay_msg = cur.fetchone()
    assert short_delay_msg is not None
    assert short_delay_msg['message_id'] == msg2['message_id']
    
    # Wait for 2 more seconds and check
    time.sleep(2)
    cur.execute("SELECT * FROM consume_message('NackQueue', 30, 1)")
    medium_delay_msg = cur.fetchone()
    assert medium_delay_msg is not None
    assert medium_delay_msg['message_id'] == msg3['message_id']
    
    # Wait final 2 seconds
    time.sleep(2)
    cur.execute("SELECT * FROM consume_message('NackQueue', 30, 1)")
    long_delay_msg = cur.fetchone()
    assert long_delay_msg is not None
    assert long_delay_msg['message_id'] == msg4['message_id']
    
    # Clean up
    cur.execute("DELETE FROM queues WHERE name = 'NackQueue'")
    cur.execute("DELETE FROM topics WHERE name = 'NackTopic'")

def test_release_message(cur: psycopg2.extensions.cursor) -> None:
    """Test release_message function for explicitly releasing a message without redelivery."""
    # Setup
    cur.execute("SELECT create_topic('ReleaseTopic')")
    cur.execute("SELECT create_queue('ReleaseQueue', 'ReleaseTopic', 3, false)")
    
    # Publish a message
    cur.execute("SELECT publish_message('ReleaseTopic', '{\"test\": \"release\"}'::jsonb)")
    
    # Consume the message
    cur.execute("""
        SELECT message_id, consumer_token
        FROM consume_message('ReleaseQueue', 30)
    """)
    result = cur.fetchone()
    assert result is not None
    msg_id, token = result['message_id'], result['consumer_token']
    
    # Verify message is in processing state
    cur.execute("""
        SELECT status
        FROM queue_messages
        WHERE queue_name = 'ReleaseQueue' AND message_id = %s
    """, (msg_id,))
    assert cur.fetchone()[0] == 'processing'
    
    # Release the message - returns void, no result to assert
    cur.execute("""
        SELECT release_message('ReleaseQueue', %s, %s)
    """, (msg_id, token))
    
    # Verify message is back to pending state
    cur.execute("""
        SELECT status, delivery_attempts
        FROM queue_messages
        WHERE queue_name = 'ReleaseQueue' AND message_id = %s
    """, (msg_id,))
    row = cur.fetchone()
    assert row[0] == 'pending', "Message should be back in pending state"
    # Should be decremented by 1, so it's back to 0
    assert row[1] == 0, "Delivery attempts should be reset to 0"
    
    # Message should be immediately available for consumption again
    cur.execute("""
        SELECT message_id
        FROM consume_message('ReleaseQueue', 30)
    """)
    assert cur.fetchone()[0] == msg_id, "Message should be available for consumption again"
    
    # Test with invalid consumer token
    with pytest.raises(Exception):
        cur.execute("""
            SELECT release_message('ReleaseQueue', %s, 'invalid-token')
        """, (msg_id,))

def test_list_topics(cur: psycopg2.extensions.cursor) -> None:
    """Test list_topics function returns all created topics."""
    # Clean up any existing topics first
    cur.execute("SELECT name FROM topics")
    existing_topics = [row[0] for row in cur.fetchall()]
    for topic in existing_topics:
        cur.execute("SELECT clean_up_topic(%s)", (topic,))
        cur.execute("SELECT delete_topic(%s)", (topic,))
    
    # Verify no topics exist
    cur.execute("SELECT COUNT(*) FROM list_topics()")
    assert cur.fetchone()[0] == 0, "Should start with no topics"
    
    # Create test topics
    test_topics = ['ListTopicA', 'ListTopicB', 'ListTopicC']
    for topic in test_topics:
        cur.execute(f"SELECT create_topic('{topic}')")
    
    # Test list_topics returns all topics - column is named "topic" not "name"
    cur.execute("SELECT topic FROM list_topics() ORDER BY topic")
    topics = [row[0] for row in cur.fetchall()]
    assert topics == sorted(test_topics), "list_topics should return all created topics"
    
    # Clean up
    for topic in test_topics:
        cur.execute(f"SELECT delete_topic('{topic}')")

def test_list_queues(cur: psycopg2.extensions.cursor) -> None:
    """Test list_queues function returns all created queues."""
    # Setup - create topics and queues
    cur.execute("SELECT create_topic('ListQueueTopic1')")
    cur.execute("SELECT create_topic('ListQueueTopic2')")
    
    # Create multiple queues for each topic
    queues = [
        ('Queue1A', 'ListQueueTopic1', 2, False),
        ('Queue1B', 'ListQueueTopic1', 3, True),
        ('Queue2A', 'ListQueueTopic2', 0, False),
        ('Queue2B', 'ListQueueTopic2', 1, True)
    ]
    
    for name, topic, attempts, exclusive in queues:
        cur.execute(f"""
            SELECT create_queue('{name}', '{topic}', {attempts}, {exclusive})
        """)
    
    # Test list_queues returns all queues - correct column names from function definition
    cur.execute("SELECT queue_name, topic_name, max_delivery_attempts, exclusive FROM list_queues() ORDER BY queue_name")
    result = cur.fetchall()
    assert len(result) >= len(queues), "Should return at least our test queues"
    
    # Verify our specific queues are in the results
    for q in queues:
        found = False
        for row in result:
            if row[0] == q[0]:  # Match by queue name
                found = True
                assert row[1] == q[1], f"Topic mismatch for queue {q[0]}"
                assert row[2] == q[2], f"Max delivery attempts mismatch for queue {q[0]}"
                assert row[3] == q[3], f"Exclusive flag mismatch for queue {q[0]}"
        
        assert found, f"Queue {q[0]} not found in results"
    
    # Clean up
    for name, topic, _, _ in queues:
        cur.execute(f"SELECT delete_queue('{name}')")
    
    cur.execute("SELECT delete_topic('ListQueueTopic1')")
    cur.execute("SELECT delete_topic('ListQueueTopic2')")

def test_get_queue_statistics(cur: psycopg2.extensions.cursor) -> None:
    """Test get_queue_statistics function returns correct statistics for queues."""
    # Setup - create topic and queue
    cur.execute("SELECT create_topic('StatsTopic')")
    cur.execute("SELECT create_queue('StatsQueue', 'StatsTopic', 3, false)")
    
    # Publish messages
    for i in range(5):
        cur.execute(
            "SELECT publish_message('StatsTopic', %s::jsonb)",
            (json.dumps({"test": f"stats-{i}"}),)
        )
    
    # Consume some messages to get different states
    cur.execute("SELECT * FROM consume_message('StatsQueue', 30, 2)")
    messages = cur.fetchall()
    
    # Complete one message
    if len(messages) > 0:
        cur.execute("""
            SELECT ack_message('StatsQueue', %s, %s)
        """, (messages[0]['message_id'], messages[0]['consumer_token']))
    
    # Get statistics for this queue - uses correct column names from SQL function
    cur.execute("SELECT pending_count, processing_count, completed_count, total_count FROM get_queue_statistics('StatsQueue')")
    stats = cur.fetchone()
    assert stats is not None
    
    # Verify the statistics - column names match the SQL function definition
    assert stats[0] == 3, "Should have 3 pending messages"
    assert stats[1] == 1, "Should have 1 processing message"
    assert stats[2] == 1, "Should have 1 completed message"
    assert stats[3] == 5, "Should have 5 total messages"
    
    # Test getting statistics for all queues
    cur.execute("SELECT pending_count, processing_count, completed_count, total_count FROM get_queue_statistics()")
    all_stats = cur.fetchone()
    assert all_stats is not None, "Should return statistics for all queues"
    assert all_stats[3] >= 5, "Should include at least our test messages in total"
    
    # Clean up - make sure to clean up the topic before deleting it
    cur.execute("SELECT clean_up_queue('StatsQueue')")
    cur.execute("SELECT delete_queue('StatsQueue')")
    cur.execute("SELECT clean_up_topic('StatsTopic')")
    cur.execute("SELECT delete_topic('StatsTopic')")

def test_list_dlq_messages(cur: psycopg2.extensions.cursor) -> None:
    """Test list_dlq_messages function returns messages in the dead letter queue."""
    # Setup - create topic and queue
    cur.execute("SELECT create_topic('DLQListTopic')")
    cur.execute("SELECT create_queue('DLQListQueue', 'DLQListTopic', 1, false)")  # Only 1 retry
    
    # Publish messages
    for i in range(3):
        cur.execute(
            "SELECT publish_message('DLQListTopic', %s::jsonb)",
            (json.dumps({"test": f"dlq-{i}"}),)
        )
    
    # Consume and nack all messages to exceed max delivery attempts
    for _ in range(2):  # Need to consume and nack twice to exceed max_delivery_attempts
        cur.execute("SELECT * FROM consume_message('DLQListQueue', 30, 3)")
        messages = cur.fetchall()
        
        for msg in messages:
            cur.execute("""
                SELECT nack_message('DLQListQueue', %s, %s)
            """, (msg['message_id'], msg['consumer_token']))
    
    # Move to DLQ
    cur.execute("SELECT move_messages_to_dlq()")
    
    # Test list_dlq_messages with column names from SQL function
    cur.execute("SELECT queue_name, message_id, retry_count, published_at FROM list_dlq_messages()")
    dlq_messages = cur.fetchall()
    
    assert len(dlq_messages) == 3, "All 3 messages should be in DLQ"
    
    # Verify message properties
    for msg in dlq_messages:
        assert msg[0] == 'DLQListQueue'  # queue_name
        assert msg[1] is not None  # message_id
        assert msg[2] == 1, "retry_count should be the max attempts (1)"
        assert msg[3] is not None  # published_at
    
    # Clean up - purge DLQ, clean up queue and topic before deletion
    cur.execute("SELECT purge_dlq()")
    cur.execute("SELECT clean_up_queue('DLQListQueue')")
    cur.execute("SELECT delete_queue('DLQListQueue')")
    cur.execute("SELECT clean_up_topic('DLQListTopic')")
    cur.execute("SELECT delete_topic('DLQListTopic')")

def test_purge_dlq(cur: psycopg2.extensions.cursor) -> None:
    """Test purge_dlq function removes all messages from the dead letter queue."""
    # Setup - create topic and queues
    cur.execute("SELECT create_topic('PurgeDLQTopic')")
    cur.execute("SELECT create_queue('PurgeDLQQueue1', 'PurgeDLQTopic', 1, false)")
    
    # Publish messages to one queue
    for i in range(2):
        cur.execute(
            "SELECT publish_message('PurgeDLQTopic', %s::jsonb)",
            (json.dumps({"test": f"purge-q1-{i}"}),)
        )
    
    # Consume and nack messages to exceed max delivery attempts
    for _ in range(2):  # Need to consume and nack twice to exceed max_delivery_attempts
        cur.execute("SELECT * FROM consume_message('PurgeDLQQueue1', 30, 2)")
        messages = cur.fetchall()
        
        for msg in messages:
            cur.execute("""
                SELECT nack_message('PurgeDLQQueue1', %s, %s)
            """, (msg['message_id'], msg['consumer_token']))
    
    # Move to DLQ
    cur.execute("SELECT move_messages_to_dlq()")
    
    # Verify messages are in DLQ
    cur.execute("SELECT COUNT(*) FROM list_dlq_messages()")
    assert cur.fetchone()[0] == 2, "Should have 2 messages in DLQ"
    
    # Test purge_dlq
    cur.execute("SELECT purge_dlq()")
    
    # Verify DLQ is empty
    cur.execute("SELECT COUNT(*) FROM list_dlq_messages()")
    assert cur.fetchone()[0] == 0, "DLQ should be empty after purge"
    
    # Clean up - make sure to clean up topic before deletion
    cur.execute("SELECT clean_up_queue('PurgeDLQQueue1')")
    cur.execute("SELECT delete_queue('PurgeDLQQueue1')")
    cur.execute("SELECT clean_up_topic('PurgeDLQTopic')")
    cur.execute("SELECT delete_topic('PurgeDLQTopic')")

def test_delete_queue_message(cur: psycopg2.extensions.cursor) -> None:
    """Test deleting a specific message from a queue."""
    # Create test topic and queue
    topic = "DeleteMsgTopic"
    queue = "DeleteMsgQueue"
    cur.execute("SELECT create_topic(%s)", (topic,))
    cur.execute("SELECT create_queue(%s, %s)", (queue, topic))
    
    # Publish messages
    message_ids = []
    for i in range(3):
        cur.execute(
            "SELECT publish_message(%s, %s::jsonb)",
            (topic, json.dumps({"test": f"delete-test-{i}"}))
        )
        cur.execute("SELECT lastval()")
        message_ids.append(cur.fetchone()[0])
    
    # Delete the second message
    target_msg_id = message_ids[1]
    cur.execute("SELECT delete_queue_message(%s, %s)", (queue, target_msg_id))
    
    # Verify message is deleted
    cur.execute("SELECT COUNT(*) FROM queue_messages WHERE queue_name = %s AND message_id = %s", (queue, target_msg_id))
    assert cur.fetchone()[0] == 0, f"Message {target_msg_id} should be deleted"
    
    # Count remaining messages
    cur.execute("SELECT COUNT(*) FROM queue_messages WHERE queue_name = %s", (queue,))
    assert cur.fetchone()[0] == 2, "Should have 2 messages remaining"
    
    # Clean up
    cur.execute("SELECT clean_up_queue(%s)", (queue,))
    cur.execute("SELECT delete_queue(%s)", (queue,))
    cur.execute("SELECT clean_up_topic(%s)", (topic,))
    cur.execute("SELECT delete_topic(%s)", (topic,))

def test_get_message(cur: psycopg2.extensions.cursor) -> None:
    """Test retrieving a specific message by ID."""
    # Create test topic and queue
    topic = "GetMsgTopic"
    queue = "GetMsgQueue"
    cur.execute("SELECT create_topic(%s)", (topic,))
    cur.execute("SELECT create_queue(%s, %s)", (queue, topic))
    
    # Publish a message
    test_payload = {"test": "get-message-test", "value": 42}
    cur.execute(
        "SELECT publish_message(%s, %s::jsonb)",
        (topic, json.dumps(test_payload))
    )
    cur.execute("SELECT lastval()")
    msg_id = cur.fetchone()[0]
    
    # Get the message by ID
    cur.execute("SELECT message_id, topic_name, payload, published_at FROM get_message(%s)", (msg_id,))
    msg = cur.fetchone()
    
    # Verify message properties
    assert msg is not None, "Message should be retrieved"
    assert msg[0] == msg_id, "Message ID should match"
    assert msg[1] == topic, "Topic should match"
    assert msg[2] == test_payload, "Payload should match"
    assert msg[3] is not None, "Published timestamp should exist"
    
    # Clean up
    cur.execute("SELECT clean_up_queue(%s)", (queue,))
    cur.execute("SELECT delete_queue(%s)", (queue,))
    cur.execute("SELECT clean_up_topic(%s)", (topic,))
    cur.execute("SELECT delete_topic(%s)", (topic,))
