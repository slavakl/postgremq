import os
from pathlib import Path
import pytest
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from typing import Generator, Any
import uuid
import pytz

# Get the path to latest.sql relative to this test file
SQL_FILE = Path(__file__).parent.parent / 'sql' / 'latest.sql'

# Configuration (could be moved to a config file)
DB_CONFIG = {
    'dbname': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD', 'postgres'),
    'host': os.getenv('PGHOST', 'localhost'),
    'port': os.getenv('PGPORT', '5432'),
}

TEST_DB_PREFIX = 'mq_test_'

@pytest.fixture(scope="session")
def test_db_name() -> str:
    """Generate a unique test database name."""
    return f"{TEST_DB_PREFIX}{uuid.uuid4().hex[:8]}"

@pytest.fixture(scope="session")
def admin_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    """Create a connection with admin privileges for database creation/deletion."""
    conn = psycopg2.connect(**DB_CONFIG)
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
def conn(test_db: str) -> Generator[psycopg2.extensions.connection, None, None]:
    """Create a connection to the test database for each test."""
    config = DB_CONFIG.copy()
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
        SELECT create_queue('TestQueue', 'TestTopic', 3, true);
        SELECT create_queue('TestQueue_ND', 'TestTopic', 2, false, interval '5 minutes');
    """)
    
    # Verify queues were created
    cur.execute("SELECT count(*) FROM queues WHERE topic_name = 'TestTopic'")
    assert cur.fetchone()[0] == 2

    # Verify keep-alive for non-durable queue
    cur.execute("""
        SELECT keep_alive_until 
        FROM queues 
        WHERE name = 'TestQueue_ND'
    """)
    keep_alive = cur.fetchone()[0]
    now = datetime.now(pytz.UTC)
    assert keep_alive > now + timedelta(minutes=4)
    assert keep_alive < now + timedelta(minutes=6)

def test_queue_keep_alive_extension(cur: psycopg2.extensions.cursor) -> None:
    """Test queue keep-alive extension functionality."""
    # Setup
    cur.execute("""
        SELECT create_topic('TestTopic');
        SELECT create_queue('TestQueue_ND', 'TestTopic', 2, false, interval '5 minutes');
        SELECT create_queue('TestQueue_D', 'TestTopic', 2, true);
    """)

    # Get initial keep-alive and extend it
    cur.execute("""
        SELECT keep_alive_until FROM queues WHERE name = 'TestQueue_ND';
        SELECT extend_queue_keep_alive('TestQueue_ND', interval '15 minutes');
    """)
    assert cur.fetchone()[0] is True

    # Verify keep-alive was extended
    cur.execute("""
        SELECT keep_alive_until 
        FROM queues 
        WHERE name = 'TestQueue_ND'
    """)
    new_keep_alive = cur.fetchone()[0]
    now = datetime.now(pytz.UTC)
    assert new_keep_alive > now + timedelta(minutes=14)
    assert new_keep_alive < now + timedelta(minutes=16)

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
        FROM consume_message('TestQueue', interval '5 minutes', 1) cm
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
        FROM consume_message('RetryQueue', interval '1 minute', 1) cm;
    """)
    msg_id, token = cur.fetchone()
    cur.execute("SELECT nack_message('RetryQueue', %s, %s)", (msg_id, token))

    # Second delivery attempt
    cur.execute("""
        SELECT message_id, consumer_token 
        FROM consume_message('RetryQueue', interval '1 minute', 1) cm;
    """)
    msg_id, token = cur.fetchone()
    cur.execute("SELECT nack_message('RetryQueue', %s, %s)", (msg_id, token))

    # Verify no more attempts available
    cur.execute("""
        SELECT count(*) 
        FROM consume_message('RetryQueue', interval '1 minute', 1) cm;
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

def test_unlimited_delivery_attempts(cur: psycopg2.extensions.cursor) -> None:
    """Test queue with unlimited delivery attempts (max_delivery_attempts = 0)."""
    # Setup
    cur.execute("""
        SELECT create_topic('UnlimitedTopic');
        SELECT create_queue('UnlimitedQueue', 'UnlimitedTopic', 0, true);
        SELECT publish_message('UnlimitedTopic', '{"test": "unlimited"}'::jsonb);
    """)

    # Try multiple delivery attempts
    for i in range(5):
        cur.execute("""
            SELECT message_id, consumer_token, delivery_attempts
            FROM consume_message('UnlimitedQueue', interval '1 minute', 1);
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
        FROM consume_message('ConcurrentQueue', interval '1 minute', 1) cm
    """)
    assert cur.fetchone() is not None

    # Second consumer should get nothing
    cur.execute("""
        SELECT count(*) 
        FROM consume_message('ConcurrentQueue', interval '1 minute', 1) cm
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