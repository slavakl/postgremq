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
        SELECT create_queue('TestQueue_Ex', 'TestTopic', 2, true, interval '300 seconds');
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
        SELECT create_queue('TestQueue_Ex', 'TestTopic', 2, true, interval '300 seconds');
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

def test_create_queue_idempotent_and_strict(cur: psycopg2.extensions.cursor) -> None:
    """create_queue follows RabbitMQ-style 'match-or-error' semantics:
    re-creating with identical parameters is a no-op success; re-creating
    with any parameter different raises PMQ03 so accidental config drift
    surfaces loudly."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_topic('OtherTopic')")

    # 1. Identical exclusive params → both calls succeed (no error).
    cur.execute("SELECT create_queue('ExQueue', 'TestTopic', 2, true, interval '300 seconds')")
    cur.execute("SELECT create_queue('ExQueue', 'TestTopic', 2, true, interval '300 seconds')")

    # 2. Identical non-exclusive params → idempotent.
    cur.execute("SELECT create_queue('NxQueue', 'TestTopic', 2, false)")
    cur.execute("SELECT create_queue('NxQueue', 'TestTopic', 2, false)")

    # 3. Param mismatches each raise PMQ03 with the existing values surfaced.
    mismatches = [
        # Different topic
        "SELECT create_queue('ExQueue', 'OtherTopic', 2, true, interval '300 seconds')",
        # Different max_delivery_attempts
        "SELECT create_queue('ExQueue', 'TestTopic', 5, true, interval '300 seconds')",
        # Different exclusive flag
        "SELECT create_queue('ExQueue', 'TestTopic', 2, false, interval '300 seconds')",
        # Different keep_alive_interval
        "SELECT create_queue('ExQueue', 'TestTopic', 2, true, interval '60 seconds')",
    ]
    for stmt in mismatches:
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute(stmt)
        assert exc_info.value.pgcode == 'PMQ03', \
            f"expected PMQ03 for {stmt!r}, got {exc_info.value.pgcode}: {exc_info.value}"
        assert "already exists with different parameters" in str(exc_info.value)
        cur.connection.rollback()

def test_create_queue_idempotent_refreshes_exclusive_keep_alive(cur: psycopg2.extensions.cursor) -> None:
    """Re-creating an exclusive queue with matching params refreshes
    keep_alive_until so an expired queue is revived (the caller is asserting
    ownership now)."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('Revive', 'TestTopic', 0, true, interval '60 seconds')")

    # Force the queue's keep_alive_until into the past.
    cur.execute("UPDATE queues SET keep_alive_until = NOW() - interval '1 hour' WHERE name = 'Revive'")
    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'Revive'")
    expired_at = cur.fetchone()[0]
    now_pre = datetime.now(pytz.UTC)
    assert expired_at < now_pre

    # Idempotent re-create with same params → revives the queue.
    cur.execute("SELECT create_queue('Revive', 'TestTopic', 0, true, interval '60 seconds')")

    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'Revive'")
    after = cur.fetchone()[0]
    now_post = datetime.now(pytz.UTC)
    assert after > now_post, f"keep_alive_until must be in the future after revive: {after}"
    # Should land at ~now+60s; allow a comfortable window for clock granularity.
    assert after > now_post + timedelta(seconds=55)
    assert after < now_post + timedelta(seconds=65)

def test_create_queue_idempotent_does_not_touch_nonexclusive(cur: psycopg2.extensions.cursor) -> None:
    """The keep_alive_until refresh in the idempotent path is gated on
    p_exclusive; non-exclusive queues' keep_alive_until stays NULL across
    re-creates."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('Plain', 'TestTopic', 0, false)")
    cur.execute("SELECT create_queue('Plain', 'TestTopic', 0, false)")
    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'Plain'")
    assert cur.fetchone()[0] is None

def test_create_queue_rejects_negative_max_attempts(cur: psycopg2.extensions.cursor) -> None:
    """create_queue must reject p_max_attempts < 0 with PMQ03. A negative
    value silently breaks consume_message (the filter
    qm.delivery_attempts < tq.max_delivery_attempts becomes never-true once
    delivery_attempts climbs past the negative threshold) and skips DLQ
    retirement (nack_message + pmq_maintenance_fast both gate on > 0)."""
    cur.execute("SELECT create_topic('TestTopic')")
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT create_queue('Bad', 'TestTopic', -1, false)")
    assert exc_info.value.pgcode == 'PMQ03'
    assert "must be >= 0" in str(exc_info.value)

def test_create_queue_missing_topic_raises_pmq02(cur: psycopg2.extensions.cursor) -> None:
    """create_queue must raise PMQ02 (not raw 23503 FK violation) when the
    referenced topic doesn't exist, so the client can map it to its
    QueueNotFoundError sentinel — same shape as publish_message."""
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT create_queue('Q', 'NoSuchTopic', 0, false)")
    assert exc_info.value.pgcode == 'PMQ02', \
        f"expected PMQ02, got {exc_info.value.pgcode}: {exc_info.value}"
    assert "does not exist" in str(exc_info.value)

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
        SELECT * FROM pmq_maintenance_fast();
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
        SELECT create_queue('NonExQueue', 'TestTopic', 2, false, interval '300 seconds');
    """)

    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'NonExQueue'")
    assert cur.fetchone()[0] is None

def test_consume_refreshes_keep_alive_for_exclusive(cur: psycopg2.extensions.cursor) -> None:
    """consume_message must advance keep_alive_until for exclusive queues so an
    actively-polling consumer cannot have its queue GC'd by drift in the
    explicit extend_queue_keep_alive timer."""
    cur.execute("SELECT create_topic('TestTopic')")
    # 60s keep-alive so the test window is comfortable.
    cur.execute("SELECT create_queue('ExActive', 'TestTopic', 0, true, interval '60 seconds')")
    cur.execute("SELECT publish_message('TestTopic', '{\"x\": 1}'::jsonb)")

    # Snapshot keep_alive_until and pin queue's view of NOW(), then consume.
    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'ExActive'")
    before = cur.fetchone()[0]

    # Sleep briefly so NOW() advances; the refresh is NOW() + interval, so the
    # post-consume value must be strictly later than the pre-consume value.
    time.sleep(1.2)
    cur.execute("SELECT consume_message('ExActive', 30, 1)")

    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'ExActive'")
    after = cur.fetchone()[0]
    assert after > before, f"keep_alive_until should advance on consume: before={before} after={after}"
    # Must be ~now+60s (the interval), not now+30 (vt) or unchanged.
    now = datetime.now(pytz.UTC)
    assert after > now + timedelta(seconds=55)
    assert after < now + timedelta(seconds=65)

def test_consume_does_not_touch_keep_alive_for_non_exclusive(cur: psycopg2.extensions.cursor) -> None:
    """Non-exclusive queues have keep_alive_until = NULL; consume must leave it alone."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('NonExActive', 'TestTopic', 0, false)")
    cur.execute("SELECT publish_message('TestTopic', '{\"x\": 1}'::jsonb)")

    cur.execute("SELECT consume_message('NonExActive', 30, 1)")

    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'NonExActive'")
    assert cur.fetchone()[0] is None

def test_consume_does_not_revive_expired_exclusive_queue(cur: psycopg2.extensions.cursor) -> None:
    """If keep_alive_until has already expired the queue is logically dead and
    consume must NOT resurrect it via the implicit refresh — otherwise an
    expired queue could survive indefinitely if a stale consumer keeps polling."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('ExExpired', 'TestTopic', 0, true, interval '60 seconds')")
    # Force expiry.
    cur.execute("UPDATE queues SET keep_alive_until = NOW() - interval '5 seconds' WHERE name = 'ExExpired'")

    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'ExExpired'")
    expired_at = cur.fetchone()[0]
    cur.execute("SELECT consume_message('ExExpired', 30, 1)")

    cur.execute("SELECT keep_alive_until FROM queues WHERE name = 'ExExpired'")
    after = cur.fetchone()[0]
    assert after == expired_at, "expired exclusive queue must not be revived by consume"

def test_pmq_maintenance_fast(cur: psycopg2.extensions.cursor) -> None:
    """pmq_maintenance_fast retires crashed-final-attempt rows to DLQ and
    reaps expired exclusive queues, returning counters for both. Both
    branches must fire when the conditions are met, and the function must
    be idempotent (counters drop to 0 on a second call).

    A "crashed-final-attempt" row has status='processing' AND vt <= NOW()
    AND delivery_attempts >= max — i.e. a consumer started the final
    attempt, the lease has expired, and nobody acked/nacked. The retire
    predicate is gated on all three conditions; merely hitting the attempt
    cap with a still-valid lease (the consumer is mid-handler) must NOT
    retire the row — see test_maintenance_fast_skips_healthy_in_flight."""
    cur.execute("SELECT create_topic('MaintTopic')")
    # Queue with max_attempts=2 so we can manufacture a crashed-final-attempt
    # row by bumping delivery_attempts past the threshold AND expiring vt
    # without going through nack — mimicking a consumer that died after
    # consuming the last allowed attempt. consume_message refuses to re-pick
    # those rows (delivery_attempts >= max), so they'd stay stuck in
    # 'processing' without this recovery path.
    cur.execute("SELECT create_queue('MaintQueue', 'MaintTopic', 2, false)")
    cur.execute("SELECT publish_message('MaintTopic', '{\"x\": 1}'::jsonb)")
    cur.execute("SELECT message_id FROM consume_message('MaintQueue', 30, 1)")
    msg_id = cur.fetchone()[0]
    cur.execute(
        """UPDATE queue_messages
           SET delivery_attempts = 2, vt = NOW() - interval '1 second'
           WHERE queue_name = 'MaintQueue' AND message_id = %s""",
        (msg_id,),
    )

    # Expired exclusive queue (the inactive-queue branch).
    cur.execute("SELECT create_queue('ExpiredEx', 'MaintTopic', 0, true, interval '60 seconds')")
    cur.execute("UPDATE queues SET keep_alive_until = NOW() - interval '1 second' WHERE name = 'ExpiredEx'")

    cur.execute("SELECT retired_to_dlq, inactive_queues_dropped FROM pmq_maintenance_fast()")
    retired, dropped = cur.fetchone()
    assert retired == 1, f"retired_to_dlq should be 1, got {retired}"
    assert dropped == 1, f"inactive_queues_dropped should be 1, got {dropped}"

    # Idempotency: a second call has nothing to do.
    cur.execute("SELECT retired_to_dlq, inactive_queues_dropped FROM pmq_maintenance_fast()")
    retired2, dropped2 = cur.fetchone()
    assert retired2 == 0 and dropped2 == 0

    # The retired message landed in the DLQ.
    cur.execute("SELECT message_id FROM dead_letter_queue WHERE queue_name = 'MaintQueue'")
    assert cur.fetchone()[0] == msg_id
    # The expired exclusive queue is gone.
    cur.execute("SELECT count(*) FROM queues WHERE name = 'ExpiredEx'")
    assert cur.fetchone()[0] == 0

def test_maintenance_fast_skips_healthy_in_flight(cur: psycopg2.extensions.cursor) -> None:
    """A consumer mid-handler on its FINAL attempt (delivery_attempts == max,
    status='processing', vt > NOW()) must NOT be retired by pmq_maintenance_fast.
    Otherwise the consumer's handler commits its side-effects, then ack returns
    PMQ01 because the row was deleted underneath it, and the message ALSO lands
    in the DLQ — silent inconsistency between application and queue state.

    Reproduces the bug reported in REVIEW.md §1.2/§2.1."""
    cur.execute("SELECT create_topic('HealthyMaintTopic')")
    cur.execute("SELECT create_queue('HealthyMaintQueue', 'HealthyMaintTopic', 1, false)")
    cur.execute("SELECT publish_message('HealthyMaintTopic', '{\"x\": 1}'::jsonb)")

    # Consume with a comfortable VT so the lease is solidly in the future.
    # max_delivery_attempts=1 so this single consume puts us at the limit.
    cur.execute("""
        SELECT message_id, consumer_token, delivery_attempts, vt
        FROM consume_message('HealthyMaintQueue', 60, 1)
    """)
    row = cur.fetchone()
    msg_id, token, attempts, vt = row
    assert attempts == 1, f"expected delivery_attempts=1 (== max), got {attempts}"

    # Run maintenance: the row hits delivery_attempts >= max but the lease is
    # still valid. Must NOT be retired.
    cur.execute("SELECT retired_to_dlq FROM pmq_maintenance_fast()")
    retired = cur.fetchone()[0]
    assert retired == 0, "healthy in-flight final-attempt row must not be retired"

    # Row is still processing, still owned by the original consumer.
    cur.execute("""
        SELECT status, consumer_token FROM queue_messages
        WHERE queue_name = 'HealthyMaintQueue' AND message_id = %s
    """, (msg_id,))
    status, current_token = cur.fetchone()
    assert status == 'processing'
    assert current_token == token, "consumer_token must be unchanged (lease still held)"

    # And not in DLQ.
    cur.execute("SELECT count(*) FROM dead_letter_queue WHERE queue_name = 'HealthyMaintQueue'")
    assert cur.fetchone()[0] == 0

    # Consumer can still ack successfully — the original goal of the lease.
    cur.execute("SELECT ack_message('HealthyMaintQueue', %s, %s)", (msg_id, token))

def test_maintenance_fast_retires_only_when_vt_expired(cur: psycopg2.extensions.cursor) -> None:
    """Boundary test: same setup as the healthy-in-flight test, but force vt
    into the past. Now the row IS abandoned and must be retired."""
    cur.execute("SELECT create_topic('ExpVtTopic')")
    cur.execute("SELECT create_queue('ExpVtQueue', 'ExpVtTopic', 1, false)")
    cur.execute("SELECT publish_message('ExpVtTopic', '{\"x\": 1}'::jsonb)")
    cur.execute("SELECT message_id FROM consume_message('ExpVtQueue', 60, 1)")
    msg_id = cur.fetchone()[0]

    # Expire the lease — simulates "consumer crashed mid-handler".
    cur.execute(
        "UPDATE queue_messages SET vt = NOW() - interval '1 second' "
        "WHERE queue_name = 'ExpVtQueue' AND message_id = %s",
        (msg_id,),
    )

    cur.execute("SELECT retired_to_dlq FROM pmq_maintenance_fast()")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT count(*) FROM dead_letter_queue WHERE queue_name = 'ExpVtQueue'")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT count(*) FROM queue_messages WHERE queue_name = 'ExpVtQueue'")
    assert cur.fetchone()[0] == 0

def test_maintenance_fast_skips_pending_at_limit(cur: psycopg2.extensions.cursor) -> None:
    """Defensive: a row with status='pending' AND delivery_attempts >= max
    can only exist via a direct UPDATE (not via the public API) but should
    still be ignored by maintenance — retirement is reserved for the
    expired-lease abandonment case."""
    cur.execute("SELECT create_topic('PendingLimitTopic')")
    cur.execute("SELECT create_queue('PendingLimitQueue', 'PendingLimitTopic', 1, false)")
    cur.execute("SELECT publish_message('PendingLimitTopic', '{\"x\": 1}'::jsonb)")
    cur.execute("SELECT message_id FROM consume_message('PendingLimitQueue', 60, 1)")
    msg_id = cur.fetchone()[0]

    # Pathological state: pending but at the attempt cap (and lease expired
    # for good measure — only the status filter should keep this row alive).
    cur.execute(
        """UPDATE queue_messages
           SET status = 'pending',
               vt = NOW() - interval '1 second',
               consumer_token = NULL
           WHERE queue_name = 'PendingLimitQueue' AND message_id = %s""",
        (msg_id,),
    )

    cur.execute("SELECT retired_to_dlq FROM pmq_maintenance_fast()")
    assert cur.fetchone()[0] == 0, "pending rows are out of scope of maintenance retirement"
    cur.execute("SELECT count(*) FROM queue_messages WHERE queue_name = 'PendingLimitQueue'")
    assert cur.fetchone()[0] == 1

def test_release_floor_at_zero(cur: psycopg2.extensions.cursor) -> None:
    """release_message must floor delivery_attempts at 0. Without the GREATEST
    guard a stale consumer racing a reclaim path could underflow the counter
    on repeated releases. The CHECK constraint on the column is the backstop
    — a direct UPDATE that would push it negative is rejected."""
    cur.execute("SELECT create_topic('FloorTopic')")
    cur.execute("SELECT create_queue('FloorQueue', 'FloorTopic', 0, false)")
    cur.execute("SELECT publish_message('FloorTopic', '{\"x\": 1}'::jsonb)")

    # Consume → delivery_attempts becomes 1, status 'processing'.
    cur.execute("SELECT message_id, consumer_token FROM consume_message('FloorQueue', 30, 1)")
    msg_id, token = cur.fetchone()
    cur.execute("SELECT delivery_attempts FROM queue_messages WHERE queue_name='FloorQueue' AND message_id=%s", (msg_id,))
    assert cur.fetchone()[0] == 1

    # Release once: 1 → 0.
    cur.execute("SELECT release_message('FloorQueue', %s, %s)", (msg_id, token))
    cur.execute("SELECT delivery_attempts FROM queue_messages WHERE queue_name='FloorQueue' AND message_id=%s", (msg_id,))
    assert cur.fetchone()[0] == 0

    # Re-consume to obtain a fresh token, force delivery_attempts back to 0
    # (bypass the consume increment), then release again. Without the floor
    # this would write -1; with it, the value stays at 0.
    cur.execute("SELECT message_id, consumer_token FROM consume_message('FloorQueue', 30, 1)")
    msg_id2, token2 = cur.fetchone()
    assert msg_id2 == msg_id
    cur.execute("UPDATE queue_messages SET delivery_attempts = 0 WHERE queue_name='FloorQueue' AND message_id=%s", (msg_id,))
    cur.execute("SELECT release_message('FloorQueue', %s, %s)", (msg_id, token2))
    cur.execute("SELECT delivery_attempts FROM queue_messages WHERE queue_name='FloorQueue' AND message_id=%s", (msg_id,))
    assert cur.fetchone()[0] == 0, "release_message must floor delivery_attempts at 0"

def test_delivery_attempts_check_constraint(cur: psycopg2.extensions.cursor) -> None:
    """The queue_messages_delivery_attempts_nonneg CHECK constraint blocks any
    direct UPDATE that would write delivery_attempts < 0 — the backstop for
    paths the application-level floor doesn't cover."""
    cur.execute("SELECT create_topic('CheckTopic')")
    cur.execute("SELECT create_queue('CheckQueue', 'CheckTopic', 0, false)")
    cur.execute("SELECT publish_message('CheckTopic', '{\"x\": 1}'::jsonb)")
    cur.execute("SELECT message_id FROM consume_message('CheckQueue', 30, 1)")
    msg_id = cur.fetchone()[0]

    with pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute(
            "UPDATE queue_messages SET delivery_attempts = -1 WHERE queue_name='CheckQueue' AND message_id=%s",
            (msg_id,),
        )

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
        SELECT * FROM pmq_maintenance_fast();
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
    
    # Test 3: Mismatched array lengths now raise PMQ03 (validation error).
    truncated_tokens = tokens[:-1]
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("""
            SELECT COUNT(*)
            FROM set_vt_batch('BatchCompQueue', %s, %s, 60)
        """, (msg_ids, truncated_tokens))
    assert exc_info.value.pgcode == 'PMQ03', f"expected PMQ03, got {exc_info.value.pgcode}"
    cur.connection.rollback()

    # Test 4: Non-existent queue — does not raise (no rows matched is fine).
    cur.execute("""
        SELECT COUNT(*)
        FROM set_vt_batch('NonExistentQueue', %s, %s, 60)
    """, (msg_ids, tokens))

    # Test 5: Negative VT now raises PMQ03 (validation error).
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("""
            SELECT COUNT(*)
            FROM set_vt_batch('BatchCompQueue', %s, %s, -60)
        """, (msg_ids, tokens))
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()
    
    # Clean up
    cur.execute("DELETE FROM queues WHERE name = 'BatchCompQueue'")
    cur.execute("DELETE FROM topics WHERE name = 'BatchCompTopic'")

def test_requeue_dlq_messages_resets_delivery_attempts(cur):
    # Create topic and queue
    topic = "test_requeue_dlq_topic"
    queue = "test_requeue_dlq_queue"
    cur.execute("SELECT create_topic(%s)", (topic,))
    cur.execute("SELECT create_queue(%s, %s, %s, %s, %s * interval '1 sec')",
                  (queue, topic, 2, True, 60))

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
    cur.execute("SELECT * FROM pmq_maintenance_fast()")

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

def test_requeue_dlq_messages_emits_notify(cur: psycopg2.extensions.cursor) -> None:
    """requeue_dlq_messages emits one NOTIFY on `pmq:q:<queue>` per call
    (regardless of how many messages were requeued). Without this signal
    consumers wait for their poll fallback (1s in TS, 10s in Go) before
    re-fetching, defeating the point of using requeue for fast recovery.
    Reported in REVIEW.md §2.2.

    Also asserts no NOTIFY fires when the DLQ is empty for the queue —
    avoids spurious wake-ups."""
    cur.execute("SELECT create_topic('RequeueNotifyTopic')")
    cur.execute("SELECT create_queue('RequeueNotifyQueue', 'RequeueNotifyTopic', 1, false)")
    cur.execute('LISTEN "pmq:q:RequeueNotifyQueue"')

    # Empty DLQ: requeue should be a silent no-op.
    cur.execute("SELECT requeue_dlq_messages('RequeueNotifyQueue')")
    cur.connection.poll()
    assert len(cur.connection.notifies) == 0, "no-op requeue must not NOTIFY"

    # Stage 3 messages in the DLQ via the inline-retire path (max_attempts=1
    # means the first nack moves them straight to DLQ).
    for i in range(3):
        cur.execute("SELECT publish_message('RequeueNotifyTopic', %s::jsonb)",
                    (json.dumps({"i": i}),))
        cur.execute("SELECT message_id, consumer_token FROM consume_message('RequeueNotifyQueue', 30)")
        msg_id, token = cur.fetchone()
        cur.execute("SELECT nack_message('RequeueNotifyQueue', %s, %s)", (msg_id, token))

    cur.execute("SELECT count(*) FROM dead_letter_queue WHERE queue_name = 'RequeueNotifyQueue'")
    assert cur.fetchone()[0] == 3

    # Drain any stray notifications from the staging steps so we measure
    # only what requeue itself emits.
    cur.connection.poll()
    cur.connection.notifies.clear()

    cur.execute("SELECT requeue_dlq_messages('RequeueNotifyQueue')")
    cur.connection.poll()

    # Exactly one NOTIFY per requeue call, empty payload.
    assert len(cur.connection.notifies) == 1, \
        f"expected exactly 1 NOTIFY, got {len(cur.connection.notifies)}"
    n = cur.connection.notifies[0]
    assert n.channel == 'pmq:q:RequeueNotifyQueue'
    assert n.payload == ''
    cur.connection.notifies.clear()

def test_requeue_dlq_messages_idempotent_on_existing_row(cur: psycopg2.extensions.cursor) -> None:
    """If a queue_messages row already exists for a (queue, message_id) we
    are about to requeue, the function must reset that row to a clean
    pending state instead of aborting the entire requeue with a unique
    constraint violation. This protects against partial-state recovery
    scenarios. Reported in REVIEW.md §2.2."""
    cur.execute("SELECT create_topic('RequeueIdempotentTopic')")
    cur.execute("SELECT create_queue('RequeueIdempotentQueue', 'RequeueIdempotentTopic', 1, false)")

    cur.execute("SELECT publish_message('RequeueIdempotentTopic', '{\"x\":1}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM consume_message('RequeueIdempotentQueue', 30)")
    msg_id, token = cur.fetchone()
    cur.execute("SELECT nack_message('RequeueIdempotentQueue', %s, %s)", (msg_id, token))

    # Now the message is in DLQ. Manually re-insert a stale queue_messages
    # row in 'completed' state to simulate the conflict scenario.
    cur.execute("""
        INSERT INTO queue_messages (queue_name, message_id, status, delivery_attempts, processed_at, vt)
        VALUES ('RequeueIdempotentQueue', %s, 'completed', 5, NOW(), NOW())
    """, (msg_id,))

    # Requeue must NOT fail with a unique violation; it must reset the
    # stale row to pending.
    cur.execute("SELECT requeue_dlq_messages('RequeueIdempotentQueue')")

    cur.execute("""
        SELECT status, delivery_attempts, consumer_token, processed_at
        FROM queue_messages
        WHERE queue_name = 'RequeueIdempotentQueue' AND message_id = %s
    """, (msg_id,))
    status, attempts, ctoken, processed_at = cur.fetchone()
    assert status == 'pending'
    assert attempts == 0
    assert ctoken is None
    assert processed_at is None

    cur.execute("SELECT count(*) FROM dead_letter_queue WHERE queue_name = 'RequeueIdempotentQueue'")
    assert cur.fetchone()[0] == 0, "DLQ should be drained after requeue"

def test_delayed_message_delivery_notifications(cur: psycopg2.extensions.cursor) -> None:
    """Publish fires NOTIFY on per-topic channel `pmq:t:<topic>`. Payload is
    empty — the channel name is the wake-up signal; clients re-fetch from
    the queue on receipt."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 2, false)")

    # Listen on the per-topic channel.
    cur.execute('LISTEN "pmq:t:TestTopic"')

    # Publish message with 2 second delay
    delay_time = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT publish_message('TestTopic', '{"test":"data"}'::jsonb, %s)
    """, (delay_time,))

    # Get notification
    conn = cur.connection
    conn.poll()
    notify = conn.notifies.pop(0)

    assert notify.channel == 'pmq:t:TestTopic'
    assert notify.payload == ''

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
    """Nack fires NOTIFY on the per-queue channel `pmq:q:<queue>`. Payload
    is empty — the channel name is the wake-up signal."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 2, false)")

    # Publish and consume message
    cur.execute("SELECT publish_message('TestTopic', '{\"test\":\"data\"}'::jsonb)")
    cur.execute("""
        SELECT message_id, consumer_token
        FROM consume_message('TestQueue', 30)
    """)
    msg_id, token = cur.fetchone()

    # Listen on the per-queue channel.
    cur.execute('LISTEN "pmq:q:TestQueue"')

    # Nack with 2 second delay
    delay_time = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT nack_message('TestQueue', %s, %s, %s)
    """, (msg_id, token, delay_time))

    # Get notification
    conn = cur.connection
    conn.poll()
    notify = conn.notifies.pop(0)

    assert notify.channel == 'pmq:q:TestQueue'
    assert notify.payload == ''

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

def test_release_message_emits_per_queue_notify(cur: psycopg2.extensions.cursor) -> None:
    """release_message fires NOTIFY on `pmq:q:<queue>` with empty payload —
    the channel is the wake-up signal; clients re-fetch on receipt."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 0, false)")

    cur.execute("SELECT publish_message('TestTopic', '{\"k\":\"v\"}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM consume_message('TestQueue', 30)")
    msg_id, token = cur.fetchone()

    cur.execute('LISTEN "pmq:q:TestQueue"')
    cur.execute("SELECT release_message('TestQueue', %s, %s)", (msg_id, token))

    conn = cur.connection
    conn.poll()
    notify = conn.notifies.pop(0)
    assert notify.channel == 'pmq:q:TestQueue'
    assert notify.payload == ''

def test_create_topic_validates_name(cur: psycopg2.extensions.cursor) -> None:
    """create_topic rejects names with characters that wouldn't be safe as NOTIFY channel suffixes."""
    cur.execute("SELECT create_topic('valid_topic-1.2:3')")  # all permitted chars
    for bad in ["topic with spaces", "topic'name", 'topic"name', "topic;name", ""]:
        try:
            cur.execute("SELECT create_topic(%s)", (bad,))
            raise AssertionError(f"create_topic should have rejected {bad!r}")
        except psycopg2.Error:
            cur.connection.rollback()

def test_create_queue_validates_name(cur: psycopg2.extensions.cursor) -> None:
    """create_queue rejects names with characters that wouldn't be safe as NOTIFY channel suffixes."""
    cur.execute("SELECT create_topic('Topic')")
    cur.execute("SELECT create_queue('valid_queue-1.2:3', 'Topic', 0, false)")
    for bad in ["queue with spaces", "queue'name", 'queue"name', "queue;name", ""]:
        try:
            cur.execute("SELECT create_queue(%s, 'Topic', 0, false)", (bad,))
            raise AssertionError(f"create_queue should have rejected {bad!r}")
        except psycopg2.Error:
            cur.connection.rollback()

def test_sqlstate_codes_pmq01_lease_lost(cur: psycopg2.extensions.cursor) -> None:
    """ack/nack/release/set_vt all raise PMQ01 when token doesn't match or vt expired."""
    cur.execute("SELECT create_topic('SqlstateTopic')")
    cur.execute("SELECT create_queue('SqlstateQueue', 'SqlstateTopic', 0, false)")
    cur.execute("SELECT publish_message('SqlstateTopic', '{}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM consume_message('SqlstateQueue', 30)")
    msg_id, _ = cur.fetchone()

    # Wrong token → PMQ01 from each operation.
    for sql, args in [
        ("SELECT ack_message('SqlstateQueue', %s, %s)", (msg_id, 'bogus')),
        ("SELECT nack_message('SqlstateQueue', %s, %s)", (msg_id, 'bogus')),
        ("SELECT release_message('SqlstateQueue', %s, %s)", (msg_id, 'bogus')),
        ("SELECT set_vt('SqlstateQueue', %s, %s, 60)", (msg_id, 'bogus')),
    ]:
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute(sql, args)
        assert exc_info.value.pgcode == 'PMQ01', f"{sql}: expected PMQ01, got {exc_info.value.pgcode}"
        cur.connection.rollback()

def test_sqlstate_codes_pmq02_topic_not_found(cur: psycopg2.extensions.cursor) -> None:
    """publish_message on a non-existent topic raises PMQ02."""
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT publish_message('NoSuchTopic', '{}'::jsonb)")
    assert exc_info.value.pgcode == 'PMQ02', f"expected PMQ02, got {exc_info.value.pgcode}"
    cur.connection.rollback()

def test_sqlstate_codes_pmq03_validation(cur: psycopg2.extensions.cursor) -> None:
    """Validation paths raise PMQ03: negative vt, p_limit<=0, mismatched arrays, malformed names."""
    cur.execute("SELECT create_topic('VTopic')")
    cur.execute("SELECT create_queue('VQueue', 'VTopic', 0, false)")

    # consume_message: negative vt
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT * FROM consume_message('VQueue', -1, 1)")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # consume_message: p_limit <= 0
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT * FROM consume_message('VQueue', 30, 0)")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # set_vt: negative vt
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT set_vt('VQueue', 1, 'tok', -1)")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # set_vt_batch: mismatched array lengths
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("""
            SELECT * FROM set_vt_batch('VQueue', ARRAY[1,2]::int[], ARRAY['a']::varchar[], 60)
        """)
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # create_topic: invalid name
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT create_topic('bad name')")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # create_queue: invalid name
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT create_queue('bad queue', 'VTopic', 0, false)")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

def test_nack_final_attempt_inline_dlq(cur: psycopg2.extensions.cursor) -> None:
    """Final-attempt nack moves the row to DLQ inline (no maintenance call needed)."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 2, false)")  # max_attempts=2
    cur.execute("SELECT publish_message('TestTopic', '{\"k\":\"v\"}'::jsonb)")

    # First consume + nack: not the final attempt, message resets to pending.
    cur.execute("""
        SELECT message_id, consumer_token FROM consume_message('TestQueue', 30)
    """)
    msg_id, token = cur.fetchone()
    cur.execute("SELECT nack_message('TestQueue', %s, %s, NOW())", (msg_id, token))

    cur.execute("""
        SELECT status, delivery_attempts FROM queue_messages
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    row = cur.fetchone()
    assert row['status'] == 'pending'
    assert row['delivery_attempts'] == 1

    # Second consume + nack: this is the final attempt; should retire inline.
    cur.execute("""
        SELECT message_id, consumer_token FROM consume_message('TestQueue', 30)
    """)
    msg_id2, token2 = cur.fetchone()
    assert msg_id2 == msg_id

    cur.execute("SELECT nack_message('TestQueue', %s, %s, NOW())", (msg_id, token2))

    # Row should be gone from queue_messages and present in DLQ — without
    # any maintenance call.
    cur.execute("""
        SELECT 1 FROM queue_messages
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    assert cur.fetchone() is None

    cur.execute("""
        SELECT retry_count FROM dead_letter_queue
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    dlq_row = cur.fetchone()
    assert dlq_row is not None
    assert dlq_row['retry_count'] == 2

def test_nack_non_final_does_not_dlq(cur: psycopg2.extensions.cursor) -> None:
    """A non-final nack must not retire the message to DLQ even if retries remain."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 5, false)")
    cur.execute("SELECT publish_message('TestTopic', '{\"k\":\"v\"}'::jsonb)")

    cur.execute("SELECT message_id, consumer_token FROM consume_message('TestQueue', 30)")
    msg_id, token = cur.fetchone()
    cur.execute("SELECT nack_message('TestQueue', %s, %s, NOW())", (msg_id, token))

    cur.execute("""
        SELECT status FROM queue_messages
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    assert cur.fetchone()['status'] == 'pending'

    cur.execute("""
        SELECT 1 FROM dead_letter_queue
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    assert cur.fetchone() is None

def test_unlimited_attempts_never_dlq_inline(cur: psycopg2.extensions.cursor) -> None:
    """When max_delivery_attempts=0 (unlimited), nack must never retire inline."""
    cur.execute("SELECT create_topic('TestTopic')")
    cur.execute("SELECT create_queue('TestQueue', 'TestTopic', 0, false)")
    cur.execute("SELECT publish_message('TestTopic', '{\"k\":\"v\"}'::jsonb)")

    for _ in range(5):
        cur.execute("SELECT message_id, consumer_token FROM consume_message('TestQueue', 30)")
        msg_id, token = cur.fetchone()
        cur.execute("SELECT nack_message('TestQueue', %s, %s, NOW())", (msg_id, token))

    cur.execute("SELECT count(*) FROM dead_letter_queue WHERE queue_name='TestQueue'")
    assert cur.fetchone()[0] == 0

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
    cur.execute("SELECT * FROM pmq_maintenance_fast()")
    
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
    cur.execute("SELECT * FROM pmq_maintenance_fast()")
    
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
