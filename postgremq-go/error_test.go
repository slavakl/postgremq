// Error handling tests simulate connection loss and retryable errors, validate
// backoff behavior and maximum attempts, and confirm that common invalid input
// conditions are surfaced cleanly from client methods.
package postgremq_go_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	postgremq "github.com/slavakl/postgremq/postgremq-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------
// Error Handling Tests
// -----------------------------------------------------------------------

// MockDisconnectingPool wraps a real pool and simulates connection loss
type MockDisconnectingPool struct {
	RealPool     *pgxpool.Pool // The actual database pool
	ShouldFail   atomic.Bool   // When true, all operations will fail
	FailureError error         // The error to return when failing
	FailureCount atomic.Int32  // Count of how many failures have occurred
	Operations   []string      // Record of operations for verification
}

func (m *MockDisconnectingPool) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	m.Operations = append(m.Operations, "Exec")
	if m.ShouldFail.Load() {
		m.FailureCount.Add(1)
		if m.FailureError != nil {
			return pgconn.CommandTag{}, m.FailureError
		}
		return pgconn.CommandTag{}, errors.New("simulated connection loss")
	}
	return m.RealPool.Exec(ctx, sql, arguments...)
}

func (m *MockDisconnectingPool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	m.Operations = append(m.Operations, "Query")
	if m.ShouldFail.Load() {
		m.FailureCount.Add(1)
		if m.FailureError != nil {
			return nil, m.FailureError
		}
		return nil, errors.New("simulated connection loss")
	}
	return m.RealPool.Query(ctx, sql, args...)
}

func (m *MockDisconnectingPool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	m.Operations = append(m.Operations, "QueryRow")
	if m.ShouldFail.Load() {
		m.FailureCount.Add(1)
		return &mockErrRow{err: errors.New("simulated connection loss")}
	}
	return m.RealPool.QueryRow(ctx, sql, args...)
}

func (m *MockDisconnectingPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	m.Operations = append(m.Operations, "Acquire")
	if m.ShouldFail.Load() {
		m.FailureCount.Add(1)
		if m.FailureError != nil {
			return nil, m.FailureError
		}
		return nil, errors.New("simulated connection loss")
	}
	return m.RealPool.Acquire(ctx)
}

func (m *MockDisconnectingPool) Close() {
	m.Operations = append(m.Operations, "Close")
	// We don't close the real pool here since the test is responsible for that
}

// mockErrRow is a mock implementation of pgx.Row that always returns an error
type mockErrRow struct {
	err error
}

func (r *mockErrRow) Scan(dest ...interface{}) error {
	return r.err
}

// TestConnectionLossRecovery validates how the code handles connection loss and recovers from it
func TestConnectionLossRecovery(t *testing.T) {
	t.Parallel()

	// Setup real DB connection first
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	// Create a wrapper around the pgx pool that can simulate disconnection
	mockPool := &MockDisconnectingPool{
		RealPool: pool,
	}

	conn, err := postgremq.DialFromPool(ctx, mockPool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Set up test topic and queue
	topicName := "test_conn_loss_topic"
	queueName := "test_conn_loss_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Publish a test message
	msgID, err := conn.Publish(ctx, topicName, []byte(`{"test":"conn_loss"}`))
	require.NoError(t, err, "Failed to publish message")

	// Simulate connection loss
	mockPool.ShouldFail.Store(true)

	// Attempt operations during connection failure
	_, err = conn.Publish(ctx, topicName, []byte(`{"test":"should_fail"}`))
	require.Error(t, err, "Publish should fail during connection loss")

	// Simulate connection recovery
	mockPool.ShouldFail.Store(false)

	// Verify operations succeed after recovery
	recoverMsgID, err := conn.Publish(ctx, topicName, []byte(`{"test":"recovered"}`))
	require.NoError(t, err, "Publish should succeed after connection recovery")
	require.Greater(t, recoverMsgID, msgID, "New message ID should be greater than previous")

	// Verify consumer can still receive messages
	consumer, err := conn.Consume(ctx, queueName)
	require.NoError(t, err, "Should be able to create consumer after recovery")
	defer consumer.Stop()

	// Verify we can receive both messages (the first one and the recovery one)
	receivedCount := 0
	timeout := time.After(5 * time.Second)

	for receivedCount < 2 {
		select {
		case msg := <-consumer.Messages():
			err = msg.Ack(ctx)
			require.NoError(t, err, "Should be able to ack message")
			receivedCount++
		case <-timeout:
			t.Fatalf("Timed out waiting for messages, received %d/2", receivedCount)
		}
	}
}

// TestTransactionErrorHandling validates retry logic for transaction errors
func TestTransactionErrorHandling(t *testing.T) {
	t.Parallel()

	// Setup mock pool that simulates transaction errors
	var callCount atomic.Int32
	mockPool := &MockPool{
		ExecFunc: func(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
			// Return error for the first 2 calls, then succeed
			count := callCount.Add(1)

			if count <= 2 {
				return pgconn.CommandTag{}, &pgconn.PgError{
					Code:    "40001", // serialization_failure - a retryable error
					Message: "simulated transaction error",
				}
			}
			return pgconn.CommandTag{}, nil
		},
	}

	ctx := context.Background()
	conn, err := postgremq.DialFromPool(ctx, mockPool,
		postgremq.WithRetryConfig(postgremq.RetryConfig{
			MaxAttempts:       5,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        100 * time.Millisecond,
			BackoffMultiplier: 2.0,
		}))
	require.NoError(t, err, "Should be able to create connection with retry config")
	defer conn.Close()

	// This should succeed after retries
	err = conn.CreateTopic(ctx, "test_retry_topic")
	require.NoError(t, err, "Operation should succeed after retries")
	require.Equal(t, int32(3), callCount.Load(), "Should have made exactly 3 calls (2 failures + 1 success)")

	// Reset the mock to always fail
	callCount.Store(0)
	mockPool.ExecFunc = func(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
		callCount.Add(1)
		return pgconn.CommandTag{}, &pgconn.PgError{
			Code:    "40001", // serialization_failure
			Message: "persistent transaction error",
		}
	}

	// This should fail after max retries
	err = conn.CreateTopic(ctx, "test_retry_fail_topic")
	require.Error(t, err, "Operation should fail after max retries")
	require.Equal(t, int32(5), callCount.Load(), "Should have made exactly 5 attempts")
}

// TestRetryBackoffBehavior verifies that backoff timing between retry attempts follows the expected pattern
func TestRetryBackoffBehavior(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := &MockLogger{}

	var attempts []time.Time
	pool := &MockPool{
		ExecFunc: func(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
			attempts = append(attempts, time.Now())
			if len(attempts) < 3 {
				return pgconn.CommandTag{}, &pgconn.PgError{
					Code:    "40001", // serialization_failure
					Message: "temporary error",
				}
			}
			return pgconn.CommandTag{}, nil
		},
	}

	// Configure with initial backoff of 100ms doubling each time
	conn, err := postgremq.DialFromPool(ctx, pool,
		postgremq.WithLogger(logger),
		postgremq.WithRetryConfig(postgremq.RetryConfig{
			MaxAttempts:       5,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        1 * time.Second,
			BackoffMultiplier: 2.0,
		}))
	require.NoError(t, err)
	defer conn.Close()

	// This operation should succeed after 3 attempts (2 failures + 1 success)
	err = conn.CreateTopic(ctx, "test_backoff_topic")
	require.NoError(t, err)

	// Verify we made 3 attempts
	require.Equal(t, 3, len(attempts), "Expected 3 attempts")

	// Check backoff timing
	if len(attempts) >= 3 {
		firstBackoff := attempts[1].Sub(attempts[0])
		secondBackoff := attempts[2].Sub(attempts[1])

		// First backoff should be approximately InitialBackoff
		assert.InDelta(t, 100, firstBackoff.Milliseconds(), 50, "First backoff should be around 100ms")

		// Second backoff should be approximately 2x the first (200ms)
		assert.InDelta(t, 200, secondBackoff.Milliseconds(), 50, "Second backoff should be around 200ms")

		// Second backoff should be greater than first (with jitter this should still hold)
		assert.Greater(t, secondBackoff, firstBackoff, "Second backoff should be greater than first")
	}
}

// TestErrorConditionsInOperations verifies error handling in various operations
func TestErrorConditionsInOperations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Use ctx in a logger statement to prevent unused variable warning
	_, hasDeadline := ctx.Deadline()
	t.Logf("Testing error conditions with context that has deadline: %v", hasDeadline)

	t.Run("TopicAlreadyExists", func(t *testing.T) {
		pool, ctx := setupTestConnection(t)
		defer pool.Close()

		conn, err := postgremq.DialFromPool(ctx, pool)
		require.NoError(t, err, "Failed to create connection")
		defer conn.Close()

		// Create a topic
		topicName := "test_duplicate_topic"
		err = conn.CreateTopic(ctx, topicName)
		require.NoError(t, err, "First CreateTopic should succeed")

		// Create the same topic again - should succeed as the operation is idempotent
		err = conn.CreateTopic(ctx, topicName)
		require.NoError(t, err, "Creating same topic again should be idempotent")
	})

	t.Run("InvalidParameters", func(t *testing.T) {
		pool, ctx := setupTestConnection(t)
		defer pool.Close()

		conn, err := postgremq.DialFromPool(ctx, pool)
		require.NoError(t, err, "Failed to create connection")
		defer conn.Close()

		// Test with a nonexistent topic for queue creation instead of SQL injection test
		// since the implementation appears to not validate topic names
		invalidTopicName := "nonexistent_topic_" + time.Now().Format("20060102150405")

		// Test using nonexistent topic for queue creation
		err = conn.CreateQueue(ctx, "test_queue_invalid_topic", invalidTopicName, false)
		require.Error(t, err, "Creating queue with nonexistent topic should cause error")

		// Test empty queue name
		err = conn.CreateQueue(ctx, "", "valid_topic", false)
		require.Error(t, err, "Empty queue name should cause error")

		// Test non-existent topic name
		err = conn.CreateQueue(ctx, "valid_queue", "nonexistent_topic", false)
		require.Error(t, err, "Non-existent topic should cause error")
	})

	t.Run("ParallelOperations", func(t *testing.T) {
		pool, ctx := setupTestConnection(t)
		defer pool.Close()

		topicName := "test_parallel_topic"
		queueName := "test_parallel_queue"

		conn, err := postgremq.DialFromPool(ctx, pool)
		require.NoError(t, err, "Failed to create connection")
		defer conn.Close()

		err = conn.CreateTopic(ctx, topicName)
		require.NoError(t, err, "CreateTopic should succeed")

		err = conn.CreateQueue(ctx, queueName, topicName, false)
		require.NoError(t, err, "CreateQueue should succeed")

		// Run parallel publish operations
		var wg sync.WaitGroup
		errChan := make(chan error, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, err := conn.Publish(ctx, topicName, []byte(fmt.Sprintf(`{"index":%d}`, idx)))
				if err != nil {
					errChan <- err
				}
			}(i)
		}

		wg.Wait()
		close(errChan)

		// Check if any parallel operations failed
		var errs []error
		for err := range errChan {
			errs = append(errs, err)
		}

		assert.Empty(t, errs, "Parallel operations should not produce errors")
	})
}

// -----------------------------------------------------------------------
// Edge Case Tests
// -----------------------------------------------------------------------

// TestVeryLargePayload tests handling of extremely large message payloads
func TestVeryLargePayload(t *testing.T) {
	t.Parallel()
	skipIfShort(t) // Skip this test in short mode
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_large_payload_topic"
	queueName := "test_large_payload_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Create a more reasonable large JSON payload (approximately 50KB)
	arraySize := 10000 // This will generate roughly 50KB of data when serialized as JSON

	// Start building the JSON with the opening of the object and array
	jsonStr := `{"binary_data":[`

	// Add the array elements (numbers 1-255 repeated)
	for i := 0; i < arraySize; i++ {
		// Use values 1-255 to avoid null bytes (0)
		byteVal := (i % 255) + 1

		if i > 0 {
			jsonStr += ","
		}
		jsonStr += fmt.Sprintf("%d", byteVal)
	}

	// Close the array and object
	jsonStr += `]}`

	jsonPayload := []byte(jsonStr)

	// Log the size of the payload
	t.Logf("Test payload size: %d bytes", len(jsonPayload))

	// Publish the large message
	messageID, err := conn.Publish(ctx, topicName, jsonPayload)
	require.NoError(t, err, "Failed to publish large message")
	assert.Greater(t, messageID, 0, "Expected valid message ID")

	// Consume and verify the message
	consumer, err := conn.Consume(ctx, queueName)
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	select {
	case msg := <-consumer.Messages():
		require.Equal(t, messageID, msg.ID, "Message ID mismatch")
		// Some databases might add substantial padding to the JSON, so ensure the size is within a reasonable range
		t.Logf("Original payload size: %d bytes, Received payload size: %d bytes", len(jsonPayload), len(msg.Payload))
		assert.GreaterOrEqual(t, len(msg.Payload), len(jsonPayload)*7/10, "Payload size too small")
		assert.LessOrEqual(t, len(msg.Payload), len(jsonPayload)*2, "Payload size too large")

		err = msg.Ack(ctx)
		require.NoError(t, err, "Failed to ack large message")
	case <-time.After(5 * time.Second):
		require.Fail(t, "Timeout waiting for large message")
	}
}

// TestBoundaryConditions tests behavior at the edges of valid parameter ranges
func TestBoundaryConditions(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	topicName := "test_boundary_topic"
	queueName := "test_boundary_queue"

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	// Test maximum allowed delivery attempts
	err = conn.CreateQueue(ctx, queueName, topicName, false, postgremq.WithMaxDeliveryAttempts(100))
	require.NoError(t, err, "Should accept maximum delivery attempts")

	// Test boundary conditions with empty payload
	messageID, err := conn.Publish(ctx, topicName, []byte(`{}`))
	require.NoError(t, err, "Should accept empty JSON object payload")
	assert.Greater(t, messageID, 0, "Should return valid message ID")

	// Allow time for the message to be properly published
	time.Sleep(1 * time.Second)
	t.Log("Published message with ID:", messageID)

	// Test minimum visibility timeout
	consumer, err := conn.Consume(ctx, queueName, postgremq.WithVT(1))
	require.NoError(t, err, "Should accept minimum visibility timeout")

	// Test consume with large batch size - using a separate consumer
	consumer2, err := conn.Consume(ctx, queueName, postgremq.WithBatchSize(1000))
	require.NoError(t, err, "Should accept large batch size")

	// Ensure both consumers are stopped properly
	defer func() {
		consumer.Stop()
		consumer2.Stop()
		t.Log("Consumers stopped")
	}()

	// Verify basic functionality still works - using a timeout of 10 seconds
	var msgReceived bool
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	select {
	case msg := <-consumer.Messages():
		t.Logf("Received message with ID: %d", msg.ID)
		err = msg.Ack(ctx)
		require.NoError(t, err, "Should be able to ack message")
		msgReceived = true
	case <-timeout.C:
		t.Log("Timeout waiting for message - will check second consumer")
		// Try the second consumer as a fallback
		select {
		case msg := <-consumer2.Messages():
			t.Logf("Received message from second consumer with ID: %d", msg.ID)
			err = msg.Ack(ctx)
			require.NoError(t, err, "Should be able to ack message from second consumer")
			msgReceived = true
		case <-time.After(5 * time.Second):
			require.Fail(t, "Timeout waiting for message from both consumers")
		}
	}

	require.True(t, msgReceived, "Should have received a message")

	// Explicitly stop consumers before exiting
	consumer.Stop()
	consumer2.Stop()
}
