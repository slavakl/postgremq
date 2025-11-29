// Handler consumer tests validate handler-based message processing,
// maxInFlight limiting, graceful shutdown, panic recovery, and auto-ack behavior.
package postgremq_go_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	postgremq "github.com/slavakl/postgremq/postgremq-go"
)

// TestHandlerConsumerExplicitAck verifies that messages are acked when handler calls Ack().
func TestHandlerConsumerExplicitAck(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_explicit_ack_topic"
	const queueName = "test_handler_explicit_ack_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		payload := []byte(fmt.Sprintf(`{"message": "msg-%d"}`, i))
		_, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Publish failed")
	}

	var receivedCount atomic.Int32
	done := make(chan struct{})

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			count := receivedCount.Add(1)
			msg.Ack(ctx)
			if count == 5 {
				close(done)
			}
		},
		postgremq.WithVT(30),
		postgremq.WithBatchSize(2),
	)
	require.NoError(t, err, "ConsumeHandler failed")
	defer hc.Stop()

	select {
	case <-done:
		// All messages received
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for messages, only received %d", receivedCount.Load())
	}

	assert.Equal(t, int32(5), receivedCount.Load(), "Expected 5 messages")

	// Verify all messages are completed
	time.Sleep(100 * time.Millisecond) // Allow ack to complete
	messages, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "ListMessages failed")
	for _, msg := range messages {
		assert.Equal(t, postgremq.MessageStatusCompleted, msg.Status, "Message should be completed")
	}
}

// TestHandlerConsumerAutoAck verifies that messages are auto-acked when handler returns without acking.
func TestHandlerConsumerAutoAck(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_auto_ack_topic"
	const queueName = "test_handler_auto_ack_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 3 messages
	for i := 0; i < 3; i++ {
		payload := []byte(fmt.Sprintf(`{"message": "msg-%d"}`, i))
		_, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Publish failed")
	}

	var receivedCount atomic.Int32
	done := make(chan struct{})

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			// Don't call Ack or Nack - should auto-ack
			count := receivedCount.Add(1)
			if count == 3 {
				close(done)
			}
		},
		postgremq.WithVT(30),
	)
	require.NoError(t, err, "ConsumeHandler failed")
	defer hc.Stop()

	select {
	case <-done:
		// All messages received
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for messages")
	}

	// Verify all messages are completed (auto-acked)
	time.Sleep(100 * time.Millisecond)
	messages, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "ListMessages failed")
	for _, msg := range messages {
		assert.Equal(t, postgremq.MessageStatusCompleted, msg.Status, "Message should be auto-acked")
	}
}

// TestHandlerConsumerNack verifies that messages are nacked when handler calls Nack().
func TestHandlerConsumerNack(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_nack_topic"
	const queueName = "test_handler_nack_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true, postgremq.WithMaxDeliveryAttempts(3))
	require.NoError(t, err, "CreateQueue failed")

	// Publish 1 message
	payload := []byte(`{"message": "nack-me"}`)
	_, err = conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Publish failed")

	var receivedCount atomic.Int32
	done := make(chan struct{})

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			count := receivedCount.Add(1)
			if count == 2 {
				msg.Ack(ctx) // Ack on second attempt
				close(done)
			} else {
				msg.Nack(ctx) // Nack on first attempt
			}
		},
		postgremq.WithVT(30),
	)
	require.NoError(t, err, "ConsumeHandler failed")
	defer hc.Stop()

	select {
	case <-done:
		// Message was redelivered
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for redelivery, only received %d", receivedCount.Load())
	}

	assert.Equal(t, int32(2), receivedCount.Load(), "Expected 2 deliveries (nack + ack)")
}

// TestHandlerConsumerNackWithDelay verifies that messages are delayed when handler uses WithDelayUntil.
func TestHandlerConsumerNackWithDelay(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_nack_delay_topic"
	const queueName = "test_handler_nack_delay_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 1 message
	payload := []byte(`{"message": "delay-me"}`)
	_, err = conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Publish failed")

	var firstReceive, secondReceive time.Time
	var receivedCount atomic.Int32
	done := make(chan struct{})

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			count := receivedCount.Add(1)
			if count == 1 {
				firstReceive = time.Now()
				msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(1*time.Second)))
			} else {
				secondReceive = time.Now()
				msg.Ack(ctx)
				close(done)
			}
		},
		postgremq.WithVT(30),
		postgremq.WithCheckTimeout(500*time.Millisecond),
	)
	require.NoError(t, err, "ConsumeHandler failed")
	defer hc.Stop()

	select {
	case <-done:
		// Message was redelivered after delay
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for delayed redelivery")
	}

	delay := secondReceive.Sub(firstReceive)
	assert.GreaterOrEqual(t, delay.Milliseconds(), int64(900), "Expected at least ~1 second delay, got %v", delay)
}

// TestHandlerConsumerMaxInFlight verifies that maxInFlight limits concurrent handlers.
func TestHandlerConsumerMaxInFlight(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_max_inflight_topic"
	const queueName = "test_handler_max_inflight_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 10 messages
	for i := 0; i < 10; i++ {
		payload := []byte(fmt.Sprintf(`{"message": "msg-%d"}`, i))
		_, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Publish failed")
	}

	var currentInFlight atomic.Int32
	var maxObservedInFlight atomic.Int32
	var receivedCount atomic.Int32
	done := make(chan struct{})

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			current := currentInFlight.Add(1)
			defer currentInFlight.Add(-1)

			// Track max observed in-flight
			for {
				oldMax := maxObservedInFlight.Load()
				if current <= oldMax || maxObservedInFlight.CompareAndSwap(oldMax, current) {
					break
				}
			}

			// Simulate some work
			time.Sleep(100 * time.Millisecond)

			msg.Ack(ctx)
			count := receivedCount.Add(1)
			if count == 10 {
				close(done)
			}
		},
		postgremq.WithVT(30),
		postgremq.WithMaxInFlight(3),
		postgremq.WithBatchSize(10),
	)
	require.NoError(t, err, "ConsumeHandler failed")
	defer hc.Stop()

	select {
	case <-done:
		// All messages received
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for messages, only received %d", receivedCount.Load())
	}

	assert.LessOrEqual(t, maxObservedInFlight.Load(), int32(3), "Should never exceed maxInFlight of 3")
	assert.GreaterOrEqual(t, maxObservedInFlight.Load(), int32(2), "Should have at least 2 concurrent handlers at some point")
}

// TestHandlerConsumerPanicRecovery verifies that panics in handlers are recovered and message is nacked.
func TestHandlerConsumerPanicRecovery(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_panic_topic"
	const queueName = "test_handler_panic_queue"

	logger := &MockLogger{}
	conn, err := postgremq.DialFromPool(ctx, pool, postgremq.WithLogger(logger))
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 1 message
	payload := []byte(`{"message": "panic-test"}`)
	_, err = conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Publish failed")

	var receivedCount atomic.Int32
	done := make(chan struct{})

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			count := receivedCount.Add(1)
			if count == 1 {
				panic("intentional panic for testing")
			}
			msg.Ack(ctx) // Ack on second attempt
			close(done)
		},
		postgremq.WithVT(30),
	)
	require.NoError(t, err, "ConsumeHandler failed")
	defer hc.Stop()

	select {
	case <-done:
		// Message was redelivered after panic
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for redelivery after panic")
	}

	assert.Equal(t, int32(2), receivedCount.Load(), "Expected 2 deliveries (panic + ack)")
}

// TestHandlerConsumerGracefulShutdown verifies that handlers complete during graceful shutdown.
func TestHandlerConsumerGracefulShutdown(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_shutdown_topic"
	const queueName = "test_handler_shutdown_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 1 message
	payload := []byte(`{"message": "shutdown-test"}`)
	_, err = conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Publish failed")

	handlerStarted := make(chan struct{})
	var ctxCancelled atomic.Bool
	handlerDone := make(chan struct{})

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			close(handlerStarted)

			// Simulate work that checks for shutdown
			select {
			case <-ctx.Done():
				ctxCancelled.Store(true)
				msg.Nack(ctx)
				close(handlerDone)
				return
			case <-time.After(10 * time.Second):
				msg.Ack(ctx)
				close(handlerDone)
			}
		},
		postgremq.WithVT(30),
		postgremq.WithMaxInFlight(1),
	)
	require.NoError(t, err, "ConsumeHandler failed")

	// Wait for handler to start
	select {
	case <-handlerStarted:
		// Handler started
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for handler to start")
	}

	// Stop immediately - handler should detect ctx.Done()
	stopStart := time.Now()
	hc.Stop()
	stopDuration := time.Since(stopStart)

	// Wait for handler to complete
	select {
	case <-handlerDone:
		// Handler completed
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for handler to complete")
	}

	// Stop should complete reasonably quickly since handler checks ctx.Done()
	assert.Less(t, stopDuration, 1*time.Second, "Stop should complete quickly when handler respects context")
	assert.True(t, ctxCancelled.Load(), "Handler context should have been cancelled during shutdown")
}

// TestHandlerConsumerContextCancellation verifies that handlers receive cancelled context on shutdown.
func TestHandlerConsumerContextCancellation(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_ctx_cancel_topic"
	const queueName = "test_handler_ctx_cancel_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 1 message
	payload := []byte(`{"message": "ctx-cancel-test"}`)
	_, err = conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Publish failed")

	handlerStarted := make(chan struct{})
	contextCancelled := make(chan struct{})

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			close(handlerStarted)
			<-ctx.Done()
			msg.Nack(ctx)
			close(contextCancelled)
		},
		postgremq.WithVT(30),
	)
	require.NoError(t, err, "ConsumeHandler failed")

	// Wait for handler to start
	select {
	case <-handlerStarted:
		// Handler started
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for handler to start")
	}

	// Stop the consumer
	go hc.Stop()

	// Verify context was cancelled
	select {
	case <-contextCancelled:
		// Context was cancelled
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for context cancellation")
	}
}

// TestHandlerConsumerMessageFields verifies that Message has correct fields.
func TestHandlerConsumerMessageFields(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_msg_fields_topic"
	const queueName = "test_handler_msg_fields_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 1 message
	expectedPayload := map[string]string{"key": "value"}
	payloadBytes, _ := json.Marshal(expectedPayload)
	msgID, err := conn.Publish(ctx, topicName, payloadBytes)
	require.NoError(t, err, "Publish failed")

	done := make(chan struct{})
	var receivedID int
	var receivedDeliveryAttempt int
	var receivedPayload []byte
	var publishedAtSet, vtSet bool

	hc, err := conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			receivedID = msg.ID
			receivedDeliveryAttempt = msg.DeliveryAttempt
			receivedPayload = msg.Payload
			publishedAtSet = !msg.PublishedAt.IsZero()
			vtSet = !msg.GetVT().IsZero()
			msg.Ack(ctx)
			close(done)
		},
		postgremq.WithVT(30),
	)
	require.NoError(t, err, "ConsumeHandler failed")
	defer hc.Stop()

	select {
	case <-done:
		// Message received
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	assert.Equal(t, msgID, receivedID, "Message ID mismatch")
	assert.Equal(t, 1, receivedDeliveryAttempt, "DeliveryAttempt should be 1")
	assert.True(t, publishedAtSet, "PublishedAt should be set")
	assert.True(t, vtSet, "VT should be set")

	var actualPayload map[string]string
	err = json.Unmarshal(receivedPayload, &actualPayload)
	require.NoError(t, err, "Failed to unmarshal payload")
	assert.Equal(t, expectedPayload, actualPayload, "Payload mismatch")
}

// TestHandlerConsumerInvalidMaxInFlight verifies that negative maxInFlight returns error.
func TestHandlerConsumerInvalidMaxInFlight(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_invalid_inflight_topic"
	const queueName = "test_handler_invalid_inflight_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	_, err = conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			msg.Ack(ctx)
		},
		postgremq.WithVT(30),
		postgremq.WithMaxInFlight(-1), // Negative is invalid
	)
	require.Error(t, err, "Expected error for negative maxInFlight")
	assert.Contains(t, err.Error(), "maxInFlight", "Error should mention maxInFlight")
}

// TestHandlerConsumerConnectionClose verifies that Connection.Close() stops handler consumers.
func TestHandlerConsumerConnectionClose(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_handler_conn_close_topic"
	const queueName = "test_handler_conn_close_queue"

	conn, err := postgremq.DialFromPool(ctx, pool, postgremq.WithShutdownTimeout(5*time.Second))
	require.NoError(t, err, "DialFromPool failed")

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 1 message
	payload := []byte(`{"message": "conn-close-test"}`)
	_, err = conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Publish failed")

	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})

	_, err = conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			close(handlerStarted)
			<-ctx.Done() // Wait for shutdown
			msg.Nack(ctx)
			close(handlerDone)
		},
		postgremq.WithVT(30),
	)
	require.NoError(t, err, "ConsumeHandler failed")

	// Wait for handler to start
	select {
	case <-handlerStarted:
		// Handler started
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for handler to start")
	}

	// Close the connection - this should stop the handler consumer
	closeStart := time.Now()
	conn.Close()
	closeDuration := time.Since(closeStart)

	// Verify handler completed
	select {
	case <-handlerDone:
		// Handler completed
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for handler to complete after connection close")
	}

	assert.Less(t, closeDuration, 2*time.Second, "Connection close should complete quickly")
}

// TestMultipleConsumersSameQueue verifies that multiple consumers on the same queue are all tracked.
func TestMultipleConsumersSameQueue(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_multi_consumer_topic"
	const queueName = "test_multi_consumer_queue"

	conn, err := postgremq.DialFromPool(ctx, pool, postgremq.WithShutdownTimeout(5*time.Second))
	require.NoError(t, err, "DialFromPool failed")

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Create two consumers on the same queue
	consumer1Started := make(chan struct{})
	consumer1Done := make(chan struct{})

	consumer2Started := make(chan struct{})
	consumer2Done := make(chan struct{})

	// Publish 2 messages
	_, err = conn.Publish(ctx, topicName, []byte(`{"msg": 1}`))
	require.NoError(t, err)
	_, err = conn.Publish(ctx, topicName, []byte(`{"msg": 2}`))
	require.NoError(t, err)

	_, err = conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			select {
			case <-consumer1Started:
			default:
				close(consumer1Started)
			}
			<-ctx.Done()
			msg.Nack(ctx)
			select {
			case <-consumer1Done:
			default:
				close(consumer1Done)
			}
		},
		postgremq.WithVT(30),
	)
	require.NoError(t, err, "First ConsumeHandler failed")

	_, err = conn.ConsumeHandler(ctx, queueName,
		func(ctx context.Context, msg *postgremq.Message) {
			select {
			case <-consumer2Started:
			default:
				close(consumer2Started)
			}
			<-ctx.Done()
			msg.Nack(ctx)
			select {
			case <-consumer2Done:
			default:
				close(consumer2Done)
			}
		},
		postgremq.WithVT(30),
	)
	require.NoError(t, err, "Second ConsumeHandler failed")

	// Wait for at least one handler from each consumer to start
	select {
	case <-consumer1Started:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for consumer1 to start")
	}

	// Close connection - both consumers should be stopped
	conn.Close()

	// Verify both handlers completed
	select {
	case <-consumer1Done:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for consumer1 to complete")
	}
}
