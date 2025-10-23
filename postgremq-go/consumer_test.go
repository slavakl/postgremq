// Consumer tests validate message fetching, redelivery on Nack/VT expiry,
// fair distribution across consumers, automatic VT extension behavior,
// buffered message release on stop, restart behavior, and options like
// WithNoAutoExtension and retry configuration.
package postgremq_go_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	postgremq "github.com/slavakl/postgremq/postgremq-go"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------
// Consumer Tests
// -----------------------------------------------------------------------

// TestConsumerStartAndReceive verifies that a consumer automatically starts
// and receives published messages.
func TestConsumerStartAndReceive(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_consumer_start_topic"
	const queueName = "test_consumer_start_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 5 messages with properly formatted JSON.
	for i := 0; i < 5; i++ {
		payload := []byte(fmt.Sprintf("{\"message\": \"message-%d\"}", i))
		messageID, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Publish failed")
		assert.Greater(t, messageID, 0, "Expected valid message ID")
	}

	// conn.Consume automatically starts the consumer.
	consumer, err := conn.Consume(ctx, queueName,
		postgremq.WithBatchSize(2),
		postgremq.WithVT(5),
	)
	require.NoError(t, err, "Consume failed")
	defer consumer.Stop()

	received := 0
	timeout := time.After(5 * time.Second)
	for received < 5 {
		select {
		case msg := <-consumer.Messages():
			assert.NotNil(t, msg, "Expected non-nil message")
			err = msg.Ack(ctx)
			require.NoError(t, err, "Failed to ack message")
			received++
		case <-timeout:
			t.Fatalf("Timeout waiting for messages, only received %d", received)
		}
	}
}

// TestConsumerMessageRetryAfterNack verifies that messages are redelivered
// after a negative acknowledgment (Nack).
func TestConsumerMessageRetryAfterNack(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_consumer_nack_topic"
	const queueName = "test_consumer_nack_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	// Use a queue with maximum delivery attempts of 2.
	err = conn.CreateQueue(ctx, queueName, topicName, true, postgremq.WithMaxDeliveryAttempts(2))
	require.NoError(t, err, "CreateQueue failed")

	// Publish a message with properly formatted JSON payload.
	payload := []byte("{\"message\": \"nack-message\"}")
	_, err = conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Publish failed")

	consumer, err := conn.Consume(ctx, queueName)
	require.NoError(t, err, "Consume failed")
	defer consumer.Stop()

	// Wait briefly until a message is received.
	var msg *postgremq.Message
	select {
	case m := <-consumer.Messages():
		msg = m
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for message")
	}

	// Instead of acknowledging, perform a Nack.
	err = msg.Nack(ctx)
	require.NoError(t, err, "Failed to nack message")

	// Ensure that the message is re-delivered with an incremented delivery attempt.
	select {
	case m := <-consumer.Messages():
		assert.Equal(t, msg.ID, m.ID, "Nacked message should be re-delivered")
		assert.Equal(t, 2, m.DeliveryAttempt, "Expected delivery attempt to be 2")
		err = m.Ack(ctx)
		require.NoError(t, err, "Failed to ack message")
	case <-time.After(2 * time.Second):
		t.Error("Timed out waiting for re-delivery")
	}
}

// TestConsumerMultipleDistribution verifies that messages are distributed
// fairly across multiple consumers.
func TestConsumerMultipleDistribution(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_multiple_consumers_topic"
	const queueName = "test_multiple_consumers_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 10 messages.
	for i := 0; i < 10; i++ {
		messageID, err := conn.Publish(ctx, topicName, []byte(fmt.Sprintf("\"msg-%d\"", i)))
		require.NoError(t, err, "Publish failed")
		assert.Greater(t, messageID, 0, "Expected valid message ID")
	}

	consumer1, err := conn.Consume(ctx, queueName, postgremq.WithBatchSize(3))
	require.NoError(t, err, "Consume failed for consumer1")
	defer consumer1.Stop()

	consumer2, err := conn.Consume(ctx, queueName, postgremq.WithBatchSize(3))
	require.NoError(t, err, "Consume failed for consumer2")
	defer consumer2.Stop()

	receivedMsgs := make(map[int]bool)
	var mu sync.Mutex
	wg := sync.WaitGroup{}
	wg.Add(2)

	consumeFunc := func(consumer *postgremq.Consumer) {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-consumer.Messages():
				if !ok {
					return
				}
				mu.Lock()
				receivedMsgs[msg.ID] = true
				receivedCount := len(receivedMsgs)
				mu.Unlock()
				err := msg.Ack(ctx)
				require.NoError(t, err, "Failed to ack message")

				// Once we've received all 10 messages, we can stop consuming
				if receivedCount >= 10 {
					return
				}
			case <-time.After(2 * time.Second):
				return
			}
		}
	}

	go consumeFunc(consumer1)
	go consumeFunc(consumer2)
	wg.Wait()

	mu.Lock()
	count := len(receivedMsgs)
	mu.Unlock()
	assert.Equal(t, 10, count, "Expected 10 unique messages distributed among consumers")
}

// TestConsumerVisibilityTimeoutExtension verifies that visibility timeouts
// for in-process messages are automatically extended.
func TestConsumerVisibilityTimeoutExtension(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_vt_extension_topic"
	const queueName = "test_vt_extension_queue"
	logger := MockLogger{}
	conn, err := postgremq.DialFromPool(ctx, pool, postgremq.WithLogger(&logger))
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "CreateQueue failed")

	// Publish a message with properly formatted JSON payload.
	payload := []byte("{\"message\": \"vt-extension-message\"}")
	_, err = conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Publish failed")

	// Create a consumer with a low VT and a fast auto extension interval.
	consumer, err := conn.Consume(ctx, queueName, postgremq.WithVT(2))
	require.NoError(t, err, "Consume failed")
	defer consumer.Stop()

	msg := <-consumer.Messages()

	// Wait to allow auto-extension to occur.
	// With VT=2 seconds, extension happens at halfway (1 second).
	// Use 1.5 seconds to account for race detector overhead.
	time.Sleep(1500 * time.Millisecond)

	// Check that the visibility timeout has been extended.
	messages, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "ListMessages failed")
	updatedVT := messages[0].VT

	err = msg.Ack(ctx)
	require.NoError(t, err, "Ack failed")

	assert.True(t, updatedVT.After(msg.GetVT()), "Visibility timeout should be extended automatically. Logs: \n\t%s", strings.Join(logger.GetMessages(), "\n\t"))
}

// TestConsumerBufferedMessagesReleased verifies that buffered messages
// are released when a consumer stops.
func TestConsumerBufferedMessagesReleased(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_buffer_release_topic"
	const queueName = "test_buffer_release_queue"
	logger := MockLogger{}
	conn, err := postgremq.DialFromPool(ctx, pool, postgremq.WithLogger(&logger))
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		payload := []byte(fmt.Sprintf("{\"msg\": \"msg-%d\"}", i))
		messageID, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Publish failed")
		require.Greater(t, messageID, 0, "Expected valid message ID")
	}

	// Create a consumer with a BatchSize of 5.
	consumer, err := conn.Consume(ctx, queueName, postgremq.WithBatchSize(5), postgremq.WithVT(10))
	require.NoError(t, err, "Consume failed")

	nackedMessages := make(map[int]struct{})
	// Read only 3 messages from the consumer and nack them.
	for i := 0; i < 3; i++ {
		msg := <-consumer.Messages()
		nackedMessages[msg.ID] = struct{}{}
		require.NotNil(t, msg, "Expected to receive a message")
		err := msg.Nack(ctx)
		require.NoError(t, err, "Nack failed")
	}
	// Stop the consumer; this should release any buffered (unread) messages.
	consumer.Stop()
	t.Log(strings.Join(logger.Messages, "\n"))

	// After the consumer has stopped, list all messages from the queue.
	messages, err := conn.ListMessages(ctx, queueName)
	require.NoError(t, err, "ListMessages failed")
	assert.Equal(t, 5, len(messages), "Expected 5 messages in the queue")

	var countAttempt0, countAttempt1 int
	for _, msg := range messages {
		assert.Equal(t, postgremq.MessageStatusPending, msg.Status, "Message should be pending after consumer stop")
		if _, ok := nackedMessages[msg.MessageID]; ok {
			assert.Equal(t, 1, msg.DeliveryAttempts, "Expected 1 delivery attempt for nacked messages")
		} else {
			assert.Equal(t, 0, msg.DeliveryAttempts, "Expected 0 delivery attempts for buffered messages")
		}
		if msg.DeliveryAttempts == 0 { //nolint:staticcheck // QF1003: if/else is clearer than switch for this case
			countAttempt0++
		} else if msg.DeliveryAttempts == 1 {
			countAttempt1++
		}
	}
	assert.Equal(t, 3, countAttempt1, "3 messages read and nacked should have 1 delivery attempt")
	assert.Equal(t, 2, countAttempt0, "2 messages not read should have 0 delivery attempts")
}

// TestConsumerStopAndRestart verifies that a consumer can be stopped and restarted
// without losing message processing capabilities.
func TestConsumerStopAndRestart(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topicName = "test_consumer_restart_topic"
	const queueName = "test_consumer_restart_queue"

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "DialFromPool failed")
	defer conn.Close()

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "CreateTopic failed")
	err = conn.CreateQueue(ctx, queueName, topicName, true)
	require.NoError(t, err, "CreateQueue failed")

	// Create first consumer before publishing with small batch size
	consumer, err := conn.Consume(ctx, queueName, postgremq.WithBatchSize(1))
	require.NoError(t, err, "First consume failed")

	// Give consumer time to set up LISTEN/NOTIFY and start fetch loop
	time.Sleep(200 * time.Millisecond)

	// Publish first batch of messages
	for i := 0; i < 3; i++ {
		payload := []byte(fmt.Sprintf("{\"batch\": \"first\", \"index\": %d}", i))
		_, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Publishing to first batch failed")
	}

	// Process first batch with longer timeout for race detector
	receivedFirst := 0
	timeout := time.After(10 * time.Second)
	for receivedFirst < 3 {
		select {
		case msg := <-consumer.Messages():
			err := msg.Ack(ctx)
			require.NoError(t, err, "Failed to ack message from first batch")
			receivedFirst++
		case <-timeout:
			t.Fatalf("Timeout waiting for first batch, only received %d/3", receivedFirst)
		}
	}

	// Stop first consumer
	consumer.Stop()

	// Create second consumer before publishing
	consumer2, err := conn.Consume(ctx, queueName, postgremq.WithBatchSize(1))
	require.NoError(t, err, "Second consume failed")
	defer consumer2.Stop()

	// Give consumer time to set up
	time.Sleep(200 * time.Millisecond)

	// Publish second batch
	for i := 0; i < 3; i++ {
		payload := []byte(fmt.Sprintf("{\"batch\": \"second\", \"index\": %d}", i))
		_, err := conn.Publish(ctx, topicName, payload)
		require.NoError(t, err, "Publishing to second batch failed")
	}

	// Process second batch with longer timeout for race detector
	receivedSecond := 0
	timeout = time.After(10 * time.Second)
	for receivedSecond < 3 {
		select {
		case msg := <-consumer2.Messages():
			err := msg.Ack(ctx)
			require.NoError(t, err, "Failed to ack message from second batch")
			receivedSecond++
		case <-timeout:
			t.Fatalf("Timeout waiting for second batch, only received %d/3", receivedSecond)
		}
	}

	assert.Equal(t, 3, receivedFirst, "Should have received all messages from first batch")
	assert.Equal(t, 3, receivedSecond, "Should have received all messages from second batch")
}

// TestConsumerWithRetry tests consumer retry behavior
func TestConsumerWithRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := &MockLogger{}

	// Test success on first attempt
	t.Run("success on first attempt", func(t *testing.T) {
		attempts := 0
		logger.Messages = nil // Reset logger

		pool := &MockPool{
			ExecFunc: func(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
				attempts++
				return pgconn.CommandTag{}, nil
			},
		}

		conn, err := postgremq.DialFromPool(ctx, pool,
			postgremq.WithLogger(logger),
			postgremq.WithRetryConfig(postgremq.RetryConfig{
				MaxAttempts:       3,
				InitialBackoff:    10 * time.Millisecond,
				MaxBackoff:        50 * time.Millisecond,
				BackoffMultiplier: 2.0,
			}))
		require.NoError(t, err)
		defer conn.Close()

		err = conn.CreateTopic(ctx, "test_topic")
		assert.NoError(t, err)
		assert.Equal(t, 1, attempts, "Expected exactly one attempt")
	})
}

// TestWithNoAutoExtension verifies that messages are not automatically extended
// when the WithNoAutoExtension option is used.
func TestWithNoAutoExtension(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	// Create a connection
	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err, "Failed to create connection")
	defer conn.Close()

	// Create topic and queue for no-extension test
	topicName := fmt.Sprintf("test_no_extension_topic_%d", time.Now().UnixNano())
	queueName := fmt.Sprintf("test_no_extension_queue_%d", time.Now().UnixNano())

	err = conn.CreateTopic(ctx, topicName)
	require.NoError(t, err, "Failed to create topic")

	err = conn.CreateQueue(ctx, queueName, topicName, false)
	require.NoError(t, err, "Failed to create queue")

	// Publish a test message
	payload := []byte(`{"test":"no_auto_extension"}`)
	msgID, err := conn.Publish(ctx, topicName, payload)
	require.NoError(t, err, "Failed to publish message")

	// Create a consumer with a very short visibility timeout (3 seconds) and no auto-extension
	shortVT := 3 // 3 seconds visibility timeout
	consumer, err := conn.Consume(ctx, queueName,
		postgremq.WithVT(shortVT),
		postgremq.WithNoAutoExtension(),
		postgremq.WithCheckTimeout(2*time.Second))
	require.NoError(t, err, "Failed to create consumer")
	defer consumer.Stop()

	// Receive the message
	select {
	case receivedMsg := <-consumer.Messages():
		defer func() { _ = receivedMsg.Nack(ctx) }()
		require.Equal(t, msgID, receivedMsg.ID, "Message ID mismatch")
	case <-time.After(1500 * time.Millisecond):
		require.Fail(t, "Timeout waiting for message")
	}

	// Don't ack the message, instead wait for visibility timeout to expire
	// We wait slightly longer than the visibility timeout
	time.Sleep(time.Duration(shortVT+1) * time.Second)

	// Receive the message again
	select {
	case receivedMsg := <-consumer.Messages():
		defer func() { _ = receivedMsg.Nack(ctx) }()
		require.Equal(t, msgID, receivedMsg.ID, "Message ID mismatch")
		require.Equal(t, 2, receivedMsg.DeliveryAttempt, "Expected delivery attempt to be 2")
	case <-time.After(1500 * time.Millisecond):
		require.Fail(t, "Timeout waiting for message redelivery")
	}
}
