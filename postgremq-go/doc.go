// Package postgremq_go provides a Go client for PostgreMQ, a PostgreSQL-based message queue system.
//
// # Overview
//
// PostgreMQ implements a reliable message queue using PostgreSQL as the backend. Messages are published
// to topics and distributed to all subscribed queues automatically. Consumers fetch messages with
// visibility timeouts instead of traditional locks, providing reliable message processing with
// automatic failure recovery.
//
// # Core Concepts
//
// Visibility Timeout (VT): When a consumer fetches a message, it becomes invisible to other consumers
// until the visibility timeout expires. This is similar to Amazon SQS's visibility timeout mechanism.
// The client automatically extends visibility timeouts for messages being processed (unless disabled).
//
// Queue Types:
//   - Non-exclusive (persistent): Queues that persist indefinitely
//   - Exclusive (temporary): Queues that expire unless kept alive by periodic extensions
//     (automatically handled by the client)
//
// Dead Letter Queue (DLQ): Messages that exceed their max delivery attempts are automatically moved
// to the DLQ for inspection and potential reprocessing.
//
// # Basic Usage
//
// Create a connection and set up topics and queues:
//
//	import (
//		"context"
//		"encoding/json"
//		"log"
//		postgremq "github.com/slavakl/postgremq/postgremq-go"
//		"github.com/jackc/pgx/v5/pgxpool"
//	)
//
//	func main() {
//		ctx := context.Background()
//		cfg, _ := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/postgremq")
//		conn, err := postgremq.Dial(ctx, cfg)
//		if err != nil {
//			log.Fatal(err)
//		}
//		defer conn.Close()
//
//		// Create topic and queue
//		_ = conn.CreateTopic(ctx, "orders")
//		_ = conn.CreateQueue(ctx, "orders-processor", "orders", false)
//	}
//
// # Publishing Messages
//
// Publish messages with optional delayed delivery:
//
//	payload := json.RawMessage(`{"order_id": 12345, "amount": 99.99}`)
//	messageID, err := conn.Publish(ctx, "orders", payload)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Publish with 5-minute delay
//	delayedID, err := conn.Publish(ctx, "orders", payload,
//		postgremq.WithDeliverAfter(time.Now().Add(5*time.Minute)))
//
// # Consuming Messages
//
// Create a consumer and process messages with automatic acknowledgment:
//
//	consumer, err := conn.Consume(ctx, "orders-processor",
//		postgremq.WithBatchSize(10),
//		postgremq.WithVT(30))
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer consumer.Stop()
//
//	for msg := range consumer.Messages() {
//		// Process the message
//		var order map[string]interface{}
//		if err := json.Unmarshal(msg.Payload, &order); err != nil {
//			log.Printf("Invalid message: %v", err)
//			_ = msg.Nack(ctx) // Return to queue for retry
//			continue
//		}
//
//		// Successful processing
//		if err := processOrder(order); err != nil {
//			log.Printf("Processing failed: %v", err)
//			// Retry after 1 minute
//			_ = msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(time.Minute)))
//		} else {
//			_ = msg.Ack(ctx)
//		}
//	}
//
// # Message Acknowledgment
//
// Messages support three acknowledgment modes:
//
//   - Ack(): Mark message as successfully processed (will not be redelivered)
//   - Nack(): Return message to queue for redelivery (increments delivery attempts)
//   - Release(): Return message to queue without incrementing delivery attempts
//     (use when message was fetched but never attempted to be processed)
//
// Example with transaction:
//
//	func processWithTx(ctx context.Context, conn *postgremq.Connection, msg *postgremq.Message) error {
//		tx, err := conn.pool.Begin(ctx)
//		if err != nil {
//			return err
//		}
//		defer tx.Rollback(ctx)
//
//		// Process message and update database atomically
//		_, err = tx.Exec(ctx, "INSERT INTO orders (id, data) VALUES ($1, $2)",
//			msg.ID, msg.Payload)
//		if err != nil {
//			return err
//		}
//
//		// Acknowledge within same transaction
//		if err := msg.AckWithTx(ctx, tx); err != nil {
//			return err
//		}
//
//		return tx.Commit(ctx)
//	}
//
// # Auto-Extension Behavior
//
// By default, the consumer automatically extends visibility timeouts for in-flight messages
// to prevent them from becoming visible to other consumers while still being processed.
//
// Extension occurs at 50% of the visibility timeout. For example:
//   - VT = 60 seconds: extension happens at 30 seconds
//   - VT = 120 seconds: extension happens at 60 seconds
//
// Messages due for extension within a 20% window are batched together for efficiency.
// For VT=30s, messages due within the next 6 seconds are extended in one batch.
//
// Disable auto-extension if you want manual control:
//
//	consumer, _ := conn.Consume(ctx, "queue-name",
//		postgremq.WithVT(60),
//		postgremq.WithNoAutoExtension())
//
//	for msg := range consumer.Messages() {
//		// Manually extend if needed
//		newVT, _ := msg.SetVT(ctx, 60)
//		// ... process message ...
//	}
//
// # Shutdown Behavior
//
// Connection.Close() performs graceful shutdown:
//  1. Stops the LISTEN/NOTIFY event listener
//  2. Signals all consumers to stop
//  3. Releases buffered messages that haven't been delivered to application code
//     (without incrementing delivery attempts)
//  4. Waits for in-flight messages to be acknowledged, nacked, or released
//     (subject to shutdown timeout if configured)
//  5. Stops keep-alive background loops for exclusive queues
//  6. Closes the database connection pool (if owned by the Connection)
//
// Consumer.Stop() behavior:
//  1. Stops fetching new messages
//  2. Closes the Messages() channel
//  3. Releases buffered messages not yet delivered to the application
//  4. Cancels the StoppedCtx context on all in-flight messages to signal
//     the application to finish processing
//  5. Waits for all in-flight messages to complete (Ack/Nack/Release)
//
// Configure shutdown timeout to prevent indefinite waits:
//
//	conn, _ := postgremq.Dial(ctx, cfg,
//		postgremq.WithShutdownTimeout(30*time.Second))
//
// Example with graceful shutdown:
//
//	func worker(ctx context.Context, conn *postgremq.Connection) {
//		consumer, _ := conn.Consume(ctx, "work-queue", postgremq.WithVT(60))
//		defer consumer.Stop()
//
//		for msg := range consumer.Messages() {
//			select {
//			case <-msg.StoppedCtx.Done():
//				// Consumer is stopping, release message for reprocessing
//				_ = msg.Release(context.Background())
//				return
//			default:
//			}
//
//			// Process with awareness of shutdown signal
//			if err := processWithContext(msg.StoppedCtx, msg.Payload); err != nil {
//				_ = msg.Nack(ctx)
//			} else {
//				_ = msg.Ack(ctx)
//			}
//		}
//	}
//
// # Retry Configuration
//
// The client automatically retries transient database errors (connection failures,
// serialization errors, deadlocks). Configure retry behavior:
//
//	conn, _ := postgremq.Dial(ctx, cfg,
//		postgremq.WithRetryConfig(postgremq.RetryConfig{
//			MaxAttempts:       5,
//			InitialBackoff:    100 * time.Millisecond,
//			MaxBackoff:        2 * time.Second,
//			BackoffMultiplier: 2.0,
//		}))
//
// Or disable retries:
//
//	conn, _ := postgremq.Dial(ctx, cfg, postgremq.WithoutRetries())
//
// Note: Methods with "WithTx" suffix (PublishWithTx, AckWithTx) do not use retry logic
// since transaction boundaries are controlled by the caller.
//
// # Exclusive Queues and Keep-Alive
//
// Exclusive queues are automatically deleted when their keep-alive expires. The client
// automatically maintains keep-alive while the connection is active:
//
//	// Create exclusive queue with 5-minute keep-alive
//	_ = conn.CreateQueue(ctx, "temp-queue", "events", true,
//		postgremq.WithKeepAliveInterval(300))
//
// The client extends keep-alive every 2.5 minutes (half the interval). If the connection
// closes or the application crashes, the queue will be deleted after 5 minutes of inactivity.
//
// # Error Handling
//
// Common errors:
//   - ErrConnectionClosed: Returned when operations are attempted on a closed connection
//   - Database constraint violations: Topic/queue doesn't exist, token mismatch, etc.
//
// Always check errors from Ack/Nack/Release:
//
//	if err := msg.Ack(ctx); err != nil {
//		if errors.Is(err, postgremq.ErrConnectionClosed) {
//			// Connection closed, handle gracefully
//		} else {
//			// Other error (token mismatch, message already processed, etc.)
//			log.Printf("Failed to ack message: %v", err)
//		}
//	}
//
// # Performance Considerations
//
// Batch Size: Larger batches reduce database round-trips but increase memory usage
// and time to first message. Default is 5.
//
//	consumer, _ := conn.Consume(ctx, "queue", postgremq.WithBatchSize(100))
//
// Visibility Timeout: Should be longer than typical processing time. Too short causes
// duplicate processing; too long delays retries on failure.
//
// Check Timeout: How often to poll for messages when no LISTEN/NOTIFY events arrive.
// Default is 10 seconds.
//
//	consumer, _ := conn.Consume(ctx, "queue", postgremq.WithCheckTimeout(5*time.Second))
//
// # Concurrency
//
// Connection methods are safe to call from multiple goroutines. Each Consumer spawns
// internal goroutines for message fetching and auto-extension.
//
// Multiple consumers can consume from the same queue - messages are automatically
// distributed among them using PostgreSQL's SKIP LOCKED mechanism.
package postgremq_go
