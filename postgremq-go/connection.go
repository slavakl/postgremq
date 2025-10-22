package postgremq_go

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrConnectionClosed is returned when an operation is attempted after
// the Connection has begun shutting down or has been closed.
var (
	ErrConnectionClosed = errors.New("connection is stopped")
)

// Connection is a client handle to the PostgreMQ schema running in a
// PostgreSQL database. It owns a connection Pool (unless created with
// DialFromPool), manages consumers and an event listener (LISTEN/NOTIFY), and
// exposes high‑level queue APIs (publish/consume/ack/nack/etc).
//
// Concurrency:
//   - Connection methods are safe to call from multiple goroutines unless
//     otherwise stated.
//   - Each Consumer created from a Connection runs internal goroutines for
//     fetching messages and (optionally) auto‑extending visibility timeouts.
type Connection struct {
	pool                Pool
	ownPool             bool // true if we created the pool and should close it
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.RWMutex
	shutdownTimeout     time.Duration
	consumers           map[string]*Consumer
	eventListener       *EventListener
	eventListenerDoOnce sync.Once
	logger              LevelLogger
	keepAliveCtx        context.Context
	keepAliveCancel     context.CancelFunc // To stop keep-alive loops
	keepAliveWg         sync.WaitGroup
	retryConfig         RetryConfig
	closedFlag          chan struct{}
}

// Dial creates a new Connection, building an underlying pgxpool.Pool from the
// provided pgx PoolConfig.
//
// The returned Connection owns the pool and will Close() it during shutdown.
func Dial(ctx context.Context, config *pgxpool.Config, opts ...ConnectionOption) (*Connection, error) {
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	return newConnection(ctx, pool, true, opts...)
}

// DialFromPool creates a new Connection using an existing Pool implementation
// (typically *pgxpool.Pool). The Connection does not own the pool and will not
// close it on Connection.Close().
func DialFromPool(ctx context.Context, pool Pool, opts ...ConnectionOption) (*Connection, error) {
	return newConnection(ctx, pool, false, opts...)
}

func newConnection(ctx context.Context, pool Pool, ownPool bool, opts ...ConnectionOption) (*Connection, error) {
	ctx, cancel := context.WithCancel(ctx)
	conn := &Connection{
		pool:                pool,
		ownPool:             ownPool,
		ctx:                 ctx,
		cancel:              cancel,
		consumers:           make(map[string]*Consumer),
		logger:              NoopLogger{},
		keepAliveWg:         sync.WaitGroup{},
		retryConfig:         defaultRetryConfig(),
		closedFlag:          make(chan struct{}),
		eventListenerDoOnce: sync.Once{},
	}
	conn.keepAliveCtx, conn.keepAliveCancel = context.WithCancel(context.Background()) // not using connection context here as we want to keep keep-alive running until consumers are stopped
	for _, opt := range opts {
		opt(conn)
	}
	if err := validateConnectionOptions(conn); err != nil {
		return nil, err
	}
	conn.eventListener = newEventListener(ctx, pool, conn.logger)

	return conn, nil
}

// Close implements graceful shutdown for the Connection and all derived
// Consumers.
//
// Behavior:
//   - Cancels internal context and stops the LISTEN/NOTIFY event listener.
//   - Signals all Consumers to Stop and (optionally) waits up to the configured
//     shutdown timeout for in‑flight messages to finish (Ack/Nack/Release).
//   - Stops keep‑alive background loops for exclusive queues.
//   - Closes the underlying pool if the Connection owns it.
//
// Side effects:
//   - Messages buffered by a Consumer but not yet delivered are released back
//     to the queue without incrementing delivery attempts.
func (c *Connection) Close() error {
	if c.isClosed() {
		return nil
	}
	close(c.closedFlag)
	c.cancel()

	// Stop event listener
	c.eventListener.Close()

	// Stop all consumers and wait for in-flight messages
	var wg sync.WaitGroup
	for _, consumer := range c.consumers {
		wg.Add(1)
		go func(cons *Consumer) {
			defer wg.Done()
			cons.Stop()
		}(consumer)
	}

	// Wait for consumers with timeout if specified
	if c.shutdownTimeout > 0 {
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All consumers finished gracefully
		case <-time.After(c.shutdownTimeout):

			c.logger.Warnf("Excedded timeout for consumers to finish. Will shutdown connection")
		}
	} else {
		wg.Wait()
	}
	// Stop keep-alive loops
	c.keepAliveCancel()
	c.keepAliveWg.Wait() // waiting for all keep-alive loops to finish

	// Close pool if we own it
	if c.ownPool {
		c.pool.Close()
	}
	return nil
}

// CreateTopic creates a topic if it does not already exist (idempotent).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - topic: Name of the topic to create.
//
// Returns an error if the operation fails due to database issues.
// The operation uses the configured retry policy for transient errors.
func (c *Connection) CreateTopic(ctx context.Context, topic string) error {
	if err := c.checkClosed(); err != nil {
		return err
	}
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT create_topic($1)", topic)
		if err != nil {
			return fmt.Errorf("failed to create topic: %w", err)
		}
		return nil
	})
}

// CreateQueue creates a queue subscribed to a topic.
//
// Parameters:
//   - name: queue name.
//   - topic: existing topic name.
//   - exclusive: when true, the queue is temporary and must be kept alive by
//     clients (see WithKeepAliveInterval). When false, the queue is persistent.
//   - opts: queue options (max delivery attempts, keep‑alive interval seconds).
//
// Side effects:
//   - If exclusive, a background keep‑alive loop is started to extend the
//     queue's expiration while the Connection is alive.
func (c *Connection) CreateQueue(ctx context.Context, name, topic string, exclusive bool, opts ...QueueOption) error {
	if err := c.checkClosed(); err != nil {
		return err
	}

	options := defaultQueueOptions()
	for _, opt := range opts {
		opt(&options)
	}

	err := c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx,
			"SELECT create_queue($1, $2, $3, $4, $5)",
			name,                         // p_queue
			topic,                        // p_topic
			options.maxDeliveryAttempts,  // p_max_delivery_attempts
			exclusive,                    // p_exclusive
			options.keepAliveIntervalSec) // p_keep_alive_sec
		if err != nil {
			return fmt.Errorf("failed to create queue: %w", err)
		}
		return nil
	})
	if err == nil && exclusive {
		c.startKeepAlive(name, time.Duration(options.keepAliveIntervalSec)*time.Second)
	}
	return err
}

// Publish publishes a message to a topic and returns its message ID.
//
// The message is immediately inserted into the messages table and distributed
// to all queues subscribed to the topic via the after_message_insert trigger.
// Use WithDeliverAfter option to delay initial visibility.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - topic: Name of the topic (must already exist).
//   - payload: JSON-encoded message payload.
//   - opts: Optional publish options (e.g., WithDeliverAfter for delayed delivery).
//
// Returns the generated message ID on success, or an error if:
//   - The topic does not exist.
//   - The payload is not valid JSON.
//   - Database operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Example:
//
//	payload := json.RawMessage(`{"order_id": 123}`)
//	id, err := conn.Publish(ctx, "orders", payload)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (c *Connection) Publish(ctx context.Context, topic string, payload json.RawMessage, opts ...PublishOption) (int, error) {
	return c.executePublish(ctx, c.pool, topic, payload, true, opts...)
}

// PublishWithTx publishes a message within an existing transaction and returns
// the message ID.
//
// This method allows publishing messages atomically with other database operations
// within the same transaction. It does NOT use the internal retry policy since
// transaction boundaries are controlled by the caller.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - tx: Active database transaction (implements Tx interface).
//   - topic: Name of the topic (must already exist).
//   - payload: JSON-encoded message payload.
//   - opts: Optional publish options (e.g., WithDeliverAfter for delayed delivery).
//
// Returns the generated message ID on success, or an error if the operation fails.
//
// Side effects:
//   - The after_message_insert trigger will run within the transaction,
//     distributing the message to queues only if the transaction commits.
//
// Example:
//
//	tx, _ := pool.Begin(ctx)
//	defer tx.Rollback(ctx)
//	id, err := conn.PublishWithTx(ctx, tx, "orders", payload)
//	if err != nil {
//	    return err
//	}
//	// ... other DB operations ...
//	tx.Commit(ctx)
func (c *Connection) PublishWithTx(ctx context.Context, tx Tx, topic string, payload json.RawMessage, opts ...PublishOption) (int, error) {
	return c.executePublish(ctx, tx, topic, payload, false, opts...)
}

// Consume starts a Consumer for the provided queue name.
//
// Behavior:
//   - Returns immediately with a Consumer whose Messages() channel yields
//     messages as they are fetched.
//   - Auto‑extension: Unless disabled via WithNoAutoExtension, the Consumer
//     automatically extends visibility timeouts for in‑flight messages around
//     halfway through their vt.
//   - Backpressure: Messages are fetched in batches (WithBatchSize) and new
//     fetches are driven by LISTEN/NOTIFY and polling (WithCheckTimeout).
//
// Shutdown:
//   - Consumer.Stop() releases buffered messages that were not delivered to the
//     client yet (no attempt), and waits for in‑flight messages to complete or
//     be released.
func (c *Connection) Consume(ctx context.Context, queue string, opts ...ConsumeOption) (*Consumer, error) {

	c.mu.Lock()
	defer c.mu.Unlock()
	consumer, err := newConsumer(c.ctx, c, c.logger, queue, c.eventListener.AddListener(queue), opts...)
	if err != nil {
		return nil, err
	}
	c.consumers[queue] = consumer
	go consumer.start()

	// starting event listener if not already started
	c.eventListenerDoOnce.Do(func() {
		c.eventListener.Start()
	})

	return consumer, nil
}

// MessageExtension identifies a message to extend in a batch visibility timeout operation.
//
// Used as input to SetVTBatch to specify which messages should have their
// visibility timeouts extended.
type MessageExtension struct {
	// ID is the message ID to extend.
	ID int
	// ConsumerToken is the consumer token that currently owns the message.
	// Must match the token in the database for the extension to succeed.
	ConsumerToken string
}

// MessageLock represents the result of a successful visibility timeout extension.
//
// Returned by SetVTBatch to indicate which messages were successfully extended
// and their new visibility timeout expiration timestamps.
type MessageLock struct {
	// ID is the message ID that was extended.
	ID int
	// VT is the new visibility timeout expiration timestamp.
	VT time.Time
}

// SetVTBatch extends visibility timeout for a batch of in-flight messages.
//
// This method is optimized for extending multiple messages in a single database
// round-trip. It orders updates by message_id to prevent deadlocks when multiple
// consumers extend overlapping sets of messages.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queue: Name of the queue.
//   - locks: Slice of MessageExtension containing message IDs and consumer tokens.
//   - vt: New visibility timeout in seconds.
//
// Returns a slice of MessageLock containing only the successfully extended messages.
// Messages that failed to extend (e.g., already completed, token mismatch, or
// visibility timeout already expired) are omitted from the result.
//
// The operation uses the configured retry policy for transient errors.
//
// This method is primarily used internally by the Consumer's auto-extension
// mechanism but can be called directly for manual batch extension.
func (c *Connection) SetVTBatch(ctx context.Context, queue string, locks []MessageExtension, vt int) ([]MessageLock, error) {
	if err := c.checkClosed(); err != nil {
		return nil, err
	}

	if len(locks) == 0 {
		return nil, nil
	}

	ids := make([]int, len(locks))
	tokens := make([]string, len(locks))
	for i, lock := range locks {
		ids[i] = lock.ID
		tokens[i] = lock.ConsumerToken
	}

	var extendedIDs []MessageLock
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx,
			"SELECT message_id, vt FROM set_vt_batch($1, $2, $3, $4)",
			queue, ids, tokens, vt)
		if err != nil {
			return fmt.Errorf("failed to set message visibility timeout: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var newLock MessageLock
			if err := rows.Scan(&newLock.ID, &newLock.VT); err != nil {
				return err
			}
			extendedIDs = append(extendedIDs, newLock)
		}
		return rows.Err()
	})

	return extendedIDs, err
}

// keep-alive loop for non-durable queues
func (c *Connection) startKeepAlive(queue string, interval time.Duration) {
	c.keepAliveWg.Add(1)
	go func() {

		defer c.keepAliveWg.Done()
		next := time.After(interval / 2)
		for {
			select {
			case <-c.keepAliveCtx.Done():
				return
			case <-next:
				if err := c.sendKeepAlive(c.keepAliveCtx, queue, interval); err != nil {
					c.logger.Errorf("Failed to send keep-alive for queue %s: %v", queue, err)
					next = time.After(1 * time.Second) // retry in a second
				} else {
					next = time.After(interval / 2)
				}
			}
		}
	}()
}

// // Database methods
// executePublish handles the actual execution of publish SQL with the given transaction
func (c *Connection) executePublish(ctx context.Context, tx Tx, topic string, payload json.RawMessage, retry bool, opts ...PublishOption) (int, error) {
	if err := c.checkClosed(); err != nil {
		return 0, err
	}

	options := &publishOptions{}
	for _, opt := range opts {
		opt(options)
	}

	publish := func(ctx context.Context) (messageID int, err error) {
		if options.deliverAfter != nil {
			err = tx.QueryRow(ctx, "SELECT publish_message($1, $2, $3)",
				topic, payload, *options.deliverAfter).Scan(&messageID)
		} else {
			err = tx.QueryRow(ctx, "SELECT publish_message($1, $2)",
				topic, payload).Scan(&messageID)
		}
		return messageID, err
	}
	var messageID int
	var err error
	if retry {
		err = c.withRetry(ctx, func(ctx context.Context) error {
			var err error
			messageID, err = publish(ctx)
			return err
		})
	} else {
		messageID, err = publish(ctx)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to publish message: %w", err)
	}
	return messageID, nil
}

func (c *Connection) consumeMessages(ctx context.Context, queue string, limit int, vt int) ([]*Message, error) {
	if c.isClosed() {
		return nil, ErrConnectionClosed
	}

	var messages []*Message
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx,
			"SELECT message_id, payload, consumer_token, delivery_attempts, vt, published_at FROM consume_message($1, $2, $3)",
			queue, vt, limit)
		if err != nil {
			return fmt.Errorf("failed to consume messages: %w", err)
		}
		defer rows.Close()

		messages = nil // Reset on retry
		for rows.Next() {
			var (
				id               int
				payload          json.RawMessage
				consumerToken    string
				deliveryAttempts int
				vt               time.Time
				publishedAt      time.Time
			)
			if err := rows.Scan(&id, &payload, &consumerToken, &deliveryAttempts, &vt, &publishedAt); err != nil {
				return fmt.Errorf("failed to scan message: %w", err)
			}

			// Create a new Message and ensure all internal fields are initialized.
			msg := &Message{
				ID:              id,
				Payload:         payload,
				consumerToken:   consumerToken,
				DeliveryAttempt: deliveryAttempts,
				PublishedAt:     publishedAt,
				conn:            c,     // Set connection so methods like Ack() will work.
				queue:           queue, // Save the originating queue name.
				VT:              vt,
			}
			messages = append(messages, msg)
		}
		return rows.Err()
	})

	return messages, err
}

func (c *Connection) sendKeepAlive(ctx context.Context, queue string, interval time.Duration) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT extend_queue_keep_alive($1, $2 * interval '1 ms')",
			queue, interval.Milliseconds(),
		)
		return err
	})
}

func (c *Connection) ackMessage(ctx context.Context, queue string, messageID int, consumerToken string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx,
			"SELECT ack_message($1, $2, $3)",
			queue, messageID, consumerToken)
		if err != nil {
			return fmt.Errorf("failed to ack message: %w", err)
		}
		return nil
	})
}

// ackMessageWithTx acknowledges a message within an existing transaction
func (c *Connection) ackMessageWithTx(ctx context.Context, tx Tx, queue string, messageID int, consumerToken string) error {
	if err := c.checkClosed(); err != nil {
		return err
	}

	_, err := tx.Exec(ctx,
		"SELECT ack_message($1, $2, $3)",
		queue, messageID, consumerToken)
	if err != nil {
		return fmt.Errorf("failed to ack message within transaction: %w", err)
	}
	return nil
}

func (c *Connection) releaseMessage(ctx context.Context, queue string, messageID int, consumerToken string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx,
			"SELECT release_message($1, $2, $3)",
			queue, messageID, consumerToken)
		if err != nil {
			return fmt.Errorf("failed to release message: %w", err)
		}
		return nil
	})
}

func (c *Connection) nackMessage(ctx context.Context, queue string, messageID int, consumerToken string, delayUntil *time.Time) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		var err error
		if delayUntil != nil && !delayUntil.IsZero() {
			_, err = c.pool.Exec(ctx,
				"SELECT nack_message($1, $2, $3, $4)",
				queue, messageID, consumerToken, *delayUntil)
		} else {
			_, err = c.pool.Exec(ctx,
				"SELECT nack_message($1, $2, $3)",
				queue, messageID, consumerToken)
		}
		if err != nil {
			return fmt.Errorf("failed to nack message: %w", err)
		}
		return nil
	})
}

// isClosed returns true if the connection is stopped
func (c *Connection) isClosed() bool {
	select {
	case <-c.closedFlag:
		return true
	default:
		return false
	}
}

// checkClosed returns an error if the connection is stopped
func (c *Connection) checkClosed() error {
	if c.isClosed() {
		return ErrConnectionClosed
	}
	return nil
}

// QueueStatistics contains message counts for a queue or aggregate statistics.
//
// Returned by GetQueueStatistics to provide insight into queue depth and
// processing state.
type QueueStatistics struct {
	// PendingCount is the number of messages with status='pending' (ready for consumption).
	PendingCount int64
	// ProcessingCount is the number of messages with status='processing' (currently being processed).
	ProcessingCount int64
	// CompletedCount is the number of messages with status='completed' (successfully processed).
	CompletedCount int64
	// TotalCount is the sum of pending + processing + completed messages.
	TotalCount int64
}

// QueueInfo contains configuration and metadata for a queue.
//
// Returned by ListQueues to show all queues and their settings.
type QueueInfo struct {
	// QueueName is the unique name of the queue.
	QueueName string
	// TopicName is the name of the topic this queue subscribes to.
	TopicName string
	// MaxDeliveryAttempts is the maximum number of delivery attempts before
	// moving a message to the DLQ. 0 means unlimited retries.
	MaxDeliveryAttempts int
	// Exclusive indicates whether this is an exclusive (temporary) queue.
	// Exclusive queues are deleted when their keep-alive expires.
	Exclusive bool
	// KeepAliveUntil is the timestamp when an exclusive queue will expire.
	// Nil for non-exclusive queues.
	KeepAliveUntil *time.Time
}

// DLQMessage represents a message that has been moved to the Dead Letter Queue.
//
// Returned by ListDLQMessages to show messages that exceeded their max
// delivery attempts and can be inspected or requeued.
type DLQMessage struct {
	// QueueName is the name of the original queue.
	QueueName string
	// MessageID is the ID of the failed message.
	MessageID int
	// RetryCount is the number of delivery attempts that were made.
	RetryCount int
	// PublishedAt is when the message was originally published.
	PublishedAt time.Time
}

// MoveToDLQ moves messages from active queues to the Dead Letter Queue (DLQ)
// if they have exceeded their max delivery attempts.
//
// This method scans all queues with max_delivery_attempts > 0 and moves messages
// where delivery_attempts >= max_delivery_attempts to the dead_letter_queue table.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns the number of messages moved to the DLQ, or an error if the operation fails.
//
// This operation is typically called periodically by a background worker or
// scheduler to clean up failed messages. It's safe to call multiple times and
// is idempotent within each call.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) MoveToDLQ(ctx context.Context) (int, error) {
	var movedCount int
	err := c.withRetry(ctx, func(ctx context.Context) error {
		return c.pool.QueryRow(ctx, "SELECT move_messages_to_dlq()").Scan(&movedCount)
	})
	return movedCount, err
}

// ListTopics returns all available topics in the system.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns a slice of topic names sorted alphabetically, or an error if
// the operation fails.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) ListTopics(ctx context.Context) ([]string, error) {
	var topics []string
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx, "SELECT topic FROM list_topics()")
		if err != nil {
			return err
		}
		defer rows.Close()

		topics = make([]string, 0)
		for rows.Next() {
			var topic string
			if err := rows.Scan(&topic); err != nil {
				return err
			}
			topics = append(topics, topic)
		}
		return rows.Err()
	})
	return topics, err
}

// ListQueues returns all available queues with their configuration details.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns a slice of QueueInfo containing queue name, topic name,
// max delivery attempts, exclusive status, and keep-alive expiration (if applicable).
// Queues are sorted alphabetically by name.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	var queues []QueueInfo
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx,
			"SELECT queue_name, topic_name, max_delivery_attempts, exclusive, keep_alive_until FROM list_queues()")
		if err != nil {
			return err
		}
		defer rows.Close()

		queues = make([]QueueInfo, 0)
		for rows.Next() {
			var q QueueInfo
			if err := rows.Scan(&q.QueueName, &q.TopicName, &q.MaxDeliveryAttempts, &q.Exclusive, &q.KeepAliveUntil); err != nil {
				return err
			}
			queues = append(queues, q)
		}
		return rows.Err()
	})
	return queues, err
}

// GetQueueStatistics returns message statistics for a specified queue or all queues.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queueName: Pointer to queue name, or nil to get statistics for all queues combined.
//
// Returns QueueStatistics containing counts of pending, processing, completed,
// and total messages. If queueName is nil, returns aggregate statistics across
// all queues.
//
// The operation uses the configured retry policy for transient errors.
//
// Example:
//
//	// Get stats for specific queue
//	queueName := "orders-queue"
//	stats, _ := conn.GetQueueStatistics(ctx, &queueName)
//
//	// Get stats for all queues
//	allStats, _ := conn.GetQueueStatistics(ctx, nil)
func (c *Connection) GetQueueStatistics(ctx context.Context, queueName *string) (*QueueStatistics, error) {
	var stats QueueStatistics
	err := c.withRetry(ctx, func(ctx context.Context) error {
		return c.pool.QueryRow(ctx,
			"SELECT pending_count, processing_count, completed_count, total_count FROM get_queue_statistics($1)",
			queueName).Scan(&stats.PendingCount, &stats.ProcessingCount, &stats.CompletedCount, &stats.TotalCount)
	})
	return &stats, err
}

// ListDLQMessages returns all messages currently in the Dead Letter Queue (DLQ).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns a slice of DLQMessage containing queue name, message ID, retry count,
// and published timestamp. Messages are sorted by published timestamp.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) ListDLQMessages(ctx context.Context) ([]DLQMessage, error) {
	var messages []DLQMessage
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx,
			"SELECT queue_name, message_id, retry_count, published_at FROM list_dlq_messages()")
		if err != nil {
			return err
		}
		defer rows.Close()

		messages = make([]DLQMessage, 0)
		for rows.Next() {
			var msg DLQMessage
			if err := rows.Scan(&msg.QueueName, &msg.MessageID, &msg.RetryCount, &msg.PublishedAt); err != nil {
				return err
			}
			messages = append(messages, msg)
		}
		return rows.Err()
	})
	return messages, err
}

// RequeueDLQMessages moves messages from the DLQ back to their original queue.
//
// Messages are moved from dead_letter_queue back to queue_messages with:
//   - status set to 'pending'
//   - delivery_attempts reset to 0
//   - vt set to NOW() (immediately visible)
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queueName: Name of the queue to requeue messages for.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Use this when you've fixed the issue causing messages to fail and want to
// retry processing them.
func (c *Connection) RequeueDLQMessages(ctx context.Context, queueName string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT requeue_dlq_messages($1)", queueName)
		return err
	})
}

// PurgeDLQ removes all messages from the Dead Letter Queue (DLQ).
//
// This is a destructive operation that permanently deletes all DLQ entries.
// The underlying message records in the messages table are not deleted.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Use with caution - this operation cannot be undone.
func (c *Connection) PurgeDLQ(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT purge_dlq()")
		return err
	})
}

// PurgeAllMessages removes all messages from the entire system.
//
// This is a destructive operation that deletes:
//   - All entries from dead_letter_queue
//   - All entries from queue_messages
//   - All entries from messages
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Use with extreme caution - this operation cannot be undone. Typically only
// used in testing or emergency cleanup scenarios.
func (c *Connection) PurgeAllMessages(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT purge_all_messages()")
		return err
	})
}

// DeleteTopic deletes a topic from the system.
//
// The topic cannot be deleted if any messages are associated with it. Use
// CleanUpTopic to remove messages first if necessary.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - topic: Name of the topic to delete.
//
// Returns an error if:
//   - Messages exist for the topic (use CleanUpTopic first).
//   - The operation fails due to database issues.
//
// Side effects:
//   - Due to ON DELETE CASCADE, all queues subscribed to this topic will also
//     be deleted, along with their queue_messages entries.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) DeleteTopic(ctx context.Context, topic string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT delete_topic($1)", topic)
		return err
	})
}

// DeleteQueue deletes a queue from the system.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queue: Name of the queue to delete.
//
// Returns an error if the operation fails.
//
// Side effects:
//   - Due to ON DELETE CASCADE, all queue_messages entries for this queue
//     will be automatically deleted.
//   - The underlying messages in the messages table are NOT deleted.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) DeleteQueue(ctx context.Context, queue string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT delete_queue($1)", queue)
		return err
	})
}

// DeleteQueueMessage deletes a specific message from a queue.
//
// This removes the queue_messages entry for the specified message and queue.
// The underlying message in the messages table is NOT deleted.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queue: Name of the queue.
//   - messageID: ID of the message to delete.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) DeleteQueueMessage(ctx context.Context, queue string, messageID int) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT delete_queue_message($1, $2)", queue, messageID)
		return err
	})
}

// CleanUpQueue removes all messages from a queue.
//
// This deletes all queue_messages entries for the specified queue but does NOT
// delete the queue itself or the underlying messages in the messages table.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queue: Name of the queue to clean up.
//
// Returns an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
func (c *Connection) CleanUpQueue(ctx context.Context, queue string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT clean_up_queue($1)", queue)
		return err
	})
}

// CleanUpTopic removes all messages associated with a topic.
//
// This deletes all messages from the messages table for the specified topic.
// Due to ON DELETE CASCADE, this also removes all related queue_messages entries.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - topic: Name of the topic to clean up.
//
// Returns an error if the operation fails.
//
// Side effects:
//   - All messages for this topic are permanently deleted.
//   - All queue_messages entries for these messages are automatically deleted.
//
// The operation uses the configured retry policy for transient errors.
//
// This operation is typically required before calling DeleteTopic.
func (c *Connection) CleanUpTopic(ctx context.Context, topic string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT clean_up_topic($1)", topic)
		return err
	})
}

// DeleteInactiveQueues deletes exclusive queues with expired keep-alive timestamps.
// A queue is considered inactive if it is marked as exclusive and its
// keep_alive_until timestamp is either NULL or has already expired.
// This is useful for automatically cleaning up client-specific queues that are
// no longer being used.
func (c *Connection) DeleteInactiveQueues(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT delete_inactive_queues()")
		return err
	})
}

// CleanupCompletedMessages removes completed messages older than the provided retention window.
//
// This function should be called periodically (e.g., via cron or scheduled task)
// to prevent unbounded growth of the queue_messages table.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - olderThanHours: Pointer to retention period in hours, or nil to use the
//     default (24 hours defined in the SQL function).
//
// Returns the number of completed messages deleted, or an error if the operation fails.
//
// The operation uses the configured retry policy for transient errors.
//
// Example:
//
//	// Delete messages completed more than 7 days ago
//	hours := 168
//	deleted, _ := conn.CleanupCompletedMessages(ctx, &hours)
//
//	// Use default retention (24 hours)
//	deleted, _ := conn.CleanupCompletedMessages(ctx, nil)
func (c *Connection) CleanupCompletedMessages(ctx context.Context, olderThanHours *int) (int, error) {
	var deleted int
	err := c.withRetry(ctx, func(ctx context.Context) error {
		var row pgx.Row
		if olderThanHours != nil {
			row = c.pool.QueryRow(ctx, "SELECT cleanup_completed_messages($1)", *olderThanHours)
		} else {
			row = c.pool.QueryRow(ctx, "SELECT cleanup_completed_messages()")
		}
		return row.Scan(&deleted)
	})
	return deleted, err
}

// QueueMessage represents message metadata without the payload.
//
// Returned by ListMessages for administrative views and monitoring.
// The payload is excluded for efficiency when listing many messages.
type QueueMessage struct {
	// MessageID is the unique message identifier.
	MessageID int
	// Status is the current processing status ('pending', 'processing', or 'completed').
	Status string
	// PublishedAt is when the message was first published to the topic.
	PublishedAt time.Time
	// DeliveryAttempts is the number of times this message has been consumed.
	DeliveryAttempts int
	// VT is the visibility timeout expiration timestamp (when the message
	// becomes visible again if not acknowledged).
	VT time.Time
	// ProcessedAt is when the message was completed (nil if not yet completed).
	ProcessedAt *time.Time
}

// PublishedMessage represents a complete message including its payload.
//
// Returned by GetMessage to retrieve the full message data for inspection.
type PublishedMessage struct {
	// MessageID is the unique message identifier.
	MessageID int
	// TopicName is the name of the topic this message was published to.
	TopicName string
	// Payload is the JSON-encoded message payload.
	Payload []byte
	// PublishedAt is when the message was first published to the topic.
	PublishedAt time.Time
}

// ListMessages lists all messages in a queue without consuming them.
//
// This is a read-only operation that does NOT change message state or acquire locks.
// The returned messages exclude the payload field for efficiency.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - queueName: Name of the queue to list messages from.
//
// Returns a slice of QueueMessage containing message metadata (ID, status,
// published timestamp, delivery attempts, visibility timeout, processed timestamp).
// Messages are sorted by published timestamp.
//
// The operation uses the configured retry policy for transient errors.
//
// Use this for debugging, monitoring, or admin interfaces.
func (c *Connection) ListMessages(ctx context.Context, queueName string) ([]QueueMessage, error) {
	var messages []QueueMessage
	err := c.withRetry(ctx, func(ctx context.Context) error {
		rows, err := c.pool.Query(ctx,
			"SELECT message_id, status, published_at, delivery_attempts, vt, processed_at FROM list_messages($1)",
			queueName)
		if err != nil {
			return err
		}
		defer rows.Close()

		messages = make([]QueueMessage, 0)
		for rows.Next() {
			var msg QueueMessage
			if err := rows.Scan(
				&msg.MessageID,
				&msg.Status,
				&msg.PublishedAt,
				&msg.DeliveryAttempts,
				&msg.VT,
				&msg.ProcessedAt,
			); err != nil {
				return err
			}
			messages = append(messages, msg)
		}
		return rows.Err()
	})
	return messages, err
}

// GetMessage retrieves a single message by ID without consuming it.
//
// This is a read-only operation that returns the complete message including
// its payload. It does NOT change message state.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - messageID: ID of the message to retrieve.
//
// Returns a pointer to PublishedMessage containing the message ID, topic name,
// payload, and published timestamp. Returns nil if the message doesn't exist.
//
// The operation uses the configured retry policy for transient errors.
//
// Use this for debugging or inspecting message payloads.
func (c *Connection) GetMessage(ctx context.Context, messageID int) (*PublishedMessage, error) {
	var msg PublishedMessage
	err := c.withRetry(ctx, func(ctx context.Context) error {
		return c.pool.QueryRow(ctx,
			"SELECT message_id, topic_name, payload, published_at FROM get_message($1)",
			messageID).Scan(&msg.MessageID, &msg.TopicName, &msg.Payload, &msg.PublishedAt)
	})
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &msg, nil
}

func (c *Connection) getNextVisibleTime(ctx context.Context, queue string) (time.Time, error) {
	if err := c.checkClosed(); err != nil {
		return time.Time{}, err
	}

	nextTime := time.Time{}
	err := c.withRetry(ctx, func(ctx context.Context) error {
		var t sql.NullTime
		err := c.pool.QueryRow(ctx,
			"SELECT get_next_visible_time($1)",
			queue).Scan(&t)
		if err != nil {
			return fmt.Errorf("failed to get next visible time: %w", err)
		}
		if t.Valid {
			nextTime = t.Time
		}
		return nil
	})
	return nextTime, err
}
