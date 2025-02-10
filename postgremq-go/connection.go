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

var (
	ErrConnectionClosed = errors.New("connection is stopped")
)

// Connection represents a connection to the PostgreSQL message queue system
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
	logger              Logger
	keepAliveCtx        context.Context
	keepAliveCancel     context.CancelFunc // To stop keep-alive loops
	keepAliveWg         sync.WaitGroup
	retryConfig         RetryConfig
	closedFlag          chan struct{}
}

// Dial creates a new Connection using a pgx PoolConfig
func Dial(ctx context.Context, config *pgxpool.Config, opts ...ConnectionOption) (*Connection, error) {
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	return newConnection(ctx, pool, true, opts...)
}

// DialFromPool creates a new Connection using an existing pool
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

// Close implements graceful shutdown
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

			c.logger.Printf("Excedded timeout for consumers to finish. Will shutdown connection")
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

// CreateTopic creates a new topic
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

// CreateQueue creates a new queue
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

// Publish publishes a message to a topic and returns the message ID
func (c *Connection) Publish(ctx context.Context, topic string, payload json.RawMessage, opts ...PublishOption) (int, error) {
	options := &publishOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var messageID int
	err := c.withRetry(ctx, func(ctx context.Context) error {
		var err error
		if options.deliverAfter != nil {
			err = c.pool.QueryRow(ctx, "SELECT publish_message($1, $2, $3)",
				topic, payload, *options.deliverAfter).Scan(&messageID)
		} else {
			err = c.pool.QueryRow(ctx, "SELECT publish_message($1, $2)",
				topic, payload).Scan(&messageID)
		}
		return err
	})
	return messageID, err
}

func (c *Connection) Consume(ctx context.Context, queue string, opts ...ConsumeOption) (*Consumer, error) {

	c.mu.Lock()
	defer c.mu.Unlock()
	consumer, err := newConsumer(c.ctx, c, queue, c.eventListener.AddListener(queue), opts...)
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

type MessageExtension struct {
	ID            int
	ConsumerToken string
}
type MessageLock struct {
	ID int
	VT time.Time // Changed from LockedUntil
}

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
					c.logger.Printf("Failed to send keep-alive for queue %s: %v", queue, err)
					next = time.After(1 * time.Second) // retry in a second
				} else {
					next = time.After(interval / 2)
				}
			}
		}
	}()
}

// // Database methods
func (c *Connection) consumeMessages(ctx context.Context, queue string, limit int, vt int) ([]*Message, error) {
	if c.isClosed() {
		return nil, ErrConnectionClosed
	}

	rows, err := c.pool.Query(ctx,
		"SELECT message_id, payload, consumer_token, delivery_attempts, vt, published_at FROM consume_message($1, $2, $3)",
		queue, vt, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to consume messages: %w", err)
	}
	defer rows.Close()

	var messages []*Message
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
			return nil, fmt.Errorf("failed to scan message: %w", err)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
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

func (c *Connection) extendLock(ctx context.Context, queue string, messageID int, consumerToken string, extraSeconds int) (time.Time, error) {
	var extendedUntil time.Time
	err := c.withRetry(ctx, func(ctx context.Context) error {
		return c.pool.QueryRow(ctx,
			"SELECT extend_lock_time($1, $2, $3, $4)",
			queue, messageID, consumerToken, extraSeconds).Scan(&extendedUntil)
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to extend message lock: %w", err)
	}
	return extendedUntil, nil
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

// QueueStatistics represents queue statistics
type QueueStatistics struct {
	PendingCount    int64
	ProcessingCount int64
	CompletedCount  int64
	TotalCount      int64
}

// QueueInfo represents queue details
type QueueInfo struct {
	QueueName           string
	TopicName           string
	MaxDeliveryAttempts int
	Exclusive           bool // Changed from Durable
	KeepAliveUntil      *time.Time
}

// DLQMessage represents a message in the dead letter queue
type DLQMessage struct {
	QueueName   string
	MessageID   int
	RetryCount  int
	PublishedAt time.Time
}

// MoveToDLQ moves messages from active queues to DLQ if they exceed max delivery attempts
func (c *Connection) MoveToDLQ(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT move_messages_to_dlq()")
		return err
	})
}

// ListTopics returns all available topics
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

// ListQueues returns all available queues with their details
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

// GetQueueStatistics returns message statistics for a specified queue or all queues
func (c *Connection) GetQueueStatistics(ctx context.Context, queueName *string) (*QueueStatistics, error) {
	var stats QueueStatistics
	err := c.withRetry(ctx, func(ctx context.Context) error {
		return c.pool.QueryRow(ctx,
			"SELECT pending_count, processing_count, completed_count, total_count FROM get_queue_statistics($1)",
			queueName).Scan(&stats.PendingCount, &stats.ProcessingCount, &stats.CompletedCount, &stats.TotalCount)
	})
	return &stats, err
}

// ListDLQMessages returns messages in the DLQ
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

// RequeueDLQMessages moves messages from DLQ back to their original queue
func (c *Connection) RequeueDLQMessages(ctx context.Context, queueName string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT requeue_dlq_messages($1)", queueName)
		return err
	})
}

// PurgeDLQ removes all messages from the DLQ
func (c *Connection) PurgeDLQ(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT purge_dlq()")
		return err
	})
}

// PurgeAllMessages removes all messages from the system
func (c *Connection) PurgeAllMessages(ctx context.Context) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT purge_all_messages()")
		return err
	})
}

// DeleteTopic deletes a topic
func (c *Connection) DeleteTopic(ctx context.Context, topic string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT delete_topic($1)", topic)
		return err
	})
}

// DeleteQueue deletes a queue
func (c *Connection) DeleteQueue(ctx context.Context, queue string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT delete_queue($1)", queue)
		return err
	})
}

// DeleteQueueMessage deletes a specific message from a queue
func (c *Connection) DeleteQueueMessage(ctx context.Context, queue string, messageID int) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT delete_queue_message($1, $2)", queue, messageID)
		return err
	})
}

// CleanUpQueue removes all messages from a queue
func (c *Connection) CleanUpQueue(ctx context.Context, queue string) error {
	return c.withRetry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(ctx, "SELECT clean_up_queue($1)", queue)
		return err
	})
}

// CleanUpTopic removes all messages from a topic
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

// QueueMessage represents a message in a queue without payload
type QueueMessage struct {
	MessageID        int
	Status           string
	PublishedAt      time.Time
	DeliveryAttempts int
	VT               time.Time
	ProcessedAt      *time.Time
}

// Message represents a complete message with payload
type PublishedMessage struct {
	MessageID   int
	TopicName   string
	Payload     []byte
	PublishedAt time.Time
}

// ListMessages lists all messages in a queue without consuming them
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

// GetMessage retrieves a single message by ID without consuming it
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
