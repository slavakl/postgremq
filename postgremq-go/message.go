package postgremq_go

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// MessageStatus values represent the possible states of a message in the queue.
// These values match the status values stored in the queue_messages table.
const (
	MessageStatusPending    = "pending"    // Initial state when message is added to queue
	MessageStatusProcessing = "processing" // Message is being processed by a consumer
	MessageStatusCompleted  = "completed"  // Message has been successfully processed
)

const (
	messageAck = iota
	messageNack
	messageRelease
)

// Message represents a single queue message fetched by a Consumer.
//
// Methods Ack, Nack and Release are idempotent at the client level
// (additional calls will be ignored by the client object) but will return
// an error if the underlying server state no longer matches (e.g., token
// mismatch or message no longer in processing state).
type Message struct {
	// ID is the unique message identifier.
	ID int
	// Payload is the JSON-encoded message payload.
	Payload json.RawMessage
	// PublishedAt is when the message was first published to the topic.
	PublishedAt time.Time
	// DeliveryAttempt is the number of times this message has been consumed
	// (including the current attempt).
	DeliveryAttempt int
	// VT is the visibility timeout expiration timestamp. If the message is not
	// acknowledged by this time, it becomes visible to other consumers.
	VT time.Time

	// StoppedCtx is a context that is cancelled when Consumer.Stop() or
	// Connection.Close() is called. Applications can monitor this context
	// to detect shutdown and release messages early.
	StoppedCtx context.Context

	// internal fields
	queue         string
	consumerToken string

	conn         *Connection
	onComplete   func(*Message)
	completeOnce sync.Once
	cancel       context.CancelFunc
	trackingID   string
}

// Ack acknowledges the message, marking it as successfully processed.
//
// This method updates the database to mark the message as completed, preventing
// further redelivery. It should be called after successful message processing.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns an error if:
//   - The consumer token doesn't match (message was already processed or stolen).
//   - The message is not in 'processing' state.
//   - The database operation fails.
//
// Side effects:
//   - Sets queue_messages.status = 'completed'
//   - Clears the consumer_token field
//   - Records processed_at timestamp
//   - Removes the message from the consumer's internal tracking
//
// This method is idempotent at the client level - calling it multiple times
// on the same Message instance will only execute once.
//
// The operation uses the configured retry policy for transient errors.
func (m *Message) Ack(ctx context.Context) error {
	err := m.conn.ackMessage(ctx, m.queue, m.ID, m.consumerToken)
	m.complete(messageAck, err)
	return err
}

// AckWithTx acknowledges the message within an existing database transaction.
//
// This allows acknowledging the message atomically with other database operations,
// ensuring all-or-nothing semantics. If the transaction is rolled back, the message
// remains in 'processing' state.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - tx: Active database transaction (implements Tx interface).
//
// Returns an error if:
//   - The consumer token doesn't match.
//   - The message is not in 'processing' state.
//   - The database operation fails.
//
// Side effects (only if transaction commits):
//   - Sets queue_messages.status = 'completed'
//   - Clears the consumer_token field
//   - Records processed_at timestamp
//
// This method does NOT use the retry policy since transaction boundaries
// are controlled by the caller.
//
// Example:
//
//	tx, _ := pool.Begin(ctx)
//	defer tx.Rollback(ctx)
//	_, err := tx.Exec(ctx, "INSERT INTO orders (...) VALUES (...)")
//	if err != nil {
//	    return err
//	}
//	if err := msg.AckWithTx(ctx, tx); err != nil {
//	    return err
//	}
//	return tx.Commit(ctx)
func (m *Message) AckWithTx(ctx context.Context, tx Tx) error {
	err := m.conn.ackMessageWithTx(ctx, tx, m.queue, m.ID, m.consumerToken)
	m.complete(messageAck, err)
	return err
}

// Nack negatively acknowledges the message, returning it to the queue for redelivery.
//
// This should be called when message processing fails but the message should be
// retried. The delivery_attempts counter is incremented by the server when the
// message was originally consumed, so calling Nack does not increment it further.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - opts: Optional message options (use WithDelayUntil to delay redelivery).
//
// Returns an error if:
//   - The consumer token doesn't match.
//   - The message is not in 'processing' state.
//   - The database operation fails.
//
// Side effects:
//   - Sets queue_messages.status = 'pending'
//   - Sets vt to the specified delay time (or NOW() if no delay)
//   - Clears the consumer_token field
//   - Emits a NOTIFY event on 'postgremq_events' channel
//   - Removes the message from the consumer's internal tracking
//
// If the message has reached max_delivery_attempts, it will be moved to the
// DLQ when move_messages_to_dlq() is next called.
//
// The operation uses the configured retry policy for transient errors.
//
// Example:
//
//	// Immediate retry
//	err := msg.Nack(ctx)
//
//	// Retry after 5 minutes
//	err := msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(5*time.Minute)))
func (m *Message) Nack(ctx context.Context, opts ...MessageOption) error {
	options := &messageOptions{}
	for _, opt := range opts {
		opt(options)
	}
	var err error
	if options != nil && options.delayUntil != nil {
		err = m.conn.nackMessage(ctx, m.queue, m.ID, m.consumerToken, options.delayUntil)
	} else {
		err = m.conn.nackMessage(ctx, m.queue, m.ID, m.consumerToken, nil)
	}
	m.complete(messageNack, err)
	return err
}

// Release returns the message back to the queue WITHOUT incrementing delivery attempts.
//
// This method should be used when a message was fetched but never actually
// attempted to be processed (e.g., buffered but not consumed by the application,
// or consumer is shutting down). This differs from Nack, which assumes the
// message was attempted but failed.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//
// Returns an error if:
//   - The consumer token doesn't match.
//   - The message is not in 'processing' state.
//   - The database operation fails.
//
// Side effects:
//   - Sets queue_messages.status = 'pending'
//   - Sets vt = NOW() (immediately visible)
//   - Clears the consumer_token field
//   - DECREMENTS delivery_attempts by 1 (to undo the increment from consume)
//   - Emits a NOTIFY event on 'postgremq_events' channel
//   - Removes the message from the consumer's internal tracking
//
// The operation uses the configured retry policy for transient errors.
//
// This method is automatically called by Consumer.Stop() for buffered messages
// that haven't been delivered to the application.
func (m *Message) Release(ctx context.Context) error {
	err := m.conn.releaseMessage(ctx, m.queue, m.ID, m.consumerToken)
	m.complete(messageRelease, err)
	return err
}

// SetVT extends the visibility timeout for this in-flight message.
//
// This allows the consumer to extend the processing time for a message that is
// taking longer than expected to process, preventing it from becoming visible
// to other consumers.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - vt: New visibility timeout in seconds from now.
//
// Returns the new visibility timeout expiration timestamp, or an error if:
//   - The consumer token doesn't match.
//   - The message is not in 'processing' state.
//   - The visibility timeout has already expired.
//   - The database operation fails.
//
// Side effects:
//   - Updates queue_messages.vt to NOW() + vt seconds
//   - Updates the Message.VT field with the new expiration time
//
// The operation uses the configured retry policy for transient errors.
//
// Note: This method is typically not needed when auto-extension is enabled (default).
// The Consumer automatically extends visibility timeouts for in-flight messages
// at 50% of the visibility timeout period.
//
// Example:
//
//	// Manually extend visibility timeout by 60 seconds
//	newVT, err := msg.SetVT(ctx, 60)
//	if err != nil {
//	    log.Printf("Failed to extend VT: %v", err)
//	}
func (m *Message) SetVT(ctx context.Context, vt int) (time.Time, error) {
	if err := m.conn.checkClosed(); err != nil {
		return time.Time{}, err
	}

	var newVT time.Time
	err := m.conn.withRetry(ctx, func(ctx context.Context) error {
		return m.conn.pool.QueryRow(ctx,
			"SELECT set_vt($1, $2, $3, $4)",
			m.queue, m.ID, m.consumerToken, vt).Scan(&newVT)
	})
	if err != nil {
		return time.Time{}, err
	}

	m.VT = newVT
	return newVT, nil
}

func (m *Message) complete(op int, err error) {
	if m.onComplete != nil {
		m.completeOnce.Do(func() {
			m.onComplete(m)
		})
	}
}

// ConsumerToken returns the consumer token associated with this message.
//
// The consumer token is a unique identifier generated when the message is consumed
// (by the consume_message SQL function). It is used to verify ownership when
// acknowledging, nacking, releasing, or extending the message.
//
// The token format is: "{timestamp}-{random}" where timestamp includes microsecond
// precision and the random component is derived from the transaction ID.
//
// Returns the consumer token as a string.
//
// This method is primarily useful for debugging or logging purposes.
func (m *Message) ConsumerToken() string {
    return m.consumerToken
}
