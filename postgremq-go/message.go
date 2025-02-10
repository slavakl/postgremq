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

// Message represents a message from the queue
type Message struct {
	ID              int
	Payload         json.RawMessage
	PublishedAt     time.Time
	DeliveryAttempt int
	VT              time.Time // Changed from LockedUntil

	StoppedCtx context.Context // StoppedCtx is a context that is cancelled when the Consumer.Stop or Connection.Close is called

	// internal fields
	queue         string
	consumerToken string

	conn         *Connection
	onComplete   func(*Message)
	completeOnce sync.Once
	cancel       context.CancelFunc
	trackingID   string
}

// Ack acknowledges the message
func (m *Message) Ack(ctx context.Context) error {
	return m.complete(ctx, messageAck, nil)
}

// Nack negatively acknowledges the message
func (m *Message) Nack(ctx context.Context, opts ...MessageOption) error {
	options := &messageOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return m.complete(ctx, messageNack, options)
}

// Release returns message back to the queue without ack/nack'ing it and increasing the delivery attempts counter
func (m *Message) Release(ctx context.Context) error {
	return m.complete(ctx, messageRelease, nil)
}

// SetVT extends the message visibility timeout
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

func (m *Message) complete(ctx context.Context, op int, options *messageOptions) error {
	var err error
	switch op {
	case messageAck:
		err = m.conn.ackMessage(ctx, m.queue, m.ID, m.consumerToken)
	case messageNack:
		if options != nil && options.delayUntil != nil {
			err = m.conn.nackMessage(ctx, m.queue, m.ID, m.consumerToken, options.delayUntil)
		} else {
			err = m.conn.nackMessage(ctx, m.queue, m.ID, m.consumerToken, nil)
		}
	case messageRelease:
		err = m.conn.releaseMessage(ctx, m.queue, m.ID, m.consumerToken)
	}
	if m.onComplete != nil {
		m.completeOnce.Do(func() {
			m.onComplete(m)
		})
	}
	return err
}

// ConsumerToken returns the consumer token associated with this message
func (m *Message) ConsumerToken() string {
	return m.consumerToken
}
