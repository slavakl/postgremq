package postgremq_go

import (
	"context"
	"time"
)

// This file is compiled only during `go test`. It exposes a few unexported
// pieces of state to the external `postgremq_go_test` package so internal
// invariants (refcounted LISTEN sharing, etc.) can be asserted without
// leaking those hooks into the public API.

// EventListener returns the connection's event listener. Tests only.
func (c *Connection) EventListener() *EventListener {
	return c.eventListener
}

// SubscriberCount returns the number of subscribers currently registered for
// a given channel (e.g. "pmq:t:topic" or "pmq:q:queue"). Tests only.
func (el *EventListener) SubscriberCount(channel string) int {
	el.mu.Lock()
	defer el.mu.Unlock()
	return el.desired[channel]
}

// StoppedChan returns the current el.stopped channel for identity comparison
// in tests. newEventListener seeds it with an already-closed channel; Start
// replaces it with a fresh open one when it actually launches goroutines.
// Tests rely on this to assert "Start was a no-op" by checking the channel
// reference is unchanged. Tests only.
func (el *EventListener) StoppedChan() <-chan struct{} {
	return el.stopped
}

// ConsumeMessages exposes the unexported consumeMessages so tests can
// drive it directly with a MockPool — useful for asserting retry
// behavior without spinning up a Consumer goroutine. Tests only.
func (c *Connection) ConsumeMessages(ctx context.Context, queue string, limit, vt int) ([]*Message, error) {
	return c.consumeMessages(ctx, queue, limit, vt)
}

// CalculateExtendAt exposes the unexported method so tests can verify
// extension-timing math directly without spinning up a real Consumer
// goroutine + database. Tests only.
func (c *Consumer) CalculateExtendAt(vtUntil time.Time) time.Time {
	return c.calculateExtendAt(vtUntil)
}

// NewConsumerForTest constructs a Consumer with just the fields needed for
// pure-math tests of CalculateExtendAt — does not start any goroutines or
// touch the database. Tests only.
func NewConsumerForTest(extensionThreshold float64) *Consumer {
	return &Consumer{extensionThreshold: extensionThreshold}
}

// ValidateConsumeOptionsForTest exposes the unexported validation function
// to external tests so option misuse can be unit-tested without spinning
// up a real Consumer. Tests only.
func ValidateConsumeOptionsForTest(opts ...ConsumeOption) error {
	options := defaultConsumeOptions()
	for _, o := range opts {
		o(&options)
	}
	return validateConsumeOptions(&options)
}
