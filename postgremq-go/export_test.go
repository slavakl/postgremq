package postgremq_go

import "context"

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
