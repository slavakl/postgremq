package postgremq_go

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
