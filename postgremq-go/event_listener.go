package postgremq_go

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// EventListener subscribes to PostgreSQL LISTEN/NOTIFY events produced by the
// SQL functions in this schema and fan‑outs the signal to in‑process Consumers
// to wake them up immediately (avoiding pure polling).
//
// This type is internal to the package; it's exported only for tests.
type EventListener struct {
	pool        Pool
	ctx         context.Context
	cancel      context.CancelFunc
	startOnce   sync.Once
	logger      LevelLogger
	muListeners sync.RWMutex
	listeners   map[string][]chan time.Time // send to each listener when message will be available in a queue
	stopped     chan struct{}
}

// Event represents a NOTIFY payload received from PostgreSQL.
//
// The SQL functions emit notifications on the 'postgremq_events' channel
// to wake up consumers when messages become available. This is used
// internally by EventListener.
type Event struct {
	// Queues is the list of queue names that have new messages available.
	Queues []string `json:"queues"`
	// VT is the visibility timeout when the messages will become available.
	VT time.Time `json:"vt"`
}

func newEventListener(ctx context.Context, pool Pool, logger LevelLogger) *EventListener {
	ctx, cancel := context.WithCancel(ctx)
	el := &EventListener{
		pool:      pool,
		ctx:       ctx,
		cancel:    cancel,
		logger:    logger,
		listeners: make(map[string][]chan time.Time),
		stopped:   make(chan struct{}),
	}
	close(el.stopped)
	return el
}

func (el *EventListener) AddListener(queue string) <-chan time.Time {
	ch := make(chan time.Time)
	el.muListeners.Lock()
	el.listeners[queue] = append(el.listeners[queue], ch)
	el.muListeners.Unlock()
	return ch
}

func (el *EventListener) Start() {
	el.startOnce.Do(func() {
		el.stopped = make(chan struct{})
		go el.listen()
	})
}

func (el *EventListener) listen() {
	defer close(el.stopped)
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-el.ctx.Done():
			return
		default:
			if err := el.doListen(); err != nil {
				if el.ctx.Err() != nil {
					return
				}
				el.logger.Warnf("Listener disconnected, reconnecting in %v: %v", backoff, err)
				time.Sleep(backoff)
				// Exponential backoff with max cap
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			} else {
				// Reset backoff on successful connection
				backoff = 1 * time.Second
			}
		}
	}
}

func (el *EventListener) doListen() error {
	conn, err := el.pool.Acquire(el.ctx)
	if err != nil {
		el.logger.Errorf("Failed to acquire connection: %v", err)
		return err
	}
	defer conn.Release()

	_, err = conn.Exec(el.ctx, "LISTEN postgremq_events")
	if err != nil {
		el.logger.Errorf("Failed to start listening: %v", err)
		return err
	}

	for {
		notification, err := conn.Conn().WaitForNotification(el.ctx)
		if err != nil {
			return fmt.Errorf("notification error: %w", err)
		}

		var event Event
		if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
			continue
		}
		for _, queue := range event.Queues {
			el.muListeners.RLock()
			list, ok := el.listeners[queue]
			el.muListeners.RUnlock()
			if !ok {
				continue
			}
			// Deliver the event to all listeners for this event name.
			for _, ch := range list {
				select {
				case ch <- event.VT:
				default:
					// Listener channel is full; drop the event since it will be notified anyway.
				}
			}
		}
	}
}

func (el *EventListener) Close() {
	el.cancel()
	<-el.stopped
	el.muListeners.Lock()
	for _, lst := range el.listeners {
		for _, ch := range lst {
			close(ch)
			for _ = range ch { // drain the channel
			}

		}
	}
	el.listeners = make(map[string][]chan time.Time)
	el.muListeners.Unlock()
}
