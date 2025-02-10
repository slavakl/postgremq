package postgremq_go

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type EventListener struct {
	pool        Pool
	ctx         context.Context
	cancel      context.CancelFunc
	startOnce   sync.Once
	logger      Logger
	muListeners sync.RWMutex
	listeners   map[string][]chan time.Time // send to each listener when message will be available in a queue
	stopped     chan struct{}
}

type Event struct {
	Queues []string  `json:"queues"`
	VT     time.Time `json:"vt"`
}

func newEventListener(ctx context.Context, pool Pool, logger Logger) *EventListener {
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
	for {
		select {
		case <-el.ctx.Done():
			break
		default:
			if err := el.doListen(); err != nil {
				// Wait before reconnecting
				time.Sleep(5 * time.Second)
			}
		}
		if el.ctx.Err() != nil {
			break
		}
	}

}

func (el *EventListener) doListen() error {
	conn, err := el.pool.Acquire(el.ctx)
	if err != nil {
		el.logger.Printf("Failed to acquire connection: %v", err)
		return err
	}
	defer conn.Release()

	_, err = conn.Exec(el.ctx, "LISTEN postgremq_events")
	if err != nil {
		el.logger.Printf("Failed to start listening: %v", err)
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
