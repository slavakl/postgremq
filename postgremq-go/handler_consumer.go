package postgremq_go

import (
	"context"
	"sync"
)

// MessageHandler processes a message. The handler should call msg.Ack() or msg.Nack()
// to acknowledge or reject the message. If the handler returns without calling
// either, the message is automatically acked. If the handler panics, the message
// is automatically nacked.
//
// The context is cancelled when the consumer is stopping - handlers should
// check ctx.Done() and return promptly.
type MessageHandler func(ctx context.Context, msg *Message)

// HandlerConsumer wraps a Consumer and dispatches messages to a handler function
// with concurrency limiting. Messages are automatically acked if the handler
// returns without calling Ack/Nack, and automatically nacked on panic.
type HandlerConsumer struct {
	conn        *Connection
	consumer    *Consumer
	handler     MessageHandler
	maxInFlight int
	logger      LevelLogger

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup // dispatch loop
	handlerWg sync.WaitGroup // active handlers
	sem       chan struct{}  // semaphore for maxInFlight
}

func newHandlerConsumer(
	ctx context.Context,
	conn *Connection,
	consumer *Consumer,
	handler MessageHandler,
	logger LevelLogger,
	options handlerConsumeOptions,
) *HandlerConsumer {
	ctx, cancel := context.WithCancel(ctx)
	hc := &HandlerConsumer{
		conn:        conn,
		consumer:    consumer,
		handler:     handler,
		maxInFlight: options.maxInFlight,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}
	if options.maxInFlight > 0 {
		hc.sem = make(chan struct{}, options.maxInFlight)
	}
	return hc
}

// start begins the underlying consumer and dispatch loop. Called internally after creation.
func (hc *HandlerConsumer) start() {
	hc.consumer.start()
	hc.wg.Add(1)
	go hc.dispatchLoop()
}

// dispatchLoop reads from consumer and dispatches to handlers
func (hc *HandlerConsumer) dispatchLoop() {
	defer hc.wg.Done()

	for {
		// Acquire slot first (if maxInFlight is set)
		if hc.sem != nil {
			select {
			case hc.sem <- struct{}{}:
				// Got a slot
			case <-hc.ctx.Done():
				return
			}
		}

		// Now read a message (we have a slot reserved)
		var msg *Message
		var ok bool
		select {
		case msg, ok = <-hc.consumer.Messages():
			if !ok {
				// Channel closed, release slot and exit
				if hc.sem != nil {
					<-hc.sem
				}
				return
			}
		case <-hc.ctx.Done():
			// Context cancelled, release slot and exit
			if hc.sem != nil {
				<-hc.sem
			}
			return
		}

		hc.handlerWg.Add(1)
		go hc.runHandler(msg)
	}
}

// runHandler executes the handler for a single message
func (hc *HandlerConsumer) runHandler(msg *Message) {
	defer func() {
		if hc.sem != nil {
			<-hc.sem
		}
		hc.handlerWg.Done()
	}()

	// Track if message was completed (acked/nacked) by the handler
	completed := false
	originalOnComplete := msg.onComplete
	msg.onComplete = func(m *Message) {
		completed = true
		if originalOnComplete != nil {
			originalOnComplete(m)
		}
	}

	// Execute handler with panic recovery (use msg.StoppedCtx as handler context)
	panicked := hc.callHandler(msg)

	// If handler panicked, nack the message
	if panicked {
		if err := msg.Nack(context.Background()); err != nil {
			hc.logger.Errorf("failed to nack message %d after panic: %v", msg.ID, err)
		}
		return
	}

	// If handler didn't ack/nack, auto-ack
	if !completed {
		if err := msg.Ack(context.Background()); err != nil {
			hc.logger.Errorf("failed to auto-ack message %d: %v", msg.ID, err)
		}
	}
}

// callHandler calls the handler with panic recovery. Returns true if panicked.
func (hc *HandlerConsumer) callHandler(msg *Message) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			hc.logger.Errorf("handler panic for message %d: %v", msg.ID, r)
			panicked = true
		}
	}()
	hc.handler(msg.StoppedCtx, msg)
	return false
}

// Stop signals the handler consumer to stop and waits for all handlers to complete.
// This method blocks until all in-flight handlers have finished.
func (hc *HandlerConsumer) Stop() {
	// Stop underlying consumer first (non-blocking) to stop new messages arriving
	// and cancel StoppedCtx for in-flight messages (signals handlers to stop)
	consumerStopped := make(chan struct{})
	go func() {
		hc.consumer.Stop()
		close(consumerStopped)
	}()

	// Signal dispatch loop to exit
	hc.cancel()

	// Wait for dispatch loop to finish (it will exit when consumer.Messages() closes)
	hc.wg.Wait()

	// Wait for all in-flight handlers to complete
	hc.handlerWg.Wait()

	// Wait for consumer stop to fully complete
	<-consumerStopped
}
