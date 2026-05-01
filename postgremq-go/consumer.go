package postgremq_go

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

// Consumer receives messages from a single queue.
//
// A Consumer maintains internal goroutines to fetch messages in batches,
// optionally auto‑extend their visibility timeouts, and track in‑flight
// messages until they are Ack/Nack/Release‑d. Create via Connection.Consume.
type Consumer struct {
	conn             *Connection
	queue            string
	topic            string
	messages         chan *Message
	batchSize        int // number of messages to fetch in one batch
	vtSec            int // visibility timeout in seconds
	noAutoExtension  bool
	checkTimeout     time.Duration // how often to check for new messages, if no events comming
	extendBatchSize  int           // how many messages to extend in one batch
	ctx              context.Context
	cancel           context.CancelFunc
	dbCtx            context.Context
	wg               sync.WaitGroup     // wg is used to wait for all goroutines to finish
	topicHandle      *ListenerHandle    // wakes on per-topic publish notifications
	queueHandle      *ListenerHandle    // wakes on nack/release for this queue
	vtMessageUpdates chan messageUpdate // signal of message updates. used to keep track of vts
	messageUpdates   chan messageUpdate
	inFlightFlag     chan struct{}
	logger           LevelLogger
}

type messageUpdate struct {
	op  messageOp
	msg *Message
}
type messageOp int

const (
	messageAdded messageOp = iota
	messageRemoved
	messageLoopStopped
)

func newConsumer(parentCtx context.Context, conn *Connection, logger LevelLogger, queue string, topicHandle, queueHandle *ListenerHandle, opts ...ConsumeOption) (*Consumer, error) {
	options := defaultConsumeOptions()
	for _, opt := range opts {
		opt(&options)
	}
	return newConsumerFromOptions(parentCtx, conn, logger, queue, topicHandle, queueHandle, options)
}

func newConsumerFromOptions(parentCtx context.Context, conn *Connection, logger LevelLogger, queue string, topicHandle, queueHandle *ListenerHandle, options consumeOptions) (*Consumer, error) {
	// Validate options
	if err := validateConsumeOptions(&options); err != nil {
		return nil, err
	}
	if options.vt == 0 {
		options.vt = 30 // if client didn't specify visibility timeout, we default to 30 seconds
	}
	ctx, cancel := context.WithCancel(parentCtx)
	dbCtx := context.Background() // some operations should not be cancelled by consumer stop
	return &Consumer{
		conn:             conn,
		queue:            queue,
		topic:            options.topic,
		messages:         make(chan *Message, options.batchSize),
		batchSize:        options.batchSize,
		vtSec:            options.vt,
		noAutoExtension:  options.noAutoExtension,
		checkTimeout:     options.checkTimeout,
		extendBatchSize:  options.extendBatchSize,
		ctx:              ctx,
		cancel:           cancel,
		dbCtx:            dbCtx,
		topicHandle:      topicHandle,
		queueHandle:      queueHandle,
		vtMessageUpdates: make(chan messageUpdate, options.batchSize*2),
		messageUpdates:   make(chan messageUpdate, 100), // Buffered to avoid blocking during shutdown
		inFlightFlag:     make(chan struct{}),
		logger:           logger,
	}, nil
}

func (c *Consumer) start() {
	if !c.noAutoExtension {
		c.startExtendLoop()
	}
	c.startMessageTrackingLoop()
	c.startMessageLoop()
}

func minTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if a.After(b) {
		return b
	}
	return a
}

func (c *Consumer) startMessageLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		// Fetch messages immediately instead of waiting for first tick
		fetchAfter := c.fetchMessages()
		topicCh := c.topicHandle.Wake()
		queueCh := c.queueHandle.Wake()
		for {
			fetch := false

			// if we don't have a signal to fetch immediately, we fetch after timeout
			fetchAfter = minTime(fetchAfter, time.Now().Add(c.checkTimeout))
			select {
			case <-c.ctx.Done():
				break //nolint:staticcheck // SA4011: Break exits select, the post-select check exits the loop
			case _, ok := <-topicCh:
				if !ok {
					topicCh = nil // listener closed; rely on queue/timer wake
					continue
				}
				fetch = true
			case _, ok := <-queueCh:
				if !ok {
					queueCh = nil
					continue
				}
				fetch = true
			case <-time.After(time.Until(fetchAfter)):
				fetch = true
			}

			if c.ctx.Err() == nil && fetch {
				fetchAfter = c.fetchMessages()
			}
			if c.ctx.Err() != nil {
				break
			}
		}

		close(c.messages) // Close channel when done

		// release all buffered messages that client hasn't consumed yet
		for msg := range c.messages {
			if err := msg.Release(context.Background()); err != nil { // release message if client is not ready to receive it
				c.logger.Errorf("Failed to release message %d: %v", msg.ID, err)
			}
		}

		// signal message tracking loop to cancel messages
		c.messageUpdates <- messageUpdate{op: messageLoopStopped}

		// wait for all in-flight messages to be completed
		<-c.inFlightFlag

		// Close channels to signal auto-extend and tracking loops to exit
		// It's safe to close now because inFlightFlag is closed, meaning no more
		// messages will call handleMessageComplete
		close(c.messageUpdates)
		close(c.vtMessageUpdates)
	}()
}

// fetchMessages fetches messages from the queue. Returns stopped channel if the outer loop should run another fetch immediately
// may also return a channel with timeout if consumeMessage failed
// will return nil in all other cases which can be interpreted as "fetch when you see a need"
func (c *Consumer) fetchMessages() time.Time {
	if c.ctx.Err() != nil {
		return time.Time{}
	}
	msgs, err := c.conn.consumeMessages(c.dbCtx, c.queue, c.batchSize, c.vtSec)
	if err != nil {
		c.logger.Errorf("Failed to consume messages: %v", err)
	}
	c.logger.Debugf("Consumer - fetched %d messages (err=%v)", len(msgs), err)

	// Process whatever messages we got — even on a partial-batch error.
	// Successfully-scanned rows are already claimed server-side
	// (status='processing', vt set, consumer_token assigned); dropping
	// them here would just make us wait for vt expiry to see them again.
	// The error path below still schedules a retry for the next fetch.
	for _, msg := range msgs {
		// Create a new context for each message. Use Background context as parent so that clients can't read any values from the parent context.
		msg.StoppedCtx, msg.cancel = context.WithCancel(context.Background())
		msg.onComplete = c.handleMessageComplete
		msg.trackingID = fmt.Sprintf("%d--%s", msg.ID, msg.consumerToken)

		c.messageUpdates <- messageUpdate{op: messageAdded, msg: msg}
		if !c.noAutoExtension {
			c.vtMessageUpdates <- messageUpdate{op: messageAdded, msg: msg}
		}
	}

	for _, msg := range msgs {
		if c.ctx.Err() != nil { // if Consumer is stopped before this message is delivered to a client, release it immediately
			if err := msg.Release(c.dbCtx); err != nil { // release message if client is not ready to receive it
				c.logger.Errorf("Failed to release message %d: %v", msg.ID, err)
			}
			continue
		}
		select {
		case c.messages <- msg: // deliver message to client
		case <-c.ctx.Done():
			// Release the current message AND fall through to the next
			// iteration so all remaining msgs in this batch are
			// released. Returning here would leave them stranded —
			// they were already added to tracking via messageAdded but
			// never delivered or released, so handleMessageComplete
			// would never fire for them and Stop() would deadlock
			// waiting for inFlight to drain.
			if err := msg.Release(c.dbCtx); err != nil {
				c.logger.Errorf("Failed to release message %d: %v", msg.ID, err)
			}
			continue
		}
	}
	if err != nil {
		return time.Now().Add(1 * time.Second) // retry after 1 second
	}
	if len(msgs) == c.batchSize && c.ctx.Err() == nil {
		return time.Now() // we consumed max available messages, try to fetch more immediately
	}
	if len(msgs) == 0 {
		nextVisibleTime, err := c.conn.getNextVisibleTime(c.dbCtx, c.queue)
		if err != nil {
			c.logger.Errorf("Failed to get next visible time: %v", err)
			return time.Now().Add(1 * time.Second) // retry after 1 second
		}
		c.logger.Debugf("Consumer - next available message in %d ms", time.Until(nextVisibleTime).Milliseconds())
		return nextVisibleTime
	}
	return time.Time{}
}

func (c *Consumer) handleMessageComplete(m *Message) {
	// vtMessageUpdates first, messageUpdates second. The tracking loop
	// uses messageUpdates to decide it's drained (decrement inFlight, then
	// close inFlightFlag once cancelled+empty). Once inFlightFlag closes,
	// the main loop closes both update channels — including
	// vtMessageUpdates. If we sent to messageUpdates first, that send
	// could race with the close: the tracking loop receives, drops
	// inFlight to 0, signals shutdown, channels close, and the still-in-
	// progress send to vtMessageUpdates panics. Sending to
	// vtMessageUpdates before messageUpdates makes the second send the
	// gate, so the close can only happen after both writes have completed.
	if !c.noAutoExtension {
		c.vtMessageUpdates <- messageUpdate{op: messageRemoved, msg: m}
	}
	c.messageUpdates <- messageUpdate{op: messageRemoved, msg: m}
}

func (c *Consumer) startMessageTrackingLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		inFlight := make(map[string]*Message)
		cancelled := false
		for {
			update, ok := <-c.messageUpdates
			if !ok {
				// Channel closed, clean up and exit
				for _, msg := range inFlight {
					msg.cancel()
				}
				close(c.inFlightFlag)
				return
			}
			switch update.op {
			case messageAdded:
				c.logger.Debugf("Message tracking - received message added: %s", update.msg.trackingID)
				inFlight[update.msg.trackingID] = update.msg
			case messageRemoved:
				c.logger.Debugf("Message tracking - Received message removed: %s", update.msg.trackingID)
				delete(inFlight, update.msg.trackingID)
			case messageLoopStopped:
				c.logger.Debugf("Message tracking - Received message loop stopped")
				for _, msg := range inFlight { // cancelling context of all in-flight messages letting client now that they should stop processing
					msg.cancel()
				}
				cancelled = true

			}
			if cancelled && len(inFlight) == 0 {
				break
			}

		}
		close(c.inFlightFlag) // letting main loop know that we're done
	}()
}

// auto-extend implementation
type vtInfo struct {
	messageID     int64
	consumerToken string
	extendAt      time.Time
	// cancel is called by extendVTs when set_vt_batch returns the row
	// missing from its result set — the lease has been lost server-side.
	// Cancelling the message's StoppedCtx lets the handler stop work that
	// will never be ackable, instead of running to completion and
	// committing non-idempotent side-effects before the eventual PMQ01 on
	// ack. We hold only the CancelFunc — not the whole *Message — to keep
	// the dependency narrow.
	cancel context.CancelFunc
}

// calculateExtendAt calculates when to extend the message's visibility timeout.
//
// We extend at the halfway point (50%) to ensure the message doesn't become
// visible to other consumers while still being processed. This provides a
// safety margin for network latency and extension execution time.
//
// For example, with VT=60s:
//   - Message locked until: 12:00:60
//   - Extension triggered at: 12:00:30 (halfway point)
//   - After extension: locked until 12:01:30
//
// Returns the timestamp when extension should occur, or now if VT has already expired.
func calculateExtendAt(vtUntil time.Time) time.Time {
	remaining := time.Until(vtUntil)
	if remaining < 0 {
		return time.Now()
	}
	return time.Now().Add(remaining / 2)
}

func (c *Consumer) startExtendLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// Drain vtMessageUpdates on EVERY exit path. handleMessageComplete
		// (driven by ack/nack/release) sends here first; if no one drains,
		// those sends block on the buffer limit, the message-tracking loop
		// never decrements inFlight, inFlightFlag never closes, and Stop()
		// deadlocks. startMessageLoop closes the channel once tracking has
		// drained, at which point this `for range` returns.
		defer func() {
			for range c.vtMessageUpdates {
			}
		}()
		var nextExtension <-chan time.Time
		vts := newVTHeap()

		var tryAfter time.Time
		updateTimer := func() {
			head := vts.peek()
			var wait time.Duration
			if !tryAfter.IsZero() {
				wait = time.Until(tryAfter)
			} else if head != nil {
				wait = time.Until(head.extendAt)
			} else {
				nextExtension = nil
				return
			}
			if wait < 0 {
				wait = 0
			}
			nextExtension = time.After(wait)
		}
		for {
			// First phase: check if we need to do an extension immediately
			if nextExtension != nil {
				select {
				case <-c.ctx.Done():
					return
				case <-nextExtension:
					tryAfter = c.extendVTs(vts)
					updateTimer()
					continue
				default:
				}
			}

			// Second phase: handle updates or wait for extension
			select {
			case <-c.ctx.Done():
				return
			case update, ok := <-c.vtMessageUpdates:
				if !ok {
					// Channel closed by startMessageLoop after tracking
					// drained — graceful shutdown.
					return
				}
				switch update.op {
				case messageAdded:
					vts.push(&vtInfo{
						messageID:     update.msg.ID,
						consumerToken: update.msg.consumerToken,
						extendAt:      calculateExtendAt(update.msg.GetVT()),
						cancel:        update.msg.cancel,
					})
				case messageRemoved:
					vts.remove(update.msg.ID)
				}
				updateTimer()
			case <-nextExtension:
				tryAfter = c.extendVTs(vts)
				updateTimer()
			}
		}
	}()
}

const (
	// ExtendWindowPercent defines what percentage of the VT period to use as the extension window
	// Messages due within this window will be batched together for efficiency
	ExtendWindowPercent = 20 // 20% of VT period
)

// extendVTs extends one batch of messages and returns to the main loop,
// which may call this function again if more messages need extending.
//
// To improve efficiency, we batch messages that are due soon rather than
// extending one at a time. The extension window is 20% of the VT period.
// For example, with VT=30s, messages due within the next 6s will be batched.
func (c *Consumer) extendVTs(vts *vtHeap) (tryAfter time.Time) {
	// Calculate the extension deadline window: current time + 20% of VT period
	extendDeadline := time.Now().Add(time.Duration(c.vtSec*ExtendWindowPercent*10) * time.Millisecond)
	extendingMap := make(map[int64]*vtInfo)
	var extending []MessageExtension

	// get the batch of messages to extend
	head := vts.peek()
	for head != nil && head.extendAt.Before(extendDeadline) && len(extending) < c.extendBatchSize {
		head = vts.pop()
		extendingMap[head.messageID] = head
		extending = append(extending, MessageExtension{
			ID:            head.messageID,
			ConsumerToken: head.consumerToken,
		})
		head = vts.peek()
	}
	extendedVTs, err := c.conn.SetVTBatch(c.dbCtx, c.queue, extending, int(c.vtSec))
	if err != nil {
		// Connection.SetVTBatch already retried transient errors via
		// withRetry; an error here means a persistent problem (DB down,
		// validation failure, etc.). Re-push the popped items so the
		// next cycle tries again — extendAt is a soft deadline (halfway
		// point), so we still have time before the server-side lease
		// actually expires. tryAfter delays the next call by 1s to avoid
		// tight-looping. If by the next attempt the lease has actually
		// expired, set_vt_batch will return the row missing and we
		// cancel cleanly on that path.
		c.logger.Errorf("Failed to extend message vts batch: %v; will retry next cycle", err)
		for _, info := range extendingMap {
			vts.push(info)
		}
		tryAfter = time.Now().Add(1 * time.Second)
		return
	}

	// Track which IDs got extended; the rest are lease-lost server-side.
	extended := make(map[int64]bool, len(extendedVTs))
	for _, vt := range extendedVTs {
		extended[vt.ID] = true
		oldVT, ok := extendingMap[vt.ID]
		if !ok {
			c.logger.Warnf("Extending message returned some unknown message : %d", vt.ID)
			continue
		}
		oldVT.extendAt = calculateExtendAt(vt.VT)
		vts.push(oldVT)
	}

	// Items in the request but missing from the response are no longer
	// ours: set_vt_batch's WHERE filters by status='processing', vt > NOW(),
	// and consumer_token; any mismatch silently omits the row. From our
	// perspective these are all "lease lost". Cancel each handler's
	// StoppedCtx so it can stop work — the eventual ack will fail with
	// PMQ01 anyway, and silently letting the handler run to completion
	// risks committing non-idempotent side-effects on a row that's now
	// owned by another consumer (or already in DLQ).
	for id, info := range extendingMap {
		if extended[id] {
			continue
		}
		c.logger.Warnf("Lease lost for message %d during extension; signalling handler", id)
		if info.cancel != nil {
			info.cancel()
		}
	}
	return
}

// Messages returns a receive-only channel that yields messages as they are fetched.
//
// The channel is created with a buffer size equal to the consumer's batch size
// (configured via WithBatchSize). Messages are fetched in batches and pushed
// to this channel for consumption.
//
// Returns a receive-only channel of *Message pointers.
//
// Typical usage pattern:
//
//	for msg := range consumer.Messages() {
//	    // Process the message
//	    err := processMessage(msg.Payload)
//	    if err != nil {
//	        msg.Nack(ctx)
//	    } else {
//	        msg.Ack(ctx)
//	    }
//	}
//
// Behavior:
//   - The channel is closed when Consumer.Stop() is called or the Connection closes.
//   - Messages buffered in the channel but not yet delivered to the application
//     are automatically released back to the queue during shutdown WITHOUT
//     incrementing their delivery_attempts counter.
//   - If auto-extension is enabled (default), messages are automatically extended
//     to prevent visibility timeout expiration while being processed.
//
// The channel will block when the buffer is full, providing natural backpressure.
func (c *Consumer) Messages() <-chan *Message {
	return c.messages
}

// Stop gracefully stops the consumer and waits for all operations to complete.
//
// This method performs the following shutdown sequence:
//
//  1. Cancels the internal context to signal all goroutines to stop.
//  2. Stops fetching new messages from the queue.
//  3. Closes the Messages() channel.
//  4. Releases all buffered messages (not yet delivered to application) back
//     to the queue WITHOUT incrementing their delivery_attempts.
//  5. Cancels the StoppedCtx context on all in-flight messages to signal the
//     application that shutdown is in progress.
//  6. Waits for all in-flight messages to be completed (Ack/Nack/Release).
//  7. Stops the auto-extension loop if running.
//  8. Unregisters the consumer from the Connection.
//
// This method is safe to call multiple times - subsequent calls are no-ops.
//
// The method blocks until all goroutines have terminated and all in-flight
// messages have been processed. Applications should ensure they complete
// message processing promptly or check msg.StoppedCtx.Done() to detect
// shutdown and release messages early.
//
// Example:
//
//	consumer, _ := conn.Consume(ctx, "queue-name")
//	defer consumer.Stop()
//
//	for msg := range consumer.Messages() {
//	    select {
//	    case <-msg.StoppedCtx.Done():
//	        // Consumer is stopping, release message quickly
//	        msg.Release(context.Background())
//	        return
//	    default:
//	    }
//	    // Normal processing...
//	}
func (c *Consumer) Stop() {
	c.cancel()
	c.wg.Wait()
	if c.topicHandle != nil {
		c.topicHandle.Close()
	}
	if c.queueHandle != nil {
		c.queueHandle.Close()
	}
}

// vtHeap implementation

type vtHeap struct {
	items     []*vtInfo
	itemIndex map[int64]int // maps messageID to index in items slice
}

func newVTHeap() *vtHeap {
	return &vtHeap{
		items:     make([]*vtInfo, 0),
		itemIndex: make(map[int64]int),
	}
}

// implement heap.Interface
func (h *vtHeap) Len() int           { return len(h.items) }
func (h *vtHeap) Less(i, j int) bool { return h.items[i].extendAt.Before(h.items[j].extendAt) }
func (h *vtHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.itemIndex[h.items[i].messageID] = i
	h.itemIndex[h.items[j].messageID] = j
}

func (h *vtHeap) Push(x interface{}) {
	item := x.(*vtInfo)
	h.itemIndex[item.messageID] = len(h.items)
	h.items = append(h.items, item)
}

func (h *vtHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	delete(h.itemIndex, item.messageID)
	return item
}

// helper methods
func (h *vtHeap) push(item *vtInfo) {
	heap.Push(h, item)
}

func (h *vtHeap) pop() *vtInfo {
	return heap.Pop(h).(*vtInfo)
}

func (h *vtHeap) peek() *vtInfo {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

func (h *vtHeap) remove(messageID int64) *vtInfo {
	if idx, ok := h.itemIndex[messageID]; ok {
		return heap.Remove(h, idx).(*vtInfo)
	}
	return nil
}
