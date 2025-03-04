package postgremq_go

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

type Consumer struct {
	conn             *Connection
	queue            string
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
	events           <-chan time.Time   // events is used to signal that new messages are available
	vtMessageUpdates chan messageUpdate // signal of message updates. used to keep track of vts
	messageUpdates   chan messageUpdate
	inFlightFlag     chan struct{}
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

func newConsumer(parentCtx context.Context, conn *Connection, queue string, events <-chan time.Time, opts ...ConsumeOption) (*Consumer, error) {
	options := defaultConsumeOptions()
	for _, opt := range opts {
		opt(&options)
	}
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
		messages:         make(chan *Message, options.batchSize),
		batchSize:        options.batchSize,
		vtSec:            options.vt,
		noAutoExtension:  options.noAutoExtension,
		checkTimeout:     options.checkTimeout,
		extendBatchSize:  options.extendBatchSize,
		ctx:              ctx,
		cancel:           cancel,
		dbCtx:            dbCtx,
		events:           events,
		vtMessageUpdates: make(chan messageUpdate, options.batchSize*2),
		messageUpdates:   make(chan messageUpdate),
		inFlightFlag:     make(chan struct{}),
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
		eventsCh := c.events
		for {
			fetch := false

			// if we don't have a signal to fetch immediately, we fetch after timeout
			fetchAfter = minTime(fetchAfter, time.Now().Add(c.checkTimeout))
			select {
			case <-c.ctx.Done():
				break
			case _, ok := <-eventsCh:
				if !ok {
					eventsCh = nil // if listener stopped we disable this branch. It means we're shutting down anyway
					continue
				}
				fetch = true
			case <-time.After(fetchAfter.Sub(time.Now())):
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

		var closingWg sync.WaitGroup
		closingWg.Add(1)
		go func() {
			defer closingWg.Done()
			// release all buffered messages that client hasn't consumed yet
			for msg := range c.messages {
				if err := msg.Release(context.Background()); err != nil { // release message if client is not ready to receive it
					c.conn.logger.Printf("Failed to release message %d: %v", msg.ID, err)
				}
			}
		}()
		closingWg.Add(1)
		go func() {
			defer closingWg.Done()
			c.messageUpdates <- messageUpdate{op: messageLoopStopped} // signal message tracking loop to cancel messages
			<-c.inFlightFlag                                          // waiting for all the messages to be completed
		}()
		closingWg.Wait()
		// let the auto-extend loop know that no more updates are coming
		close(c.vtMessageUpdates)
		close(c.messageUpdates)
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
		c.conn.logger.Printf("Failed to consume messages: %v", err)
		return time.Now().Add(1 * time.Second) // retry after 1 second
	}

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
				c.conn.logger.Printf("Failed to release message %d: %v", msg.ID, err)
			}
			continue
		}
		select {
		case c.messages <- msg: // deliver message to client
		case <-c.ctx.Done():
			if err := msg.Release(c.dbCtx); err != nil { // release message if client is not ready to receive it
				c.conn.logger.Printf("Failed to release message %d: %v", msg.ID, err)
			}
			return time.Time{}
		}
	}
	if len(msgs) == c.batchSize && c.ctx.Err() == nil {
		return time.Now() // we consumed max available messages, try to fetch more immediately
	}
	if len(msgs) == 0 {
		nextVisibleTime, err := c.conn.getNextVisibleTime(c.dbCtx, c.queue)
		if err != nil {
			c.conn.logger.Printf("Failed to get next visible time: %v", err)
			return time.Now().Add(1 * time.Second) // retry after 1 second
		}
		return nextVisibleTime
	}
	return time.Time{}
}

func (c *Consumer) handleMessageComplete(m *Message) {
	c.messageUpdates <- messageUpdate{op: messageRemoved, msg: m}
	if !c.noAutoExtension {
		c.vtMessageUpdates <- messageUpdate{op: messageRemoved, msg: m}
	}
}

func (c *Consumer) startMessageTrackingLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		inFlight := make(map[string]*Message)
		cancelled := false
		for {
			select {
			case update := <-c.messageUpdates:
				switch update.op {
				case messageAdded:
					inFlight[update.msg.trackingID] = update.msg
				case messageRemoved:
					delete(inFlight, update.msg.trackingID)
				case messageLoopStopped:
					for _, msg := range inFlight { // cancelling context of all in-flight messages letting client now that they should stop processing
						msg.cancel()
					}
					cancelled = true
				}
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
	messageID     int
	consumerToken string
	extendAt      time.Time
}

// it takes the difference between the current time and expiration time and adds half of it to the current time
func calculateExtendAt(vtUntil time.Time) time.Time {
	remaining := vtUntil.Sub(time.Now())
	if remaining < 0 {
		return time.Now()
	}
	return time.Now().Add(remaining / 2)
}

func (c *Consumer) startExtendLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
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
			// First phase: check if we need to do an extension
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
				break
			case update := <-c.vtMessageUpdates:
				switch update.op {
				case messageAdded:
					vts.push(&vtInfo{
						messageID:     update.msg.ID,
						consumerToken: update.msg.consumerToken,
						extendAt:      calculateExtendAt(update.msg.VT),
					})
				case messageRemoved:
					vts.remove(update.msg.ID)
				}
				updateTimer()
			case <-nextExtension:
				tryAfter = c.extendVTs(vts)
				updateTimer()
			}
			if c.ctx.Err() != nil {
				break
			}
		}
		// on shutdown drain updates channel
		for _ = range c.vtMessageUpdates {
		}

	}()
}

// this function extends one batch and returns back to the main loop that can return back here if more messages needs
// to be extended
func (c *Consumer) extendVTs(vts *vtHeap) (tryAfter time.Time) {
	extendDeadline := time.Now().Add(time.Duration(c.vtSec*200) * time.Millisecond) // take a 20% of period ahead to avoid extending only first message
	extendingMap := make(map[int]*vtInfo)
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
		c.conn.logger.Printf("Failed to extend message vts batch: %v", err)
		tryAfter = time.Now().Add(1 * time.Second) // take a break before trying again
		return
	}
	if len(extendedVTs) != len(extending) {
		c.conn.logger.Printf("Some messages were not extended: expected %d, got %d",
			len(extending), len(extendedVTs))
	}
	// add new VTs back to the heap
	for _, vt := range extendedVTs {
		oldVT, ok := extendingMap[vt.ID]
		if !ok {
			c.conn.logger.Printf("Extending message returned some unknown message : %d", vt.ID)
		}
		oldVT.extendAt = calculateExtendAt(vt.VT)
		vts.push(oldVT)
	}
	return
}

func (c *Consumer) Messages() <-chan *Message {
	return c.messages
}

func (c *Consumer) Stop() {
	c.cancel()
	c.wg.Wait()
}

// vtHeap implementation

type vtHeap struct {
	items     []*vtInfo
	itemIndex map[int]int // maps messageID to index in items slice
}

func newVTHeap() *vtHeap {
	return &vtHeap{
		items:     make([]*vtInfo, 0),
		itemIndex: make(map[int]int),
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

func (h *vtHeap) remove(messageID int) *vtInfo {
	if idx, ok := h.itemIndex[messageID]; ok {
		return heap.Remove(h, idx).(*vtInfo)
	}
	return nil
}
