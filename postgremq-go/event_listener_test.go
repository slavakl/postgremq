package postgremq_go_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	postgremq "github.com/slavakl/postgremq/postgremq-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForListen probes the given LISTEN channel with test NOTIFYs until the
// handle receives one — proving that the listener has actually issued LISTEN
// on the wire and is delivering events to this handle. Without this sync,
// publish-and-expect-wake tests can race the asynchronous LISTEN registration
// (control NOTIFY → reconcile → LISTEN command).
func waitForListen(t *testing.T, pool *pgxpool.Pool, handle *postgremq.ListenerHandle, channel string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := pool.Exec(context.Background(),
			"SELECT pg_notify($1, 'probe')", channel)
		require.NoError(t, err)
		select {
		case <-handle.Wake():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("LISTEN for %q did not become active within 2s", channel)
}

// drainHandle drains pending wake events on the handle (e.g. probe events
// queued by waitForListen). Returns when no event arrives within 50ms.
func drainHandle(handle *postgremq.ListenerHandle) {
	for {
		select {
		case <-handle.Wake():
		case <-time.After(50 * time.Millisecond):
			return
		}
	}
}

// TestPerTopicNotify_CacheHit verifies that Consume against a queue created
// through this Connection does not need a DB lookup to discover the topic.
// We delete the queue between CreateQueue and Consume; if Consume hits the
// cache, it succeeds; if it falls back to DB, it would error.
func TestPerTopicNotify_CacheHit(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic = "TopicCacheHit"
	const queue = "QueueCacheHit"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queue, topic, false, postgremq.WithMaxDeliveryAttempts(0)))

	// Drop the queue at the SQL level so a DB lookup would fail. The cache
	// entry from CreateQueue lets Consume succeed regardless.
	_, err = pool.Exec(ctx, "DELETE FROM queues WHERE name = $1", queue)
	require.NoError(t, err)

	consumer, err := conn.Consume(ctx, queue, postgremq.WithVT(30))
	require.NoError(t, err)
	consumer.Stop()
}

// TestPerTopicNotify_OutOfBandQueueNeedsWithTopic verifies that a queue that
// exists in the database but was not created through this Connection requires
// the caller to pass WithTopic — the client does not query the database to
// resolve the topic.
func TestPerTopicNotify_OutOfBandQueueNeedsWithTopic(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	const topic = "TopicOutOfBand"
	const queue = "QueueOutOfBand"
	_, err := pool.Exec(ctx, "SELECT create_topic($1)", topic)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "SELECT create_queue($1, $2, 0, false, interval '30 seconds')", queue, topic)
	require.NoError(t, err)

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	// Without WithTopic, Consume errors because the cache is empty.
	_, err = conn.Consume(ctx, queue, postgremq.WithVT(30))
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrQueueNotFound))

	// With WithTopic, it succeeds.
	consumer, err := conn.Consume(ctx, queue, postgremq.WithVT(30), postgremq.WithTopic(topic))
	require.NoError(t, err)
	consumer.Stop()
}

// TestPerTopicNotify_WithTopicOption verifies WithTopic bypasses both the
// cache and any DB lookup. Even with the queue absent from the database, the
// listener registration succeeds.
func TestPerTopicNotify_WithTopicOption(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	consumer, err := conn.Consume(ctx, "QueueDoesNotExistInDB",
		postgremq.WithVT(30),
		postgremq.WithTopic("ExplicitTopic"),
	)
	require.NoError(t, err)
	consumer.Stop()
}

// TestPerTopicNotify_UnknownQueueReturnsError verifies that consuming from a
// queue that does not exist in the database (and is not in the cache) returns
// ErrQueueNotFound.
func TestPerTopicNotify_UnknownQueueReturnsError(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Consume(ctx, "GhostQueue", postgremq.WithVT(30))
	require.Error(t, err)
	assert.True(t, errors.Is(err, postgremq.ErrQueueNotFound),
		"expected ErrQueueNotFound, got: %v", err)
}

// TestPerTopicNotify_RefcountSharing verifies that two consumers on the same
// topic share a single physical LISTEN per channel.
func TestPerTopicNotify_RefcountSharing(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic = "RefcountTopic"
	const queueA = "RefcountQueueA"
	const queueB = "RefcountQueueB"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queueA, topic, false, postgremq.WithMaxDeliveryAttempts(0)))
	require.NoError(t, conn.CreateQueue(ctx, queueB, topic, false, postgremq.WithMaxDeliveryAttempts(0)))

	c1, err := conn.Consume(ctx, queueA, postgremq.WithVT(30))
	require.NoError(t, err)
	c2, err := conn.Consume(ctx, queueB, postgremq.WithVT(30))
	require.NoError(t, err)

	// Brief settle so both subscriptions have registered.
	time.Sleep(50 * time.Millisecond)

	el := conn.EventListener()
	assert.Equal(t, 2, el.SubscriberCount("pmq:t:"+topic),
		"two consumers on the same topic should share a refcount of 2")
	assert.Equal(t, 1, el.SubscriberCount("pmq:q:"+queueA))
	assert.Equal(t, 1, el.SubscriberCount("pmq:q:"+queueB))

	c1.Stop()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, el.SubscriberCount("pmq:t:"+topic),
		"after one consumer stops, the topic refcount drops to 1")
	assert.Equal(t, 0, el.SubscriberCount("pmq:q:"+queueA),
		"queueA listener should be released")

	c2.Stop()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, el.SubscriberCount("pmq:t:"+topic),
		"after both consumers stop, the topic listener is released")
}

// TestEventListener_DeliversTopicNotify verifies that a NOTIFY on a per-topic
// channel is delivered to a handle's Wake channel with the parsed message id.
// Tests the listener directly, bypassing Consumer to avoid conflating the
// listener path with Consumer's initial fetch / polling fallbacks.
func TestEventListener_DeliversTopicNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()

	const topic = "DirectTopic"
	handle := el.AddTopicListener(topic)
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:t:"+topic)
	drainHandle(handle)

	_, err = pool.Exec(ctx, "SELECT pg_notify($1, $2)", "pmq:t:"+topic, "42")
	require.NoError(t, err)

	select {
	case msgID := <-handle.Wake():
		require.Equal(t, int64(42), msgID)
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive NOTIFY on per-topic channel")
	}
}

// TestEventListener_DeliversQueueNotify is the per-queue counterpart.
func TestEventListener_DeliversQueueNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	el := conn.EventListener()
	el.Start()

	const queue = "DirectQueue"
	handle := el.AddQueueListener(queue)
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:q:"+queue)
	drainHandle(handle)

	_, err = pool.Exec(ctx, "SELECT pg_notify($1, $2)", "pmq:q:"+queue, "777")
	require.NoError(t, err)

	select {
	case msgID := <-handle.Wake():
		require.Equal(t, int64(777), msgID)
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive NOTIFY on per-queue channel")
	}
}

// TestEventListener_PublishTriggersTopicNotify verifies that the SQL
// distribute_message trigger emits NOTIFY on `pmq:t:<topic>` carrying the
// new message id, and that it reaches a directly-attached handle.
func TestEventListener_PublishTriggersTopicNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "PubTopic"))

	el := conn.EventListener()
	el.Start()

	handle := el.AddTopicListener("PubTopic")
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:t:PubTopic")
	drainHandle(handle)

	msgID, err := conn.Publish(ctx, "PubTopic", []byte(`{}`))
	require.NoError(t, err)

	select {
	case got := <-handle.Wake():
		require.Equal(t, int64(msgID), got)
	case <-time.After(2 * time.Second):
		t.Fatal("publish_message did not produce NOTIFY on per-topic channel")
	}
}

// TestEventListener_NackTriggersPerQueueNotify verifies that the SQL
// nack_message function emits NOTIFY on `pmq:q:<queue>` (when not retiring
// to DLQ inline) carrying the affected message id.
func TestEventListener_NackTriggersPerQueueNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "NackTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "NackQueue", "NackTopic", false))

	msgID, err := conn.Publish(ctx, "NackTopic", []byte(`{}`))
	require.NoError(t, err)

	// Consume via raw SQL to grab a token without involving Consumer.
	var consumedID int64
	var token string
	err = pool.QueryRow(ctx, `
		SELECT message_id, consumer_token FROM consume_message($1, 30)
	`, "NackQueue").Scan(&consumedID, &token)
	require.NoError(t, err)
	require.Equal(t, int64(msgID), consumedID)

	el := conn.EventListener()
	el.Start()

	handle := el.AddQueueListener("NackQueue")
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:q:NackQueue")
	drainHandle(handle)

	_, err = pool.Exec(ctx, `SELECT nack_message($1, $2, $3, NOW())`, "NackQueue", msgID, token)
	require.NoError(t, err)

	select {
	case got := <-handle.Wake():
		require.Equal(t, int64(msgID), got)
	case <-time.After(2 * time.Second):
		t.Fatal("nack_message did not produce NOTIFY on per-queue channel")
	}
}

// TestEventListener_ReleaseTriggersPerQueueNotify verifies release_message
// emits NOTIFY on `pmq:q:<queue>`.
func TestEventListener_ReleaseTriggersPerQueueNotify(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.CreateTopic(ctx, "RelTopic"))
	require.NoError(t, conn.CreateQueue(ctx, "RelQueue", "RelTopic", false))

	msgID, err := conn.Publish(ctx, "RelTopic", []byte(`{}`))
	require.NoError(t, err)

	var token string
	err = pool.QueryRow(ctx, `SELECT consumer_token FROM consume_message($1, 30)`, "RelQueue").Scan(&token)
	require.NoError(t, err)

	el := conn.EventListener()
	el.Start()

	handle := el.AddQueueListener("RelQueue")
	defer handle.Close()

	waitForListen(t, pool, handle, "pmq:q:RelQueue")
	drainHandle(handle)

	_, err = pool.Exec(ctx, `SELECT release_message($1, $2, $3)`, "RelQueue", msgID, token)
	require.NoError(t, err)

	select {
	case got := <-handle.Wake():
		require.Equal(t, int64(msgID), got)
	case <-time.After(2 * time.Second):
		t.Fatal("release_message did not produce NOTIFY on per-queue channel")
	}
}

// TestEventListener_HandleCloseClosesWake verifies that ListenerHandle.Close
// closes the Wake channel — both via Consumer.Stop() and via direct handle
// close. Previously, only EventListener.Close closed wake channels and the
// Consumer-driven path left them open.
func TestEventListener_HandleCloseClosesWake(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	conn.EventListener().Start()

	h1 := conn.EventListener().AddTopicListener("HandleCloseTopic")
	h2 := conn.EventListener().AddQueueListener("HandleCloseQueue")
	wake1 := h1.Wake()
	wake2 := h2.Wake()

	h1.Close()
	h2.Close()

	// Both wake channels must be closed after the handle's Close.
	assertChannelClosed(t, wake1, "topic handle wake should close on Close()")
	assertChannelClosed(t, wake2, "queue handle wake should close on Close()")

	// Refcounts must be back to zero.
	assert.Equal(t, 0, conn.EventListener().SubscriberCount("pmq:t:HandleCloseTopic"))
	assert.Equal(t, 0, conn.EventListener().SubscriberCount("pmq:q:HandleCloseQueue"))

	// Idempotent.
	h1.Close()
	h2.Close()
}

// TestEventListener_CloseClosesRemainingWakes verifies EventListener.Close
// closes the wake channels of any handles still registered.
func TestEventListener_CloseClosesRemainingWakes(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)

	conn.EventListener().Start()

	h := conn.EventListener().AddTopicListener("ListenerCloseTopic")
	wake := h.Wake()

	require.NoError(t, conn.Close())

	assertChannelClosed(t, wake, "EventListener.Close should close remaining handle wakes")

	// Subsequent handle.Close is a safe no-op.
	h.Close()
}

// TestEventListener_DispatchDoesNotPanicOnConcurrentClose stress-tests the
// dispatch / handle.Close interaction under the race detector. If the per-
// handle mutex were missing or used incorrectly, a send-on-closed-channel
// panic would crash the test.
func TestEventListener_DispatchDoesNotPanicOnConcurrentClose(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic = "RaceTopic"
	const queue = "RaceQueue"
	require.NoError(t, conn.CreateTopic(ctx, topic))
	require.NoError(t, conn.CreateQueue(ctx, queue, topic, false, postgremq.WithMaxDeliveryAttempts(0)))

	// Repeatedly add+close subscribers while the publisher is firing
	// notifications — exercises the dispatch + handle.Close race.
	stop := make(chan struct{})
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = conn.Publish(ctx, topic, []byte(`{}`))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for i := 0; i < 200; i++ {
		h := conn.EventListener().AddTopicListener(topic)
		// Drain the wake channel briefly so dispatch has somewhere to send.
		go func(h *postgremq.ListenerHandle) {
			for range h.Wake() {
			}
		}(h)
		// Race close against any in-flight dispatch.
		h.Close()
	}

	close(stop)
	<-publisherDone
}

// TestEventListener_NotifyDrainerCoalescesChurn verifies that rapid
// Add/Remove churn does not spawn N goroutines per change — the single
// notifyDrainer goroutine handles all NOTIFYs sequentially. If the drainer
// were broken (e.g. one goroutine per signal), the test would deadlock or
// timeout due to pool exhaustion.
func TestEventListener_NotifyDrainerCoalescesChurn(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	conn.EventListener().Start()

	// Issue many subscribe/unsubscribe pairs quickly. The wake-via-NOTIFY
	// drainer should coalesce these into far fewer NOTIFY round-trips than
	// the number of changes.
	const churn = 500
	for i := 0; i < churn; i++ {
		h := conn.EventListener().AddTopicListener("CoalesceTopic")
		h.Close()
	}

	// Give the drainer time to settle (any in-flight NOTIFY should flush
	// without exhausting the pool).
	require.Eventually(t, func() bool {
		return conn.EventListener().SubscriberCount("pmq:t:CoalesceTopic") == 0
	}, 2*time.Second, 10*time.Millisecond)
}

// TestEventListener_MultipleSubscribersAllReceive verifies that two
// subscribers on the same channel both receive each NOTIFY (refcount
// sharing must not multiplex events to a single subscriber).
func TestEventListener_MultipleSubscribersAllReceive(t *testing.T) {
	t.Parallel()
	pool, ctx := setupTestConnection(t)
	defer pool.Close()

	conn, err := postgremq.DialFromPool(ctx, pool)
	require.NoError(t, err)
	defer conn.Close()

	const topic = "FanOutTopic"
	require.NoError(t, conn.CreateTopic(ctx, topic))

	conn.EventListener().Start()

	h1 := conn.EventListener().AddTopicListener(topic)
	defer h1.Close()
	h2 := conn.EventListener().AddTopicListener(topic)
	defer h2.Close()

	// Sync both handles separately. Both share the single physical LISTEN,
	// but each handle has its own wake channel, and we want each one to
	// have already drained any probe events before the real publish fires.
	waitForListen(t, pool, h1, "pmq:t:"+topic)
	waitForListen(t, pool, h2, "pmq:t:"+topic)
	drainHandle(h1)
	drainHandle(h2)

	// Trigger one publish; both subscribers must wake.
	_, err = conn.Publish(ctx, topic, []byte(`{}`))
	require.NoError(t, err)

	receiveOne := func(ch <-chan int64, name string) {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not receive wake event", name)
		}
	}
	receiveOne(h1.Wake(), "subscriber 1")
	receiveOne(h2.Wake(), "subscriber 2")
}

// assertChannelClosed asserts that a wake channel has been closed (a receive
// returns immediately with ok=false). Drains any pending values first.
func assertChannelClosed(t *testing.T, ch <-chan int64, msg string) {
	t.Helper()
	deadline := time.After(1 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
			// Drain pending value and continue.
		case <-deadline:
			t.Fatalf("%s: channel did not close within 1s", msg)
		}
	}
}
