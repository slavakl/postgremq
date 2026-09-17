> Historical design notes. The implemented ownership and shutdown contract is in [delivery-lifecycle.md](delivery-lifecycle.md); it supersedes the generation, renewal and cancellation details below.

# PostgreMQ Go client — connection-actor refactor (executable spec)

> **How to use this document.** This is a self-contained work order. A fresh
> session should be able to execute it end-to-end with no other context. Read
> §0–§3 for orientation, then execute the phases in order (§4 → §5 → §6). Each
> phase has *exact changes*, *gotchas*, *tests to add*, and an *acceptance gate*.
> Do not start a phase until the previous phase's acceptance gate is green.
> Work in a git worktree; keep it off `main` until each phase passes `-race`.
>
> **Scope discipline:** this is a *structural simplification* — no behaviour
> changes, no feature loss. Every guard listed in §2 must still hold. When in
> doubt, preserve behaviour and add a comment; do not "improve" semantics.

---

## 0. Status

| Component | SQL + tests | Go client | TS client |
|---|---|---|---|
| vt extension — `set_vt_batch_multi` | ✅ DONE | ⬜ Phase B | ⬜ Phase D |
| keep-alive — `extend_queue_keep_alive_multi` | ✅ DONE | ⬜ Phase A | ⬜ Phase D |
| single-loop consumer | n/a | ⬜ Phase C | ⬜ Phase D |

The two cross-queue SQL functions already exist in **both** `mq/sql/latest.sql`
and `mq/migrations/000001_initial_schema.up.sql` (kept byte-identical — this
project is unshipped, so we edit in place; **do not add a new migration**), with
Python tests in `mq/tests/tests.py` (`test_set_vt_batch_multi_*`,
`test_extend_keep_alive_multi_*`). The singular/per-queue functions
(`set_vt_batch`, `extend_queue_keep_alive`) still exist and are still used by the
current Go/TS code; **delete them only after both clients are ported** (end of
§7).

**Pre-existing red:** `mq/tests/tests.py::test_maintenance_grace_period_for_just_expired_queue`
fails on clean `main` — it asserts a 5s keep-alive grace that commit `3b1788d`
deliberately removed. Fix or delete it at some point; it is unrelated to this
work and is the only expected failure in the SQL suite.

---

## 1. Why & orientation

The Go client started clean and accreted many small concurrency patches. The
target simplification: move latency-tolerant background work off the
per-consumer / per-queue goroutines and onto **connection-level actors** that
batch across queues using the new SQL. Net effect:

- consumer goes from **3 goroutines + a heap + 4 channels + 2 flags** to **1
  goroutine** (Phase C),
- keep-alive goes from **N goroutines (one per exclusive queue)** to **1**
  (Phase A),
- vt extension moves out of the consumer into **1 connection-level actor** that
  coalesces every consumer's extensions into one DB round-trip per tick (Phase B).

### Files

- `postgremq-go/consumer.go` — the 3 goroutines, `vtHeap`, auto-extension.
- `postgremq-go/connection.go` — `Connection`, `Consume`/`ConsumeHandler`,
  `SetVTBatch`, `startKeepAlive`/`sendKeepAlive`, `Close`, `CreateQueue`.
- `postgremq-go/handler_consumer.go` — `HandlerConsumer` (dispatch loop +
  semaphore). Barely changes.
- `postgremq-go/message.go` — `Message.Ack/Nack/Release`, `complete()`,
  `onComplete`, `cancel`/`StoppedCtx`.
- `postgremq-go/event_listener.go` — `EventListener` + `ListenerHandle`. **Do
  not touch** (start-once by design; restarting it reintroduces a fixed
  reconnect race). Consumers receive wake signals via `handle.Wake()` (a
  `<-chan struct{}` that is *closed* when the listener/handle closes).
- `postgremq-go/options.go` — consume + queue options.
- `postgremq-go/retry.go` — `withRetry`, error classification.

### How it works today (must understand before changing)

**Consumer (`consumer.go`)** runs three goroutines started by `start()`:
1. `startMessageLoop` — fetches a batch (`fetchMessages` → `consumeMessages`),
   delivers to the buffered `c.messages` channel; on `ctx.Done` closes+drains
   `messages`, signals teardown, waits on `inFlightFlag`, then closes the two
   update channels.
2. `startMessageTrackingLoop` — owns `inFlight map[trackingID]*Message`; on a
   `messageLoopStopped` sentinel it cancels every in-flight `StoppedCtx`; closes
   `inFlightFlag` once cancelled **and** empty.
3. `startExtendLoop` — owns `vtHeap` (keyed by `messageID`), batches
   `SetVTBatch`, and on lease-loss cancels the message's `StoppedCtx`.

Channels/flags that exist **only** to coordinate those three:
`vtMessageUpdates`, `messageUpdates`, `inFlightFlag`, the `messageLoopStopped`
op, the `handleMessageComplete` "send vt-channel before tracking-channel" rule,
and the extend-loop's deferred `for range vtMessageUpdates` drain.

**Keep-alive (`connection.go`)** is already connection-scoped but as **one
goroutine per exclusive queue**: `CreateQueue` calls `startKeepAlive(name,
interval)` when `exclusive`; each goroutine fires every `interval/2`, calls
`sendKeepAlive` → `extend_queue_keep_alive`. On `PMQ02`(gone)/`PMQ03`
(non-exclusive) it stops; on transient error it retries after 1s, **forever, with
no failure signal**. All run on `c.keepAliveCtx`/`c.keepAliveWg`, cancelled in
`Close()` *after* consumers drain.

**Settle (`message.go`)** — `Ack/Nack/Release` call the matching `Connection`
method (immediate DB round-trip) then `complete()`, which fires `onComplete`
exactly once (`completeOnce`) and **untracks unconditionally** (even on error).

---

## 2. Invariants that MUST survive (guard catalogue)

Each is backed by a real fixed bug — cite the commit in code comments when you
preserve it. Removing one silently reintroduces its bug.

- **G1 — lease-lost cancel (`c087e24`).** When the batch-extend result omits a
  requested row, that lease is gone server-side; the message's `StoppedCtx`
  **must** be cancelled so the handler stops before committing non-idempotent
  side-effects (it would otherwise run to completion then fail ack with PMQ01).
- **G2 — override-on-(composite)-key (`3b1788d`).** A re-picked message (lease
  lapsed → re-consumed, **new token**) must *replace* its tracking entry, not
  duplicate it. Per-consumer today the key is `messageID`; **at connection scope
  the key MUST be `(queue, message_id)`** — `distribute_message` fans one
  `message_id` into every queue on the topic, so `message_id` alone collides
  across queues and would corrupt the index / stop one copy from being extended.
  This is the single most important correctness point of the connection-level
  move.
- **G3 — unconditional untrack (`3b1788d`).** `complete()` untracks whether or
  not the settle succeeded. A failed settle (PMQ01/lease-lost) is terminal — the
  row is no longer ours and can never be acked; skipping untrack strands it and
  deadlocks shutdown.
- **G4 — consume is non-retryable + partial-batch (`0dd149e`).**
  `consumeMessages` must **not** be wrapped in `withRetry` (a retry double-claims
  a batch). Rows successfully scanned before an iteration error are returned
  *with* the error and delivered normally. **This lives in the fetch path and is
  untouched by every phase — do not refactor it.**
- **G5 — no stranded batch (`133ad6d` #2).** Every fetched message that was
  registered for tracking must eventually be delivered **or** released, or
  shutdown waits forever. (Old bug: `fetchMessages` `return`ed instead of
  `continue`ing on `ctx.Done` mid-delivery, stranding the rest of the batch.)
- **G6 — extend in-flight through the drain.** In-flight messages keep getting
  their vt extended until they settle, even during a slow shutdown drain (today:
  vt channel is closed only *after* tracking drains). A slow handler must not let
  its own lease expire mid-shutdown.
- **G7 — keep-alive outlives the drain & needs no consumer.** Keep-alive runs on
  its own context, is stopped **last** in `Close()` (after consumers drain), and
  must keep firing for exclusive queues that have **zero consumers** (producer-only
  or created-but-unconsumed).
- **G8 — `withRetry` boundaries.** Extension (`set_vt`/`set_vt_batch*`) and
  keep-alive ARE idempotent → keep `withRetry`. Consume is NOT (G4). Settle
  (`ack/nack/release`) keeps its existing retry behaviour and stays an immediate
  per-call round-trip — **never batch settle ops** (explicit product decision:
  they carry a synchronous error contract and `AckWithTx` rides the caller's tx).

---

## 3. The shared actor skeleton

Both new actors (keep-alive in §4, vt extender in §5) are the same shape. Build
keep-alive first as the simpler instance; factor the common skeleton out only if
it falls out naturally — **do not over-abstract up front**. The pattern:

```
one goroutine, owns ALL its state, no mutex. A select loop where:
  - <-ctx.Done()        -> return
  - register/deregister -> mutate in-memory schedule (heap/map), re-arm timer
  - <-timer             -> compute the DUE set (cheap), dispatch ONE batched DB
                           call on a SEPARATE goroutine, set an "in-flight" flag
  - <-results           -> clear in-flight flag, apply outcome (reschedule kept,
                           handle failed), re-arm timer
```

**The one rule that makes a single select loop safe:** *every case body is
O(in-memory); anything that can block on I/O is dispatched to a goroutine that
reports back on a channel.* A slow DB call therefore can never stall
registration, deregistration, or shutdown. Guard with an "in-flight" bool so
ticks can't pile up overlapping DB calls.

**Channel topology (no deadlock):** consumers/CreateQueue send *to* the actor
(`register`/`deregister`, buffered). The actor **never sends back** to a consumer
— lease-loss is delivered by calling the message's `cancel` (a context cancel,
safe from any goroutine) **directly**. This removes the classic two-actor
send/send cycle. Keep `register`/`deregister` buffered (e.g. cap = a few × batch
size); the actor drains them fast because it never blocks on I/O.

---

## 4. PHASE A — connection-level keep-alive actor (do first)

**Goal:** replace the N per-queue keep-alive goroutines with one connection
actor that batches via `extend_queue_keep_alive_multi`, and add a failure signal
(aligning Go to TS — review item #13).

### SQL contract (already built)

```
extend_queue_keep_alive_multi(p_queue_names VARCHAR[], p_intervals_ms BIGINT[])
  RETURNS TABLE(queue_name VARCHAR)
```
Returns the queues actually kept alive. **Omitted from a successful result =
permanent failure** (queue gone or non-exclusive). A transient DB error raises
(fails the whole call) → retry the whole batch next tick. Intervals are
milliseconds (matches `create_queue` / `sendKeepAlive`). Lengths must match
(`PMQ03`).

### Go changes

1. **New Connection method** (mirror `sendKeepAlive`, keep `withRetry` — G8):
   ```go
   // returns the queue names actually kept alive; omitted = permanent failure
   func (c *Connection) extendKeepAliveMulti(ctx context.Context, names []string, intervalsMs []int64) ([]string, error)
   //   SELECT queue_name FROM extend_queue_keep_alive_multi($1, $2)
   //   args: names ([]string -> VARCHAR[]), intervalsMs ([]int64 -> BIGINT[])
   ```
2. **New keep-alive actor.** Put it in a new file `postgremq-go/keepalive.go`
   (or a method set on `Connection`). State owned by one goroutine:
   ```go
   type kaEntry struct {
       queue      string
       intervalMs int64
       nextAt     time.Time   // when to extend next (= last + interval/2)
       failTries  int         // consecutive transient failures (for bounded retry)
   }
   ```
   Loop (skeleton from §3):
   - `register <-`: upsert by `queue` (dedupe — `CreateQueue` may be called twice).
     If the actor goroutine isn't started yet, start it (see lazy-start gotcha).
   - `deregister <-`: delete by `queue` (used by `DeleteQueue`; see gotcha).
   - `<-timer`: collect entries with `nextAt <= now`; if any and no flush
     in-flight, dispatch `go flush(due)`.
   - `flush(due)`: call `extendKeepAliveMulti(due.names, due.intervalsMs)`; send
     `{kept []string, err error, due []kaEntry}` to `results`.
   - `results <-`:
     - `err != nil` (transient, already `withRetry`-exhausted): bump `failTries`
       on each due entry; if `failTries` exceeds a small budget → permanent (drop
       + notify); else reschedule due entries a short delay later (e.g. 1s, but
       clamp so the next try still lands inside `interval/2` headroom). Reset
       `failTries` to 0 on any success.
     - `err == nil`: for each due entry, if its queue is in `kept` → reschedule
       `nextAt = now + interval/2`, `failTries = 0`; else (omitted) → **permanent
       failure**: drop the entry and fire `onKeepAliveFailure(queue, err)`.
3. **Wire `CreateQueue`:** replace `c.startKeepAlive(name, options.keepAliveInterval)`
   with `c.keepAliveRegister(name, options.keepAliveInterval.Milliseconds())`.
4. **Wire `DeleteQueue`:** add `c.keepAliveDeregister(queue)` (unconditional;
   no-op if absent) **before/around** the DB delete, so an intentional delete
   doesn't trigger a spurious `onKeepAliveFailure` on the next tick (the queue
   would otherwise be "omitted = permanent" — see gotcha).
5. **Wire `Close`:** keep the exact ordering — cancel `keepAliveCtx` and join the
   actor goroutine **last**, after consumers drain (where `keepAliveCancel()` +
   `keepAliveWg.Wait()` are today). Reuse `keepAliveCtx`/`keepAliveWg`.
6. **Add the failure hook** as a connection option:
   `WithKeepAliveFailureHandler(func(queue string, err error))`. Default no-op
   (or log). Invoke it from the `results` handler; if it might block, call it in
   a goroutine — never inline in the select loop.
7. **Delete** `startKeepAlive` and `sendKeepAlive` once nothing references them.

### Gotchas (Phase A)

- **G7 lifetime:** the actor must keep firing for queues with no consumers and
  must outlive the consumer drain. Stay on `keepAliveCtx` (separate from
  `conn.ctx`); stop it last in `Close()`. Do not parent it on `conn.ctx`.
- **Lazy start:** today there's no keep-alive goroutine when there are no
  exclusive queues. Preserve that: start the actor on first `register` via a
  `sync.Once`-style guard, OR accept one idle goroutine per connection (simpler;
  fine). If lazy, the start must be race-safe with `Close` (mirror the
  `eventListenerDoOnce` + `checkClosed`-under-lock pattern in `Consume`).
- **Spurious-failure-on-delete:** without the `DeleteQueue` deregister (step 4),
  deleting an exclusive queue makes the next tick see it "omitted = permanent"
  and fire `onKeepAliveFailure` for what was an intentional delete. Deregister
  first. (Also consider `DeleteInactiveQueues`/`CleanUpQueue` if they can remove
  a still-registered exclusive queue — at minimum document; ideally deregister.)
- **Transient vs permanent:** distinguish ONLY by the call's error: `err != nil`
  ⇒ transient (retry whole batch); `err == nil` + omitted ⇒ permanent. Do not
  try to subclassify PMQ02 vs PMQ03 — the batch can't report per-row reasons, and
  both mean "stop keeping this queue alive".
- **Bounded retry budget (review #13):** Go currently retries transients forever
  with no signal. The new policy (matching TS): bounded transient retries, then
  give up + `onKeepAliveFailure`. Keep the retry window comfortably inside
  `interval/2` so a recovered transient still extends before expiry.
- **Don't block the loop:** `flush` and the failure callback go off-loop.

### Tests (Phase A) — `postgremq-go/*_test.go`

- N exclusive queues → assert one tick issues **one** `extendKeepAliveMulti`
  call covering all (use a mock pool / spy, like `TestConsumeMessagesDoesNotRetry`).
- `keep_alive_until` actually advances for each registered queue.
- Permanent failure: drop a queue server-side (or mock omission) → assert it's
  removed from the schedule and `onKeepAliveFailure` fires exactly once, and no
  further calls reference it.
- Transient failure: mock the call to error once → assert retry, then success,
  and **no** failure callback.
- `DeleteQueue` deregisters → no `onKeepAliveFailure` after an intentional delete.
- `-race`: interleave `CreateQueue`(exclusive) with `Close()`.

### Acceptance gate A
`go test -race ./...` green (Go suite), the new keep-alive tests green, and **no
remaining references** to `startKeepAlive`/`sendKeepAlive`. Keep-alive integration
tests that previously passed still pass.

---

## 5. PHASE B — connection-level vt extender

**Goal:** move auto-extension out of the consumer into one connection actor that
coalesces all consumers' extensions into one `set_vt_batch_multi` call per tick.
In this phase the consumer keeps `startMessageLoop` + `startMessageTrackingLoop`;
only `startExtendLoop`/`vtHeap` are removed and replaced by register/deregister
against the extender. (The full single-loop collapse is Phase C.)

### SQL contract (already built)

```
set_vt_batch_multi(p_queue_names VARCHAR[], p_message_ids BIGINT[],
                   p_consumer_tokens VARCHAR[], p_vts INTEGER[])  -- vts in SECONDS
  RETURNS TABLE(queue_name VARCHAR, message_id BIGINT, vt TIMESTAMPTZ)
```
Per-row VT (no client-side grouping needed). Returns rows actually extended;
**correlate by `(queue_name, message_id)`** — `message_id` alone is ambiguous
across queues. Omitted `(queue,id)` = lease lost. Global `ORDER BY (queue_name,
message_id) FOR UPDATE` (deadlock-safe across callers/processes).

### Go changes

1. **New Connection method** (keep `withRetry` — extension is idempotent, G8):
   ```go
   type MultiExtension struct { Queue string; ID int64; Token string; VTSec int }
   type MultiLock      struct { Queue string; ID int64; VT time.Time }
   func (c *Connection) SetVTBatchMulti(ctx context.Context, exts []MultiExtension) ([]MultiLock, error)
   //   SELECT queue_name, message_id, vt FROM set_vt_batch_multi($1,$2,$3,$4)
   //   build 4 parallel arrays from exts; empty -> return nil,nil
   ```
2. **New vt-extender actor** (`postgremq-go/extender.go`). One goroutine, owns:
   - a min-heap ordered by `extendAt`,
   - an index map keyed by the **composite** `extKey{queue string; id int64}` (G2),
   - entries:
     ```go
     type extEntry struct {
         queue, token string
         id           int64
         vtSec        int
         threshold    float64           // per-consumer (extensionThreshold)
         extendAt     time.Time
         cancel       context.CancelFunc // == msg.cancel; called directly on lease-loss (G1)
     }
     ```
   Loop (skeleton §3):
   - `register <-`: `upsert` by `extKey` (override-on-push, G2). Compute
     `extendAt = calculateExtendAt(msg.GetVT(), threshold)`.
   - `deregister <-`: remove by `extKey`.
   - `<-timer`: pop entries whose `extendAt <= now` up to `extendBatchSize`;
     dispatch `go flush(popped)`.
   - `flush(popped)`: `SetVTBatchMulti(popped→MultiExtension)`; send
     `{locks, err, popped}` to `results`.
   - `results <-`:
     - `err != nil` (persistent after `withRetry`): re-push all `popped` with a
       backoff (`tryAfter ≈ now+1s`) — `extendAt` is a soft halfway deadline, so
       there's headroom before the real server lease expires (mirror current
       `extendVTs` error path).
     - else: build a set of returned `(queue,id)`; for each → reschedule
       `extendAt = calculateExtendAt(newVT, threshold)` and re-push; for each
       `popped` **not** returned → **lease lost (G1)**: call `entry.cancel()` and
       drop it (do not re-push).
   - Move `calculateExtendAt` and `ExtendWindowPercent`/the heap into this file.
     Keep the heap (urgent-first ordering matters once in-flight >
     `extendBatchSize`).
3. **Consumer changes (Phase B subset):**
   - Delete `startExtendLoop`, `vtHeap` (+ its methods), `vtMessageUpdates`,
     `extendVTs`, `calculateExtendAt` (moved), `ExtendWindowPercent` (moved).
   - In `fetchMessages`, after setting `msg.cancel`/`StoppedCtx`, **register**
     each message: `c.conn.extender.register(extEntry{queue:c.queue, id:msg.ID,
     token:msg.consumerToken, vtSec:c.vtSec, threshold:c.extensionThreshold,
     cancel:msg.cancel})` instead of sending to `vtMessageUpdates`. (Register
     applies to buffered-but-undelivered messages too — they hold a lease.)
   - In `handleMessageComplete`, **deregister** from the extender instead of
     sending the vt-channel update. The old "vt-channel before tracking-channel"
     ordering guard is now moot for the vt side — delete that comment/ordering;
     the tracking-channel send still gates `inFlightFlag`.
   - On `Consumer.Stop`, after the wg completes, call
     `c.conn.extender.deregisterAll(c.queue, …)` as a belt-and-suspenders cleanup
     (no-op if already drained). (Cheapest: deregister happens naturally as each
     message settles; this just covers any pathological leftover.)
4. **Wire `Close`:** stop the extender after consumers drain (alongside, in any
   order relative to, keep-alive). In-flight messages stay registered until they
   settle, so the extender must outlive the consumer drain (G6) — same position
   as keep-alive.
5. **Lazy start** the extender like the event listener (once, race-safe with
   `Close`).

### Gotchas (Phase B)

- **G2 composite key — the headline.** Index by `(queue, message_id)`. Same
  `message_id` on two queues = two independent entries (both extended). Same
  `(queue,id)` re-registered with a new token = in-place override. Getting this
  wrong silently stops one queue's copy from being extended → lease expiry →
  redelivery. **Add a test for `(q1,5)+(q2,5)` extended in one tick.**
- **G1 lease-loss = direct cancel.** The extender holds `msg.cancel` and calls it
  directly; it does **not** route back to the consumer. The consumer learns of it
  when the handler settles and gets PMQ01, which untracks via G3. No extra
  channel, no deadlock.
- **G6 extend-through-drain:** in-flight entries are deregistered only on settle,
  so they keep extending during shutdown. Verify the extender is stopped after
  the consumers' `Stop()` returns in `Close()`.
- **withRetry kept** (G8) — extension is idempotent (unlike consume).
- **`extendBatchSize` cap** stays as a per-tick bound on lock fan-out; with the
  global SQL ordering it's about statement size, not correctness.
- **No consumer↔extender send cycle:** extender never sends to a consumer;
  `register`/`deregister` are buffered and drained fast.
- **Do not reroute `Message.SetVT`** (the manual single-message API) through the
  batch path — it must keep raising PMQ01 directly (explicit review "don't").
- Keep old `SetVTBatch` + `set_vt_batch` SQL alive (TS still uses them until
  Phase D).

### Tests (Phase B)

- `(q1, msgID) + (q2, msgID)` (same message_id, two queues on one topic) both
  extended in one tick; extending only q1 leaves q2's vt untouched
  (composite-key / no-bleed; mirror the SQL test
  `test_set_vt_batch_multi_composite_key_no_cross_queue_bleed`).
- Lease-loss: force `SetVTBatchMulti` to omit a row → assert that message's
  `StoppedCtx` is cancelled (G1) and the entry is dropped.
- Multiple consumers on different queues → one tick issues **one**
  `SetVTBatchMulti` covering all.
- Existing `c087e24` lease-loss consumer tests still pass (now driven by the
  extender).
- `-race`: `TestMultipleConsumersSameQueue` ×20.

### Acceptance gate B
`go test -race ./...` green; `startExtendLoop`/`vtHeap`/`vtMessageUpdates` gone;
all prior consumer/handler tests still green; the SQL `test_set_vt_batch_multi_*`
remain green.

---

## 6. PHASE C — single-loop consumer

**Goal:** collapse the remaining two consumer goroutines
(`startMessageLoop` + `startMessageTrackingLoop`) into one actor loop, deleting
the sentinel, `inFlightFlag`, `messageUpdates`, and the send-ordering machinery.
Extension is already gone (Phase B), so the consumer only fetches, delivers,
tracks in-flight, and drains.

### Target loop

`Consumer.run()` is one goroutine owning all consumer state. Two sets:
`outbox` (fetched, not yet delivered) and `inflight` (delivered, not yet
settled). The fetch DB call is dispatched to a goroutine (results on `fetched`);
the outbound user send uses the **nil-channel trick** so a slow consumer
backpressures delivery without freezing completions/fetch/shutdown.

```go
func (c *Consumer) run() {
    defer close(c.done)
    outbox := []*Message{}
    inflight := map[string]*Message{}
    fetching, shutting, cancelledInflight := false, false, false

    arm := func() { /* schedule next fetch from wake chans + checkTimeout + nextVisible */ }

    for {
        var sendCh chan *Message
        var next *Message
        if !shutting && len(outbox) > 0 { sendCh, next = c.messages, outbox[0] }

        select {
        case <-c.ctx.Done():
            shutting = true

        case b := <-c.fetched:                  // batch from a fetch goroutine
            fetching = false
            for _, m := range b {
                inflight[m.trackingID] = m       // book BEFORE deliverable
                c.conn.extender.register(...)     // (Phase B) start extension
                outbox = append(outbox, m)
            }
            arm()

        case sendCh <- next:                    // delivered to the user
            outbox = outbox[1:]                  // remains in `inflight` until settle

        case id := <-c.completed:               // msg.complete() fired (settle, any outcome)
            if m, ok := inflight[id]; ok {
                delete(inflight, id)
                c.conn.extender.deregister(...)
            }

        case <-c.fetchTimer:                    // wake (topic/queue NOTIFY) or checkTimeout
            if !fetching && !shutting { fetching = true; go c.fetchInto(c.fetched) }
        }

        if shutting {
            // release everything fetched-but-undelivered (no delivery-attempt bump)
            for _, m := range outbox { c.conn.extender.deregister(...); _ = m.Release(bg) }
            outbox = outbox[:0]
            // cancel in-flight handlers ONCE; they keep extending until they settle (G6)
            if !cancelledInflight { for _, m := range inflight { m.cancel() }; cancelledInflight = true }
            if len(inflight) == 0 { return }
        }
    }
}
```

`Stop()` becomes: `c.cancel(); <-c.done; close listener handles`.
`message.complete()` → fire `onComplete` once (unchanged) → `onComplete` sends
`m.trackingID` on `c.completed` (buffered) and the loop deregisters from the
extender. `handleMessageComplete` is reduced to that single send.

### Delete in Phase C
`startMessageLoop`, `startMessageTrackingLoop`, `messageUpdates`, `inFlightFlag`,
`messageLoopStopped`, the `messageAdded/messageRemoved/messageLoopStopped`
`messageUpdate` plumbing, and the `handleMessageComplete` send-ordering comment.
Add `c.done chan struct{}`, `c.fetched chan []*Message`, `c.completed chan
string`, `c.fetchTimer` wiring.

### Gotchas (Phase C)

- **Head-of-line blocking** is the risk of one select loop. The rule: every case
  body is O(memory). The only blocking ops — DB fetch and user delivery — are a
  dispatched goroutine and the nil-channel-guarded `sendCh` respectively. A slow
  user must never stall `completed` or shutdown; the nil-channel trick guarantees
  it (when `outbox` empty or `shutting`, `sendCh==nil` so that case is disabled
  but the others run).
- **G5 (no stranded batch):** there is no "tracked-but-not-delivered" gap — the
  single loop does `inflight[id]=m` and the eventual deliver/release atomically
  w.r.t. its own state. On shutdown, `outbox` is released; `inflight` is
  cancelled and drained. The `133ad6d` #2 bug cannot recur.
- **G3 (unconditional untrack):** `complete()` still untracks regardless of
  settle error — it must still send on `c.completed` even when the settle errored
  (PMQ01/lease-lost), or the loop never drains. Keep `completeOnce` so it fires
  once.
- **G6:** in-flight stay registered with the extender (cancel ≠ deregister);
  they deregister only when they settle, so they keep extending through the drain.
- **WaitGroup/`done` accounting (review #11):** the fetch goroutine must be
  fully joined before `run` returns / `c.done` closes. Don't `Add` after `Wait`.
  Prefer the `c.done` close at the top via `defer` and a single owned fetch
  goroutine whose result is always consumed (or discarded after `shutting`).
- **`completed` buffering:** must not deadlock — a settling handler sends on
  `c.completed` while the loop may be doing other work. Buffer it (≥ batch size)
  and ensure the loop always returns to the select promptly (it does — all cases
  are O(memory)).
- **Fetch-after-shutdown:** if a fetch goroutine returns a batch after
  `shutting` became true, release those messages (don't deliver). Handle in the
  `fetched` case by checking `shutting`.
- `HandlerConsumer` is unchanged structurally — it still reads
  `consumer.Messages()` and runs handlers under its semaphore; it just waits on a
  simpler `Consumer.Stop()`.

### Tests (Phase C)
- `TestHandlerConsumerStopUnderLoadDoesNotDeadlock`, `TestHandlerConsumerMaxInFlight`,
  and all existing consumer/shutdown tests — must stay green unchanged.
- New: shutdown with buffered-but-undelivered messages → assert they're
  `Release`d (no delivery-attempt bump) while in-flight drain.
- New: `-race` interleave of `Consumer.Stop()` with `Connection.Close()`.
- Slow-consumer test: a consumer that reads slowly must not stall completion of
  already-delivered messages (head-of-line).

### Acceptance gate C
`go test -race ./...` green (run the shutdown/handler tests ×20); the deleted
symbols are gone; benchmarks (`go test -bench=. -benchmem`) show no regression.

---

## 7. PHASE D — TS port, then delete the old SQL

Port the same two actors to `postgremq-ts` (`connection.ts`, `consumer.ts`):
- vt extension → connection-level, `set_vt_batch_multi`, composite `(queue,id)`
  key (TS uses a sorted array / `ExtensionQueue` today).
- keep-alive → one connection-level timer + `extend_queue_keep_alive_multi`,
  collapsing the two `Map`s (`exclusiveQueueTimers`/`exclusiveQueueIntervals`)
  the review flagged; TS already has `onKeepAliveFailure`.
- Mirror the same gotchas (composite key, direct lease-loss cancel via the
  message's AbortController, never-block, keep-alive lifetime).

**Only after Go + TS both use the `*_multi` functions:** delete `set_vt_batch`
and `extend_queue_keep_alive` from `mq/sql/latest.sql` **and** the migration
(keep them byte-identical), remove their Python tests, and remove `SetVTBatch`
(Go) / the TS equivalent. Keep singular `set_vt` (manual `Message.SetVT`) and
singular `extend_queue_keep_alive` only if a non-batch public API still needs
them — otherwise drop them too.

---

## 8. Validation commands

```bash
# SQL (testcontainers; one pre-existing unrelated failure — see §0)
cd mq && python3 -m pytest tests/tests.py -q
cd mq && python3 -m pytest tests/tests.py -q -k "multi"     # the new cross-queue fns

# Go
cd postgremq-go && go test -race ./...
cd postgremq-go && go test -race -run TestMultipleConsumersSameQueue -count=20 ./...
cd postgremq-go && go test -run TestHandlerConsumerStopUnderLoadDoesNotDeadlock -count=20 ./...
cd postgremq-go && go test -bench=. -benchmem ./...

# TS (Phase D)
cd postgremq-ts && npm test
```

## 9. Execution checklist

- [x] Worktree created off `main`. (Worked on branch `actor-consumer-refactor`.)
- [x] **Phase A** keep-alive actor + `extendKeepAliveMulti` + failure hook
      (`WithKeepAliveFailureHandler`); `startKeepAlive`/`sendKeepAlive` deleted;
      gate A green.
- [x] **Phase B** vt extender + `SetVTBatchMulti`; `startExtendLoop`/`vtHeap`
      deleted; composite-key test added; gate B green.
- [x] **Phase C** single-loop consumer; sentinel/`inFlightFlag`/`messageUpdates`
      deleted; gate C green (×20 race runs).
- [x] **Phase D** TS port (connection-level keep-alive + vt-extender actors);
      then deleted old `set_vt_batch` / `extend_queue_keep_alive` (both SQL
      files, kept byte-identical) + dead Go (`SetVTBatch`/`MessageExtension`/
      `MessageLock`) and TS (`setMessagesVtBatch`/`extendQueueKeepAlive`) code +
      their Python/Go/TS tests. Kept singular `set_vt` (manual SetVT).
- [x] Fixed the stale `test_maintenance_grace_period_for_just_expired_queue`
      (renamed to `..._reaps_expired_exclusive_queue_no_grace`; asserts the
      no-grace behaviour commit `3b1788d` introduced).
- [x] Updated `CLAUDE.md` architecture notes (connection-level actors).
