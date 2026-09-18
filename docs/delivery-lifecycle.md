# Delivery lifecycle

The SQL ownership token, queue generation and client resource lifetime define the same boundaries in both clients.

## Ownership

A delivery is `(queue name, message ID, consumer token)`. A superseded handler may still be running, but its settlement cannot affect the new delivery's tracking or heartbeat. A heartbeat result applies only to the exact registration that initiated it. Scheduling state is separate from the authoritative live registry.

The first ack, nack, release or transactional ack owns settlement and completion. Completion removes runtime tracking even when SQL fails: the operation has ended and its lease is allowed to expire. Concurrent terminal operations do not issue competing SQL. Go subsequent terminal calls return `ErrLeaseLost`; TypeScript reports already processed. Applications should handle a failed settlement explicitly instead of assuming a successful business outcome.

A queue has a UUID generation. Consumers bind to it at startup (from CreateQueue's result or a metadata query). A replacement queue with the same name is a new resource; old consumers and keepalives cannot operate on it. Expired exclusive queues remain dead before the reaper runs. Delete/recreate is deliberate; there is no implicit revival or catch-up of publications missed while expired.

## Handlers and cancellation

A handler returning without explicit settlement auto-acks only while its context/signal remains live. A cancelled return, panic (Go), or exception (TypeScript) auto-nacks. Explicit Ack can report completed work even during cancellation. Only buffered, never-delivered messages are automatically released as unattempted work.

Cancellation is advisory. It cannot undo external side effects or stop a handler that ignores it. Use idempotency keys or application-level fencing for such effects. With caller-owned SQL transactions, commit/rollback remains the caller's responsibility; transactional Ack ends auto-extension, and rollback leaves the original delivery available after lease expiry.

## Connection shutdown

1. Enter draining and reject new publishers/consumers.
2. Stop fetching, release buffered deliveries, and cancel running handlers.
3. Continue settlement, queue keepalive and message renewal while deliveries drain.
4. At completion or the overall deadline, stop background I/O and close owned resources.

Concurrent Close calls await the same shutdown. Go uses `WithShutdownTimeout`; its default of zero allows unlimited handler drain. Set a timeout for a bounded deployment shutdown. TypeScript uses positive `shutdownTimeoutMs` (default 30 seconds) for both consumer Stop and Connection Close. A standalone Go Consumer.Stop waits for explicit settlement; Connection.Close can terminate its internal drain at the configured deadline.

At a forced deadline, active work is abandoned without reducing delivery attempts. Its database lease expires normally. Late fetch responses may only be released or left to expire; they cannot register heartbeats or enter a stopped consumer's buffer. A timed-out or cancelled fetch can have an ambiguous commit, so not every claimed row can necessarily be released immediately.

Clients bound internal queries. TypeScript destroys a timed-out pooled socket rather than merely rejecting a Promise. Go passes cancellation/deadlines through pgx and joins actor flushes. An application-supplied pool must honor cancellation, and application handlers may outlive a forced close if they ignore cancellation.

## Renewal and notification recovery

Heartbeat SQL does not wait for contended row locks. Busy rows are retried within the last confirmed lease budget; unrelated rows can renew immediately. Missing/wrong-token/expired deliveries are lost. Network failures remain retryable only until the confirmed deadline, at which point cancellation/fatal signalling occurs. A fresh wall-clock check after locking prevents old transaction timestamps from reviving leases.

One Go actor loop owns each schedule and uses a single ordered command channel. One TypeScript notification loop owns its LISTEN session and reconciles the current subscriptions after every connection attempt. Failed acquisition or LISTEN setup retries with bounded backoff while subscribers remain. Polling remains the correctness fallback for notifications.

## Publication and maintenance

Automatic publication retry is limited to aborted transactions (`40001`, `40P01`). A disconnect after commit has an unknown outcome and is returned to the caller. An application retry may publish twice; no broker can make arbitrary external effects exactly once.

Installations must schedule `postgremq.pmq_maintenance_fast()` and bounded `postgremq.cleanup_completed_messages(retention_hours, batch_size)` calls. Orphan payload collection preserves references from every queue and the DLQ. See [SQL operations and schedules](../mq/README.md).

## Acceptance checks

- SQL tests cover strict expiry, queue generations, nonblocking heartbeat contention, wall-clock expiry and bounded retention with lagging queues/DLQ.
- Go and TypeScript tests cover stale deliveries/results, shutdown settlement, cancelled handlers, late fetches and notification recovery.
- `POSTGREMQ_SOAK_SECONDS=300 go test -race -run TestProductionTopologySoak -count=1 -timeout=7m -v` in `postgremq-go` runs 20 topics, 100 queues, 400 consumers (2–6 per queue), 10 publications/sec and 1/16/256 KiB payloads with maintenance. It checks exact delivery counts in a healthy run and that payload storage drains to zero.

The completed five-minute run and full-suite results are recorded in the [validation report](production-readiness-fixes-2026-09-17.md). The topology test uses immediate acknowledgements and zero-hour cleanup retention to verify delivery and reclamation; it does not model application handler cost or long-term storage growth. Deployment capacity must account for actual payloads, fan-out, handler duration and retention.
