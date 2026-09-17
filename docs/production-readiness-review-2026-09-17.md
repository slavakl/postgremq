> This is the original pre-fix review. The completed fixes and validation are recorded in [production-readiness-fixes-2026-09-17.md](production-readiness-fixes-2026-09-17.md). The observations below are retained as the review record.

**PostgreMQ production-readiness review — 17 September 2026**

**Verdict at review time, before fixes: not ready for production.** The database design is viable for the stated workload (about 10 publications/second, 20 topics, 100+ queues, 2–6 competing consumers per queue). The blockers are delivery lifecycle and failure recovery, not a need to replace the architecture or increase throughput.

This review covers the SQL schema/functions, Go and TypeScript runtime implementations, the uncommitted/untracked actor and consumer changes, and relevant tests. Fresh installation is the deployment model: incremental migration compatibility is deliberately excluded. No implementation fixes had been made at the time of this review.

P1 below means fix before production; P2 means a concrete reliability defect that also needs resolution, but has a narrower trigger or less severe immediate consequence. Findings include both existing defects and defects introduced or amplified by the working-tree refactor.

**Validation performed.** The existing Go suite passed with `go test -race -count=1 -timeout=3m ./...` (main package: 17.194 seconds); `go vet ./...` passed. All 76 Python SQL tests passed. TypeScript compiled. The first TypeScript run passed 11 suites/164 tests and failed one entire suite because its testcontainer could not resolve a host port; that suite passed separately on rerun (25 passed, one skipped). Across the successful executions: 189 TypeScript tests passed and two were skipped. This is not a claim that the first aggregate run was green. The two SQL installation files are byte-identical, and `git diff --check` passed.

Additional reproductions ran against a separate PostgreSQL 15.10 container and a copy of the current Go tree. Expected-behavior regression assertions fail on the defects below; the original test suites do not cover these interleavings. TypeScript timing/mocking reproductions are identified explicitly. Evidence is in [/tmp/postgremq-audit.9c0xg1](/tmp/postgremq-audit.9c0xg1). The disposable database is removed after review. Existing user containers were not stopped or altered.

**R1 — P1: connection shutdown prevents the Go drain from settling or extending messages.**

[connection.go:149](/Users/slavak/repositories/postgremq/postgremq-go/connection.go:149) closes `closedFlag` before stopping consumers. [retry.go:88](/Users/slavak/repositories/postgremq/postgremq-go/retry.go:88) rejects all operations once that flag is closed. Ack, nack, release, and keepalive use that helper; `SetVTBatchMulti` also checks closed state directly. Consequently, keeping the background actors running through drain does not keep their SQL operations usable.

Reproduction: receive a message, start `Connection.Close`, wait for the message's cancellation signal, then acknowledge with a fresh background context. Ack returns `ErrConnectionClosed`, and the database row remains `processing` after Close returns. This is an ordinary deployment/shutdown path, with a healthy database. Buffered-message releases fail through the same guard; successful work can be redelivered.

Fix: distinguish accepting-new-work, draining, and fully-closed states. Reject new publishers/consumers when draining starts, but allow existing deliveries and background lease/keepalive work to settle until the drain finishes or its deadline expires. Make that state transition a shared contract across both clients.

Evidence: `TestAuditCloseAllowsSettlement` in [audit_regression_test.go](/tmp/postgremq-audit.9c0xg1/postgremq-go/audit_regression_test.go); [go-regressions.log](/tmp/postgremq-audit.9c0xg1/go-regressions.log).

**R2 — P1: old deliveries can remove newer deliveries' heartbeat state in both clients.**

The SQL ownership token is correct, but the connection-level runtime deregisters only by `(queue, message_id)`: [Go extender.go:93](/Users/slavak/repositories/postgremq/postgremq-go/extender.go:93), [TypeScript connection.ts:1809](/Users/slavak/repositories/postgremq/postgremq-ts/src/connection.ts:1809). After a lease expires and another consumer on the same Connection receives the message, the new token replaces the old entry. When the old handler eventually attempts ack/nack—even if the database correctly rejects its token—its completion removes the newer delivery's heartbeat.

TypeScript also tracks a consumer's in-flight messages by message ID alone, so this problem can affect its drain accounting when that consumer receives the same ID again.

Fix: settlement must carry and match the delivery token/generation. Use a generation-aware registry, preserve cancellation/accounting for old handlers, and remove an entry only if it belongs to the settling delivery. The same rule must apply to late asynchronous results and cancellation callbacks.

Evidence: `TestAuditOldSettlementKeepsNewLease`; TypeScript reproduction prints `old-settlement-removes-new-heartbeat: true` in [ts-repros.log](/tmp/postgremq-audit.9c0xg1/ts-repros.log). The shared connection-level design makes this particularly relevant to the pending refactor.

**R3 — P1: Go applies stale extension results after settlement or replacement.**

[extender.go:124](/Users/slavak/repositories/postgremq/postgremq-go/extender.go:124) removes due entries from the heap while the asynchronous SQL call runs. A concurrent deregistration sees no entry and does nothing. [extender.go:148](/Users/slavak/repositories/postgremq/postgremq-go/extender.go:148) then unconditionally reinserts the old entry on success or error. A result can resurrect a settled entry or overwrite a newer token registered while the old call was running.

Fix: keep authoritative generation/tombstone state independent of the scheduling heap. Apply a result only if its exact delivery generation is still active. Also preserve register/settle ordering: the actor's separate registration and deregistration channels do not guarantee cross-channel ordering. TypeScript already checks entry identity when applying its flush result; Go needs the equivalent, plus R2's token-aware deregistration.

Evidence: deterministic failures `TestAuditExtenderDeregisterDuringFlush` and `TestAuditExtenderOldFlushOverwritesNewLease`. These are working-tree actor-refactor defects, not data races that `-race` can detect.

**R4 — P1: cancellation followed by normal handler return can acknowledge unfinished work.**

[Go handler_consumer.go:143](/Users/slavak/repositories/postgremq/postgremq-go/handler_consumer.go:143) and [TypeScript handler-consumer.ts:148](/Users/slavak/repositories/postgremq/postgremq-ts/src/handler-consumer.ts:148) auto-ack any normal return without settlement, including a handler that returns because shutdown cancelled its context/signal. The documentation encourages handlers to observe cancellation and return promptly, but return alone is interpreted as successful processing.

Reproduction: the Go handler signals that it started, waits for `ctx.Done()`, and returns without performing its work. Calling `HandlerConsumer.Stop()` while the Connection remains open results in database status `completed`. This is independent of R1: using consumer Stop directly avoids the closed-connection error and exposes the accidental acknowledgement.

Fix: define cancellation settlement explicitly. A conservative rule is no automatic acknowledgement after cancellation unless the handler explicitly reported success/acked. Consider an error-returning Go handler or mandatory explicit settlement. Existing applications could work around this with explicit nack/release on cancellation, but that requirement must be consistent with examples and automatic handler behavior.

Evidence: `TestAuditCancelledHandlerDoesNotAckUnfinished` (real PostgreSQL); the TypeScript equivalent is apparent in the matching auto-ack branch, rather than separately asserted in that test.

**R5 — P1: TypeScript releases deliveries while their handlers are still executing.**

[consumer.ts:487](/Users/slavak/repositories/postgremq/postgremq-ts/src/consumer.ts:487) waits a hardcoded two seconds during stop, then calls `release()` on all remaining in-flight messages. `HandlerConsumer.stop()` waits for handlers separately, but that does not prevent the underlying consumer from releasing their messages first.

Reproduction: a handler remains active beyond cancellation; start stop and inspect the database after 2.3 seconds. The handler is still active, but its message is already `pending` and `delivery_attempts` has been decremented to zero. Another consumer can claim it while the old handler continues side effects. Release also misclassifies attempted work as unattempted, undermining retry limits during repeated shutdowns.

Fix: release only buffered, never-delivered messages immediately. For running handlers, coordinate a configurable drain deadline and settlement ownership; do not mark still-running work as unattempted. Define the forced-shutdown behavior explicitly, since cancelling a signal does not terminate a JavaScript function.

Evidence: real PostgreSQL reproduction in [ts-repros.js](/tmp/postgremq-audit.9c0xg1/ts-repros.js), `stop-releases-still-running-handler` output.

**R6 — P1: a TypeScript fetch completing after stop can strand a message under indefinite auto-extension.**

Stop abandons waiting for an outstanding fetch after 30 seconds. However, [consumer.ts:355](/Users/slavak/repositories/postgremq/postgremq-ts/src/consumer.ts:355) still appends its returned messages to the buffer and registers them with the connection-level extender without checking whether this consumer already finished stopping. If the Connection remains alive, the extender can keep renewing a delivery that no running consumer can process. Calling stop again returns immediately because `running` is false.

Fix: gate the post-await fetch result on a consumer generation/terminal state. A result belonging to a stopped consumer must be released or left to expire, never newly registered for extension or delivery. Join/cancel outstanding I/O where possible and retain a cleanup path for late responses.

Evidence: deferred-fetch reproduction in `ts-repros.js`; output is `running:false, buffer:1, liveHeartbeats:1`. The reproduction shortens the static 30-second drain timeout to 20 milliseconds; it does not change the result-processing code. The move to connection-level extension turns this late-result bug into possible indefinite retention.

**R7 — P1: an expired exclusive queue can be revived after silently missing publications.**

Publish and consume treat expiry as immediate death, but [latest.sql:945](/Users/slavak/repositories/postgremq/mq/sql/latest.sql:945) renews any existing exclusive queue, with no live-deadline predicate. Re-declaring an existing exclusive queue also refreshes its deadline. If the keepalive was late but the maintenance reaper has not run yet, the queue can resume with a hole in its event stream and no fatal signal.

Reproduction: expire an exclusive queue, publish an event, call `extend_queue_keep_alive_multi`, then publish another event. Renewal succeeds and consumption returns only the second event. The first event was omitted during distribution.

Fix: choose one consistent policy. Under the documented strict-expiry policy, an expired generation must remain dead; keepalive must refuse it and consumption must surface expiry as fatal even before physical reaping. Recreating it should be a deliberate new generation. If a grace period is desired instead, publication must continue during that same grace period—renewal alone cannot repair dropped fan-out entries.

Evidence: [sql-repros.py](/tmp/postgremq-audit.9c0xg1/sql-repros.py), `expired_queue_revived` in [sql-repros.log](/tmp/postgremq-audit.9c0xg1/sql-repros.log).

**R8 — P1: one blocked heartbeat batch can expire unrelated deliveries, then report expired leases as successful extensions.**

[latest.sql:1410](/Users/slavak/repositories/postgremq/mq/sql/latest.sql:1410) locks every selected row with blocking `FOR UPDATE`. Both clients allow one extension flush at a time per Connection, with no per-flush deadline. A row held by an application transaction can block extension of unrelated messages/queues. Deterministic ordering prevents some extender-versus-extender deadlocks; it does not bound lock waits against application transactions.

The query also tests and sets deadlines with `NOW()`, which is transaction-start time. After a sufficiently long wait, it can return a new deadline that is already in the past. [PostgreSQL time semantics](https://www.postgresql.org/docs/15/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)

Reproduction: claim two messages for two seconds, lock one in a separate transaction, start their batch extension for another two seconds, and hold the lock for 2.4 seconds. Both original leases expire while the extension is blocked. After unlocking, both returned extension deadlines are already expired.

Fix: bound heartbeat I/O by the remaining lease budget, isolate/split contended work so it cannot starve every queue, and evaluate live-lease/deadline semantics against actual time after acquiring locks. Do not simply add `SKIP LOCKED` while preserving “omitted means lease lost”: contention must not masquerade as lost ownership.

Evidence: `expired_while_batch_blocked: 2` and `extension_returns_already_expired` in `sql-repros.log`.

**R9 — P1: routine cleanup never collects the shared payload rows.**

[latest.sql:1549](/Users/slavak/repositories/postgremq/mq/sql/latest.sql:1549) deletes completed `queue_messages`, but not unreferenced `messages`. DLQ purge and expired-queue deletion likewise leave payloads. `clean_up_topic` deletes every payload on a topic and cascades into active deliveries, so it is a destructive purge rather than safe retention maintenance.

Reproduction: publish, consume, ack, run `cleanup_completed_messages(0)`; one payload remains and zero delivery rows remain. At the stated 10 publications/second, continuous operation creates up to 864,000 new payloads/day.

Fix: add bounded, scheduled collection of payloads that have no active/completed delivery references and no DLQ references, with an explicit retention policy. Index the reference checks: current delivery/DLQ primary keys begin with queue name, not message ID. Validate cleanup while other queues are lagging and while DLQ requeue runs. Make installation/runbook documentation name the actual maintenance functions and required schedules.

Evidence: `gc_payload_and_delivery_counts (1, 0)` in `sql-repros.log`.

**R10 — P2: Go's shutdown timeout does not bound blocked internal I/O, and actor shutdown does not join its flush.**

[consumer.go:83](/Users/slavak/repositories/postgremq/postgremq-go/consumer.go:83) uses an uncancellable background context for fetching and cleanup. The configured timeout bounds only waiting for consumer Stop; [connection.go:198](/Users/slavak/repositories/postgremq/postgremq-go/connection.go:198) then calls pool.Close, which waits for outstanding acquired connections. A blocked fetch can therefore hold Close indefinitely after the nominal timeout.

Reproduction: lock an exclusive queue row so its consume-side refresh blocks. With `WithShutdownTimeout(50*time.Millisecond)`, Close is still blocked after 400 milliseconds and only finishes when the database lock is released.

There are two related lifecycle defects in the pending Go refactor: [actor.go:81](/Users/slavak/repositories/postgremq/postgremq-go/actor.go:81) joins the actor loop but not its dispatched flush goroutine; and [consumer.go:179](/Users/slavak/repositories/postgremq/postgremq-go/consumer.go:179) continues selecting from an already-closed cancellation channel while draining, causing a busy loop until outstanding work settles.

Fix: use separate cancellable fetch and bounded drain contexts, join actor I/O goroutines, enforce an overall shutdown budget, and disable the cancellation select case after handling it once. Preserve the ability to finish normal settlement during the graceful portion of shutdown (R1).

Evidence: [go-timeout.log](/tmp/postgremq-audit.9c0xg1/go-timeout.log); `TestAuditActorStopsAfterFlushJoins` / [go-actor-join.log](/tmp/postgremq-audit.9c0xg1/go-actor-join.log). Busy looping is a source-level finding, not a measured CPU benchmark.

**R11 — P2: TypeScript notification recovery stops after the first failed reconnect.**

[connection.ts:625](/Users/slavak/repositories/postgremq/postgremq-ts/src/connection.ts:625) logs a failed restart but does not schedule another attempt. If the database remains unavailable for the first reconnect, an otherwise live consumer can remain on polling indefinitely after recovery. Initial listener-start failures have a similar logging-only path.

Fix: keep retrying with bounded backoff while there are subscribers and the Connection is open; stop retries only when that lifecycle ends. Reset all channel subscription state for each connection generation and cover failure during LISTEN setup, not just errors on an established listener.

Evidence: mocked listener failure in `ts-repros.js`, with the reconnect base delay shortened: `attempts:1, scheduled:false`. Polling means this is primarily latency/load degradation rather than message loss.

**Additional hardening before release.** These are source-level follow-ups, not additional independently reproduced P1 findings:

- TypeScript lifecycle checks are inconsistent: `consume`/`consumeHandler` check `connected` but not `isShuttingDown`, so they can admit consumers after Close took its stop snapshot. Concurrent close/stop calls should await the same completion promise. Stopping a never-started Consumer currently returns without unregistering it.
- Go retains stopped consumers in `Connection.consumers`; normal Stop does not remove them. Automatic fatal teardown does not close listener handles through the ordinary Stop path unless the caller later stops/closes explicitly. Test long-running queue/consumer churn for retained consumers and subscriptions.
- Keepalive actors permanently drop entries after a short transient failure budget. For queues without a polling consumer, recovery before queue expiry does not automatically resume those heartbeats. Decide how clients observe and recover from this without relying on a later consumer fetch.
- TypeScript uses `number` for BIGINT IDs without safe-integer validation. This is not a practical time-to-exhaustion blocker at 10 messages/second, but the API should either use bigint/string or explicitly reject values above Number.MAX_SAFE_INTEGER.
- Automatic publish retry can duplicate publications after an ambiguous commit; publish is not idempotent despite a contrary code comment. Document the contract and use application idempotency keys where duplicates matter. SQL transaction support does not make external side effects exactly once.
- Bound and validate configuration values consistently (positive keepalive intervals; finite/integer batch/concurrency values in TypeScript). Clarify that temporary queues are lease-expiring queues, not enforced RabbitMQ connection-exclusive ownership.
- Documentation needs reconciliation with code: several SQL names/signatures, ID types, notification channels, and shutdown guarantees are stale. A passing example/install smoke test is more useful than adding more narrative claims.

**What is already sound.** Transactional fan-out and per-delivery SQL ownership tokens are appropriate foundations. The SQL consume path uses row locking/SKIP LOCKED; DLQ retirement and requeue are transactional; completed rows are excluded from consume indexes; typed SQLSTATE errors and explicit transaction APIs are useful. The design deserves continued development. These findings call for repair of state machines and failure semantics, not a rewrite of the queue.

**Recommended acceptance gate.** First establish common contracts for delivery generations, settlement, cancellation, expiry, and connection drain. Fix R1–R9 and the relevant P2 paths, then preserve the reproductions as regression tests. Run all existing suites plus tests with real disconnects, held row locks, old/new deliveries on one Connection, consumer/queue recreation, and shutdown during fetch/ack/extension. Finally run a sustained test at the actual topology and varied payload sizes while cleanup and DLQ maintenance run; verify payload storage stabilizes and consumer/connection resources return to baseline after churn. The short throughput comparison from the earlier review is not that acceptance test.

There is no migration prerequisite for this first deployment. A production-readiness claim should wait for the failure-path fixes and those acceptance checks.
