# Production-readiness fixes — 17 September 2026

All eleven findings in the [original review](production-readiness-review-2026-09-17.md) have implementation fixes. The design remains a shared-payload PostgreSQL queue with transactional fan-out. The changes establish explicit ownership and lifetime boundaries rather than adding special-case recovery states.

| Finding | Resolution |
|---|---|
| R1: Go settlement rejected during close | Separate draining from final closure; settlement and renewal remain usable during drain. |
| R2: old delivery removes new heartbeat | Track `(queue, ID, token)` consistently in both clients. Only one terminal operation owns settlement. |
| R3: late Go flush resurrects entries | Keep an authoritative live registry outside the heap; apply only to the original live entry. Registration and deregistration share one FIFO. |
| R4: cancelled handler auto-acks | Cancelled returns and handler failures auto-nack. Explicit Ack still reports successful work during cancellation. |
| R5: TypeScript releases running work | Release only buffered deliveries. Running work drains or is abandoned at the configured deadline without decrementing attempts. |
| R6: late fetch creates orphan heartbeat | Late results cannot enter a stopped consumer or register renewal. Pending queue binding cannot initiate a new claim after Stop. |
| R7: expired queue resurrection | Strict wall-clock expiry, fatal consume errors and deliberate recreation. Queue generations fence old consumers/keepalives from replacements. |
| R8: contended heartbeat blocks all work | Nonblocking row locks; explicit busy results; deadline checks after locking; client I/O bounded by the lease budget. Single-message extension also uses NOWAIT. |
| R9: payloads grow forever | Bounded completed-delivery and orphan-payload collection, reference indexes, explicit schedules, and retention of lagging queue/DLQ references. |
| R10: shutdown cannot cancel/join I/O | Bounded fetch/cleanup contexts, cancellation of connection I/O, joined actor flushes, disabled cancellation select after first use, and consumer deregistration. |
| R11: TypeScript reconnect stops retrying | One notification loop owns acquisition, subscription reconciliation, retry and teardown. |

Additional fixes include safe-integer message ID checks, numeric option validation, retrying publish only after confirmed transaction abortion, preserving keepalive after failed deletion, synchronous handler bootstrap errors, and awaiting test-container cleanup instead of forcing Jest to exit. SQL/client documentation and example signatures now match the implementation.

The [lifecycle contract](delivery-lifecycle.md) explains intentional behavior changes. [SQL operations](../mq/README.md) gives installation and maintenance commands. Schema changes are applied directly to both identical initial-install SQL files; no compatibility migration was added.

## Final validation

These results supersede the earlier mixed full-suite/targeted-run counts. Validation used the current working tree, local PostgreSQL 15 testcontainers, Go 1.26.5, Node 24.16.0 and Python 3.12. The CI version matrix was not run locally.

| Check | Working directory and command | Result |
|---|---|---|
| Go race suite | `postgremq-go`: `go test -race -count=1 -timeout=3m ./...` | Passed: 165 tests/subtests; examples compile. The opt-in topology test is executed separately below. |
| Go vet | `postgremq-go`: `go vet ./...` | Passed. |
| Go lint | `postgremq-go`: `golangci-lint run --timeout=5m` | Passed: zero issues (golangci-lint 2.12.2). |
| SQL | `mq`: `python3 -m pytest tests/tests.py -q` | Passed: 82 tests, zero skipped. Locally, Python was `/opt/homebrew/opt/python@3.12/libexec/bin/python3`. |
| TypeScript build | `postgremq-ts`: `npm run build` | Passed. |
| TypeScript full suite | `postgremq-ts`: `npm test -- --runInBand --detectOpenHandles` | Passed: 14 suites, 205 tests, zero skipped. Normal process exit, no open handles reported. |
| TypeScript coverage | `postgremq-ts`: `npm run test:coverage -- --runInBand --detectOpenHandles` | Passed: 14 suites, 205 tests, zero skipped; lines 88.98%, statements 87.72%, functions 94.41%, branches 77.60%. All configured thresholds passed; no open handles reported. |
| CLI and installation | `cmd/postgremq`: `go test -count=1 -timeout=3m ./...` | Passed: 11 tests/subtests, including fresh installation. |
| Topology acceptance | `postgremq-go`: `POSTGREMQ_SOAK_SECONDS=300 go test -race -run '^TestProductionTopologySoak$' -count=1 -timeout=7m -v` | Passed: five minutes, 3,000 publications, 15,000 deliveries. |
| Source consistency | Repository root: `git diff --check`, `cmp mq/sql/latest.sql mq/migrations/000001_initial_schema.up.sql`, `gofmt -l postgremq-go` | Passed; identical schemas and no Go formatting differences. |

The topology run used 20 topics, 100 queues, 400 consumers (2–6 per queue), 10 publications/sec, and 1/16/256 KiB JSON payloads. It checked every expected delivery, found no duplicate deliveries in this healthy run, drained payload storage to zero, and verified listener subscription counts returned to zero. Handlers acknowledged immediately; maintenance ran every 100 ms with zero-hour retention to exercise reclamation within the run. This measures the queue's delivery/lifecycle behavior, not application handler performance or multi-day storage growth.

Both formerly skipped TypeScript tests now execute: competing consumers use coordinated claims without abandoned iterator reads, and unlimited retries use explicit claims to prevent prefetch from changing the attempt under inspection. Lint cleanup removed unused refactor code and made scheduler interface conformance explicit. No runtime guard layers were added to address validation failures.

Raw output and machine-readable Go/Jest results are in the local [validation artifacts](/tmp/postgremq-final-validation.Yw9uI6). The table above records the results even if temporary artifacts are later removed.

## Remaining work

**No implementation fixes or validation tasks from this review remain outstanding.** All eleven findings and the additional hardening items are addressed. The changes remain uncommitted for review; no migration is needed for the first installation.

Before sending application traffic, deployment setup must install the schema, schedule maintenance, configure the connection pool and shutdown budget, and handle redelivery in application side effects. These are concrete integration requirements in the [lifecycle contract](delivery-lifecycle.md) and [SQL operations guide](../mq/README.md). For the tested fan-out, the operations guide now specifies fast maintenance every second and 1,000-row retention batches every ten seconds, providing cleanup capacity above the expected 3,000 delivery rows/minute.

Evidence is limited to the stated local environment and workload. Production hardware capacity, actual handler cost and the application's payload distribution have not been measured here.
