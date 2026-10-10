# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
All components (SQL schema, Go, TypeScript and Rust clients, CLI) share one
version number; see [RELEASE.md](./RELEASE.md).

## [Unreleased]

First public release, to be published as 0.2.0.

### SQL (`mq`)

- Schema and functions in a dedicated `postgremq` schema, installed from
  `mq/sql/latest.sql` or as golang-migrate migrations (embedded in the Go `mq`
  module). Both record the schema version, so either install can be upgraded
  by later migrations. PostgreSQL 15+, no extensions.
- Topics with fan-out to any number of queues through a distribution trigger.
- Visibility-timeout leases with a per-delivery ownership token; ack, nack
  (optionally delayed), release, and lease extension, single or batched across
  queues (`set_vt_batch_multi`).
- Delivery attempt limits with a dead letter queue, plus DLQ listing, requeue
  and purge.
- Delayed delivery (`deliver_after`).
- Message groups: ordered, one-at-a-time delivery per group key within each
  queue, in publish commit order.
- Queue generations, so a queue re-created under the same name is a distinct
  resource.
- Exclusive (temporary) queues kept alive by their owner and reaped at expiry.
- Notifications on `pmq:t:<topic>` and `pmq:q:<queue>`;
  `get_next_visible_time` for timed wake-ups.
- Maintenance and retention functions: `pmq_maintenance_fast`,
  `cleanup_completed_messages`, `cleanup_unreferenced_messages`.
- `queue_metrics()` for queue depth and age, usable by a least-privilege
  scraper role.

### Go client (`postgremq-go`)

- Connection over a pgx v5 pool; publish (also on a caller's transaction),
  channel-based and handler-based consumers.
- Connection-level batched lease renewal and exclusive-queue keep-alive.
- Shared LISTEN session with reconnect and polling fallback.
- Queue-loss teardown (`Consumer.NotifyClose`, `WithQueueFatalHandler`,
  `ErrQueueGone`), graceful shutdown with a configurable deadline, retry with
  exponential backoff for transient errors.
- Schema migrations (`Migrate`, `GetMigrationStatus`), up only, to the latest
  embedded version.
- Optional OpenTelemetry metrics.

### TypeScript client (`postgremq`)

- Connection over a node-postgres pool; publish (also on a caller's
  transaction), async-iterator and handler-based consumers.
- Connection-level batched lease renewal and exclusive-queue keep-alive.
- Shared LISTEN session with reconnect and polling fallback.
- Schema migrations (`migrate`, `getMigrationStatus`), compatible with the Go
  client and the CLI.
- Queue-loss teardown (`onClose`, `onQueueFatal` / `'queueFatal'`,
  `QueueFatalError`), graceful shutdown, retry for transient errors.
- Optional OpenTelemetry metrics.

### Rust client (`postgremq` crate)

- sqlx 0.9 and Tokio; `publish` / `publish_tx`, `Stream` consumers and handler
  consumers; `ack` / `ack_tx` / `nack` / `release` / `extend`.
- Connection-level batched lease renewal and exclusive-queue keep-alive.
- Dedicated LISTEN session with reconnect and polling fallback.
- Queue-loss teardown (`Error::QueueGone`, `on_queue_fatal`), graceful
  shutdown, retry for transient errors, maintenance passthroughs.
- Schema migrations (`migrate`, `migration_status`), compatible with the Go
  client and the CLI.
- Optional OpenTelemetry metrics behind the `otel` feature.

### CLI (`cmd/postgremq`)

- `migrate` and `status` commands for the schema migrations.

### Observability

- A shared client metrics contract (`docs/observability.md`,
  `observability/client-contract.json`), a tested OpenTelemetry Collector
  configuration, and runnable metrics examples for every client.

[Unreleased]: https://github.com/slavakl/postgremq/commits/main
