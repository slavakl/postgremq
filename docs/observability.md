# Observability

PostgreMQ exposes queue state through `postgremq.queue_metrics()` and client
activity through optional OpenTelemetry metrics. SQL is the authority for queue
state across all languages and application replicas. Clients record observations
that require application context. Neither client creates an SDK, exporter,
collection timer or global provider; your application owns those resources.

## Run the complete example

From the repository root, with Docker, Go, and Node.js 18.19+ installed:

```sh
docker compose -f observability/compose.yaml up -d
```

This starts an isolated PostgreSQL database on localhost:55432 and Collector
Contrib **0.147.0**, with OTLP/HTTP on localhost:4318 and a Prometheus endpoint on
localhost:8889. The SQL schema and demo queues are installed automatically.
Demo credentials are only for this local database.

Run the clients in separate terminals, or sequentially:

```sh
cd postgremq-go
go run ./examples/metrics
```

```sh
cd postgremq-ts
npm ci
npm exec -- ts-node examples/metrics.ts
```

Each example publishes and handles a message, drains the connection, and flushes
its application-owned meter provider. They use the public client APIs. Set
`DATABASE_URL` and `OTEL_EXPORTER_OTLP_ENDPOINT` to override their defaults.

```sh
curl http://localhost:8889/metrics
```

Allow up to 15 seconds for queue metrics to appear. Names are translated to
Prometheus syntax: for example `messaging.client.operation.duration` becomes
`messaging_client_operation_duration_seconds`, with `_bucket`, `_count`, and
`_sum` series. Prometheus can scrape this endpoint; no Prometheus server is
required to run the example. Stop and remove the demo database with:

```sh
docker compose -f observability/compose.yaml down
```

## Database metrics

```sql
SELECT * FROM postgremq.queue_metrics();
```

The function returns one row per queue, including empty and expired exclusive
queues. Every row uses the same statement timestamp and database snapshot. It
runs with caller privileges, is read-only, and never reads message payloads.
The existing `get_queue_statistics()` API remains available for raw status counts.

| SQL column | OTel metric | Meaning |
|---|---|---|
| `active` | `postgremq.queue.active` | 1 for durable queues and unexpired exclusive queues; otherwise 0 |
| `ready` | `postgremq.queue.messages.ready` | Pending or expired processing deliveries eligible for another attempt; 0 for expired exclusive queues |
| `delayed` | `postgremq.queue.messages.delayed` | Pending deliveries whose visibility time is in the future |
| `processing` | `postgremq.queue.messages.processing` | Processing deliveries whose visibility timeout has not expired, including the final allowed attempt |
| `exhausted` | `postgremq.queue.messages.exhausted` | Pending/processing deliveries whose visibility timeout has expired and retry limit has been reached, awaiting DLQ maintenance |
| `dead_letter` | `postgremq.queue.messages.dead_letter` | Deliveries currently in the DLQ |
| `oldest_ready_age_seconds` | `postgremq.queue.oldest_ready_age` | Seconds since the earliest currently eligible visibility time (`vt`); 0 when nothing is ready |

All are gauges, with `queue_name` and `topic_name` attributes. Count units are
`{message}`, age is `s`, and active is `1`. `ready` describes eligibility; a row
may still be locked by an in-progress transaction. `delayed`, `processing`,
`exhausted` and DLQ counts describe retained state even in an expired queue;
`active` identifies that queue's unavailable state. They are not counts of active
application handlers.

Ready age measures waiting in the **current** delivery cycle. It excludes the
scheduled delay and resets when release/nack changes `vt`; it is not age since
original publication. There is no sequence-derived "total published" counter.
Rolled-back publications are not counted. Completed retention is excluded so
routine collection does not require counting retained completed deliveries.

The query aggregates active delivery rows and DLQ rows once across all queues.
It can use the existing partial index for pending/processing deliveries; its
cost still grows with backlog and DLQ size. Measure scrape duration with your
backlog, retain a query deadline, and adjust collection frequency as needed.
It adds no counters, triggers, payload processing, or writes to the queue hot path.

## Configure the Collector

[`observability/collector.yaml`](../observability/collector.yaml) is a complete,
tested configuration using the [SQL Query receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.147.0/receiver/sqlqueryreceiver). Supply:

- `POSTGREMQ_METRICS_DSN`: PostgreSQL DSN for a dedicated monitoring login. The
  example sets `connect_timeout=5` and `statement_timeout=5000` (milliseconds).
- `POSTGREMQ_DATABASE_ID`: A stable identity for this database deployment, unique
  among databases sharing the same telemetry destination. It becomes the SQL
  metrics' `service.instance.id`; the SQL service name is `postgremq`.

Provision a monitoring role through your normal credential management, then grant:

```sql
GRANT USAGE ON SCHEMA postgremq TO postgremq_metrics;
GRANT SELECT ON postgremq.queues, postgremq.queue_messages,
    postgremq.dead_letter_queue TO postgremq_metrics;
GRANT EXECUTE ON FUNCTION postgremq.queue_metrics() TO postgremq_metrics;
```

`queue_metrics()` is security-invoker, not security-definer. These table grants
are required even when execution is granted. The login needs no access to
`postgremq.messages` or application tables. Use your deployment's database TLS
settings. The demo publishes Collector ports only on loopback; configure network
access and OTLP authentication/TLS for a shared deployment.

Run **one active SQL scraper per database**, independently of application replica
count. Multiple collectors scraping the same database can duplicate these gauges.
Application replicas should each send their own client metrics, with their own
`service.name` and `service.instance.id`. Never overwrite application resource
identity with the SQL scraper's identity. The supplied configuration uses separate
SQL/client pipelines to preserve this distinction.

The SQL Query receiver's metrics support is alpha. Keep the Collector image pinned
and run the integration test before upgrades. The configuration exports Prometheus
metrics for a self-contained example. For an OTLP backend, replace the exporter
and reference it in both pipelines:

```yaml
exporters:
  otlphttp/backend:
    endpoint: ${env:OTEL_EXPORTER_OTLP_ENDPOINT}
```

The provided Prometheus exporter promotes resource attributes to labels. Keep
application resource attributes bounded, as well as queue/topic names.

## Client instrumentation contract, version 1

Pass a meter provider when constructing the connection:

```go
conn, err := postgremq.Dial(ctx, poolConfig,
    postgremq.WithMeterProvider(meterProvider))
```

```typescript
const connection = await connect({ connectionString, meterProvider });
```

Metrics are disabled when the provider is omitted. To use a global provider, pass
it explicitly. Close/drain the queue connection before flushing and shutting down
the provider. Neither client shuts down or flushes a supplied provider. They obtain
a library-scoped meter from it and create instruments; this is different from
constructing an SDK meter provider. The API-only runtime dependencies are separate
from SDK dependencies used by tests/examples. This follows [OTel library guidance](https://opentelemetry.io/docs/specs/otel/library-guidelines/#requirements).

To share an existing global provider explicitly:

```go
postgremq.WithMeterProvider(otel.GetMeterProvider())
```

```typescript
// import { metrics } from '@opentelemetry/api';
const connection = await connect({ connectionString, meterProvider: metrics.getMeterProvider() });
```

Omitting the option stays a no-op even if another library has configured a global
provider. Complete SDK and OTLP exporter setup is in the
[Go example](../postgremq-go/examples/metrics/main.go) and
[TypeScript example](../postgremq-ts/examples/metrics.ts).

Both clients use instrumentation scope `postgremq`, version `1`. The
`messaging.*` names follow the OTel messaging conventions (1.44.0, still in
Development); the shared PostgreMQ contract is versioned separately and is not
silently changed when dependencies update.

| Metric | Instrument / unit | Recording boundary |
|---|---|---|
| `messaging.client.operation.duration` | Histogram / `s` | One logical operation, including pool acquisition and internal retries |
| `messaging.process.duration` | Histogram / `s` | One handler callback, from invocation until return, throw or panic |
| `messaging.client.sent.messages` | Counter / `{message}` | One message per attempted publish SQL query, successful or failed |
| `messaging.client.consumed.messages` | Counter / `{message}` | Each delivery successfully decoded from a consume result |
| `postgremq.client.handlers.active` | UpDownCounter / `{handler}` | +1 at callback entry, -1 at callback exit |
| `postgremq.client.renewal.lost` | Counter / `{message}` | The automatic renewal actor retires a tracked delivery after missing ownership or exhausting its lease deadline |

Histogram boundaries in seconds are `0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25,
0.5, 0.75, 1, 2.5, 5, 7.5, 10`. Applications can override aggregation through SDK
views. Histogram counts provide operation/handler counts without duplicate counters.

Operation names are `publish`, `consume`, `ack`, `nack`, `release`, `extend`,
`extend_batch`, and `keep_alive`. Admin operations and notification reconnects
are not separate instruments in contract v1. Duration histograms record one
sample per logical operation, not per SQL attempt. Empty consume calls are
included. A duplicate local settlement rejected before the connection operation
is invoked produces no operation sample. Batch operations produce one sample per
batch, including batches with no eligible rows. Row-level renewal loss is reported
by the separate renewal-loss counter, since the SQL batch can succeed overall.

Common operation/processing/sent/consumed attributes:

- `messaging.system = postgremq`.
- `messaging.operation.name`: the operation listed above, or `process`.
- `messaging.operation.type`: `send` for publish, `receive` for consume, `settle`
  for ack/nack/release; `process`, `extend`, `extend_batch`, or `keep_alive` otherwise.
- `messaging.destination.name`: topic for publish, queue for single-queue
  operations/processing/consumed. Omitted for maintenance batches spanning queues.

Operation duration and the sent counter also have `postgremq.transaction`: true for caller-owned
publish/ack transactions. A successful transactional call means **the SQL operation
returned successfully**, not that the transaction committed. A failed call may
also have an ambiguous server outcome. These metrics are not committed-publication
counters; use SQL gauges for database state.

`error.type` is present on failed operation durations and failed send attempts,
using bounded categories:
`lease_lost`, `queue_not_found`, `validation`, `connection_closed`, `cancelled`,
`deadline_exceeded` (where the client exposes it), or `other`. Raw error text,
SQL, message IDs, tokens, and payloads are never metric attributes.

Sent counts follow the attempted-send definition in the [OTel conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#producer-metrics).
A send attempt begins when the client invokes its driver's publish query API.
Each completed attempt adds one message, with `error.type` if that attempt failed.
Internal retries count separately: an aborted first query followed by a successful
retry produces two sent messages (one failed, one successful) and one successful
operation-duration observation. A connection rejection or TypeScript payload
serialization failure before the query is invoked adds no sent messages. A driver
error can occur before the request reaches PostgreSQL; this is a client attempt
counter, not a wire-delivery or durable-commit count. Transactional attempts count
even if the caller later rolls back. Messages only prepared locally are not counted.

Consumed counts have `postgremq.redelivered` (boolean), based on delivery attempt
> 1. They include prefetch that may subsequently be released, and can count the
same logical publication multiple times. A Go partial batch counts its decoded
rows even when the receive also returns an error, with that operation's
`error.type` attached. They are recorded once per delivery, not again on processing. A response lost entirely cannot
be counted reliably by the client.

Processing duration excludes automatic settlement performed **after** the callback
returns. Explicit settlement awaited **inside** the callback is part of callback
time and also has its own operation duration. Returning normally has no error
attribute; panic/throw uses `handler_error`; returning with the message's stop
context/signal cancelled uses `cancelled`. Explicit nack is measured as a nack
operation, not inferred as a callback exception. These observations describe
callback execution, not business success or transaction commit.

Manual iterator/channel consumption has no automatic handler duration/active count:
the library cannot identify your processing boundary. Instrument that application
code directly. Other client metrics still work for manual consumers.

Renewal loss has only `messaging.system` and `messaging.destination.name`. It counts
retirement by the automatic renewal actor, not every lease-lost API error, and is
not emitted for an entry already deregistered when a batch finishes. It does not
count ordinary shutdown cancellation.

No distributed tracing or trace-header propagation is added by this metrics API.

## Tests

From the repository root:

```sh
.venv/bin/pytest mq/tests/tests.py observability/test_collector.py -v
```

The Collector test starts isolated Docker containers with the pinned image, runs
the exact shipped configuration using a restricted database login, verifies SQL
metric values and changes after a scrape, runs both client examples, and verifies
their exported OTLP metrics. It uses dynamic host ports and removes all containers
and networks on exit. Dependencies: `mq/tests/requirements.txt`, Go, and `npm ci`
in `postgremq-ts`. The dedicated observability CI job runs this test.

The normal Go and TypeScript suites include in-memory SDK tests. Both read
[`client-contract.json`](../observability/client-contract.json) to check names,
units, scope, buckets, operation types and an identical sequence of transactional publish/ack,
rollback, redelivery, empty receive and failed settlement. Additional tests verify
active handlers, panic/throw/cancellation, automatic renewal loss, and one logical
operation sample across a retried SQL publication, failed/successful sent counts,
and no-op instrumentation with an omitted provider. Provider-ownership tests
verify that closing a queue connection leaves the application provider usable.

Useful initial alerts are sustained ready age, exhausted deliveries awaiting
maintenance, nonzero DLQ, increasing renewal loss, and failed operations. Set
thresholds from your processing latency and retry policy.
