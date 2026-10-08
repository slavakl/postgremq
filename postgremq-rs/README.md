# postgremq

Rust client for [PostgreMQ](https://github.com/slavakl/postgremq), a message
queue that lives in PostgreSQL: topics fan out to queues, deliveries are
leased with a visibility timeout and fenced by a per-claim token, failed
deliveries retire to a dead letter queue, and `LISTEN`/`NOTIFY` wakes
consumers (polling is the fallback). Publishing and acknowledging can run
inside your own transaction.

The client follows the Go client's shape and the
[delivery lifecycle contract](https://github.com/slavakl/postgremq/blob/main/docs/delivery-lifecycle.md). It uses
`sqlx` (Postgres, Tokio, rustls) and needs the schema from
[`mq/sql/latest.sql`](https://github.com/slavakl/postgremq/blob/main/mq/sql/latest.sql)
installed in the database.

```rust,no_run
use postgremq::{Connection, ConnectionOptions, ConsumeOptions, PublishOptions, QueueOptions};

# async fn example() -> postgremq::Result<()> {
let conn = Connection::connect("postgres://localhost/app", ConnectionOptions::default()).await?;
conn.create_topic("orders").await?;
conn.create_queue("billing", "orders", QueueOptions::default().max_delivery_attempts(5))
    .await?;

conn.publish("orders", &postgremq::serde_json::json!({ "order_id": 7 }), PublishOptions::default())
    .await?;

let mut consumer = conn.consume("billing", ConsumeOptions::default()).await?;
while let Some(delivery) = consumer.next().await {
    let delivery = delivery?; // Err(QueueGone) ends the stream
    // ... process delivery.payload() ...
    delivery.ack().await?;
}
conn.close().await;
# Ok(())
# }
```

## Transactions

`publish_tx` and `Delivery::ack_tx` take the caller's connection (`&mut tx`
for a `sqlx::Transaction`). They are never retried: the caller owns the
transaction. `ack_tx` ends lease renewal once its statement completes; if the
transaction rolls back, the delivery is redelivered after its lease expires.

```rust,no_run
# async fn example(conn: postgremq::Connection, delivery: postgremq::Delivery) -> postgremq::Result<()> {
let mut tx = conn.pool().begin().await?;
// ... application writes on &mut *tx ...
conn.publish_tx(&mut tx, "audit", &"order billed", Default::default()).await?;
delivery.ack_tx(&mut tx).await?;
tx.commit().await?;
# Ok(())
# }
```

## Ordered delivery (message groups)

Publish with `PublishOptions::default().group_key(session_id)`: within each
queue a group's messages are delivered one at a time, in publish commit order
(`Delivery::group_key`, `Delivery::group_seq`). A leased or delayed head
blocks its group; a head that exhausts its attempts moves to the dead letter
queue, which unblocks the group. Publishers of one group are serialised by the
database, so a group should be a session or an order, never a hot shared key.
See the [SQL README](https://github.com/slavakl/postgremq/blob/main/mq/README.md)
("Message groups") for the full contract.

## Handlers

`Connection::consume_with_handler(queue, options, max_in_flight, handler)`
runs `handler` for each delivery, at most `max_in_flight` at once (`None`:
unlimited, like the Go client's `WithMaxInFlight(0)`). A handler that returns `Ok` without
settling auto-acks while its `stopped()` token is live; `Err`, a panic, or a
return after cancellation auto-nacks.

## Shutdown

`Connection::close` rejects new work, stops fetching, releases buffered
never-delivered messages, cancels in-flight deliveries' `stopped()` tokens and
keeps settling and renewing them until they finish or
`ConnectionOptions::shutdown_timeout` passes; the background tasks stop last.

## Errors

`Error::LeaseLost` (`PMQ01`), `Error::QueueNotFound` (`PMQ02`),
`Error::Validation` (`PMQ03`), `Error::Busy` (`55P03`, retry within the
lease), `Error::QueueGone` (fatal for a consumer: its stream ends with it),
`Error::Closed`, and `Error::Sqlx` for everything else.

## Diagnostics

The crate logs through `tracing` and never installs a subscriber. Timestamps
are `std::time::SystemTime`; lease scheduling uses deadlines measured on the
database server, so client clock skew does not shorten leases.

## Testing

`cargo test` starts (or reuses) a `postgres:15` container named
`postgremq-rs-test` through testcontainers (Docker required; remove it with
`docker rm -f postgremq-rs-test`). To use an existing PostgreSQL 15+ server
instead, whose user can create databases:

```sh
export POSTGREMQ_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo test
```

## Pools

`Connection::connect` builds its pool without sqlx's ping before every
checkout (`test_before_acquire(false)`, which otherwise doubles the round
trips of each claim and settlement) and with a 3-minute idle timeout, so
idle sockets are recycled before typical load-balancer cut-offs; operations
that are safe to repeat retry a dropped connection. For `Connection::from_pool`
consider the same pool settings.

## Dependencies

The crate uses sqlx 0.9 with rustls (`tls-rustls-ring-native-roots`, a fixed
choice rather than a feature) and sqlx's `uuid` feature for queue
generations.
