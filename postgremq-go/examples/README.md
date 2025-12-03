# PostgreMQ Go Examples

This directory contains runnable examples demonstrating various PostgreMQ features.

## Prerequisites

- PostgreSQL 15+ running locally or accessible remotely
- Go 1.23+

## Setup

### 1. Install PostgreMQ schema

Install the PostgreMQ schema using one of these methods:

**Option A: Using the CLI (recommended)**
```bash
cd ../../cmd/postgremq
go build -o postgremq .
./postgremq migrate --dsn "postgres://user:password@localhost:5432/dbname?sslmode=disable"
```

**Option B: Using psql directly**
```bash
cd ../../mq
psql -U postgres -d your_database -f sql/latest.sql
```

**Option C: Programmatically (see migration example)**
```bash
cd migration
go run main.go
```

### 2. Set database connection

Set the `DATABASE_URL` environment variable (optional):

```bash
export DATABASE_URL="postgres://user:password@localhost:5432/dbname?sslmode=disable"
```

If not set, examples will use: `postgres://postgres:postgres@localhost:5432/postgres`

## Running Examples

### Basic Example

Demonstrates:
- Creating topics and queues
- Publishing messages
- Consuming messages
- Acknowledgment
- Graceful shutdown

```bash
cd basic
go run main.go
```

### Transaction Example

Demonstrates:
- Publishing within transactions
- Acknowledging within transactions
- Transaction rollback

```bash
cd transactions
go run main.go
```

## Creating Your Own Examples

1. Create a new directory under `examples/`
2. Initialize a Go module or use workspace mode
3. Import PostgreMQ:

```go
import postgremq "github.com/slavakl/postgremq/postgremq-go"
```

4. Follow patterns from existing examples

## Common Patterns

### Connection Setup

```go
cfg, _ := pgxpool.ParseConfig("postgres://...")
conn, _ := postgremq.Dial(ctx, cfg)
defer conn.Close()
```

### Publishing

```go
payload := json.RawMessage(`{"key": "value"}`)
messageID, _ := conn.Publish(ctx, "topic-name", payload)
```

### Consuming

```go
consumer, _ := conn.Consume(ctx, "queue-name",
    postgremq.WithBatchSize(10),
    postgremq.WithVT(30))
defer consumer.Stop()

for msg := range consumer.Messages() {
    // Process message
    msg.Ack(ctx)
}
```

## Troubleshooting

### Connection refused

Ensure PostgreSQL is running:

```bash
psql -U postgres -c "SELECT version();"
```

### Schema not found

Make sure you've installed the PostgreMQ schema (see Setup step 1).

### Permission denied

Ensure your database user has sufficient privileges:

```sql
GRANT ALL PRIVILEGES ON DATABASE your_database TO your_user;
```

## More Examples

For more examples, see:
- [Example tests](../example_test.go)
- [Integration tests](../integration_test.go)
- [TypeScript examples](../../postgremq-ts/examples/)
