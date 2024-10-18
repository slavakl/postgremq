# PostgreMQ Go Client

Go client library for PostgreMQ - a message queue system built on PostgreSQL.

## Testing Approach

The test suite uses a robust approach designed for reliable parallel test execution:

1. **Database per Test**: Each test gets its own isolated PostgreSQL database, allowing tests to run in true parallel mode without interfering with each other.

2. **Docker-based**: Tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up a single PostgreSQL container that's shared across all tests.

3. **Synchronized Operations**: Critical database operations like schema initialization and database cleanup use mutexes to prevent race conditions when running in parallel.

4. **Automatic Cleanup**: Test databases are automatically created before each test and dropped after all its subtests complete, ensuring no leftover test data.

5. **Subtests Support**: For tests using `t.Run()` with multiple subtests, the `cleanTestData()` function cleans the database between subtests since they share the same database connection.

6. **Error Resilience**: The test framework handles common PostgreSQL errors gracefully, especially during parallel execution, making the tests more reliable.

### Running Tests

#### Running all tests:
```bash
go test ./...
```

#### Running tests in short mode (skips long-running tests):
```bash
go test -short ./...
```

#### Running tests in parallel:
```bash
# Run with specified parallelism level (e.g., 4 parallel tests)
go test -parallel 4 ./...

# Run short tests in parallel
go test -short -parallel 4 ./...
```

#### Using the quick test script:
```bash
# Makes the script executable if needed
chmod +x scripts/quicktest.sh

# Run quick tests with default parallelism (4)
./scripts/quicktest.sh

# Run quick tests with custom parallelism
./scripts/quicktest.sh 8  # Run with 8 parallel tests
```

## Usage

### Basic Usage

```go
// Initialize a connection
conn, err := postgremq_go.Dial(ctx, pgxConfig)
if err != nil {
    log.Fatalf("Failed to connect: %v", err)
}
defer conn.Close()

// Create a topic
if err := conn.CreateTopic(ctx, "orders"); err != nil {
    log.Fatalf("Failed to create topic: %v", err)
}

// Create a queue
if err := conn.CreateQueue(ctx, "order-processing", "orders", false); err != nil {
    log.Fatalf("Failed to create queue: %v", err)
}

// Publish a message
payload := json.RawMessage(`{"order_id": 12345, "status": "new"}`)
messageID, err := conn.Publish(ctx, "orders", payload)
if err != nil {
    log.Fatalf("Failed to publish message: %v", err)
}

// Consume messages
consumer, err := conn.Consume(ctx, "order-processing")
if err != nil {
    log.Fatalf("Failed to create consumer: %v", err)
}

// Process messages
for msg := range consumer.Messages() {
    // Process the message
    fmt.Printf("Received message: %s\n", string(msg.Payload))
    
    // Acknowledge the message
    if err := msg.Ack(ctx); err != nil {
        log.Printf("Failed to ack message: %v", err)
    }
}
```

### Using Transactions

You can use transactions to ensure that message publishing and acknowledgment operations are performed atomically with other database operations:

```go
// Assume we have a pgx.Conn or pgxpool.Pool
pgxPool, err := pgxpool.New(ctx, connString)
if err != nil {
    log.Fatalf("Failed to create pool: %v", err)
}

// Create PostgreMQ connection using the pool
conn, err := postgremq_go.DialFromPool(ctx, pgxPool)
if err != nil {
    log.Fatalf("Failed to connect to PostgreMQ: %v", err)
}
defer conn.Close()

// Start a transaction
tx, err := pgxPool.Begin(ctx)
if err != nil {
    log.Fatalf("Failed to begin transaction: %v", err)
}
defer tx.Rollback(ctx) // Will be ignored if transaction is committed

// Perform regular database operations
_, err = tx.Exec(ctx, "INSERT INTO orders (order_id, customer_id) VALUES ($1, $2)", 12345, 67890)
if err != nil {
    log.Fatalf("Failed to insert order: %v", err)
}

// Publish message within the transaction
payload := json.RawMessage(`{"order_id": 12345, "status": "new"}`)
messageID, err := conn.PublishWithTx(ctx, tx, "orders", payload)
if err != nil {
    log.Fatalf("Failed to publish message within transaction: %w", err)
}

// Commit the transaction
if err := tx.Commit(ctx); err != nil {
    log.Fatalf("Failed to commit transaction: %v", err)
}
```

### Acknowledging Messages within Transactions

```go
// Process a message within a transaction
msg := <-consumer.Messages()

// Start a transaction
tx, err := pgxPool.Begin(ctx)
if err != nil {
    log.Fatalf("Failed to begin transaction: %v", err)
}
defer tx.Rollback(ctx)

// Process the message and update database
_, err = tx.Exec(ctx, "UPDATE orders SET status = 'processed' WHERE order_id = $1", orderID)
if err != nil {
    log.Fatalf("Failed to update order: %v", err)
}

// Acknowledge the message within the transaction
if err := msg.AckWithTx(ctx, tx); err != nil {
    log.Fatalf("Failed to ack message within transaction: %v", err)
}

// Commit the transaction
if err := tx.Commit(ctx); err != nil {
    log.Fatalf("Failed to commit transaction: %v", err)
}
```

Note: Only `PublishWithTx` and `AckWithTx` are supported in transactions for common use cases. For other acknowledgment patterns like `Nack` or `Release`, use the standard non-transactional methods.