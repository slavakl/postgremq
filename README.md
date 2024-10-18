# PostgreMQ

A reliable, feature-rich message queue system built entirely on PostgreSQL.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-blue)](https://go.dev/)
[![Node Version](https://img.shields.io/badge/Node-14.0%2B-green)](https://nodejs.org/)

## Overview

PostgreMQ is a message queue system that leverages PostgreSQL's reliability and ACID guarantees to provide durable, distributed message processing. It consists of three main components:

- **mq** - Core PostgreSQL implementation (SQL schema and functions)
- **postgremq-go** - Go client library
- **postgremq-ts** - TypeScript/Node.js client library

## Key Features

- **No Extension Required**: Pure SQL schema - just run a .sql file, no PostgreSQL extension installation needed
- **Topic-to-Queue Fan-out**: Publish once to a topic, automatically distributed to multiple queues via triggers
- **ACID Guarantees**: Full PostgreSQL transaction support for reliable message persistence and processing
- **Transactional Outbox Pattern**: Publish and acknowledge messages atomically within application transactions
- **Visibility Timeout**: Messages use visibility timeouts instead of locks (similar to AWS SQS)
- **Auto Visibility Extension**: Client libraries automatically extend timeouts during long-running processing
- **Real-time Notifications**: Uses PostgreSQL LISTEN/NOTIFY for instant message delivery
- **Dead Letter Queue**: Configurable max delivery attempts with automatic DLQ for failed messages
- **Delayed Delivery**: Schedule messages for future delivery
- **Multiple Consumers**: Parallel message processing with automatic load distribution via SKIP LOCKED
- **Queue Types**:
  - **Non-exclusive queues**: Persistent, standard message queues
  - **Exclusive queues**: Temporary queues that auto-expire without keep-alive

## Quick Start

### Prerequisites

- PostgreSQL 15 or later
- Go 1.23+ (for Go client)
- Node.js 14+ (for TypeScript client)

### Installation

#### SQL Schema

First, install the core PostgreMQ schema in your PostgreSQL database:

```sql
-- Run the SQL from mq/sql/latest.sql
-- This creates the topics, queues, messages, and queue_messages tables
-- along with all necessary functions and triggers
```

#### Go Client

```bash
go get github.com/slavakl/postgremq/postgremq-go
```

```go
import postgremq "github.com/slavakl/postgremq/postgremq-go"
```

#### TypeScript Client

```bash
npm install postgremq
```

```typescript
import { connect } from 'postgremq';
```

## Usage Examples

### Go

```go
package main

import (
    "context"
    "encoding/json"
    "log"

    postgremq "github.com/slavakl/postgremq/postgremq-go"
    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    ctx := context.Background()

    // Connect to PostgreSQL
    cfg, _ := pgxpool.ParseConfig("postgres://user:pass@localhost:5432/mydb")
    conn, err := postgremq.Dial(ctx, cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Create topic and queue
    conn.CreateTopic(ctx, "orders")
    conn.CreateQueue(ctx, "order-processor", "orders", false)

    // Publish a message
    payload := json.RawMessage(`{"order_id": 12345, "total": 99.99}`)
    messageID, _ := conn.Publish(ctx, "orders", payload)
    log.Printf("Published message: %d", messageID)

    // Consume messages
    consumer, _ := conn.Consume(ctx, "order-processor")
    defer consumer.Stop()

    for msg := range consumer.Messages() {
        log.Printf("Received: %s", string(msg.Payload))
        msg.Ack(ctx)
    }
}
```

### TypeScript

```typescript
import { connect } from 'postgremq';

async function main() {
  // Connect to PostgreSQL
  const client = await connect({
    connectionString: 'postgresql://user:pass@localhost:5432/mydb'
  });

  // Create topic and queue
  await client.createTopic('orders');
  await client.createQueue('order-processor', 'orders', false);

  // Publish a message
  const messageId = await client.publish('orders', {
    order_id: 12345,
    total: 99.99
  });
  console.log(`Published message: ${messageId}`);

  // Consume messages
  const consumer = client.consume('order-processor');

  for await (const message of consumer.messages()) {
    console.log('Received:', message.payload);
    await message.ack();
  }

  await client.close();
}

main().catch(console.error);
```

## Architecture

### Message Flow

1. **Publish**: Messages are published to a **topic**
2. **Distribution**: A trigger automatically copies messages to all **queues** subscribed to that topic
3. **Consume**: Consumers fetch messages from queues with a **visibility timeout**
4. **Processing**: Messages are invisible to other consumers during processing
5. **Acknowledgment**: Messages can be:
   - **Acked**: Marked as successfully processed (removed from queue)
   - **Nacked**: Returned to queue with incremented delivery attempt (optional delay)
   - **Released**: Returned to queue without incrementing delivery attempt
6. **Dead Letter Queue**: Messages exceeding max delivery attempts are moved to DLQ

### Database Schema

```
topics
  └─> queues (one-to-many)
        └─> queue_messages (many-to-many with messages)
              └─> messages

dead_letter_queue (failed messages)
```

All tables use `ON DELETE CASCADE` from topics down for easy cleanup.

## Core Concepts

### Visibility Timeout

Instead of traditional message locks, PostgreMQ uses **visibility timeouts** (VT). When a consumer fetches a message, it becomes invisible to other consumers until the timeout expires. This is similar to AWS SQS's model.

- Default VT is configurable per consumer
- Automatic extension prevents timeout during long processing
- Manual extension available via `SetVT()` / `extend()`

### Queue Types

- **Non-exclusive** (formerly "durable"): Persistent queues that live indefinitely
- **Exclusive** (formerly "non-durable"): Temporary queues that expire without keep-alive

### Event Notification

PostgreMQ uses PostgreSQL's `LISTEN/NOTIFY` on channel `queue:{queue_name}` to provide real-time notifications when new messages arrive, avoiding constant polling.

## Client Libraries

| Feature | Go Client | TypeScript Client |
|---------|-----------|-------------------|
| Connection pooling | ✅ | ✅ |
| Auto visibility timeout extension | ✅ | ✅ |
| Transaction support | ✅ | ✅ |
| Async iteration | ✅ | ✅ |
| Delayed delivery | ✅ | ✅ |
| Dead letter queue | ✅ | ✅ |
| Retry with backoff | ✅ | ✅ |
| LISTEN/NOTIFY | ✅ | ✅ |
| Keep-alive for exclusive queues | ✅ | ✅ |

## Documentation

- [Go Client Documentation](./postgremq-go/README.md)
- [TypeScript Client Documentation](./postgremq-ts/README.md)
- [SQL Implementation](./mq/README.md)
- [Contributing Guidelines](./CONTRIBUTING.md)
- [Security Policy](./SECURITY.md)

## Testing

All components include comprehensive test suites using testcontainers for isolated PostgreSQL instances.

### Go Tests

```bash
cd postgremq-go
go test -v ./...
```

### TypeScript Tests

```bash
cd postgremq-ts
npm test
```

### SQL Tests

```bash
cd mq
pip install -r tests/requirements.txt
pytest tests/tests.py -v
```

## Performance Characteristics

- **Message throughput**: Depends on PostgreSQL performance
- **Latency**: Sub-second delivery via LISTEN/NOTIFY
- **Scalability**: Scales with PostgreSQL (supports read replicas for some operations)
- **Durability**: ACID guarantees from PostgreSQL

## Comparison with Other Message Queues

| Feature | PostgreMQ | RabbitMQ | AWS SQS | Redis |
|---------|-----------|----------|---------|-------|
| Persistence | PostgreSQL | Disk/RAM | AWS-managed | RAM (optional disk) |
| ACID guarantees | ✅ | ⚠️ | ⚠️ | ❌ |
| Transaction support | ✅ Full | ⚠️ | ❌ | ⚠️ |
| Additional infrastructure | ❌ | ✅ | ❌ | ✅ |
| Topic-based pub/sub | ✅ | ✅ | ❌ | Pub/Sub |
| Message ordering | Per topic | Per queue | FIFO queues | ❌ |
| Delayed delivery | ✅ | ✅ | ✅ | ❌ |
| Auto visibility extension | ✅ | N/A | ❌ | N/A |
| Extension required | ❌ | N/A | N/A | N/A |

*For PostgreSQL-based alternatives, see [PGMQ](https://github.com/pgmq/pgmq) in [Acknowledgments](#acknowledgments).*

## Use Cases

PostgreMQ is ideal when you:

- Already use PostgreSQL and want to avoid additional infrastructure
- Need ACID guarantees for message processing
- Want to publish/acknowledge messages within database transactions
- Need a simple, reliable queue without operational complexity
- Require message durability and consistency over extreme throughput

PostgreMQ may not be ideal when you:

- Need millions of messages per second
- Require advanced routing (use RabbitMQ)
- Need distributed queue across multiple regions (use AWS SQS)
- Want minimal latency (use Redis Streams)

## Roadmap

- [ ] Enhanced monitoring and metrics
- [ ] Admin UI/dashboard
- [ ] Migration to new version 

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](./CONTRIBUTING.md) for details.

## License

PostgreMQ is released under the [MIT License](./LICENSE).

## Support

- **Issues**: [GitHub Issues](https://github.com/slavakl/postgremq/issues)
- **Discussions**: [GitHub Discussions](https://github.com/slavakl/postgremq/discussions)

## Acknowledgments

PostgreMQ was inspired by [PGMQ](https://github.com/pgmq/pgmq) and the broader PostgreSQL message queue ecosystem. We're grateful to the PGMQ team for pioneering PostgreSQL-based message queues and demonstrating the viability of this approach.

### Key Differences

Building on the PostgreSQL message queue concept, PostgreMQ offers:

**No Extension Installation Required**
- Pure SQL schema - just run a .sql file
- No need to install PostgreSQL extensions or restart the database
- Works on any PostgreSQL 15+ instance, including managed services (RDS, Cloud SQL, etc.)

**Topic-to-Queue Fan-out**
- Publish once to a topic, automatically distributed to all subscribed queues
- Perfect for fan-out patterns and event broadcasting
- Implemented via PostgreSQL triggers for automatic, reliable distribution

**Transactional Outbox Pattern**
- Publish and acknowledge messages within application transactions
- Ensures atomic message operations with business logic
- Full rollback support prevents orphaned messages

**Automatic Visibility Timeout Extension**
- Client libraries automatically extend timeouts during long-running processing
- No manual timeout management required
- Prevents message redelivery for legitimate long-running jobs

**Non-Exclusive and Exclusive Queue Support**
- **Non-exclusive queues**: Persistent, standard message queues
- **Exclusive queues**: Temporary queues that auto-expire without keep-alive
- Supports different messaging patterns (persistent vs ephemeral)

---