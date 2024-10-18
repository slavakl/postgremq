# PostgreMQ TypeScript Client

A TypeScript client for PostgreMQ - a message queue system built on top of PostgreSQL.

## Features

- Simple, promise-based API for publishing and consuming messages
- Asynchronous iterator-based message consumption for easy processing
- Smart visibility timeout extension to prevent message locks from expiring
- Efficient notification system using PostgreSQL's LISTEN/NOTIFY
- Robust error handling and retry logic for transient database errors
- Comprehensive admin functions for managing topics, queues, and messages
- Support for exclusive (non-durable) queues with automatic keep-alive
- Dead Letter Queue (DLQ) support for handling failed messages

## Installation

```bash
npm install postgremq
```

## Quick Start

```typescript
import { connect } from 'postgremq';

async function main() {
  // Connect to PostgreSQL
  const client = await connect({
    connectionString: 'postgresql://user:password@localhost:5432/mydb'
  });

  try {
    // Create topic and queue
    await client.createTopic('orders');
    await client.createQueue('processing-queue', 'orders', false, { 
      maxDeliveryAttempts: 5 
    });

    // Publish a message
    const messageId = await client.publish('orders', { 
      orderId: '12345', 
      items: ['item1', 'item2'], 
      total: 99.99 
    });
    console.log(`Published message with ID: ${messageId}`);

    // Create a consumer
    const consumer = client.consume('processing-queue', {
      visibilityTimeoutSec: 60,
      batchSize: 5
    });

    // Process messages using async iterator
    for await (const message of consumer.messages()) {
      try {
        // Process the message
        console.log('Processing:', message.payload);
        
        // Acknowledge successful processing
        await message.ack();
      } catch (error) {
        // If processing fails, negatively acknowledge
        // This will make the message available again after a delay
        await message.nack({ delaySeconds: 10 });
      }
    }
  } finally {
    // Close the connection when done
    await client.close();
  }
}

main().catch(console.error);
```

## Client Configuration

The client can be configured with the following options:

```typescript
import { connect, ConnectionOptions } from 'postgremq';

const options: ConnectionOptions = {
  // Connection string (if provided, other connection properties are ignored)
  connectionString: 'postgresql://user:password@localhost:5432/mydb',
  
  // Or individual connection properties
  host: 'localhost',
  port: 5432,
  user: 'user',
  password: 'password',
  database: 'mydb',
  
  // Connection pool settings
  pool: {
    min: 2,
    max: 10,
    idleTimeoutMillis: 30000
  },
  
  // Retry policy for transient errors
  retry: {
    maxAttempts: 5,
    initialBackoffMs: 100,
    backoffMultiplier: 2,
    maxBackoffMs: 5000
  },
  
  // Maximum time to wait for in-flight messages during shutdown
  shutdownTimeoutMs: 30000
};

const client = await connect(options);
```

## Publishing Messages

You can publish messages to topics with optional delivery delays:

```typescript
// Simple publish
const messageId = await client.publish('orders', { orderId: '12345' });

// Delayed delivery
const delayedId = await client.publish('orders', { orderId: '67890' }, {
  deliverAfter: new Date(Date.now() + 60000) // Deliver after 1 minute
});

// Publishing within a transaction
await client.executeWithTransaction(async (tx) => {
  await client.publishWithTransaction(tx, 'orders', { orderId: '12345' });
  await client.publishWithTransaction(tx, 'notifications', { type: 'order_created' });
  // All messages are published atomically
});
```

## Consuming Messages

The client supports async iterator-based consumption:

```typescript
// Create a consumer
const consumer = client.consume('processing-queue', {
  // How many messages to fetch in a single batch
  batchSize: 10,
  
  // Default visibility timeout in seconds
  visibilityTimeoutSec: 60,
  
  // Auto-extension settings for visibility timeout
  autoExtension: {
    enabled: true,               // Enable automatic extension
    extensionThreshold: 0.5,     // Extend when 50% of timeout has elapsed
    extensionSec: 60             // Extend by 60 seconds
  },
  
  // Polling interval in milliseconds (when no notifications)
  pollingIntervalMs: 1000
});

// Process messages
for await (const message of consumer.messages()) {
  console.log('Message ID:', message.id);
  console.log('Payload:', message.payload);
  console.log('Delivery attempts:', message.deliveryAttempts);
  
  try {
    // Process the message
    await processMessage(message.payload);
    
    // Acknowledge successful processing
    await message.ack();
  } catch (error) {
    // Handle processing errors
    if (isRetryableError(error)) {
      // Negatively acknowledge with a delay
      await message.nack({ delaySeconds: 30 });
    } else {
      // Non-retryable error, still acknowledge
      // (the message will go to the DLQ if it has exceeded max delivery attempts)
      await message.ack();
    }
  }
}

// Stop the consumer when done
await consumer.stop();
```

## Administrative Operations

The client provides comprehensive administrative functions:

```typescript
// Topics
await client.createTopic('orders');
const topics = await client.listTopics();
await client.deleteTopic('orders');

// Queues
await client.createQueue('processing-queue', 'orders', false, { 
  maxDeliveryAttempts: 5 
});
const queues = await client.listQueues();
await client.deleteQueue('processing-queue');

// Queue statistics
const stats = await client.getQueueStatistics('processing-queue');
console.log(`Pending: ${stats.pendingCount}, Processing: ${stats.processingCount}`);

// Dead Letter Queue operations
const dlqMessages = await client.listDLQMessages();
await client.requeueDLQMessages('processing-queue');
await client.purgeDLQ();

// Message operations
const messages = await client.listMessages('processing-queue');
const message = await client.getMessage(123);
await client.deleteQueueMessage('processing-queue', 123);

// Cleanup operations
await client.cleanUpQueue('processing-queue');
await client.cleanUpTopic('orders');
await client.purgeAllMessages();
```

## Exclusive Queues

Exclusive queues (non-durable) are automatically deleted when their keep-alive timeout expires:

```typescript
// Create an exclusive queue that expires after 60 seconds of inactivity
await client.createQueue('temp-queue', 'orders', true, {
  keepAliveSeconds: 60
});

// Extend the queue's lifetime
await client.extendQueueKeepAlive('temp-queue', 60);
```

## Error Handling

The client includes built-in retry logic for transient database errors:

```typescript
try {
  await client.publish('orders', { orderId: '12345' });
} catch (error) {
  // This will only be thrown after all retries have failed
  console.error('Failed to publish message:', error);
}
```

## License

MIT
