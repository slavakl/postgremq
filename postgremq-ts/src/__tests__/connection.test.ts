/**
 * Connection Unit Tests
 * Tests all connection-level operations and lifecycle management
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import {
  TestDatabase,
  createIsolatedTestConnection,
  generateTestPayload,
  sleep,
  assertThrows
} from './helpers';
import { Connection } from '../connection';

describe('Connection', () => {
  let testDb: TestDatabase;
  let connection: Connection;

  let dropDb: (() => Promise<void>) | null = null;
  let rawPool: import('pg').Pool | null = null;

  beforeAll(async () => {
    testDb = new TestDatabase();
    await testDb.start();
  });

  beforeEach(async () => {
    const iso = await createIsolatedTestConnection(testDb);
    connection = iso.connection;
    dropDb = iso.dropDatabase;
    rawPool = iso.pool;
  });

  afterEach(async () => {
    if (connection) {
      await connection.close();
    }
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
    rawPool = null;
  });

  afterAll(async () => {
    if (testDb) {
      await testDb.stop();
    }
  });

  describe('Lifecycle', () => {
    test('should connect and disconnect successfully', async () => {
      const conn = new Connection({
        connectionString: testDb.connectionString
      });

      await conn.connect();
      expect(conn.isClientShuttingDown()).toBe(false);

      await conn.close();
      expect(conn.isClientShuttingDown()).toBe(true);
    });

    test('should throw error when operating on closed connection', async () => {
      await connection.close();

      await assertThrows(
        () => connection.createTopic('test'),
        'not connected'
      );
    });

    test('should support connection pool reuse', async () => {
      const pool = rawPool!;

      const conn1 = new Connection({ pool });
      const conn2 = new Connection({ pool });

      await conn1.connect();
      await conn2.connect();

      // Both should work
      await conn1.createTopic('topic1');
      await conn2.createTopic('topic2');

      await conn1.close();
      await conn2.close();

      // Pool should still be alive (not owned by connections)
      const client = await pool.connect();
      await client.query('SELECT 1');
      client.release();
    });
  });

  describe('Topic Operations', () => {
    test('should create and list topics', async () => {
      await connection.createTopic('topic-1');
      await connection.createTopic('topic-2');

      const topics = await connection.listTopics();

      expect(topics).toContain('topic-1');
      expect(topics).toContain('topic-2');
      expect(topics.length).toBeGreaterThanOrEqual(2);
    });

    test('should delete topic', async () => {
      await connection.createTopic('deletable-topic');

      // Should exist
      let topics = await connection.listTopics();
      expect(topics).toContain('deletable-topic');

      // Delete it
      await connection.deleteTopic('deletable-topic');

      // Should not exist
      topics = await connection.listTopics();
      expect(topics).not.toContain('deletable-topic');
    });

    test('should not delete topic with existing messages', async () => {
      await connection.createTopic('topic-with-messages');
      await connection.createQueue('queue-for-topic', 'topic-with-messages', false);
      await connection.publish('topic-with-messages', { test: 'data' });

      await assertThrows(
        () => connection.deleteTopic('topic-with-messages')
      );

      // Cleanup
      await connection.cleanUpTopic('topic-with-messages');
      await connection.deleteTopic('topic-with-messages');
      await connection.deleteQueue('queue-for-topic');
    });

    test('should clean up topic', async () => {
      await connection.createTopic('cleanup-topic');
      await connection.createQueue('cleanup-queue', 'cleanup-topic', false);
      await connection.publish('cleanup-topic', { test: 'data' });

      // Clean up topic (removes messages)
      await connection.cleanUpTopic('cleanup-topic');

      // Now delete should work
      await connection.deleteTopic('cleanup-topic');

      const topics = await connection.listTopics();
      expect(topics).not.toContain('cleanup-topic');
    });
  });

  describe('Queue Operations', () => {
    beforeEach(async () => {
      await connection.createTopic('queue-test-topic');
    });

    test('should create and list queues', async () => {
      await connection.createQueue('queue-1', 'queue-test-topic', false);
      await connection.createQueue('queue-2', 'queue-test-topic', false, {
        maxDeliveryAttempts: 5
      });

      const queues = await connection.listQueues();

      const q1 = queues.find(q => q.queueName === 'queue-1');
      const q2 = queues.find(q => q.queueName === 'queue-2');

      expect(q1).toBeDefined();
      expect(q1!.topicName).toBe('queue-test-topic');
      expect(q1!.exclusive).toBe(false);

      expect(q2).toBeDefined();
      expect(q2!.maxDeliveryAttempts).toBe(5);
    });

    test('should delete queue', async () => {
      await connection.createQueue('deletable-queue', 'queue-test-topic', false);

      let queues = await connection.listQueues();
      expect(queues.some(q => q.queueName === 'deletable-queue')).toBe(true);

      await connection.deleteQueue('deletable-queue');

      queues = await connection.listQueues();
      expect(queues.some(q => q.queueName === 'deletable-queue')).toBe(false);
    });

    test('should clean up queue', async () => {
      await connection.createQueue('cleanup-queue-test', 'queue-test-topic', false);
      await connection.publish('queue-test-topic', { test: 'data' });

      await sleep(100);

      let stats = await connection.getQueueStatistics('cleanup-queue-test');
      expect(stats.totalCount).toBeGreaterThan(0);

      await connection.cleanUpQueue('cleanup-queue-test');

      stats = await connection.getQueueStatistics('cleanup-queue-test');
      expect(stats.totalCount).toBe(0);
    });

    test('should get queue statistics', async () => {
      await connection.createQueue('stats-queue', 'queue-test-topic', false);

      // Initially empty
      let stats = await connection.getQueueStatistics('stats-queue');
      expect(stats.pendingCount).toBe(0);
      expect(stats.processingCount).toBe(0);
      expect(stats.completedCount).toBe(0);

      // Publish some messages
      await connection.publish('queue-test-topic', { test: 1 });
      await connection.publish('queue-test-topic', { test: 2 });

      await sleep(100);

      stats = await connection.getQueueStatistics('stats-queue');
      expect(stats.pendingCount).toBe(2);
    });
  });

  describe('Message Operations', () => {
    beforeEach(async () => {
      await connection.createTopic('msg-topic');
      await connection.createQueue('msg-queue', 'msg-topic', false);
    });

    test('should publish message and return ID', async () => {
      const payload = generateTestPayload();
      const messageId = await connection.publish('msg-topic', payload);

      expect(messageId).toBeGreaterThan(0);
      expect(typeof messageId).toBe('number');
    });

    test('should publish message with delayed delivery', async () => {
      const payload = generateTestPayload();
      const deliverAfter = new Date(Date.now() + 5000);

      const messageId = await connection.publish('msg-topic', payload, {
        deliverAfter
      });

      expect(messageId).toBeGreaterThan(0);

      // Verify message is not yet available
      const stats = await connection.getQueueStatistics('msg-queue');
      expect(stats.pendingCount).toBe(1);

      // Get message details
      const message = await connection.getMessage(messageId);
      expect(message).not.toBeNull();
      expect(message!.payload).toEqual(payload);
    });

    test('should list messages in queue', async () => {
      await connection.publish('msg-topic', { test: 1 });
      await connection.publish('msg-topic', { test: 2 });

      await sleep(100);

      const messages = await connection.listMessages('msg-queue');

      expect(messages.length).toBe(2);
      expect(messages[0].status).toBe('pending');
      expect(messages[0].deliveryAttempts).toBe(0);
    });

    test('should delete specific queue message', async () => {
      const messageId = await connection.publish('msg-topic', { test: 'delete-me' });

      await sleep(100);

      let messages = await connection.listMessages('msg-queue');
      expect(messages.length).toBe(1);

      await connection.deleteQueueMessage('msg-queue', messageId);

      messages = await connection.listMessages('msg-queue');
      expect(messages.length).toBe(0);
    });

    test('should purge all messages', async () => {
      await connection.publish('msg-topic', { test: 1 });
      await connection.publish('msg-topic', { test: 2 });

      await sleep(100);

      let stats = await connection.getQueueStatistics('msg-queue');
      expect(stats.totalCount).toBeGreaterThan(0);

      await connection.purgeAllMessages();

      stats = await connection.getQueueStatistics('msg-queue');
      expect(stats.totalCount).toBe(0);
    });
  });

  describe('Dead Letter Queue', () => {
    beforeEach(async () => {
      await connection.createTopic('dlq-topic');
      await connection.createQueue('dlq-queue', 'dlq-topic', false, {
        maxDeliveryAttempts: 2
      });
    });

    test('should move messages to DLQ after max attempts', async () => {
      const payload = generateTestPayload();
      await connection.publish('dlq-topic', payload);

      // Use the same consumer for both attempts
      const consumer = connection.consume('dlq-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
        autoExtension: { enabled: false }
      });

      const messages = consumer.messages();

      // First attempt - nack
      const { value: msg1 } = await messages.next();
      await msg1.nack();

      // Second attempt - nack (should hit max)
      const { value: msg2 } = await messages.next();
      expect(msg2.id).toBe(msg1.id); // Ensure it's the same message
      await msg2.nack();

      // Stop consumer after both nacks
      await consumer.stop();

      await sleep(100);

      // Explicitly move messages to DLQ
      const movedCount = await connection.moveToDLQ();
      expect(movedCount).toBe(1);

      // Message should be in DLQ now
      const dlqMessages = await connection.listDLQMessages();
      expect(dlqMessages.length).toBe(1);
      expect(dlqMessages[0].retryCount).toBe(2);
    }, 30000);

    test('should requeue messages from DLQ', async () => {
      // First, create a message in DLQ (similar to above test)
      await connection.publish('dlq-topic', { test: 'requeue' });

      const consumer = connection.consume('dlq-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
        autoExtension: { enabled: false }
      });

      const messages = consumer.messages();

      // First nack
      const { value: msg1 } = await messages.next();
      await msg1.nack();

      // Second nack (hits max)
      const { value: msg2 } = await messages.next();
      await msg2.nack();

      await consumer.stop();

      await sleep(100);

      // Explicitly move messages to DLQ
      await connection.moveToDLQ();

      // Verify in DLQ
      let dlqMessages = await connection.listDLQMessages();
      expect(dlqMessages.length).toBe(1);

      // Requeue it
      await connection.requeueDLQMessages('dlq-queue');

      // Should be back in queue
      dlqMessages = await connection.listDLQMessages();
      expect(dlqMessages.length).toBe(0);

      const stats = await connection.getQueueStatistics('dlq-queue');
      expect(stats.pendingCount).toBe(1);
    }, 30000);

    test('should purge DLQ', async () => {
      // Add message to DLQ (similar to above)
      await connection.publish('dlq-topic', { test: 'purge' });

      const consumer = connection.consume('dlq-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
        autoExtension: { enabled: false }
      });

      const messages = consumer.messages();

      // First nack
      const { value: msg1 } = await messages.next();
      await msg1.nack();

      // Second nack (hits max)
      const { value: msg2 } = await messages.next();
      await msg2.nack();

      await consumer.stop();

      await sleep(100);

      // Explicitly move messages to DLQ
      await connection.moveToDLQ();

      let dlqMessages = await connection.listDLQMessages();
      expect(dlqMessages.length).toBeGreaterThan(0);

      await connection.purgeDLQ();

      dlqMessages = await connection.listDLQMessages();
      expect(dlqMessages.length).toBe(0);
    }, 30000);
  });

  describe('Maintenance Operations', () => {
    test('should cleanup completed messages using retention thresholds', async () => {
      const pool = rawPool!;

      await connection.createTopic('cleanup-retention-topic');
      await connection.createQueue('cleanup-retention-queue', 'cleanup-retention-topic', false);

      const firstId = await connection.publish('cleanup-retention-topic', { index: 1 });
      const secondId = await connection.publish('cleanup-retention-topic', { index: 2 });

      await sleep(100);

      // Consume and ack two messages via client API to ensure tokens/states
      const consumer = connection.consume('cleanup-retention-queue', {
        batchSize: 2,
        visibilityTimeoutSec: 60,
        autoExtension: { enabled: false }
      });
      const it = consumer.messages();
      const { value: m1 } = await it.next();
      const { value: m2 } = await it.next();

      expect(m1).toBeDefined();
      expect(m2).toBeDefined();

      await m1.ack();
      await m2.ack();
      await consumer.stop();

      await pool.query(
        "UPDATE queue_messages SET processed_at = NOW() - interval '2 hours' WHERE queue_name = $1 AND message_id = $2",
        ['cleanup-retention-queue', firstId]
      );
      await pool.query(
        "UPDATE queue_messages SET processed_at = NOW() - interval '30 minutes' WHERE queue_name = $1 AND message_id = $2",
        ['cleanup-retention-queue', secondId]
      );

      // Verify how many rows qualify as older than 1 hour
      const olderCountRes = await pool.query(
        "SELECT COUNT(*)::int AS cnt FROM queue_messages WHERE queue_name = $1 AND status = 'completed' AND processed_at < NOW() - interval '1 hour'",
        ['cleanup-retention-queue']
      );
      const olderCount: number = olderCountRes.rows[0].cnt;

      const deletedOld = await connection.cleanupCompletedMessages(1);
      expect(deletedOld).toBe(olderCount);

      let messages = await connection.listMessages('cleanup-retention-queue');
      expect(messages).toHaveLength(2 - deletedOld);

      await pool.query(
        "UPDATE queue_messages SET processed_at = NOW() - interval '48 hours' WHERE queue_name = $1 AND message_id = $2",
        ['cleanup-retention-queue', secondId]
      );

      const deletedDefault = await connection.cleanupCompletedMessages();
      // One more message should be deleted with default retention
      expect(deletedDefault).toBe(1);

      messages = await connection.listMessages('cleanup-retention-queue');
      expect(messages).toHaveLength(1 - deletedOld);

      await connection.deleteQueue('cleanup-retention-queue');
      await connection.cleanUpTopic('cleanup-retention-topic');
      await connection.deleteTopic('cleanup-retention-topic');
    }, 30000);
  });

  describe('Graceful Shutdown', () => {
    test('should wait for consumers to finish during shutdown', async () => {
      await connection.createTopic('shutdown-topic');
      await connection.createQueue('shutdown-queue', 'shutdown-topic', false);
      await connection.publish('shutdown-topic', { test: 'shutdown' });

      const consumer = connection.consume('shutdown-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Start shutdown (don't await yet)
      const closePromise = connection.close();

      // Message should still be processable
      await message.ack();

      // Now shutdown should complete
      await closePromise;
    });

    test('should close quickly even with unprocessed messages', async () => {
      const conn = new Connection({
        connectionString: testDb.connectionString,
        shutdownTimeoutMs: 1000 // 1 second timeout
      });

      await conn.connect();
      await conn.createTopic('timeout-topic');
      await conn.createQueue('timeout-queue', 'timeout-topic', false);
      await conn.publish('timeout-topic', { test: 'timeout' });

      const consumer = conn.consume('timeout-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Start shutdown - should complete quickly by releasing the message
      const start = Date.now();
      await conn.close();
      const elapsed = Date.now() - start;

      // Should complete quickly (within shutdown timeout)
      expect(elapsed).toBeLessThan(2000);
      expect(conn.isClientShuttingDown()).toBe(true);
    });
  });
});
