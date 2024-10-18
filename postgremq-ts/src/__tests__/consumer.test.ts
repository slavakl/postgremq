/**
 * Consumer Unit Tests
 * Tests consumer behavior, batching, auto-extension, and shutdown
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import {
  TestDatabase,
  createIsolatedTestConnection,
  generateTestPayload,
  sleep,
  waitFor
} from './helpers';
import { Connection } from '../connection';

describe('Consumer', () => {
  let testDb: TestDatabase;
  let connection: Connection;

  let dropDb: (() => Promise<void>) | null = null;

  beforeAll(async () => {
    testDb = new TestDatabase();
    await testDb.start();
  });

  beforeEach(async () => {
    const iso = await createIsolatedTestConnection(testDb);
    connection = iso.connection;
    dropDb = iso.dropDatabase;
    await connection.createTopic('consumer-test-topic');
    await connection.createQueue('consumer-test-queue', 'consumer-test-topic', false);
  });

  afterEach(async () => {
    if (connection) {
      await connection.close();
    }
    if (dropDb) {
      await dropDb();
      dropDb = null;
    }
  }, 20000);

  afterAll(async () => {
    if (testDb) {
      await testDb.stop();
    }
  });

  describe('Message Consumption', () => {
    test('should consume messages with correct batch size', async () => {
      // Publish 10 messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('consumer-test-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const received: any[] = [];

      // Consume all messages
      for (let i = 0; i < 10; i++) {
        const { value: message } = await messages.next();
        received.push(message);
        await message.ack();
      }

      await consumer.stop();

      expect(received.length).toBe(10);
    });

    test('should respect visibility timeout', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer1 = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 2, // 2 seconds
        autoExtension: { enabled: false } // Disable auto-extension so VT can actually expire
      });

      const messages1 = consumer1.messages();
      const { value: message1 } = await messages1.next();

      expect(message1).toBeDefined();
      expect(message1.deliveryAttempts).toBe(1);

      // Don't ack - let VT expire naturally without stopping consumer
      // Wait for VT to expire
      await sleep(2500);

      // Stop consumer1 after VT has expired
      await consumer1.stop();

      // Start another consumer - should get the message with incremented attempts
      const consumer2 = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      expect(message2).toBeDefined();
      expect(message2.id).toBe(message1.id);
      expect(message2.deliveryAttempts).toBe(2);

      await message2.ack();
      await consumer2.stop();
    });

    test('should auto-extend visibility timeout', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 3, // 3 seconds
        autoExtension: {
          enabled: true,
          extensionThreshold: 0.5, // Extend at 50%
          extensionSec: 3
        }
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      const initialVT = message.vt;

      // Wait past 50% of VT (should trigger auto-extension)
      await sleep(2000);

      // VT should have been extended
      // We can't directly observe the extension, but message should still be "locked"
      // Try to consume with another consumer - shouldn't get it
      const consumer2 = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      // Should timeout waiting for message (it's still locked)
      const messages2 = consumer2.messages();

      let receivedInTime = false;
      const racePromise = Promise.race([
        messages2.next().then(() => { receivedInTime = true; }),
        sleep(1000)
      ]);

      await racePromise;

      expect(receivedInTime).toBe(false); // Should NOT have received message

      // Clean up
      await message.ack();
      await consumer.stop();
      await consumer2.stop();
    });

    test('should disable auto-extension when configured', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 2,
        autoExtension: {
          enabled: false,
          extensionThreshold: 0.5,
          extensionSec: 30
        }
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Wait for VT to expire (no auto-extension should happen)
      await sleep(2500);

      // Message should now be available to another consumer
      const consumer2 = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages2 = consumer2.messages();
      const { value: message2 } = await messages2.next();

      expect(message2).toBeDefined();
      expect(message2.id).toBe(message.id);

      await message2.ack();
      await consumer.stop();
      await consumer2.stop();
    });
  });

  describe('Shutdown Behavior', () => {
    test('should stop gracefully and release buffered messages', async () => {
      // Publish multiple messages
      for (let i = 0; i < 5; i++) {
        await connection.publish('consumer-test-topic', generateTestPayload(i));
      }

      await sleep(100);

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30
      });

      // Let consumer fetch but don't process
      await sleep(500);

      // Stop consumer - should release buffered messages
      await consumer.stop();

      // Messages should be available again
      const stats = await connection.getQueueStatistics('consumer-test-queue');
      expect(stats.pendingCount).toBeGreaterThan(0);
    });

    test('should complete in-flight messages before stopping', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Start stop (don't await)
      const stopPromise = consumer.stop();

      // Message should still be processable
      await message.ack();

      // Stop should complete
      await stopPromise;

      const stats = await connection.getQueueStatistics('consumer-test-queue');
      expect(stats.completedCount).toBe(1);
    });

    test('should handle rapid stop/start', async () => {
      for (let i = 0; i < 3; i++) {
        const consumer = connection.consume('consumer-test-queue', {
          batchSize: 1,
          visibilityTimeoutSec: 30
        });

        await sleep(100);
        await consumer.stop();
      }

      // Should not throw or leave dangling resources
      expect(true).toBe(true);
    });
  });

  describe('Event Notifications', () => {
    test('should react to new message notifications', async () => {
      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30,
        pollingIntervalMs: 5000 // Long polling interval
      });

      const messages = consumer.messages();

      // Start consuming (no messages yet)
      const messagePromise = messages.next();

      // Publish a message after consumer is waiting
      await sleep(500);
      await connection.publish('consumer-test-topic', generateTestPayload());

      // Should receive message quickly (via notification, not polling)
      const start = Date.now();
      const { value: message } = await messagePromise;
      const elapsed = Date.now() - start;

      expect(message).toBeDefined();
      expect(elapsed).toBeLessThan(2000); // Should be much faster than polling interval

      await message.ack();
      await consumer.stop();
    });
  });

  describe('Concurrent Consumers', () => {
    test.skip('should distribute messages across multiple consumers', async () => {
      // Publish 10 messages
      for (let i = 0; i < 10; i++) {
        await connection.publish('consumer-test-topic', generateTestPayload(i));
      }

      await sleep(100);

      // Create 2 consumers
      const consumer1 = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30
      });

      const consumer2 = connection.consume('consumer-test-queue', {
        batchSize: 5,
        visibilityTimeoutSec: 30
      });

      const received: Set<number> = new Set();
      const consumer1Messages: any[] = [];
      const consumer2Messages: any[] = [];

      // Consume concurrently with timeout
      const consumeWithTimeout = async (consumer: any, name: string, storage: any[]) => {
        const messages = consumer.messages();
        const endTime = Date.now() + 3000; // 3 second timeout

        while (Date.now() < endTime && received.size < 10) {
          try {
            const result = await Promise.race([
              messages.next(),
              sleep(500).then(() => ({ done: true, value: undefined }))
            ]);

            if (result.done || !result.value) {
              break;
            }

            // Only process if we haven't seen this message yet
            if (!received.has(result.value.id)) {
              received.add(result.value.id);
              storage.push(result.value);
              await result.value.ack();
            }
          } catch (error) {
            // Ignore errors and continue
            break;
          }
        }
      };

      // Run both consumers
      await Promise.all([
        consumeWithTimeout(consumer1, 'consumer1', consumer1Messages),
        consumeWithTimeout(consumer2, 'consumer2', consumer2Messages)
      ]);

      // Stop consumers
      await consumer1.stop();
      await consumer2.stop();

      // Verify all 10 messages were consumed
      expect(received.size).toBe(10);

      // Both consumers should have received at least some messages
      expect(consumer1Messages.length).toBeGreaterThan(0);
      expect(consumer2Messages.length).toBeGreaterThan(0);

      // Total should be 10
      expect(consumer1Messages.length + consumer2Messages.length).toBe(10);
    }, 15000);
  });

  describe('Error Handling', () => {
    test('should handle database connection errors gracefully', async () => {
      await connection.publish('consumer-test-topic', generateTestPayload());

      const consumer = connection.consume('consumer-test-queue', {
        batchSize: 1,
        visibilityTimeoutSec: 30
      });

      const messages = consumer.messages();
      const { value: message } = await messages.next();

      // Close connection while consumer is active
      await connection.close();

      // Mark connection as null so afterEach doesn't try to close it again
      connection = null as any;

      // Consumer should handle this gracefully
      // Message may have been released during connection.close() or connection may be closed
      try {
        await message.ack();
        // If it doesn't throw, that's unexpected
        expect(true).toBe(false);
      } catch (error: any) {
        const validErrors = [
          'not connected',
          'already been processed'  // Message was released during connection close
        ];
        const hasValidError = validErrors.some(msg => error.message.includes(msg));
        expect(hasValidError).toBe(true);
      }
    });
  });
});
