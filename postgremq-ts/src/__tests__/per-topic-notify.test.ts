/**
 * Tests for the per-topic / per-queue NOTIFY refactor.
 */

import { describe, test, expect, beforeAll, afterAll } from '@jest/globals';
import { TestDatabase, createIsolatedTestConnection, getSharedTestDatabase, sleep, waitFor } from './helpers';
import { Connection } from '../connection';

// Test-only peek into the connection's per-channel subscription map.
// connection.ts intentionally does not expose this on the public surface;
// tests poke the private field via a typed cast.
type ChannelState = { refCount: number; listening: boolean };
function channelState(conn: Connection, channel: string): ChannelState | undefined {
  return ((conn as any).channelStates as Map<string, ChannelState>).get(channel);
}

describe('Per-topic NOTIFY', () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await getSharedTestDatabase();
  });

  afterAll(async () => {
    if (db) await db.stop();
  });

  test('cache hit: createQueue then consume needs no DB lookup', async () => {
    const { connection, dropDatabase, pool } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('TopicCacheHit');
      await connection.createQueue('QueueCacheHit', 'TopicCacheHit', false, { maxDeliveryAttempts: 0 });

      // Drop the queue at the SQL level so a DB lookup would fail. The
      // cache entry from createQueue lets consume() succeed regardless.
      await pool.query("DELETE FROM queues WHERE name = 'QueueCacheHit'");

      const consumer = connection.consume('QueueCacheHit', { visibilityTimeoutSec: 30 });
      // Force the iterator to start so the topic resolution and subscribe
      // run; iterator should not throw.
      const it = consumer.messages();
      // Trigger the lazy start, then immediately stop.
      await Promise.race([it.next(), sleep(200)]);
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  test('out-of-band queue requires explicit topic option (no DB lookup)', async () => {
    const { connection, dropDatabase, pool } = await createIsolatedTestConnection(db);
    try {
      // Queue created out of band: not via this Connection's createQueue, so
      // the topic cache is empty for this queue.
      await pool.query("SELECT create_topic('TopicOutOfBand')");
      await pool.query("SELECT create_queue('QueueOutOfBand', 'TopicOutOfBand', 0, false, 30)");

      // Without explicit topic, starting iteration must throw — no DB fallback.
      const noTopicConsumer = connection.consume('QueueOutOfBand', { visibilityTimeoutSec: 30 });
      expect(() => noTopicConsumer.messages()).toThrow();

      // With explicit topic, it works.
      const consumer = connection.consume('QueueOutOfBand', {
        visibilityTimeoutSec: 30,
        topic: 'TopicOutOfBand',
      });
      const it = consumer.messages();
      await Promise.race([it.next(), sleep(200)]);
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  test('explicit topic option bypasses cache and DB lookup', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      const consumer = connection.consume('GhostQueue', {
        visibilityTimeoutSec: 30,
        topic: 'ExplicitTopic'
      });
      const it = consumer.messages();
      await Promise.race([it.next(), sleep(200)]);
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  test('refcount sharing: two consumers on same topic share one physical LISTEN', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('RefcountTopic');
      await connection.createQueue('RefA', 'RefcountTopic', false, { maxDeliveryAttempts: 0 });
      await connection.createQueue('RefB', 'RefcountTopic', false, { maxDeliveryAttempts: 0 });

      const c1 = connection.consume('RefA', { visibilityTimeoutSec: 30 });
      const c2 = connection.consume('RefB', { visibilityTimeoutSec: 30 });
      const it1 = c1.messages();
      const it2 = c2.messages();
      // Kick off start; iterators settle without messages.
      await Promise.race([it1.next(), sleep(50)]);
      await Promise.race([it2.next(), sleep(50)]);

      // Wait for both subscriptions to register.
      await waitFor(() => channelState(connection, 'pmq:t:RefcountTopic')?.refCount === 2, 2000, 25);

      const topicState = channelState(connection, 'pmq:t:RefcountTopic');
      expect(topicState).toBeDefined();
      expect(topicState!.refCount).toBe(2);

      expect(channelState(connection, 'pmq:q:RefA')!.refCount).toBe(1);
      expect(channelState(connection, 'pmq:q:RefB')!.refCount).toBe(1);

      await c1.stop();
      await waitFor(() => channelState(connection, 'pmq:t:RefcountTopic')?.refCount === 1, 2000, 25);

      await c2.stop();
      await waitFor(() => channelState(connection, 'pmq:t:RefcountTopic') === undefined, 2000, 25);
    } finally {
      await dropDatabase();
    }
  });

  test('reconnect re-issues LISTEN for all subscribed channels', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('ReconnectTopic');
      await connection.createQueue('ReconnectQueue', 'ReconnectTopic', false, { maxDeliveryAttempts: 0 });

      const consumer = connection.consume('ReconnectQueue', { visibilityTimeoutSec: 30, batchSize: 1 });
      const it = consumer.messages();
      // Settle subscription.
      await sleep(200);

      // Confirm the subscriptions are flagged listening.
      expect(channelState(connection, 'pmq:t:ReconnectTopic')!.listening).toBe(true);
      expect(channelState(connection, 'pmq:q:ReconnectQueue')!.listening).toBe(true);

      // Simulate notify client failure: emit an error event on the notify
      // client. This calls handleNotifyClientError which releases the client
      // and schedules a reconnect 1s later.
      const notifyClient = (connection as any).notifyClient;
      expect(notifyClient).not.toBeNull();
      notifyClient.emit('error', new Error('simulated disconnect'));

      // Wait for reconnect (handleNotifyClientError schedules reconnect after
      // 1000ms). Allow a generous margin.
      await waitFor(() => channelState(connection, 'pmq:t:ReconnectTopic')?.listening === true, 3000, 50);

      expect(channelState(connection, 'pmq:t:ReconnectTopic')!.listening).toBe(true);
      expect(channelState(connection, 'pmq:q:ReconnectQueue')!.listening).toBe(true);

      // End-to-end: publish should wake the consumer through the new client.
      await connection.publish('ReconnectTopic', { hello: 'reconnect' });
      const result = await Promise.race([
        it.next(),
        sleep(2000).then(() => ({ done: true, value: undefined as any })),
      ]);
      expect((result as IteratorResult<any>).done).toBe(false);
      const msg = (result as IteratorResult<any>).value as any;
      await msg.ack();
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });

  test('publish wakes consumer end-to-end via per-topic NOTIFY', async () => {
    const { connection, dropDatabase } = await createIsolatedTestConnection(db);
    try {
      await connection.createTopic('WakeTopic');
      await connection.createQueue('WakeQueue', 'WakeTopic', false, { maxDeliveryAttempts: 0 });

      const consumer = connection.consume('WakeQueue', { visibilityTimeoutSec: 30, batchSize: 1 });
      const it = consumer.messages();

      // Settle subscription, then publish; iterator should yield the message.
      await sleep(100);
      await connection.publish('WakeTopic', { hello: 'world' });

      const result = await Promise.race([
        it.next(),
        sleep(2000).then(() => ({ done: true, value: undefined as any })),
      ]);
      expect((result as IteratorResult<any>).done).toBe(false);
      const msg = (result as IteratorResult<any>).value as any;
      expect(msg).toBeDefined();
      await msg.ack();
      await consumer.stop();
    } finally {
      await dropDatabase();
    }
  });
});
