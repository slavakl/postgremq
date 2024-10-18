/**
 * PostgreMQ TypeScript Client
 * Connection management
 */

import { Pool, PoolClient, PoolConfig } from 'pg';
import {
  ConnectionOptions,
  Consumer,
  ConsumerOptions,
  DLQMessage,
  EventType,
  IConnection,
  MessageOptions,
  NotificationEvent,
  PublishOptions,
  PublishedMessage,
  QueueInfo,
  QueueMessage,
  QueueOptions,
  QueueStatistics,
  RetryPolicy,
  Transaction
} from './types';
import { Consumer as ConsumerImpl } from './consumer';
import { EventEmitter, DEFAULT_RETRY_POLICY, shouldRetry, sleep, withRetry } from './utils';

// Forward declaration for Consumer
// export class Consumer {
//   constructor(queueName: string, connection: Connection, options?: Partial<ConsumerOptions>) {}
//   async stop(): Promise<void> { return Promise.resolve(); }
// }

/**
 * Connection class for PostgreMQ client
 * Manages the connection to PostgreSQL and provides methods for
 * queue operations.
 */
export class Connection implements IConnection {
  /** PostgreSQL connection pool */
  private pool: Pool;
  
  /** Whether this connection owns the pool (should close it) */
  private ownsPool: boolean;

  /** Client for notifications */
  private notifyClient: PoolClient | null = null;
  
  /** Event emitter for notification events */
  private eventEmitter: EventEmitter = new EventEmitter();
  
  /** Flag indicating if client is shutting down */
  private isShuttingDown: boolean = false;
  
  /** Set of active consumers */
  private consumers: Set<ConsumerImpl> = new Set();
  
  /** Map of exclusive queue keep-alive timeouts */
  private exclusiveQueueTimers: Map<string, NodeJS.Timeout> = new Map();
  
  /** Map of exclusive queue keep-alive intervals in seconds */
  private exclusiveQueueIntervals: Map<string, number> = new Map();
  
  /** Retry policy for operations */
  private retryPolicy: RetryPolicy;
  
  /** Shutdown timeout in milliseconds */
  private shutdownTimeoutMs: number;
  
  /** Flag indicating if client is connected */
  private connected: boolean = false;
  
  /** Flag indicating if notification listener is active */
  private notificationListenerActive: boolean = false;
  private stoppingNotify: boolean = false;

  /**
   * Create a new connection manager
   * @param options - Connection options
   */
  constructor(options: ConnectionOptions = {}) {
    // Set up the connection pool
    if (options.pool) {
      // Use existing pool
      this.pool = options.pool;
      this.ownsPool = false;
    } else {
      // Create new pool from connection string or config
      const poolConfig: PoolConfig = options.config || {};
      
      if (options.connectionString) {
        poolConfig.connectionString = options.connectionString;
      }
      
      this.pool = new Pool(poolConfig);
      this.ownsPool = true;
    }
    
    // Set up retry policy
    this.retryPolicy = options.retry || DEFAULT_RETRY_POLICY;
    
    // Set shutdown timeout
    this.shutdownTimeoutMs = options.shutdownTimeoutMs || 30000;

    // Install a single pool-level error handler to avoid unhandled errors from idle clients
    try {
      const anyPool: any = this.pool as any;
      if (typeof anyPool.on === 'function' && !anyPool.__postgremqErrorHookInstalled) {
        const errorHandler = (err: any) => {
          const msg = err?.message ?? String(err);
          const expected =
            msg.includes('terminating connection') ||
            msg.includes('Connection terminated unexpectedly');
          if (expected) {
            console.debug('Pool client error during shutdown (ignored):', msg);
          } else {
            console.error('Pool client error:', err);
          }
        };
        anyPool.on('error', errorHandler);
        // Also attach to acquired clients so in-flight clients don't cause unhandled errors
        if (typeof anyPool.on === 'function') {
          anyPool.on('acquire', (client: any) => {
            try {
              if (!client.__postgremqClientErrorHookInstalled) {
                client.on('error', errorHandler);
                client.__postgremqClientErrorHookInstalled = true;
              }
            } catch {}
          });
        }
        anyPool.__postgremqErrorHookInstalled = true;
      }
    } catch {
      // best-effort; do not fail constructor
    }
  }

  /**
   * Connect to the database and start the notification listener
   * @returns Promise that resolves when connected
   */
  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    
    try {
      // Test the connection by getting a client and releasing it
      const client = await this.pool.connect();
      await client.query('SELECT 1');
      client.release();
      
      this.connected = true;
    } catch (error: any) {
      throw new Error(`Failed to connect to PostgreSQL: ${error.message}`);
    }
  }

  /**
   * Close the connection and clean up resources
   * @returns Promise that resolves when disconnected
   */
  async close(): Promise<void> {
    if (!this.connected || this.isShuttingDown) {
      return;
    }

    try {
      this.isShuttingDown = true;

      // Stop all consumers BEFORE setting isShuttingDown
      // This allows consumers to complete their shutdown process
      const consumerStopPromises = Array.from(this.consumers).map(consumer => {
        return consumer.stop().catch(error => {
          console.error(`Error stopping consumer: ${error}`);
        });
      });

      // Wait for consumers to stop with timeout
      let timeoutId: NodeJS.Timeout | null = null;
      const timeoutPromise = new Promise<void>((resolve) => {
        timeoutId = setTimeout(resolve, this.shutdownTimeoutMs);
      });
      await Promise.race([Promise.all(consumerStopPromises), timeoutPromise]);
      if (timeoutId) clearTimeout(timeoutId);

      // Clear all exclusive queue keep-alive timers
      for (const [queueName, timer] of this.exclusiveQueueTimers.entries()) {
        clearTimeout(timer);
      }
      this.exclusiveQueueTimers.clear();
      this.exclusiveQueueIntervals.clear();
      
      // Close notification listener
      await this.stopNotificationListener();
      
      // Close the pool if we own it (with timeout to avoid hangs under load)
      if (this.ownsPool) {
        let poolTimeoutId: NodeJS.Timeout | null = null;
        const poolTimeout = new Promise<void>((resolve) => {
          poolTimeoutId = setTimeout(resolve, this.shutdownTimeoutMs);
        });
        await Promise.race([this.pool.end(), poolTimeout]);
        if (poolTimeoutId) clearTimeout(poolTimeoutId);
      }
      
      // Clean up event listeners
      this.eventEmitter.removeAllListeners();

      this.connected = false;
      // Keep isShuttingDown true - connection cannot be reused after close
    } catch (error: any) {
      const message = error?.message ?? String(error);
      const isExpectedShutdownError =
        message.includes('terminating connection') ||
        message.includes('Connection terminated unexpectedly') ||
        message.includes('Connection close timeout');

      if (isExpectedShutdownError) {
        console.warn(`Ignoring shutdown error during connection close: ${message}`);
        this.connected = false;
        return;
      }

      console.error(`Error during connection close: ${message}`);
      throw error;
    }
  }

  /**
   * Start the notification listener
   * @internal
   * @returns Promise that resolves when listener is started
   */
  private async startNotificationListener(): Promise<void> {
    if (this.notifyClient || this.isShuttingDown || this.notificationListenerActive) {
      return;
    }
    
    try {
      // Get a dedicated client for notifications
      this.notifyClient = await this.pool.connect();
      
      // Set up notification handling
      this.notifyClient.on('notification', msg => {
        try {
          // Parse the payload
          const notification = JSON.parse(msg.payload!) as NotificationEvent;

          // Emit event to listeners
          this.eventEmitter.emit(msg.channel, notification);

          // If this is a queue notification, also emit to specific queue channels
          if (notification.queues && notification.queues.length > 0) {
            for (const queue of notification.queues) {
              this.eventEmitter.emit(`queue:${queue}`, notification);
            }
          }
        } catch (error) {
          console.error('Failed to parse notification:', {
            channel: msg.channel,
            payload: msg.payload,
            error: error instanceof Error ? error.message : String(error)
          });
          // Emit parse error event for monitoring
          this.eventEmitter.emit('notification-error', {
            channel: msg.channel,
            payload: msg.payload,
            error
          });
        }
      });
      
      // Listen for message events on the postgremq_events channel
      await this.notifyClient.query('LISTEN postgremq_events');
      
      // Set up error handling
      this.notifyClient.on('error', (error) => {
        this.handleNotifyClientError(error);
      });
      
      this.notificationListenerActive = true;
    } catch (error: any) {
      if (this.notifyClient) {
        this.notifyClient.release();
        this.notifyClient = null;
      }
      
      this.notificationListenerActive = false;
      throw new Error(`Failed to start notification listener: ${error.message}`);
    }
  }
  
  /**
   * Handle errors from the notification client
   * @internal
   * @param error - The error that occurred
   */
  private handleNotifyClientError(error: any): void {
    if (this.isShuttingDown) {
      // Expected during shutdown when connections are terminated by server
      console.debug('Notification listener error during shutdown (ignored):', error?.message ?? error);
    } else {
      console.error('Notification listener error:', error);
    }
    
    // Clean up
    if (this.notifyClient) {
      try {
        this.notifyClient.release();
      } catch (releaseError) {
        console.error('Error releasing notification client:', releaseError);
      }
      this.notifyClient = null;
    }
    
    this.notificationListenerActive = false;
    
    // Use setTimeout to avoid potential recursion
    if (!this.isShuttingDown && this.consumers.size > 0 && !this.stoppingNotify) {
      setTimeout(() => {
        if (!this.isShuttingDown && this.consumers.size > 0 && !this.stoppingNotify) {
          this.startNotificationListener().catch(err => {
            console.error('Failed to restart notification listener:', err);
          });
        }
      }, 1000);
    }
  }

  /**
   * Register a consumer with this connection
   * @internal
   * @param consumer - The consumer to register
   */
  registerConsumer(consumer: ConsumerImpl): void {
    this.consumers.add(consumer);
  }

  /**
   * Unregister a consumer from this connection
   * @internal
   * @param consumer - The consumer to unregister
   */
  unregisterConsumer(consumer: ConsumerImpl): void {
    this.consumers.delete(consumer);
    // If there are no more consumers, proactively stop the notification listener
    // to avoid lingering clients and open handles under heavy load.
    if (this.consumers.size === 0) {
      this.stopNotificationListener().catch(err => {
        // Safe to ignore during normal shutdown
        console.debug('Error stopping notification listener (ignored):', err);
      });
    }
  }

  /**
   * Stop the notification listener if running.
   * Ensures the dedicated client is unlistened, listeners removed, and released.
   */
  private async stopNotificationListener(): Promise<void> {
    if (this.stoppingNotify) return;
    this.stoppingNotify = true;
    const client = this.notifyClient;
    if (!client) {
      this.notificationListenerActive = false;
      this.stoppingNotify = false;
      return;
    }
    try {
      try {
        await client.query('UNLISTEN *');
      } catch {}
      // Remove all handlers to avoid error storms during shutdown
      try { client.removeAllListeners('notification'); } catch {}
      try { client.removeAllListeners('error'); } catch {}
      try { client.release(); } catch {}
    } finally {
      if (this.notifyClient === client) {
        this.notifyClient = null;
      }
      this.notificationListenerActive = false;
      this.stoppingNotify = false;
    }
  }

  /**
   * Subscribe to events for a specific queue
   * @internal
   * @param queueName - The queue name to subscribe to
   * @param callback - The callback function to invoke on events
   * @returns Unsubscribe function
   */
  subscribeToQueue(queueName: string, callback: (event: NotificationEvent) => void): () => void {
    const channel = `queue:${queueName}`;
    this.eventEmitter.on(channel, callback);
    return () => this.eventEmitter.off(channel, callback);
  }

  /**
   * Execute an operation with retry logic
   * @internal
   * @param operation - The operation to execute
   * @param retryPolicy - The retry policy to use
   * @returns The result of the operation
   */
  async executeWithRetry<T>(
    operation: (client: PoolClient) => Promise<T>,
    retryPolicy: RetryPolicy = this.retryPolicy
  ): Promise<T> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return withRetry(async () => {
      const client = await this.pool.connect();
      try {
        return await operation(client);
      } finally {
        client.release();
      }
    }, retryPolicy);
  }

  /**
   * Publish a message to a topic
   * @param topic - The topic name
   * @param payload - The message payload
   * @param options - Publishing options
   * @returns Promise resolving to the message ID
   */
  async publish(
    topic: string,
    payload: any,
    options: PublishOptions = {}
  ): Promise<number> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      let query: string;
      let params: any[];
      
      if (options.deliverAfter) {
        query = 'SELECT publish_message($1, $2, $3) as publish_message';
        params = [topic, JSON.stringify(payload), options.deliverAfter];
      } else {
        query = 'SELECT publish_message($1, $2) as publish_message';
        params = [topic, JSON.stringify(payload)];
      }
      
      const result = await client.query(query, params);
      return result.rows[0].publish_message;
    });
  }

  /**
   * Create a consumer for a queue
   * @param queue - The queue name
   * @param options - Consumer options
   * @returns A new consumer instance
   */
  consume(queue: string, options: Partial<ConsumerOptions> = {}): ConsumerImpl {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    // Start notification listener if it's not already active
    if (!this.notificationListenerActive) {
      this.startNotificationListener().catch(error => {
        console.error('Failed to start notification listener:', error);
      });
    }
    
    const consumer = new ConsumerImpl(queue, this, options);
    this.registerConsumer(consumer);
    return consumer;
  }

  /**
   * Create a new topic
   * @param topic - The topic name
   * @returns Promise that resolves when the topic is created
   */
  async createTopic(topic: string): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT create_topic($1)', [topic]);
    });
  }

  /**
   * Create a new queue
   * @param name - The queue name
   * @param topic - The topic name
   * @param exclusive - Whether the queue is exclusive (non-durable)
   * @param options - Queue options
   * @returns Promise that resolves when the queue is created
   */
  async createQueue(
    name: string,
    topic: string,
    exclusive: boolean = false,
    options: QueueOptions = {}
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }

    await this.executeWithRetry(async (client) => {
      // Always pass all parameters to avoid parameter index misalignment
      const maxDeliveryAttempts = options.maxDeliveryAttempts ?? 0;
      const keepAliveSeconds = options.keepAliveSeconds ?? 30;

      await client.query(
        'SELECT create_queue($1, $2, $3, $4, $5)',
        [name, topic, maxDeliveryAttempts, exclusive, keepAliveSeconds]
      );

      // If this is an exclusive queue with a keep-alive period, set up the keep-alive timer
      if (exclusive && options.keepAliveSeconds) {
        this.startQueueKeepAlive(name, options.keepAliveSeconds);
      }
    });
  }

  /**
   * Delete a topic
   * @param topic - The topic name
   * @returns Promise that resolves when the topic is deleted
   */
  async deleteTopic(topic: string): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT delete_topic($1)', [topic]);
    });
  }

  /**
   * Delete a queue
   * @param queue - The queue name
   * @returns Promise that resolves when the queue is deleted
   */
  async deleteQueue(queue: string): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    // Cancel any keep-alive timer for this queue
    this.stopQueueKeepAlive(queue);
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT delete_queue($1)', [queue]);
    });
  }

  /**
   * List all topics
   * @returns Promise that resolves to a list of topic names
   */
  async listTopics(): Promise<string[]> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM list_topics()');
      return result.rows.map(row => row.topic);
    });
  }

  /**
   * List all queues
   * @returns Promise that resolves to a list of queue information
   */
  async listQueues(): Promise<QueueInfo[]> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM list_queues()');
      
      return result.rows.map(row => ({
        queueName: row.queue_name,
        topicName: row.topic_name,
        maxDeliveryAttempts: row.max_delivery_attempts,
        exclusive: row.exclusive,
        keepAliveUntil: row.keep_alive_until
      }));
    });
  }

  /**
   * Get statistics for a queue
   * @param queue - Optional queue name (if omitted, gets stats for all queues)
   * @returns Promise that resolves to queue statistics
   */
  async getQueueStatistics(queue?: string): Promise<QueueStatistics> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT * FROM get_queue_statistics($1)',
        [queue || null]
      );
      
      if (result.rows.length === 0) {
        return {
          pendingCount: 0,
          processingCount: 0,
          completedCount: 0,
          totalCount: 0
        };
      }
      
      const row = result.rows[0];
      return {
        pendingCount: parseInt(row.pending_count, 10),
        processingCount: parseInt(row.processing_count, 10),
        completedCount: parseInt(row.completed_count, 10),
        totalCount: parseInt(row.total_count, 10)
      };
    });
  }

  /**
   * Move messages from active queues to DLQ if they exceed max delivery attempts
   * @returns Promise that resolves to the number of messages moved
   */
  async moveToDLQ(): Promise<number> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT move_messages_to_dlq() as moved_count');
      return result.rows[0].moved_count;
    });
  }

  /**
   * List messages in the Dead Letter Queue
   * @returns Promise that resolves to a list of DLQ messages
   */
  async listDLQMessages(): Promise<DLQMessage[]> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query('SELECT * FROM list_dlq_messages()');

      return result.rows.map(row => ({
        queueName: row.queue_name,
        messageId: row.message_id,
        retryCount: row.retry_count,
        publishedAt: row.published_at
      }));
    });
  }

  /**
   * Move messages from the Dead Letter Queue back to their original queue
   * @param queue - The queue name
   * @returns Promise that resolves when messages are requeued
   */
  async requeueDLQMessages(queue: string): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT requeue_dlq_messages($1)', [queue]);
    });
  }

  /**
   * Purge all messages from the Dead Letter Queue
   * @returns Promise that resolves when DLQ is purged
   */
  async purgeDLQ(): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT purge_dlq()');
    });
  }

  /**
   * Purge all messages from the system
   * @returns Promise that resolves when all messages are purged
   */
  async purgeAllMessages(): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT purge_all_messages()');
    });
  }

  /**
   * Delete a specific message from a queue
   * @param queue - The queue name
   * @param messageID - The message ID
   * @returns Promise that resolves when the message is deleted
   */
  async deleteQueueMessage(queue: string, messageID: number): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query(
        'SELECT delete_queue_message($1, $2)',
        [queue, messageID]
      );
    });
  }

  /**
   * List all messages in a queue
   * @param queue - The queue name
   * @returns Promise that resolves to a list of queue messages
   */
  async listMessages(queue: string): Promise<QueueMessage[]> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT * FROM list_messages($1)',
        [queue]
      );
      
      return result.rows.map(row => ({
        messageId: row.message_id,
        status: row.status,
        publishedAt: row.published_at,
        deliveryAttempts: row.delivery_attempts,
        vt: row.vt,
        processedAt: row.processed_at
      }));
    });
  }

  /**
   * Get a specific message by ID
   * @param messageID - The message ID
   * @returns Promise that resolves to the message information or null if not found
   */
  async getMessage(messageID: number): Promise<PublishedMessage | null> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT * FROM get_message($1)',
        [messageID]
      );
      
      if (result.rows.length === 0) {
        return null;
      }
      
      const row = result.rows[0];
      return {
        messageId: row.message_id,
        topicName: row.topic_name,
        payload: row.payload,
        publishedAt: row.published_at
      };
    });
  }

  /**
   * Consume messages from a queue
   * @internal
   * @param queueName - The queue name
   * @param visibilityTimeout - Visibility timeout in seconds
   * @param limit - Maximum number of messages to fetch
   * @returns Promise resolving to array of raw messages
   */
  async consumeMessages(
    queueName: string,
    visibilityTimeout: number,
    limit: number = 1
  ): Promise<any[]> {
    if (!this.connected || this.isShuttingDown) {
      throw new Error('Client is not connected or shutting down');
    }

    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT * FROM consume_message($1, $2, $3)',
        [queueName, visibilityTimeout, limit]
      );

      return result.rows;
    });
  }

  /**
   * Acknowledge a message
   * @internal
   * @param queueName - The queue name
   * @param messageId - The message ID
   * @param consumerToken - The consumer token
   * @param tx - Optional transaction object
   * @returns Promise that resolves when the message is acknowledged
   */
  async ackMessage(
    queueName: string,
    messageId: number,
    consumerToken: string,
    tx?: Transaction
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    if (tx) {
      await tx.query('SELECT ack_message($1, $2, $3)', [
        queueName,
        messageId,
        consumerToken
      ]);
      return;
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT ack_message($1, $2, $3)', [
        queueName,
        messageId,
        consumerToken
      ]);
    });
  }

  /**
   * Negatively acknowledge a message
   * @internal
   * @param queueName - The queue name
   * @param messageId - The message ID
   * @param consumerToken - The consumer token
   * @param delayUntil - Optional delay until the message is visible again
   * @returns Promise that resolves when the message is negative acknowledged
   */
  async nackMessage(
    queueName: string,
    messageId: number,
    consumerToken: string,
    delayUntil?: Date
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      if (delayUntil) {
        await client.query('SELECT nack_message($1, $2, $3, $4)', [
          queueName,
          messageId,
          consumerToken,
          delayUntil
        ]);
      } else {
        await client.query('SELECT nack_message($1, $2, $3)', [
          queueName,
          messageId,
          consumerToken
        ]);
      }
    });
  }

  /**
   * Release a message back to the queue
   * @internal
   * @param queueName - The queue name
   * @param messageId - The message ID
   * @param consumerToken - The consumer token
   * @returns Promise that resolves when the message is released
   */
  async releaseMessage(
    queueName: string,
    messageId: number,
    consumerToken: string
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT release_message($1, $2, $3)', [
        queueName,
        messageId,
        consumerToken
      ]);
    });
  }

  /**
   * Set the visibility timeout for a message
   * @internal
   * @param queueName - The queue name
   * @param messageId - The message ID
   * @param consumerToken - The consumer token
   * @param visibilityTimeout - The new visibility timeout in seconds
   * @returns Promise that resolves to the new expiration date
   */
  async setMessageVt(
    queueName: string,
    messageId: number,
    consumerToken: string,
    visibilityTimeout: number
  ): Promise<Date> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT set_vt($1, $2, $3, $4) AS new_vt',
        [queueName, messageId, consumerToken, visibilityTimeout]
      );
      
      return new Date(result.rows[0].new_vt);
    });
  }

  /**
   * Set the visibility timeout for multiple messages
   * @internal
   * @param queueName - The queue name
   * @param messageIds - Array of message IDs
   * @param consumerTokens - Array of consumer tokens
   * @param visibilityTimeout - The new visibility timeout in seconds
   * @returns Promise that resolves to array of [messageId, new expiration date] tuples
   */
  async setMessagesVtBatch(
    queueName: string,
    messageIds: number[],
    consumerTokens: string[],
    visibilityTimeout: number
  ): Promise<Array<[number, Date]>> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT * FROM set_vt_batch($1, $2, $3, $4)',
        [queueName, messageIds, consumerTokens, visibilityTimeout]
      );
      
      return result.rows.map(row => [
        row.message_id,
        new Date(row.new_vt)
      ]);
    });
  }

  /**
   * Get the next time a message will become visible
   * @internal
   * @param queueName - The queue name
   * @returns Promise that resolves to the next visible time or null if no messages
   */
  async getNextVisibleTime(queueName: string): Promise<Date | null> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT get_next_visible_time($1) AS next_time',
        [queueName]
      );
      
      const nextTime = result.rows[0].next_time;
      return nextTime ? new Date(nextTime) : null;
    });
  }

  /**
   * Extend the keep-alive time for an exclusive queue
   * @param queueName - The queue name
   * @param seconds - The number of seconds to extend by
   * @returns Promise that resolves to a boolean indicating success
   */
  async extendQueueKeepAlive(queueName: string, seconds: number): Promise<boolean> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    return this.executeWithRetry(async (client) => {
      const result = await client.query(
        'SELECT extend_queue_keep_alive($1, make_interval(secs => $2)) AS success',
        [queueName, seconds]
      );
      
      return result.rows[0].success;
    });
  }

  /**
   * Clean up a queue by removing all messages
   * @param queue - The queue name
   * @returns Promise that resolves when the queue is cleaned up
   */
  async cleanUpQueue(queue: string): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT clean_up_queue($1)', [queue]);
    });
  }

  /**
   * Clean up a topic by removing all associated messages
   * @param topic - The topic name
   * @returns Promise that resolves when the topic is cleaned up
   */
  async cleanUpTopic(topic: string): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT clean_up_topic($1)', [topic]);
    });
  }

  /**
   * Delete inactive (expired) exclusive queues
   * @returns Promise that resolves when inactive queues are deleted
   */
  async deleteInactiveQueues(): Promise<void> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }
    
    await this.executeWithRetry(async (client) => {
      await client.query('SELECT delete_inactive_queues()');
    });
  }

  /**
   * Remove completed messages older than the specified retention period.
   * @param olderThanHours - Optional retention threshold in hours. Uses default when omitted.
   * @returns Number of messages deleted.
   */
  async cleanupCompletedMessages(olderThanHours?: number): Promise<number> {
    if (!this.connected) {
      throw new Error('Client is not connected');
    }

    return this.executeWithRetry(async (client) => {
      const result = olderThanHours !== undefined
        ? await client.query('SELECT cleanup_completed_messages($1) AS deleted', [olderThanHours])
        : await client.query('SELECT cleanup_completed_messages() AS deleted');

      return Number(result.rows[0].deleted);
    });
  }

  /**
   * Check if client is shutting down
   * @returns True if the client is in the process of shutting down
   */
  isClientShuttingDown(): boolean {
    return this.isShuttingDown;
  }

  /**
   * Start keep-alive timer for an exclusive queue
   * @internal
   * @param queueName - The queue name
   * @param keepAliveSeconds - The keep-alive period in seconds
   */
  private startQueueKeepAlive(queueName: string, keepAliveSeconds: number): void {
    // Stop any existing timer for this queue
    this.stopQueueKeepAlive(queueName);
    
    // Store the keep-alive interval
    this.exclusiveQueueIntervals.set(queueName, keepAliveSeconds);
    
    // Calculate when to send the keep-alive (at half the keep-alive period)
    const extendIntervalMs = Math.floor(keepAliveSeconds * 1000 / 2);
    
    // Set up the timer
    const timer = setTimeout(() => {
      this.sendQueueKeepAlive(queueName);
    }, extendIntervalMs);
    
    // Store the timer reference
    this.exclusiveQueueTimers.set(queueName, timer);
  }
  
  /**
   * Stop keep-alive timer for a queue
   * @internal
   * @param queueName - The queue name
   */
  private stopQueueKeepAlive(queueName: string): void {
    const timer = this.exclusiveQueueTimers.get(queueName);
    if (timer) {
      clearTimeout(timer);
      this.exclusiveQueueTimers.delete(queueName);
      this.exclusiveQueueIntervals.delete(queueName);
    }
  }
  
  /**
   * Send a keep-alive for an exclusive queue
   * @internal
   * @param queueName - The queue name
   * @param retryCount - Number of consecutive retry attempts (internal use)
   */
  private async sendQueueKeepAlive(queueName: string, retryCount: number = 0): Promise<void> {
    // Get the keep-alive interval for this queue
    const keepAliveSeconds = this.exclusiveQueueIntervals.get(queueName);
    if (!keepAliveSeconds || this.isShuttingDown || !this.connected) {
      return;
    }
    
    // Maximum number of quick retries before going back to normal schedule
    const MAX_QUICK_RETRIES = 3;
    // Delay for quick retries in milliseconds
    const QUICK_RETRY_DELAY_MS = 5000; // 5 seconds
    
    try {
      // Extend the keep-alive
      const success = await this.extendQueueKeepAlive(queueName, keepAliveSeconds);
      
      if (success) {
        if (retryCount > 0) {
          console.info(`Successfully extended keep-alive for exclusive queue ${queueName} after ${retryCount} retries`);
        } else {
          console.debug(`Extended keep-alive for exclusive queue ${queueName} by ${keepAliveSeconds} seconds`);
        }
        
        // Reset retry count and schedule next keep-alive at normal interval
        retryCount = 0;
        
        // Schedule the next keep-alive at normal interval (half the keep-alive period)
        if (!this.isShuttingDown) {
          const extendIntervalMs = Math.floor(keepAliveSeconds * 1000 / 2);
          const timer = setTimeout(() => {
            this.sendQueueKeepAlive(queueName, 0);
          }, extendIntervalMs);
          this.exclusiveQueueTimers.set(queueName, timer);
        }
      } else {
        console.warn(`Failed to extend keep-alive for exclusive queue ${queueName}`);
        
        // If we've reached max retries, stop the keep-alive timer
        if (retryCount >= MAX_QUICK_RETRIES) {
          console.error(`Giving up on extending keep-alive for exclusive queue ${queueName} after ${retryCount} retries`);
          this.stopQueueKeepAlive(queueName);
          return;
        }
        
        // Otherwise, retry quickly
        console.info(`Will retry extending keep-alive for exclusive queue ${queueName} in ${QUICK_RETRY_DELAY_MS}ms`);
        const timer = setTimeout(() => {
          this.sendQueueKeepAlive(queueName, retryCount + 1);
        }, QUICK_RETRY_DELAY_MS);
        this.exclusiveQueueTimers.set(queueName, timer);
      }
    } catch (error) {
      console.error(`Error extending keep-alive for exclusive queue ${queueName}:`, error);
      
      // If we've reached max retries, stop the keep-alive timer
      if (retryCount >= MAX_QUICK_RETRIES) {
        console.error(`Giving up on extending keep-alive for exclusive queue ${queueName} after ${retryCount} retries`);
        this.stopQueueKeepAlive(queueName);
        return;
      }
      
      // Otherwise, retry quickly
      console.info(`Will retry extending keep-alive for exclusive queue ${queueName} in ${QUICK_RETRY_DELAY_MS}ms`);
      const timer = setTimeout(() => {
        this.sendQueueKeepAlive(queueName, retryCount + 1);
      }, QUICK_RETRY_DELAY_MS);
      this.exclusiveQueueTimers.set(queueName, timer);
    }
  }
} 
