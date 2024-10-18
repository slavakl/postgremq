/**
 * PostgreMQ TypeScript Client
 * Types and interfaces
 */

import { Pool, PoolConfig } from 'pg';

/** 
 * Forward declaration of Consumer interface
 * Full implementation is in consumer.ts
 */
export interface Consumer {
  stop(): Promise<void>;
  messages(): AsyncIterableIterator<Message>;
}

/** 
 * Forward declaration of Message interface
 * Full implementation is in message.ts
 */
export interface Message {
  readonly id: number;
  readonly queueName: string;
  readonly payload: any;
  readonly consumerToken: string;
  readonly deliveryAttempts: number;
  readonly publishedAt: Date;
  vt: Date;
  ack(): Promise<void>;
  ackWithTransaction(tx: Transaction): Promise<void>;
  nack(options?: MessageOptions): Promise<void>;
  release(): Promise<void>;
  setVt(newVt: number): Promise<Date>;
}

/**
 * Connection options for PostgreMQ client
 */
export interface ConnectionOptions {
  /** PostgreSQL connection string (overrides config if provided) */
  connectionString?: string;
  /** Connection pool configuration (creates new pool) */
  config?: PoolConfig;
  /** Existing pool instance (reuses existing pool) */
  pool?: Pool;
  /** Maximum time (ms) to wait for in-flight messages during shutdown */
  shutdownTimeoutMs?: number;
  /** Retry policy for transient errors */
  retry?: RetryPolicy;
}

/**
 * Public interface for the PostgreMQ Connection
 * Only methods intended for direct use by library users are exposed here
 */
export interface IConnection {
  /** Connect to the database */
  connect(): Promise<void>;
  
  /** Close the connection and clean up resources */
  close(): Promise<void>;
  
  /** 
   * Publish a message to a topic 
   * @returns Message ID
   */
  publish(topic: string, payload: any, options?: PublishOptions): Promise<number>;
  
  /** Create a consumer for a queue */
  consume(queue: string, options?: Partial<ConsumerOptions>): Consumer;
  
  /** Create a new topic */
  createTopic(topic: string): Promise<void>;
  
  /** Create a new queue */
  createQueue(
    name: string, 
    topic: string, 
    exclusive?: boolean, 
    options?: QueueOptions
  ): Promise<void>;
  
  /** Delete a topic */
  deleteTopic(topic: string): Promise<void>;
  
  /** Delete a queue */
  deleteQueue(queue: string): Promise<void>;
  
  /** List all topics */
  listTopics(): Promise<string[]>;
  
  /** List all queues */
  listQueues(): Promise<QueueInfo[]>;
  
  /** Get statistics for a queue or all queues */
  getQueueStatistics(queue?: string): Promise<QueueStatistics>;

  /** Move messages from active queues to DLQ if they exceed max delivery attempts */
  moveToDLQ(): Promise<number>;

  /** List all messages in the Dead Letter Queue */
  listDLQMessages(): Promise<DLQMessage[]>;
  
  /** Requeue messages from the Dead Letter Queue to a specific queue */
  requeueDLQMessages(queue: string): Promise<void>;
  
  /** Purge the Dead Letter Queue */
  purgeDLQ(): Promise<void>;
  
  /** Purge all messages from all queues */
  purgeAllMessages(): Promise<void>;
  
  /** Delete a specific message from a queue */
  deleteQueueMessage(queue: string, messageID: number): Promise<void>;
  
  /** List all messages in a queue */
  listMessages(queue: string): Promise<QueueMessage[]>;
  
  /** Get a specific message by ID */
  getMessage(messageID: number): Promise<PublishedMessage | null>;
  
  /** Clean up a queue (remove completed messages) */
  cleanUpQueue(queue: string): Promise<void>;
  
  /** Clean up a topic (remove messages with no subscribers) */
  cleanUpTopic(topic: string): Promise<void>;
  
  /** Delete inactive queues */
  deleteInactiveQueues(): Promise<void>;

  /** Remove completed messages older than the specified retention window */
  cleanupCompletedMessages(olderThanHours?: number): Promise<number>;
  
  /** Check if the client is shutting down */
  isClientShuttingDown(): boolean;
}

/**
 * Retry policy for database operations
 */
export interface RetryPolicy {
  /** Maximum number of retry attempts */
  maxAttempts: number;
  /** Initial backoff delay in milliseconds */
  initialBackoffMs: number;
  /** Backoff multiplier for each subsequent retry */
  backoffMultiplier: number;
  /** Maximum backoff delay in milliseconds */
  maxBackoffMs: number;
}

/**
 * Options for publishing a message
 */
export interface PublishOptions {
  /** Time at which the message should be delivered (defaults to immediate) */
  deliverAfter?: Date;
}

/**
 * Options for creating a queue
 */
export interface QueueOptions {
  /** Maximum number of delivery attempts before moving to DLQ (0 for unlimited) */
  maxDeliveryAttempts?: number;
  /** The keep-alive time in seconds (for exclusive queues) */
  keepAliveSeconds?: number;
}

/**
 * Options for consuming messages
 */
export interface ConsumerOptions {
  /** Maximum number of messages to fetch in a single batch */
  batchSize?: number;
  /** Default visibility timeout for messages in seconds */
  visibilityTimeoutSec?: number;
  /** Auto-extension settings for visibility timeout */
  autoExtension?: {
    /** Whether to automatically extend message visibility */
    enabled: boolean;
    /** Percentage of visibility timeout to wait before extending (e.g., 0.5 = 50%) */
    extensionThreshold?: number;
    /** How much to extend visibility by (in seconds) */
    extensionSec?: number;
    /** Maximum number of messages to process in a single extension batch */
    maxBatchSize?: number;
  };
  /** Polling interval in milliseconds (when no messages and no notifications) */
  pollingIntervalMs?: number;
}

/**
 * Options for negative acknowledgment (nack) or release of a message
 */
export interface MessageOptions {
  /** Delay before the message becomes visible again (in seconds) */
  delaySeconds?: number;
}

/**
 * Information about a queue
 */
export interface QueueInfo {
  /** Queue name */
  queueName: string;
  /** Associated topic name */
  topicName: string;
  /** Maximum delivery attempts (0 for unlimited) */
  maxDeliveryAttempts: number;
  /** Whether the queue is exclusive (non-durable) */
  exclusive: boolean;
  /** Expiration timestamp for exclusive queues */
  keepAliveUntil: Date | null;
}

/**
 * Queue statistics
 */
export interface QueueStatistics {
  /** Number of pending messages */
  pendingCount: number;
  /** Number of messages being processed */
  processingCount: number;
  /** Number of completed messages */
  completedCount: number;
  /** Total number of messages */
  totalCount: number;
}

/**
 * Information about a message in the Dead Letter Queue
 */
export interface DLQMessage {
  /** Queue name */
  queueName: string;
  /** Message ID */
  messageId: number;
  /** Retry count (number of delivery attempts made) */
  retryCount: number;
  /** Timestamp when the message was published to DLQ */
  publishedAt: Date;
}

/**
 * Information about a message in a queue
 */
export interface QueueMessage {
  /** Message ID */
  messageId: number;
  /** Message status (pending, processing, completed) */
  status: string;
  /** Timestamp when the message was published */
  publishedAt: Date;
  /** Number of delivery attempts */
  deliveryAttempts: number;
  /** Visibility timeout expiration */
  vt: Date;
  /** Timestamp when the message was processed (if completed) */
  processedAt: Date | null;
}

/**
 * Information about a published message
 */
export interface PublishedMessage {
  /** Message ID */
  messageId: number;
  /** Topic name */
  topicName: string;
  /** Message payload */
  payload: any;
  /** Timestamp when the message was published */
  publishedAt: Date;
}

/**
 * Event types for the notification system
 */
export enum EventType {
  MESSAGE_PUBLISHED = 'message_published',
  MESSAGE_NACKED = 'message_nacked',
  MESSAGE_RELEASED = 'message_released'
}

/**
 * Event notification from PostgreSQL
 */
export interface NotificationEvent {
  /** Event type */
  event: EventType;
  /** Affected queues */
  queues: string[];
  /** Visibility timestamp */
  vt?: string | Date;
}

/**
 * Message processing status
 */
export enum MessageStatus {
  PENDING = 'pending',
  PROCESSING = 'processing',
  COMPLETED = 'completed'
}

/**
 * Transaction interface for transaction support
 */
export interface Transaction {
  /** Execute query within transaction */
  query(text: string, values?: any[]): Promise<any>;
} 
