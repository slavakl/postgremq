/**
 * PostgreMQ TypeScript Client
 * Consumer implementation
 */

import { ConsumerOptions, NotificationEvent } from './types';
import { Connection } from './connection';
import { Message } from './message';
import { createDeferred, sleep } from './utils';

/**
 * Track which message is due next for visibility timeout extension
 */
interface MessageExtensionInfo {
  messageId: number;
  consumerToken: string;
  nextExtensionTime: Date;
}

/**
 * Consumer class for consuming messages from a queue.
 *
 * - Implements an asynchronous iterator pattern for message consumption
 *   via `messages()`.
 * - Supports automatic visibility timeout extension (autoExtension) that
 *   extends in‑flight messages before they expire to prevent redelivery
 *   while processing.
 */
export class Consumer {
  /** Queue name */
  private readonly queueName: string;
  
  /** Connection manager reference */
  private readonly connection: Connection;
  
  /** Consumer options */
  private readonly options: Required<ConsumerOptions>;
  
  /** Default options for the consumer */
  private static readonly DEFAULT_OPTIONS: Required<ConsumerOptions> = {
    batchSize: 10,
    visibilityTimeoutSec: 30,
    autoExtension: {
      enabled: true,
      extensionThreshold: 0.5,
      extensionSec: 30,
      maxBatchSize: 100
    },
    pollingIntervalMs: 1000
  };
  
  /** Map of in-flight messages by ID */
  private inFlightMessages: Map<number, Message> = new Map();
  
  /** Buffer of fetched messages waiting to be consumed */
  private messageBuffer: Message[] = [];
  
  /** Flag indicating if the consumer is actively running */
  private running: boolean = false;
  
  /** Flag indicating if the consumer is actively fetching messages */
  private fetching: boolean = false;
  
  /** Notification unsubscribe function */
  private unsubscribe: (() => void) | null = null;
  
  /** Auto-extension timer handle */
  private nextExtensionTimer: NodeJS.Timeout | null = null;
  
  /** Reference to the current iterator for signaling availability */
  private iteratorSignal: [Promise<void>, () => void] | null = null;
  
  /** Timer for the next scheduled fetch */
  private nextFetchTimer: NodeJS.Timeout | null = null;
  
  /** Flag indicating if the last fetch returned a full batch */
  private lastFetchWasFull: boolean = false;
  
  /** Threshold for triggering new fetches (1/3 of batchSize) */
  private readonly fetchThreshold: number;
  
  /** Sorted array of messages by next extension time */
  private extensionQueue: MessageExtensionInfo[] = [];
  
  /** Flag to indicate if an extension operation is currently in progress */
  private extending: boolean = false;

  /**
   * Create a new Consumer
   * @param queueName - Name of the queue to consume from
   * @param connection - Connection manager instance
   * @param options - Consumer options
   */
  constructor(
    queueName: string,
    connection: Connection,
    options: Partial<ConsumerOptions> = {}
  ) {
    this.queueName = queueName;
    this.connection = connection;
    
    // Merge default options with provided options
    this.options = {
      ...Consumer.DEFAULT_OPTIONS,
      ...options,
      autoExtension: {
        ...Consumer.DEFAULT_OPTIONS.autoExtension,
        ...options.autoExtension
      }
    };
    
    // Calculate the fetch threshold once (1/3 of batch size)
    this.fetchThreshold = Math.max(1, Math.floor(this.options.batchSize / 3));
  }

  /**
   * Start the consumer
   * This method is called internally when messages() is called
   */
  private start(): void {
    if (this.running) {
      return;
    }
    
    this.running = true;
    
    // Subscribe to queue notifications
    this.unsubscribe = this.connection.subscribeToQueue(
      this.queueName,
      this.handleNotification.bind(this)
    );
    
    // Start the auto-extension scheduler if enabled
    if (this.options.autoExtension.enabled) {
      this.scheduleNextExtension();
    }
    
    // Start with an initial fetch
    this.triggerFetch();
  }

  /**
   * Handle a notification event
   * @param event - The notification event
   */
  private handleNotification(event: NotificationEvent): void {
    // If consumer is not running, ignore the notification
    if (!this.running) {
      return;
    }
    
    // Events always trigger fetch immediately
    this.triggerFetch();
  }

  /**
   * Add a message to the extension queue
   * @param message - The message to add to the extension queue
   */
  private addMessageToExtensionQueue(message: Message): void {
    if (!this.options.autoExtension.enabled) {
      return;
    }

    // Calculate when this message needs to be extended
    const extensionThreshold = this.options.autoExtension.extensionThreshold ?? 0.5;
    const thresholdMs = this.options.visibilityTimeoutSec * extensionThreshold * 1000;
    const now = new Date();

    // Calculate the time when the message should be extended
    const extensionTime = new Date(message.vt.getTime() - thresholdMs);

    // Prevent unbounded growth: don't add messages that are too old
    // Maximum age is 3x the visibility timeout
    const MAX_EXTENSION_AGE_MS = this.options.visibilityTimeoutSec * 3 * 1000;
    if (message.vt.getTime() < now.getTime() - MAX_EXTENSION_AGE_MS) {
      // Message VT is too old, likely already expired or stuck
      return;
    }

    // Only add if extension time is in the future
    if (extensionTime > now) {
      const newItem = {
        messageId: message.id,
        consumerToken: message.consumerToken,
        nextExtensionTime: extensionTime
      };

      // Use binary search to find the correct insertion position
      // This maintains sorted order with O(log n) search complexity
      // instead of O(n log n) for a full array sort
      const insertIndex = this.findInsertionIndex(extensionTime, message.id);

      // Insert at the correct position
      this.extensionQueue.splice(insertIndex, 0, newItem);

      // Update the extension schedule
      this.scheduleNextExtension();
    }
  }
  
  /**
   * Binary search to find the correct insertion index in the sorted extension queue
   * Uses message ID as secondary sort key to ensure stable ordering
   * @param extensionTime - The next extension time to insert
   * @param messageId - The message ID (used as secondary sort key)
   * @returns The index where the new element should be inserted
   */
  private findInsertionIndex(extensionTime: Date, messageId: number): number {
    let low = 0;
    let high = this.extensionQueue.length;

    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      const midTime = this.extensionQueue[mid].nextExtensionTime.getTime();
      const targetTime = extensionTime.getTime();

      // Primary sort: by extension time
      // Secondary sort: by message ID for stable ordering
      if (midTime > targetTime ||
          (midTime === targetTime && this.extensionQueue[mid].messageId > messageId)) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }

    return low;
  }
  
  /**
   * Remove a message from the extension queue
   * @param messageId - The ID of the message to remove
   */
  private removeMessageFromExtensionQueue(messageId: number): void {
    const initialLength = this.extensionQueue.length;
    
    // Remove the message from the extension queue
    this.extensionQueue = this.extensionQueue.filter(info => info.messageId !== messageId);
    
    // If we actually removed something and auto-extension is enabled, reschedule
    if (initialLength !== this.extensionQueue.length && this.options.autoExtension.enabled) {
      this.scheduleNextExtension();
    }
  }
  
  /**
   * Schedule the next extension based on the extension queue
   */
  private scheduleNextExtension(): void {
    // Clear any existing extension timer
    if (this.nextExtensionTimer) {
      clearTimeout(this.nextExtensionTimer);
      this.nextExtensionTimer = null;
    }
    
    // If no messages need extension or we're not running, just return
    if (this.extensionQueue.length === 0 || !this.running) {
      return;
    }
    
    // Get the soonest message that needs extension
    const nextExtension = this.extensionQueue[0];
    const now = new Date();
    
    // Calculate how long to wait
    let waitTime = nextExtension.nextExtensionTime.getTime() - now.getTime();
    
    // If it's already past due, process extensions immediately
    if (waitTime <= 0) {
      this.processExtensions();
      return;
    }
    
    // Schedule the timer for the next extension
    this.nextExtensionTimer = setTimeout(() => {
      this.processExtensions();
    }, waitTime);
  }
  
  /**
   * Process message extensions that are due
   */
  private async processExtensions(): Promise<void> {
    // If already extending or not running, return
    if (this.extending || !this.running) {
      return;
    }
    
    this.extending = true;
    
    try {
      const now = new Date();
      const extensionSec = this.options.autoExtension.extensionSec ?? 30;
      const maxBatchSize = this.options.autoExtension.maxBatchSize ?? 100;
      
      // Find messages that are due for extension
      const dueForExtension = this.extensionQueue
        .filter(info => info.nextExtensionTime <= now)
        .slice(0, maxBatchSize);
      
      // If no messages are due, reschedule and return
      if (dueForExtension.length === 0) {
        this.scheduleNextExtension();
        return;
      }
      
      // Remove the due messages from the queue
      this.extensionQueue = this.extensionQueue.filter(info => 
        info.nextExtensionTime > now || 
        !dueForExtension.some(due => due.messageId === info.messageId)
      );
      
      // If there's only one message, use the single message API
      if (dueForExtension.length === 1) {
        const info = dueForExtension[0];
        const message = this.inFlightMessages.get(info.messageId);
        
        if (message) {
          try {
            const newVt = await message.setVt(extensionSec);
            console.debug(`Extended visibility for message ${info.messageId} until ${newVt}`);
            
            // Re-add the message to the extension queue with updated time
            this.addMessageToExtensionQueue(message);
          } catch (error) {
            // Only log if not shutting down
            if (!this.connection.isClientShuttingDown()) {
              console.debug(`Failed to extend visibility for message ${info.messageId}: ${error}`);
            }
            // Don't re-add to queue if extension failed - it might be completed already
          }
        }
      } 
      // For multiple messages, use the batch extension API
      else if (dueForExtension.length > 1) {
        try {
          const messageIds = dueForExtension.map(info => info.messageId);
          const consumerTokens = dueForExtension.map(info => info.consumerToken);
          
          const results = await this.connection.setMessagesVtBatch(
            this.queueName,
            messageIds,
            consumerTokens,
            extensionSec
          );
          
          // Update message VT values and re-add to extension queue
          for (const [messageId, newVt] of results) {
            const message = this.inFlightMessages.get(messageId);
            if (message) {
              message.vt = newVt;
              this.addMessageToExtensionQueue(message);
            }
          }
          
          console.debug(`Extended visibility for ${results.length} messages`);
          
          // For messages that failed to extend, they're not returned in results
          // We won't re-add them to the queue
        } catch (error) {
          console.error(`Error extending visibility for batch of messages: ${error}`);
          // Don't re-add batch if the entire operation failed
        }
      }
      
      // If we still have messages past due (e.g., more than maxBatchSize),
      // process them immediately (but only if still running)
      if (this.running && this.extensionQueue.length > 0 && this.extensionQueue[0].nextExtensionTime <= now) {
        setTimeout(() => this.processExtensions(), 0);
      } else if (this.running) {
        // Otherwise, schedule the next extension (but only if still running)
        this.scheduleNextExtension();
      }
    } finally {
      this.extending = false;
    }
  }

  /**
   * Handle message completion (called when a message is acked, nacked, or released)
   * @param messageId - The ID of the completed message
   */
  private handleMessageComplete(messageId: number): void {
    // Remove from in-flight tracking
    this.inFlightMessages.delete(messageId);
    
    // Remove from extension queue
    this.removeMessageFromExtensionQueue(messageId);
    
    // We don't trigger fetch here anymore - message completion is
    // not the same as message consumption from the buffer
  }

  /**
   * Trigger a fetch operation based on the rules
   */
  private triggerFetch(): void {
    // Cancel any pending fetch timer
    if (this.nextFetchTimer) {
      clearTimeout(this.nextFetchTimer);
      this.nextFetchTimer = null;
    }
    
    // If already fetching or not running, don't start another fetch
    if (this.fetching || !this.running) {
      return;
    }
    
    // If buffer is full, don't fetch now (will trigger after consumption)
    if (this.messageBuffer.length >= this.options.batchSize) {
      return;
    }
    
    // Execute fetch with inline error handling
    (async () => {
      try {
        await this.fetchMessages();
      } catch (error) {
        console.error(`Error fetching messages: ${error}`);
        
        // On error, retry in one second
        if (this.running) {
          this.nextFetchTimer = setTimeout(() => this.triggerFetch(), 1000);
        }
      }
    })();
  }

  /**
   * Fetch messages from the queue
   */
  private async fetchMessages(): Promise<void> {
    // If already fetching or not running, return
    if (this.fetching || !this.running) {
      return;
    }
    
    this.fetching = true;
    
    try {
      // Calculate how many messages to fetch
      const fetchCount = Math.min(
        this.options.batchSize - this.messageBuffer.length,
        this.options.batchSize
      );
      
      if (fetchCount <= 0) {
        this.fetching = false;
        return;
      }
      
      // Fetch messages
      const rawMessages = await this.connection.consumeMessages(
        this.queueName,
        this.options.visibilityTimeoutSec,
        fetchCount
      );
      
      // Process fetched messages
      for (const rawMessage of rawMessages) {
        // Create Message object
        const message = new Message(
          rawMessage.message_id,
          this.queueName,
          rawMessage.payload,
          rawMessage.consumer_token,
          rawMessage.delivery_attempts,
          new Date(rawMessage.vt),
          new Date(rawMessage.published_at),
          this.handleMessageComplete.bind(this),
          {
            ack: this.connection.ackMessage.bind(this.connection),
            nack: this.connection.nackMessage.bind(this.connection),
            release: this.connection.releaseMessage.bind(this.connection),
            setVt: this.connection.setMessageVt.bind(this.connection)
          }
        );
        
        // Add to buffer and in-flight tracking
        this.messageBuffer.push(message);
        this.inFlightMessages.set(message.id, message);
        
        // Add to extension queue if auto-extension is enabled
        if (this.options.autoExtension.enabled) {
          this.addMessageToExtensionQueue(message);
        }
      }
      
      // Check if we received a full batch
      this.lastFetchWasFull = rawMessages.length === fetchCount;
      
      // Signal iterator if waiting
      if (this.iteratorSignal && this.messageBuffer.length > 0) {
        this.iteratorSignal[1]();
        this.iteratorSignal = null;
      }
      
      // If we got a partial batch, schedule next fetch based on visibility times
      if (rawMessages.length < fetchCount && this.running) {
        this.scheduleNextFetch();
      }
    } finally {
      this.fetching = false;
    }
  }
  
  /**
   * Schedule the next fetch based on message visibility times
   */
  private async scheduleNextFetch(): Promise<void> {
    try {
      // Get the next visible time from the database
      const nextVisibleTime = await this.connection.getNextVisibleTime(this.queueName);
      
      if (!this.running) return;
      
      let waitTime = this.options.pollingIntervalMs;
      
      if (nextVisibleTime) {
        const now = new Date();
        const timeUntilNextVisible = Math.max(0, nextVisibleTime.getTime() - now.getTime());
        
        // Use the shorter of the two wait times
        waitTime = Math.min(timeUntilNextVisible, this.options.pollingIntervalMs);
      }
      
      // Schedule the next fetch
      this.nextFetchTimer = setTimeout(() => this.triggerFetch(), waitTime);
    } catch (error) {
      console.error(`Error scheduling next fetch: ${error}`);
      
      // On error, retry in one second
      if (this.running) {
        this.nextFetchTimer = setTimeout(() => this.triggerFetch(), 1000);
      }
    }
  }

  /**
   * Stop the consumer
   * @returns Promise that resolves when consumer is stopped
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;

    // Clear the next fetch timer
    if (this.nextFetchTimer) {
      clearTimeout(this.nextFetchTimer);
      this.nextFetchTimer = null;
    }

    // Clear the extension timer
    if (this.nextExtensionTimer) {
      clearTimeout(this.nextExtensionTimer);
      this.nextExtensionTimer = null;
    }

    // Unsubscribe from notifications
    if (this.unsubscribe) {
      this.unsubscribe();
      this.unsubscribe = null;
    }

    // Signal iterator if waiting - MUST do this before releasing messages
    // This ensures the iterator returns done immediately
    if (this.iteratorSignal) {
      this.iteratorSignal[1]();
      this.iteratorSignal = null;
    }

    // Release any buffered messages (similar to Go closing the channel and releasing buffered messages)
    await this.releaseBufferedMessages();

    // Wait for all in-flight messages to complete with a shorter timeout
    // This allows ack/nack/release operations to finish
    if (this.inFlightMessages.size > 0) {
      const startTime = Date.now();
      const timeout = 2000; // 2 second timeout for in-flight messages (reduced from 5s)

      while (this.inFlightMessages.size > 0) {
        await sleep(50); // Check every 50ms

        // Timeout if messages aren't completing
        if (Date.now() - startTime > timeout) {
          // Only log if not in connection shutdown (prevents "Cannot log after tests are done" warnings)
          if (!this.connection.isClientShuttingDown()) {
            console.debug(`Consumer stop: releasing ${this.inFlightMessages.size} in-flight messages after ${timeout}ms timeout`);
          }

          // Force release any remaining in-flight messages
          // This matches the Go implementation which cancels contexts for all in-flight messages
          const remainingMessages = Array.from(this.inFlightMessages.values());

          // Release all in parallel for speed
          const releasePromises = remainingMessages.map(async (message) => {
            try {
              await message.release();
            } catch (error) {
              // Expected during shutdown - message may already be processed
              // Only log if not in connection shutdown
              if (!this.connection.isClientShuttingDown()) {
                console.debug(`Message ${message.id} release failed during shutdown (expected): ${error}`);
              }
              // Remove from tracking even if release fails
              this.handleMessageComplete(message.id);
            }
          });

          // Wait for all releases with a short timeout
          try {
            await Promise.race([
              Promise.all(releasePromises),
              sleep(500) // 500ms max for releases
            ]);
          } catch {
            // Ignore errors during shutdown
          }

          break;
        }
      }
    }

    // Unregister from connection
    this.connection.unregisterConsumer(this);
  }

  /**
   * Release all buffered messages back to the queue
   */
  private async releaseBufferedMessages(): Promise<void> {
    // Create array of release promises
    const promises: Promise<void>[] = [];

    for (const message of this.messageBuffer) {
      try {
        // Only release if it's still in flight (hasn't been processed yet)
        if (this.inFlightMessages.has(message.id)) {
          promises.push(
            message.release().catch(err => {
              // Silently ignore release errors for messages that are no longer in processing state
              // This can happen if VT expired and another consumer picked it up
              const errorStr = err.toString();
              const isExpectedError = errorStr.includes('not in processing state') ||
                                       errorStr.includes('token mismatch');

              // Only log if not in connection shutdown and not an expected error
              if (!this.connection.isClientShuttingDown() && !isExpectedError) {
                console.error(`Failed to release buffered message ${message.id}: ${err}`);
              }
              // Don't rethrow - continue with other releases
            })
          );
        }
      } catch (error) {
        console.error(`Error releasing buffered message ${message.id}: ${error}`);
      }
    }

    // Clear the buffer
    this.messageBuffer = [];

    // Wait for all releases to complete with timeout
    // Use shorter timeout (2s) to avoid hanging during shutdown
    // Failed releases are expected when VT expires
    try {
      await Promise.race([
        Promise.all(promises),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Release timeout')), 2000)
        )
      ]);
    } catch (error) {
      console.debug(`Some buffered messages may not have been released (expected during shutdown): ${error}`);
      // Continue shutdown anyway - messages will become available after VT expires
    }
  }

  /**
   * Create an asynchronous iterator for consuming messages
   * @returns Asynchronous iterator that yields messages
   */
  messages(): AsyncIterableIterator<Message> {
    // Start the consumer if not already running
    this.start();
    
    // Create a message iterator
    const messageIterator: AsyncIterableIterator<Message> = {
      // Implementation of Symbol.asyncIterator
      [Symbol.asyncIterator]: () => messageIterator,
      
      // The next method for the iterator
      next: async (): Promise<IteratorResult<Message>> => {
        // If not running, return done
        if (!this.running) {
          return { done: true, value: undefined as any };
        }
        
        // If buffer has messages, return the next one
        if (this.messageBuffer.length > 0) {
          const message = this.messageBuffer.shift()!;
          
          // Only fetch more if buffer is below threshold AND last fetch was full
          const shouldFetch = this.messageBuffer.length < this.fetchThreshold && this.lastFetchWasFull;
            
          if (shouldFetch) {
            this.triggerFetch();
          }
          
          return {
            done: false,
            value: message
          };
        }
        
        // No messages available, wait for more
        const [promise, resolve] = createDeferred<void>();
        this.iteratorSignal = [promise, resolve];
        
        // Trigger a fetch if not already fetching
        this.triggerFetch();
        
        // Wait for signal or shutdown
        await promise;
        
        // Check if we've been stopped while waiting
        if (!this.running) {
          return { done: true, value: undefined as any };
        }
        
        // If buffer now has messages, return the next one
        if (this.messageBuffer.length > 0) {
          const message = this.messageBuffer.shift()!;
          
          // Only fetch more if buffer is below threshold AND last fetch was full
          const shouldFetch = this.messageBuffer.length < this.fetchThreshold && this.lastFetchWasFull;
            
          if (shouldFetch) {
            this.triggerFetch();
          }
          
          return {
            done: false,
            value: message
          };
        }
        
        // This shouldn't happen, but just in case
        return messageIterator.next();
      }
    };
    
    return messageIterator;
  }
} 
