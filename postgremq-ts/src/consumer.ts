/**
 * PostgreMQ TypeScript Client
 * Consumer implementation
 */

import { ConsumerOptions } from './types';
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
 * Schedule of in-flight messages awaiting auto-extension, sorted by
 * `nextExtensionTime` (primary) then `messageId` (secondary, for stable
 * order). The class encapsulates the sorted-insert invariant and the
 * "vt is too old" cap so the Consumer can think in terms of add / remove
 * / replace / due / headTime instead of array math.
 */
class ExtensionQueue {
  private items: MessageExtensionInfo[] = [];

  /**
   * @param visibilityTimeoutSec — the consumer's configured VT, used to
   *        derive the next-extension time and the staleness cap.
   * @param threshold — fraction of the VT to use as the safety margin
   *        before extending. 0.5 means "extend at the halfway point".
   */
  constructor(
    private readonly visibilityTimeoutSec: number,
    private readonly threshold: number,
  ) {}

  get size(): number {
    return this.items.length;
  }

  /** Time at which the soonest extension is scheduled, or null if empty. */
  headTime(): Date | null {
    return this.items.length > 0 ? this.items[0].nextExtensionTime : null;
  }

  /** Up to `limit` items whose `nextExtensionTime` is at or before `now`. */
  due(now: Date, limit: number): MessageExtensionInfo[] {
    const out: MessageExtensionInfo[] = [];
    for (const item of this.items) {
      if (item.nextExtensionTime > now) break;  // sorted: rest are later
      out.push(item);
      if (out.length >= limit) break;
    }
    return out;
  }

  /**
   * Schedule `message` for extension. The first attempt is at
   * `vt - visibilityTimeoutSec * threshold * 1000` (typically halfway
   * through the lease). No-op if the message's vt is older than 3× the
   * lease window (likely already expired or stuck) or if the planned
   * extension time is already past — in the latter case the caller's
   * normal scheduling/retry path will pick it up.
   */
  add(message: Message): void {
    const thresholdMs = this.visibilityTimeoutSec * this.threshold * 1000;
    const now = Date.now();
    const MAX_AGE_MS = this.visibilityTimeoutSec * 3 * 1000;
    if (message.vt.getTime() < now - MAX_AGE_MS) return;
    const extensionTime = new Date(message.vt.getTime() - thresholdMs);
    if (extensionTime.getTime() <= now) return;

    const idx = this.findInsertionIndex(extensionTime, message.id);
    this.items.splice(idx, 0, {
      messageId: message.id,
      consumerToken: message.consumerToken,
      nextExtensionTime: extensionTime,
    });
  }

  /** Drop the entry for `messageId`. No-op if absent. */
  remove(messageId: number): void {
    this.items = this.items.filter(i => i.messageId !== messageId);
  }

  /** Replace the entry for `message.id` with one carrying `message`'s
   *  current vt. Equivalent to `remove(message.id)` + `add(message)`. */
  replace(message: Message): void {
    this.remove(message.id);
    this.add(message);
  }

  /** Binary-search the insertion index that preserves the sort order. */
  private findInsertionIndex(extensionTime: Date, messageId: number): number {
    let low = 0;
    let high = this.items.length;
    const targetTime = extensionTime.getTime();
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      const midTime = this.items[mid].nextExtensionTime.getTime();
      // Primary: nextExtensionTime ascending. Secondary: messageId ascending.
      if (midTime > targetTime ||
          (midTime === targetTime && this.items[mid].messageId > messageId)) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }
    return low;
  }
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
    pollingIntervalMs: 1000,
    topic: ''
  };
  
  /** Map of in-flight messages by ID */
  private inFlightMessages: Map<number, Message> = new Map();
  
  /** Buffer of fetched messages waiting to be consumed */
  private messageBuffer: Message[] = [];
  
  /** Flag indicating if the consumer is actively running */
  private running: boolean = false;

  /** Flag indicating if the consumer is actively fetching messages */
  private fetching: boolean = false;

  /** Promise that resolves when the currently in-flight fetch completes
   *  (success or error), or null when no fetch is running. Set
   *  synchronously at triggerFetch() — before any await — so stop() can
   *  reliably wait for messages a fetch already claimed server-side to
   *  reach the buffer/inFlight tracking, even if the SQL call takes
   *  longer than the old 1s spin cap. Without this, a fetch in progress
   *  during stop() leaks: consume_message has already moved rows to
   *  'processing', but the consumer is gone before the messages get
   *  released, so the rows sit until vt expires. */
  private fetchInFlight: Promise<void> | null = null;

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
  
  /** Schedule of in-flight messages awaiting auto-extension. */
  private readonly queue: ExtensionQueue;

  /** Flag to indicate if an extension operation is currently in progress */
  private extending: boolean = false;

  /** Earliest moment at which the next extension call may run, regardless
   *  of what the head of the queue says. Set by processExtensions on a
   *  transient failure to defer the next attempt by ~1s and avoid
   *  tight-looping against a dead DB. Cleared on entry to the next call.
   *  Mirrors Go's `tryAfter` in extendLoop. */
  private tryAfter: Date | null = null;

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

    this.queue = new ExtensionQueue(
      this.options.visibilityTimeoutSec,
      this.options.autoExtension.extensionThreshold ?? 0.5,
    );
  }

  /**
   * Start the consumer.
   * Called internally when messages() is called. resolveTopic is now
   * synchronous (explicit option > cache > throw), so subscription and the
   * initial fetch happen on the same tick.
   */
  private start(): void {
    if (this.running) {
      return;
    }

    const topic = this.connection.resolveTopic(
      this.queueName,
      this.options.topic || undefined
    );

    this.running = true;
    this.unsubscribe = this.connection.subscribeForConsumer(
      this.queueName,
      topic,
      this.handleNotification.bind(this)
    );

    if (this.options.autoExtension.enabled) {
      this.scheduleNextExtension();
    }

    // Start with an initial fetch
    this.triggerFetch();
  }

  /**
   * Handle a notification event from the connection's per-topic or
   * per-queue NOTIFY listener. PostgreMQ NOTIFYs carry no payload — the
   * arrival itself is the signal to re-fetch.
   */
  private handleNotification(): void {
    if (!this.running) return;
    this.triggerFetch();
  }

  /** Track `message` for auto-extension, then update the timer. No-op
   *  if auto-extension is disabled. */
  private addToQueue(message: Message): void {
    if (!this.options.autoExtension.enabled) return;
    this.queue.add(message);
    this.scheduleNextExtension();
  }

  /** Drop tracking for `messageId`. No-op if auto-extension is disabled
   *  or the message isn't tracked. */
  private removeFromQueue(messageId: number): void {
    if (!this.options.autoExtension.enabled) return;
    this.queue.remove(messageId);
  }
  
  /**
   * Schedule the next extension based on the extension queue
   */
  private scheduleNextExtension(): void {
    if (this.nextExtensionTimer) {
      clearTimeout(this.nextExtensionTimer);
      this.nextExtensionTimer = null;
    }

    const head = this.queue.headTime();
    if (!head || !this.running) return;

    // Target = max(head's planned time, tryAfter). tryAfter is set on a
    // transient failure to defer the next attempt; otherwise null.
    let targetTime = head;
    if (this.tryAfter && this.tryAfter > targetTime) {
      targetTime = this.tryAfter;
    }
    const waitTime = Math.max(0, targetTime.getTime() - Date.now());

    if (waitTime === 0) {
      this.processExtensions();
      return;
    }

    this.nextExtensionTimer = setTimeout(() => {
      this.processExtensions();
    }, waitTime);
  }
  
  /** How long to defer the next extension call after a transient failure.
   *  Avoids tight-looping against a dead DB. */
  private static readonly TRANSIENT_RETRY_MS = 1000;

  /** Maximum time stop() will wait for an in-flight fetch to complete
   *  before giving up. The fetch may have already claimed messages
   *  server-side; if it doesn't return in time, those messages are
   *  re-delivered after vt expiry. 30s matches the default vt. */
  private static readonly FETCH_DRAIN_TIMEOUT_MS = 30_000;

  /**
   * Process extensions that are due. Always uses set_vt_batch — even for
   * a single message — to keep one code path. set_vt_batch silently omits
   * rows that no longer match the (token, status='processing', vt>NOW())
   * predicate, so missing rows are interpreted as "lease lost server-side"
   * and we cancel their handlers.
   *
   * Outcomes:
   *   - Success: each returned id has its vt updated and is re-scheduled.
   *     Each requested id missing from the response is "lease lost
   *     server-side" — cancel its handler, drop from tracking.
   *   - Thrown error (transient — DB blip past Connection's retry budget):
   *     keep items in the queue, set tryAfter so the next call defers
   *     ~1s. The next attempt will either succeed or report rows missing
   *     (→ cancel + drop).
   */
  private async processExtensions(): Promise<void> {
    if (this.extending || !this.running) return;
    this.extending = true;

    try {
      // We're running now — reset any deferral; a fresh failure below
      // will set it again.
      this.tryAfter = null;

      const now = new Date();
      const extensionSec = this.options.autoExtension.extensionSec ?? 30;
      const maxBatchSize = this.options.autoExtension.maxBatchSize ?? 100;

      const dueForExtension = this.queue.due(now, maxBatchSize);
      if (dueForExtension.length === 0) return;

      const messageIds = dueForExtension.map(info => info.messageId);
      const consumerTokens = dueForExtension.map(info => info.consumerToken);

      let results: Array<[number, Date]>;
      try {
        results = await this.connection.setMessagesVtBatch(
          this.queueName, messageIds, consumerTokens, extensionSec,
        );
      } catch (error) {
        // Transient: leave items in queue, defer next attempt globally.
        this.tryAfter = new Date(Date.now() + Consumer.TRANSIENT_RETRY_MS);
        if (!this.connection.isClientShuttingDown()) {
          console.warn(`Transient error extending ${dueForExtension.length} messages: ${error}; will retry`);
        }
        return;
      }

      const extendedIds = new Set<number>();
      for (const [messageId, newVt] of results) {
        const message = this.inFlightMessages.get(messageId);
        if (message) {
          message.vt = newVt;
          this.queue.replace(message);
          extendedIds.add(messageId);
        }
      }

      // Missing from results = lease lost server-side.
      let leaseLost = 0;
      for (const info of dueForExtension) {
        if (extendedIds.has(info.messageId)) continue;
        const msg = this.inFlightMessages.get(info.messageId);
        if (msg) {
          msg._cancel();
          leaseLost++;
        }
        this.queue.remove(info.messageId);
      }

      if (leaseLost > 0 && !this.connection.isClientShuttingDown()) {
        console.debug(`Extended ${extendedIds.size}/${dueForExtension.length} (${leaseLost} lease lost; handlers cancelled)`);
      }
    } finally {
      this.extending = false;
      if (this.running) this.scheduleNextExtension();
    }
  }

  /**
   * Handle message completion (called when a message is acked, nacked, or released)
   * @param messageId - The ID of the completed message
   */
  private handleMessageComplete(messageId: number): void {
    this.inFlightMessages.delete(messageId);
    this.removeFromQueue(messageId);
    // We don't trigger fetch here — message completion is not the same
    // as message consumption from the buffer.
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
    
    // Execute fetch with inline error handling. Capture the promise
    // synchronously so stop() can await it before tearing down — see the
    // fetchInFlight comment.
    this.fetchInFlight = (async () => {
      try {
        await this.fetchMessages();
      } catch (error) {
        console.error(`Error fetching messages: ${error}`);

        // On error, retry in one second
        if (this.running) {
          this.nextFetchTimer = setTimeout(() => this.triggerFetch(), 1000);
        }
      } finally {
        this.fetchInFlight = null;
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
        this.addToQueue(message);
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

    // Signal every in-flight handler that the consumer is shutting down so
    // it can short-circuit work — the application can still finish acking,
    // nacking, or releasing a message after the signal fires (the abort is
    // advisory). Mirrors Go's Consumer.Stop() which cancels every
    // in-flight message's StoppedCtx during shutdown.
    for (const message of this.inFlightMessages.values()) {
      message._cancel();
    }

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

    // Unsubscribe from notifications (fire-and-forget UNLISTEN inside).
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

    // Wait for any in-flight fetchMessages to complete so its messages
    // reach the buffer / inFlight tracking before we drain. Without
    // this, a fetch that already claimed rows server-side would orphan
    // them: they'd be locked in 'processing' with no one to release.
    // Bounded by FETCH_DRAIN_TIMEOUT_MS so a wedged fetch can't block
    // shutdown indefinitely.
    if (this.fetchInFlight) {
      try {
        await Promise.race([
          this.fetchInFlight,
          sleep(Consumer.FETCH_DRAIN_TIMEOUT_MS).then(() => {
            throw new Error('fetch did not complete within drain timeout');
          }),
        ]);
      } catch (err) {
        if (!this.connection.isClientShuttingDown()) {
          console.warn(`Consumer stop: ${err}; messages claimed by the in-flight fetch may be redelivered after vt expiry`);
        }
      }
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
      },

      // Called by the runtime when the consumer of `for await ... of` exits
      // early — break, return, throw, an outer-scope catch. Without this, the
      // auto-extension timer, LISTEN refcount, and any buffered/in-flight
      // messages would leak until the GC eventually finalized the iterator.
      return: async (): Promise<IteratorResult<Message>> => {
        await this.stop();
        return { done: true, value: undefined as any };
      },
    };

    return messageIterator;
  }
}
