/**
 * PostgreMQ TypeScript Client
 * Handler-based consumer
 */

import { Connection } from './connection';
import { Consumer } from './consumer';
import { Message } from './message';
import { MessageHandler } from './types';

// Re-export so callers can `import { MessageHandler } from 'postgremq'`.
// The settlement rules (auto-ack on return, auto-nack on throw, no-op if the
// handler settled the message itself) are documented on the type in types.ts
// and implemented in runHandler below.
export { MessageHandler };

/**
 * HandlerConsumer wraps a Consumer and dispatches each message to a handler
 * function with optional concurrency limiting (maxInFlight). It is the
 * push-based counterpart to the pull-based `for await (const msg of consumer)`
 * iteration, and mirrors the Go client's ConsumeHandler.
 *
 * Create via Connection.consumeHandler — do not construct directly.
 */
export class HandlerConsumer {
  private readonly connection: Connection;
  private readonly consumer: Consumer;
  private readonly handler: MessageHandler;
  /** 0 means unlimited concurrency. */
  private readonly maxInFlight: number;

  private stopping = false;
  private dispatchPromise: Promise<void> | null = null;
  private stopPromise: Promise<void> | null = null;

  /** In-flight handler invocations, awaited on stop(). */
  private readonly activeHandlers = new Set<Promise<void>>();

  // Async counting semaphore for maxInFlight. slotsAvailable starts at
  // maxInFlight; waiters queue when none are free and are handed a slot
  // directly as handlers complete.
  private slotsAvailable: number;
  private readonly slotWaiters: Array<() => void> = [];

  /** @internal — use Connection.consumeHandler. */
  constructor(
    connection: Connection,
    consumer: Consumer,
    handler: MessageHandler,
    maxInFlight: number
  ) {
    this.connection = connection;
    this.consumer = consumer;
    this.handler = handler;
    this.maxInFlight = maxInFlight;
    this.slotsAvailable = maxInFlight;
  }

  /** @internal — start the dispatch loop. Called by Connection.consumeHandler. */
  start(): void {
    this.dispatchPromise = this.dispatchLoop();
  }

  /**
   * Acquire a concurrency slot. Resolves immediately when a slot is free (or
   * concurrency is unlimited); otherwise resolves when a running handler
   * releases one.
   */
  private acquireSlot(): Promise<void> {
    if (this.maxInFlight === 0) return Promise.resolve();
    if (this.slotsAvailable > 0) {
      this.slotsAvailable--;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.slotWaiters.push(resolve));
  }

  /** Release a concurrency slot, handing it to the next waiter if any. */
  private releaseSlot(): void {
    if (this.maxInFlight === 0) return;
    const waiter = this.slotWaiters.shift();
    if (waiter) {
      // Hand the slot straight to the waiter without bumping the counter.
      waiter();
    } else {
      this.slotsAvailable++;
    }
  }

  /**
   * Pull messages from the underlying consumer and dispatch each to a handler.
   * Acquires a concurrency slot BEFORE pulling a message so a message is never
   * fetched without capacity to process it (its visibility timeout would
   * otherwise tick while it waits) — matching the Go dispatch loop.
   */
  private async dispatchLoop(): Promise<void> {
    const iterator = this.consumer.messages();

    while (true) {
      await this.acquireSlot();
      if (this.stopping) {
        this.releaseSlot();
        break;
      }

      const result = await iterator.next();
      if (result.done) {
        // Underlying consumer stopped (its iterator only ends once
        // consumer.stop() runs). Release the unused slot and exit.
        this.releaseSlot();
        break;
      }

      const msg = result.value;
      const p = this.runHandler(msg);
      this.activeHandlers.add(p);
      // Release the slot and drop the tracking entry when the handler settles.
      p.finally(() => {
        this.activeHandlers.delete(p);
        this.releaseSlot();
      });
    }
  }

  /**
   * Run the handler for one message and apply the auto-ack / auto-nack rules.
   * Never rejects — failures are handled and logged here so the dispatch loop
   * and stop() can always await the returned promise safely.
   */
  private async runHandler(msg: Message): Promise<void> {
    try {
      await this.handler(msg);
    } catch (err) {
      // Handler threw — auto-nack so the message is retried (parity with the
      // Go client nacking on panic). Skip if the handler already settled it.
      console.error(`Handler error for message ${msg.id}: ${err}`);
      if (!msg.isSettled) {
        try {
          await msg.nack();
        } catch (nackErr) {
          console.error(`Failed to auto-nack message ${msg.id} after handler error: ${nackErr}`);
        }
      }
      return;
    }

    // Handler returned without settling — auto-ack.
    if (!msg.isSettled) {
      try {
        await msg.ack();
      } catch (ackErr) {
        console.error(`Failed to auto-ack message ${msg.id}: ${ackErr}`);
      }
    }
  }

  /**
   * Stop the handler consumer: stop fetching, signal in-flight handlers via
   * their message AbortSignal, wait for the dispatch loop to exit and all
   * in-flight handlers to finish, then unregister from the connection.
   *
   * Idempotent — concurrent / repeated calls share the same shutdown.
   * Mirrors the Go HandlerConsumer.Stop() ordering: the underlying consumer
   * stop runs concurrently with the handler wait so cancelled handlers can
   * finish acking before the consumer's release path runs.
   */
  async stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    this.stopPromise = this.doStop();
    return this.stopPromise;
  }

  private async doStop(): Promise<void> {
    // Stop the underlying consumer. This cancels every in-flight message's
    // AbortSignal (so handlers can short-circuit), stops fetching, and
    // signals the iterator so a dispatch loop blocked in next() gets `done`.
    const consumerStopped = this.consumer.stop();

    // Wait for the dispatch loop to exit. It may be parked in next()
    // (unblocked by consumer.stop above) or in acquireSlot() (unblocked as
    // running handlers release their slots).
    if (this.dispatchPromise) {
      await this.dispatchPromise.catch(() => {});
    }

    // Wait for every in-flight handler to settle. runHandler never rejects,
    // but allSettled keeps this defensive.
    await Promise.allSettled(Array.from(this.activeHandlers));

    // Ensure the underlying consumer finished its own shutdown.
    await consumerStopped;

    this.connection.unregisterConsumer(this);
  }
}
