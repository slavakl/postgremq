/**
 * PostgreMQ TypeScript Client
 * Utility functions
 */

import { RetryPolicy } from './types';
import { PoolConfig, Pool } from 'pg';

// Define transient error codes for Node errors
const transientNodeErrorCodes = new Set([
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ECONNREFUSED',
  'ENOTFOUND',
  'EAI_AGAIN'
]);

// Define transient SQLSTATE codes for PostgreSQL errors
const transientSQLStateCodes = new Set([
  '40001', // serialization_failure
  '40P01'  // deadlock_detected
]);

/**
 * Determines if an error should trigger a retry.
 * @param {Error} error - The error object returned by node-postgres.
 * @returns {boolean} - True if the error is transient and can be retried.
 */
export function shouldRetry(error: any): boolean {
  // First, check if it's a Node.js network error.
  if (error.code && transientNodeErrorCodes.has(error.code)) {
    return true;
  }
  
  /* 
   * PostgreSQL errors include a SQLSTATE code in the error object.
   * Depending on node-postgres version and error type, the SQLSTATE may be in error.code
   * or as a separate property like error.sqlState.
   */
  if (error.sqlState && transientSQLStateCodes.has(error.sqlState)) {
    return true;
  }
  
  // In some cases, error.code itself is the SQLSTATE code.
  if (error.code && transientSQLStateCodes.has(error.code)) {
    return true;
  }
  
  return false;
}

/**
 * Implements an exponential backoff retry mechanism for database operations.
 * @param operation - The async function to retry
 * @param retryPolicy - The retry policy configuration
 * @returns The result of the operation if successful
 * @throws The last error encountered if all retries fail
 */
export async function withRetry<T>(
  operation: () => Promise<T>,
  retryPolicy: RetryPolicy
): Promise<T> {
  let lastError: Error | null = null;
  let attempt = 0;
  let backoffMs = retryPolicy.initialBackoffMs;

  while (attempt < retryPolicy.maxAttempts) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      // Only retry if the error is transient
      if (!shouldRetry(error)) {
        throw error;
      }
      
      attempt++;
      
      // If we've reached maximum attempts, throw the last error
      if (attempt >= retryPolicy.maxAttempts) {
        break;
      }
      
      // Calculate backoff time for next attempt
      await sleep(backoffMs);
      
      // Increase backoff for next attempt, up to the maximum
      backoffMs = Math.min(
        backoffMs * retryPolicy.backoffMultiplier,
        retryPolicy.maxBackoffMs
      );
    }
  }
  
  // If we've exhausted all retries, throw the last error
  throw lastError;
}

/**
 * Sleep for a specified duration
 * @param ms - Time in milliseconds to sleep
 * @returns A promise that resolves after the specified delay
 */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Default retry policy settings
 */
export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 5,
  initialBackoffMs: 100,
  backoffMultiplier: 2,
  maxBackoffMs: 5000
};

/**
 * Create a deferred promise that can be resolved/rejected from outside
 * @returns A tuple with the promise and functions to resolve/reject it
 */
export function createDeferred<T>(): [Promise<T>, (value: T) => void, (reason?: any) => void] {
  let resolve!: (value: T) => void;
  let reject!: (reason?: any) => void;
  
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  
  return [promise, resolve, reject];
}

/**
 * Event emitter class for internal event handling
 */
export class EventEmitter {
  private listeners: Map<string, Set<(...args: any[]) => void>> = new Map();

  /**
   * Register an event listener
   * @param event - The event name
   * @param listener - The callback function
   */
  on(event: string, listener: (...args: any[]) => void): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(listener);
  }

  /**
   * Remove an event listener
   * @param event - The event name
   * @param listener - The callback function to remove
   */
  off(event: string, listener: (...args: any[]) => void): void {
    const listeners = this.listeners.get(event);
    if (listeners) {
      listeners.delete(listener);
      if (listeners.size === 0) {
        this.listeners.delete(event);
      }
    }
  }

  /**
   * Emit an event with arguments
   * @param event - The event name
   * @param args - Arguments to pass to listeners
   */
  emit(event: string, ...args: any[]): void {
    const listeners = this.listeners.get(event);
    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(...args);
        } catch (error) {
          console.error(`Error in event listener for ${event}:`, error);
        }
      }
    }
  }

  /**
   * Remove all listeners for an event
   * @param event - The event name (optional - if omitted, removes all listeners)
   */
  removeAllListeners(event?: string): void {
    if (event) {
      this.listeners.delete(event);
    } else {
      this.listeners.clear();
    }
  }
}

export interface ConnectionOptions {
  // PostgreSQL connection string (overrides config if provided)
  connectionString?: string;
  // Connection pool configuration (creates new pool)
  config?: PoolConfig;
  // Existing pool instance (reuses existing pool)
  pool?: Pool;
  // Client-specific options
  shutdownTimeoutMs?: number;
  retry?: RetryPolicy;
} 