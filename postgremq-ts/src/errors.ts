/**
 * PostgreMQ typed errors.
 *
 * The SQL functions raise exceptions with custom SQLSTATE codes (PMQ01,
 * PMQ02, PMQ03) so application code can match on type, not error text.
 * The classes below mirror the Go client's sentinels (ErrLeaseLost,
 * ErrQueueNotFound, ErrValidation).
 *
 * Use `instanceof LeaseLostError` (or check `err.code === 'PMQ01'`) to
 * branch on the failure mode.
 */

/** SQLSTATE code for "lease lost" (PMQ01). */
export const ErrCodeLeaseLost = 'PMQ01';
/** SQLSTATE code for "queue or topic not found" (PMQ02). */
export const ErrCodeQueueNotFound = 'PMQ02';
/** SQLSTATE code for "validation error" (PMQ03). */
export const ErrCodeValidation = 'PMQ03';

/**
 * LeaseLostError is thrown by ack/nack/release/setVt/setVtBatch when the
 * consumer's lease is no longer valid: the visibility timeout expired,
 * the consumer token doesn't match (another consumer re-consumed), or
 * the message is no longer in 'processing' state. Handlers must be
 * idempotent — receiving this error means another consumer may have
 * processed the same message.
 */
export class LeaseLostError extends Error {
  readonly code = ErrCodeLeaseLost;
  constructor(message: string) {
    super(message);
    this.name = 'LeaseLostError';
  }
}

/**
 * QueueNotFoundError is thrown when a referenced queue or topic does not
 * exist (or is an expired exclusive queue). Usually a misconfiguration
 * — the operation will fail until the queue/topic is created.
 */
export class QueueNotFoundError extends Error {
  readonly code = ErrCodeQueueNotFound;
  constructor(message: string) {
    super(message);
    this.name = 'QueueNotFoundError';
  }
}

/**
 * ValidationError is thrown on caller-rejection paths: input validation
 * failures (negative timeouts, mismatched batch arrays, names containing
 * characters outside `[A-Za-z0-9_:.\-]`) AND state-precondition failures
 * (exclusive-queue duplicate, deleting a topic that still has messages).
 * The common thread is "the request was rejected before any state change."
 */
export class ValidationError extends Error {
  readonly code = ErrCodeValidation;
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

/**
 * ConnectionClosedError is thrown by `connect()` when the connection has
 * already been closed. close() is terminal: a connection cannot be
 * resurrected — callers must construct a new Connection. Mirrors the Go
 * client's ErrConnectionClosed sentinel.
 *
 * Pure client-side error (no SQLSTATE), so it carries no `code`.
 */
export class ConnectionClosedError extends Error {
  constructor(message: string = 'Connection has been closed; construct a new Connection to reconnect') {
    super(message);
    this.name = 'ConnectionClosedError';
  }
}

/**
 * Map a database error to one of the typed errors, if its SQLSTATE matches
 * a PostgreMQ code. Otherwise returns the input unchanged. nil pass-through.
 *
 * @internal — used by the Connection wrappers.
 */
export function mapDbError(err: any): Error {
  if (err == null) return err;
  // node-pg surfaces the SQLSTATE on `code`. Some drivers may use
  // `sqlState`. Both are safe to inspect.
  const code: string | undefined = err.code ?? err.sqlState;
  if (typeof code !== 'string') return err;
  switch (code) {
    case ErrCodeLeaseLost:
      return new LeaseLostError(err.message ?? 'lease lost');
    case ErrCodeQueueNotFound:
      return new QueueNotFoundError(err.message ?? 'queue or topic not found');
    case ErrCodeValidation:
      return new ValidationError(err.message ?? 'validation error');
  }
  return err;
}
