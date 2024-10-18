package postgremq_go

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// RetryConfig holds configuration for automatic retry of transient database errors.
//
// The client automatically retries operations that fail due to:
//   - Connection failures (PostgreSQL error class 08)
//   - Serialization failures (PostgreSQL error code 40001)
//   - Deadlock detection (PostgreSQL error code 40P01)
//
// Retry uses exponential backoff with jitter to avoid thundering herd problems.
type RetryConfig struct {
	// Disabled turns off retry logic entirely. When true, operations fail immediately on error.
	Disabled bool

	// MaxAttempts is the maximum number of attempts (including the initial attempt).
	// For example, MaxAttempts=3 means: 1 initial attempt + up to 2 retries.
	MaxAttempts int

	// InitialBackoff is the wait time before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff caps the backoff duration. Even with exponential growth, backoff
	// will never exceed this value.
	MaxBackoff time.Duration

	// BackoffMultiplier is applied to the previous backoff to calculate the next wait time.
	// For example, with InitialBackoff=100ms and BackoffMultiplier=2.0:
	//   Retry 1: wait 100ms
	//   Retry 2: wait 200ms
	//   Retry 3: wait 400ms (capped by MaxBackoff)
	BackoffMultiplier float64
}

func defaultRetryConfig() RetryConfig {
	return RetryConfig{
		Disabled:          false,
		MaxAttempts:       3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

// IsRetryableError determines if an error represents a transient database failure
// that should trigger a retry.
//
// Retryable errors include:
//   - Serialization failures (40001): Transaction conflicts that can succeed on retry
//   - Deadlock detection (40P01): Deadlocks that may resolve on retry
//   - Connection errors (08xxx): Network failures, connection timeouts, etc.
//
// Returns true if the operation should be retried, false otherwise.
func IsRetryableError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "40001" || pgErr.Code == "40P01" || strings.HasPrefix(pgErr.Code, "08") {
			return true
		}
		return false
	}
	// Fallback: Check if the error text contains known retryable codes.
	errStr := err.Error()
	if strings.Contains(errStr, "40001") || strings.Contains(errStr, "40P01") || strings.Contains(errStr, "08") {
		return true
	}
	return false
}

// withRetry executes the given operation with retries
// It checks whether the connection is stopped (via isClosed) both before and during retries.
func (c *Connection) withRetry(ctx context.Context, operation func(context.Context) error) error {
	if c.isClosed() {
		return ErrConnectionClosed
	}

	if c.retryConfig.Disabled {
		return operation(ctx)
	}

	var lastErr error
	backoff := c.retryConfig.InitialBackoff

	for attempt := 0; attempt < c.retryConfig.MaxAttempts; attempt++ {
		// If the connection is stopped mid-retry, return immediately.
		if c.isClosed() {
			return ErrConnectionClosed
		}

		err := operation(ctx)
		if err == nil {
			return nil
		}

		if !IsRetryableError(err) {
			return err // Non-retryable error, return immediately
		}

		lastErr = err
		c.logger.Warnf("Retryable error occurred (attempt %d/%d): %v",
			attempt+1, c.retryConfig.MaxAttempts, err)

		// Check if we should make another attempt
		if attempt == c.retryConfig.MaxAttempts-1 {
			break
		}

		// Wait before next attempt, but respect context cancellation
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}

		// Increase backoff for next attempt
		backoff = time.Duration(float64(backoff) * c.retryConfig.BackoffMultiplier)
		if backoff > c.retryConfig.MaxBackoff {
			backoff = c.retryConfig.MaxBackoff
		}
	}

	return lastErr
}
