package postgremq_go

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// RetryConfig holds configuration for how to retry operations.
type RetryConfig struct {
	Disabled          bool
	MaxAttempts       int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
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

// IsRetryableError determines if an error should trigger a retry
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
		c.logger.Printf("Retryable error occurred (attempt %d/%d): %v",
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
