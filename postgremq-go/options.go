package postgremq_go

import (
	"fmt"
	"time"
)

// ConnectionOption configures a Connection at construction time.
//
// Connection options are passed to Dial to customize the behavior of
// the connection, such as shutdown timeout, retry policy, and logging.
type ConnectionOption func(*Connection)

// WithShutdownTimeout sets how long Connection.Close waits for all Consumers
// to finish before continuing shutdown.
//
// When Connection.Close() is called, it waits for consumers to stop and
// in-flight messages to be processed. This timeout applies to the entire
// shutdown process.
//
// Parameters:
//   - d: Maximum duration to wait. Zero means wait indefinitely (default).
//
// If the timeout expires, shutdown continues anyway, potentially leaving
// some messages in 'processing' state (they will become visible again when
// their visibility timeout expires).
func WithShutdownTimeout(d time.Duration) ConnectionOption {
	return func(c *Connection) {
		c.shutdownTimeout = d
	}
}

// WithRetryConfig sets the retry policy for transient database errors.
//
// The retry policy is used by all Connection methods except those with
// "WithTx" suffix (PublishWithTx, AckWithTx), since transaction boundaries
// are controlled by the caller for those methods.
//
// Parameters:
//   - config: RetryConfig struct specifying max attempts, backoff parameters, etc.
//
// The default retry policy is:
//   - MaxAttempts: 3
//   - InitialBackoff: 100ms
//   - MaxBackoff: 2s
//   - BackoffMultiplier: 2.0
func WithRetryConfig(config RetryConfig) ConnectionOption {
	return func(c *Connection) {
		c.retryConfig = RetryConfig{
			Disabled:          config.Disabled,
			MaxAttempts:       config.MaxAttempts,
			InitialBackoff:    config.InitialBackoff,
			MaxBackoff:        config.MaxBackoff,
			BackoffMultiplier: config.BackoffMultiplier,
		}
	}
}

// WithoutRetries disables the retry policy for this Connection.
//
// When retries are disabled, operations fail immediately on the first error
// without any retry attempts. This can be useful for:
//   - Testing failure scenarios
//   - Applications that implement their own retry logic
//   - Strict latency requirements where retries are unacceptable
func WithoutRetries() ConnectionOption {
	return func(c *Connection) {
		c.retryConfig.Disabled = true
	}
}

func validateConnectionOptions(options *Connection) error {
	if options.shutdownTimeout < 0 {
		return fmt.Errorf("shutdown timeout must be positive")
	}
	if options.retryConfig.Disabled {
		return nil
	}
	if options.retryConfig.MaxAttempts <= 0 {
		return fmt.Errorf("max attempts must be positive")
	}
	if options.retryConfig.InitialBackoff <= 0 {
		return fmt.Errorf("initial backoff must be positive")
	}
	if options.retryConfig.MaxBackoff <= 0 {
		return fmt.Errorf("max backoff must be positive")
	}
	if options.retryConfig.BackoffMultiplier <= 0 {
		return fmt.Errorf("backoff multiplier must be positive")
	}
	return nil
}

// WithLogger installs a Logger (Printf-style) adapter used for warnings and errors.
//
// Parameters:
//   - logger: Logger interface with a single Printf method.
//
// The logger is used for:
//   - Warning messages (keep-alive failures, notification listener issues)
//   - Error messages (unexpected failures during background operations)
//   - Debug messages (when enabled)
func WithLogger(logger Logger) ConnectionOption {
	return func(c *Connection) {
		c.logger = &levelLogAdapter{logger: logger}
	}
}
// WithLevelLogger installs a leveled logger that supports Debug, Info, Warn, and Error methods.
//
// Parameters:
//   - logger: LevelLogger interface with Debug, Info, Warn, and Error methods.
//
// This provides more granular control over log levels compared to WithLogger.
func WithLevelLogger(logger LevelLogger) ConnectionOption {
	return func(c *Connection) {
		c.logger = logger
	}
}

// queueOptions holds configuration for queue creation.
type queueOptions struct {
	maxDeliveryAttempts  int
	keepAliveIntervalSec int
}

// QueueOption configures queue creation parameters.
//
// Queue options are passed to CreateQueue to customize the queue's behavior,
// such as max delivery attempts and keep-alive interval for exclusive queues.
type QueueOption func(*queueOptions)

func defaultQueueOptions() queueOptions {
	return queueOptions{
		maxDeliveryAttempts:  3,
		keepAliveIntervalSec: 300,
	}
}

// WithMaxDeliveryAttempts sets the maximum delivery attempts for a queue.
//
// When a message's delivery_attempts reaches this limit, it is moved to the
// Dead Letter Queue (DLQ) by the move_messages_to_dlq() function.
//
// Parameters:
//   - n: Maximum delivery attempts (0 for unlimited retries).
//
// Default is 3 when creating queues via the client.
//
// Messages increment delivery_attempts each time they are consumed. If a
// message is Nack'd or released due to visibility timeout expiration, it
// remains eligible for redelivery until max_delivery_attempts is reached.
func WithMaxDeliveryAttempts(n int) QueueOption {
	return func(o *queueOptions) {
		o.maxDeliveryAttempts = n
	}
}

// WithKeepAliveInterval sets the keep-alive interval (seconds) for exclusive queues.
//
// Exclusive queues are automatically deleted when their keep_alive_until
// timestamp expires. The Connection automatically extends the keep-alive
// timestamp roughly every (interval / 2) seconds while active.
//
// Parameters:
//   - seconds: Keep-alive interval in seconds. Default is 300 (5 minutes).
//
// If the Connection closes or crashes, the queue will be deleted after
// the interval expires. This is useful for temporary queues that should
// be cleaned up when no longer needed.
//
// Only applicable when creating exclusive queues (exclusive=true parameter
// in CreateQueue).
func WithKeepAliveInterval(seconds int) QueueOption {
	return func(o *queueOptions) {
		o.keepAliveIntervalSec = seconds
	}
}

// consumeOptions holds configuration for message consumption.
type consumeOptions struct {
	batchSize       int
	checkTimeout    time.Duration
	vt              int
	noAutoExtension bool
	extendBatchSize int
}

// ConsumeOption configures consumer behavior.
//
// Consumer options are passed to Consume to customize fetching, visibility
// timeout, and auto-extension behavior.
type ConsumeOption func(*consumeOptions)

func defaultConsumeOptions() consumeOptions {
	return consumeOptions{
		batchSize:       5,
		checkTimeout:    10 * time.Second,
		vt:              0,
		noAutoExtension: false,
		extendBatchSize: 100,
	}
}

func validateConsumeOptions(options *consumeOptions) error {
	if options.checkTimeout <= 0 {
		return fmt.Errorf("check timeout must be positive")
	}
	if options.vt == 0 && options.noAutoExtension {
		return fmt.Errorf("no auto extension is not supported without lock timeout")
	}
	if options.batchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	if options.vt < 0 {
		return fmt.Errorf("lock timeout must be positive")
	}
	return nil
}

// WithBatchSize sets how many messages to fetch per batch.
//
// Larger batch sizes reduce database round-trips but increase memory usage
// and time to first message. The Messages() channel is buffered to this size.
//
// Parameters:
//   - n: Number of messages per batch. Default is 5. Must be > 0.
func WithBatchSize(n int) ConsumeOption {
	return func(o *consumeOptions) {
		o.batchSize = n
	}
}

// WithCheckTimeout sets the maximum polling interval when no notifications are received.
//
// The consumer uses PostgreSQL LISTEN/NOTIFY for real-time notification when
// new messages arrive. However, notifications can be missed in some failure
// scenarios. This timeout ensures the consumer wakes up periodically to check
// for messages even if no notification was received.
//
// Parameters:
//   - d: Maximum wait time between checks. Default is 10 seconds. Must be > 0.
//
// Shorter timeouts increase database load but reduce latency in notification
// failure scenarios.
func WithCheckTimeout(d time.Duration) ConsumeOption {
	return func(o *consumeOptions) {
		o.checkTimeout = d
	}
}

// WithVT sets the visibility timeout (seconds) for consumed messages.
//
// When a message is consumed, it becomes invisible to other consumers for
// the visibility timeout duration. This prevents multiple consumers from
// processing the same message simultaneously.
//
// Parameters:
//   - seconds: Visibility timeout in seconds. Default is 0 (must be set). Must be >= 0.
//
// The visibility timeout should be longer than typical message processing time.
// If auto-extension is enabled (default), messages are automatically extended
// at 50% of the visibility timeout to prevent expiration during processing.
//
// If a message's visibility timeout expires before being acknowledged, it
// becomes visible to other consumers again.
func WithVT(seconds int) ConsumeOption {
	return func(o *consumeOptions) {
		o.vt = seconds
	}
}

// WithNoAutoExtension disables automatic visibility timeout extension.
//
// By default, the Consumer automatically extends visibility timeouts for
// in-flight messages at 50% of the VT period to prevent them from becoming
// visible to other consumers while still being processed.
//
// When auto-extension is disabled, the application is responsible for calling
// Message.SetVT() to extend the visibility timeout if processing takes longer
// than expected.
//
// Requires a non-zero VT (set via WithVT).
//
// Use this option if:
//   - You want fine-grained control over extension timing
//   - You know message processing always completes within the VT period
//   - You want to minimize database operations
func WithNoAutoExtension() ConsumeOption {
	return func(o *consumeOptions) {
		o.noAutoExtension = true
	}
}

// WithExtendBatchSize limits how many messages are extended in a single batch.
//
// When auto-extension is enabled, the Consumer batches visibility timeout
// extensions for efficiency. This option sets the maximum batch size.
//
// Parameters:
//   - size: Maximum messages per extension batch. Default is 100.
//
// Larger batch sizes reduce database round-trips but may cause delays in
// extending individual messages. The batch is processed when the first
// message in the batch reaches 50% of its VT period.
//
// Only applicable when auto-extension is enabled (default).
func WithExtendBatchSize(size int) ConsumeOption {
	return func(o *consumeOptions) {
		o.extendBatchSize = size
	}
}

// MessageOption configures message acknowledgment operations.
//
// Message options are passed to Message.Nack to customize behavior such as
// delayed redelivery.
type MessageOption func(*messageOptions)

type messageOptions struct {
	delayUntil *time.Time
}

// WithDelayUntil sets the absolute time when a Nacked message should become visible again.
//
// This allows implementing exponential backoff or scheduled retries for
// failed messages.
//
// Parameters:
//   - t: Absolute timestamp when the message should become visible.
//
// If not specified, Nack makes the message immediately visible for redelivery.
//
// Example:
//
//	// Retry after 5 minutes
//	msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(5*time.Minute)))
//
//	// Exponential backoff
//	delay := time.Duration(math.Pow(2, float64(msg.DeliveryAttempts))) * time.Second
//	msg.Nack(ctx, postgremq.WithDelayUntil(time.Now().Add(delay)))
func WithDelayUntil(t time.Time) MessageOption {
	return func(o *messageOptions) {
		o.delayUntil = &t
	}
}

// PublishOption configures message publishing behavior.
//
// Publish options are passed to Publish or PublishWithTx to customize
// behavior such as delayed delivery.
type PublishOption func(*publishOptions)

type publishOptions struct {
	deliverAfter *time.Time
}

// WithDeliverAfter sets the time when a newly published message should become visible.
//
// This allows scheduling messages for future delivery. The message is inserted
// into the database immediately but remains invisible to consumers until the
// specified time.
//
// Parameters:
//   - t: Absolute timestamp when the message should become visible.
//
// If not specified, the message is immediately visible for consumption.
//
// Example:
//
//	// Deliver in 1 hour
//	conn.Publish(ctx, "topic", payload, postgremq.WithDeliverAfter(time.Now().Add(time.Hour)))
//
//	// Schedule for specific time
//	scheduleTime := time.Date(2025, 12, 25, 9, 0, 0, 0, time.UTC)
//	conn.Publish(ctx, "topic", payload, postgremq.WithDeliverAfter(scheduleTime))
func WithDeliverAfter(t time.Time) PublishOption {
	return func(o *publishOptions) {
		o.deliverAfter = &t
	}
}
