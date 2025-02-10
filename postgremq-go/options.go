package postgremq_go

import (
	"fmt"
	"time"
)

// Connection options
type ConnectionOption func(*Connection)

func WithShutdownTimeout(d time.Duration) ConnectionOption {
	return func(c *Connection) {
		c.shutdownTimeout = d
	}
}

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

func WithLogger(logger Logger) ConnectionOption {
	return func(c *Connection) {
		c.logger = logger
	}
}

// Queue options
type queueOptions struct {
	maxDeliveryAttempts  int
	keepAliveIntervalSec int
}

type QueueOption func(*queueOptions)

func defaultQueueOptions() queueOptions {
	return queueOptions{
		maxDeliveryAttempts:  3,
		keepAliveIntervalSec: 300,
	}
}

func WithMaxDeliveryAttempts(n int) QueueOption {
	return func(o *queueOptions) {
		o.maxDeliveryAttempts = n
	}
}

func WithKeepAliveInterval(seconds int) QueueOption {
	return func(o *queueOptions) {
		o.keepAliveIntervalSec = seconds
	}
}

// Consumer options
type consumeOptions struct {
	batchSize       int
	checkTimeout    time.Duration
	vt              int
	noAutoExtension bool
	extendBatchSize int
}

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

func WithBatchSize(n int) ConsumeOption {
	return func(o *consumeOptions) {
		o.batchSize = n
	}
}

func WithCheckTimeout(d time.Duration) ConsumeOption {
	return func(o *consumeOptions) {
		o.checkTimeout = d
	}
}

func WithVT(seconds int) ConsumeOption {
	return func(o *consumeOptions) {
		o.vt = seconds
	}
}

func WithNoAutoExtension() ConsumeOption {
	return func(o *consumeOptions) {
		o.noAutoExtension = true
	}
}

func WithExtendBatchSize(size int) ConsumeOption {
	return func(o *consumeOptions) {
		o.extendBatchSize = size
	}
}

// MessageOption represents an option for message operations
type MessageOption func(*messageOptions)

type messageOptions struct {
	delayUntil *time.Time
}

// WithDelayUntil sets when the message should become visible again
func WithDelayUntil(t time.Time) MessageOption {
	return func(o *messageOptions) {
		o.delayUntil = &t
	}
}

// PublishOption represents an option for message publishing
type PublishOption func(*publishOptions)

type publishOptions struct {
	deliverAfter *time.Time
}

// WithDeliverAfter sets the time when the message should become visible
func WithDeliverAfter(t time.Time) PublishOption {
	return func(o *publishOptions) {
		o.deliverAfter = &t
	}
}
