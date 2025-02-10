package postgremq_go

// Logger is the interface that wraps the basic logging method
type Logger interface {
	Printf(format string, v ...interface{})
}

// NoopLogger implements Logger interface but does nothing
type NoopLogger struct{}

func (l NoopLogger) Printf(format string, v ...interface{}) {}
