package core

import (
	"context"
	"time"
)

type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

type LogEvent struct {
	Time      time.Time
	Level     LogLevel
	Component string
	Operation string
	Message   string
	Fields    map[string]any
	Err       error
}

type Logger interface {
	Log(context.Context, LogEvent)
}

type LoggerFunc func(context.Context, LogEvent)

func (f LoggerFunc) Log(ctx context.Context, event LogEvent) {
	if f != nil {
		f(ctx, event)
	}
}

func logEvent(ctx context.Context, logger Logger, event LogEvent) {
	if logger == nil {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	logger.Log(ctx, event)
}
