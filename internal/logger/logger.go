package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

// Init sets up the global structured logger outputting to standard out.
func Init(levelStr string) {
	initCore(os.Stdout, levelStr)
}

// initCore handles the actual configuration, allowing tests to inject memory buffers.
func initCore(w io.Writer, levelStr string) {
	var level slog.Level

	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: level == slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Auto-redact sensitive configuration fields
			switch strings.ToLower(a.Key) {
			case "password", "secret", "token", "accesskey", "secretkey":
				return slog.Attr{Key: a.Key, Value: slog.StringValue("[REDACTED]")}
			}

			// Format timestamps for Loki/ELK compatibility
			if a.Key == slog.TimeKey {
				return slog.Attr{
					Key:   "timestamp",
					Value: slog.StringValue(a.Value.Time().UTC().Format(time.RFC3339)),
				}
			}

			return a
		},
	}

	handler := slog.NewJSONHandler(w, opts)
	slog.SetDefault(slog.New(handler))
}

type ctxKey string

// TraceKey is used to pass request IDs through the context
const TraceKey ctxKey = "trace_id"

// FromContext extracts trace IDs from context to keep distributed logs tied together
func FromContext(ctx context.Context) *slog.Logger {
	l := slog.Default()
	if traceID, ok := ctx.Value(TraceKey).(string); ok {
		return l.With("trace_id", traceID)
	}
	return l
}
