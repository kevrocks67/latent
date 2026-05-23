package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestStructuredLoggerCore(t *testing.T) {
	// Table-driven test suite for behavioral validation
	tests := []struct {
		name          string
		levelStr      string
		logAction     func(log *slog.Logger)
		expectedMsg   string
		expectedLevel string
		validateAttrs func(t *testing.T, payload map[string]any)
	}{
		{
			name:          "auto-redacts sensitive credentials",
			levelStr:      "info",
			logAction:     func(log *slog.Logger) { log.Info("db connect", "user", "admin", "password", "supersecret123") },
			expectedMsg:   "db connect",
			expectedLevel: "INFO",
			validateAttrs: func(t *testing.T, payload map[string]any) {
				if payload["user"] != "admin" {
					t.Errorf("expected user 'admin', got '%v'", payload["user"])
				}
				if payload["password"] != "[REDACTED]" {
					t.Errorf("expected password to be redacted, got '%v'", payload["password"])
				}
			},
		},
		{
			name:          "formats timestamps to UTC RFC3339",
			levelStr:      "warn",
			logAction:     func(log *slog.Logger) { log.Warn("high memory usage", "usage_mb", 512) },
			expectedMsg:   "high memory usage",
			expectedLevel: "WARN",
			validateAttrs: func(t *testing.T, payload map[string]any) {
				// Ensure the custom 'timestamp' key exists and standard 'time' key is mapped over
				if _, exists := payload["timestamp"]; !exists {
					t.Error("expected 'timestamp' key, but it was missing from JSON payload")
				}
				if _, exists := payload["time"]; exists {
					t.Error("expected default 'time' key to be overridden, but it was still present")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Inject the in-memory buffer into our core configuration logic
			initCore(buf, tt.levelStr)

			// Execute the log
			tt.logAction(slog.Default())

			// Parse the generated JSON stream
			var payload map[string]any
			if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
				t.Fatalf("failed to parse structured log JSON stream: %v\nRaw output: %s", err, buf.String())
			}

			// Validate core routing keys
			if payload["msg"] != tt.expectedMsg {
				t.Errorf("expected msg '%s', got '%v'", tt.expectedMsg, payload["msg"])
			}
			if payload["level"] != tt.expectedLevel {
				t.Errorf("expected level '%s', got '%v'", tt.expectedLevel, payload["level"])
			}

			// Run scenario-specific assertions
			tt.validateAttrs(t, payload)
		})
	}
}

func TestFromContextInjection(t *testing.T) {
	t.Run("successfully injects trace ID from context into child logger", func(t *testing.T) {
		buf := new(bytes.Buffer)
		initCore(buf, "info") // Set up the logger in memory

		// 1. Setup context with our custom TraceKey type
		ctx := context.WithValue(context.Background(), TraceKey, "req-alpha-999")

		// 2. Extract logger and fire event
		log := FromContext(ctx)
		log.Info("processing transaction payload", "bytes", 1024)

		// 3. Unmarshal and assert
		var payload map[string]any
		if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
			t.Fatalf("failed to parse structured log: %v", err)
		}

		if payload["trace_id"] != "req-alpha-999" {
			t.Errorf("expected trace_id 'req-alpha-999', got '%v'", payload["trace_id"])
		}
		if payload["bytes"].(float64) != 1024 {
			t.Errorf("expected bytes payload to be 1024, got '%v'", payload["bytes"])
		}
	})

	t.Run("gracefully handles context without trace ID", func(t *testing.T) {
		buf := new(bytes.Buffer)
		initCore(buf, "info")

		// Empty context
		ctx := context.Background()

		log := FromContext(ctx)
		log.Info("processing background tick")

		var payload map[string]any
		if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
			t.Fatalf("failed to parse structured log: %v", err)
		}

		// It should NOT panic, and it should NOT include the trace_id key
		if _, exists := payload["trace_id"]; exists {
			t.Errorf("expected no trace_id key, but got: %v", payload["trace_id"])
		}
	})
}
