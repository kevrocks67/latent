// Package logger provides the structured logging engine for Latent
//
// It wraps the Go standard library's log/slog package to enforce strict cloud-native
// logging standards required by modern log aggregators (e.g., Loki, ELK, Datadog).
// By standardizing on slog, this package maintains zero third-party dependencies while
// providing zero-allocation logging capabilities.
//
// Core Features:
//   - Structured JSON Output: All logs are emitted as JSON to standard out for easy scraping.
//   - Automatic Redaction: Keys containing sensitive terms (password, secret, token) are automatically masked.
//   - Timestamp Normalization: Time records are strictly forced into UTC RFC3339 format.
//   - Distributed Tracing: Native support for extracting and appending trace_id values from context.Context.
//
// Initialization:
// The logger must be initialized once during application bootstrap, typically after
// the global configuration has been parsed:
//
//	logger.Init(cfg.Logging.Level)
//
// Usage:
// For standard application logs, use the global slog package directly:
//
//	slog.Info("server starting", "port", 8080)
//	slog.Error("failed to connect to database", "err", err, "host", dbHost)
//
// For context-aware operations (like HTTP requests or worker jobs), use FromContext
// to automatically attach distributed tracing identifiers:
//
//	log := logger.ForContext(ctx)
//	log.Info("processing job", "job_id", 123)
package logger
