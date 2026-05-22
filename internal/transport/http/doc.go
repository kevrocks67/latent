// Package http implements the primary REST API layer for Latent.
//
// It provides the Gin-backed router orchestration, content-type sniffing,
// streaming proxies, and self-hosted OpenAPI/Swagger interactive UI consoles.
//
// # Architecture Hierarchy
//
// The HTTP delivery layer strictly functions as an entry point adapter. It completely decouples
// transport mechanisms from your execution domains by relying on an abstract Orchestrator interface:
//
//	Client  --->  [HTTP Handler]  --->  [Orchestrator Interface]  --->  Core Engines (S3/Valkey/Postgres)
//
// # Critical Transport Design Primitives
//
//   - Non-Blocking Content Sniffing: To prevent downloading massive files into local memory buffers
//     before writing to the wire, the handler wraps underlying storage streams in a buffered reader,
//     safely peering into the initial 64 bytes to sniff the target Content-Type dynamically.
//   - Safe Stream Resource Closure: All incoming data reads utilize strict deferred loop closures to
//     prevent network socket descriptor leaking or unclosed TCP pools.
//   - Chunked Encoding Streaming: Responses use an unspecified length parameter (-1) passing directly
//     to DataFromReader, triggering real-time transfer-encoding chunks rather than allocating massive
//     heap blocks on host machinery.
//
// # Swagger Interface Directives
//
// To properly rebuild the specification from your project root using this schema, execute:
//
//	swag init -g cmd/latent/main.go --parseDependency
package http
