package coordinator

import (
	"context"
	"time"
)

// Coordinator handles distributed locking and cluster-wide state synchronization.
// It uses Redis to prevent 'Thundering Herds' by ensuring only one node
// performs an upstream fetch for a specific key at a time.
//
// Implementation: internal/cache/valkey/valkey.go
type Coordinator interface {
	// AcquireLock attempts to get a distributed lock for a specific key.
	// Returns true if acquired, false if another node is currently filling.
	AcquireLock(ctx context.Context, key string, nodeId string, ttl time.Duration) (bool, error)

	// ReleaseLock removes the lock immediately.
	// Should be called via 'defer' once the Fill operation is completed or failed.
	ReleaseLock(ctx context.Context, key string, nodeId string) error

	// WaitForReady is a blocking call that uses Redis Pub/Sub or polling
	// to wait until a concurrent 'Fill' operation by another node transitions to 'Ready'.
	WaitForReady(ctx context.Context, key string) error

	// SignalReady broadcasts to all waiting nodes that an artifact is now available.
	// This unblocks any routines currently stuck in 'WaitForReady'.
	SignalReady(ctx context.Context, key string) error

	// Close the client connection
	Close()
}
