package metadata

import (
	"context"
	"time"
)

// CacheState represents the current lifecycle status of an artifact in the system.
// @Description Status of the artifact: missing, filling, ready, or stale.
type CacheState string

const (
	// StateMissing indicates the artifact is not present in storage
	StateMissing CacheState = "missing"

	// StateFilling indicates a coordinated fetch is currently in progress
	StateFilling CacheState = "filling"

	// StateReady indicates the artifact is stored and valid for serving.
	StateReady CacheState = "ready"

	// StateError indicates a failure to fill the cache
	StateError CacheState = "error"

	// StateStale indicates the artifact is past its TTL but may be served if revalidation fails.
	StateStale CacheState = "stale"
)

// Record represents the durable metadata for an artifact
// This is the source of truth for artifact identity and freshness.
type Record struct {
	// Unique identifier for the artifact request
	CacheKey string `json:"cache_key" db:"cache_key" example:"e3b0c44..."`

	// The location in the GCS/S3 bucket
	ObjectKey string `json:"object_key" db:"object_key"`

	// OwnerNode identifies which cluster member is responsible for this record.
	// In a stateless/non-sharded setup, this remains nil.
	// @Description Internal node ID for sharding/placement logic.
	OwnerNode *string `json:"owner_node,omitempty" db:"owner_node"`

	// The current lifecycle state (ready, filling, etc.)
	State CacheState `json:"state" db:"state" enums:"missing,filling,ready,stale"`

	// ETag is the entity tag from the origin server, used for conditional GETs (If-None-Match).
	ETag string `json:"etag" db:"etag"`

	// Total size in bytes
	SizeBytes int64 `json:"size_bytes" db:"size_bytes"`

	// When the cache entry expires
	FreshUntil time.Time `json:"fresh_until" db:"fresh_until"`

	// ValidatedAt is the timestamp of the last successful communication with the origin.
	// This is updated on 200 OK (new download) AND 304 Not Modified (revalidation).
	// Crucial for eviction policies and "Stale-While-Revalidate" logic.
	ValidatedAt time.Time `json:"validated_at" db:"validated_at"`

	// CreatedAt is used for auditing and cleanup policies.
	CreatedAt time.Time `json:"created_at" db:"created_at"`

	// UpdatedAt is used for auditing
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// MetadataStore defines the durable storage operations for cache records.
// This layer maps metadata objects to the database schema.
//
// Data Mapping:
// - CacheKey  -> db:"cache_key"   json:"cache_key"
// - ObjectKey -> db:"object_key"  json:"object_key"
// - State     -> db:"state"       json:"state"
//
// Implementation: internal/metadata/mysql.go
type MetadataStore interface {
	// GetRecord retrieves the metadata for a given key.
	// Returns nil, nil if the record is not found.
	GetRecord(ctx context.Context, key string) (*Record, error)

	// UpsertRecord creates or updates a record.
	// Mapping: Inserts into the 'artifacts' table using 'db' tags.
	// Used primarily to initialize a record in the 'FILLING' state.
	UpsertRecord(ctx context.Context, record *Record) error

	// UpdateState performs a partial update of the record's lifecycle state.
	// Use this for simple transitions like READY -> STALE or FILLING -> ERROR.
	UpdateState(ctx context.Context, key string, state CacheState) error
	// UpdateSizeBytes provides a "size hint" while a download is in progress.
	// This is used for progress tracking and is not considered the final verified size.
	UpdateSizeBytes(ctx context.Context, key string, size int64) error

	// SetReady marks a record as READY and sets its final size atomically.
	// This ensures that SizeBytes is never out of sync with the READY state.
	SetReady(ctx context.Context, key string, size int64, etag string) error

	// DeleteRecord removes the metadata entry from durable storage.
	DeleteRecord(ctx context.Context, key string) error
}
