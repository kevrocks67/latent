package storage

import (
	"context"
	"errors"
	"io"
)

// ErrKeyNotFound is a sentinel error for when the artifact is not found in the bucket
var ErrKeyNotFound = errors.New("requested object key not found in storage bucket")

// BlobStore handles the 'Data Plane'—the raw storage of binary artifacts.
// This abstracts cloud provider specifics (GCS/S3) from the core engine.
//
// Implementation: internal/storage/gcs.go
type BlobStore interface {
	// Writer returns a stream to upload binary data.
	// The caller is responsible for calling Close() to commit the upload.
	Writer(ctx context.Context, objectKey string) (io.WriteCloser, error)

	// Reader returns a stream to download data from the bucket.
	// Returns an error if the object does not exist.
	Reader(ctx context.Context, objectKey string) (io.ReadCloser, error)

	// Exists provides a lightweight check to see if the artifact
	// is physically present in the storage backend.
	Exists(ctx context.Context, objectKey string) (bool, error)

	// Delete removes the artifact from the storage backend.
	Delete(ctx context.Context, objectKey string) error
}
