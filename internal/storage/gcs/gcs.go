package gcs

import (
	"context"
	"errors"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GCSStore implements the BlobStore interface for Google Cloud Storage.
type GCSStore struct {
	client *storage.Client
	bucket string
}

// NewGCSStore creates a new GCS-backed BlobStore.
func NewGCSStore(client *storage.Client, bucket string) *GCSStore {
	return &GCSStore{
		client: client,
		bucket: bucket,
	}
}

// Writer returns an io.WriteCloser that streams data to a GCS object.
// GCS's native writer handles chunking and resumable uploads internally.
func (s *GCSStore) Writer(ctx context.Context, objectKey string) (io.WriteCloser, error) {
	return s.client.Bucket(s.bucket).Object(objectKey).NewWriter(ctx), nil
}

// Reader returns an io.ReadCloser for the specified GCS object.
func (s *GCSStore) Reader(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	reader, err := s.client.Bucket(s.bucket).Object(objectKey).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, err // Or wrap in a custom NotFound error if preferred
		}
		return nil, err
	}
	return reader, nil
}

// Exists checks if an object exists in the GCS bucket using Attrs.
func (s *GCSStore) Exists(ctx context.Context, objectKey string) (bool, error) {
	_, err := s.client.Bucket(s.bucket).Object(objectKey).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Delete removes an object from the GCS bucket.
func (s *GCSStore) Delete(ctx context.Context, objectKey string) error {
	err := s.client.Bucket(s.bucket).Object(objectKey).Delete(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil // Consistent with S3 behavior (idempotent delete)
		}
		return err
	}
	return nil
}

// ListObjects is an optional helper if you need to scan keys in a prefix.
func (s *GCSStore) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	it := s.client.Bucket(s.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		keys = append(keys, attrs.Name)
	}
	return keys, nil
}
