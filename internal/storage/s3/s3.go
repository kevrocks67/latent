package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// BlobStore defines the interface for raw binary storage.
type BlobStore interface {
	Writer(ctx context.Context, objectKey string) (io.WriteCloser, error)
	Reader(ctx context.Context, objectKey string) (io.ReadCloser, error)
	Exists(ctx context.Context, objectKey string) (bool, error)
	Delete(ctx context.Context, objectKey string) error
}

const (
	// S3 minimum part size is 5MB.
	minPartSize = 5 * 1024 * 1024
)

type S3Store struct {
	client *s3.Client
	bucket string
}

func NewS3Store(client *s3.Client, bucket string) *S3Store {
	return &S3Store{
		client: client,
		bucket: bucket,
	}
}

type s3MultipartWriter struct {
	ctx      context.Context
	client   *s3.Client
	bucket   string
	key      string
	uploadID string
	buffer   []byte
	parts    []types.CompletedPart
	partNum  int32
	closed   bool
	mu       sync.Mutex
}

func (w *s3MultipartWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, io.ErrClosedPipe
	}

	n = len(p)
	w.buffer = append(w.buffer, p...)

	// Upload parts as they reach the minimum size threshold
	for len(w.buffer) >= minPartSize {
		chunk := w.buffer[:minPartSize]
		if err := w.uploadPart(chunk); err != nil {
			return 0, err
		}
		// Move remaining data to start of buffer
		w.buffer = w.buffer[minPartSize:]
	}

	return n, nil
}

func (w *s3MultipartWriter) uploadPart(data []byte) error {
	w.partNum++
	partNum := w.partNum

	// bytes.NewReader satisfies io.ReadSeeker, allowing the SDK to calculate
	// Content-SHA256 and perform retries automatically.
	reader := bytes.NewReader(data)

	input := &s3.UploadPartInput{
		Bucket:     aws.String(w.bucket),
		Key:        aws.String(w.key),
		UploadId:   aws.String(w.uploadID),
		PartNumber: aws.Int32(partNum),
		Body:       reader,
	}

	output, err := w.client.UploadPart(w.ctx, input)
	if err != nil {
		return err
	}

	w.parts = append(w.parts, types.CompletedPart{
		ETag:       output.ETag,
		PartNumber: aws.Int32(partNum),
	})
	return nil
}

func (w *s3MultipartWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	// Upload any remaining bytes or ensure at least one part for empty files
	if len(w.buffer) > 0 || len(w.parts) == 0 {
		if err := w.uploadPart(w.buffer); err != nil {
			w.abort()
			return err
		}
	}

	// Finalize the multipart upload on S3
	_, err := w.client.CompleteMultipartUpload(w.ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(w.bucket),
		Key:      aws.String(w.key),
		UploadId: aws.String(w.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: w.parts,
		},
	})

	if err != nil {
		w.abort()
		return err
	}

	return nil
}

func (w *s3MultipartWriter) abort() {
	_, _ = w.client.AbortMultipartUpload(w.ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(w.bucket),
		Key:      aws.String(w.key),
		UploadId: aws.String(w.uploadID),
	})
}

func (s *S3Store) Writer(ctx context.Context, objectKey string) (io.WriteCloser, error) {
	output, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, err
	}

	return &s3MultipartWriter{
		ctx:      ctx,
		client:   s.client,
		bucket:   s.bucket,
		key:      objectKey,
		uploadID: *output.UploadId,
		buffer:   make([]byte, 0, minPartSize),
	}, nil
}

func (s *S3Store) Reader(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (s *S3Store) Exists(ctx context.Context, objectKey string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		var nf *types.NotFound
		if errors.As(err, &nsk) || errors.As(err, &nf) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *S3Store) Delete(ctx context.Context, objectKey string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	return err
}
