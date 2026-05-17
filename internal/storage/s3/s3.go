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

	// background uploader fields
	tasks  chan []byte
	wg     sync.WaitGroup
	err    error
	errMu  sync.Mutex
}

// background uploader: processes chunks sent to tasks channel serially.
func (w *s3MultipartWriter) startUploader() {
	go func() {
		for chunk := range w.tasks {
			// perform upload synchronously here so part numbers are sequential
			w.errMu.Lock()
			if w.err != nil {
				w.errMu.Unlock()
				w.wg.Done()
				continue
			}
			w.errMu.Unlock()

			// Upload the part
			partNum := func() int32 {
				w.mu.Lock()
				w.partNum++
				pn := w.partNum
				w.mu.Unlock()
				return pn
			}()

			reader := bytes.NewReader(chunk)
			input := &s3.UploadPartInput{
				Bucket:     aws.String(w.bucket),
				Key:        aws.String(w.key),
				UploadId:   aws.String(w.uploadID),
				PartNumber: aws.Int32(partNum),
				Body:       reader,
			}

			output, err := w.client.UploadPart(w.ctx, input)
			if err != nil {
				w.errMu.Lock()
				w.err = err
				w.errMu.Unlock()
				w.wg.Done()
				continue
			}

			w.mu.Lock()
			w.parts = append(w.parts, types.CompletedPart{
				ETag:       output.ETag,
				PartNumber: aws.Int32(partNum),
			})
			w.mu.Unlock()

			w.wg.Done()
		}
	}()
}

func (w *s3MultipartWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return 0, io.ErrClosedPipe
	}

	n = len(p)
	w.buffer = append(w.buffer, p...)

	// Drain full parts into the tasks queue without performing network I/O here.
	for len(w.buffer) >= minPartSize {
		chunk := make([]byte, minPartSize)
		copy(chunk, w.buffer[:minPartSize])
		w.buffer = w.buffer[minPartSize:]

		w.wg.Add(1)
		// non-blocking send should be safe because startUploader is running
	w.tasks <- chunk
	}

	w.mu.Unlock()

	return n, nil
}

func (w *s3MultipartWriter) Close() error {
	// Prevent further writes
	w.mu.Lock()
if w.closed {
	w.mu.Unlock()
	return nil
}
w.closed = true

// If there's remaining data or no parts uploaded yet, send it as final part
if len(w.buffer) > 0 || len(w.parts) == 0 {
	chunk := make([]byte, len(w.buffer))
	copy(chunk, w.buffer)
	w.buffer = nil
	w.wg.Add(1)
	w.tasks <- chunk
}

// Close the tasks channel and wait for uploads to finish
w.mu.Unlock()
close(w.tasks)

w.wg.Wait()

w.errMu.Lock()
if w.err != nil {
	// If any upload failed, attempt abort and return error
	w.errMu.Unlock()
	w.abort()
	return w.err
}
w.errMu.Unlock()

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

	w := &s3MultipartWriter{
		ctx:      ctx,
		client:   s.client,
		bucket:   s.bucket,
		key:      objectKey,
		uploadID: *output.UploadId,
		buffer:   make([]byte, 0, minPartSize),
		tasks:    make(chan []byte, 8), // small buffer to decouple writes from network
	}

	// Start background uploader
	w.startUploader()

	return w, nil
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
