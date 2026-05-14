package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// mockS3Env encapsulates a local HTTP server that responds with AWS-compatible
// XML, allowing us to test the actual S3 SDK client without hitting the network.
type mockS3Env struct {
	server *httptest.Server
	client *s3.Client
	mu     sync.Mutex
	parts  [][]byte
}

func setupMockS3() *mockS3Env {
	env := &mockS3Env{}
	env.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env.mu.Lock()
		defer env.mu.Unlock()

		switch r.Method {
		case http.MethodPost:
			// CreateMultipartUpload
			if r.URL.Query().Has("uploads") {
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><UploadId>mock-upload-id</UploadId></InitiateMultipartUploadResult>`))
				// CompleteMultipartUpload
			} else if r.URL.Query().Has("uploadId") {
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ETag>"mock-etag"</ETag></CompleteMultipartUploadResult>`))
			}
		case http.MethodPut:
			// UploadPart
			if r.URL.Query().Has("partNumber") {
				data, _ := io.ReadAll(r.Body)
				env.parts = append(env.parts, data)
				w.Header().Set("ETag", `"mock-etag"`)
			} else {
				w.WriteHeader(http.StatusOK)
			}
		case http.MethodGet:
			// GetObject
			for _, p := range env.parts {
				w.Write(p)
			}
		case http.MethodHead:
			// HeadObject (Exists)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			// DeleteObject
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))

	// Initialize the real S3 client pointed at our mock server
	env.client = s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(env.server.URL),
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "mock", SecretAccessKey: "mock"}, nil
		}),
	})

	return env
}

func TestS3Store_Writer(t *testing.T) {
	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-key"

	tests := []struct {
		name          string
		size          int
		expectedParts int
	}{
		{
			name:          "Zero Byte File",
			size:          0,
			expectedParts: 1, // S3 multipart requires at least one part
		},
		{
			name:          "Small File (1KB)",
			size:          1024,
			expectedParts: 1,
		},
		{
			name:          "Exact Boundary (5MB)",
			size:          minPartSize,
			expectedParts: 1,
		},
		{
			name:          "Slightly Over Boundary (5MB + 1KB)",
			size:          minPartSize + 1024,
			expectedParts: 2,
		},
		{
			name:          "Large File (12MB)",
			size:          12 * 1024 * 1024,
			expectedParts: 3, // 5MB + 5MB + 2MB
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := setupMockS3()
			defer env.server.Close()

			store := NewS3Store(env.client, bucket)

			// 1. Initialize Writer
			writer, err := store.Writer(ctx, key)
			if err != nil {
				t.Fatalf("failed to create writer: %v", err)
			}

			// 2. Generate random test data
			data := make([]byte, tt.size)
			rand.Read(data)

			// 3. Perform the write
			n, err := io.Copy(writer, bytes.NewReader(data))
			if err != nil {
				t.Fatalf("io.Copy failed: %v", err)
			}
			if int(n) != tt.size {
				t.Errorf("wrote %d bytes, expected %d", n, tt.size)
			}

			// 4. Close (triggers final chunk upload)
			if err := writer.Close(); err != nil {
				t.Fatalf("writer.Close() failed: %v", err)
			}

			// 5. Verify chunking logic
			env.mu.Lock()
			defer env.mu.Unlock()

			if len(env.parts) != tt.expectedParts {
				t.Errorf("expected %d parts to be uploaded, got %d", tt.expectedParts, len(env.parts))
			}

			// Verify that all intermediate parts are exactly minPartSize (5MB)
			for i, p := range env.parts {
				if i < len(env.parts)-1 && len(p) != minPartSize {
					t.Errorf("intermediate part %d has size %d, expected exactly %d", i, len(p), minPartSize)
				}
			}
		})
	}
}

func TestS3Store_RoundTrip(t *testing.T) {
	ctx := context.Background()
	env := setupMockS3()
	defer env.server.Close()

	store := NewS3Store(env.client, "test-bucket")
	key := "engine/artifact.bin"

	content := make([]byte, 7*1024*1024) // 7MB file
	rand.Read(content)

	// --- SIMULATE ENGINE WRITING ---
	writer, err := store.Writer(ctx, key)
	if err != nil {
		t.Fatalf("failed to open writer: %v", err)
	}

	// Write in chunks (e.g. simulating HTTP stream of 512KB chunks)
	buf := content
	for len(buf) > 0 {
		take := 1024 * 512
		if take > len(buf) {
			take = len(buf)
		}
		_, err := writer.Write(buf[:take])
		if err != nil {
			t.Fatalf("failed to write chunk: %v", err)
		}
		buf = buf[take:]
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// --- SIMULATE ENGINE READING ---
	reader, err := store.Reader(ctx, key)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	captured, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read artifact: %v", err)
	}

	// --- VERIFICATION ---
	if !bytes.Equal(content, captured) {
		t.Fatal("Round-trip failed: captured content does not match original written content")
	}
}
