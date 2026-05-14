package orchestrator

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// KeyTestCase defines the structure for our JSON-based test fixtures
// used specifically for validating URL normalization and hashing logic.
type KeyTestCase struct {
	Name    string `json:"name"`
	URL1    string `json:"url1"`
	URL2    string `json:"url2"`
	IsEqual bool   `json:"is_equal"`
}

func TestGenerateCacheKey(t *testing.T) {
	// Load the fixtures from the testdata directory
	fixturePath := filepath.Join("testdata", "utils_GenerateCacheKey_fixtures.json")
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("failed to read fixtures at %s: %v", fixturePath, err)
	}

	var tests []KeyTestCase
	if err := json.Unmarshal(data, &tests); err != nil {
		t.Fatalf("failed to unmarshal fixtures: %v", err)
	}

	// Run the tests
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			hash1, err1 := GenerateCacheKey(tt.URL1)
			if err1 != nil {
				t.Fatalf("Unexpected error for URL1 (%s): %v", tt.URL1, err1)
			}

			hash2, err2 := GenerateCacheKey(tt.URL2)
			if err2 != nil {
				t.Fatalf("Unexpected error for URL2 (%s): %v", tt.URL2, err2)
			}

			if tt.IsEqual {
				if hash1 != hash2 {
					t.Errorf("Expected hashes to be EQUAL\nURL1: %s\nURL2: %s\nHash1: %s\nHash2: %s", tt.URL1, tt.URL2, hash1, hash2)
				}
			} else {
				if hash1 == hash2 {
					t.Errorf("Expected hashes to be DIFFERENT\nURL1: %s\nURL2: %s\nHash: %s", tt.URL1, tt.URL2, hash1)
				}
			}
		})
	}
}

func TestGenerateCacheKey_InvalidURL(t *testing.T) {
	_, err := GenerateCacheKey("!!:not-a-valid-url-at-all")
	if err == nil {
		t.Error("Expected error for invalid URL string, got nil")
	}
}

type mockWriteCloser struct {
	bytes.Buffer
	closed bool
}

func (m *mockWriteCloser) Close() error {
	m.closed = true
	return nil
}

func TestWriteCounter(t *testing.T) {
	mock := &mockWriteCloser{}
	counter := &writeCounter{w: mock}

	data := []byte("hello world")
	n, err := counter.Write(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if int64(n) != int64(len(data)) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}

	if counter.total != int64(len(data)) {
		t.Errorf("expected total %d, got %d", len(data), counter.total)
	}

	if mock.String() != "hello world" {
		t.Errorf("expected buffer to contain 'hello world', got %s", mock.String())
	}
}

func TestFillReadCloser(t *testing.T) {
	content := "test content"
	reader := bytes.NewReader([]byte(content))
	closed := false

	frc := &fillReadCloser{
		reader: reader,
		onClose: func() {
			closed = true
		},
	}

	// Read data
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, frc)
	if err != nil {
		t.Fatalf("unexpected error during read: %v", err)
	}

	if buf.String() != content {
		t.Errorf("expected content %s, got %s", content, buf.String())
	}

	// Test Close callback
	if err := frc.Close(); err != nil {
		t.Fatalf("unexpected error during close: %v", err)
	}

	if !closed {
		t.Error("expected onClose callback to be triggered")
	}
}

func TestNormalizeHost(t *testing.T) {
	// Pre-generate base keys for different protocols to ensure protocol differences
	// are preserved while port 80/443 are stripped correctly.
	httpsBase, _ := GenerateCacheKey("https://example.com/file")
	httpBase, _ := GenerateCacheKey("http://example.com/file")

	tests := []struct {
		name    string
		url     string
		wantKey string
	}{
		{
			name:    "Strip Default HTTPS Port",
			url:     "https://example.com:443/file",
			wantKey: httpsBase,
		},
		{
			name:    "Strip Default HTTP Port",
			url:     "http://example.com:80/file",
			wantKey: httpBase,
		},
		{
			name:    "Case Insensitive Host",
			url:     "HTTPS://EXAMPLE.COM/file",
			wantKey: httpsBase,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKey, err := GenerateCacheKey(tt.url)
			if err != nil {
				t.Fatalf("Failed to generate key: %v", err)
			}
			if gotKey != tt.wantKey {
				t.Errorf("Normalization failed for %s: expected key %s, got %s", tt.url, tt.wantKey, gotKey)
			}
		})
	}
}

func TestNormalizeHost_CustomPorts(t *testing.T) {
	// These should NOT match the base key because the ports are non-standard
	baseKey, _ := GenerateCacheKey("https://example.com/file")

	customPortURL := "https://example.com:8080/file"
	gotKey, _ := GenerateCacheKey(customPortURL)

	if gotKey == baseKey {
		t.Errorf("Normalization incorrectly stripped a non-standard port: %s", customPortURL)
	}
}

func TestGenerateCacheKey_Errors(t *testing.T) {
	// Trigger the error path for url.Parse
	_, err := GenerateCacheKey(":\x00")
	if err == nil {
		t.Error("Expected error for unparsable URL, got nil")
	}
}

func TestGenerateCacheKey_AdvancedNormalization(t *testing.T) {
	t.Run("Query Parameter Determinism", func(t *testing.T) {
		url1 := "https://example.com/api?b=2&a=1"
		url2 := "https://example.com/api?a=1&b=2"

		key1, _ := GenerateCacheKey(url1)
		key2, _ := GenerateCacheKey(url2)

		if key1 != key2 {
			t.Error("Query parameters should be sorted before hashing")
		}
	})

	t.Run("Auth Parameter Stripping", func(t *testing.T) {
		// Test that common signature/token params are stripped to avoid cache fragmentation
		urlWithToken := "https://example.com/file?token=secret123&data=important"
		urlNoToken := "https://example.com/file?data=important"

		keyWith, _ := GenerateCacheKey(urlWithToken)
		keyWithout, _ := GenerateCacheKey(urlNoToken)

		if keyWith != keyWithout {
			t.Errorf("Auth parameters should be stripped from cache key.\nWith: %s\nWithout: %s", keyWith, keyWithout)
		}
	})

	t.Run("Cloud Provider Signatures", func(t *testing.T) {
		// Example AWS S3 Presigned URL params
		urlS3 := "https://bucket.s3.amazonaws.com/obj?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=xyz&v=1"
		urlClean := "https://bucket.s3.amazonaws.com/obj?v=1"

		keyS3, _ := GenerateCacheKey(urlS3)
		keyClean, _ := GenerateCacheKey(urlClean)

		if keyS3 != keyClean {
			t.Error("Cloud provider signature parameters should be stripped")
		}
	})
}
