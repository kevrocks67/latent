package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// normalizeHost ensures hostnames are consistent (lowercase and removing default ports).
// It uses the scheme to safely identify if a port is redundant.
func normalizeHost(scheme, host string) string {
	h := strings.ToLower(host)
	if scheme == "http" && strings.HasSuffix(h, ":80") {
		return strings.TrimSuffix(h, ":80")
	} else if scheme == "https" && strings.HasSuffix(h, ":443") {
		return strings.TrimSuffix(h, ":443")
	}
	return h
}

// GenerateCacheKey creates a deterministic SHA256 hash for a given URL.
// It normalizes the input to ensure that equivalent URLs result in the same key,
// which is critical for sharding and avoiding redundant cache entries.
// Returns:
//   - string: A 64-character hexadecimal representation of the SHA256 hash.
//   - error: An error if the rawURL cannot be parsed as a valid URL.
func GenerateCacheKey(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	host := normalizeHost(u.Scheme, u.Host)

	// Trim trailing slashes from path and normalize spaces
	path := strings.ReplaceAll(u.EscapedPath(), "+", "%20")
	if path == "" {
		path = "/"
	}
	if path != "/" {
		path = strings.TrimSuffix(path, "/")
	}

	// Get URI parameters, sanitize common auth params, spaces and sort
	query_params := u.Query()
	for param := range query_params {
		if authParams[strings.ToLower(param)] {
			query_params.Del(param)
		}
	}
	queryStr := strings.ReplaceAll(query_params.Encode(), "+", "%20")

	// Generate sanitized URL
	// We include the scheme to prevent cache poisoning across http/https
	base := fmt.Sprintf("%s://%s%s?%s", u.Scheme, host, path, queryStr)

	hash := sha256.Sum256([]byte(base))
	return hex.EncodeToString(hash[:]), nil
}

// writeCounter tracks total bytes written to the underlying writer.
type writeCounter struct {
	w     io.WriteCloser
	total int64
}

func (wc *writeCounter) Write(p []byte) (int, error) {
	n, err := wc.w.Write(p)
	wc.total += int64(n)
	return n, err
}

// writeFunc is a small helper to allow inline Writers via closures.
// Example: lwp := writeFunc(func(p []byte) (int, error) { ... })
type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(p []byte) (int, error) { return f(p) }

// fillReadCloser triggers the finalization callback when the stream is closed.
type fillReadCloser struct {
	reader  io.Reader
	onClose func()
}

func (f *fillReadCloser) Read(p []byte) (int, error) {
	return f.reader.Read(p)
}

func (f *fillReadCloser) Close() error {
	f.onClose()
	return nil
}
