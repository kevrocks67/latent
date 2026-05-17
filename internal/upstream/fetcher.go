package upstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"time"

	"golang.org/x/sync/semaphore"
)

// UpstreamResult now includes essential metadata for caching and large-file handling.
type UpstreamResult struct {
	Body          io.ReadCloser
	ContentLength int64
	ETag          string
	StatusCode    int
}

type Fetcher interface {
	Fetch(ctx context.Context, url string) (*UpstreamResult, error)
}

type httpFetcher struct {
	client *http.Client
	sem    *semaphore.Weighted
}

// NewHTTPFetcher initializes a fetcher with strict concurrency limits and transport optimization.
func NewHTTPFetcher(timeout time.Duration, maxConcurrentRequests int64) Fetcher {
	transport := &http.Transport{
		MaxIdleConnsPerHost: int(maxConcurrentRequests),
		MaxIdleConns:        int(maxConcurrentRequests) * 2,
		MaxConnsPerHost:     int(maxConcurrentRequests),
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   90 * time.Second,
	}

	return &httpFetcher{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		sem: semaphore.NewWeighted(maxConcurrentRequests),
	}
}

func (f *httpFetcher) Fetch(ctx context.Context, url string) (*UpstreamResult, error) {
	// 1. Acquire semaphore to throttle concurrent socket usage.
	if err := f.sem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("concurrency limit reached/context cancelled: %w", err)
	}

	start := time.Now()
	var dnsStart, connStart, tlsStart time.Time
	var dnsDuration, connDuration, tlsDuration, ttfb time.Duration

	trace := &httptrace.ClientTrace{
		DNSStart:             func(info httptrace.DNSStartInfo) { dnsStart = time.Now() },
		DNSDone:              func(info httptrace.DNSDoneInfo) { dnsDuration = time.Since(dnsStart) },
		ConnectStart:         func(network, addr string) { connStart = time.Now() },
		ConnectDone:          func(network, addr string, err error) { connDuration = time.Since(connStart) },
		TLSHandshakeStart:    func() { tlsStart = time.Now() },
		TLSHandshakeDone:     func(state tls.ConnectionState, err error) { tlsDuration = time.Since(tlsStart) },
		GotFirstResponseByte: func() { ttfb = time.Since(start); log.Printf("fetch trace: first byte after %v for %s", ttfb, url) },
	}

	ctx = httptrace.WithClientTrace(ctx, trace)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		f.sem.Release(1)
		return nil, err
	}

	resp, err := f.client.Do(req)
	if err != nil {
		f.sem.Release(1)
		return nil, err
	}

	total := time.Since(start)
	log.Printf("fetch trace: url=%s status=%d total=%v dns=%v conn=%v tls=%v ttfb=%v", url, resp.StatusCode, total, dnsDuration, connDuration, tlsDuration, ttfb)

	if resp.StatusCode >= 400 {
		resp.Body.Close()
		f.sem.Release(1)
		return nil, fmt.Errorf("upstream error: %d", resp.StatusCode)
	}

	// Extract metadata needed for git pull / large object management
	contentLength, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	etag := resp.Header.Get("ETag")

	// 2. Wrap the body to release the semaphore only when the caller is done.
	return &UpstreamResult{
		Body: &sharedBody{
			ReadCloser: resp.Body,
			release:    func() { f.sem.Release(1) },
		},
		ContentLength: contentLength,
		ETag:          etag,
		StatusCode:    resp.StatusCode,
	}, nil
}

// sharedBody ensures the concurrency slot is returned to the pool
// only when the data has been fully consumed or the stream closed.
type sharedBody struct {
	io.ReadCloser
	release func()
}

func (s *sharedBody) Close() error {
	err := s.ReadCloser.Close()
	s.release()
	return err
}
