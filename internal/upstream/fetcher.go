package upstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/kevrocks67/latent/internal/logger"
)

// Result now includes essential metadata for caching and large-file handling.
type Result struct {
	Body          io.ReadCloser
	ContentLength int64
	ETag          string
	StatusCode    int

	// Telemetry fields expressed in milliseconds as floats for fidelity
	TotalMS float64
	DNSMS   float64
	ConnMS  float64
	TLSMS   float64
	TTFBMS  float64
}

// Fetcher is the interface for getting artifacts from upstream
type Fetcher interface {
	Fetch(ctx context.Context, url string) (*Result, error)
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
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
	}

	return &httpFetcher{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		sem: semaphore.NewWeighted(maxConcurrentRequests),
	}
}

func (f *httpFetcher) Fetch(ctx context.Context, url string) (*Result, error) {
	// Acquire semaphore to throttle concurrent socket usage.
	if err := f.sem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("concurrency limit reached/context cancelled: %w", err)
	}

	start := time.Now()
	var dnsStart, connStart, tlsStart time.Time
	var dnsDuration, connDuration, tlsDuration, ttfb time.Duration

	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				// Explicitly wipe metrics if the connection was pulled from the keep-alive pool
				dnsDuration = 0
				connDuration = 0
				tlsDuration = 0
			}
		},
		DNSStart:          func(info httptrace.DNSStartInfo) { dnsStart = time.Now() },
		DNSDone:           func(info httptrace.DNSDoneInfo) { dnsDuration = time.Since(dnsStart) },
		ConnectStart:      func(network, addr string) { connStart = time.Now() },
		ConnectDone:       func(network, addr string, err error) { connDuration = time.Since(connStart) },
		TLSHandshakeStart: func() { tlsStart = time.Now() },
		TLSHandshakeDone:  func(state tls.ConnectionState, err error) { tlsDuration = time.Since(tlsStart) },
		GotFirstResponseByte: func() {
			ttfb = time.Since(start)
			logger.FromContext(ctx).Debug("fetch trace: first byte", "ttfb_ms", float64(ttfb.Seconds()*1000), "url", url)
		},
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
	logger.FromContext(ctx).Debug("fetch trace: summary",
		"url", url,
		"status", resp.StatusCode,
		"total_ms", total.Milliseconds(),
		"dns_ms", dnsDuration.Milliseconds(),
		"conn_ms", connDuration.Milliseconds(),
		"tls_ms", tlsDuration.Milliseconds(),
		"ttfb_ms", ttfb.Milliseconds(),
	)

	// Populate telemetry fields on the result for upstream callers

	if resp.StatusCode >= 400 {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.FromContext(ctx).Error("fetch failed to close response body", "err", cerr)
		}
		f.sem.Release(1)
		return nil, fmt.Errorf("upstream error: %d", resp.StatusCode)
	}

	// Extract metadata needed for git pull / large object management
	contentLength, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	etag := resp.Header.Get("ETag")

	// Wrap the body to release the semaphore only when the caller is done.
	return &Result{
		Body: &sharedBody{
			ReadCloser: resp.Body,
			release:    func() { f.sem.Release(1) },
		},
		ContentLength: contentLength,
		ETag:          etag,
		StatusCode:    resp.StatusCode,

		TotalMS: float64(total.Milliseconds()),
		DNSMS:   float64(dnsDuration.Milliseconds()),
		ConnMS:  float64(connDuration.Milliseconds()),
		TLSMS:   float64(tlsDuration.Milliseconds()),
		TTFBMS:  float64(ttfb.Milliseconds()),
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
