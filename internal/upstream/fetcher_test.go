package upstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPFetcher_Fetch_Success(t *testing.T) {
	etag := "\"test-etag\""
	content := "hello world"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", "11")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(content))
	}))
	defer ts.Close()

	fetcher := NewHTTPFetcher(2*time.Second, 1)
	res, err := fetcher.Fetch(context.Background(), ts.URL)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, etag, res.ETag)
	assert.Equal(t, int64(len(content)), res.ContentLength)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(t, content, string(body))
	res.Body.Close()
}

func TestHTTPFetcher_ConcurrencyLimiting(t *testing.T) {
	headerSent := make(chan struct{})
	finishRequest := make(chan struct{})
	var once sync.Once

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() {
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			close(headerSent)
		})

		<-finishRequest
		_, _ = w.Write([]byte("done"))
	}))
	defer ts.Close()

	fetcher := NewHTTPFetcher(5*time.Second, 1)

	res1, err := fetcher.Fetch(context.Background(), ts.URL)
	require.NoError(t, err)
	<-headerSent

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = fetcher.Fetch(ctx, ts.URL)
	assert.Error(t, err)

	isTimeout := errors.Is(err, context.DeadlineExceeded)
	isLimit := IsConcurrencyLimitError(err)
	assert.True(t, isTimeout || isLimit, "Expected context timeout or concurrency limit error, got: %v", err)

	close(finishRequest)
	res1.Body.Close()

	res2, err := fetcher.Fetch(context.Background(), ts.URL)
	assert.NoError(t, err)
	if err == nil {
		res2.Body.Close()
	}
}

func TestHTTPFetcher_HighConcurrency(t *testing.T) {
	const limit = 10
	var activeCount atomic.Uint64
	var maxObserved atomic.Uint64
	var mu sync.Mutex

	releaseGate := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := activeCount.Add(1)

		mu.Lock()
		if current > maxObserved.Load() {
			maxObserved.Store(current)
		}
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		<-releaseGate
		activeCount.Add(^uint64(0)) // Decrement
	}))
	defer ts.Close()

	fetcher := NewHTTPFetcher(10*time.Second, limit)

	var wg sync.WaitGroup
	errs := make(chan error, limit+1)
	responses := make(chan io.ReadCloser, limit+1)

	for i := range limit {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			res, err := fetcher.Fetch(context.Background(), ts.URL)
			if err != nil {
				errs <- fmt.Errorf("worker %d failed: %w", id, err)
				return
			}
			responses <- res.Body
		}(i)
	}

	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := fetcher.Fetch(ctx, ts.URL)
	assert.Error(t, err)

	close(releaseGate)
	wg.Wait()
	close(responses)
	close(errs)

	for body := range responses {
		body.Close()
	}

	for err := range errs {
		assert.NoError(t, err)
	}

	finalMax := maxObserved.Load()
	assert.Equal(t, uint64(limit), finalMax)
}

func TestHTTPFetcher_ContextCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	fetcher := NewHTTPFetcher(5*time.Second, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := fetcher.Fetch(ctx, ts.URL)
	assert.Error(t, err)
}

func IsConcurrencyLimitError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "concurrency limit reached" || errors.Is(err, context.DeadlineExceeded)
}
