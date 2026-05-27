package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kevrocks67/latent/internal/metadata"
	"github.com/kevrocks67/latent/internal/upstream"
)

// --- Mocks ---

type MockMetaStore struct {
	mu            sync.Mutex
	records       map[string]*metadata.Record
	failSetReady  bool
	failGetRecord bool
	failUpsert    bool
	onGet         func(key string)
}

func (m *MockMetaStore) GetRecord(ctx context.Context, key string) (*metadata.Record, error) {
	if m.onGet != nil {
		m.onGet(key)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failGetRecord {
		return nil, errors.New("database connection error")
	}
	r, ok := m.records[key]
	if !ok {
		return nil, nil
	}
	copyRec := *r
	return &copyRec, nil
}

func (m *MockMetaStore) UpsertRecord(ctx context.Context, rec *metadata.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failUpsert {
		return errors.New("upsert failed")
	}
	copyRec := *rec
	if m.records == nil {
		m.records = make(map[string]*metadata.Record)
	}
	m.records[rec.CacheKey] = &copyRec
	return nil
}

func (m *MockMetaStore) UpdateSizeBytes(ctx context.Context, key string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.records[key]; ok {
		r.SizeBytes = size
	}
	return nil
}

func (m *MockMetaStore) SetReady(ctx context.Context, key string, size int64, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSetReady {
		return errors.New("failed to commit ready state")
	}
	if r, ok := m.records[key]; ok {
		r.State = metadata.StateReady
		r.SizeBytes = size
		r.ETag = etag
		// reset failure tracking
		r.FailureCount = 0
		r.LastErrorAt = nil
	}
	return nil
}

func (m *MockMetaStore) IncrementFailure(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.records[key]; ok {
		r.FailureCount++
		now := time.Now()
		r.LastErrorAt = &now
		r.State = metadata.StateError
		return nil
	}
	return errors.New("record not found")
}

func (m *MockMetaStore) UpdateState(ctx context.Context, key string, state metadata.CacheState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.records[key]; ok {
		r.State = state
	}
	return nil
}

func (m *MockMetaStore) DeleteRecord(ctx context.Context, key string) error { return nil }

type MockCoordinator struct {
	mu          sync.Mutex
	locks       map[string]bool
	waiting     map[string]chan struct{}
	done        map[string]bool
	failAcquire bool
	failWait    bool
}

func (m *MockCoordinator) getChan(key string) chan struct{} {
	if m.waiting == nil {
		m.waiting = make(map[string]chan struct{})
	}
	if ch, ok := m.waiting[key]; ok {
		return ch
	}
	ch := make(chan struct{})
	m.waiting[key] = ch
	return ch
}

func (m *MockCoordinator) AcquireLock(ctx context.Context, key string, nodeId string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failAcquire {
		return false, errors.New("coordinator unreachable")
	}
	if m.locks == nil {
		m.locks = make(map[string]bool)
	}
	if m.locks[key] {
		return false, nil
	}
	m.locks[key] = true
	return true, nil
}

func (m *MockCoordinator) ReleaseLock(ctx context.Context, key string, nodeId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks != nil {
		delete(m.locks, key)
	}
	return nil
}

func (m *MockCoordinator) WaitForReady(ctx context.Context, key string) error {
	m.mu.Lock()
	if m.failWait {
		m.mu.Unlock()
		return errors.New("coordinator wait failure")
	}
	ch := m.getChan(key)
	m.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MockCoordinator) SignalReady(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done == nil {
		m.done = make(map[string]bool)
	}
	m.done[key] = true
	if ch, ok := m.waiting[key]; ok {
		select {
		case <-ch:
		default:
			close(ch)
		}
	} else {
		ch := make(chan struct{})
		close(ch)
		if m.waiting == nil {
			m.waiting = make(map[string]chan struct{})
		}
		m.waiting[key] = ch
	}
	return nil
}

func (m *MockCoordinator) Close() {}

type MockStorage struct {
	mu        sync.Mutex
	data      map[string][]byte
	failWrite bool
}

func (m *MockStorage) Reader(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MockStorage) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrite {
		return nil, errors.New("storage write error")
	}
	return &mockWriter{key: key, store: m}, nil
}

func (m *MockStorage) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[key]
	return ok, nil
}

func (m *MockStorage) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data != nil {
		delete(m.data, key)
	}
	return nil
}

type mockWriter struct {
	key   string
	store *MockStorage
	buf   bytes.Buffer
}

func (w *mockWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *mockWriter) Close() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if w.store.data == nil {
		w.store.data = make(map[string][]byte)
	}
	w.store.data[w.key] = w.buf.Bytes()
	return nil
}

type MockFetcher struct {
	FetchFunc func(ctx context.Context, url string) (*upstream.Result, error)
}

func (f *MockFetcher) Fetch(ctx context.Context, url string) (*upstream.Result, error) {
	if f.FetchFunc != nil {
		return f.FetchFunc(ctx, url)
	}
	return &upstream.Result{
		Body: io.NopCloser(strings.NewReader("mock-fetch-data")),
		ETag: "mock-etag",
	}, nil
}

// --- Tests ---

func TestOrchestrator_Pull_Logic(t *testing.T) {
	t.Run("Cache Hit - Serving from Storage", func(t *testing.T) {
		url := "http://hit.com/data"
		key, _ := GenerateCacheKey(url)
		objKey := "cached/hit"
		content := []byte("already-here")

		store := &MockMetaStore{records: map[string]*metadata.Record{
			key: {State: metadata.StateReady, ObjectKey: objKey},
		}}
		storage := &MockStorage{data: map[string][]byte{
			objKey: content,
		}}

		orchestrator := New(store, nil, storage, nil, 1*time.Hour)
		reader, err := orchestrator.Pull(context.Background(), url)
		if err != nil {
			t.Fatalf("Pull failed on hit: %v", err)
		}
		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, content) {
			t.Errorf("expected %s, got %s", content, data)
		}
	})

	t.Run("Leader Success - Fill and Serve", func(t *testing.T) {
		key, _ := GenerateCacheKey("http://fresh.com")
		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		storage := &MockStorage{}
		fetcher := &MockFetcher{
			FetchFunc: func(ctx context.Context, url string) (*upstream.Result, error) {
				return &upstream.Result{
					Body: io.NopCloser(strings.NewReader("fresh-data")),
					ETag: "v1",
				}, nil
			},
		}

		orchestrator := New(store, coord, storage, fetcher, 1*time.Hour)

		reader, err := orchestrator.Pull(context.Background(), "http://fresh.com")
		if err != nil {
			t.Fatalf("Pull failed for leader: %v", err)
		}

		data, _ := io.ReadAll(reader)
		if string(data) != "fresh-data" {
			t.Errorf("expected fresh-data, got %s", string(data))
		}

		// Wait for background activities
		time.Sleep(100 * time.Millisecond)

		rec, _ := store.GetRecord(context.Background(), key)
		if rec == nil || rec.State != metadata.StateReady {
			t.Errorf("expected state ready, got %v", rec)
		}
	})
}

func TestOrchestrator_executeFill_Failures(t *testing.T) {
	t.Run("Metadata Upsert Failure", func(t *testing.T) {
		store := &MockMetaStore{failUpsert: true}
		coord := &MockCoordinator{locks: map[string]bool{"key": true}}
		orchestrator := New(store, coord, nil, nil, 1*time.Hour)

		_, err := orchestrator.executeFill(context.Background(), "key", "http://example.com")
		if err == nil || !strings.Contains(err.Error(), "failed to initialize metadata") {
			t.Errorf("expected upsert failure, got %v", err)
		}
		if coord.locks["key"] {
			t.Error("lock should have been released")
		}
	})

	t.Run("Fetcher Execution Failure", func(t *testing.T) {
		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		fetcher := &MockFetcher{
			FetchFunc: func(ctx context.Context, url string) (*upstream.Result, error) {
				return nil, errors.New("upstream timeout")
			},
		}
		orchestrator := New(store, coord, &MockStorage{}, fetcher, 1*time.Hour)

		_, err := orchestrator.executeFill(context.Background(), "key", "http://example.com")

		// The orchestrator likely wraps the error, check for "upstream"
		if err == nil || !strings.Contains(err.Error(), "upstream") {
			t.Errorf("expected upstream failure, got %v", err)
		}

		rec, _ := store.GetRecord(context.Background(), "key")
		if rec == nil || rec.State != metadata.StateError {
			t.Errorf("expected state error, got %v", rec)
		}
	})

	t.Run("Storage Writer Failure", func(t *testing.T) {
		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		storage := &MockStorage{failWrite: true}
		fetcher := &MockFetcher{}
		orchestrator := New(store, coord, storage, fetcher, 1*time.Hour)

		_, err := orchestrator.executeFill(context.Background(), "key", "http://example.com")
		if err == nil {
			t.Error("expected storage writer failure, got nil")
		}
	})

	t.Run("Async Copy Failure", func(t *testing.T) {
		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		storage := &MockStorage{}

		pr, pw := io.Pipe()
		go func() {
			pw.Write([]byte("some-data"))
			// simulate mid-stream failure
			pw.CloseWithError(errors.New("stream-interrupted"))
		}()

		fetcher := &MockFetcher{
			FetchFunc: func(ctx context.Context, url string) (*upstream.Result, error) {
				return &upstream.Result{Body: pr}, nil
			},
		}
		orchestrator := New(store, coord, storage, fetcher, 1*time.Hour)

		reader, err := orchestrator.executeFill(context.Background(), "key", "http://example.com")
		if err != nil {
			t.Fatalf("executeFill failed: %v", err)
		}

		// Consuming the reader triggers the copy to storage
		_, _ = io.ReadAll(reader)

		// Wait for the background goroutine that handles the copy error
		time.Sleep(150 * time.Millisecond)

		rec, _ := store.GetRecord(context.Background(), "key")
		if rec == nil || rec.State != metadata.StateError {
			t.Errorf("expected error state after failed copy, got %v", rec)
		}
	})
}

// New tests for StateError retry/backoff
func TestOrchestrator_Pull_StateErrorRetry(t *testing.T) {
	// Allow retry after cooldown (FailureCount=1 -> cooldown=60s). Set LastErrorAt far in the past.
	url := "http://retry.example/fetch"
	key, _ := GenerateCacheKey(url)
	past := time.Now().Add(-2 * time.Minute)
	store := &MockMetaStore{records: map[string]*metadata.Record{
		key: {State: metadata.StateError, ObjectKey: "obj-retry", FailureCount: 1, LastErrorAt: &past},
	}}
	coord := &MockCoordinator{}
	storage := &MockStorage{}
	fetcher := &MockFetcher{FetchFunc: func(ctx context.Context, u string) (*upstream.Result, error) {
		return &upstream.Result{Body: io.NopCloser(strings.NewReader("retried-data")), ETag: "e1"}, nil
	}}
	orch := New(store, coord, storage, fetcher, 1*time.Hour)

	r, err := orch.Pull(context.Background(), url)
	if err != nil {
		t.Fatalf("expected Pull to succeed after cooldown, got error: %v", err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(b) != "retried-data" {
		t.Fatalf("unexpected body: %s", string(b))
	}

	// allow background SetReady to run
	time.Sleep(50 * time.Millisecond)
	rec, _ := store.GetRecord(context.Background(), key)
	if rec == nil || rec.State != metadata.StateReady {
		t.Fatalf("expected record ready after retry, got %+v", rec)
	}
}

func TestOrchestrator_Pull_StateErrorTooSoon(t *testing.T) {
	// Too soon to retry: LastErrorAt within cooldown window
	url := "http://retry.soon/fetch"
	key, _ := GenerateCacheKey(url)
	recent := time.Now().Add(-10 * time.Second)
	store := &MockMetaStore{records: map[string]*metadata.Record{
		key: {State: metadata.StateError, ObjectKey: "obj-retry", FailureCount: 1, LastErrorAt: &recent},
	}}
	orch := New(store, &MockCoordinator{}, &MockStorage{}, &MockFetcher{}, 1*time.Hour)

	_, err := orch.Pull(context.Background(), url)
	if err == nil || !strings.Contains(err.Error(), "retry after") {
		t.Fatalf("expected retry-after error, got: %v", err)
	}
}
