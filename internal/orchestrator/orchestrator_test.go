package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kevrocks67/latent/internal/metadata"
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
	}
	return nil
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

func (m *MockCoordinator) Close() {
}

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

// --- Tests ---

func TestOrchestrator_TTLConfiguration(t *testing.T) {
	t.Run("Custom Domain TTL", func(t *testing.T) {
		orchestrator := New(nil, nil, nil, nil, 10*time.Minute)
		domain := "special.com"
		expected := 24 * time.Hour

		orchestrator.SetDomainTTL(domain, expected)

		// Test internal getTTLForURL through a simulated URL
		ttl := orchestrator.getTTLForURL("https://special.com/artifact")
		if ttl != expected {
			t.Errorf("expected %v, got %v", expected, ttl)
		}

		// Test default fallback
		ttlDefault := orchestrator.getTTLForURL("https://other.com/artifact")
		if ttlDefault != 10*time.Minute {
			t.Errorf("expected default 10m, got %v", ttlDefault)
		}
	})
}

func TestOrchestrator_Fetch_Logic(t *testing.T) {
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
		reader, err := orchestrator.Fetch(context.Background(), url)
		if err != nil {
			t.Fatalf("Fetch failed on hit: %v", err)
		}
		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, content) {
			t.Errorf("expected %s, got %s", content, data)
		}
	})

	t.Run("Leader Success - Fill and Serve", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("fresh-data"))
		}))
		defer ts.Close()

		key, _ := GenerateCacheKey(ts.URL)
		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		storage := &MockStorage{}
		orchestrator := New(store, coord, storage, nil, 1*time.Hour)

		reader, err := orchestrator.Fetch(context.Background(), ts.URL)
		if err != nil {
			t.Fatalf("Fetch failed for leader: %v", err)
		}

		data, _ := io.ReadAll(reader)
		if string(data) != "fresh-data" {
			t.Errorf("expected fresh-data, got %s", string(data))
		}

		// Verify side effects
		time.Sleep(50 * time.Millisecond) // wait for async storage write
		if string(storage.data["artifacts/"+key]) != "fresh-data" {
			t.Error("data was not saved to storage")
		}
		rec, _ := store.GetRecord(context.Background(), key)
		if rec.State != metadata.StateReady {
			t.Errorf("expected state ready, got %v", rec.State)
		}
	})

	t.Run("StateError Handling", func(t *testing.T) {
		key, _ := GenerateCacheKey("http://error.com")
		store := &MockMetaStore{records: map[string]*metadata.Record{
			key: {State: metadata.StateError},
		}}
		orchestrator := New(store, nil, nil, nil, 1*time.Hour)
		_, err := orchestrator.Fetch(context.Background(), "http://error.com")
		if err == nil || !strings.Contains(err.Error(), "previous fill attempt failed") {
			t.Errorf("expected state error message, got %v", err)
		}
	})

	t.Run("Follower Wait and Retry", func(t *testing.T) {
		url := "http://follower.com"
		key, _ := GenerateCacheKey(url)
		store := &MockMetaStore{records: map[string]*metadata.Record{
			key: {State: metadata.StateFilling},
		}}
		coord := &MockCoordinator{}
		storage := &MockStorage{data: map[string][]byte{
			"artifacts/" + key: []byte("finally-ready"),
		}}
		orchestrator := New(store, coord, storage, nil, 1*time.Hour)

		// Simulate background process making it ready
		go func() {
			time.Sleep(50 * time.Millisecond)
			store.mu.Lock()
			store.records[key].State = metadata.StateReady
			store.records[key].ObjectKey = "artifacts/" + key
			store.mu.Unlock()
			coord.SignalReady(context.Background(), key)
		}()

		reader, err := orchestrator.Fetch(context.Background(), url)
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}
		data, _ := io.ReadAll(reader)
		if string(data) != "finally-ready" {
			t.Errorf("expected finally-ready, got %s", string(data))
		}
	})

	t.Run("Race - Record becomes ready after lock acquired", func(t *testing.T) {
		url := "http://race.com"
		key, _ := GenerateCacheKey(url)
		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		storage := &MockStorage{data: map[string][]byte{
			"artifacts/" + key: []byte("late-arrival"),
		}}
		orchestrator := New(store, coord, storage, nil, 1*time.Hour)

		// Set to ready just before Fetch gets through its internal checks
		store.mu.Lock()
		store.records[key] = &metadata.Record{State: metadata.StateReady, ObjectKey: "artifacts/" + key}
		store.mu.Unlock()

		reader, err := orchestrator.Fetch(context.Background(), url)
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}
		data, _ := io.ReadAll(reader)
		if string(data) != "late-arrival" {
			t.Errorf("expected late-arrival, got %s", string(data))
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

	t.Run("Upstream Request Construction Failure", func(t *testing.T) {
		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		orchestrator := New(store, coord, nil, nil, 1*time.Hour)
		// invalid URL characters trigger NewRequest error
		_, err := orchestrator.executeFill(context.Background(), "key", "http://[invalid-url]")
		if err == nil {
			t.Error("expected error for invalid URL")
		}
	})

	t.Run("Upstream Non-200 Response", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer ts.Close()

		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		orchestrator := New(store, coord, nil, nil, 1*time.Hour)

		_, err := orchestrator.executeFill(context.Background(), "key", ts.URL)
		if err == nil || !strings.Contains(err.Error(), "404") {
			t.Errorf("expected 404 error, got %v", err)
		}
		rec, _ := store.GetRecord(context.Background(), "key")
		if rec.State != metadata.StateError {
			t.Error("expected record to be in StateError")
		}
	})

	t.Run("Storage Writer Failure", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer ts.Close()

		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		storage := &MockStorage{failWrite: true}
		orchestrator := New(store, coord, storage, nil, 1*time.Hour)

		_, err := orchestrator.executeFill(context.Background(), "key", ts.URL)
		if err == nil || !strings.Contains(err.Error(), "failed to open storage writer") {
			t.Errorf("expected storage writer failure, got %v", err)
		}
	})

	t.Run("Async Copy Failure", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Flaky body that errors during read
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			// Close connection mid-stream to cause copy error
			panic("force close")
		}))
		defer ts.Close()

		store := &MockMetaStore{records: make(map[string]*metadata.Record)}
		coord := &MockCoordinator{}
		storage := &MockStorage{}
		orchestrator := New(store, coord, storage, nil, 1*time.Hour)

		// executeFill returns the pipe reader
		pr, err := orchestrator.executeFill(context.Background(), "key", ts.URL)
		if err != nil {
			t.Fatalf("executeFill failed: %v", err)
		}

		// Reading from the pipe should eventually show the error or just close
		_, _ = io.ReadAll(pr)

		// Check if state was updated to error in the background
		time.Sleep(50 * time.Millisecond)
		rec, _ := store.GetRecord(context.Background(), "key")
		if rec.State != metadata.StateError {
			t.Errorf("expected error state after failed copy, got %v", rec.State)
		}
	})
}

// errorTransport implements http.RoundTripper to always return an error
type errorTransport struct{}

func (e *errorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, errors.New("connection refused")
}

func TestOrchestrator_executeFill_HttpClientFailure(t *testing.T) {
	store := &MockMetaStore{records: make(map[string]*metadata.Record)}
	coord := &MockCoordinator{}
	storage := &MockStorage{}

	// Create client with failing transport
	client := &http.Client{Transport: &errorTransport{}}

	orchestrator := New(store, coord, storage, client, 1*time.Hour)

	_, err := orchestrator.Fetch(context.Background(), "http://fail.com")
	if err == nil || !strings.Contains(err.Error(), "upstream request failed") {
		t.Errorf("expected upstream request failure, got %v", err)
	}

	// Verify state was set to error
	key, _ := GenerateCacheKey("http://fail.com")
	rec, _ := store.GetRecord(context.Background(), key)
	if rec == nil || rec.State != metadata.StateError {
		t.Errorf("expected state to be set to error, got %v", rec)
	}
}

func TestOrchestrator_Fetch_KeyGenerationError(t *testing.T) {
	// Minimal initialization to avoid nil dereference if path continues
	orchestrator := New(&MockMetaStore{}, &MockCoordinator{}, &MockStorage{}, nil, 1*time.Hour)
	_, err := orchestrator.Fetch(context.Background(), "http://:invalid")
	if err == nil {
		t.Error("expected error for malformed URL")
	}
}

func TestOrchestrator_Fetch_MetadataStoreError(t *testing.T) {
	store := &MockMetaStore{failGetRecord: true}
	orchestrator := New(store, &MockCoordinator{}, &MockStorage{}, nil, 1*time.Hour)
	_, err := orchestrator.Fetch(context.Background(), "http://example.com")
	if err == nil || !strings.Contains(err.Error(), "metadata lookup failed") {
		t.Errorf("expected metadata lookup error, got %v", err)
	}
}

func TestOrchestrator_Fetch_CoordinatorWaitError(t *testing.T) {
	key, _ := GenerateCacheKey("http://wait-fail.com")
	store := &MockMetaStore{records: map[string]*metadata.Record{
		key: {State: metadata.StateFilling},
	}}
	coord := &MockCoordinator{failWait: true}
	orchestrator := New(store, coord, &MockStorage{}, nil, 1*time.Hour)
	_, err := orchestrator.Fetch(context.Background(), "http://wait-fail.com")
	if err == nil || !strings.Contains(err.Error(), "wait for ready failed") {
		t.Errorf("expected wait failure, got %v", err)
	}
}

func TestOrchestrator_Fetch_LockAcquisitionError(t *testing.T) {
	store := &MockMetaStore{records: make(map[string]*metadata.Record)}
	coord := &MockCoordinator{failAcquire: true}
	orchestrator := New(store, coord, &MockStorage{}, nil, 1*time.Hour)
	_, err := orchestrator.Fetch(context.Background(), "http://example.com")
	if err == nil || !strings.Contains(err.Error(), "failed to acquire coordination lock") {
		t.Errorf("expected lock acquisition failure, got %v", err)
	}
}

func TestOrchestrator_Fetch_FollowerRetryLoop(t *testing.T) {
	url := "http://retry.com"
	key, _ := GenerateCacheKey(url)
	store := &MockMetaStore{records: make(map[string]*metadata.Record)}
	coord := &MockCoordinator{locks: map[string]bool{key: true}}
	storage := &MockStorage{data: map[string][]byte{"foo": []byte("follower-data")}}
	orchestrator := New(store, coord, storage, nil, 1*time.Hour)

	go func() {
		time.Sleep(50 * time.Millisecond)
		coord.mu.Lock()
		delete(coord.locks, key)
		store.mu.Lock()
		store.records[key] = &metadata.Record{
			State:     metadata.StateReady,
			ObjectKey: "foo",
			CacheKey:  key,
		}
		store.mu.Unlock()
		coord.mu.Unlock()
	}()

	reader, err := orchestrator.Fetch(context.Background(), url)
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	defer reader.Close()
}

func TestOrchestrator_Fetch_DoubleCheckRace(t *testing.T) {
	url := "http://race-after.com"
	key, _ := GenerateCacheKey(url)
	store := &MockMetaStore{records: make(map[string]*metadata.Record)}
	coord := &MockCoordinator{}
	storage := &MockStorage{data: map[string][]byte{"fast": []byte("data")}}
	orchestrator := New(store, coord, storage, nil, 1*time.Hour)

	// Simulate a race where the record becomes Ready between the first Get and the Lock
	callCount := 0
	store.onGet = func(k string) {
		if k == key {
			callCount++
			if callCount == 2 {
				store.mu.Lock()
				store.records[key] = &metadata.Record{
					State:     metadata.StateReady,
					ObjectKey: "fast",
					CacheKey:  key,
				}
				store.mu.Unlock()
			}
		}
	}

	reader, err := orchestrator.Fetch(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if string(data) != "data" {
		t.Errorf("expected data, got %s", string(data))
	}
}
