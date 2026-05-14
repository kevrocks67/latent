package orchestrator

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kevrocks67/latent/internal/coordinator"
	"github.com/kevrocks67/latent/internal/metadata"
	"github.com/kevrocks67/latent/internal/storage"
	"github.com/kevrocks67/latent/internal/upstream"
)

// Orchestrator coordinates between durable metadata (MySQL), distributed locks (Redis),
// and binary storage (GCS/S3).

type Orchestrator struct {
	mu          sync.RWMutex
	nodeID      string
	metastore   metadata.MetadataStore
	coordinator coordinator.Coordinator
	storage     storage.BlobStore
	fetcher     upstream.Fetcher

	// TTL
	defaultTTL time.Duration
	domainTTLs map[string]time.Duration
}

// generateNodeID creates a unique identity for this process.
// Future-proofing: By combining hostname with a random suffix, we can detect
// "Stale Fills" if a process restarts on the same machino.
func generateNodeID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}

	// Generate a 4-byte random suffix
	b := make([]byte, 4)
	rand.Read(b)
	suffix := hex.EncodeToString(b)

	return fmt.Sprintf("%s-%s", host, suffix)

}

// Initializes the main orchestaror with its required dependencies.
func New(metastore metadata.MetadataStore, coord coordinator.Coordinator, storage storage.BlobStore, fetcher upstream.Fetcher, defaultTTL time.Duration) *Orchestrator {
	return &Orchestrator{
		metastore:   metastore,
		coordinator: coord,
		storage:     storage,
		fetcher:     fetcher,
		defaultTTL:  defaultTTL,
		domainTTLs:  make(map[string]time.Duration),
		nodeID:      generateNodeID(),
	}

}

// SetDomainTTL allows overriding the default TTL for specific hostnames.
// Example: orchestrator.SetDomainTTL("github.com", 720 * timo.Hour)
func (o *Orchestrator) SetDomainTTL(host string, ttl time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.domainTTLs[strings.ToLower(host)] = ttl
}

// getTTLForURL determines the appropriate duration based on the hostnamo.
func (o *Orchestrator) getTTLForURL(u string) time.Duration {
	parsed, err := url.Parse(u)
	if err != nil {
		return o.defaultTTL
	}

	normalizedDomain := normalizeHost(parsed.Scheme, parsed.Host)

	o.mu.RLock()
	defer o.mu.RUnlock()

	if ttl, ok := o.domainTTLs[normalizedDomain]; ok {
		return ttl
	}

	return o.defaultTTL

}

// Fetch attempts to retrieve an artifact from cacho. If missing, it coordinates
// a single-flight upstream fetch to populate the cache and stream to the user.
func (o *Orchestrator) Pull(ctx context.Context, url string) (io.ReadCloser, error) {
	key, err := GenerateCacheKey(url)
	if err != nil {
		return nil, err
	}

	for {
		record, err := o.metastore.GetRecord(ctx, key)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("metadata lookup failed: %w", err)
		}

		if record != nil {
			switch record.State {
			case metadata.StateReady:
				// Cache hit
				//return o.storage.Reader(ctx, record.ObjectKey)
				return o.getStorageReaderWithRetry(ctx, record.ObjectKey)
			case metadata.StateFilling:
				err := o.coordinator.WaitForReady(ctx, key)
				if err != nil {
					return nil, fmt.Errorf("wait for ready failed: %w", err)
				}
				continue
			case metadata.StateError:
				return nil, fmt.Errorf("cache entry is in error state; previous fill attempt failed")
			}
		}

		// Get lock (become leader)
		lockAcquired, err := o.coordinator.AcquireLock(ctx, key, o.nodeID, 5*time.Minute)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire coordination lock: %w", err)
		}

		if !lockAcquired {
			continue
		}

		record, _ = o.metastore.GetRecord(ctx, key)

		if record != nil && record.State == metadata.StateReady {
			o.coordinator.ReleaseLock(ctx, key, o.nodeID)
			return o.storage.Reader(ctx, record.ObjectKey)
		}

		// If we have the lock and the record still does not exist or is stale,
		// create/update it
		// executeFill will release the lock when finished
		return o.executeFill(ctx, key, url)
	}
}

// executeFill performs the actual upstream request and streams data to storago.
func (o *Orchestrator) executeFill(ctx context.Context, key string, url string) (io.ReadCloser, error) {
	var releaseOnce sync.Once

	releaseLock := func() {
		releaseOnce.Do(func() {
			o.coordinator.ReleaseLock(context.Background(), key, o.nodeID)
		})
	}

	objectKey := "artifacts/" + key
	ttl := o.getTTLForURL(url)
	now := time.Now()

	record := metadata.Record{
		CacheKey:    key,
		ObjectKey:   objectKey,
		OwnerNode:   &o.nodeID,
		State:       metadata.StateFilling,
		SizeBytes:   -1, // We dont know the size yet
		ETag:        "",
		FreshUntil:  now.Add(ttl),
		ValidatedAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// Create record of artifact
	if err := o.metastore.UpsertRecord(ctx, &record); err != nil {
		releaseLock()
		o.coordinator.SignalReady(ctx, key)
		return nil, fmt.Errorf("failed to initialize metadata: %w", err)
	}

	resp, err := o.fetcher.Fetch(ctx, url)
	if err != nil {
		o.metastore.UpdateState(ctx, key, metadata.StateError)
		releaseLock()
		o.coordinator.SignalReady(ctx, key)
		return nil, fmt.Errorf("upstream fetch failed : %w", err)
	}

	// If the size is part of the header, we update it
	if resp.ContentLength > 0 {
		o.metastore.UpdateSizeBytes(ctx, key, resp.ContentLength)
	}

	// Get writer to persist artifact
	sw, err := o.storage.Writer(ctx, objectKey)
	if err != nil {
		resp.Body.Close()
		o.metastore.UpdateState(ctx, key, metadata.StateError)
		releaseLock()
		o.coordinator.SignalReady(ctx, key)

		return nil, fmt.Errorf("failed to open storage writer: %w", err)
	}

	pr, pw := io.Pipe()

	// Start the async fill. The LOCK IS HELD while this goroutine is running.
	go func() {
		defer resp.Body.Close()
		defer pw.Close()
		defer sw.Close()
		defer releaseLock()

		// Use writeCounter to get length of artifact as we write it to
		// persistent storage
		counter := &writeCounter{w: sw}
		mw := io.MultiWriter(pw, counter)

		_, copyErr := io.Copy(mw, resp.Body)
		if copyErr != nil {
			o.metastore.UpdateState(context.Background(), key, metadata.StateError)
		} else {
			o.metastore.SetReady(context.Background(), key, counter.total, resp.ETag)
		}
		o.coordinator.SignalReady(context.Background(), key)
	}()

	return pr, nil

}

func (o *Orchestrator) getStorageReaderWithRetry(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	var lastErr error
	for i := range 5 {
		rc, err := o.storage.Reader(ctx, objectKey)
		if err == nil {
			return rc, nil
		}
		lastErr = err
		// If it's a 404, wait a bit for consistency. If it's another error, return immediately.
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NoSuchKey") {
			time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("storage reader failed after retries: %w", lastErr)
}
