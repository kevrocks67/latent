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
	"github.com/kevrocks67/latent/internal/logger"
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

// New initializes the main orchestaror with its required dependencies.
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

// Pull attempts to retrieve an artifact from cacho. If missing, it coordinates
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
				// Determine cooldown: base doubled per failure, capped
				base := 30 * time.Second
				timeCap := 1 * time.Hour
				cooldown := base
				for i := 0; i < record.FailureCount; i++ {
					cooldown *= 2
					if cooldown >= timeCap {
						cooldown = timeCap
						break
					}
				}
				// If last error is unset or cooldown expired, allow retry
				if record.LastErrorAt == nil || time.Since(*record.LastErrorAt) >= cooldown {
					logger.FromContext(ctx).Info("Pull: retrying", "key", key, "cooldown", cooldown.String())
					break
				}
				return nil, fmt.Errorf("cache entry is in error state; retry after %v", cooldown)
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

		record, err = o.metastore.GetRecord(ctx, key)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			// Release the distributed lock and return the error
			if relErr := o.coordinator.ReleaseLock(ctx, key, o.nodeID); relErr != nil {
				logger.FromContext(ctx).Warn("failed to release distributed lock after metadata lookup error",
					"artifact.key", key,
					"err", relErr,
				)
			}
			return nil, fmt.Errorf("metadata lookup failed: %w", err)
		}

		if record != nil && record.State == metadata.StateReady {
			if err := o.coordinator.ReleaseLock(ctx, key, o.nodeID); err != nil {
				logger.FromContext(ctx).Warn("distributed lock release failed during cache hit path",
					"artifact.key", key,
					"err", err,
				)
			}
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
	logger.FromContext(ctx).Info("artifact cache miss: executing backend fill pipeline", "artifact.key", key, "artifact.url", url)
	var releaseOnce sync.Once

	releaseLock := func() {
		releaseOnce.Do(func() {
			if err := o.coordinator.ReleaseLock(context.Background(), key, o.nodeID); err != nil {
				logger.FromContext(ctx).Warn("distributed lock release failed during cache fill path",
					"artifact.key", key,
					"err", err,
				)
			}
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
		if err := o.coordinator.SignalReady(ctx, key); err != nil {
			logger.FromContext(ctx).Warn("distributed lock failed to signal ready during metadata upsert",
				"artifact.key", key,
				"err", err,
			)
		}
		return nil, fmt.Errorf("failed to initialize metadata: %w", err)
	}
	logger.FromContext(ctx).Debug("cache lock intent registered in metadata database", "artifact.key", key)

	resp, err := o.fetcher.Fetch(ctx, url)
	if err != nil {
		// record failure and mark error state with increment
		if ierr := o.metastore.IncrementFailure(ctx, key); ierr != nil {
			logger.FromContext(ctx).Error("executeFill: IncrementFailure failed", "key", key, "err", ierr)
		}
		releaseLock()
		if serr := o.coordinator.SignalReady(ctx, key); serr != nil {
			logger.FromContext(ctx).Warn("failed to signal ready after upstream fetch failure",
				"artifact.key", key,
				"err", serr,
			)
		}
		return nil, fmt.Errorf("upstream fetch failed : %w", err)
	}
	logger.FromContext(ctx).Info("upstream artifact retrieval transaction complete",
		"artifact.url", url,
		"http.status_code", resp.StatusCode,
		"http.content_len", resp.ContentLength,
		"telemetry.total_ms", resp.TotalMS,
		"telemetry.dns_ms", resp.DNSMS,
		"telemetry.tls_ms", resp.TLSMS,
		"telemetry.ttfb_ms", resp.TTFBMS,
	)

	// If the size is part of the header, we update it
	if resp.ContentLength > 0 {
		if err := o.metastore.UpdateSizeBytes(ctx, key, resp.ContentLength); err != nil {
			logger.FromContext(ctx).Warn("failed to update stored size for artifact",
				"artifact.key", key,
				"content_length", resp.ContentLength,
				"err", err,
			)
		}
	}

	// Get writer to persist artifact
	sw, err := o.storage.Writer(ctx, objectKey)
	if err != nil {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.FromContext(ctx).Warn("failed to close response body after storage writer error",
				"artifact.key", key,
				"err", cerr,
			)
		}
		// record failure
		if ierr := o.metastore.IncrementFailure(ctx, key); ierr != nil {
			logger.FromContext(ctx).Error("executeFill: IncrementFailure failed", "key", key, "err", ierr)
		}
		releaseLock()
		if serr := o.coordinator.SignalReady(ctx, key); serr != nil {
			logger.FromContext(ctx).Warn("failed to signal ready after storage writer error",
				"artifact.key", key,
				"err", serr,
			)
		}

		return nil, fmt.Errorf("failed to open storage writer: %w", err)
	}
	logger.FromContext(ctx).Debug("executeFill: opened storage writer", "key", key)

	pr, pw := io.Pipe()

	detachedCtx := context.WithoutCancel(ctx)
	// Start the async fill. The LOCK IS HELD while this goroutine is running.
	go func() {
		defer func() {
			if cerr := resp.Body.Close(); cerr != nil {
				logger.FromContext(detachedCtx).Warn("failed to close response body in fill goroutine",
					"artifact.key", key,
					"err", cerr,
				)
			}
		}()
		defer func() {
			if cerr := pw.Close(); cerr != nil {
				logger.FromContext(detachedCtx).Warn("failed to close pipe writer in fill goroutine",
					"artifact.key", key,
					"err", cerr,
				)
			}
		}()
		defer func() {
			if cerr := sw.Close(); cerr != nil {
				logger.FromContext(detachedCtx).Warn("failed to close storage writer in fill goroutine",
					"artifact.key", key,
					"err", cerr,
				)
			}
		}()
		defer releaseLock()

		// Use writeCounter to get length of artifact as we write it to
		// persistent storage
		counter := &writeCounter{w: sw}

		// Wrap the pipe writer to log when the first bytes are written to client
		var firstOnce sync.Once
		lwp := writeFunc(func(p []byte) (int, error) {
			firstOnce.Do(func() {
				logger.FromContext(detachedCtx).Debug("executeFill: first bytes sent to client", "key", key, "len", len(p))
			})
			return pw.Write(p)
		})
		mw := io.MultiWriter(lwp, counter)

		logger.FromContext(detachedCtx).Info("streaming remote payload to object storage backend", "artifact.key", key)
		_, copyErr := io.Copy(mw, resp.Body)
		if copyErr != nil {
			if ierr := o.metastore.IncrementFailure(detachedCtx, key); ierr != nil {
				logger.FromContext(detachedCtx).Error("executeFill: IncrementFailure failed", "key", key, "err", ierr)
			}
			logger.FromContext(detachedCtx).Error("executeFill: copy error", "key", key, "err", copyErr)
		} else {
			if err := o.metastore.SetReady(detachedCtx, key, counter.total, resp.ETag); err != nil {
				logger.FromContext(detachedCtx).Error("failed to mark artifact ready in metadata store",
					"artifact.key", key,
					"storage.size_bytes", counter.total,
					"err", err,
				)
			} else {
				logger.FromContext(detachedCtx).Info("artifact cache fill successful: resource marked ready", "artifact.key", key, "storage.size_bytes", counter.total)
			}
		}
		if serr := o.coordinator.SignalReady(detachedCtx, key); serr != nil {
			logger.FromContext(detachedCtx).Warn("failed to signal ready after fill completion",
				"artifact.key", key,
				"err", serr,
			)
		}
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
