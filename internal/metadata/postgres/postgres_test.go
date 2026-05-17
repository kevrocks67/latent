//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kevrocks67/latent/internal/metadata"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testDB    *sql.DB
	testStore *PostgresStore
)

// ptr is a helper to take the address of a string literal.
func ptr(s string) *string {
	return &s
}

// TestMain manages the lifecycle of the Postgres container for the entire package.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemaPath, _ := filepath.Abs("../../db/schema.sql")

	container, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("latent_test"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
		postgres.WithInitScripts(schemaPath),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to start postgres container: %s", err))
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}

	testDB, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	testStore = NewPostgresStore(testDB)

	// Run tests
	code := m.Run()

	// Cleanup
	testDB.Close()
	container.Terminate(ctx)

	// Exit
	fmt.Printf("Tests exited with code %d\n", code)
}

// cleanupDB clears the table between tests to ensure isolation.
func cleanupDB(t *testing.T) {
	_, err := testDB.Exec("DELETE FROM cache_records")
	if err != nil {
		t.Fatalf("failed to cleanup database: %v", err)
	}
}

func TestPostgresStore_UpsertRecord(t *testing.T) {
	cleanupDB(t)
	ctx := context.Background()

	rec := &metadata.Record{
		CacheKey:    "key-1",
		ObjectKey:   "obj-1",
		OwnerNode:   ptr("node-1"),
		State:       metadata.StateFilling,
		FreshUntil:  time.Now().Add(time.Hour).Truncate(time.Microsecond),
		ValidatedAt: time.Now().Truncate(time.Microsecond),
	}

	if err := testStore.UpsertRecord(ctx, rec); err != nil {
		t.Fatalf("UpsertRecord failed: %v", err)
	}

	got, err := testStore.GetRecord(ctx, "key-1")
	if err != nil {
		t.Fatal(err)
	}

	if got.CacheKey != rec.CacheKey || got.State != rec.State {
		t.Errorf("got %+v, want %+v", got, rec)
	}
}

func TestPostgresStore_SetReady(t *testing.T) {
	cleanupDB(t)
	ctx := context.Background()

	key := "ready-key"
	testStore.UpsertRecord(ctx, &metadata.Record{
		CacheKey:   key,
		ObjectKey:  "obj-2",
		OwnerNode:  ptr("node-1"),
		State:      metadata.StateFilling,
		FreshUntil: time.Now().Add(time.Hour),
	})

	size := int64(5000)
	etag := "v1-etag"

	// Updated to match the 4-arg signature: ctx, key, size, etag
	if err := testStore.SetReady(ctx, key, size, etag); err != nil {
		t.Fatalf("SetReady failed: %v", err)
	}

	got, err := testStore.GetRecord(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if got.State != metadata.StateReady {
		t.Errorf("expected ready state, got %s", got.State)
	}
	if got.SizeBytes != size || got.ETag != etag {
		t.Errorf("metadata mismatch: size %d, etag %s", got.SizeBytes, got.ETag)
	}
}

func TestPostgresStore_UpdateState(t *testing.T) {
	cleanupDB(t)
	ctx := context.Background()

	key := "state-key"
	testStore.UpsertRecord(ctx, &metadata.Record{
		CacheKey:   key,
		ObjectKey:  "obj-3",
		OwnerNode:  ptr("node-1"),
		State:      metadata.StateFilling,
		FreshUntil: time.Now().Add(time.Hour),
	})

	if err := testStore.UpdateState(ctx, key, metadata.StateError); err != nil {
		t.Fatal(err)
	}

	got, err := testStore.GetRecord(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if got.State != metadata.StateError {
		t.Errorf("expected error state, got %s", got.State)
	}
}

func TestPostgresStore_DeleteRecord(t *testing.T) {
	cleanupDB(t)
	ctx := context.Background()

	key := "delete-key"
	testStore.UpsertRecord(ctx, &metadata.Record{
		CacheKey:   key,
		ObjectKey:  "obj-4",
		OwnerNode:  ptr("node-1"),
		State:      metadata.StateReady,
		FreshUntil: time.Now().Add(time.Hour),
	})

	if err := testStore.DeleteRecord(ctx, key); err != nil {
		t.Fatal(err)
	}

	got, err := testStore.GetRecord(ctx, key)
	// We check for the specific 'no rows' error because your PostgresStore implementation
	// currently returns sql.ErrNoRows instead of (nil, nil) for missing records.
	if err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			// This is actually success - the record is gone.
			return
		}
		t.Fatalf("unexpected error during GetRecord after deletion: %v", err)
	}

	if got != nil {
		t.Error("expected record to be deleted, but it still exists")
	}
}

func TestPostgresStore_IncrementFailure(t *testing.T) {
	cleanupDB(t)
	ctx := context.Background()
	key := "failure-key"
	rec := &metadata.Record{
		CacheKey:   key,
		ObjectKey:  "obj-fail",
		State:      metadata.StateFilling,
		FreshUntil: time.Now().Add(time.Hour),
	}

	if err := testStore.UpsertRecord(ctx, rec); err != nil {
		t.Fatalf("UpsertRecord failed: %v", err)
	}

	if err := testStore.IncrementFailure(ctx, key); err != nil {
		t.Fatalf("IncrementFailure failed: %v", err)
	}

	got, err := testStore.GetRecord(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got.FailureCount == 0 || got.LastErrorAt == nil {
		t.Errorf("expected failure recorded, got count=%d last=%v", got.FailureCount, got.LastErrorAt)
	}

	// Now call SetReady and ensure reset
	if err := testStore.SetReady(ctx, key, 123, "etag"); err != nil {
		t.Fatal(err)
	}
	got2, err := testStore.GetRecord(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got2.FailureCount != 0 || got2.LastErrorAt != nil {
		t.Errorf("expected failures reset on SetReady, got count=%d last=%v", got2.FailureCount, got2.LastErrorAt)
	}
}
