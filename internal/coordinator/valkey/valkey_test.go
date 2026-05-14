package valkey

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/valkey-io/valkey-go"
)

var (
	testClient      valkey.Client
	testCoordinator *ValkeyCoordinator
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Using 8.1-alpine for a stable, modern test environment
	req := testcontainers.ContainerRequest{
		Image:        "valkey/valkey:8.1-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Printf("failed to start container: %v\n", err)
		os.Exit(1)
	}

	defer func() {
		if err := container.Terminate(ctx); err != nil {
			fmt.Printf("failed to terminate container: %v\n", err)
		}
	}()

	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		fmt.Printf("failed to get endpoint: %v\n", err)
		os.Exit(1)
	}

	testClient, err = valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{endpoint},
		SelectDB:    0,
	})
	if err != nil {
		fmt.Printf("failed to create valkey client: %v\n", err)
		os.Exit(1)
	}
	defer testClient.Close()

	testCoordinator = NewCoordinator(testClient)

	os.Exit(m.Run())
}

func flushDB(t *testing.T) {
	err := testClient.Do(context.Background(), testClient.B().Flushdb().Build()).Error()
	if err != nil {
		t.Fatalf("failed to flush db: %v", err)
	}
}

func TestValkeyCoordinator_AcquireAndRelease(t *testing.T) {
	flushDB(t)
	ctx := context.Background()
	key := "lock:resource-1"
	nodeID := "node-a"
	ttl := 2 * time.Second

	ok, err := testCoordinator.AcquireLock(ctx, key, nodeID, ttl)
	if err != nil || !ok {
		t.Fatalf("expected to acquire lock, got ok=%v, err=%v", ok, err)
	}

	ok, err = testCoordinator.AcquireLock(ctx, key, "node-b", ttl)
	if err != nil || ok {
		t.Errorf("expected node-b to fail, got ok=%v", ok)
	}

	if err := testCoordinator.ReleaseLock(ctx, key, nodeID); err != nil {
		t.Errorf("failed to release lock: %v", err)
	}

	ok, err = testCoordinator.AcquireLock(ctx, key, "node-b", ttl)
	if err != nil || !ok {
		t.Errorf("expected node-b to succeed after release, got ok=%v", ok)
	}
}

func TestValkeyCoordinator_WaitForReady(t *testing.T) {
	flushDB(t)
	key := "status:ready"
	// Use a slightly longer timeout for CI/Container environments
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	started := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(started) // Signal that the goroutine has launched
		if err := testCoordinator.WaitForReady(ctx, key); err != nil {
			errChan <- err
		}
	}()

	<-started
	// Give the coordinator a moment to enter its wait loop
	time.Sleep(200 * time.Millisecond)

	if err := testCoordinator.SignalReady(context.Background(), key); err != nil {
		t.Fatalf("failed to signal: %v", err)
	}

	// Create a channel to wait for the WaitGroup
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errChan:
		t.Errorf("WaitForReady returned error: %v", err)
	case <-done:
		// Success: WaitForReady returned nil and WaitGroup is done
	case <-ctx.Done():
		t.Fatal("timed out waiting for ready signal - ensure implementation handles context and notifications correctly")
	}
}

func TestValkeyCoordinator_TTL(t *testing.T) {
	flushDB(t)
	ctx := context.Background()
	key := "lock:temporary"
	ttl := 500 * time.Millisecond

	ok, _ := testCoordinator.AcquireLock(ctx, key, "node-1", ttl)
	if !ok {
		t.Fatal("failed initial lock")
	}

	time.Sleep(ttl + 200*time.Millisecond)

	ok, _ = testCoordinator.AcquireLock(ctx, key, "node-2", ttl)
	if !ok {
		t.Error("expected lock to be available after TTL expiration")
	}
}
