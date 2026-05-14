//go:build integration

package gcs

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

var (
	gcsClient     *storage.Client
	gcsBucketName = "gcs-integration-test-bucket"
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start GCS Emulator once for all tests in this package
	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server:1.54.0",
		ExposedPorts: []string{"4443/tcp"},
		Cmd: []string{
			"-scheme", "http",
			"-public-host", "localhost",
		},
		WaitingFor: wait.ForHTTP("/storage/v1/b").WithPort("4443/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Printf("failed to start GCS container: %v\n", err)
		os.Exit(1)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4443")
	endpoint := fmt.Sprintf("http://%s:%s/storage/v1/", host, port.Port())

	gcsClient, err = storage.NewClient(ctx,
		option.WithEndpoint(endpoint),
		option.WithHTTPClient(http.DefaultClient),
		option.WithoutAuthentication(),
	)
	if err != nil {
		fmt.Printf("failed to create GCS client: %v\n", err)
		container.Terminate(ctx)
		os.Exit(1)
	}

	// Create the test bucket
	if err := gcsClient.Bucket(gcsBucketName).Create(ctx, "test-project", nil); err != nil {
		fmt.Printf("failed to create GCS bucket: %v\n", err)
		gcsClient.Close()
		container.Terminate(ctx)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	gcsClient.Close()
	container.Terminate(ctx)

	os.Exit(code)
}

func TestGCSStore_BasicCRUD(t *testing.T) {
	ctx := context.Background()
	store := NewGCSStore(gcsClient, gcsBucketName)
	key := "test/basic.txt"
	data := []byte("hello gcs")

	t.Run("WriteAndRead", func(t *testing.T) {
		w, err := store.Writer(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		w.Close()

		r, err := store.Reader(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, _ := io.ReadAll(r)
		if !bytes.Equal(got, data) {
			t.Errorf("got %s, want %s", got, data)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		ok, err := store.Exists(ctx, key)
		if err != nil || !ok {
			t.Errorf("expected file to exist")
		}
		ok, _ = store.Exists(ctx, "non-existent")
		if ok {
			t.Errorf("expected file to not exist")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		if err := store.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		ok, _ := store.Exists(ctx, key)
		if ok {
			t.Error("file should be gone")
		}
	})
}

func TestGCSStore_LargeFile(t *testing.T) {
	ctx := context.Background()
	store := NewGCSStore(gcsClient, gcsBucketName)
	key := "test/large-file.bin"

	// 5MB file
	size := 5 * 1024 * 1024
	largeData := make([]byte, size)
	if _, err := rand.Read(largeData); err != nil {
		t.Fatal(err)
	}

	w, err := store.Writer(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// Write in chunks
	chunkSize := 1024 * 1024
	for i := 0; i < len(largeData); i += chunkSize {
		end := i + chunkSize
		if end > len(largeData) {
			end = len(largeData)
		}
		if _, err := w.Write(largeData[i:end]); err != nil {
			t.Fatalf("failed writing chunk at %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	r, err := store.Reader(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	downloaded, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if len(downloaded) != size {
		t.Errorf("size mismatch: got %d, want %d", len(downloaded), size)
	}

	if !bytes.Equal(downloaded, largeData) {
		t.Error("content corruption in large file")
	}
}

func TestGCSStore_List(t *testing.T) {
	ctx := context.Background()
	store := NewGCSStore(gcsClient, gcsBucketName)
	prefix := "logs-list/"
	files := []string{prefix + "1.txt", prefix + "2.txt", "other/3.txt"}

	for _, f := range files {
		w, _ := store.Writer(ctx, f)
		w.Write([]byte("data"))
		w.Close()
	}

	items, err := store.ListObjects(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items for prefix %s, got %v", prefix, items)
	}
}
