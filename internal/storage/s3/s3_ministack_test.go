//go:build integration

package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testClient *s3.Client
	testBucket = "integration-test-bucket"

	// Singleton state
	containerInstance testcontainers.Container
	setupOnce         sync.Once
)

// getTestClient ensures the container is running and returns the S3 client.
// It uses sync.Once to ensure the container starts only once per test run.
func getTestClient(t *testing.T) *s3.Client {
	setupOnce.Do(func() {
		ctx := context.Background()
		internalPort := "4566/tcp"

		req := testcontainers.ContainerRequest{
			Image:        "ministackorg/ministack:1.3.36",
			ExposedPorts: []string{internalPort},
			WaitingFor: wait.ForHTTP("/_ministack/health").
				WithStartupTimeout(60 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("failed to start ministack: %v", err)
		}

		containerInstance = container

		host, _ := container.Host(ctx)
		port, _ := container.MappedPort(ctx, "4566")
		endpoint := fmt.Sprintf("%s:%s", host, port.Port())

		testClient = s3.New(s3.Options{
			Region:       "us-east-1",
			BaseEndpoint: aws.String(fmt.Sprintf("http://%s", endpoint)),
			UsePathStyle: true,
			Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
			}),
		})

		// Create the global test bucket
		_, err = testClient.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &testBucket})
		if err != nil {
			t.Fatalf("failed to create bucket: %v", err)
		}
	})

	return testClient
}

func TestS3Store_Lifecycle_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getTestClient(t)
	store := NewS3Store(client, testBucket)
	ctx := context.Background()
	key := "test-object.txt"
	content := []byte("hello ministack")

	t.Run("WriteObject", func(t *testing.T) {
		w, err := store.Writer(ctx, key)
		if err != nil {
			t.Fatalf("writer error: %v", err)
		}
		if _, err := w.Write(content); err != nil {
			t.Fatalf("write error: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close error: %v", err)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		exists, err := store.Exists(ctx, key)
		if err != nil {
			t.Fatalf("exists error: %v", err)
		}
		if !exists {
			t.Error("expected object to exist")
		}
	})

	t.Run("ReadObject", func(t *testing.T) {
		r, err := store.Reader(ctx, key)
		if err != nil {
			t.Fatalf("reader error: %v", err)
		}
		defer r.Close()
		data, _ := io.ReadAll(r)
		if !bytes.Equal(data, content) {
			t.Errorf("expected %s, got %s", content, data)
		}
	})

	t.Run("DeleteObject", func(t *testing.T) {
		if err := store.Delete(ctx, key); err != nil {
			t.Fatalf("delete error: %v", err)
		}
		exists, _ := store.Exists(ctx, key)
		if exists {
			t.Error("object should have been deleted")
		}
	})
}

func TestS3Store_Multipart_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getTestClient(t)
	store := NewS3Store(client, testBucket)
	ctx := context.Background()
	key := "large-blob.bin"

	largeData := make([]byte, 7*1024*1024)
	rand.Read(largeData)

	t.Run("UploadLargeFile", func(t *testing.T) {
		w, err := store.Writer(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(w, bytes.NewReader(largeData)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("VerifyLargeFile", func(t *testing.T) {
		r, err := store.Reader(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		downloaded, _ := io.ReadAll(r)
		if !bytes.Equal(downloaded, largeData) {
			t.Error("multipart data integrity check failed")
		}
	})
}
