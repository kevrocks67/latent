//go:build integration

package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3_sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/valkey-io/valkey-go"

	v_coord "github.com/kevrocks67/latent/internal/coordinator/valkey"
	pg_store "github.com/kevrocks67/latent/internal/metadata/postgres"
	"github.com/kevrocks67/latent/internal/storage/s3"
)

func setupIntegration(t *testing.T) (*Orchestrator, func()) {
	ctx := context.Background()

	// 1. Postgres using the official Postgres module
	schemaPath, err := filepath.Abs("../db/schema.sql")
	require.NoError(t, err)

	pgContainer, err := postgres.Run(ctx,
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
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)

	// 2. Valkey
	vkContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "valkey/valkey:8.1-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	require.NoError(t, err)
	vkEndpoint, _ := vkContainer.Endpoint(ctx, "")

	vkClient, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{vkEndpoint}})
	require.NoError(t, err)
	coord := v_coord.NewCoordinator(vkClient)

	// 3. Ministack (S3)
	miniContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ministackorg/ministack:1.3.36",
			ExposedPorts: []string{"4566/tcp"},
			WaitingFor:   wait.ForHTTP("/_ministack/health").WithPort("4566/tcp"),
		},
		Started: true,
	})
	require.NoError(t, err)
	mHost, _ := miniContainer.Host(ctx)
	mPort, _ := miniContainer.MappedPort(ctx, "4566")

	s3Endpoint := fmt.Sprintf("http://%s:%s", mHost, mPort.Port())
	s3Client := s3_sdk.NewFromConfig(aws.Config{
		Region: "us-east-1",
	}, func(o *s3_sdk.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true
		o.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		})
	})

	bucket := "integration-test"
	_, err = s3Client.CreateBucket(ctx, &s3_sdk.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err)

	orchestrator := New(pg_store.NewPostgresStore(db), coord, s3.NewS3Store(s3Client, bucket), nil, 1*time.Hour)

	cleanup := func() {
		coord.Close()
		db.Close()
		pgContainer.Terminate(ctx)
		vkContainer.Terminate(ctx)
		miniContainer.Terminate(ctx)
	}

	return orchestrator, cleanup
}

func TestCoordinatedFetchIntegration(t *testing.T) {
	orchestrator, cleanup := setupIntegration(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var mu sync.Mutex
	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		mu.Unlock()
		// Sleep long enough that other requests hit the coordinator lock/wait logic
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("integration-data"))
	}))
	defer ts.Close()

	const numClients = 5
	var wg sync.WaitGroup
	wg.Add(numClients)

	errChan := make(chan error, numClients)

	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			// Stagger start slightly to ensure one clearly becomes the leader
			time.Sleep(time.Duration(id*10) * time.Millisecond)

			r, err := orchestrator.Fetch(ctx, ts.URL)
			if err != nil {
				errChan <- fmt.Errorf("client %d fetch error: %w", id, err)
				return
			}
			if r == nil {
				errChan <- fmt.Errorf("client %d got nil reader", id)
				return
			}
			defer r.Close()

			body, err := io.ReadAll(r)
			if err != nil {
				errChan <- fmt.Errorf("client %d read error: %w", id, err)
				return
			}
			if string(body) != "integration-data" {
				errChan <- fmt.Errorf("client %d data mismatch: expected 'integration-data', got '%s'", id, string(body))
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent Fetch Failure: %v", err)
	}

	mu.Lock()
	finalCount := callCount
	mu.Unlock()

	assert.Equal(t, 1, finalCount, "Upstream server should only be called once for concurrent identical requests")
}
