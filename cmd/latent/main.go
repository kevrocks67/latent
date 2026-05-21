// @title Latent API
// @version 1.0
// @description Distributed artifact cache adapter for high-concurrency environments.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://github.com/kevindiaz/latent

// @host localhost:8060
// @BasePath /api/v1
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	vk "github.com/valkey-io/valkey-go"

	"github.com/kevrocks67/latent/internal/coordinator/valkey"
	"github.com/kevrocks67/latent/internal/metadata/postgres"
	"github.com/kevrocks67/latent/internal/orchestrator"
	s3impl "github.com/kevrocks67/latent/internal/storage/s3"
	"github.com/kevrocks67/latent/internal/transport/http"
	"github.com/kevrocks67/latent/internal/upstream"
)

// @title Latent API
// @version 1.0
// @description Distributed artifact cache adapter.
// @host localhost:8060
// @BasePath /api/v1
func main() {
	ctx := context.Background()

	// 1. Metadata Store (Postgres)
	// Implementation: internal/metadata/postgres/postgres.go -> NewPostgresStore
	db, err := sql.Open("postgres", getEnv("DB_URL", "postgres://user:pass@localhost:5432/latent?sslmode=disable"))
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	metaStore := postgres.NewPostgresStore(db)

	// 2. Coordinator (Valkey)
	// Implementation: internal/coordinator/valkey/valkey.go -> NewCoordinator
	vkClient, err := vk.NewClient(vk.ClientOption{
		InitAddress: []string{getEnv("VALKEY_ADDR", "localhost:6379")},
	})
	if err != nil {
		log.Fatalf("failed to connect to valkey: %v", err)
	}
	coord := valkey.NewCoordinator(vkClient)

	// 3. Blob Store (S3)
	// Implementation: internal/storage/s3/s3.go -> NewS3Store
	s3Bucket := getEnv("S3_BUCKET", "artifacts")
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(getEnv("AWS_REGION", "us-east-1")),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			getEnv("AWS_ACCESS_KEY", ""),
			getEnv("AWS_SECRET_KEY", ""),
			"",
		)),
	)
	if err != nil {
		log.Fatalf("failed to load aws config: %v", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://latent-storage:4566")
	})
	blobStore := s3impl.NewS3Store(s3Client, s3Bucket)

	// 4. Upstream Fetcher (HTTP)
	// Implementation: internal/upstream/fetcher.go -> NewHTTPFetcher
	timeout := getDurationEnv("FETCH_TIMEOUT", 30*time.Second)
	maxConcurrent := getInt64Env("MAX_CONCURRENT_FETCHES", 50)
	fetcher := upstream.NewHTTPFetcher(timeout, maxConcurrent)

	// 5. Orchestrator
	// Ties all interfaces together
	defaultTTL := getDurationEnv("DEFAULT_CACHE_TTL", 24*time.Hour)
	orch := orchestrator.New(
		metaStore,
		coord,
		blobStore,
		fetcher,
		defaultTTL,
	)

	// 6. Transport
	r := gin.Default()
	handler := http.NewHandler(orch)
	handler.RegisterRoutes(r)

	log.Printf("Latent Adapter listening on :8060")
	r.Run(":8060")
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return fallback
	}
	return d
}

func getInt64Env(key string, fallback int64) int64 {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return fallback
	}
	return i
}

func pingWithRetry(db *sql.DB) error {
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := db.PingContext(ctx)
		cancel()
		if err == nil {
			return nil
		}
		log.Printf("Retrying database connection... (%d/5)", i+1)
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("could not connect to postgres")
}
