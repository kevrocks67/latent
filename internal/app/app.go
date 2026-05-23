package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	gcs_storage "cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
	"github.com/kevrocks67/latent/internal/coordinator"
	transport_http "github.com/kevrocks67/latent/internal/transport/http"
	"github.com/kevrocks67/latent/internal/upstream"
	_ "github.com/lib/pq" // Imported to ensure postgres driver is loaded
	vk "github.com/valkey-io/valkey-go"
	"google.golang.org/api/option"

	"github.com/kevrocks67/latent/internal/config"
	"github.com/kevrocks67/latent/internal/coordinator/valkey"
	"github.com/kevrocks67/latent/internal/metadata"
	"github.com/kevrocks67/latent/internal/metadata/postgres"
	"github.com/kevrocks67/latent/internal/orchestrator"
	"github.com/kevrocks67/latent/internal/storage"
	gcsimpl "github.com/kevrocks67/latent/internal/storage/gcs"
	s3impl "github.com/kevrocks67/latent/internal/storage/s3"
)

// Application orchestrates the lifecycle, dependency injection, and
// initialization for all of latent's underlying resource pools
type Application struct {
	cfg *config.Config
}

// New initializes an Application container with a parsed configuration tree,
// preparing the internal dependency graph for execution.
func New(cfg *config.Config) *Application {
	return &Application{cfg: cfg}
}

// Run uses our config to initiate clients to our dependencies and
// start the main HTTP server
func (a *Application) Run(ctx context.Context) error {
	a.setupGlobalState()

	// Metadata Layer
	metaStore, closeDB, err := a.initMetadataStore(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := closeDB(); err != nil {
			log.Printf("warning: metadata database pool failed to close cleanly: %v", err)
		}
	}()

	// Consensus/Locking Layer
	coord, closeValkey, err := a.initCoordinator()
	if err != nil {
		return err
	}
	defer closeValkey()

	// Storage Layer
	blobStore, closeStorage, err := a.initBlobStore(ctx)
	if err != nil {
		return err
	}
	defer closeStorage()

	fetcher := upstream.NewHTTPFetcher(
		a.cfg.Upstream.Timeout,
		a.cfg.Upstream.MaxConcurrent,
	)

	engine := orchestrator.New(
		metaStore, coord, blobStore,
		fetcher, a.cfg.Orchestrator.DefaultArtifactTTL,
	)

	// Bind & Start Network Transport
	r := gin.Default()
	handler := transport_http.NewHandler(engine)
	handler.RegisterRoutes(r)

	addr := fmt.Sprintf("%s:%d", a.cfg.Server.Host, a.cfg.Server.Port)
	log.Printf("Latent Adapter listening on %s", addr)

	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 3 * time.Second,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("Latent Adapter listening on http://%s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- fmt.Errorf("http router runtime failure: %w", err)
		}
	}()

	// Block execution stream until the terminal signal triggers context cancellation OR server crashes
	select {
	case err := <-serverErrCh:
		return err
	case <-ctx.Done():
		log.Println("SIGTERM/SIGINT trapped. Initiating server transport drain...")

		// Establish a strict grace window for remaining active socket transfers to complete
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("Forced transport shutdown warning: %v", err)
		}
		log.Println("HTTP transport layer fully released.")
	}

	return nil
}

func (a *Application) setupGlobalState() {
	if a.cfg.Logging.Level != "debug" {
		gin.SetMode(gin.ReleaseMode)
	}
}

func (a *Application) initMetadataStore(ctx context.Context) (metadata.MetadataStore, func() error, error) {
	sslMode := "disable"
	if a.cfg.Metadata.Postgres.SSLEnabled {
		sslMode = "require"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		a.cfg.Metadata.Postgres.User,
		a.cfg.Metadata.Postgres.Password,
		a.cfg.Metadata.Postgres.Host,
		a.cfg.Metadata.Postgres.Port,
		a.cfg.Metadata.Postgres.Name,
		sslMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database handle: %w", err)
	}

	db.SetMaxOpenConns(a.cfg.Metadata.Postgres.MaxOpenConns)
	db.SetMaxIdleConns(a.cfg.Metadata.Postgres.MaxIdleConns)
	db.SetConnMaxLifetime(a.cfg.Metadata.Postgres.ConnMaxLifetime)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed to verify metadata storage link: %w", err)
	}

	return postgres.NewPostgresStore(db), db.Close, nil
}

func (a *Application) initCoordinator() (coordinator.Coordinator, func(), error) {
	vkOpts := vk.ClientOption{
		InitAddress: a.cfg.Coordinator.Valkey.Endpoints,
		Username:    a.cfg.Coordinator.Valkey.User,
		Password:    a.cfg.Coordinator.Valkey.Password,
		Dialer: net.Dialer{
			Timeout: a.cfg.Coordinator.Valkey.DialTimeout,
		},
	}

	vkClient, err := vk.NewClient(vkOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compile valkey client drivers: %w", err)
	}

	return valkey.NewCoordinator(vkClient), vkClient.Close, nil
}

func (a *Application) initBlobStore(ctx context.Context) (storage.BlobStore, func(), error) {
	switch a.cfg.Storage.Provider {
	case "s3":
		awsOpts := []func(*awsconfig.LoadOptions) error{
			awsconfig.WithRegion(a.cfg.Storage.S3.Region),
		}

		if a.cfg.Storage.S3.AccessKey != "" && a.cfg.Storage.S3.SecretKey != "" {
			awsOpts = append(awsOpts, awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					a.cfg.Storage.S3.AccessKey,
					a.cfg.Storage.S3.SecretKey,
					"",
				),
			))
		}

		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
		if err != nil {
			return nil, func() {}, fmt.Errorf("failed to authenticate cloud storage client: %w", err)
		}

		s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.UsePathStyle = a.cfg.Storage.S3.ForcePathStyle
			if a.cfg.Storage.S3.Endpoint != "" {
				o.BaseEndpoint = aws.String(a.cfg.Storage.S3.Endpoint)
			}
		})
		return s3impl.NewS3Store(s3Client, a.cfg.Storage.S3.Bucket), func() {}, nil

	case "gcs":
		var gcsOpts []option.ClientOption

		// If a service account json key file is explicitly declared, inject it
		if a.cfg.Storage.GCS.CredentialsFile != "" {
			credBytes, err := os.ReadFile(a.cfg.Storage.GCS.CredentialsFile)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read GCS service account credentials file: %w", err)
			}
			gcsOpts = append(gcsOpts, option.WithAuthCredentialsJSON(option.ServiceAccount, credBytes))
		}

		gcsClient, err := gcs_storage.NewClient(ctx, gcsOpts...)
		if err != nil {
			return nil, nil, err
		}

		closeFn := func() {
			_ = gcsClient.Close()
		}

		return gcsimpl.NewGCSStore(gcsClient, a.cfg.Storage.GCS.Bucket), closeFn, nil

	default:
		return nil, func() {}, fmt.Errorf("invalid storage provider: %s", a.cfg.Storage.Provider)
	}
}
