// Package config defines the system configuration schema and handles
// parsing environment variables and YAML files into the core runtime settings.
package config

import "time"

// Config holds all application configuration
type Config struct {
	Server      ServerConfig      `mapstructure:"server"`
	Storage     StorageConfig     `mapstructure:"storage"`
	Metadata    MetadataConfig    `mapstructure:"metadata"`
	Coordinator CoordinatorConfig `mapstructure:"coordinator"`
	Tracing     TracingConfig     `mapstructure:"tracing"`
	Logging     LoggingConfig     `mapstructure:"logging"`
}

// ServerConfig configures the primary HTTP API listener
type ServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

// StorageConfig configures the blob storage provider (e.g., S3, GCS)
// used for caching and distributing immutable artifacts
type StorageConfig struct {
	Provider string    `mapstructure:"provider"`
	S3       S3Config  `mapstructure:"s3"`
	GCS      GCSConfig `mapstructure:"gcs"`
}

// S3Config holds parameters for the AWS S3 or S3-compatible blob storage provider
type S3Config struct {
	Bucket         string `mapstructure:"bucket"`
	Region         string `mapstructure:"region"`
	Endpoint       string `mapstructure:"endpoint"`
	ForcePathStyle bool   `mapstructure:"force_path_style"`
	AccessKey      string `mapstructure:"access_key"`
	SecretKey      string `mapstructure:"secret_key"`
}

// GCSConfig holds parameters for the Google Cloud Storage provider.
type GCSConfig struct {
	Bucket          string `mapstructure:"bucket"`
	CredentialsFile string `mapstructure:"credentials_file"`
}

// MetadataConfig configures the relational database that
// keeps track of the artifacts and their state
type MetadataConfig struct {
	Postgres PostgresConfig `mapstructure:"postgres"`
}

// PostgresConfig holds the connection parameters required to
// initialize the database connection pool.
type PostgresConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	Name            string        `mapstructure:"name"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	SSLEnabled      bool          `mapstructure:"ssl_enabled"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

// CoordinatorConfig configures the Valkey instance which handles distributed
// locking and consensus
type CoordinatorConfig struct {
	Valkey ValkeyConfig `mapstructure:"valkey"`
}

// ValkeyConfig holds the connection parameters required to initialize
// the valkey client
type ValkeyConfig struct {
	Endpoints       []string      `mapstructure:"endpoints"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	ClusterMode     bool          `mapstructure:"cluster_mode"`
	SSLEnabled      bool          `mapstructure:"ssl_enabled"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	MaxActiveConns  int           `mapstructure:"max_active_conns"`
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
	DialTimeout     time.Duration `mapstructure:"dial_timeout"`
}

// MetricsConfig manages the internal telemetry server exposing
// Prometheus performance and runtime primitives
type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Address string `mapstructure:"address"`
	Path    string `mapstructure:"path"`
}

// TracingConfig manages OpenTelemetry (OTEL) exporter endpoints and sampling
// rates for distributed request tracing
type TracingConfig struct {
	Enabled        bool    `mapstructure:"enabled"`
	OtelEndpoint   string  `mapstructure:"otel_endpoint"`
	SampleFraction float64 `mapstructure:"sample_fraction"`
}

// LoggingConfig defines the log emission formatting and verbosity thresholds
// across internal subsystems
type LoggingConfig struct {
	Level string `mapstructure:"level"`
}
