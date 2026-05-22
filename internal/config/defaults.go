package config

import (
	"time"

	"github.com/spf13/viper"
)

// setDefaults registers fallback constraints across all internal schemas
func setDefaults(v *viper.Viper) {
	v.SetDefault("server.host", "localhost")
	v.SetDefault("server.port", 8060)
	v.SetDefault("upstream.timeout", 30*time.Second)
	v.SetDefault("upstream.max_concurrent", int64(100))
	v.SetDefault("orchestrator.default_artifact_ttl", 24*time.Hour)
	v.SetDefault("storage.provider", "")
	v.SetDefault("storage.s3.bucket", "")
	v.SetDefault("storage.s3.region", "us-east-1")
	v.SetDefault("storage.s3.endpoint", "")
	v.SetDefault("storage.s3.force_path_style", false)
	v.SetDefault("storage.s3.access_key", "")
	v.SetDefault("storage.s3.secret_key", "")
	v.SetDefault("storage.gcs.bucket", "")
	v.SetDefault("storage.gcs.credentials_file", "")
	v.SetDefault("metadata.postgres.host", "localhost")
	v.SetDefault("metadata.postgres.port", 5432)
	v.SetDefault("metadata.postgres.name", "latent")
	v.SetDefault("metadata.postgres.user", "postgres")
	v.SetDefault("metadata.postgres.password", "")
	v.SetDefault("metadata.postgres.ssl_enabled", true)
	v.SetDefault("metadata.postgres.max_open_conns", 25)
	v.SetDefault("metadata.postgres.max_idle_conns", 25)
	v.SetDefault("metadata.postgres.conn_max_lifetime", 5*time.Minute)
	v.SetDefault("coordinator.valkey.endpoints", []string{"localhost:6379"})
	v.SetDefault("coordinator.valkey.user", "")
	v.SetDefault("coordinator.valkey.password", "")
	v.SetDefault("coordinator.valkey.cluster_mode", false)
	v.SetDefault("coordinator.valkey.ssl_enabled", false)
	v.SetDefault("coordinator.valkey.max_idle_conns", 10)
	v.SetDefault("coordinator.valkey.max_active_conns", 50)
	v.SetDefault("coordinator.valkey.conn_max_idle_time", 5*time.Minute)
	v.SetDefault("coordinator.valkey.dial_timeout", 5*time.Second)
	v.SetDefault("tracing.enabled", false)
	v.SetDefault("tracing.otel_endpoint", "")
	v.SetDefault("tracing.sample_fraction", 0.1)
	v.SetDefault("logging.level", "info")
}
