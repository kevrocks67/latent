package config

import (
	"errors"
	"fmt"
	"strings"
)

// validateConfig acts as an immediate safety guardrail for mandatory configuration states
func validateConfig(cfg *Config) error {
	if cfg.Storage.Provider == "" {
		return errors.New("invalid configuration: storage.provider must be set ('s3' or 'gcs')")
	}

	switch strings.ToLower(cfg.Storage.Provider) {
	case "gcs":
		if cfg.Storage.GCS.Bucket == "" {
			return errors.New("invalid configuration: storage.gcs.bucket cannot be empty when provider is 'gcs'")
		}
	case "s3":
		if cfg.Storage.S3.Bucket == "" {
			return errors.New("invalid configuration: storage.s3.bucket cannot be empty when provider is 's3'")
		}
	default:
		return fmt.Errorf("invalid configuration: unsupported storage provider %q", cfg.Storage.Provider)
	}

	if cfg.Metadata.Postgres.Host == "" || cfg.Metadata.Postgres.Name == "" {
		return errors.New("invalid configuration: metadata.postgres target configurations cannot be empty")
	}

	if len(cfg.Coordinator.Valkey.Endpoints) == 0 || cfg.Coordinator.Valkey.Endpoints[0] == "" {
		return errors.New("invalid configuration: coordinator.valkey.endpoints requires at least one address target")
	}

	return nil
}
