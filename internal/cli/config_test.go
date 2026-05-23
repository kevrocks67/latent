package cli

import (
	"bytes"
	_ "embed" // Hook the embed compiler driver
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
)

// setEnvHelper sets an environment variable and fails the test if it hits an error, satisfying errcheck
func setEnvHelper(t *testing.T, key, value string) {
	t.Helper()
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("failed to set env var %s: %v", key, err)
	}
}

//go:embed testdata/valid_config.yml
var validConfigFixture []byte

// TestConfigValidateSubcommand verifies the behavior of the validation engine
func TestConfigValidateSubcommand(t *testing.T) {
	t.Run("successfully parses and validates an explicit file target payload", func(t *testing.T) {
		os.Clearenv()

		// Keep the file isolated in a safe, auto-cleaned directory
		tmpDir := t.TempDir()
		mockConfigPath := filepath.Join(tmpDir, "test_config.yml")

		// Write the clean, embedded fixture asset directly
		if err := os.WriteFile(mockConfigPath, validConfigFixture, 0600); err != nil {
			t.Fatalf("failed to stage temporary test asset: %v", err)
		}

		buf := new(bytes.Buffer)
		RootCmd.SetOut(buf)
		RootCmd.SetErr(buf)
		RootCmd.SetArgs([]string{"config", "validate", mockConfigPath})

		if err := RootCmd.Execute(); err != nil {
			t.Fatalf("failed to validate valid configuration file: %v", err)
		}
	})

	// Test case: Successful inline validation using mocked environment hooks
	t.Run("succeeds when valid configuration environment variables are fully present", func(t *testing.T) {
		os.Clearenv()

		setEnvHelper(t, "LATENT_SERVER_HOST", "127.0.0.1")
		setEnvHelper(t, "LATENT_SERVER_PORT", "9000")
		setEnvHelper(t, "LATENT_STORAGE_PROVIDER", "s3")
		setEnvHelper(t, "LATENT_STORAGE_S3_BUCKET", "test-bucket")
		setEnvHelper(t, "LATENT_STORAGE_S3_REGION", "us-west-2")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_HOST", "localhost")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_PORT", "5432")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_NAME", "latent_test")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_USER", "postgres")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_PASSWORD", "secret")
		setEnvHelper(t, "LATENT_COORDINATOR_VALKEY_ENDPOINTS", "localhost:6379")

		buf := new(bytes.Buffer)
		RootCmd.SetOut(buf)
		RootCmd.SetErr(buf)
		RootCmd.SetArgs([]string{"config", "validate"})

		if err := RootCmd.Execute(); err != nil {
			t.Fatalf("validation unexpectedly failed with filled environment variables: %v", err)
		}
	})

	// Test case: Reading and passing an explicit local file target path
	t.Run("successfully parses and validates an explicit file target payload", func(t *testing.T) {
		os.Clearenv()

		tmpDir := t.TempDir()
		mockConfigPath := filepath.Join(tmpDir, "test_config.yml")

		yamlPayload := []byte(`
server:
  host: "0.0.0.0"
  port: 8060
storage:
  provider: "s3"
  s3:
    bucket: "local-artifacts"
    region: "us-east-1"
metadata:
  postgres:
    host: "localhost"
    port: 5432
    name: "latent"
    user: "user"
    password: "password"
coordinator:
  valkey:
    endpoints: ["localhost:6379"]
`)

		if err := os.WriteFile(mockConfigPath, yamlPayload, 0600); err != nil {
			t.Fatalf("failed to stage temporary test asset: %v", err)
		}

		buf := new(bytes.Buffer)
		RootCmd.SetOut(buf)
		RootCmd.SetErr(buf)
		RootCmd.SetArgs([]string{"config", "validate", mockConfigPath})

		if err := RootCmd.Execute(); err != nil {
			t.Fatalf("failed to validate valid configuration file: %v", err)
		}
	})
}

// TestConfigExplainSubcommand verifies serialization output behavior
func TestConfigExplainSubcommand(t *testing.T) {
	t.Run("serializes compiled memory state to structural output successfully", func(t *testing.T) {
		os.Clearenv()

		setEnvHelper(t, "LATENT_SERVER_HOST", "127.0.0.1")
		setEnvHelper(t, "LATENT_SERVER_PORT", "8060")
		setEnvHelper(t, "LATENT_STORAGE_PROVIDER", "s3")
		setEnvHelper(t, "LATENT_STORAGE_S3_BUCKET", "explain-test-bucket")
		setEnvHelper(t, "LATENT_STORAGE_S3_REGION", "us-east-1")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_HOST", "localhost")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_PORT", "5432")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_NAME", "latent")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_USER", "user")
		setEnvHelper(t, "LATENT_METADATA_POSTGRES_PASSWORD", "pass")
		setEnvHelper(t, "LATENT_COORDINATOR_VALKEY_ENDPOINTS", "localhost:6379")

		buf := new(bytes.Buffer)
		RootCmd.SetOut(buf)
		RootCmd.SetErr(buf)
		RootCmd.SetArgs([]string{"config", "explain"})

		if err := RootCmd.Execute(); err != nil {
			t.Fatalf("explain command runtime error: %v", err)
		}

		fullOutput := buf.String()

		// Verify the header is present
		if !strings.Contains(fullOutput, "Active Configuration Blueprint:") {
			t.Fatalf("expected header block missing from output stream. Got:\n%s", fullOutput)
		}

		// Cleanly remove the header line and any potential leading whitespace
		// This leaves us with pure, unadulterated YAML text
		yamlPart := fullOutput
		if idx := strings.Index(fullOutput, "server:"); idx != -1 {
			yamlPart = fullOutput[idx:]
		} else {
			t.Fatalf("could not isolate YAML payload body. Raw output was:\n%s", fullOutput)
		}

		// Unmarshal into a generic map to guarantee struct tag issues aren't masking the problem
		var genericMap map[string]any
		if err := yaml.Unmarshal([]byte(yamlPart), &genericMap); err != nil {
			t.Fatalf("failed to unmarshal isolated YAML payload: %v\nIsolated snippet was:\n%s", err, yamlPart)
		}

		// Navigate and assert on values logically
		server, ok := genericMap["server"].(map[string]any)
		if !ok {
			t.Fatalf("missing or invalid 'server' block in YAML object map. Parsed tree: %v", genericMap)
		}

		if server["host"] != "127.0.0.1" {
			t.Errorf("expected server.host to be '127.0.0.1', got '%v'", server["host"])
		}

		storage, ok := genericMap["storage"].(map[string]any)
		if !ok {
			t.Fatal("missing 'storage' block in YAML object map")
		}
		s3, ok := storage["s3"].(map[string]interface{})
		if !ok {
			t.Fatal("missing 'storage.s3' block in YAML object map")
		}
		if s3["bucket"] != "explain-test-bucket" {
			t.Errorf("expected storage.s3.bucket to be 'explain-test-bucket', got '%v'", s3["bucket"])
		}
	})
}
