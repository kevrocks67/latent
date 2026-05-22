package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

const (
	// DefaultConfigPath is the primary location that latent looks for the config
	DefaultConfigPath = "/etc/latent/config.yml"
)

// Load reads configuration utilizing a strict override hierarchy:
// 1. Explicit CLI Flag path
// 2. System baseline default (/etc/latent/config.yml)
// 3. Environment Variable Path fallback ($LATENT_CONFIG_PATH)
// 4. Environment Variable based overrides / pure config (absolute priority for values)
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Configure environment matching rules
	v.SetEnvPrefix("LATENT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// Seed defaults. This establishes safe fallbacks AND informs Viper
	// of structural schema paths so environment-only runs work natively
	setDefaults(v)

	var activeConfigPath string
	if configPath != "" {
		activeConfigPath = filepath.Clean(configPath)
	} else if envPath := os.Getenv("LATENT_CONFIG_PATH"); envPath != "" {
		activeConfigPath = filepath.Clean(envPath)
	} else {
		// Fall back to system default path if it exists on the host filesystem
		defaultPath := filepath.Clean(DefaultConfigPath)
		if _, err := os.Stat(defaultPath); err == nil {
			activeConfigPath = defaultPath
		}
	}

	// Read file only if one was explicitly requested or found natively
	if activeConfigPath != "" {
		if _, err := os.Stat(activeConfigPath); err != nil {
			// If the user explicitly requested a file via flag or env var, a missing file is a hard crash
			if os.IsNotExist(err) && (configPath != "" || os.Getenv("LATENT_CONFIG_PATH") != "") {
				return nil, fmt.Errorf("configuration file not found at %q", activeConfigPath)
			}
			// Surface any other permission or formatting issues regardless of how the file was found
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to access configuration file %q: %w", activeConfigPath, err)
			}
		} else {
			v.SetConfigFile(activeConfigPath)
			if err := v.ReadInConfig(); err != nil {
				return nil, fmt.Errorf("failed to parse config file %q: %w", activeConfigPath, err)
			}
		}
	}

	// Unmarshal file and env based config. Environment variables should take
	// precedence
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed parsing unmarshaled system config: %w", err)
	}

	// Validate core requirements to eliminate silent bootstrap failures
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
