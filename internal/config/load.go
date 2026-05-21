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
// 2. Environment Variable fallback ($LATENT_CONFIG_PATH)
// 3. System baseline default (/etc/latent/config.yml)
func Load(configPath string) (*Config, error) {
	v := viper.New()

	var activeConfigPath string
	if configPath != "" {
		activeConfigPath = configPath
	} else if envPath := os.Getenv("LATENT_CONFIG_PATH"); envPath != "" {
		activeConfigPath = envPath
	} else {
		activeConfigPath = DefaultConfigPath
	}
	activeConfigPath = filepath.Clean(activeConfigPath)

	if _, err := os.Stat(activeConfigPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("configuration file not found at %q", activeConfigPath)
		}
		return nil, fmt.Errorf("failed to access configuration file %q: %w", activeConfigPath, err)
	}

	v.SetConfigFile(activeConfigPath)
	v.SetEnvPrefix("LATENT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to parse config file %q: %w", activeConfigPath, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed parsing unmarshaled system config: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.host", "localhost")
	v.SetDefault("server.port", "8060")
}
