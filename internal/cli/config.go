package cli

import (
	"fmt"

	"github.com/goccy/go-yaml"
	"github.com/kevrocks67/latent/internal/config"
	"github.com/spf13/cobra"
)

// ConfigCmd allows you to validate and parse your config
var ConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage and inspect the latent configuration profile",
}

var validateCmd = &cobra.Command{
	Use:   "validate [path]",
	Short: "Validate a configuration file syntax and requirements",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var targetPath string
		if len(args) > 0 {
			targetPath = args[0]
		}

		// config.Load handles empty strings by running its default path lookups
		cfg, err := config.Load(targetPath)
		if err != nil {
			return fmt.Errorf("configuration validation failed: %w", err)
		}

		cmd.Printf("Configuration is valid\n")
		cmd.Printf("   Active Storage Provider: %s\n", cfg.Storage.Provider)

		return nil
	},
}

var explainCmd = &cobra.Command{
	Use:   "explain [path]",
	Short: "Print the fully parsed configuration state merging defaults, files, and env variables",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var targetPath string
		if len(args) > 0 {
			targetPath = args[0]
		}

		cfg, err := config.Load(targetPath)
		if err != nil {
			return fmt.Errorf("cannot compile state for explanation: %w", err)
		}

		cmd.Println("Active Configuration Blueprint:")

		encoder := yaml.NewEncoder(cmd.OutOrStdout())
		defer func() {
			if cerr := encoder.Close(); cerr != nil {
				cmd.Printf("warning: failed to close yaml encoder cleanly: %v\n", cerr)
			}
		}()

		if err := encoder.Encode(cfg); err != nil {
			return fmt.Errorf("failed to format configuration YAML: %w", err)
		}

		return nil
	},
}

func init() {
	ConfigCmd.AddCommand(validateCmd)
	ConfigCmd.AddCommand(explainCmd)
	RootCmd.AddCommand(ConfigCmd)
}
