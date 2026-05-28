package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/kevrocks67/latent/internal/app"
	"github.com/kevrocks67/latent/internal/config"
	"github.com/kevrocks67/latent/internal/logger"
	"github.com/spf13/cobra"
)

// CfgPath holds any user defined path to the config file. Exported for subcommands
var (
	CfgPath string
	cfg     *config.Config
)

// RootCmd is the main entrypoint to the application
var RootCmd = &cobra.Command{
	Use:           "latent",
	Short:         "latent orchestrates http/s artifact caching",
	Long:          `A high-performance distriibuted cache engine`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Walk up the command tree to see if any ancestor has declared that the
		// root global configuration should NOT be loaded. Commands like `config`
		// perform their own config.Load and must not trigger the global loader.
		for c := cmd; c != nil; c = c.Parent() {
			if v, ok := c.Annotations["load_config"]; ok && (v == "false" || v == "no") {
				return nil
			}
		}

		var err error
		cfg, err = config.Load(CfgPath)
		if err != nil {
			return fmt.Errorf("failed to process config: %w", err)
		}

		logger.Init(cfg.Logging.Level)

		slog.Info("telemetry stream initialized",
			"level", cfg.Logging.Level,
			"storage", cfg.Storage.Provider,
		)
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(
			context.Background(),
			os.Interrupt,
			syscall.SIGTERM,
			syscall.SIGINT,
		)
		defer stop()

		application := app.New(cfg)
		if err := application.Run(ctx); err != nil {
			slog.Error("fatal application crash", "err", err)
			return err
		}

		cmd.Println("Latent process gracefully shut down")
		return nil
	},
}

// Execute wraps the Cobra initialization routine for cmd/latent/main.go
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		if !logger.IsInitialized() {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}
}

func init() {
	RootCmd.Flags().StringVarP(
		&CfgPath,
		"config",
		"c",
		"",
		"Explicit path to configuration file (falls back to LATENT_CONFIG_PATH)",
	)
}
