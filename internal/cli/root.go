package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/kevrocks67/latent/internal/app"
	"github.com/kevrocks67/latent/internal/config"
	"github.com/spf13/cobra"
)

// CfgPath holds any user defined path to the config file. Exported for subcommands
var CfgPath string

// RootCmd is the main entrypoint to the application
var RootCmd = &cobra.Command{
	Use:   "latent",
	Short: "latent orchestrates http/s artifact caching",
	Long:  `A high-performance distriibuted cache engine`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load(CfgPath)
		if err != nil {
			return fmt.Errorf("failed to process config: %w", err)
		}

		ctx, stop := signal.NotifyContext(
			context.Background(),
			os.Interrupt,
			syscall.SIGTERM,
			syscall.SIGINT,
		)
		defer stop()

		application := app.New(cfg)
		if err := application.Run(ctx); err != nil {
			return fmt.Errorf("fatal application crash: %w", err)
		}

		cmd.Println("Latent process gracefully shut down")
		return nil
	},
}

// Execute wraps the Cobra initialization routine for cmd/latent/main.go
func Execute() {
	if err := RootCmd.Execute(); err != nil {
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
