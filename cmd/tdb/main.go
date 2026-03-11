package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/minee/tdb/internal/app"
	"github.com/minee/tdb/internal/config"
	"github.com/minee/tdb/internal/db"
	"github.com/spf13/cobra"
)

// Build-time variables set by goreleaser via ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	rootCmd := &cobra.Command{
		Use:     "tdb",
		Short:   "TUI Database Client",
		Long:    "A terminal-based database client with an intuitive TUI interface, inspired by TablePlus.",
		Version: formatVersion(version, commit, date),
		RunE:    run,
	}

	rootCmd.Flags().String("dsn", "", "Database connection string (DSN)")
	rootCmd.Flags().String("type", "postgres", "Database type (postgres, mysql, sqlite, mssql)")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	dsn, _ := cmd.Flags().GetString("dsn")
	dbType, _ := cmd.Flags().GetString("type")

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if dsn == "" {
		// No DSN provided; launch connection manager TUI
		return app.StartConnectionScreen(cfg)
	}

	// DSN provided; connect and launch TUI
	driverType := db.DriverType(dbType)
	driver, err := db.NewDriver(driverType)
	if err != nil {
		return fmt.Errorf("failed to create driver: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Printf("Connecting to %s database...\n", dbType)
	if err := driver.Connect(ctx, dsn); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer driver.Close()

	// Launch TUI
	a := app.New(driver, cfg, dsn)
	return a.Start()
}

// formatVersion returns a human-readable version string.
func formatVersion(v, c, d string) string {
	if c == "none" && d == "unknown" {
		return v
	}
	short := c
	if len(short) > 7 {
		short = short[:7]
	}
	return fmt.Sprintf("%s (commit: %s, built: %s)", v, short, d)
}
