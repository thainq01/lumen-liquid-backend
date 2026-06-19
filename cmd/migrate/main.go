package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/lumenliquid/backend/internal/config"
	"github.com/lumenliquid/backend/internal/log"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	logger := log.Init(cfg.LogLevel)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	m, err := migrate.New("file://migrations", cfg.DatabaseURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("migrate.New")
	}
	defer m.Close()

	command := os.Args[1]

	switch command {
	case "up":
		logger.Info().Msg("running migrations up")
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			logger.Fatal().Err(err).Msg("migrate up")
		}
		logger.Info().Msg("migrations complete")

	case "down":
		logger.Info().Msg("rolling back one migration")
		if err := m.Steps(-1); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			logger.Fatal().Err(err).Msg("migrate down")
		}
		logger.Info().Msg("rollback complete")

	case "force":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: migrate force <version>")
			os.Exit(1)
		}
		version, err := strconv.Atoi(os.Args[2])
		if err != nil {
			logger.Fatal().Err(err).Msg("invalid version")
		}
		logger.Info().Int("version", version).Msg("forcing version")
		if err := m.Force(version); err != nil {
			logger.Fatal().Err(err).Msg("migrate force")
		}
		logger.Info().Msg("version forced")

	case "version":
		version, dirty, err := m.Version()
		if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
			logger.Fatal().Err(err).Msg("get version")
		}
		if errors.Is(err, migrate.ErrNilVersion) {
			fmt.Println("No migrations applied yet")
		} else {
			fmt.Printf("Current version: %d (dirty: %v)\n", version, dirty)
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: migrate <command>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  up           Run all pending migrations")
	fmt.Println("  down         Rollback one migration")
	fmt.Println("  force <ver>  Force set migration version (use when dirty)")
	fmt.Println("  version      Show current migration version")
}
