// Package main builds the database initialization CLI.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/transmogr/transmogr/internal/app"
	"github.com/transmogr/transmogr/pkg/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("transmogr-init exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", app.ConfigPathFromEnv(), "path to YAML config")
	stateSchema := flag.String("schema", "", "override state schema for migrations")
	flag.Parse()

	cfg, err := app.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if *stateSchema != "" {
		cfg.DB.Postgres.StateSchema = *stateSchema
	}

	logger, err := app.NewLogger(cfg)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	slog.SetDefault(logger)

	pool, err := pgxpool.New(context.Background(), cfg.DB.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	if err := migrations.ApplyUp(context.Background(), pool, cfg.DB.Postgres.StateSchema); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	slog.Info("migrations applied", "state_schema", cfg.DB.Postgres.StateSchema)

	return nil
}
