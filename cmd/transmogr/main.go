// Package main builds the Transmogr service CLI.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/transmogr/transmogr/internal/app"
)

func main() {
	if err := run(); err != nil {
		slog.Error("transmogr exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if addr := os.Getenv("PPROF_ADDR"); addr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

			server := &http.Server{
				Addr:              addr,
				Handler:           mux,
				ReadHeaderTimeout: 5 * time.Second,
			}

			slog.Info("pprof server started", "addr", addr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("pprof server stopped", "error", err)
			}
		}()
	}

	configPath := app.ConfigPathFromEnv()
	cfg, err := app.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger, err := app.NewLogger(cfg)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	slog.SetDefault(logger)

	application, err := app.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create app: %w", err)
	}

	if err := application.Run(ctx); err != nil {
		return fmt.Errorf("run app: %w", err)
	}

	return nil
}
