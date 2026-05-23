package app

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// NewLogger creates the application logger from config.
func NewLogger(cfg Config) (*slog.Logger, error) {
	level, err := parseLogLevel(cfg.LogLevel)
	if err != nil {
		return nil, err
	}

	options := &slog.HandlerOptions{Level: level}
	switch strings.ToLower(strings.TrimSpace(cfg.LogFormat)) {
	case "", "text":
		return slog.New(slog.NewTextHandler(os.Stdout, options)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, options)), nil
	default:
		return nil, fmt.Errorf("unsupported log format %q", cfg.LogFormat)
	}
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unsupported log level %q", value)
	}
}
