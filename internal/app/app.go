// Package app wires the top-level application components.
package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	cryptoservice "github.com/transmogr/transmogr/internal/service/crypto"
)

// Runner is the interface for components that can be run.
type Runner interface {
	Run(context.Context) error
}

// runnerFunc adapts a plain function to the Runner interface.
type runnerFunc func(context.Context) error

func (f runnerFunc) Run(ctx context.Context) error { return f(ctx) }

// App wires the top-level Transmogr services and transports.
type App struct {
	cfg     Config
	runners []Runner
	closers []io.Closer
	reg     *prometheus.Registry
}

// New constructs the application from validated configuration.
func New(ctx context.Context, cfg Config) (*App, error) {
	instanceID := newInstanceID()
	a := &App{cfg: cfg}
	success := false
	defer func() {
		if success {
			return
		}
		if closeErr := a.closeAll(); closeErr != nil {
			slog.Error("cleanup failed during initialization", "error", closeErr)
		}
	}()

	pool, repo, err := a.initRepository(ctx)
	if err != nil {
		return nil, err
	}

	km, err := a.initCrypto(ctx)
	if err != nil {
		return nil, err
	}

	cryptoSvc, err := cryptoservice.NewService(km, cfg.Transformers)
	if err != nil {
		return nil, err
	}

	ob, repSvc, peerMgr, err := a.initReplication(ctx, instanceID, pool, repo)
	if err != nil {
		return nil, err
	}

	if err := a.initTransport(instanceID, km, cryptoSvc, repSvc, peerMgr, ob, repo); err != nil {
		return nil, err
	}

	success = true
	return a, nil
}

// Run starts the application and blocks until shutdown or an unrecoverable error.
func (a *App) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(a.runners))
	var wg sync.WaitGroup

	for _, r := range a.runners {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- r.Run(runCtx)
		}()
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()

	slog.Info("application started")

	select {
	case <-ctx.Done():
		slog.Info("application shutdown requested")
		closeErr := a.closeAll()
		return errors.Join(a.collectRunnerErrors(errCh), closeErr)
	case err := <-errCh:
		if err == nil || errors.Is(err, context.Canceled) {
			slog.Info("application runner stopped")
			cancel()
			closeErr := a.closeAll()
			return errors.Join(a.collectRunnerErrors(errCh), closeErr)
		}

		slog.Error("application runner failed", "error", err)
		cancel()
		closeErr := a.closeAll()
		return errors.Join(err, a.collectRunnerErrors(errCh), closeErr)
	}
}

func (a *App) collectRunnerErrors(errCh <-chan error) error {
	var errs []error
	for err := range errCh {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (a *App) closeAll() error {
	var errs []error
	for i := len(a.closers) - 1; i >= 0; i-- {
		errs = append(errs, a.closers[i].Close())
	}
	return errors.Join(errs...)
}

func newInstanceID() string {
	return uuid.NewString()
}
