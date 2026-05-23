package app

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	repositorypostgres "github.com/transmogr/transmogr/internal/repository/postgres"
)

func (a *App) initRepository(ctx context.Context) (*pgxpool.Pool, *repositorypostgres.Repository, error) {
	pool, err := pgxpool.New(ctx, a.cfg.DB.Postgres.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}

	repo := repositorypostgres.New(pool, a.cfg.Tables, a.cfg.DB.Postgres.StateSchema)
	a.runners = append(a.runners, runnerFunc(func(ctx context.Context) error {
		return repo.RunExpiredStateLeaseCleanup(ctx, a.cfg.Lease.CleanupInterval)
	}))
	a.closers = append(a.closers, repo)
	return pool, repo, nil
}
