package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/transmogr/transmogr/internal/models"
)

// WithTx runs repository operations inside a PostgreSQL transaction.
func (r *Repository) WithTx(ctx context.Context, fn func(ctx context.Context, repo *Repository) error) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	txRepo := &Repository{
		pool:        r.pool,
		tableByName: r.tableByName,
		stateSchema: r.stateSchema,
		tx:          tx,
		pkTypes:     make(map[string]map[string]models.PrimaryKeyType, len(r.tableByName)),
	}
	if err := fn(ctx, txRepo); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return fmt.Errorf("rollback tx: %w", errors.Join(err, rollbackErr))
		}

		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}
