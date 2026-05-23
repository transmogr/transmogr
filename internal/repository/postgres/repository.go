package postgres

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/transmogr/transmogr/internal/models"
)

// Repository exposes application and state operations through one PostgreSQL-backed object.
type Repository struct {
	pool        *pgxpool.Pool
	tableByName map[string]models.TableSpec
	stateSchema string
	tx          pgx.Tx
	pkTypesMu   sync.RWMutex
	pkTypes     map[string]map[string]models.PrimaryKeyType
}

// New creates a unified PostgreSQL repository backed by an existing pool.
func New(pool *pgxpool.Pool, tables []models.TableSpec, stateSchema string) *Repository {
	tableByName := make(map[string]models.TableSpec, len(tables))
	for _, table := range tables {
		tableByName[table.Name] = table
	}

	return &Repository{
		pool:        pool,
		tableByName: tableByName,
		stateSchema: stateSchema,
		pkTypes:     make(map[string]map[string]models.PrimaryKeyType, len(tables)),
	}
}

// Close releases the underlying PostgreSQL resources.
func (r *Repository) Close() error {
	if r == nil || r.pool == nil {
		return nil
	}

	r.pool.Close()
	return nil
}

// RawPoolForTests exposes the shared PostgreSQL pool to tests.
func (r *Repository) RawPoolForTests() *pgxpool.Pool {
	return r.pool
}

func (r *Repository) stateTable(name string) string {
	return fmt.Sprintf("%s.%s", pgx.Identifier{r.stateSchema}.Sanitize(), pgx.Identifier{name}.Sanitize())
}

type rowScanner interface {
	Scan(dest ...any) error
}

func (r *Repository) exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if r.tx != nil {
		return r.tx.Exec(ctx, query, args...)
	}

	return r.pool.Exec(ctx, query, args...)
}

func (r *Repository) queryRow(ctx context.Context, query string, args ...any) rowScanner {
	if r.tx != nil {
		return r.tx.QueryRow(ctx, query, args...)
	}

	return r.pool.QueryRow(ctx, query, args...)
}

func (r *Repository) query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	if r.tx != nil {
		return r.tx.Query(ctx, query, args...)
	}

	return r.pool.Query(ctx, query, args...)
}
