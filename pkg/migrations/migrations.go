// Package migrations applies embedded database schema migrations.
package migrations

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	migrationfiles "github.com/transmogr/transmogr/migrations"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaPlaceholder = "__state_schema__"

// ApplyUp applies all up migrations using the configured state schema.
func ApplyUp(ctx context.Context, pool *pgxpool.Pool, stateSchema string) error {
	return applyUp(ctx, pool, stateSchema)
}

// ApplyDown applies all down migrations using the configured state schema.
func ApplyDown(ctx context.Context, pool *pgxpool.Pool, stateSchema string) error {
	return applyDown(ctx, pool, stateSchema)
}

func applyUp(ctx context.Context, pool *pgxpool.Pool, stateSchema string) error {
	if strings.TrimSpace(stateSchema) == "" {
		return fmt.Errorf("state schema is required")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for migrations: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext('transmogr_schema_v1'))"); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer conn.Exec(ctx, "SELECT pg_advisory_unlock(hashtext('transmogr_schema_v1'))") //nolint:errcheck

	paths, err := migrationPaths("*.up.sql")
	if err != nil {
		return err
	}

	renderedSchema := pgx.Identifier{stateSchema}.Sanitize()
	if err := ensureMigrationsTable(ctx, conn.Conn(), renderedSchema); err != nil {
		return err
	}
	currentVersion, dirty, err := currentVersion(ctx, conn.Conn(), renderedSchema)
	if err != nil {
		return err
	}
	if dirty {
		return fmt.Errorf("schema migrations are dirty at version %d", currentVersion)
	}

	for _, path := range paths {
		version := migrationVersion(path)
		if version <= currentVersion {
			continue
		}

		raw, err := fs.ReadFile(migrationfiles.Files, path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", path, err)
		}

		rendered := strings.ReplaceAll(string(raw), schemaPlaceholder, renderedSchema)
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration tx %s: %w", path, err)
		}
		if err := setVersion(ctx, tx.Conn(), renderedSchema, version, true); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}

		if _, err := tx.Exec(ctx, rendered); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", path, err)
		}
		if err := setVersion(ctx, tx.Conn(), renderedSchema, version, false); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", path, err)
		}
	}

	return nil
}

func applyDown(ctx context.Context, pool *pgxpool.Pool, stateSchema string) error {
	if strings.TrimSpace(stateSchema) == "" {
		return fmt.Errorf("state schema is required")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for migrations: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext('transmogr_schema_v1'))"); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer conn.Exec(ctx, "SELECT pg_advisory_unlock(hashtext('transmogr_schema_v1'))") //nolint:errcheck

	paths, err := migrationPaths("*.down.sql")
	if err != nil {
		return err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))

	renderedSchema := pgx.Identifier{stateSchema}.Sanitize()
	if err := ensureMigrationsTable(ctx, conn.Conn(), renderedSchema); err != nil {
		return err
	}
	currentVersion, dirty, err := currentVersion(ctx, conn.Conn(), renderedSchema)
	if err != nil {
		return err
	}
	if dirty {
		return fmt.Errorf("schema migrations are dirty at version %d", currentVersion)
	}

	for _, path := range paths {
		version := migrationVersion(path)
		if version != currentVersion {
			continue
		}

		raw, err := fs.ReadFile(migrationfiles.Files, path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", path, err)
		}

		rendered := strings.ReplaceAll(string(raw), schemaPlaceholder, renderedSchema)
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration tx %s: %w", path, err)
		}
		if err := setVersion(ctx, tx.Conn(), renderedSchema, version, true); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}

		if _, err := tx.Exec(ctx, rendered); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", path, err)
		}
		nextVersion := int64(0)
		if idx := findDownPathIndex(paths, path); idx >= 0 && idx < len(paths)-1 {
			nextVersion = migrationVersion(paths[idx+1])
		}
		if err := setVersion(ctx, tx.Conn(), renderedSchema, nextVersion, false); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", path, err)
		}
		currentVersion = nextVersion
	}

	return nil
}

func migrationPaths(pattern string) ([]string, error) {
	paths, err := fs.Glob(migrationfiles.Files, pattern)
	if err != nil {
		return nil, fmt.Errorf("glob migrations: %w", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no migration files matched %q", pattern)
	}
	return paths, nil
}

func ensureMigrationsTable(ctx context.Context, conn *pgx.Conn, renderedSchema string) error {
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", renderedSchema)); err != nil {
		return fmt.Errorf("ensure state schema: %w", err)
	}
	if _, err := conn.Exec(
		ctx,
		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.schema_migrations (
				version BIGINT NOT NULL PRIMARY KEY,
				dirty BOOLEAN NOT NULL
			)`,
			renderedSchema,
		),
	); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	return nil
}

func currentVersion(ctx context.Context, conn *pgx.Conn, renderedSchema string) (int64, bool, error) {
	var version int64
	var dirty bool
	if err := conn.QueryRow(
		ctx,
		fmt.Sprintf("SELECT version, dirty FROM %s.schema_migrations ORDER BY version DESC LIMIT 1", renderedSchema),
	).Scan(&version, &dirty); err != nil {
		if err == pgx.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read schema_migrations: %w", err)
	}

	return version, dirty, nil
}

func setVersion(ctx context.Context, conn *pgx.Conn, renderedSchema string, version int64, dirty bool) error {
	if _, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.schema_migrations", renderedSchema)); err != nil {
		return fmt.Errorf("truncate schema_migrations: %w", err)
	}
	if version == 0 && !dirty {
		return nil
	}
	if _, err := conn.Exec(
		ctx,
		fmt.Sprintf("INSERT INTO %s.schema_migrations (version, dirty) VALUES ($1, $2)", renderedSchema),
		version,
		dirty,
	); err != nil {
		return fmt.Errorf("write schema_migrations: %w", err)
	}

	return nil
}

func migrationVersion(path string) int64 {
	name := filepath.Base(path)
	raw := name
	switch {
	case strings.HasSuffix(name, ".up.sql"):
		raw = strings.TrimSuffix(name, ".up.sql")
	case strings.HasSuffix(name, ".down.sql"):
		raw = strings.TrimSuffix(name, ".down.sql")
	}

	prefix, _, _ := strings.Cut(raw, "_")
	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0
	}

	return version
}

func findDownPathIndex(paths []string, target string) int {
	for i, path := range paths {
		if path == target {
			return i
		}
	}

	return -1
}
