package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/transmogr/transmogr/internal/models"
)

// ChangefeedCapturePrimaryKeyTypes returns cached or queried PK type metadata for one table.
func (r *Repository) ChangefeedCapturePrimaryKeyTypes(
	ctx context.Context,
	table string,
) (map[string]models.PrimaryKeyType, error) {
	return r.primaryKeyTypes(ctx, table)
}

// ChangefeedCaptureEnsureReplicaIdentityFull sets REPLICA IDENTITY FULL on all configured tables.
func (r *Repository) ChangefeedCaptureEnsureReplicaIdentityFull(ctx context.Context, tables []models.TableSpec) error {
	for _, spec := range sortedTableSpecs(tables) {
		tableRef, err := quoteTableName(spec.Name)
		if err != nil {
			return err
		}
		query := fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableRef)
		if _, err := r.exec(ctx, query); err != nil {
			return fmt.Errorf("set replica identity full for %s: %w", spec.Name, err)
		}
	}
	return nil
}

// ChangefeedCaptureEnsurePublication creates or updates a publication for the configured tables.
func (r *Repository) ChangefeedCaptureEnsurePublication(
	ctx context.Context,
	publication string,
	tables []models.TableSpec,
) error {
	if strings.TrimSpace(publication) == "" {
		return fmt.Errorf("cdc publication name is required")
	}

	tableNames := make([]string, 0, len(tables))
	for _, spec := range sortedTableSpecs(tables) {
		tableRef, err := quoteTableName(spec.Name)
		if err != nil {
			return err
		}
		tableNames = append(tableNames, tableRef)
	}
	sort.Strings(tableNames)

	if len(tableNames) == 0 {
		return nil
	}

	var exists bool
	if err := r.queryRow(
		ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`,
		publication,
	).Scan(&exists); err != nil {
		return fmt.Errorf("check publication %q: %w", publication, err)
	}

	quotedPublication := quoteIdentifier(publication)
	if !exists {
		query := fmt.Sprintf(
			"CREATE PUBLICATION %s FOR TABLE %s",
			quotedPublication,
			strings.Join(tableNames, ", "),
		)
		if _, err := r.exec(ctx, query); err != nil {
			return fmt.Errorf("create publication %q: %w", publication, err)
		}
		return nil
	}

	query := fmt.Sprintf(
		"ALTER PUBLICATION %s SET TABLE %s",
		quotedPublication,
		strings.Join(tableNames, ", "),
	)
	if _, err := r.exec(ctx, query); err != nil {
		return fmt.Errorf("alter publication %q: %w", publication, err)
	}

	return nil
}

// ChangefeedCaptureEnsureReplicationSlot returns the current slot LSN or creates the slot if missing.
func (r *Repository) ChangefeedCaptureEnsureReplicationSlot(ctx context.Context, slot string) (pglogrepl.LSN, error) {
	var confirmedFlushLSN string
	err := r.queryRow(
		ctx,
		`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`,
		slot,
	).Scan(&confirmedFlushLSN)
	if err == nil {
		lsn, parseErr := pglogrepl.ParseLSN(confirmedFlushLSN)
		if parseErr != nil {
			return 0, fmt.Errorf("parse slot lsn %q: %w", confirmedFlushLSN, parseErr)
		}
		return lsn, nil
	}
	if err != nil && err != pgx.ErrNoRows {
		return 0, fmt.Errorf("query replication slot %q: %w", slot, err)
	}

	var slotName string
	var lsnText string
	if err := r.queryRow(
		ctx,
		`SELECT slot_name, lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')`,
		slot,
	).Scan(&slotName, &lsnText); err != nil {
		return 0, fmt.Errorf("create logical replication slot %q: %w", slot, err)
	}
	_ = slotName

	lsn, err := pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return 0, fmt.Errorf("parse created slot lsn %q: %w", lsnText, err)
	}

	return lsn, nil
}

func sortedTableSpecs(tables []models.TableSpec) []models.TableSpec {
	sorted := append([]models.TableSpec(nil), tables...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})
	return sorted
}
