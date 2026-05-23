package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/transmogr/transmogr/internal/models"
)

// ReadSnapshotChunk reads the next snapshot chunk from PostgreSQL.
func (r *Repository) ReadSnapshotChunk(
	ctx context.Context,
	req models.SnapshotReadRequest,
) (models.SnapshotChunk, error) {
	spec, tableRef, err := r.tableSpec(req.Table)
	if err != nil {
		return models.SnapshotChunk{}, err
	}
	pkTypes, err := r.primaryKeyTypes(ctx, req.Table)
	if err != nil {
		return models.SnapshotChunk{}, err
	}

	args := make([]any, 0, 3)
	query := fmt.Sprintf("SELECT to_jsonb(t) AS payload, 0 AS version FROM %s AS t", tableRef)

	if len(req.AfterPrimaryKey) > 0 {
		afterPayload, err := primaryKeyPayload(req.AfterPrimaryKey)
		if err != nil {
			return models.SnapshotChunk{}, err
		}
		args = append(args, afterPayload)
		query += fmt.Sprintf(`, json_populate_record(NULL::%s, $%d::json) AS cursor`, tableRef, len(args))
	}

	var whereParts []string
	if spec.RegionField != "" && req.LocalRegion != "" {
		args = append(args, req.LocalRegion)
		whereParts = append(whereParts, fmt.Sprintf("t.%s = $%d", quoteIdentifier(spec.RegionField), len(args)))
	}
	if len(req.AfterPrimaryKey) > 0 {
		whereParts = append(whereParts, fmt.Sprintf("%s > %s",
			tupleExpression("t", spec.PrimaryKey, quoteIdentifier),
			tupleExpression("cursor", spec.PrimaryKey, quoteIdentifier),
		))
	}
	if len(whereParts) > 0 {
		query += " WHERE " + strings.Join(whereParts, " AND ")
	}

	args = append(args, req.Limit)
	orderBy := orderByExpression("t", spec.PrimaryKey, quoteIdentifier)
	query += fmt.Sprintf(" ORDER BY %s LIMIT $%d", orderBy, len(args))

	rows, err := r.query(ctx, query, args...)
	if err != nil {
		return models.SnapshotChunk{}, fmt.Errorf("read snapshot chunk for %s: %w", req.Table, err)
	}
	defer rows.Close()

	chunkRows := make([]models.SnapshotRow, 0, req.Limit)
	for rows.Next() {
		var payload []byte
		var version int64
		if err := rows.Scan(&payload, &version); err != nil {
			return models.SnapshotChunk{}, fmt.Errorf("scan snapshot row for %s: %w", req.Table, err)
		}
		primaryKey, err := models.PrimaryKeyPartsFromPayload(payload, spec.PrimaryKey, pkTypes)
		if err != nil {
			return models.SnapshotChunk{}, fmt.Errorf("extract snapshot primary key for %s: %w", req.Table, err)
		}

		chunkRows = append(chunkRows, models.SnapshotRow{
			PrimaryKey: primaryKey,
			Payload:    payload,
			Version:    version,
		})
	}

	if err := rows.Err(); err != nil {
		return models.SnapshotChunk{}, fmt.Errorf("iterate snapshot rows for %s: %w", req.Table, err)
	}

	return models.SnapshotChunk{
		Rows: chunkRows,
		Last: len(chunkRows) < req.Limit,
	}, nil
}

// ApplySnapshotChunk upserts a chunk of snapshot rows into PostgreSQL.
func (r *Repository) ApplySnapshotChunk(
	ctx context.Context,
	peerRegion string,
	table string,
	rows []models.SnapshotRow,
) error {
	if len(rows) == 0 {
		return nil
	}

	spec, tableRef, err := r.tableSpec(table)
	if err != nil {
		return err
	}

	provenance := provenanceValues(spec, peerRegion)

	// Build the query once using the first row as a representative sample.
	// All rows from the same table share the same column set (to_jsonb always returns identical keys),
	// so the resulting query is valid for every row in the batch.
	// If building fails, no transaction is needed.
	samplePayload, err := buildUpsertPayload(rows[0].Payload, provenance)
	if err != nil {
		return err
	}
	query, err := buildBatchUpsertQuery(spec, tableRef, samplePayload, true)
	if err != nil {
		return err
	}

	// Collect all payloads into a JSON array for a single INSERT via json_populate_recordset.
	payloads := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		payload, err := buildUpsertPayload(row.Payload, provenance)
		if err != nil {
			return err
		}
		payloads = append(payloads, payload)
	}
	batchPayload, err := json.Marshal(payloads)
	if err != nil {
		return fmt.Errorf("marshal batch snapshot payload for %s: %w", table, err)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin snapshot apply tx for %s: %w", table, err)
	}

	// Snapshot apply uses the same trigger suppression as incremental apply:
	// FK checks and any user-defined table triggers on the target table are
	// disabled for this transaction.
	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("disable triggers for snapshot apply %s: %w", table, err)
	}

	if _, err := tx.Exec(ctx, query, batchPayload); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("apply snapshot chunk for %s: %w", table, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit snapshot apply tx for %s: %w", table, err)
	}

	return nil
}

func (r *Repository) tableSpec(table string) (models.TableSpec, string, error) {
	spec, ok := r.tableByName[table]
	if !ok {
		return models.TableSpec{}, "", fmt.Errorf("table %q is not configured", table)
	}

	tableRef, err := quoteTableName(spec.Name)
	if err != nil {
		return models.TableSpec{}, "", err
	}

	return spec, tableRef, nil
}

// buildBatchUpsertQuery builds an upsert query that accepts a JSON array via json_populate_recordset,
// inserting all rows in a single statement.
func buildBatchUpsertQuery(spec models.TableSpec, tableRef string, payload []byte, allowUpdate bool) (string, error) {
	var data map[string]json.RawMessage
	if err := json.Unmarshal(payload, &data); err != nil {
		return "", fmt.Errorf("decode snapshot payload for %s: %w", spec.Name, err)
	}

	for _, column := range spec.PrimaryKey {
		if _, ok := data[column]; !ok {
			return "", fmt.Errorf("snapshot payload for %s missing primary key %q", spec.Name, column)
		}
	}

	columns := make([]string, 0, len(data))
	for column := range data {
		columns = append(columns, column)
	}
	sort.Strings(columns)

	quotedColumns := make([]string, 0, len(columns))
	selectColumns := make([]string, 0, len(columns))
	updateAssignments := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted := quoteIdentifier(column)
		quotedColumns = append(quotedColumns, quoted)
		selectColumns = append(selectColumns, quoted)
		if slices.Contains(spec.PrimaryKey, column) {
			continue
		}
		updateAssignments = append(updateAssignments, fmt.Sprintf("%s = EXCLUDED.%s", quoted, quoted))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s AS target (%s) SELECT %s FROM json_populate_recordset(NULL::%s, $1::json) AS src ON CONFLICT (%s)",
		tableRef,
		strings.Join(quotedColumns, ", "),
		strings.Join(selectColumns, ", "),
		tableRef,
		conflictTarget(spec.PrimaryKey, quoteIdentifier),
	)

	if len(updateAssignments) == 0 || !allowUpdate {
		return query + " DO NOTHING", nil
	}

	query += " DO UPDATE SET " + strings.Join(updateAssignments, ", ")
	if conflictWhere := ownerRegionConflictWhere(spec); conflictWhere != "" {
		query += conflictWhere
	}

	return query, nil
}

func buildUpsertPayload(payload []byte, extra map[string]any) ([]byte, error) {
	if len(extra) == 0 {
		return payload, nil
	}

	var data map[string]json.RawMessage
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, fmt.Errorf("decode upsert payload: %w", err)
	}

	for key, value := range extra {
		if key == "" {
			continue
		}

		raw, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal provenance field %q: %w", key, err)
		}
		data[key] = raw
	}

	merged, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("encode upsert payload: %w", err)
	}

	return merged, nil
}

func provenanceValues(spec models.TableSpec, region string) map[string]any {
	values := make(map[string]any, 1)
	if spec.RegionField != "" && region != "" {
		values[spec.RegionField] = region
	}

	return values
}

func quoteTableName(name string) (string, error) {
	parts := strings.Split(name, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return "", fmt.Errorf("invalid table name %q", name)
	}

	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", fmt.Errorf("invalid table name %q", name)
		}
		quoted = append(quoted, quoteIdentifier(part))
	}

	return strings.Join(quoted, "."), nil
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func ownerRegionConflictWhere(spec models.TableSpec) string {
	if ownerCondition := ownerRegionCondition(spec); ownerCondition != "" {
		return " WHERE " + ownerCondition
	}

	return ""
}

func ownerRegionCondition(spec models.TableSpec) string {
	if spec.RegionField == "" {
		return ""
	}

	quotedRegion := quoteIdentifier(spec.RegionField)
	return fmt.Sprintf(
		"(target.%s IS NULL OR target.%s = EXCLUDED.%s)",
		quotedRegion,
		quotedRegion,
		quotedRegion,
	)
}
