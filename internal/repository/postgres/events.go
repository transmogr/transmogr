// Package postgres implements the PostgreSQL-backed repository layer.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/transmogr/transmogr/internal/models"
)

// applyEventsBatch applies multiple events efficiently by batching per (peerRegion, table).
// Within each group, duplicate PKs are deduplicated — the last event per PK wins.
func (r *Repository) applyEventsBatch(
	ctx context.Context,
	events []models.ReplicationEvent,
	mode models.ReplicationApplyMode,
) error {
	if len(events) == 0 {
		return nil
	}

	type groupKey struct{ peerRegion, table string }

	type group struct {
		pkOrder []string
		byPK    map[string]models.ReplicationEvent
	}

	groupOrder := make([]groupKey, 0)
	groups := make(map[groupKey]*group)

	for _, event := range events {
		key := groupKey{event.PeerRegion, event.Table}
		g, ok := groups[key]
		if !ok {
			g = &group{byPK: make(map[string]models.ReplicationEvent)}
			groups[key] = g
			groupOrder = append(groupOrder, key)
		}
		pkKey := string(models.MustEncodePrimaryKey(event.PrimaryKey))
		if _, exists := g.byPK[pkKey]; !exists {
			g.pkOrder = append(g.pkOrder, pkKey)
		}
		g.byPK[pkKey] = event
	}

	for _, key := range groupOrder {
		g := groups[key]
		var upserts, updates, deletes []models.ReplicationEvent
		for _, pkKey := range g.pkOrder {
			event := g.byPK[pkKey]
			switch event.Operation {
			case models.ReplicationOperationDelete:
				deletes = append(deletes, event)
			case models.ReplicationOperationUpdate:
				updates = append(updates, event)
			default:
				upserts = append(upserts, event)
			}
		}
		if len(upserts) > 0 {
			if err := r.applyUpsertBatch(ctx, upserts, mode); err != nil {
				return err
			}
		}
		if len(updates) > 0 {
			if err := r.applyPartialUpdateBatch(ctx, updates, mode); err != nil {
				return err
			}
		}
		if len(deletes) > 0 {
			if err := r.applyDeleteBatch(ctx, deletes); err != nil {
				return err
			}
		}
	}

	return nil
}

// applyUpsertBatch inserts/updates a batch of events for one (peerRegion, table) using json_populate_recordset.
func (r *Repository) applyUpsertBatch(
	ctx context.Context,
	events []models.ReplicationEvent,
	mode models.ReplicationApplyMode,
) error {
	table := events[0].Table
	spec, tableRef, err := r.tableSpec(table)
	if err != nil {
		return err
	}

	provenance := provenanceValues(spec, events[0].PeerRegion)
	allowUpdate := mode != models.ReplicationApplyModeInsertOnly

	payloads := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		payload, err := models.FieldsToPayload(event.Fields)
		if err != nil {
			return err
		}
		payload, err = buildUpsertPayload(payload, provenance)
		if err != nil {
			return err
		}
		payloads = append(payloads, payload)
	}

	query, err := buildBatchUpsertQuery(spec, tableRef, payloads[0], allowUpdate)
	if err != nil {
		return err
	}

	batchPayload, err := json.Marshal(payloads)
	if err != nil {
		return fmt.Errorf("marshal batch upsert payload for %s: %w", table, err)
	}

	if _, err := r.exec(ctx, query, batchPayload); err != nil {
		return fmt.Errorf("apply upsert batch for %s: %w", table, err)
	}

	return nil
}

// applyPartialUpdateBatch applies partial UPDATE events (from cdc_update_diff) that carry only changed fields.
// Events are grouped by their field set so rows with the same changed columns share one SQL statement.
// If mode is insert_only, partial updates are skipped.
func (r *Repository) applyPartialUpdateBatch(
	ctx context.Context,
	events []models.ReplicationEvent,
	mode models.ReplicationApplyMode,
) error {
	if mode == models.ReplicationApplyModeInsertOnly {
		return nil
	}

	// Group events by their sorted field names so each group shares one UPDATE query.
	type groupKey struct{ table, cols string }
	type partialGroup struct {
		events   []models.ReplicationEvent
		payloads []json.RawMessage
	}

	groupOrder := make([]groupKey, 0)
	groups := make(map[groupKey]*partialGroup)

	for _, event := range events {
		cols := sortedFieldKeys(event.Fields)
		key := groupKey{table: event.Table, cols: strings.Join(cols, ",")}
		g, ok := groups[key]
		if !ok {
			g = &partialGroup{}
			groups[key] = g
			groupOrder = append(groupOrder, key)
		}
		payload, err := models.FieldsToPayload(event.Fields)
		if err != nil {
			return err
		}
		g.events = append(g.events, event)
		g.payloads = append(g.payloads, payload)
	}

	for _, key := range groupOrder {
		g := groups[key]
		if err := r.applyPartialUpdateGroup(ctx, g.events, g.payloads); err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) applyPartialUpdateGroup(
	ctx context.Context,
	events []models.ReplicationEvent,
	payloads []json.RawMessage,
) error {
	table := events[0].Table
	peerRegion := events[0].PeerRegion
	spec, tableRef, err := r.tableSpec(table)
	if err != nil {
		return err
	}

	query, err := buildBatchPartialUpdateQuery(spec, tableRef, payloads[0])
	if err != nil {
		return err
	}

	batchPayload, err := json.Marshal(payloads)
	if err != nil {
		return fmt.Errorf("marshal batch partial update payload for %s: %w", table, err)
	}

	args := []any{batchPayload}
	if spec.RegionField != "" && peerRegion != "" {
		args = append(args, peerRegion)
	}

	if _, err := r.exec(ctx, query, args...); err != nil {
		return fmt.Errorf("apply partial update batch for %s: %w", table, err)
	}

	return nil
}

// buildBatchPartialUpdateQuery builds an UPDATE query that applies partial field sets from
// json_populate_recordset. Only non-PK columns present in samplePayload are updated.
// The WHERE clause matches on primary key; an optional $2 region filter is added when RegionField is set.
func buildBatchPartialUpdateQuery(spec models.TableSpec, tableRef string, samplePayload []byte) (string, error) {
	var data map[string]json.RawMessage
	if err := json.Unmarshal(samplePayload, &data); err != nil {
		return "", fmt.Errorf("decode partial update payload for %s: %w", spec.Name, err)
	}

	pkSet := make(map[string]struct{}, len(spec.PrimaryKey))
	for _, pk := range spec.PrimaryKey {
		pkSet[pk] = struct{}{}
	}

	// Collect non-PK columns to update, sorted for stable query text.
	updateCols := make([]string, 0, len(data))
	for col := range data {
		if _, isPK := pkSet[col]; !isPK {
			updateCols = append(updateCols, col)
		}
	}
	sort.Strings(updateCols)

	if len(updateCols) == 0 {
		// Nothing to update beyond the PK — treat as no-op.
		return fmt.Sprintf("SELECT 1 FROM %s WHERE false", tableRef), nil
	}

	assignments := make([]string, 0, len(updateCols))
	for _, col := range updateCols {
		quoted := quoteIdentifier(col)
		assignments = append(assignments, fmt.Sprintf("%s = src.%s", quoted, quoted))
	}

	query := fmt.Sprintf(
		`UPDATE %s AS target SET %s FROM json_populate_recordset(NULL::%s, $1::json) AS src WHERE %s = %s`,
		tableRef,
		strings.Join(assignments, ", "),
		tableRef,
		tupleExpression("target", spec.PrimaryKey, quoteIdentifier),
		tupleExpression("src", spec.PrimaryKey, quoteIdentifier),
	)

	if spec.RegionField != "" {
		query += fmt.Sprintf(" AND target.%s = $2", quoteIdentifier(spec.RegionField))
	}

	return query, nil
}

func sortedFieldKeys(fields map[string][]byte) []string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// applyDeleteBatch deletes a batch of events for one (peerRegion, table) using json_populate_recordset.
func (r *Repository) applyDeleteBatch(ctx context.Context, events []models.ReplicationEvent) error {
	table := events[0].Table
	spec, tableRef, err := r.tableSpec(table)
	if err != nil {
		return err
	}

	query := buildBatchDeleteQuery(spec, tableRef)

	pkPayloads := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		pkPayload, err := primaryKeyPayload(event.PrimaryKey)
		if err != nil {
			return err
		}
		pkPayloads = append(pkPayloads, pkPayload)
	}

	batchPayload, err := json.Marshal(pkPayloads)
	if err != nil {
		return fmt.Errorf("marshal batch delete payload for %s: %w", table, err)
	}

	args := []any{batchPayload}
	if spec.RegionField != "" {
		args = append(args, events[0].PeerRegion)
	}

	if _, err := r.exec(ctx, query, args...); err != nil {
		return fmt.Errorf("apply delete batch for %s: %w", table, err)
	}

	return nil
}

func buildBatchDeleteQuery(spec models.TableSpec, tableRef string) string {
	query := fmt.Sprintf(
		"DELETE FROM %s AS target USING json_populate_recordset(NULL::%s, $1::json) AS source WHERE %s = %s",
		tableRef,
		tableRef,
		tupleExpression("target", spec.PrimaryKey, quoteIdentifier),
		tupleExpression("source", spec.PrimaryKey, quoteIdentifier),
	)
	if spec.RegionField != "" {
		query += fmt.Sprintf(
			" AND (target.%s IS NULL OR target.%s = $2)",
			quoteIdentifier(spec.RegionField),
			quoteIdentifier(spec.RegionField),
		)
	}
	return query
}
