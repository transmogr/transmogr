// Package transformers applies inbound and outbound replication transformation stages.
package transformers

import (
	"context"

	"github.com/transmogr/transmogr/internal/models"
)

// OutboundTransformer transforms one outbox batch after it leaves durable storage
// and before it is encoded onto the wire.
type OutboundTransformer interface {
	PrepareOutboundBatch(ctx context.Context, batch models.OutboxBatch) (models.OutboxBatch, error)
	PrepareOutboundSnapshotRows(ctx context.Context, table string, rows []models.SnapshotRow) ([]models.SnapshotRow, error)
}

// Outbound runs an ordered outbound transformer chain.
type Outbound struct {
	transformers []OutboundTransformer
}

// NewOutbound creates an outbound pipeline.
func NewOutbound(transformers ...OutboundTransformer) Outbound {
	return Outbound{transformers: append([]OutboundTransformer(nil), transformers...)}
}

// PrepareBatch applies all outbound transformers in order.
func (p Outbound) PrepareBatch(ctx context.Context, batch models.OutboxBatch) (models.OutboxBatch, error) {
	prepared := cloneOutboxBatch(batch)
	for _, transformer := range p.transformers {
		var err error
		prepared, err = transformer.PrepareOutboundBatch(ctx, prepared)
		if err != nil {
			return models.OutboxBatch{}, err
		}
	}
	return prepared, nil
}

// PrepareSnapshotRows applies all outbound transformers to snapshot rows in order.
func (p Outbound) PrepareSnapshotRows(
	ctx context.Context,
	table string,
	rows []models.SnapshotRow,
) ([]models.SnapshotRow, error) {
	prepared := cloneSnapshotRows(rows)
	for _, transformer := range p.transformers {
		var err error
		prepared, err = transformer.PrepareOutboundSnapshotRows(ctx, table, prepared)
		if err != nil {
			return nil, err
		}
	}
	return prepared, nil
}

func cloneOutboxBatch(batch models.OutboxBatch) models.OutboxBatch {
	batch.Events = cloneReplicationEvents(batch.Events)
	return batch
}

func cloneReplicationEvents(events []models.ReplicationEvent) []models.ReplicationEvent {
	if events == nil {
		return nil
	}

	cloned := make([]models.ReplicationEvent, len(events))
	for i, event := range events {
		cloned[i] = event
		cloned[i].PrimaryKey = models.ClonePrimaryKeyParts(event.PrimaryKey)
		cloned[i].Fields = models.CloneFields(event.Fields)
		cloned[i].Metadata = models.CloneFields(event.Metadata)
	}

	return cloned
}

func cloneSnapshotRows(rows []models.SnapshotRow) []models.SnapshotRow {
	if rows == nil {
		return nil
	}

	cloned := make([]models.SnapshotRow, len(rows))
	for i, row := range rows {
		cloned[i] = row
		cloned[i].PrimaryKey = models.ClonePrimaryKeyParts(row.PrimaryKey)
		cloned[i].Payload = append([]byte(nil), row.Payload...)
		cloned[i].Metadata = models.CloneFields(row.Metadata)
	}

	return cloned
}
