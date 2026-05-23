package transformers

import (
	"context"

	"github.com/transmogr/transmogr/internal/models"
)

// InboundTransformer transforms inbound replication payloads after they are
// decoded from the wire and before they are applied locally.
type InboundTransformer interface {
	PrepareInboundEvents(ctx context.Context, events []models.ReplicationEvent) ([]models.ReplicationEvent, error)
	PrepareInboundSnapshotRows(ctx context.Context, table string, rows []models.SnapshotRow) ([]models.SnapshotRow, error)
}

// Inbound runs an ordered inbound transformer chain.
type Inbound struct {
	transformers []InboundTransformer
}

// NewInbound creates an inbound pipeline.
func NewInbound(transformers ...InboundTransformer) Inbound {
	return Inbound{transformers: append([]InboundTransformer(nil), transformers...)}
}

// PrepareEvents applies all inbound transformers in order.
func (p Inbound) PrepareEvents(
	ctx context.Context,
	events []models.ReplicationEvent,
) ([]models.ReplicationEvent, error) {
	prepared := cloneReplicationEvents(events)
	for _, transformer := range p.transformers {
		var err error
		prepared, err = transformer.PrepareInboundEvents(ctx, prepared)
		if err != nil {
			return nil, err
		}
	}
	return prepared, nil
}

// PrepareSnapshotRows applies all inbound transformers to snapshot rows in order.
func (p Inbound) PrepareSnapshotRows(
	ctx context.Context,
	table string,
	rows []models.SnapshotRow,
) ([]models.SnapshotRow, error) {
	prepared := cloneSnapshotRows(rows)
	for _, transformer := range p.transformers {
		var err error
		prepared, err = transformer.PrepareInboundSnapshotRows(ctx, table, prepared)
		if err != nil {
			return nil, err
		}
	}
	return prepared, nil
}
