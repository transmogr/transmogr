package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/transmogr/transmogr/internal/models"
)

const defaultReplayPageSize = 256

// OutboxNextSequence allocates the next durable sequence for one peer and table.
func (r *Repository) OutboxNextSequence(ctx context.Context, peerRegion, table string) (uint64, error) {
	tableRef := r.stateTable("outbound_sequences")
	query := fmt.Sprintf(`
		INSERT INTO %s AS s (peer_region, table_name, next_sequence, updated_at)
		VALUES ($1, $2, 1, NOW())
		ON CONFLICT (peer_region, table_name)
		DO UPDATE SET
			next_sequence = s.next_sequence + 1,
			updated_at = NOW()
		RETURNING next_sequence
	`, tableRef)

	var sequence uint64
	if err := r.pool.QueryRow(ctx, query, peerRegion, table).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("allocate outbox sequence for %s/%s: %w", peerRegion, table, err)
	}

	return sequence, nil
}

// OutboxEnqueue persists a pending batch and returns the stored batch if inserted.
func (r *Repository) OutboxEnqueue(ctx context.Context, batch models.OutboxBatch) (models.OutboxBatch, bool, error) {
	if err := batch.Validate(); err != nil {
		return models.OutboxBatch{}, false, err
	}

	payload, err := json.Marshal(batch.Events)
	if err != nil {
		return models.OutboxBatch{}, false, fmt.Errorf(
			"encode outbox batch for %s/%s: %w",
			batch.PeerRegion,
			batch.Table,
			err,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			peer_region, table_name, sequence, last_event_id, tx_id, payload, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (peer_region, table_name, last_event_id) DO NOTHING
		RETURNING id, created_at
	`, r.stateTable("outbound_batches"))

	var createdAt time.Time
	var id uint64
	err = r.pool.QueryRow(
		ctx,
		query,
		batch.PeerRegion,
		batch.Table,
		batch.Sequence,
		batch.LastEventID(),
		batch.TxID,
		payload,
	).Scan(&id, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.OutboxBatch{}, false, nil
		}
		return models.OutboxBatch{}, false, fmt.Errorf(
			"enqueue outbox batch for %s/%s seq=%d: %w",
			batch.PeerRegion,
			batch.Table,
			batch.Sequence,
			err,
		)
	}

	batch.ID = id
	batch.CreatedAt = createdAt.UTC()
	return batch, true, nil
}

// OutboxListPending returns the oldest pending batches for one peer.
// Non-positive limits disable the LIMIT clause and return the full backlog.
func (r *Repository) OutboxListPending(
	ctx context.Context,
	peerRegion string,
	limit int,
) ([]models.OutboxBatch, error) {
	query := fmt.Sprintf(`
		SELECT id, peer_region, table_name, sequence, last_event_id, tx_id, payload, created_at
		FROM %s
		WHERE peer_region = $1
		ORDER BY id ASC
	`, r.stateTable("outbound_batches"))
	args := []any{peerRegion}
	if limit > 0 {
		query += "\n\t\tLIMIT $2"
		args = append(args, limit)
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox batches for %s: %w", peerRegion, err)
	}
	defer rows.Close()

	return scanPendingRows(rows, peerRegion)
}

// OutboxListPendingWindow returns a page of pending batches within one replay window.
func (r *Repository) OutboxListPendingWindow(
	ctx context.Context,
	peerRegion string,
	lowerExclusive, upperInclusive time.Time,
	afterID uint64,
	limit int,
) ([]models.OutboxBatch, error) {
	if limit <= 0 {
		limit = defaultReplayPageSize
	}

	query := fmt.Sprintf(`
		SELECT id, peer_region, table_name, sequence, last_event_id, tx_id, payload, created_at
		FROM %s
		WHERE peer_region = $1
		  AND id > $2
	`, r.stateTable("outbound_batches"))

	args := []any{peerRegion, afterID}
	argPos := 3
	if !lowerExclusive.IsZero() {
		query += fmt.Sprintf("\n\t\t  AND created_at > $%d", argPos)
		args = append(args, lowerExclusive)
		argPos++
	}
	if !upperInclusive.IsZero() {
		query += fmt.Sprintf("\n\t\t  AND created_at <= $%d", argPos)
		args = append(args, upperInclusive)
		argPos++
	}
	query += fmt.Sprintf("\n\t\tORDER BY id ASC\n\t\tLIMIT $%d", argPos)
	args = append(args, limit)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox window for %s after id=%d: %w", peerRegion, afterID, err)
	}
	defer rows.Close()

	return scanPendingRows(rows, peerRegion)
}

// OutboxAck deletes one persisted pending batch.
func (r *Repository) OutboxAck(ctx context.Context, peerRegion, table, lastEventID string) (bool, error) {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE peer_region = $1
		  AND table_name = $2
		  AND last_event_id = $3
	`, r.stateTable("outbound_batches"))

	tag, err := r.pool.Exec(ctx, query, peerRegion, table, lastEventID)
	if err != nil {
		return false, fmt.Errorf(
			"ack outbox batch for %s/%s last_event_id=%s: %w",
			peerRegion,
			table,
			lastEventID,
			err,
		)
	}

	return tag.RowsAffected() > 0, nil
}

// OutboxSnapshotWatermark returns the database timestamp used as the sender-side snapshot boundary.
func (r *Repository) OutboxSnapshotWatermark(ctx context.Context) (time.Time, error) {
	var watermark time.Time
	if err := r.pool.QueryRow(ctx, "SELECT NOW()").Scan(&watermark); err != nil {
		return time.Time{}, fmt.Errorf("snapshot watermark: %w", err)
	}
	return watermark.UTC(), nil
}

// OutboxReady reports whether the PostgreSQL-backed outbox can reach its database.
func (r *Repository) OutboxReady(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// OutboxCleanup prunes stale or excessive pending batches and returns deleted counts.
func (r *Repository) OutboxCleanup(
	ctx context.Context,
	maxBatchAge time.Duration,
	maxPendingPerPeer int,
) (int64, int64, error) {
	var deletedByAge int64
	if maxBatchAge > 0 {
		query := fmt.Sprintf(`
			DELETE FROM %s
			WHERE created_at < NOW() - $1::interval
		`, r.stateTable("outbound_batches"))

		tag, err := r.pool.Exec(ctx, query, formatInterval(maxBatchAge))
		if err != nil {
			return 0, 0, fmt.Errorf("cleanup outbox batches by age: %w", err)
		}
		deletedByAge = tag.RowsAffected()
	}

	var deletedByCount int64
	if maxPendingPerPeer > 0 {
		query := fmt.Sprintf(`
			WITH ranked AS (
				SELECT ctid,
				       row_number() OVER (PARTITION BY peer_region ORDER BY id DESC) AS row_num
				FROM %s
			)
			DELETE FROM %s AS b
			USING ranked
			WHERE b.ctid = ranked.ctid
			  AND ranked.row_num > $1
		`, r.stateTable("outbound_batches"), r.stateTable("outbound_batches"))

		tag, err := r.pool.Exec(ctx, query, maxPendingPerPeer)
		if err != nil {
			return 0, 0, fmt.Errorf("cleanup outbox batches by count: %w", err)
		}
		deletedByCount = tag.RowsAffected()
	}

	return deletedByAge, deletedByCount, nil
}

// OutboxLoadMetricsSnapshot returns a point-in-time backlog snapshot from PostgreSQL.
func (r *Repository) OutboxLoadMetricsSnapshot(ctx context.Context) (models.OutboxMetricsSnapshot, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*)::bigint,
			COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0)
		FROM %s
	`, r.stateTable("outbound_batches"))

	var (
		pending       int64
		oldestAgeSecs float64
	)
	if err := r.pool.QueryRow(ctx, query).Scan(&pending, &oldestAgeSecs); err != nil {
		return models.OutboxMetricsSnapshot{}, err
	}
	if pending < 0 {
		pending = 0
	}
	if oldestAgeSecs < 0 {
		oldestAgeSecs = 0
	}

	return models.OutboxMetricsSnapshot{
		PendingBatches: uint64(pending),
		OldestBatchAge: time.Duration(oldestAgeSecs * float64(time.Second)),
	}, nil
}

func scanPendingRows(rows pgx.Rows, peerRegion string) ([]models.OutboxBatch, error) {
	var batches []models.OutboxBatch
	for rows.Next() {
		var (
			batch       models.OutboxBatch
			lastEventID string
			payload     []byte
		)
		if err := rows.Scan(
			&batch.ID,
			&batch.PeerRegion,
			&batch.Table,
			&batch.Sequence,
			&lastEventID,
			&batch.TxID,
			&payload,
			&batch.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending outbox batch for %s: %w", peerRegion, err)
		}
		if err := json.Unmarshal(payload, &batch.Events); err != nil {
			return nil, fmt.Errorf(
				"decode pending outbox batch for %s/%s seq=%d: %w",
				batch.PeerRegion,
				batch.Table,
				batch.Sequence,
				err,
			)
		}
		if batch.LastEventID() != lastEventID {
			return nil, fmt.Errorf(
				"pending outbox batch for %s/%s seq=%d is inconsistent",
				batch.PeerRegion,
				batch.Table,
				batch.Sequence,
			)
		}
		batches = append(batches, batch)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox batches for %s: %w", peerRegion, err)
	}

	return batches, nil
}

func formatInterval(value time.Duration) string {
	return fmt.Sprintf("%f seconds", value.Seconds())
}
