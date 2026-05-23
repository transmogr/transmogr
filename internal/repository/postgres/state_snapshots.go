package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/transmogr/transmogr/internal/models"
)

// GetStateSnapshotJob loads persisted snapshot state for a peer and table.
func (r *Repository) GetStateSnapshotJob(
	ctx context.Context,
	peerRegion,
	table string,
) (models.StateSnapshotJob, error) {
	query := fmt.Sprintf(`
		SELECT peer_region, table_name, status, last_primary_key, watermark_at, finished_at IS NOT NULL
		FROM %s
		WHERE peer_region = $1 AND table_name = $2
	`, r.stateTable("snapshot_jobs"))

	var job models.StateSnapshotJob
	var lastPrimaryKey []byte
	var watermarkAt *time.Time
	err := r.queryRow(ctx, query, peerRegion, table).Scan(
		&job.PeerRegion,
		&job.Table,
		&job.Status,
		&lastPrimaryKey,
		&watermarkAt,
		&job.Finished,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.StateSnapshotJob{
				PeerRegion: peerRegion,
				Table:      table,
			}, nil
		}

		return models.StateSnapshotJob{}, fmt.Errorf("get snapshot job: %w", err)
	}
	job.LastPrimaryKey, err = models.DecodePrimaryKey(lastPrimaryKey)
	if err != nil {
		return models.StateSnapshotJob{}, fmt.Errorf("decode snapshot job primary key: %w", err)
	}
	if watermarkAt != nil {
		job.WatermarkAt = watermarkAt.UTC()
	}

	return job, nil
}

// UpsertStateSnapshotJob creates or resets a snapshot job.
func (r *Repository) UpsertStateSnapshotJob(ctx context.Context, job models.StateSnapshotJob) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (peer_region, table_name, status, started_at, finished_at, last_primary_key, watermark_at)
		VALUES ($1, $2, $3, NOW(), NULL, $4, $5)
		ON CONFLICT (peer_region, table_name)
		DO UPDATE SET
			status = EXCLUDED.status,
			last_primary_key = EXCLUDED.last_primary_key,
			watermark_at = COALESCE(EXCLUDED.watermark_at, snapshot_jobs.watermark_at),
			finished_at = NULL
	`, r.stateTable("snapshot_jobs"))

	encodedPrimaryKey, err := models.EncodePrimaryKey(job.LastPrimaryKey)
	if err != nil {
		return fmt.Errorf("encode snapshot job primary key: %w", err)
	}

	_, err = r.exec(ctx, query, job.PeerRegion, job.Table, job.Status, encodedPrimaryKey, nullableTime(job.WatermarkAt))
	if err != nil {
		return fmt.Errorf("upsert snapshot job: %w", err)
	}

	return nil
}

// CommitStateSnapshotCheckpoint atomically persists snapshot progress when unfinished.
func (r *Repository) CommitStateSnapshotCheckpoint(
	ctx context.Context,
	job models.StateSnapshotJob,
) (models.CommitSnapshotCheckpointResult, error) {
	query := fmt.Sprintf(`
		INSERT INTO %s (peer_region, table_name, status, started_at, finished_at, last_primary_key, watermark_at)
		VALUES ($1, $2, $3, NOW(), NULL, $4, $5)
		ON CONFLICT (peer_region, table_name)
		DO UPDATE SET
			status = EXCLUDED.status,
			last_primary_key = EXCLUDED.last_primary_key,
			watermark_at = COALESCE(EXCLUDED.watermark_at, snapshot_jobs.watermark_at)
		WHERE snapshot_jobs.finished_at IS NULL
	`, r.stateTable("snapshot_jobs"))

	if job.Status == "" {
		job.Status = models.SnapshotJobStatusSnapshotting
	}

	encodedPrimaryKey, err := models.EncodePrimaryKey(job.LastPrimaryKey)
	if err != nil {
		return models.CommitSnapshotCheckpointResult{}, fmt.Errorf("encode snapshot checkpoint primary key: %w", err)
	}

	tag, err := r.exec(
		ctx,
		query,
		job.PeerRegion,
		job.Table,
		job.Status,
		encodedPrimaryKey,
		nullableTime(job.WatermarkAt),
	)
	if err != nil {
		return models.CommitSnapshotCheckpointResult{}, fmt.Errorf("commit snapshot checkpoint: %w", err)
	}

	if tag.RowsAffected() == 1 {
		return models.CommitSnapshotCheckpointResult{Updated: true}, nil
	}

	return models.CommitSnapshotCheckpointResult{
		Updated: false,
		Reason:  models.CommitSkipReasonSnapshotDone,
	}, nil
}

// MarkStateSnapshotFinished marks a snapshot job as completed.
func (r *Repository) MarkStateSnapshotFinished(ctx context.Context, peerRegion, table string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = $3, finished_at = NOW()
		WHERE peer_region = $1 AND table_name = $2
	`, r.stateTable("snapshot_jobs"))

	_, err := r.exec(ctx, query, peerRegion, table, models.SnapshotJobStatusFinished)
	if err != nil {
		return fmt.Errorf("mark snapshot finished: %w", err)
	}

	return nil
}

// DeleteStateSnapshotJob removes persisted snapshot state for a peer and table.
func (r *Repository) DeleteStateSnapshotJob(ctx context.Context, peerRegion, table string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE peer_region = $1 AND table_name = $2
	`, r.stateTable("snapshot_jobs"))

	_, err := r.exec(ctx, query, peerRegion, table)
	if err != nil {
		return fmt.Errorf("delete snapshot job: %w", err)
	}

	return nil
}

// ListPendingStateSnapshotJobs returns unfinished snapshot jobs.
func (r *Repository) ListPendingStateSnapshotJobs(ctx context.Context) ([]models.StateSnapshotJob, error) {
	query := fmt.Sprintf(`
		SELECT peer_region, table_name, status, last_primary_key, watermark_at, finished_at IS NOT NULL
		FROM %s
		WHERE finished_at IS NULL
		ORDER BY peer_region, table_name
	`, r.stateTable("snapshot_jobs"))

	rows, err := r.query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list pending snapshot jobs: %w", err)
	}
	defer rows.Close()

	var jobs []models.StateSnapshotJob
	for rows.Next() {
		var job models.StateSnapshotJob
		var lastPrimaryKey []byte
		var watermarkAt *time.Time
		if err := rows.Scan(
			&job.PeerRegion,
			&job.Table,
			&job.Status,
			&lastPrimaryKey,
			&watermarkAt,
			&job.Finished,
		); err != nil {
			return nil, fmt.Errorf("scan snapshot job: %w", err)
		}
		job.LastPrimaryKey, err = models.DecodePrimaryKey(lastPrimaryKey)
		if err != nil {
			return nil, fmt.Errorf("decode pending snapshot job primary key: %w", err)
		}
		if watermarkAt != nil {
			job.WatermarkAt = watermarkAt.UTC()
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot jobs: %w", err)
	}

	return jobs, nil
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}
