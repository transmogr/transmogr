package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/transmogr/transmogr/internal/models"
)

// GetStateCursor loads the persisted cursor for a peer and table.
func (r *Repository) GetStateCursor(ctx context.Context, peerRegion, table string) (models.StateCursor, error) {
	return r.getCursor(ctx, "peer_cursors", peerRegion, table)
}

// GetChangefeedCursor loads the persisted changefeed cursor for a table.
func (r *Repository) GetChangefeedCursor(ctx context.Context, table string) (models.ChangefeedCursor, error) {
	query := fmt.Sprintf(`
		SELECT table_name, source, last_version, last_primary_key
		FROM %s
		WHERE table_name = $1
	`, r.stateTable("changefeed_cursors"))

	var cursor models.ChangefeedCursor
	var lastPrimaryKey []byte
	err := r.queryRow(ctx, query, table).Scan(
		&cursor.Table,
		&cursor.Source,
		&cursor.LastVersion,
		&lastPrimaryKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.ChangefeedCursor{Table: table}, nil
		}

		return models.ChangefeedCursor{}, fmt.Errorf("get changefeed cursor: %w", err)
	}
	cursor.LastPrimaryKey, err = models.DecodePrimaryKey(lastPrimaryKey)
	if err != nil {
		return models.ChangefeedCursor{}, fmt.Errorf("decode changefeed cursor primary key: %w", err)
	}

	return cursor, nil
}

func (r *Repository) getCursor(ctx context.Context, tableName, peerRegion, table string) (models.StateCursor, error) {
	query := fmt.Sprintf(`
		SELECT peer_region, table_name, last_event_id, last_version, last_primary_key
		FROM %s
		WHERE peer_region = $1 AND table_name = $2
	`, r.stateTable(tableName))

	var cursor models.StateCursor
	var lastPrimaryKey []byte
	err := r.queryRow(ctx, query, peerRegion, table).Scan(
		&cursor.PeerRegion,
		&cursor.Table,
		&cursor.LastEventID,
		&cursor.LastVersion,
		&lastPrimaryKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.StateCursor{
				PeerRegion: peerRegion,
				Table:      table,
			}, nil
		}

		return models.StateCursor{}, fmt.Errorf("get cursor: %w", err)
	}
	cursor.LastPrimaryKey, err = models.DecodePrimaryKey(lastPrimaryKey)
	if err != nil {
		return models.StateCursor{}, fmt.Errorf("decode cursor primary key: %w", err)
	}

	return cursor, nil
}

func (r *Repository) getStateCursorForUpdate(
	ctx context.Context,
	peerRegion, table string,
) (models.StateCursor, error) {
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			peer_region, table_name, last_event_id, last_version, last_primary_key, updated_at
		)
		VALUES ($1, $2, '', 0, NULL, NOW())
		ON CONFLICT (peer_region, table_name) DO NOTHING
	`, r.stateTable("peer_cursors"))

	if _, err := r.exec(ctx, insertQuery, peerRegion, table); err != nil {
		return models.StateCursor{}, fmt.Errorf("ensure cursor row: %w", err)
	}

	query := fmt.Sprintf(`
		SELECT peer_region, table_name, last_event_id, last_version, last_primary_key
		FROM %s
		WHERE peer_region = $1 AND table_name = $2
		FOR UPDATE
	`, r.stateTable("peer_cursors"))

	var cursor models.StateCursor
	var lastPrimaryKey []byte
	err := r.queryRow(ctx, query, peerRegion, table).Scan(
		&cursor.PeerRegion,
		&cursor.Table,
		&cursor.LastEventID,
		&cursor.LastVersion,
		&lastPrimaryKey,
	)
	if err != nil {
		return models.StateCursor{}, fmt.Errorf("get cursor for update: %w", err)
	}
	cursor.LastPrimaryKey, err = models.DecodePrimaryKey(lastPrimaryKey)
	if err != nil {
		return models.StateCursor{}, fmt.Errorf("decode cursor primary key for update: %w", err)
	}

	return cursor, nil
}

// SaveStateCursor upserts the cursor for a peer and table.
func (r *Repository) SaveStateCursor(ctx context.Context, cursor models.StateCursor) error {
	return r.saveCursor(ctx, "peer_cursors", cursor)
}

// SaveChangefeedCursor upserts the changefeed cursor for a table.
func (r *Repository) SaveChangefeedCursor(ctx context.Context, cursor models.ChangefeedCursor) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			table_name, source, last_version, last_primary_key, updated_at
		)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (table_name)
		DO UPDATE SET
			source = EXCLUDED.source,
			last_version = EXCLUDED.last_version,
			last_primary_key = EXCLUDED.last_primary_key,
			updated_at = NOW()
	`, r.stateTable("changefeed_cursors"))

	encodedPrimaryKey, err := models.EncodePrimaryKey(cursor.LastPrimaryKey)
	if err != nil {
		return fmt.Errorf("encode changefeed cursor primary key: %w", err)
	}

	_, err = r.exec(
		ctx,
		query,
		cursor.Table,
		cursor.Source,
		cursor.LastVersion,
		encodedPrimaryKey,
	)
	if err != nil {
		return fmt.Errorf("save changefeed cursor: %w", err)
	}

	return nil
}

func (r *Repository) saveCursor(ctx context.Context, tableName string, cursor models.StateCursor) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			peer_region, table_name, last_event_id, last_version, last_primary_key, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (peer_region, table_name)
		DO UPDATE SET
			last_event_id = EXCLUDED.last_event_id,
			last_version = EXCLUDED.last_version,
			last_primary_key = EXCLUDED.last_primary_key,
			updated_at = NOW()
	`, r.stateTable(tableName))

	encodedPrimaryKey, err := models.EncodePrimaryKey(cursor.LastPrimaryKey)
	if err != nil {
		return fmt.Errorf("encode cursor primary key: %w", err)
	}

	_, err = r.exec(
		ctx,
		query,
		cursor.PeerRegion,
		cursor.Table,
		cursor.LastEventID,
		cursor.LastVersion,
		encodedPrimaryKey,
	)
	if err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}

	return nil
}
