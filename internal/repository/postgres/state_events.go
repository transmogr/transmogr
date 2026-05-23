package postgres

import (
	"context"

	"github.com/transmogr/transmogr/internal/models"
)

// CommitStateAppliedTransaction atomically applies all events in a CDC transaction within a single DB transaction.
func (r *Repository) CommitStateAppliedTransaction(
	ctx context.Context,
	events []models.ReplicationEvent,
	mode models.ReplicationApplyMode,
) error {
	return r.WithTx(ctx, func(ctx context.Context, repo *Repository) error {
		// Disable all table triggers for the duration of this transaction so that
		// cascaded parent-before-child DELETE order from the source WAL does not
		// violate FK constraints on replicas that lack CASCADE. This also
		// suppresses user-defined triggers on the target tables while applying
		// replicated changes.
		if _, err := repo.exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
			return err
		}

		type cursorKey struct{ peerRegion, table string }

		// Lock each unique (peerRegion, table) cursor exactly once instead of per event.
		lockedCursors := make(map[cursorKey]models.StateCursor)
		for _, event := range events {
			key := cursorKey{event.PeerRegion, event.Table}
			if _, ok := lockedCursors[key]; !ok {
				cursor, err := repo.getStateCursorForUpdate(ctx, event.PeerRegion, event.Table)
				if err != nil {
					return err
				}
				lockedCursors[key] = cursor
			}
		}

		// Filter stale events, tracking the running cursor position in memory.
		runningCursors := make(map[cursorKey]models.StateCursor, len(lockedCursors))
		for k, c := range lockedCursors {
			runningCursors[k] = c
		}
		finalCursors := make(map[cursorKey]models.StateCursor)
		toApply := make([]models.ReplicationEvent, 0, len(events))

		for _, event := range events {
			key := cursorKey{event.PeerRegion, event.Table}
			if isStaleCursorPosition(event.Version, event.PrimaryKey, runningCursors[key]) {
				continue
			}
			toApply = append(toApply, event)
			newCursor := models.StateCursor{
				PeerRegion:     event.PeerRegion,
				Table:          event.Table,
				LastEventID:    event.EventID,
				LastVersion:    event.Version,
				LastPrimaryKey: models.ClonePrimaryKeyParts(event.PrimaryKey),
			}
			runningCursors[key] = newCursor
			finalCursors[key] = newCursor
		}

		// Apply all non-stale events in batch.
		if err := repo.applyEventsBatch(ctx, toApply, mode); err != nil {
			return err
		}

		// Save the final cursor once per (peerRegion, table).
		for _, cursor := range finalCursors {
			if err := repo.SaveStateCursor(ctx, cursor); err != nil {
				return err
			}
		}

		return nil
	})
}

func isStaleCursorPosition(version int64, primaryKey []models.PrimaryKeyPart, cursor models.StateCursor) bool {
	if version < cursor.LastVersion {
		return true
	}
	if version > cursor.LastVersion {
		return false
	}

	return models.ComparePrimaryKeys(primaryKey, cursor.LastPrimaryKey) <= 0
}
