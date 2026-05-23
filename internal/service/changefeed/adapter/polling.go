package adapter

import (
	"context"
	"sort"
	"sync"
	"time"

	repositorypostgres "github.com/transmogr/transmogr/internal/repository/postgres"
	changefeedservice "github.com/transmogr/transmogr/internal/service/changefeed"

	"github.com/transmogr/transmogr/internal/models"
)

type pollingRepository interface {
	ChangefeedPollingCurrentMaxVersion(ctx context.Context, spec models.TableSpec, localRegion string) (int64, error)
	ChangefeedPollingListTableChanges(
		ctx context.Context,
		spec models.TableSpec,
		localRegion string,
		afterVersion int64,
		afterPrimaryKey []models.PrimaryKeyPart,
	) ([]models.ChangefeedChange, error)
}

type pollCursor struct {
	version    int64
	primaryKey []models.PrimaryKeyPart
}

// Polling implements xmin-based polling change capture for PostgreSQL tables.
type Polling struct {
	localRegion  string
	tableByName  map[string]models.TableSpec
	pollInterval time.Duration
	repo         pollingRepository
	cursorMu     sync.RWMutex
	cursors      map[string]pollCursor
}

// NewPolling creates a polling adapter backed by a PostgreSQL repository.
func NewPolling(
	localRegion string,
	repo *repositorypostgres.Repository,
	tables []models.TableSpec,
	pollInterval time.Duration,
) *Polling {
	tableByName := make(map[string]models.TableSpec, len(tables))
	for _, table := range tables {
		tableByName[table.Name] = table
	}
	return &Polling{
		localRegion:  localRegion,
		tableByName:  tableByName,
		pollInterval: pollInterval,
		repo:         repo,
		cursors:      make(map[string]pollCursor, len(tables)),
	}
}

// Run polls each configured table and emits change batches in cursor order.
func (a *Polling) Run(ctx context.Context, onBatch func(context.Context, models.ChangefeedBatch) error) error {
	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	for {
		for _, table := range a.tables() {
			if err := a.pollTable(ctx, table, onBatch); err != nil {
				return err
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// RestoreCursors restores or initializes the durable cursor for each table.
func (a *Polling) RestoreCursors(
	ctx context.Context,
	load func(table string) (models.ChangefeedCursor, error),
) error {
	for _, table := range a.tables() {
		cursor, err := load(table.Name)
		if err != nil {
			return err
		}

		version := cursor.LastVersion
		primaryKey := cursor.LastPrimaryKey
		if version == 0 {
			version, err = a.repo.ChangefeedPollingCurrentMaxVersion(ctx, table, a.localRegion)
			if err != nil {
				return err
			}
			primaryKey = nil
		}

		a.setCursor(table.Name, version, primaryKey)
	}

	return nil
}

// CursorTables returns the table names tracked by this polling adapter.
func (a *Polling) CursorTables() []string {
	tables := a.tables()
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

func (a *Polling) pollTable(
	ctx context.Context,
	spec models.TableSpec,
	onBatch func(context.Context, models.ChangefeedBatch) error,
) error {
	cursor := a.cursor(spec.Name)

	for {
		changes, err := a.repo.ChangefeedPollingListTableChanges(ctx, spec, a.localRegion, cursor.version, cursor.primaryKey)
		if err != nil {
			return err
		}

		for _, change := range changes {
			cursor.version = change.Version
			cursor.primaryKey = change.PrimaryKey
		}

		if len(changes) > 0 {
			if err := onBatch(ctx, models.ChangefeedBatch{Changes: changes}); err != nil {
				return err
			}
		}

		a.setCursor(spec.Name, cursor.version, cursor.primaryKey)
		if len(changes) < 1000 {
			return nil
		}
	}
}

func (a *Polling) cursor(table string) pollCursor {
	a.cursorMu.RLock()
	defer a.cursorMu.RUnlock()

	cursor := a.cursors[table]
	return pollCursor{
		version:    cursor.version,
		primaryKey: models.ClonePrimaryKeyParts(cursor.primaryKey),
	}
}

func (a *Polling) setCursor(table string, version int64, primaryKey []models.PrimaryKeyPart) {
	a.cursorMu.Lock()
	defer a.cursorMu.Unlock()

	a.cursors[table] = pollCursor{
		version:    version,
		primaryKey: models.ClonePrimaryKeyParts(primaryKey),
	}
}

func (a *Polling) tables() []models.TableSpec {
	names := make([]string, 0, len(a.tableByName))
	for name := range a.tableByName {
		names = append(names, name)
	}
	sort.Strings(names)

	tables := make([]models.TableSpec, 0, len(names))
	for _, name := range names {
		tables = append(tables, a.tableByName[name])
	}

	return tables
}

var _ changefeedservice.Adapter = (*Polling)(nil)
