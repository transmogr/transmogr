// Package adapter provides changefeed adapters for PostgreSQL polling and CDC.
package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	repositorypostgres "github.com/transmogr/transmogr/internal/repository/postgres"
	changefeedservice "github.com/transmogr/transmogr/internal/service/changefeed"

	"github.com/transmogr/transmogr/internal/models"
)

type captureRepository interface {
	ChangefeedCaptureEnsurePublication(ctx context.Context, publication string, tables []models.TableSpec) error
	ChangefeedCaptureEnsureReplicaIdentityFull(ctx context.Context, tables []models.TableSpec) error
	ChangefeedCaptureEnsureReplicationSlot(ctx context.Context, slot string) (pglogrepl.LSN, error)
	ChangefeedCapturePrimaryKeyTypes(ctx context.Context, table string) (map[string]models.PrimaryKeyType, error)
}

// ChangeDataCapture implements PostgreSQL logical replication capture.
type ChangeDataCapture struct {
	localRegion    string
	dsn            string
	pool           *pgxpool.Pool
	tableByName    map[string]models.TableSpec
	publication    string
	slot           string
	statusInterval time.Duration
	updateDiff     bool
	repo           captureRepository
}

// NewChangeDataCapture creates a PostgreSQL logical replication adapter.
func NewChangeDataCapture(
	localRegion string,
	dsn string,
	pool *pgxpool.Pool,
	repo *repositorypostgres.Repository,
	tables []models.TableSpec,
	publication string,
	slot string,
	statusInterval time.Duration,
	updateDiff bool,
) *ChangeDataCapture {
	tableByName := make(map[string]models.TableSpec, len(tables))
	for _, table := range tables {
		tableByName[table.Name] = table
	}
	return &ChangeDataCapture{
		localRegion:    localRegion,
		dsn:            dsn,
		pool:           pool,
		tableByName:    tableByName,
		publication:    publication,
		slot:           slot,
		statusInterval: statusInterval,
		updateDiff:     updateDiff,
		repo:           repo,
	}
}

// Run streams CDC batches from PostgreSQL logical replication.
func (a *ChangeDataCapture) Run(
	ctx context.Context,
	onBatch func(context.Context, models.ChangefeedBatch) error,
) error {
	if err := a.repo.ChangefeedCaptureEnsurePublication(ctx, a.publication, a.tables()); err != nil {
		return err
	}
	if a.updateDiff {
		if err := a.repo.ChangefeedCaptureEnsureReplicaIdentityFull(ctx, a.tables()); err != nil {
			return err
		}
	}

	conn, err := a.openReplicationConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("identify system: %w", err)
	}

	slotLSN, err := a.repo.ChangefeedCaptureEnsureReplicationSlot(ctx, a.slot)
	if err != nil {
		return err
	}
	startLSN := slotLSN
	if startLSN == 0 {
		startLSN = sysident.XLogPos
	}

	err = pglogrepl.StartReplication(
		ctx,
		conn,
		a.slot,
		startLSN,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", strings.ReplaceAll(a.publication, "'", "''")),
			},
		},
	)
	if err != nil {
		return fmt.Errorf("start logical replication: %w", err)
	}

	relations := make(map[uint32]postgresRelation)
	relationIDByTable := make(map[string]uint32, len(a.tableByName))
	durableLSN := startLSN
	lastStatus := time.Now().UTC()
	var txChanges []models.ChangefeedChange
	var txID string

	for {
		if time.Since(lastStatus) >= a.statusInterval {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: durableLSN,
				WALFlushPosition: durableLSN,
				WALApplyPosition: durableLSN,
				ClientTime:       time.Now(),
			}); err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			lastStatus = time.Now().UTC()
		}

		receiveCtx, cancel := context.WithDeadline(ctx, time.Now().Add(a.statusInterval))
		msg, err := conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if pgconnTimeout(err) {
				continue
			}
			return fmt.Errorf("receive replication message: %w", err)
		}

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse primary keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				lastStatus = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}
			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("parse logical replication message: %w", err)
			}
			nextLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))

			switch message := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				relation, ok, err := a.resolveRelation(ctx, message)
				if err != nil {
					return err
				}
				if ok {
					if previousID, exists := relationIDByTable[relation.tableName]; exists && previousID != message.RelationID {
						delete(relations, previousID)
					}
					relations[message.RelationID] = relation
					relationIDByTable[relation.tableName] = message.RelationID
				}
			case *pglogrepl.InsertMessage:
				change, ok, err := a.buildInsertChange(relations, message.RelationID, message.Tuple, nextLSN)
				if err != nil {
					return err
				}
				if ok {
					txChanges = append(txChanges, change)
				}
			case *pglogrepl.UpdateMessage:
				change, ok, err := a.buildUpdateChange(relations, message.RelationID, message.OldTuple, message.NewTuple, nextLSN)
				if err != nil {
					return err
				}
				if ok {
					txChanges = append(txChanges, change)
				}
			case *pglogrepl.DeleteMessage:
				change, ok, err := a.buildDeleteChange(relations, message.RelationID, message.OldTuple, nextLSN)
				if err != nil {
					return err
				}
				if ok {
					txChanges = append(txChanges, change)
				}
			case *pglogrepl.BeginMessage:
				txChanges = txChanges[:0]
				txID = message.FinalLSN.String()
			case *pglogrepl.CommitMessage:
				if len(txChanges) > 0 {
					if err := onBatch(ctx, models.ChangefeedBatch{TxID: txID, Changes: txChanges}); err != nil {
						return err
					}
				}
				txChanges = txChanges[:0]
				txID = ""
				durableLSN = nextLSN
			}
		}
	}
}

// RestoreCursors is a no-op for CDC because PostgreSQL replication slots
// already persist the durable WAL position.
func (a *ChangeDataCapture) RestoreCursors(
	context.Context,
	func(table string) (models.ChangefeedCursor, error),
) error {
	return nil
}

// CursorTables returns the table names tracked by this capture adapter.
func (a *ChangeDataCapture) CursorTables() []string {
	tables := a.tables()
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

func (a *ChangeDataCapture) buildInsertChange(
	relations map[uint32]postgresRelation,
	relationID uint32,
	tuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
) (models.ChangefeedChange, bool, error) {
	return a.buildWriteChange(relations, relationID, tuple, lsn, models.ReplicationOperationInsert)
}

func (a *ChangeDataCapture) buildUpdateChange(
	relations map[uint32]postgresRelation,
	relationID uint32,
	oldTuple, newTuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
) (models.ChangefeedChange, bool, error) {
	if a.updateDiff {
		return a.buildUpdateDiffChange(relations, relationID, oldTuple, newTuple, lsn)
	}
	return a.buildWriteChange(relations, relationID, newTuple, lsn, models.ReplicationOperationUpdate)
}

func (a *ChangeDataCapture) buildUpdateDiffChange(
	relations map[uint32]postgresRelation,
	relationID uint32,
	oldTuple, newTuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
) (models.ChangefeedChange, bool, error) {
	relation, ok := relations[relationID]
	if !ok {
		return models.ChangefeedChange{}, false, nil
	}
	if !tupleRegionIsLocal(relation, newTuple, a.localRegion) {
		return models.ChangefeedChange{}, false, nil
	}
	if oldTuple == nil {
		return models.ChangefeedChange{}, false, fmt.Errorf(
			"cdc update diff for %s: old tuple is nil; REPLICA IDENTITY FULL is required",
			relation.tableName,
		)
	}
	primaryKey, err := relation.ExtractPrimaryKey(newTuple)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	version, err := lsnToVersion(lsn)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	payload, err := buildUpdateDiffPayload(relation, oldTuple, newTuple)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	return models.ChangefeedChange{
		Table:      relation.Table(),
		PrimaryKey: primaryKey,
		Operation:  models.ReplicationOperationUpdate,
		Version:    version,
		Payload:    payload,
	}, true, nil
}

func buildUpdateDiffPayload(relation postgresRelation, oldTuple, newTuple *pglogrepl.TupleData) ([]byte, error) {
	payload := make(map[string]json.RawMessage)
	for colName, colIdx := range relation.columnByName {
		if colIdx >= len(newTuple.Columns) {
			continue
		}
		newCol := newTuple.Columns[colIdx]
		isPK := slices.Contains(relation.primaryKey, colName)
		if isPK {
			raw, err := tupleColumnToJSON(newCol)
			if err != nil {
				return nil, fmt.Errorf("encode pk column %q for %s: %w", colName, relation.tableName, err)
			}
			payload[colName] = raw
			continue
		}
		if newCol.DataType == pglogrepl.TupleDataTypeToast {
			continue
		}
		if colIdx < len(oldTuple.Columns) {
			oldCol := oldTuple.Columns[colIdx]
			if newCol.DataType == oldCol.DataType && string(newCol.Data) == string(oldCol.Data) {
				continue
			}
		}
		raw, err := tupleColumnToJSON(newCol)
		if err != nil {
			return nil, fmt.Errorf("encode column %q for %s: %w", colName, relation.tableName, err)
		}
		payload[colName] = raw
	}
	return json.Marshal(payload)
}

func tupleColumnToJSON(col *pglogrepl.TupleDataColumn) (json.RawMessage, error) {
	if col.DataType == pglogrepl.TupleDataTypeNull {
		return json.RawMessage("null"), nil
	}
	raw, err := json.Marshal(string(col.Data))
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (a *ChangeDataCapture) buildWriteChange(
	relations map[uint32]postgresRelation,
	relationID uint32,
	tuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
	operation models.ReplicationOperation,
) (models.ChangefeedChange, bool, error) {
	relation, ok := relations[relationID]
	if !ok {
		return models.ChangefeedChange{}, false, nil
	}
	if !tupleRegionIsLocal(relation, tuple, a.localRegion) {
		return models.ChangefeedChange{}, false, nil
	}
	primaryKey, err := relation.ExtractPrimaryKey(tuple)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	version, err := lsnToVersion(lsn)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	payload, err := buildTuplePayload(relation, tuple)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	return models.ChangefeedChange{
		Table:      relation.Table(),
		PrimaryKey: primaryKey,
		Operation:  operation,
		Version:    version,
		Payload:    payload,
	}, true, nil
}

func buildTuplePayload(relation postgresRelation, tuple *pglogrepl.TupleData) ([]byte, error) {
	if tuple == nil {
		return nil, fmt.Errorf("cdc tuple is required")
	}

	payload := make(map[string]json.RawMessage, len(relation.columnByName))
	for colName, colIdx := range relation.columnByName {
		if colIdx >= len(tuple.Columns) {
			continue
		}
		col := tuple.Columns[colIdx]
		if col.DataType == pglogrepl.TupleDataTypeToast {
			return nil, fmt.Errorf(
				"cdc tuple for %s contains unchanged TOAST value for column %q; "+
					"enable replication.source.cdc.update_diff to publish WAL-native updates",
				relation.tableName,
				colName,
			)
		}
		raw, err := tupleColumnToJSON(col)
		if err != nil {
			return nil, fmt.Errorf("encode column %q for %s: %w", colName, relation.tableName, err)
		}
		payload[colName] = raw
	}

	return json.Marshal(payload)
}

func tupleRegionIsLocal(relation postgresRelation, tuple *pglogrepl.TupleData, localRegion string) bool {
	if relation.regionField == "" || localRegion == "" || tuple == nil {
		return false
	}
	colIdx, ok := relation.columnByName[relation.regionField]
	if !ok || colIdx >= len(tuple.Columns) {
		return false
	}
	col := tuple.Columns[colIdx]
	return col.DataType == pglogrepl.TupleDataTypeText && string(col.Data) == localRegion
}

func deleteRegionIsLocal(relation postgresRelation, tuple *pglogrepl.TupleData, localRegion string) (bool, error) {
	if relation.regionField == "" || localRegion == "" {
		return false, fmt.Errorf(
			"delete event for %s: region field or local region is not configured",
			relation.tableName,
		)
	}
	if tuple == nil {
		return false, fmt.Errorf(
			"delete event for %s: old tuple is nil; REPLICA IDENTITY FULL is required",
			relation.tableName,
		)
	}
	colIdx, ok := relation.columnByName[relation.regionField]
	if !ok || colIdx >= len(tuple.Columns) {
		return false, fmt.Errorf(
			"delete event for %s: region column %q not found in tuple; REPLICA IDENTITY FULL is required",
			relation.tableName,
			relation.regionField,
		)
	}
	col := tuple.Columns[colIdx]
	if col.DataType != pglogrepl.TupleDataTypeText {
		return false, fmt.Errorf(
			"delete event for %s: region column %q has data type %q instead of text; "+
				"REPLICA IDENTITY FULL is required for region-filtered tables",
			relation.tableName,
			relation.regionField,
			string(col.DataType),
		)
	}
	return string(col.Data) == localRegion, nil
}

func (a *ChangeDataCapture) buildDeleteChange(
	relations map[uint32]postgresRelation,
	relationID uint32,
	tuple *pglogrepl.TupleData,
	lsn pglogrepl.LSN,
) (models.ChangefeedChange, bool, error) {
	relation, ok := relations[relationID]
	if !ok {
		return models.ChangefeedChange{}, false, nil
	}
	local, err := deleteRegionIsLocal(relation, tuple, a.localRegion)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	if !local {
		return models.ChangefeedChange{}, false, nil
	}
	primaryKey, err := relation.ExtractPrimaryKey(tuple)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	version, err := lsnToVersion(lsn)
	if err != nil {
		return models.ChangefeedChange{}, false, err
	}
	return models.ChangefeedChange{
		Table:      relation.Table(),
		PrimaryKey: primaryKey,
		Operation:  models.ReplicationOperationDelete,
		Version:    version,
	}, true, nil
}

func (a *ChangeDataCapture) openReplicationConn(ctx context.Context) (*pgconn.PgConn, error) {
	dsn, err := withReplicationMode(a.dsn)
	if err != nil {
		return nil, err
	}
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open replication connection: %w", err)
	}
	return conn, nil
}

func (a *ChangeDataCapture) resolveRelation(
	ctx context.Context,
	message *pglogrepl.RelationMessage,
) (postgresRelation, bool, error) {
	columnByName := make(map[string]int, len(message.Columns))
	for i, column := range message.Columns {
		columnByName[column.Name] = i
	}
	tableName := message.RelationName
	if message.Namespace != "" && message.Namespace != "public" {
		tableName = message.Namespace + "." + message.RelationName
	}
	spec, ok := a.tableByName[tableName]
	if !ok && message.Namespace == "public" {
		spec, ok = a.tableByName[message.RelationName]
		tableName = message.RelationName
	}
	if !ok {
		return postgresRelation{}, false, nil
	}
	pkTypes, err := a.repo.ChangefeedCapturePrimaryKeyTypes(ctx, tableName)
	if err != nil {
		return postgresRelation{}, false, fmt.Errorf("resolve relation primary key types for %s: %w", tableName, err)
	}
	return postgresRelation{
		tableName:       tableName,
		primaryKey:      spec.PrimaryKey,
		primaryKeyTypes: pkTypes,
		columnByName:    columnByName,
		regionField:     spec.RegionField,
	}, true, nil
}

type postgresRelation struct {
	tableName       string
	primaryKey      []string
	primaryKeyTypes map[string]models.PrimaryKeyType
	columnByName    map[string]int
	regionField     string
}

func (r postgresRelation) Table() string { return r.tableName }

func (r postgresRelation) ExtractPrimaryKey(tuple *pglogrepl.TupleData) ([]models.PrimaryKeyPart, error) {
	if tuple == nil {
		return nil, fmt.Errorf("cdc tuple is required")
	}
	parts := make([]models.PrimaryKeyPart, 0, len(r.primaryKey))
	for _, pkColumn := range r.primaryKey {
		index, ok := r.columnByName[pkColumn]
		if !ok || index >= len(tuple.Columns) {
			return nil, fmt.Errorf("cdc tuple for %s missing primary key column %q", r.tableName, pkColumn)
		}
		column := tuple.Columns[index]
		if len(column.Data) == 0 {
			return nil, fmt.Errorf("cdc tuple for %s missing primary key value for %q", r.tableName, pkColumn)
		}
		pkType := r.primaryKeyTypes[pkColumn]
		value, err := models.PrimaryKeyValueFromString(pkType, string(column.Data))
		if err != nil {
			return nil, fmt.Errorf("marshal cdc primary key for %s column %q: %w", r.tableName, pkColumn, err)
		}
		parts = append(parts, models.PrimaryKeyPart{Column: pkColumn, Type: pkType, Value: value})
	}
	return parts, nil
}

func lsnToVersion(lsn pglogrepl.LSN) (int64, error) {
	value := uint64(lsn)
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("lsn exceeds int64 version range: %s", lsn.String())
	}

	version, err := strconv.ParseInt(strconv.FormatUint(value, 10), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse lsn version %s: %w", lsn.String(), err)
	}
	return version, nil
}

func pgconnTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func withReplicationMode(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse replication dsn: %w", err)
	}
	query := parsed.Query()
	query.Set("replication", "database")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (a *ChangeDataCapture) tables() []models.TableSpec {
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

var _ changefeedservice.Adapter = (*ChangeDataCapture)(nil)
