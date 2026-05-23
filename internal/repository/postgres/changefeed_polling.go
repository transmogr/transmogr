package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/transmogr/transmogr/internal/models"
)

const defaultPollBatchSize = 1000

// ChangefeedPollingCurrentMaxVersion returns the current maximum polling version for a table.
func (r *Repository) ChangefeedPollingCurrentMaxVersion(
	ctx context.Context,
	spec models.TableSpec,
	localRegion string,
) (int64, error) {
	tableRef, err := quoteTableName(spec.Name)
	if err != nil {
		return 0, err
	}

	quotedUpdatedAt := quoteIdentifier(spec.UpdatedAtField)
	versionExpr := fmt.Sprintf(`(extract(epoch from t.%s) * 1000000)::bigint`, quotedUpdatedAt)
	query := fmt.Sprintf(`SELECT COALESCE(MAX(%s), 0) FROM %s AS t`, versionExpr, tableRef)

	var args []any
	if spec.RegionField != "" && localRegion != "" {
		query += fmt.Sprintf(` WHERE t.%s = $1`, quoteIdentifier(spec.RegionField))
		args = append(args, localRegion)
	}

	var maxVersion int64
	if err := r.queryRow(ctx, query, args...).Scan(&maxVersion); err != nil {
		return 0, fmt.Errorf("query max version for %s: %w", spec.Name, err)
	}

	return maxVersion, nil
}

// ChangefeedPollingListTableChanges loads one page of polling changes after the provided cursor.
func (r *Repository) ChangefeedPollingListTableChanges(
	ctx context.Context,
	spec models.TableSpec,
	localRegion string,
	afterVersion int64,
	afterPrimaryKey []models.PrimaryKeyPart,
) ([]models.ChangefeedChange, error) {
	tableRef, err := quoteTableName(spec.Name)
	if err != nil {
		return nil, err
	}

	pkTypes, err := r.ChangefeedCapturePrimaryKeyTypes(ctx, spec.Name)
	if err != nil {
		return nil, err
	}

	quotedUpdatedAt := quoteIdentifier(spec.UpdatedAtField)
	versionExpr := fmt.Sprintf(`(extract(epoch from t.%s) * 1000000)::bigint`, quotedUpdatedAt)

	var query string
	var queryArgs []any
	var primaryKeyJSON []byte
	if len(afterPrimaryKey) > 0 {
		primaryKeyJSON, err = primaryKeyPayload(afterPrimaryKey)
		if err != nil {
			return nil, err
		}
	}
	if spec.RegionField != "" {
		query = fmt.Sprintf(`
			SELECT
				%s AS version,
				to_jsonb(t) AS payload
			FROM %s AS t
		`, versionExpr, tableRef)
		if len(primaryKeyJSON) > 0 {
			query += fmt.Sprintf(
				`, json_populate_record(NULL::%s, $2::json) AS cursor
				WHERE
					t.%s = $3
					AND (
						%s > $1
						OR (%s = $1 AND %s > %s)
					)
				ORDER BY %s ASC, %s
				LIMIT $4
			`,
				tableRef,
				quoteIdentifier(spec.RegionField),
				versionExpr,
				versionExpr,
				tupleExpression("t", spec.PrimaryKey, quoteIdentifier),
				tupleExpression("cursor", spec.PrimaryKey, quoteIdentifier),
				versionExpr,
				orderByExpression("t", spec.PrimaryKey, quoteIdentifier),
			)
			queryArgs = []any{afterVersion, primaryKeyJSON, localRegion, defaultPollBatchSize}
		} else {
			query += fmt.Sprintf(`
				WHERE
					t.%s = $2
					AND %s > $1
				ORDER BY %s ASC, %s
				LIMIT $3
			`,
				quoteIdentifier(spec.RegionField),
				versionExpr,
				versionExpr,
				orderByExpression("t", spec.PrimaryKey, quoteIdentifier),
			)
			queryArgs = []any{afterVersion, localRegion, defaultPollBatchSize}
		}
	} else {
		query = fmt.Sprintf(`
			SELECT
				%s AS version,
				to_jsonb(t) AS payload
			FROM %s AS t
		`, versionExpr, tableRef)
		if len(primaryKeyJSON) > 0 {
			query += fmt.Sprintf(
				`, json_populate_record(NULL::%s, $2::json) AS cursor
				WHERE
					%s > $1
					OR (%s = $1 AND %s > %s)
				ORDER BY %s ASC, %s
				LIMIT $3
			`,
				tableRef,
				versionExpr,
				versionExpr,
				tupleExpression("t", spec.PrimaryKey, quoteIdentifier),
				tupleExpression("cursor", spec.PrimaryKey, quoteIdentifier),
				versionExpr,
				orderByExpression("t", spec.PrimaryKey, quoteIdentifier),
			)
			queryArgs = []any{afterVersion, primaryKeyJSON, defaultPollBatchSize}
		} else {
			query += fmt.Sprintf(`
				WHERE %s > $1
				ORDER BY %s ASC, %s
				LIMIT $2
			`,
				versionExpr,
				versionExpr,
				orderByExpression("t", spec.PrimaryKey, quoteIdentifier),
			)
			queryArgs = []any{afterVersion, defaultPollBatchSize}
		}
	}

	rows, err := r.query(ctx, query, queryArgs...)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, nil
		}
		return nil, fmt.Errorf("poll table %s: %w", spec.Name, err)
	}
	defer rows.Close()

	changes := make([]models.ChangefeedChange, 0, defaultPollBatchSize)
	for rows.Next() {
		var version int64
		var payload []byte
		if err := rows.Scan(&version, &payload); err != nil {
			return nil, fmt.Errorf("scan polling row for %s: %w", spec.Name, err)
		}
		primaryKey, err := models.PrimaryKeyPartsFromPayload(payload, spec.PrimaryKey, pkTypes)
		if err != nil {
			return nil, fmt.Errorf("extract polling primary key for %s: %w", spec.Name, err)
		}

		changes = append(changes, models.ChangefeedChange{
			Table:      spec.Name,
			PrimaryKey: primaryKey,
			Operation:  models.ReplicationOperationUpsert,
			Version:    version,
			Payload:    payload,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate polling rows for %s: %w", spec.Name, err)
	}

	return changes, nil
}
