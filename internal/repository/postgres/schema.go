package postgres

import (
	"context"
	"fmt"

	"github.com/transmogr/transmogr/internal/models"
)

// LoadTableColumns queries information_schema.columns for each requested table and returns
// a map of table name → ordered column list. Tables not found in the schema are returned
// with an empty slice rather than an error.
func (r *Repository) LoadTableColumns(
	ctx context.Context,
	tableNames []string,
) (map[string][]models.ColumnInfo, error) {
	if len(tableNames) == 0 {
		return map[string][]models.ColumnInfo{}, nil
	}

	const q = `
		SELECT table_name, column_name, data_type
		FROM information_schema.columns
		WHERE table_name = ANY($1)
		ORDER BY table_name, ordinal_position`

	rows, err := r.query(ctx, q, tableNames)
	if err != nil {
		return nil, fmt.Errorf("query table columns: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]models.ColumnInfo, len(tableNames))
	for rows.Next() {
		var tableName, columnName, dataType string
		if err := rows.Scan(&tableName, &columnName, &dataType); err != nil {
			return nil, fmt.Errorf("scan column row: %w", err)
		}
		result[tableName] = append(result[tableName], models.ColumnInfo{
			Name:     columnName,
			DataType: dataType,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column rows: %w", err)
	}

	return result, nil
}
