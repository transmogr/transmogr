package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/transmogr/transmogr/internal/models"
)

func primaryKeyPayload(parts []models.PrimaryKeyPart) ([]byte, error) {
	data, err := json.Marshal(models.PrimaryKeyMap(parts))
	if err != nil {
		return nil, fmt.Errorf("marshal primary key payload: %w", err)
	}

	return data, nil
}

func tupleExpression(alias string, columns []string, quoteIdentifier func(string) string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, alias+"."+quoteIdentifier(column))
	}

	return "(" + strings.Join(quoted, ", ") + ")"
}

func orderByExpression(alias string, columns []string, quoteIdentifier func(string) string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, alias+"."+quoteIdentifier(column)+" ASC")
	}

	return strings.Join(quoted, ", ")
}

func conflictTarget(columns []string, quoteIdentifier func(string) string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, quoteIdentifier(column))
	}

	return strings.Join(quoted, ", ")
}

func normalizePrimaryKeyType(pgType string) models.PrimaryKeyType {
	switch pgType {
	case "int2", "int4", "int8":
		return models.PrimaryKeyTypeInteger
	case "bool":
		return models.PrimaryKeyTypeBoolean
	default:
		return models.PrimaryKeyTypeString
	}
}

func queryPrimaryKeyTypes(
	ctx context.Context,
	query func(context.Context, string, ...any) (pgx.Rows, error),
	spec models.TableSpec,
) (map[string]models.PrimaryKeyType, error) {
	schemaName := "public"
	tableName := spec.Name
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		schemaName = strings.TrimSpace(parts[0])
		tableName = strings.TrimSpace(parts[1])
	}

	rows, err := query(ctx, `
		SELECT a.attname, t.typname
		FROM pg_attribute AS a
		JOIN pg_class AS c ON c.oid = a.attrelid
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		JOIN pg_type AS t ON t.oid = a.atttypid
		WHERE n.nspname = $1
		  AND c.relname = $2
		  AND a.attname = ANY($3)
		  AND a.attnum > 0
		  AND NOT a.attisdropped
	`, schemaName, tableName, spec.PrimaryKey)
	if err != nil {
		return nil, fmt.Errorf("load primary key types for %s: %w", spec.Name, err)
	}
	defer rows.Close()

	types := make(map[string]models.PrimaryKeyType, len(spec.PrimaryKey))
	for rows.Next() {
		var column string
		var pgType string
		if err := rows.Scan(&column, &pgType); err != nil {
			return nil, fmt.Errorf("scan primary key type for %s: %w", spec.Name, err)
		}
		types[column] = normalizePrimaryKeyType(pgType)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate primary key types for %s: %w", spec.Name, err)
	}
	for _, column := range spec.PrimaryKey {
		if _, ok := types[column]; !ok {
			return nil, fmt.Errorf("primary key column %q type not found for %s", column, spec.Name)
		}
	}

	return types, nil
}
