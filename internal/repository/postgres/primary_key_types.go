package postgres

import (
	"context"
	"fmt"

	"github.com/transmogr/transmogr/internal/models"
)

func (r *Repository) primaryKeyTypes(ctx context.Context, table string) (map[string]models.PrimaryKeyType, error) {
	r.pkTypesMu.RLock()
	if cached, ok := r.pkTypes[table]; ok {
		r.pkTypesMu.RUnlock()
		return cached, nil
	}
	r.pkTypesMu.RUnlock()

	spec, ok := r.tableByName[table]
	if !ok {
		return nil, fmt.Errorf("table %q is not configured", table)
	}

	types, err := queryPrimaryKeyTypes(ctx, r.query, spec)
	if err != nil {
		return nil, err
	}

	r.pkTypesMu.Lock()
	r.pkTypes[table] = types
	r.pkTypesMu.Unlock()

	return types, nil
}
