package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/transmogr/transmogr/internal/models"
)

// TryStateAcquireLease acquires a lease if it is free, expired, or already owned by this instance.
func (r *Repository) TryStateAcquireLease(
	ctx context.Context,
	kind string,
	region string,
	peerRegion string,
	table string,
	ownerID string,
	ttl time.Duration,
) (models.Lease, bool, error) {
	expiresAt, err := leaseExpiry(ttl)
	if err != nil {
		return models.Lease{}, false, err
	}

	query := fmt.Sprintf(`
		INSERT INTO %s AS l (
			lease_kind,
			region,
			peer_region,
			table_name,
			owner_id,
			generation,
			expires_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, 1, $6, NOW())
		ON CONFLICT (lease_kind, region, peer_region, table_scope) DO UPDATE SET
			owner_id = EXCLUDED.owner_id,
			generation = CASE
				WHEN l.owner_id = EXCLUDED.owner_id THEN l.generation
				ELSE l.generation + 1
			END,
			expires_at = EXCLUDED.expires_at,
			updated_at = NOW()
		WHERE l.expires_at <= NOW() OR l.owner_id = EXCLUDED.owner_id
		RETURNING lease_kind, region, peer_region, COALESCE(table_name, ''), owner_id, generation, expires_at, updated_at
	`, r.stateTable("leases"))

	var lease models.Lease
	if err := r.queryRow(ctx, query, kind, region, peerRegion, nullableText(table), ownerID, expiresAt).Scan(
		&lease.Kind,
		&lease.Region,
		&lease.PeerRegion,
		&lease.Table,
		&lease.OwnerID,
		&lease.Generation,
		&lease.ExpiresAt,
		&lease.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Lease{}, false, nil
		}

		return models.Lease{}, false, fmt.Errorf("try acquire lease %s/%s/%s: %w", kind, region, peerRegion, err)
	}

	return lease, true, nil
}

// RenewStateLease extends an existing lease if this instance still owns it.
func (r *Repository) RenewStateLease(
	ctx context.Context,
	kind string,
	region string,
	peerRegion string,
	table string,
	ownerID string,
	generation int64,
	ttl time.Duration,
) (models.Lease, bool, error) {
	expiresAt, err := leaseExpiry(ttl)
	if err != nil {
		return models.Lease{}, false, err
	}

	query := fmt.Sprintf(`
		UPDATE %s
		SET expires_at = $7, updated_at = NOW()
		WHERE lease_kind = $1
		  AND region = $2
		  AND peer_region = $3
		  AND table_name IS NOT DISTINCT FROM $4
		  AND owner_id = $5
		  AND generation = $6
		  AND expires_at > NOW()
		RETURNING lease_kind, region, peer_region, COALESCE(table_name, ''), owner_id, generation, expires_at, updated_at
	`, r.stateTable("leases"))

	var lease models.Lease
	if err := r.queryRow(ctx, query, kind, region, peerRegion, nullableText(table), ownerID, generation, expiresAt).Scan(
		&lease.Kind,
		&lease.Region,
		&lease.PeerRegion,
		&lease.Table,
		&lease.OwnerID,
		&lease.Generation,
		&lease.ExpiresAt,
		&lease.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Lease{}, false, nil
		}

		return models.Lease{}, false, fmt.Errorf("renew lease %s/%s/%s: %w", kind, region, peerRegion, err)
	}

	return lease, true, nil
}

// ReleaseStateLease deletes a lease if it is still owned by this instance.
func (r *Repository) ReleaseStateLease(
	ctx context.Context,
	kind string,
	region string,
	peerRegion string,
	table string,
	ownerID string,
	generation int64,
) (bool, error) {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE lease_kind = $1
		  AND region = $2
		  AND peer_region = $3
		  AND table_name IS NOT DISTINCT FROM $4
		  AND owner_id = $5
		  AND generation = $6
	`, r.stateTable("leases"))

	tag, err := r.exec(ctx, query, kind, region, peerRegion, nullableText(table), ownerID, generation)
	if err != nil {
		return false, fmt.Errorf("release lease %s/%s/%s: %w", kind, region, peerRegion, err)
	}

	return tag.RowsAffected() > 0, nil
}

// GetStateLease returns the current lease row for a given scope.
func (r *Repository) GetStateLease(ctx context.Context, kind, region, peerRegion, table string) (models.Lease, error) {
	query := fmt.Sprintf(`
		SELECT lease_kind, region, peer_region, COALESCE(table_name, ''), owner_id, generation, expires_at, updated_at
		FROM %s
		WHERE lease_kind = $1
		  AND region = $2
		  AND peer_region = $3
		  AND table_name IS NOT DISTINCT FROM $4
	`, r.stateTable("leases"))

	var lease models.Lease
	if err := r.queryRow(ctx, query, kind, region, peerRegion, nullableText(table)).Scan(
		&lease.Kind,
		&lease.Region,
		&lease.PeerRegion,
		&lease.Table,
		&lease.OwnerID,
		&lease.Generation,
		&lease.ExpiresAt,
		&lease.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Lease{}, nil
		}

		return models.Lease{}, fmt.Errorf("get lease %s/%s/%s: %w", kind, region, peerRegion, err)
	}

	return lease, nil
}

// CleanupExpiredStateLeases removes lease rows whose expiry has passed.
func (r *Repository) CleanupExpiredStateLeases(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE expires_at <= NOW()
	`, r.stateTable("leases"))

	tag, err := r.exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("cleanup expired leases: %w", err)
	}

	return tag.RowsAffected(), nil
}

// RunExpiredStateLeaseCleanup periodically removes expired lease rows.
func (r *Repository) RunExpiredStateLeaseCleanup(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("lease cleanup interval must be positive")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := r.CleanupExpiredStateLeases(ctx); err != nil && ctx.Err() == nil {
				return err
			}
		}
	}
}

func leaseExpiry(ttl time.Duration) (time.Time, error) {
	if ttl <= 0 {
		return time.Time{}, errors.New("lease ttl must be positive")
	}

	return time.Now().UTC().Add(ttl), nil
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}

	return value
}
