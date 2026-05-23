# State Storage

## Purpose

Transmogr state tables live in the schema configured by `db.postgres.state_schema` within the same PostgreSQL database as the application tables. The default schema is `transmogr`.
They store internal replication metadata required for reliability, replay, and resume behavior.
They are derived operational state, not the source of truth for replicated business rows.

Packages:

- `internal/repository/postgres`
- `pkg/migrations`

The state repository is initialized from the same `*pgxpool.Pool` as the application repository. Pool lifecycle is owned by the application.

## Tables

### `<state_schema>.peer_cursors`

Stores inbound replication position for `peer_region + table`.

Purpose:

- stale / replay guard on inbound apply
- last applied event tracking

Used by:

- `CommitStateAppliedTransaction`

Notes:

- `last_primary_key` is stored as `JSONB`
- it contains ordered primary-key parts, not an opaque encoded blob

### `<state_schema>.snapshot_jobs`

Stores snapshot state for `peer_region + table`.

Purpose:

- automatic bootstrap decision
- resume after interruption
- persisted checkpoint
- catch-up phase tracking
- finished marker

Notes:

- `last_primary_key` is stored as `JSONB`
- checkpoints support composite keys
- `status` is the persisted snapshot phase (`pending`, `snapshotting`, `catching_up`, `finished`)
- `watermark_at` stores the sender-side snapshot boundary captured before chunk streaming began

### `<state_schema>.leases`

Stores distributed ownership for coordinated workers across multiple pods in the same region.

Purpose:

- single owner per consume session
- single owner per snapshot worker scope
- single owner for the CDC producer

Current key fields:

- `lease_kind`
- `region`
- `peer_region`
- `table_name`
- `table_scope` generated from `COALESCE(table_name, '')` for uniqueness
- `owner_id`
- `generation`
- `expires_at`
- `updated_at`

### `<state_schema>.outbound_sequences`

Stores the next durable outbound sequence for `peer_region + table`.

Notes:

- polling batches use `peer_region + table` as the sequence key
- CDC transaction batches use `peer_region + ''` as a shared cross-table sequence key

### `<state_schema>.outbound_batches`

Stores pending outbound batches waiting for remote acknowledgement.

Purpose:

- durable handoff between local capture and served peer feed
- replay when a peer reconnects with `Consume`
- bounded backlog between local WAL consumption and remote delivery

## Key atomic operations

### `CommitStateAppliedTransaction`

Used by both inbound protocol paths. Applies one or more events atomically:

1. wraps all events in one database transaction
2. for each event: cursor read, stale check, apply, cursor update
3. stale events within the batch are skipped individually; the transaction is not aborted for them

For CDC this preserves source transaction atomicity: either all new events from the transaction are visible or none are.
For polling this gives the same cursor and apply semantics through the same repository path.

### `CommitStateSnapshotCheckpoint`

Atomically updates snapshot progress.

If the snapshot is already finished:

- the checkpoint is not overwritten
- the result reason is `snapshot_finished`

### Lease operations

`TryStateAcquireLease`:

- acquires the lease if it is free
- acquires the lease if it is expired
- refreshes the lease if the owner is already the same instance
- increments `generation` when ownership changes after expiry
- treats `table_name = NULL` and `table_name = ''` equivalently at the repository boundary for non-table-scoped leases

`RenewStateLease`:

- extends the lease only for the current owner and generation

`ReleaseStateLease`:

- releases the lease only for the current owner and generation

## At-least-once delivery model

At-least-once delivery is guaranteed by:

1. durable enqueue into `outbound_batches`
2. replay of pending batches whenever a peer reconnects with `Consume`
3. inbound deduplication via `peer_cursors`
4. logical replication slot resume for the local producer

This means:

- events staged before a reconnect window are replayed after recovery
- the receiver idempotently skips already-applied events

## Derived-state model

The application database is the source of truth for replicated business rows.
State tables are used for:

- peer cursors
- snapshot resume
- distributed worker ownership
- outbound sequences
- durable outbound replay state

The system relies on:

- at-least-once delivery
- idempotent application upserts
- eventual convergence of state metadata after retries
- logical replication slot resume for local producers

Optional provenance columns such as `region_field` can be stored in application tables to make replay and troubleshooting safer and easier to observe.

## Current limitations

- the state store is PostgreSQL-only
- inbound dedup relies on monotonic source ordering per `(peer_region, table)`
- `CommitStateAppliedTransaction` does not expose per-event `Applied` results; metrics count the whole batch as one apply
