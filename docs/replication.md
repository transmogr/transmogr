# Replication Flow

## Protocol

Peers replicate with one server-streaming feed plus unary acknowledgements:

`rpc Consume(ConsumeRequest) returns (stream ConsumeResponse)`

`ConsumeRequest` carries:

- consumer `region`
- consumer `version`
- consumer `configuration`
- `snapshot_requests` for tables that need snapshot bootstrap, including resume checkpoints

`ConsumeResponse` may carry:

- `Handshake`
- `SnapshotChunk`
- `SnapshotComplete`
- `EventBatch` for the polling path
- `TransactionBatch` for the CDC path
- `Heartbeat`

Acknowledgements are sent separately:

`rpc Ack(AckRequest) returns (AckResponse)`

When `transport.grpc.tls.cert_file` is set, the connection uses mTLS for peer authentication and transport encryption.
When `transport.grpc.insecure: true` is set, the client uses plaintext gRPC credentials and the server accepts plaintext gRPC.
Omitting both is a startup validation error.

## Client-side lifecycle

Files:

- `internal/transport/grpc/client/peer_loop.go`
- `internal/transport/grpc/client/session.go`
- `internal/transport/grpc/client/recv.go`

Behavior:

1. the client manager starts one reconnect loop per remote region
2. it tries to acquire the consume-session lease for `stream + local_region + remote_region`
3. only the lease owner opens `Consume`
4. before connecting, it checks persisted `peer_cursors` and `snapshot_jobs` to decide which tables need bootstrap
5. it sends one `ConsumeRequest` with local handshake data and requested snapshot bootstrap descriptors
6. it receives the server handshake and validates compatibility
7. it applies incoming `SnapshotChunk`, `SnapshotComplete`, `EventBatch`, and `TransactionBatch` messages
8. after durable local apply, it sends unary `Ack`
9. it renews the lease for the lifetime of the session
10. on session failure it waits for backoff and reconnects

## Server-side lifecycle

File:

- `internal/transport/grpc/server/handlers.go`

Behavior:

1. the server validates the incoming `ConsumeRequest`
2. it checks remote region policy and, when mTLS is enabled, verifies the region against the certificate URI SAN
3. it starts one dedicated send loop for the response stream
4. it sends its handshake
5. if snapshot bootstrap was requested, it captures a snapshot watermark and streams snapshots for the requested tables
6. it subscribes to live outbox batches for the requesting peer region
7. it replays pending outbox batches in catch-up windows bounded by the snapshot and live watermarks
8. it emits `SnapshotComplete` for each bootstrapped table
9. it forwards only post-boundary live outbox batches to the stream until cancellation

Important:

- the transport layer does not make business decisions
- apply/send orchestration is delegated to `internal/service/replication`
- the server never trusts the caller's region blindly; request validation happens before any feed data is served

## Handshake

Handshake compatibility is checked in `internal/transport/grpc/protocol/handshake.go`.

The compatibility contract is:

- protocol versions must match exactly
- replicated table sets must match exactly
- each side sends a normalized configuration payload derived from:
  - source type
  - replicated tables
  - primary keys
  - region field
  - discovered table columns

This prevents peers with incompatible table configuration from silently replicating.

## Incremental replication

Files:

- `internal/service/changefeed/service.go`
- `internal/service/changefeed/adapter/capture.go`
- `internal/service/changefeed/adapter/polling.go`

Flow:

1. one pod per region acquires the producer lease
2. the configured adapter captures local changes and emits normalized `models.ChangefeedBatch`
3. `internal/service/changefeed.HandleBatch` converts changes into replication events
4. one outbound batch per peer is staged through the PostgreSQL outbox
5. producer cursors are persisted only after successful enqueue for all peers

`ChangefeedBatch` carries:

- `TxID` for CDC batches
- `Changes` as one or more normalized row mutations

Current assumptions:

- `replication.source.type = cdc`
  - PostgreSQL logical replication uses `pgoutput`
  - publication and slot names come from config
  - changes from a single WAL transaction are accumulated between `BeginMessage` and `CommitMessage`
  - one source transaction becomes one `ChangefeedBatch` with `TxID = FinalLSN`
  - upsert payloads are loaded from committed table state after WAL messages are observed
  - delete versions are derived from WAL LSN
  - CDC batches are sent as `TransactionBatch` and applied atomically on the receiver
- `replication.source.type = polling`
  - polling uses `updated_at_field` plus native primary-key tuple ordering
  - each polled row becomes a single-event `ChangefeedBatch` with empty `TxID`
  - polling batches are sent as `EventBatch`
  - polling emits insert/update operations only
  - polling does not observe hard deletes

Implementation split:

- `internal/service/changefeed/adapter` owns runtime adapter loops for CDC and polling
- `internal/repository/postgres` owns PostgreSQL-specific changefeed data access used by those adapters

## Event model

Each replicated event carries:

- `event_id`
- `origin_region`
- `table`
- `primary_key` as ordered typed key parts
- `version`
- `operation`
- `fields`
- optional `metadata`

Current operations:

- `insert`
- `update`
- `delete`

Behavior:

- CDC emits insert/update/delete and orders them by WAL position
- polling emits insert/update and orders them by `(updated_at_field, primary_key)`
- both adapters support single-column and composite primary keys

`metadata` is for in-flight transformer data only. Keys under `transit.*` must be consumed before repository apply. Crypto transformers use `transit.crypto.<field>` for decrypted field values that must not remain in `fields` during transfer.

## Outbound

Outbound flow:

1. the outbox allocates a sequence
   - polling batches use `peerRegion + table`
   - CDC transaction batches use `peerRegion + ""`
2. the batch is stored in `<state_schema>.outbound_batches`
3. on each `Consume` connection, pending batches are replayed first
4. new batches are pushed to live subscribers for that peer
5. the consumer sends unary `Ack`
6. `Ack` removes the pending batch from the outbox

Notes:

- PostgreSQL outbox is always used
- reconnect replay comes from durable database state, not from in-memory transport state
- in-memory live delivery fan-out is provided by `pkg/fanout`; it is keyed by peer region and carries `models.StreamMessage` as an internal payload
- outbound transformers run after outbox reads, so plaintext never lands in the outbox

## Inbound apply

### EventBatch

Entry point: `replication.Service.ApplyEventBatch`

Flow:

1. protobuf events are decoded into `models.ReplicationEvent`
2. inbound transformers restore transit metadata into fields, re-encrypt configured fields, and remove leftover `transit.*` metadata
3. the replication service delegates to transaction apply logic
4. `CommitStateAppliedTransaction` updates repository state and application rows transactionally
5. during that transaction, the receiver sets `SET LOCAL session_replication_role = replica` to suppress FK failures from replicated write ordering; this disables all table triggers for the affected target tables, including user-defined triggers
6. the client sends `Ack` with the first event's table name and the last event id in the batch

### TransactionBatch

Entry point: `replication.Service.ApplyTransactionBatch`

Flow:

1. inbound transformers restore transit metadata into fields, re-encrypt configured fields, and remove leftover `transit.*` metadata
2. `CommitStateAppliedTransaction` applies all events within one database transaction
3. that transaction sets `SET LOCAL session_replication_role = replica` to suppress FK failures from replicated write ordering; this disables all table triggers for the affected target tables, including user-defined triggers
4. stale events inside the batch are skipped individually without breaking atomicity
5. the client sends `Ack` with an empty table key because CDC sequences are cross-table

## Snapshot replication

### Request

Snapshot bootstrap is automatic.
Before opening `Consume`, the client checks each configured table:

- if a peer cursor already exists, no snapshot is requested
- if a snapshot job is already finished, no snapshot is requested
- otherwise the table is included in `ConsumeRequest.snapshot_requests`

### Send

`replication.Service.SendSnapshotToPeer`:

1. acquires the snapshot worker lease for `snapshot + local_region + remote_region + table`
2. captures a sender-side snapshot watermark
3. reads local chunks via the snapshot service
4. attaches that watermark to every `SnapshotChunk` for the table
5. hands each `SnapshotChunk` to the gRPC server send callback
6. continues until `Last = true`
7. after snapshot row streaming, the transport replays outbox batches up to the live boundary and emits `SnapshotComplete`

Snapshot chunk pagination and resume use PostgreSQL row comparison on the configured primary-key tuple, so integer keys keep native ordering and composite keys resume correctly.

Before protobuf encoding, outbound snapshot transformers run over `SnapshotRow` payloads and metadata. Crypto uses the same contract as events: configured encrypted fields are decrypted after the local durable read, moved into `transit.crypto.<field>`, and removed from the row payload sent over the wire.

### Apply

`replication.Service.ApplySnapshotChunk`:

1. delegates to `snapshot.ApplyIncomingChunk`
2. inbound snapshot transformers restore transit metadata into the payload, re-encrypt configured fields, and remove leftover `transit.*` metadata
3. application rows are upserted in one transaction using batched SQL
4. during that transaction, the receiver sets `SET LOCAL session_replication_role = replica`; FK checks and all table triggers on the target table are disabled for that apply transaction
5. snapshot progress, phase, and sender watermark are persisted in `snapshot_jobs`
6. on the last chunk, the snapshot job advances to `catching_up`
7. on `SnapshotComplete`, the snapshot job is marked finished

## What to preserve when changing this code

- never call `stream.Send` concurrently from multiple goroutines without one send loop
- do not move state logic into the transport layer
- do not bypass the outbox for durable outbound delivery
- do not bypass repository apply for inbound batches
- do not break the idempotent cursor update path
- do not apply a `TransactionBatch` outside of a single database transaction
- do not mix CDC and polling paths: `TxID != ""` must produce `TransactionBatch`, empty `TxID` must produce `EventBatch`
- do not make snapshot progress in-memory only
- do not bypass the distributed lease path in the client manager
- do not bypass the logical replication slot as the producer resume source
