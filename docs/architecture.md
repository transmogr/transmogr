# Architecture

## Purpose

Transmogr replicates selected PostgreSQL tables between regions without a central coordinator.
Each region runs its own node, serves its own replication feed to peers, consumes peer feeds over persistent gRPC sessions, and keeps only local key material.

## Core invariants

- the transport layer must not contain business logic
- no node may have access to encryption keys from multiple regions
- inbound replication must be idempotent
- outbound delivery is staged through the PostgreSQL outbox; at-least-once delivery comes from outbox replay plus source resume
- peer identity is region-level, not pod-level
- exactly one consume session per `(local_region, remote_region)` is coordinated through distributed leases in the shared database

## Runtime model

At a high level, one node:

1. starts shared PostgreSQL-backed state and runtime services
2. captures local changes through either CDC or polling
3. stages outbound batches in the PostgreSQL outbox
4. opens or serves one consume stream per peer region
5. applies inbound data durably and acknowledges delivered outbound batches

Detailed protocol and replication flow live in [`docs/replication.md`](replication.md).

## Package boundaries

### Entry points

- `cmd/transmogr`
  Main service startup and config loading.

- `cmd/init`
  One-shot state migration entrypoint.

### App composition

- `internal/app`
  Top-level composition and lifecycle. Builds the repository, services, transports, and optional metrics server.

### Domain services

- `internal/service/changefeed`
  Local change capture orchestration and producer-side batch staging.

- `internal/service/replication`
  Replication orchestration for handshake data, snapshot bootstrap decisions, and inbound apply.

- `internal/service/snapshot`
  Snapshot bootstrap, resume, checkpoint persistence, and chunk apply/send flow.

- `internal/service/lease`
  Distributed ownership for producer, stream, and snapshot work.

- `internal/service/peers`
  Peer selection and outbound target policy.

- `internal/service/crypto`
  Table-aware crypto policy and key-manager integration.

### Adapters and transport

- `internal/service/changefeed/adapter`
  PostgreSQL-specific runtime adapters for CDC and polling capture.

- `internal/transport/grpc/server`
  gRPC serving path for `Consume` and `Ack`.

- `internal/transport/grpc/client`
  Peer consume sessions, reconnect loops, lease ownership, receive loop, and ack calls.

- `internal/transport/grpc/protocol`
  Mapping between protobuf messages and internal replication models.

- `internal/replication/transformers`
  Ordered inbound/outbound payload transforms at the transport boundary.

### Persistence and shared types

- `internal/repository/postgres`
  PostgreSQL persistence for state tables, outbox, leases, snapshots, changefeed reads, and replicated row apply.

- `internal/models`
  Shared domain types used across services, repositories, and transport.

### Reusable and generated packages

- `pkg/fanout`
  Generic keyed in-memory pub/sub primitives used by replication wiring.

- `api/proto/peer/v1`
  Protobuf schema.

- `pkg/proto/transmogr/peerv1`
  Generated protobuf and gRPC code.

## Startup composition

At startup, `internal/app.New` assembles:

1. PostgreSQL pool and repository
2. crypto key manager and crypto service
3. replication-side services and metrics
4. changefeed adapter and producer orchestration
5. gRPC server
6. gRPC client manager
7. metrics HTTP server when enabled

## Deployment model

Current deployment assumptions:

- ingress is region-level
- the remote endpoint is a stable regional address, usually a Kubernetes `Service`
- multiple pods may run in the same region
- only one pod in a region owns the consume session for `local_region -> remote_region`
- snapshot and producer work are also coordinated through distributed leases

Lease identity:

- `lease_kind = stream`
  Session ownership for `(local_region, remote_region)`.

- `lease_kind = snapshot`
  Per-table snapshot worker ownership.

- `lease_kind = producer_cdc`
  Producer ownership within a region.

## Related docs

- [`docs/replication.md`](replication.md) for protocol and replication flow
- [`docs/state.md`](state.md) for persisted state model
- [`docs/operations.md`](operations.md) for operational and configuration details
