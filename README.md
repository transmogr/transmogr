# Transmogr

**Peer-to-peer multi-region PostgreSQL table replication — no central coordinator, field-level encryption, at-least-once delivery.**

Transmogr replicates selected application tables between regions over persistent gRPC consume sessions. Each region runs its own node, serves its local change feed to peers, consumes remote feeds directly, and keeps only its own encryption keys. There is no shared coordinator or control plane.

---

> **Important:** Each node only owns the rows that originate in its region. Only the application running in that region may create, update, or delete its own records. If you need to modify data owned by another region, proxy the request to the application instance in that region — writing foreign-region data locally will cause conflicts and break convergence guarantees.

---

## Features

- **Selective table replication** — replicate only the tables you choose, not whole databases; single and composite primary keys supported
- **CDC and polling capture** — PostgreSQL logical replication (`pgoutput`) or `updated_at`-based polling; CDC changes are grouped by source transaction and applied atomically on the receiver
- **At-least-once delivery** — PostgreSQL outbox-backed replay + logical replication slot resume; pending batches survive reconnects and are removed only after remote `Ack`
- **Durable outbox** — PostgreSQL-backed outbox ensures pending events survive restarts and are retried until acknowledged
- **Inbound idempotency** — cursor-based deduplication; replayed events are safely skipped
- **Field-level encryption** — AEAD encryption per field, keys isolated per region; plaintext never stored cross-region; startup is blocked until all configured key IDs are successfully loaded
- **Configurable apply mode** — control how inbound events are applied: `insert_only`, `upsert`, or `full_sync` (inserts + updates + deletes)
- **Configurable broadcast mode** — control what local changes are sent to peers: `insert_only`, `upsert`, or `full_sync`
- **Peer consistency validation** — on each connection, peers exchange region, protocol version, and a configuration payload; a feed is rejected on mismatched replicated table set or incompatible protocol version, preventing silent misconfiguration
- **Owner-region conflict resolution** — rows carry an `owner_region` field; incoming writes are gated against local ownership
- **Horizontal scaling** — run multiple pods per region; distributed DB-backed leases ensure exactly one active consume session per region pair, one CDC producer, and one snapshot worker per table — no duplicate delivery, no split-brain
- **Initial snapshot replication** — automatic bootstrap for tables without a cursor; chunked with persisted checkpoints and resume support
- **Prometheus metrics** — `/metrics`, `/livez`, `/readyz`
- **mTLS transport** — optional mutual TLS between peers for both encryption and peer authentication; unauthorized nodes cannot connect

---

## How it works

```
 Region A                                        Region B
┌──────────────────┐                            ┌──────────────────┐
│  PostgreSQL      │                            │  PostgreSQL      │
│  (app + state)   │                            │  (app + state)   │
│                  │                            │                  │
│  transmogr-init  │   Consume() server feed    │  transmogr-init  │
│  transmogr    ───┼───────────────────────────►│  transmogr       │
│                  │   ◄─────────────────────── │  Ack()           │
└──────────────────┘                            └──────────────────┘
```

1. `transmogr-init` applies state schema migrations on startup.
2. `transmogr` captures local changes from PostgreSQL (`cdc` or `polling`) and durably stages outbound batches in the PostgreSQL outbox.
3. One pod per remote region acquires a lease and opens `Consume` to that peer. The request carries the local region, version, configuration payload, and per-table snapshot bootstrap requests with resume checkpoints when bootstrap is needed.
4. The serving peer validates the consumer, sends a handshake, streams snapshot chunks, replays catch-up outbox batches up to the live boundary, emits `SnapshotComplete`, and only then keeps streaming live `EventBatch` / `TransactionBatch` messages plus heartbeats.
5. After durable local apply, the consumer sends unary `Ack` requests. The sender removes acknowledged batches from the outbox.
6. For encrypted fields, the sender runs an outbound message pipeline after reading from the outbox or snapshot source: encrypted values are decrypted locally, moved into transit-only metadata, sent over the wire, then restored and re-encrypted by the receiver's inbound pipeline before local apply.

---

## Quick start

Transmogr runs as a sidecar or standalone service alongside your PostgreSQL database — one instance per region. Each node needs a config file, a one-shot migration run (`transmogr-init`), and the main service (`transmogr`).

Runnable Docker Compose examples for CDC, polling, field-level encryption, and simulated WAN latency live in the separate [`transmogr/examples`](https://github.com/transmogr/examples) repository.

The demo application used by those examples is [Replicalens](https://github.com/transmogr/replicalens) — a small multi-region PostgreSQL app that applies its own schema migrations on startup and exposes a simple UI for generating and inspecting replicated data.

The system keeps its operational metadata in `db.postgres.state_schema` inside the same PostgreSQL database: peer cursors for inbound idempotency, snapshot checkpoints, distributed leases, outbound sequences, and pending outbox batches.

Current internal layering keeps PostgreSQL data access in `internal/repository/postgres`, changefeed runtime adapters in `internal/service/changefeed/adapter`, producer orchestration in `internal/service/changefeed`, and keyed in-memory pub/sub primitives in `pkg/fanout`. For a deeper code-map and flow walkthrough, see [`docs/architecture.md`](docs/architecture.md) and [`docs/replication.md`](docs/replication.md).

`transmogr/transmogr` ships two binaries: `/usr/local/bin/transmogr` (the replication service) and `/usr/local/bin/transmogr-init` (one-shot state migration runner). Run `transmogr-init` once before starting `transmogr`. It applies internal state migrations into the configured `db.postgres.state_schema`. Application table migrations remain your responsibility.

For local verification and CI, the repository also contains a `tests/e2e` suite built on `testcontainers-go`. It covers cross-region happy paths, failover and lease behavior, replay after reconnect, owner-region gating, memory-stability checks, and separate long-running memory soak scenarios under a build tag. Docker must be available locally for these tests to run.

Example Kubernetes init container:

```yaml
initContainers:
  - name: transmogr-init
    image: transmogr/transmogr
    command: ["/usr/local/bin/transmogr-init"]
    env:
      - name: TRANSMOGR_CONFIG
        value: /etc/transmogr/config.yaml
    volumeMounts:
      - name: config
        mountPath: /etc/transmogr
containers:
  - name: transmogr
    image: transmogr/transmogr
    command: ["/usr/local/bin/transmogr"]
    env:
      - name: TRANSMOGR_CONFIG
        value: /etc/transmogr/config.yaml
    volumeMounts:
      - name: config
        mountPath: /etc/transmogr
```

---

## Testing

Common test entrypoints:

- `make test-e2e` runs the default end-to-end suite in `tests/e2e`
- `make test-e2e-smoke` runs the fast smoke subset
- `make test-e2e-slow` runs the slower resilience and memory-stability subset
- `make test-e2e-memory-soak` runs the longer memory soak suite behind the `soak` build tag

The default `tests/e2e` suite focuses on correctness and recoverability:

- `polling` and `cdc` snapshot bootstrap plus live replication
- reconnect with durable outbox replay
- same-region scaling and lease ownership
- snapshot resume after interruption
- producer failover
- bidirectional replication and owner-region conflict gating
- bounded `heap` and `RSS` checks for sender and receiver paths

The tagged memory soak suite focuses on long-lived stability rather than just correctness:

- `polling` sender offline-backlog plateau
- `cdc` sender offline-backlog plateau
- `polling` receiver snapshot plus extended live-apply plateau
- `cdc` receiver snapshot plus extended live-apply plateau

These soak tests assert that `heap`, `RSS`, and goroutine counts stay within a bounded envelope across many waves while replication still makes forward progress through row-count and outbox-state checks.

The most detailed scenario descriptions live directly in the test files, where each file starts with a comment describing the intent and invariants of that group of tests.

---

## Configuration reference

### Top-level

| Key | Default | Description |
|---|---|---|
| `region` | — | This node's region name. **Required.** |
| `log_level` | `info` | Minimum log severity: `debug`, `info`, `warn`, `error` |
| `log_format` | `text` | Log output format: `text`, `json` |

### Transport

| Key | Default | Description |
|---|---|---|
| `transport.reconnect_delay` | `5s` | Wait between peer reconnection attempts |
| `transport.ping_interval` | `15s` | How often peer health pings are sent |
| `transport.grpc.listen_addr` | `:8443` | Address the gRPC server binds to |
| `transport.grpc.insecure` | `false` | Opt out of TLS for plaintext transport. Mutually exclusive with `tls.cert_file` |
| `transport.grpc.tls.cert_file` | — | PEM-encoded certificate path. Setting this enables mTLS |
| `transport.grpc.tls.key_file` | — | PEM-encoded private key path. Required when `cert_file` is set |
| `transport.grpc.tls.client_ca_file` | — | PEM-encoded CA certificate used to verify peer certs. Required when `cert_file` is set |
| `transport.grpc.tls.server_name` | — | Override TLS hostname verification. Defaults to peer endpoint host |
| `transport.metrics.listen_addr` | — | HTTP address for `/metrics`, `/livez`, `/readyz`. Disabled if empty |

### Database

| Key | Default | Description |
|---|---|---|
| `db.postgres.dsn` | — | PostgreSQL connection string. Supports query params for pool tuning: `pool_max_conns`, `pool_min_conns`, `pool_max_conn_lifetime`, `pool_max_conn_idle_time`, `pool_health_check_period`. **Required.** |
| `db.postgres.state_schema` | `transmogr` | PostgreSQL schema for Transmogr internal state tables |

### Replication

| Key | Default | Description |
|---|---|---|
| `replication.source.type` | `cdc` | Change capture backend: `cdc` (logical replication) or `polling` |
| `replication.source.cdc.publication` | `transmogr_publication` | PostgreSQL publication name. `cdc` only |
| `replication.source.cdc.slot` | `transmogr_slot` | Logical replication slot name. Created automatically if absent. `cdc` only |
| `replication.source.cdc.status_interval` | `10s` | How often the CDC adapter sends WAL keepalive status. `cdc` only |
| `replication.source.cdc.update_diff` | `false` | Send only changed fields on UPDATE instead of full row. Enables `REPLICA IDENTITY FULL` automatically. `cdc` only |
| `replication.source.polling.interval` | `5s` | How often the polling backend queries for changes. `polling` only |
| `replication.snapshot.chunk_size` | `1000` | Rows fetched and sent per snapshot chunk |
| `replication.apply_mode` | `upsert` | How inbound events from peers are applied locally: `insert_only`, `upsert`, `full_sync` |
| `replication.broadcast_mode` | `full_sync` | Which local changes are sent to peers: `insert_only`, `upsert`, `full_sync` |
| `replication.max_batch_events` | `1000` | Maximum number of events per outbound batch. Large transactions are split into multiple batches to stay within the gRPC message size limit |

### Lease

| Key | Default | Description |
|---|---|---|
| `lease.ttl` | `15s` | How long a lease stays valid without renewal |
| `lease.renew_interval` | `5s` | How often a held lease is renewed. Must be less than `ttl` |
| `lease.cleanup_interval` | `5m` | How often expired leases are purged from the database |

### Outbox

| Key | Default | Description |
|---|---|---|
| `outbox.cleanup_interval` | `5m` | How often outbox retention cleanup runs |
| `outbox.max_batch_age` | `168h` | How long an unacknowledged batch is retained before cleanup may delete it, dropping undelivered events for a disconnected peer |
| `outbox.max_pending_per_peer` | `100000` | Maximum pending batches per peer before cleanup drops the oldest unacknowledged batches |

### Crypto

| Key | Default | Description |
|---|---|---|
| `crypto.refresh_interval` | `1m` | How often the key manager re-fetches keys from sources |
| `crypto.retry_attempts` | `3` | Attempts per load/refresh cycle |
| `crypto.retry_delay` | `1s` | Wait between retry attempts |
| `crypto.source.env.active_key_id` | — | Key ID assigned to the key read from `active_key_env` |
| `crypto.source.env.active_key_env` | — | Env var holding the base64-encoded active key. Required to enable the env source |
| `crypto.source.env.keyring_env` | — | Env var holding a JSON object of additional keys keyed by key ID |
| `crypto.source.http.url` | — | Endpoint to fetch keys from. Setting this enables the HTTP source |
| `crypto.source.http.timeout` | `5s` | HTTP request deadline |
| `crypto.source.http.token_env` | — | Env var holding the bearer token for the request |
| `crypto.source.http.header_name` | `Authorization` | HTTP header used to send the token |
| `crypto.source.vault.address` | — | Vault server URL. Required when vault source is configured |
| `crypto.source.vault.token_env` | — | Env var holding the Vault token |
| `crypto.source.vault.path` | — | KV v2 secret path. Required when vault source is configured |
| `crypto.source.vault.timeout` | `5s` | Vault request deadline |

### Peers

| Key | Default | Description |
|---|---|---|
| `peers[].region` | — | Peer region name. **Required. Must be unique across `peers[]` to ensure only one outbound stream per remote region.** |
| `peers[].endpoint` | — | Peer gRPC address. **Required.** |

### Tables

| Key | Default | Description |
|---|---|---|
| `tables[].name` | — | Table to replicate. **Required.** |
| `tables[].primary_key` | — | Ordered list of primary key columns. **Required.** |
| `tables[].region_field` | — | Column that stores the owning region. **Required.** |
| `tables[].updated_at_field` | — | Timestamp column used as the polling watermark and ordering key. Required when `replication.source.type = polling` |

### Transformers

Transformers are configured as a top-level `transformers` list. Crypto policy is no longer nested under `tables[]`; each transformer references the table it applies to through `transformers[].table`.

| Key | Default | Description |
|---|---|---|
| `transformers[].type` | — | Transformer kind. Currently supported: `crypto`. **Required.** |
| `transformers[].table` | — | Table this transformer applies to. Must reference a configured table. **Required.** |
| `transformers[].crypto_type` | — | Crypto type name for `type = crypto`. Built-ins: `aes-gcm`, `chacha20poly1305`, `xchacha20poly1305`. Custom types may be registered in application code. **Required for crypto.** |
| `transformers[].fields` | — | One or more fields handled by this transformer. **Required for crypto.** |
| `transformers[].key_id` | — | Key ID from the crypto source. **Required for crypto.** |

See [`configs/transmogr.yaml`](configs/transmogr.yaml) for a full annotated example and [`docs/operations.md`](docs/operations.md) for complete field reference.

---

## Environment variable substitution

Any config value can reference an environment variable using `${VAR_NAME}` syntax. Placeholders are expanded before the YAML is parsed.

```yaml
db:
  postgres:
    dsn: ${DATABASE_URL}

crypto:
  source:
    env:
      active_key_id: ${CRYPTO_KEY_ID}
      active_key_env: ${CRYPTO_KEY_ENV_VAR}

peers:
  - region: us-east
    endpoint: ${PEER_US_EAST_ENDPOINT}
```

If a referenced variable is not set, the placeholder expands to an empty string. Config validation will then report a missing required field as usual.

To include a literal `$` in a value, write `$$`.

---

## Encryption

Sensitive fields are encrypted at rest with the local region key. During replication, the sender decrypts configured fields only after reading durable local state, moves those plaintext values into transit-only message metadata, and omits them from the stored row payload sent over the wire. The receiver restores those values from transit metadata, re-encrypts them with its own regional key, and only then applies them locally. Plaintext is never written to the PostgreSQL outbox or snapshot checkpoint state, and encryption keys never leave the region.

Transit metadata contract:

- `Event.metadata` and `SnapshotRow.metadata` may carry in-flight pipeline data
- keys under `transit.*` are temporary and must never reach repository apply unchanged
- crypto-owned transit fields use `transit.crypto.<field>`
- the inbound pipeline removes any leftover `transit.*` metadata before local persistence

**Supported algorithms:**

| Algorithm | Key size |
|---|---|
| `aes-gcm` | 16, 24, or 32 bytes |
| `chacha20poly1305` | 32 bytes |
| `xchacha20poly1305` | 32 bytes |

**Ciphertext format stored in the database** — a JSON string containing the raw base64-encoded nonce prepended to the ciphertext:

```
"<base64(nonce+ciphertext)>"
```

The crypto type and key ID are not embedded in the stored value — they are always taken from the configured top-level `transformers[]` crypto entry. To change the key or crypto type, update the config and re-encrypt existing rows manually.

**Key sources** — all configured sources are merged. When the same key ID appears in multiple sources, later sources win (merge order: http → vault → env):

- **env** — enabled only when `crypto.source.env.active_key_env` resolves to a non-empty env var; loads the active key from that env var (assigned the configured `active_key_id`) plus an optional JSON keyring via `TRANSMOGR_CRYPTO_KEYRING` (`{"<key_id>": "<base64>", ...}`)
- **http** — JSON endpoint at `crypto.source.http.url`; expected response: `{"active_key_id": "...", "keys": {"<id>": "<base64>"}, "version": "..."}`
- **vault** — HashiCorp Vault KV v2 at `crypto.source.vault.path`; same response shape expected inside `data.data`

Keys are loaded on startup and refreshed in the background every `crypto.refresh_interval`. A refresh failure leaves the last successful snapshot in place. **Startup is blocked until all key IDs referenced in `transformers[]` with `type = crypto` are successfully loaded.**

---

## Observability

| Endpoint | Description |
|---|---|
| `GET /metrics` | Prometheus metrics |
| `GET /livez` | Liveness — process is running |
| `GET /readyz` | Readiness — DB reachable, crypto key manager healthy |

Metrics include: reconnect attempts, outbound ack latency, changefeed emit and batch counts, inbound apply results (applied / stale / error), lease outcomes, outbox backlog size and age.

The Compose examples in [`transmogr/examples`](https://github.com/transmogr/examples) include a shared Prometheus and Grafana stack.

---

## Requirements

- PostgreSQL 14+ with `wal_level = logical` (for CDC mode)
- Go 1.23+

---

## Development

```bash
# Run tests (requires Docker for testcontainers)
go test ./...

# Run all e2e tests
make test-e2e

# Run only the faster e2e smoke scenarios
make test-e2e-smoke

# Run only the slower resilience/failover e2e scenarios
make test-e2e-slow

# Lint
make lint-docker

# Regenerate protobuf
make proto
```

### E2E test suite

The `tests/e2e` suite boots multiple real Transmogr nodes against PostgreSQL
containers via `testcontainers-go` and verifies cross-node behavior end to end.

- `make test-e2e-smoke` runs the faster replication and replay scenarios intended
  for regular local development.
- `make test-e2e-slow` runs the heavier resilience scenarios such as snapshot
  resume and same-region lease failover.
- `make test-e2e` runs the full e2e suite.

Docker is required for all e2e targets because the suite provisions PostgreSQL
containers dynamically.

---

## Limitations

- PostgreSQL is the only supported database backend
- CDC mode requires PostgreSQL logical replication permissions
- Polling mode does not capture hard deletes
- Inbound apply uses `SET LOCAL session_replication_role = replica` to avoid foreign-key failures during replicated writes. This disables all table triggers for that apply transaction on the receiver, including user-defined audit, cache, and soft-delete triggers.
- Plaintext transport requires `transport.grpc.insecure: true`; omitting both TLS and insecure is a startup error

---

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — package layout and runtime composition
- [`docs/replication.md`](docs/replication.md) — replication protocol and flow
- [`docs/state.md`](docs/state.md) — database state schema
- [`docs/operations.md`](docs/operations.md) — full configuration reference and operational guide

---

## License

MIT
