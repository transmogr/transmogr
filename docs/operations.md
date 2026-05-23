# Operations

## Configuration

Main config file:

- `configs/transmogr.yaml`

Current keys:

- `region`
- `log_level`
- `log_format`
- `transport.reconnect_delay`
- `transport.ping_interval`
- `transport.grpc.listen_addr`
- `transport.grpc.insecure`
- `transport.grpc.tls.cert_file`
- `transport.grpc.tls.key_file`
- `transport.grpc.tls.client_ca_file`
- `transport.grpc.tls.server_name`
- `transport.metrics.listen_addr`
- `crypto.refresh_interval`
- `crypto.retry_attempts`
- `crypto.retry_delay`
- `crypto.source.env.active_key_id`
- `crypto.source.env.active_key_env`
- `crypto.source.env.keyring_env`
- `crypto.source.http.url`
- `crypto.source.http.timeout`
- `crypto.source.http.token_env`
- `crypto.source.http.header_name`
- `crypto.source.vault.address`
- `crypto.source.vault.token_env`
- `crypto.source.vault.path`
- `crypto.source.vault.timeout`
- `replication.source.type`
- `replication.source.cdc.publication`
- `replication.source.cdc.slot`
- `replication.source.cdc.status_interval`
- `replication.source.cdc.update_diff`
- `replication.source.polling.interval`
- `replication.snapshot.chunk_size`
- `replication.apply_mode`
- `replication.broadcast_mode`
- `replication.max_batch_events`
- `lease.ttl`
- `lease.renew_interval`
- `lease.cleanup_interval`
- `outbox.cleanup_interval`
- `outbox.max_batch_age`
- `outbox.max_pending_per_peer`
- `db.postgres.dsn`
- `db.postgres.state_schema`
- `peers`
- `tables`
- `transformers`

Current `tables` fields:

- `name`
- `primary_key` ordered list of one or more columns
- `updated_at_field` required when `replication.source.type = polling`
- `region_field` required owner field written on snapshot and event apply

Current `transformers` fields:

- `type` transformer kind; currently only `crypto`
- `table` configured table name this transformer applies to
- `crypto_type` crypto algorithm for `type = crypto`
- `fields` one or more fields handled by this transformer
- `key_id` key id loaded from the crypto key source

Crypto policy is configured only through the top-level `transformers` list. It is not nested under `tables`.

## Snapshot

`replication.snapshot.chunk_size` is the number of rows fetched and applied per snapshot chunk. Default: `1000`.

Each chunk is one paginated `SELECT` on the sender and one batched write on the receiver inside a single transaction. Increase the value to reduce round-trip overhead on large tables. Decrease it if memory pressure or statement timeout is a concern.

Snapshot bootstrap behavior:

- a table is requested automatically when there is no peer cursor and no finished snapshot job
- requested tables are sent in `ConsumeRequest.snapshot_requests`
- progress is persisted in `<state_schema>.snapshot_jobs`

Owner-region rules:

- `region_field` is treated as the owner-region column
- if a local row already has a non-null owner, incoming upserts and deletes are applied only when the incoming region matches that owner
- rows with a null owner can be adopted by the incoming region on first replicated write

## Crypto

Key manager behavior:

- the active keyring is cached in memory
- refresh runs every `crypto.refresh_interval`
- each load or refresh cycle retries `crypto.retry_attempts` times with `crypto.retry_delay` between attempts
- refresh failures leave the last successful in-memory snapshot in place
- startup fails if the initial load cannot produce the full set of `key_id` values referenced by crypto transformers
- after a refresh exhausts all retry attempts, `/readyz` reports the process as not ready until a later refresh succeeds

Supported key sources:

- `crypto.source.env`
- `crypto.source.http`
- `crypto.source.vault`

Source selection:

- all configured sources are used together
- `http` is enabled when `crypto.source.http.url` is set
- `vault` is enabled when both `crypto.source.vault.address` and `crypto.source.vault.path` are set
- `env` is enabled when `crypto.source.env.active_key_env` or `crypto.source.env.keyring_env` resolves to a non-empty value in the process environment

Env source:

- `crypto.source.env.active_key_id`
  Key id assigned to the key read from `crypto.source.env.active_key_env`
- `crypto.source.env.active_key_env`
  Env var name holding the base64-encoded active key
- `crypto.source.env.keyring_env`
  Env var name holding the optional JSON keyring
- `TRANSMOGR_CRYPTO_KEY`
  Typical active-key env var
- `TRANSMOGR_CRYPTO_KEYRING`
  Typical keyring env var containing `{"<key_id>":"<base64>", ...}`

HTTP source response format:

```json
{
  "active_key_id": "current",
  "keys": {
    "current": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    "previous": "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk="
  },
  "version": "2026-03-15T12:00:00Z"
}
```

Vault source secret payload format:

- the implementation expects a Vault KV v2 read response
- `active_key_id`, `keys`, and optional `version` must be present inside `data.data`

Merged-source behavior:

- providers are merged in declaration order
- later providers override earlier keys with the same `key_id`
- current order is `http`, then `vault`, then `env`

Algorithms:

- `aes-gcm`
- `chacha20poly1305`
- `xchacha20poly1305`

Ciphertext format stored in the database is a JSON string containing raw base64-encoded `nonce + ciphertext`:

```text
"<base64(nonce+ciphertext)>"
```

The crypto type and key ID are not embedded in the stored value; they are always taken from the matching top-level `transformers` crypto entry.

Storage contract:

- encrypted fields are stored in the local database as ciphertext strings
- outbound event and snapshot pipelines decrypt those fields only after durable local reads
- decrypted transit values are moved into `metadata` keys under `transit.crypto.<field>` and removed from the wire payload fields
- inbound event and snapshot pipelines restore `transit.crypto.<field>` back into fields, encrypt them again with the local region key, and then remove all leftover `transit.*` metadata
- the target column must be able to store the ciphertext as a text-compatible value

## gRPC transport security

Every deployment must explicitly choose a transport mode. Omitting both is a startup error.

Plaintext:

```yaml
transport:
  grpc:
    insecure: true
```

mTLS:

```yaml
transport:
  grpc:
    tls:
      cert_file: /etc/transmogr/tls/tls.crt
      key_file: /etc/transmogr/tls/tls.key
      client_ca_file: /etc/transmogr/tls/ca.crt
```

Behavior:

- the same local cert/key pair is used for both server and client auth
- `transport.grpc.tls.cert_file` and `transport.grpc.insecure` are mutually exclusive
- when mTLS is enabled, the peer certificate must contain URI SAN `transmogr://region/<region-name>`
- the declared region in `ConsumeRequest` must match the certificate region

## Local run

Runnable Compose examples live in the separate `transmogr/examples` repository:

- `https://github.com/transmogr/examples/tree/main/cdc/default`
- `https://github.com/transmogr/examples/tree/main/cdc/mtls`
- `https://github.com/transmogr/examples/tree/main/cdc/snailproxy`
- `https://github.com/transmogr/examples/tree/main/cdc/crypto`
- `https://github.com/transmogr/examples/tree/main/polling/default`

Each example includes:

- one PostgreSQL instance per region
- `transmogr-init` per region
- one `transmogr` node per region
- shared Prometheus and Grafana from `https://github.com/transmogr/examples/tree/main/monitoring`

Start any example via its `Makefile`:

```bash
git clone git@github.com:transmogr/examples.git
cd examples/cdc/default
make up
```

Manual alternatives:

```bash
go run ./cmd/init
go run ./cmd/transmogr
```

With an explicit config path:

```bash
TRANSMOGR_CONFIG=./configs/transmogr.yaml go run ./cmd/transmogr
```

Runtime instance identity:

- `instance_id` is not configured in YAML
- it is generated automatically at process start
- it is used only for lease ownership and process identity

## Change capture

Supported values:

- `replication.source.type = cdc`
- `replication.source.type = polling`

Polling semantics:

- uses configured `updated_at_field` and native primary-key tuple ordering
- emits insert/update only
- does not observe hard deletes
- `tables[].updated_at_field` must be present for every replicated table

CDC-specific operational requirements:

- PostgreSQL must allow logical replication
- the configured user must be allowed to create or use the publication and replication slot
- `replication.source.cdc.publication` and `replication.source.cdc.slot` must be stable across restarts
- full-row CDC events are published from the WAL tuple, not reloaded from the live table
- UPDATE events that contain unchanged TOAST placeholders cannot be published as full row images; use `replication.source.cdc.update_diff = true` for such tables

CDC update diff (`replication.source.cdc.update_diff`):

- when enabled, only changed fields are sent in UPDATE events instead of the full row image
- avoids full-row CDC failures on unchanged TOAST columns because UPDATE payloads are derived from the WAL diff
- automatically sets `REPLICA IDENTITY FULL` on all configured tables at startup
- this increases WAL volume and may reduce HOT update efficiency
- incompatible with `polling`

Batching:

- `replication.max_batch_events` limits the number of events in one outbound batch
- large CDC transactions are split into multiple protocol batches to stay within message limits

The outbox always uses PostgreSQL with durable enqueue.

## Protobuf generation

Use the `Makefile`:

```bash
make proto
make proto-check
```

## Quality gates

```bash
go test ./...
make lint-docker
```

## Metrics

HTTP endpoints:

- `GET /metrics`
- `GET /livez`
- `GET /readyz`

Health semantics:

- `/livez` only reports that the process is running
- `/readyz` verifies database reachability and crypto key-manager health

Metrics include:

- reconnect attempts
- ack latency
- changefeed emitted events and batches
- inbound apply results
- lease acquire and renew outcomes
- outbox pending batch count and oldest pending batch age
- outbox cleanup deleted count

Outbox metrics collection:

- PostgreSQL outbox metrics are refreshed in the background and served from a cached snapshot
- `/metrics` does not run a live backlog query on every scrape

## Multi-pod deployment

Current assumptions:

- each peer is exposed through one stable regional endpoint
- multiple pods may run in the same region
- consume-session ownership is coordinated through leases in the shared database
- snapshot workers and the CDC producer also use leases
- all pods in a region share the same PostgreSQL database

Minimum operational requirements:

- all pods in a region must share the same PostgreSQL database
- the configured `db.postgres.state_schema` must be created by `transmogr-init` or another migration runner before starting the app
- `lease.renew_interval` must be smaller than `lease.ttl`
- peers should be configured as stable region-level ingress addresses

## Integration tests

Integration tests run through `testcontainers-go`.

Requirements:

- Docker must be available locally
- the current user must be allowed to run containers
