CREATE SCHEMA IF NOT EXISTS __state_schema__;

CREATE TABLE IF NOT EXISTS __state_schema__.peer_cursors (
    peer_region TEXT NOT NULL,
    table_name TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    last_version BIGINT NOT NULL DEFAULT 0,
    last_primary_key JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (peer_region, table_name)
);

CREATE TABLE IF NOT EXISTS __state_schema__.changefeed_cursors (
    table_name TEXT NOT NULL,
    source TEXT NOT NULL,
    last_version BIGINT NOT NULL DEFAULT 0,
    last_primary_key JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_name)
);

CREATE TABLE IF NOT EXISTS __state_schema__.snapshot_jobs (
    peer_region TEXT NOT NULL,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    last_primary_key JSONB,
    watermark_at TIMESTAMPTZ,
    PRIMARY KEY (peer_region, table_name)
);

CREATE TABLE IF NOT EXISTS __state_schema__.leases (
    lease_kind TEXT NOT NULL,
    region TEXT NOT NULL,
    peer_region TEXT NOT NULL,
    table_name TEXT,
    table_scope TEXT GENERATED ALWAYS AS (COALESCE(table_name, '')) STORED,
    owner_id TEXT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (lease_kind, region, peer_region, table_scope)
);

CREATE INDEX IF NOT EXISTS leases_expires_at_idx
    ON __state_schema__.leases (expires_at);

CREATE TABLE IF NOT EXISTS __state_schema__.outbound_sequences (
    peer_region TEXT NOT NULL,
    table_name TEXT NOT NULL,
    next_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (peer_region, table_name)
);

CREATE TABLE IF NOT EXISTS __state_schema__.outbound_batches (
    id BIGSERIAL NOT NULL,
    peer_region TEXT NOT NULL,
    table_name TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    last_event_id TEXT NOT NULL,
    tx_id TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (peer_region, table_name, sequence),
    UNIQUE (id),
    UNIQUE (peer_region, table_name, last_event_id)
);

CREATE INDEX IF NOT EXISTS outbound_batches_peer_id_idx
    ON __state_schema__.outbound_batches (peer_region, id);
