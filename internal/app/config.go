package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/transmogr/transmogr/internal/models"
)

const (
	defaultConfigPath           = "configs/transmogr.yaml"
	configPathEnvKey            = "TRANSMOGR_CONFIG"
	defaultCDCPublication       = "transmogr_publication"
	defaultCDCSlot              = "transmogr_slot"
	defaultCDCStatusInterval    = 10 * time.Second
	defaultPollingInterval      = 5 * time.Second
	defaultCryptoActiveKeyID    = "current"
	defaultCryptoActiveKeyEnv   = "TRANSMOGR_CRYPTO_KEY"
	defaultCryptoKeyringEnv     = "TRANSMOGR_CRYPTO_KEYRING"
	defaultOutboxMaxBatchEvents = 1000
)

// Config is the top-level application configuration loaded from YAML.
type Config struct {
	// Region identifies this node's region. Required.
	Region string `yaml:"region"`

	// LogLevel sets the minimum log severity. One of debug, info, warn, error. Default: info.
	LogLevel string `yaml:"log_level"`

	// LogFormat sets the log output format. One of text, json. Default: text.
	LogFormat string `yaml:"log_format"`

	// Transport groups all transport-layer settings.
	Transport TransportConfig `yaml:"transport"`

	// Crypto contains encryption key management settings.
	Crypto CryptoConfig `yaml:"crypto"`

	// Replication controls change capture, broadcast to peers, and how inbound events are applied.
	Replication ReplicationConfig `yaml:"replication"`

	// Lease controls distributed lease behaviour used to coordinate workers across pods.
	Lease LeaseConfig `yaml:"lease"`

	// Outbox controls outbound event staging and retention.
	Outbox OutboxConfig `yaml:"outbox"`

	// DB holds the connection settings for the shared PostgreSQL database.
	// All pods in a region must connect to the same instance.
	DB DatabaseConfig `yaml:"db"`

	// Peers is the list of remote regional endpoints to replicate with.
	// Each entry must have a unique region name and a stable ingress address (e.g. a Kubernetes Service).
	// A PostgreSQL outbox is always used.
	Peers []models.Endpoint `yaml:"peers"`

	// Tables lists the application tables to replicate.
	// Each entry requires name, primary_key, and region_field.
	// updated_at_field is required when changefeed.source = polling.
	Tables []models.TableSpec `yaml:"tables"`

	// Transformers configures transport-side payload transformations applied
	// before outbound gRPC encoding and before inbound local apply.
	Transformers []models.TransformerSpec `yaml:"transformers"`
}

// ReplicationConfig controls change capture, event broadcasting, and inbound event application.
type ReplicationConfig struct {
	// Source configures the change capture backend (CDC or polling).
	Source SourceConfig `yaml:"source"`

	// Snapshot controls snapshot transfer behaviour.
	Snapshot SnapshotConfig `yaml:"snapshot"`

	// ApplyMode controls how inbound events received from peers are written locally.
	// Default: upsert.
	//
	//   insert_only — only inserts new rows; existing rows and deletes are ignored.
	//   upsert      — inserts and updates rows but does not apply deletes.
	//   full_sync   — applies inserts, updates, and deletes; a complete mirror.
	ApplyMode models.ReplicationApplyMode `yaml:"apply_mode"`

	// BroadcastMode controls which local change events are published to peers.
	// Default: full_sync.
	//
	//   insert_only — only broadcasts new-row events.
	//   upsert      — broadcasts inserts and updates but not deletes.
	//   full_sync   — broadcasts all events including deletes.
	BroadcastMode models.ReplicationBroadcastMode `yaml:"broadcast_mode"`

	// MaxBatchEvents is the maximum number of events per outbound batch. Large transactions
	// are split into multiple batches to stay within the gRPC message size limit. Default: 1000.
	MaxBatchEvents int `yaml:"max_batch_events"`
}

// SourceConfig selects and configures the change capture backend.
type SourceConfig struct {
	// Type selects the capture backend. One of cdc, polling. Default: cdc.
	//
	//   cdc     — PostgreSQL logical replication; captures inserts, updates, and deletes.
	//             Requires wal_level=logical and a replication-capable user.
	//   polling — periodic table scan ordered by (updated_at_field, primary_key);
	//             emits only upsert events; does not observe hard deletes.
	Type string `yaml:"type"`

	// CDC contains settings used only when type = cdc.
	CDC CDCSourceConfig `yaml:"cdc"`

	// Polling contains settings used only when type = polling.
	Polling PollingSourceConfig `yaml:"polling"`
}

// CDCSourceConfig contains PostgreSQL logical replication settings.
type CDCSourceConfig struct {
	// Publication is the PostgreSQL logical replication publication name. Default: transmogr_publication.
	Publication string `yaml:"publication"`

	// Slot is the PostgreSQL replication slot name. Default: transmogr_slot.
	// Created automatically on first start if it does not exist.
	Slot string `yaml:"slot"`

	// StatusInterval is how often the CDC adapter sends WAL keepalive status to PostgreSQL. Default: 10s.
	StatusInterval time.Duration `yaml:"status_interval"`

	// UpdateDiff enables field-level diff for UPDATE events.
	// When true, only changed fields are sent instead of the full row image.
	// Recommended for tables with TOASTed columns because full-row CDC cannot
	// reconstruct unchanged TOAST values from pgoutput WAL tuples.
	// Requires REPLICA IDENTITY FULL — set automatically on all configured tables at startup.
	// Incompatible with type = polling.
	UpdateDiff bool `yaml:"update_diff"`
}

// PollingSourceConfig contains updated_at-based polling settings.
type PollingSourceConfig struct {
	// Interval is how often tables are queried for new or updated rows. Default: 5s.
	Interval time.Duration `yaml:"interval"`
}

// DatabaseConfig contains connection settings for a database.
type DatabaseConfig struct {
	Postgres PostgresConfig `yaml:"postgres"`
}

// PostgresConfig contains PostgreSQL-specific connection settings.
type PostgresConfig struct {
	// DSN is the PostgreSQL connection string. Required.
	// Accepts both keyword/value format (host=... user=... dbname=...)
	// and URL format (postgres://user:pass@host/dbname?sslmode=...).
	// See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
	//
	// Pool settings can be configured via query parameters:
	//   pool_max_conns         — maximum number of open connections (default: max(4, numCPU))
	//   pool_min_conns         — minimum number of idle connections (default: 0)
	//   pool_max_conn_lifetime — maximum connection lifetime (default: 1h)
	//   pool_max_conn_idle_time — maximum idle time before connection is closed (default: 30m)
	//   pool_health_check_period — interval between health checks (default: 1m)
	// Example: postgres://user:pass@host/db?pool_max_conns=10&pool_min_conns=2
	DSN string `yaml:"dsn"`

	// StateSchema is the PostgreSQL schema used for Transmogr state tables.
	// Must be created by transmogr-init (or another migration runner) before the application starts.
	// Default: transmogr.
	StateSchema string `yaml:"state_schema"`
}

// CryptoConfig contains encryption engine settings.
type CryptoConfig struct {
	// RefreshInterval is how often the key manager re-fetches keys from sources.
	// Refresh failures leave the last successful in-memory snapshot in place.
	// Startup fails if the initial load cannot produce all configured key IDs. Default: 1m.
	RefreshInterval time.Duration `yaml:"refresh_interval"`

	// RetryAttempts is the number of attempts per load/refresh cycle. Default: 3.
	RetryAttempts int `yaml:"retry_attempts"`

	// RetryDelay is the wait between retry attempts. Default: 1s.
	RetryDelay time.Duration `yaml:"retry_delay"`

	Source CryptoSourceConfig `yaml:"source"`
}

// CryptoSourceConfig selects and configures the crypto key source.
// All configured sources are merged in order: http, vault, env.
// Later sources override earlier keys with the same key_id.
// HTTP is enabled when http.url is set; Vault when both vault.address and vault.path are set;
// Env when ActiveKeyEnv resolves to a non-empty value in the process environment.
type CryptoSourceConfig struct {
	HTTP CryptoHTTPConfig `yaml:"http"`

	Vault CryptoVaultConfig `yaml:"vault"`

	Env CryptoEnvConfig `yaml:"env"`
}

// CryptoEnvConfig contains env-backed key source settings.
// The env source is active only when ActiveKeyEnv resolves to a non-empty value in the process environment.
// KeyringEnv is optional and adds extra decrypt-only keys when present.
type CryptoEnvConfig struct {
	// ActiveKeyID is the key_id assigned to the key read from ActiveKeyEnv.
	ActiveKeyID string `yaml:"active_key_id"`

	// ActiveKeyEnv is the name of the env var holding the base64-encoded active key.
	// Conventionally TRANSMOGR_CRYPTO_KEY. Required only when at least one transformer uses crypto.
	ActiveKeyEnv string `yaml:"active_key_env"`

	// KeyringEnv is the name of the env var holding an optional JSON object of additional keys,
	// keyed by key_id. Conventionally TRANSMOGR_CRYPTO_KEYRING.
	// Each value must be a base64-encoded key compatible with the field's algorithm.
	// Example: {"oldKey": "<base64>", "archiveKey": "<base64>"}
	KeyringEnv string `yaml:"keyring_env"`
}

// CryptoHTTPConfig contains HTTP-backed key source settings.
// The HTTP source is enabled when URL is non-empty.
// Expected JSON response: {"active_key_id": "...", "keys": {"<id>": "<base64>"}, "version": "..."}.
type CryptoHTTPConfig struct {
	// URL is the endpoint to fetch keys from. Setting this enables the HTTP source.
	URL string `yaml:"url"`

	// Timeout is the HTTP request deadline. Default: 5s. Zero uses the default.
	Timeout time.Duration `yaml:"timeout"`

	// TokenEnv is the name of the env var holding the bearer token sent in the request.
	// If empty, no authentication header is added.
	TokenEnv string `yaml:"token_env"`

	// HeaderName is the HTTP header used to send the token. Default: Authorization.
	// Only used when TokenEnv is set.
	HeaderName string `yaml:"header_name"`
}

// CryptoVaultConfig contains Vault-backed key source settings.
// The Vault source is enabled when both Address and Path are non-empty.
// Expects a KV v2 read response; active_key_id, keys, and optional version must be inside data.data.
type CryptoVaultConfig struct {
	// Address is the Vault server URL (e.g. https://vault.example.com). Required when vault source is configured.
	Address string `yaml:"address"`

	// TokenEnv is the name of the env var holding the Vault token used for authentication.
	TokenEnv string `yaml:"token_env"`

	// Path is the KV v2 secret path (e.g. secret/data/transmogr/keys). Required when vault source is configured.
	Path string `yaml:"path"`

	// Timeout is the Vault request deadline. Default: 5s. Zero uses the default.
	Timeout time.Duration `yaml:"timeout"`
}

// TransportConfig groups all transport-layer settings.
type TransportConfig struct {
	// ReconnectDelay is the wait between peer reconnection attempts. Default: 5s.
	ReconnectDelay time.Duration `yaml:"reconnect_delay"`

	// PingInterval is how often peer health pings are sent. Default: 15s.
	PingInterval time.Duration `yaml:"ping_interval"`

	GRPC    GRPCTransportConfig `yaml:"grpc"`
	Metrics MetricsConfig       `yaml:"metrics"`
}

// MetricsConfig contains settings for the metrics HTTP server.
type MetricsConfig struct {
	// ListenAddr is the address the metrics HTTP server (GET /metrics, /livez, /readyz) binds to.
	// If empty, the metrics server is disabled.
	ListenAddr string `yaml:"listen_addr"`
}

// GRPCTransportConfig contains gRPC server and client settings.
type GRPCTransportConfig struct {
	// ListenAddr is the address the gRPC server binds to. Default: :8443.
	ListenAddr string `yaml:"listen_addr"`

	// Insecure explicitly opts out of TLS, allowing plaintext gRPC transport.
	// Must be set to true when TLS is not configured; omitting both TLS and Insecure
	// is a validation error to prevent accidental insecure deployments.
	Insecure bool `yaml:"insecure"`

	// TLS contains mTLS settings for peer-to-peer transport.
	// Mutually exclusive with Insecure.
	TLS GRPCTLSConfig `yaml:"tls"`
}

// GRPCTLSConfig contains mTLS settings for peer transport.
type GRPCTLSConfig struct {
	// CertFile is the path to the PEM-encoded certificate. Setting this enables mTLS.
	CertFile string `yaml:"cert_file"`

	// KeyFile is the path to the PEM-encoded private key. Required when CertFile is set.
	KeyFile string `yaml:"key_file"`

	// ClientCAFile is the path to the PEM-encoded CA certificate used to verify peer certs.
	// Required when CertFile is set.
	ClientCAFile string `yaml:"client_ca_file"`

	// ServerName overrides TLS hostname verification. If empty, the peer endpoint host is used.
	ServerName string `yaml:"server_name"`
}

// LeaseConfig contains distributed lease settings.
// Leases coordinate outbound stream ownership and CDC/snapshot workers across pods.
type LeaseConfig struct {
	// TTL is how long a lease remains valid without renewal. Default: 15s.
	TTL time.Duration `yaml:"ttl"`

	// RenewInterval is how often a held lease is renewed. Default: 5s.
	// Must be less than TTL.
	RenewInterval time.Duration `yaml:"renew_interval"`

	// CleanupInterval is how often expired leases are purged from the database. Default: 5m.
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
}

// SnapshotConfig controls snapshot transfer behaviour.
type SnapshotConfig struct {
	// ChunkSize is the number of rows fetched and sent per snapshot chunk.
	// Default: 1000.
	ChunkSize int `yaml:"chunk_size"`
}

// OutboxConfig contains outbound staging settings.
// The outbox always uses PostgreSQL for durable event staging.
type OutboxConfig struct {
	// CleanupInterval is how often outbox retention cleanup runs. Default: 5m.
	CleanupInterval time.Duration `yaml:"cleanup_interval"`

	// MaxBatchAge is how long an unacknowledged batch is retained before cleanup
	// can delete it, potentially dropping undelivered events for a disconnected
	// peer. Default: 7d.
	MaxBatchAge time.Duration `yaml:"max_batch_age"`

	// MaxPendingPerPeer is the maximum pending batch count allowed per peer
	// before cleanup starts dropping the oldest unacknowledged batches.
	// Default: 100000.
	MaxPendingPerPeer int `yaml:"max_pending_per_peer"`
}

// ConfigPathFromEnv returns the config path from environment or the default path.
func ConfigPathFromEnv() string {
	if value := strings.TrimSpace(os.Getenv(configPathEnvKey)); value != "" {
		return value
	}

	return defaultConfigPath
}

// Load reads, decodes, defaults, and validates the application config.
func Load(path string) (Config, error) {
	cleanPath := filepath.Clean(path)

	// #nosec G304 -- configuration path is intentionally operator-supplied.
	raw, err := os.ReadFile(cleanPath)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", cleanPath, err)
	}

	expanded := os.Expand(string(raw), os.Getenv)

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", cleanPath, err)
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", cleanPath, err)
	}

	return cfg, nil
}

func (c *Config) applyDefaults() {
	if !isConfigured(c.LogLevel) {
		c.LogLevel = "info"
	}
	if !isConfigured(c.LogFormat) {
		c.LogFormat = "text"
	}

	if c.Transport.ReconnectDelay <= 0 {
		c.Transport.ReconnectDelay = 5 * time.Second
	}
	if c.Transport.PingInterval <= 0 {
		c.Transport.PingInterval = 15 * time.Second
	}

	if !isConfigured(c.Transport.GRPC.ListenAddr) {
		c.Transport.GRPC.ListenAddr = ":8443"
	}

	if c.Crypto.RefreshInterval <= 0 {
		c.Crypto.RefreshInterval = time.Minute
	}
	if c.Crypto.RetryAttempts <= 0 {
		c.Crypto.RetryAttempts = 3
	}
	if c.Crypto.RetryDelay <= 0 {
		c.Crypto.RetryDelay = time.Second
	}

	if !isConfigured(c.Replication.Source.Type) {
		c.Replication.Source.Type = "cdc"
	}
	if !isConfigured(c.Replication.Source.CDC.Publication) {
		c.Replication.Source.CDC.Publication = defaultCDCPublication
	}
	if !isConfigured(c.Replication.Source.CDC.Slot) {
		c.Replication.Source.CDC.Slot = defaultCDCSlot
	}
	if c.Replication.Source.CDC.StatusInterval <= 0 {
		c.Replication.Source.CDC.StatusInterval = defaultCDCStatusInterval
	}
	if c.Replication.Source.Polling.Interval <= 0 {
		c.Replication.Source.Polling.Interval = defaultPollingInterval
	}

	if !isConfigured(c.Crypto.Source.Env.ActiveKeyID) {
		c.Crypto.Source.Env.ActiveKeyID = defaultCryptoActiveKeyID
	}
	if !isConfigured(c.Crypto.Source.Env.ActiveKeyEnv) {
		c.Crypto.Source.Env.ActiveKeyEnv = defaultCryptoActiveKeyEnv
	}
	if !isConfigured(c.Crypto.Source.Env.KeyringEnv) {
		c.Crypto.Source.Env.KeyringEnv = defaultCryptoKeyringEnv
	}

	if c.Replication.ApplyMode == "" {
		c.Replication.ApplyMode = models.ReplicationApplyModeUpsert
	}
	if c.Replication.BroadcastMode == "" {
		c.Replication.BroadcastMode = models.ReplicationBroadcastModeFullSync
	}

	if c.Lease.TTL <= 0 {
		c.Lease.TTL = 15 * time.Second
	}
	if c.Lease.RenewInterval <= 0 {
		c.Lease.RenewInterval = 5 * time.Second
	}
	if c.Lease.CleanupInterval <= 0 {
		c.Lease.CleanupInterval = 5 * time.Minute
	}

	if c.Replication.Snapshot.ChunkSize <= 0 {
		c.Replication.Snapshot.ChunkSize = 1000
	}

	if c.Outbox.CleanupInterval <= 0 {
		c.Outbox.CleanupInterval = 5 * time.Minute
	}
	if c.Outbox.MaxBatchAge <= 0 {
		c.Outbox.MaxBatchAge = 7 * 24 * time.Hour
	}
	if c.Outbox.MaxPendingPerPeer <= 0 {
		c.Outbox.MaxPendingPerPeer = 100000
	}

	if c.Replication.MaxBatchEvents <= 0 {
		c.Replication.MaxBatchEvents = defaultOutboxMaxBatchEvents
	}

	if !isConfigured(c.DB.Postgres.StateSchema) {
		c.DB.Postgres.StateSchema = "transmogr"
	}
}

// Validate checks that required configuration fields are present.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Region) == "" {
		return errors.New("region is required")
	}

	switch strings.ToLower(strings.TrimSpace(c.LogLevel)) {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("log_level must be one of debug, info, warn, or error")
	}
	switch strings.ToLower(strings.TrimSpace(c.LogFormat)) {
	case "text", "json":
	default:
		return errors.New("log_format must be one of text or json")
	}

	if c.Transport.PingInterval <= 0 {
		return errors.New("transport.ping_interval must be positive")
	}

	if strings.TrimSpace(c.Transport.GRPC.ListenAddr) == "" {
		return errors.New("transport.grpc.listen_addr is required")
	}
	hasTLS := strings.TrimSpace(c.Transport.GRPC.TLS.CertFile) != ""
	if !hasTLS && !c.Transport.GRPC.Insecure {
		return errors.New("transport.grpc: either tls.cert_file or insecure must be set")
	}
	if hasTLS && c.Transport.GRPC.Insecure {
		return errors.New("transport.grpc: tls.cert_file and insecure are mutually exclusive")
	}
	if hasTLS {
		if strings.TrimSpace(c.Transport.GRPC.TLS.KeyFile) == "" {
			return errors.New("transport.grpc.tls.key_file is required when cert_file is set")
		}
		if strings.TrimSpace(c.Transport.GRPC.TLS.ClientCAFile) == "" {
			return errors.New("transport.grpc.tls.client_ca_file is required when cert_file is set")
		}
	}

	if c.Crypto.RefreshInterval <= 0 {
		return errors.New("crypto.refresh_interval must be positive")
	}
	if c.Crypto.RetryAttempts <= 0 {
		return errors.New("crypto.retry_attempts must be positive")
	}
	if c.Crypto.RetryDelay <= 0 {
		return errors.New("crypto.retry_delay must be positive")
	}
	if strings.TrimSpace(c.Crypto.Source.Vault.Address) != "" ||
		strings.TrimSpace(c.Crypto.Source.Vault.Path) != "" {
		if strings.TrimSpace(c.Crypto.Source.Vault.Address) == "" {
			return errors.New("crypto.source.vault.address is required when vault source is configured")
		}
		if strings.TrimSpace(c.Crypto.Source.Vault.Path) == "" {
			return errors.New("crypto.source.vault.path is required when vault source is configured")
		}
	}

	switch strings.ToLower(strings.TrimSpace(c.Replication.Source.Type)) {
	case "cdc", "polling":
	default:
		return errors.New("replication.source.type must be one of cdc or polling")
	}
	if c.Replication.Source.CDC.UpdateDiff && strings.EqualFold(strings.TrimSpace(c.Replication.Source.Type), "polling") {
		return errors.New("replication.source.cdc.update_diff is not supported with polling source")
	}
	if c.Replication.Source.Polling.Interval <= 0 {
		return errors.New("replication.source.polling.interval must be positive")
	}
	if c.Replication.Source.CDC.StatusInterval <= 0 {
		return errors.New("replication.source.cdc.status_interval must be positive")
	}

	switch c.Replication.ApplyMode {
	case models.ReplicationApplyModeInsertOnly, models.ReplicationApplyModeUpsert, models.ReplicationApplyModeFullSync:
	default:
		return errors.New("replication.apply_mode must be one of insert_only, upsert, or full_sync")
	}
	switch c.Replication.BroadcastMode {
	case models.ReplicationBroadcastModeInsertOnly,
		models.ReplicationBroadcastModeUpsert,
		models.ReplicationBroadcastModeFullSync:
	default:
		return errors.New("replication.broadcast_mode must be one of insert_only, upsert, or full_sync")
	}

	if c.Lease.TTL <= 0 {
		return errors.New("lease.ttl must be positive")
	}
	if c.Lease.RenewInterval <= 0 {
		return errors.New("lease.renew_interval must be positive")
	}
	if c.Lease.RenewInterval >= c.Lease.TTL {
		return errors.New("lease.renew_interval must be less than lease.ttl")
	}
	if c.Lease.CleanupInterval <= 0 {
		return errors.New("lease.cleanup_interval must be positive")
	}

	if c.Replication.Snapshot.ChunkSize <= 0 {
		return errors.New("replication.snapshot.chunk_size must be positive")
	}

	if c.Outbox.CleanupInterval <= 0 {
		return errors.New("outbox.cleanup_interval must be positive")
	}
	if c.Outbox.MaxBatchAge <= 0 {
		return errors.New("outbox.max_batch_age must be positive")
	}
	if c.Outbox.MaxPendingPerPeer <= 0 {
		return errors.New("outbox.max_pending_per_peer must be positive")
	}

	if strings.TrimSpace(c.DB.Postgres.DSN) == "" {
		return errors.New("db.postgres.dsn is required")
	}
	if strings.TrimSpace(c.DB.Postgres.StateSchema) == "" {
		return errors.New("db.postgres.state_schema is required")
	}

	if len(c.Tables) == 0 {
		return errors.New("at least one table is required")
	}
	for i, table := range c.Tables {
		if err := validateTable(i, table, c.Replication.Source.Type); err != nil {
			return err
		}
	}
	tableIndexes := make(map[string]int, len(c.Tables))
	for i, table := range c.Tables {
		tableIndexes[strings.TrimSpace(table.Name)] = i
	}
	for i, transformer := range c.Transformers {
		if err := validateTransformer(i, transformer, tableIndexes); err != nil {
			return err
		}
	}

	for i, endpoint := range c.Peers {
		if strings.TrimSpace(endpoint.Region) == "" {
			return fmt.Errorf("peers[%d].region is required", i)
		}
		if strings.TrimSpace(endpoint.Endpoint) == "" {
			return fmt.Errorf("peers[%d].endpoint is required", i)
		}
	}
	seenPeerRegions := make(map[string]int, len(c.Peers))
	for i, endpoint := range c.Peers {
		region := strings.TrimSpace(endpoint.Region)
		if prev, ok := seenPeerRegions[region]; ok {
			return fmt.Errorf("peers[%d].region duplicates peers[%d].region %q", i, prev, region)
		}
		seenPeerRegions[region] = i
	}

	return nil
}

func validateTable(i int, table models.TableSpec, changefeedSource string) error {
	if !isConfigured(table.Name) {
		return fmt.Errorf("tables[%d].name is required", i)
	}
	if len(table.PrimaryKey) == 0 {
		return fmt.Errorf("tables[%d].primary_key must contain at least one column", i)
	}
	for j, column := range table.PrimaryKey {
		if !isConfigured(column) {
			return fmt.Errorf("tables[%d].primary_key[%d] is required", i, j)
		}
	}
	if !isConfigured(table.RegionField) {
		return fmt.Errorf("tables[%d].region_field is required", i)
	}
	if strings.EqualFold(changefeedSource, "polling") && !isConfigured(table.UpdatedAtField) {
		return fmt.Errorf("tables[%d].updated_at_field is required for changefeed.source polling", i)
	}
	return nil
}

func validateTransformer(
	transformerIdx int,
	spec models.TransformerSpec,
	tableIndexes map[string]int,
) error {
	if !isConfigured(spec.Type) {
		return fmt.Errorf("transformers[%d].type is required", transformerIdx)
	}
	if !isConfigured(spec.Table) {
		return fmt.Errorf("transformers[%d].table is required", transformerIdx)
	}
	if _, ok := tableIndexes[strings.TrimSpace(spec.Table)]; !ok {
		return fmt.Errorf("transformers[%d].table references unknown table %q", transformerIdx, spec.Table)
	}

	switch strings.ToLower(strings.TrimSpace(spec.Type)) {
	case "crypto":
		return validateCryptoTransformerSpec(transformerIdx, spec)
	default:
		return fmt.Errorf("transformers[%d].type %q is not supported", transformerIdx, spec.Type)
	}
}

func validateCryptoTransformerSpec(transformerIdx int, spec models.TransformerSpec) error {
	if !isConfigured(spec.CryptoType) {
		return fmt.Errorf("transformers[%d].crypto_type is required", transformerIdx)
	}
	if len(spec.Fields) == 0 {
		return fmt.Errorf("transformers[%d].fields must contain at least one field", transformerIdx)
	}
	for fieldIdx, field := range spec.Fields {
		if !isConfigured(field) {
			return fmt.Errorf("transformers[%d].fields[%d] is required", transformerIdx, fieldIdx)
		}
	}
	if !isConfigured(spec.KeyID) {
		return fmt.Errorf("transformers[%d].key_id is required", transformerIdx)
	}
	return nil
}

// TableNames returns configured table names in declaration order.
func (c *Config) TableNames() []string {
	names := make([]string, 0, len(c.Tables))
	for _, table := range c.Tables {
		names = append(names, table.Name)
	}

	return names
}
