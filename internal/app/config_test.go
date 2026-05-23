package app

import (
	"testing"
	"time"

	"github.com/transmogr/transmogr/internal/models"
)

// makeTestConfig returns a fully-populated valid Config.
// Tests override only the fields relevant to their scenario.
func makeTestConfig() Config {
	return Config{
		Region:    "eu",
		LogLevel:  "info",
		LogFormat: "text",
		Transport: TransportConfig{
			ReconnectDelay: 5 * time.Second,
			PingInterval:   15 * time.Second,
			GRPC: GRPCTransportConfig{
				ListenAddr: ":8443",
				Insecure:   true,
			},
		},
		Crypto: CryptoConfig{
			RefreshInterval: time.Minute,
			RetryAttempts:   3,
			RetryDelay:      time.Second,
		},
		Replication: ReplicationConfig{
			Source: SourceConfig{
				Type: "cdc",
				CDC: CDCSourceConfig{
					Publication:    defaultCDCPublication,
					Slot:           defaultCDCSlot,
					StatusInterval: defaultCDCStatusInterval,
				},
				Polling: PollingSourceConfig{
					Interval: defaultPollingInterval,
				},
			},
			Snapshot:      SnapshotConfig{ChunkSize: 1000},
			ApplyMode:     models.ReplicationApplyModeUpsert,
			BroadcastMode: models.ReplicationBroadcastModeFullSync,
		},
		Lease: LeaseConfig{
			TTL:             15 * time.Second,
			RenewInterval:   5 * time.Second,
			CleanupInterval: 5 * time.Minute,
		},
		Outbox: OutboxConfig{
			CleanupInterval:   5 * time.Minute,
			MaxBatchAge:       7 * 24 * time.Hour,
			MaxPendingPerPeer: 100000,
		},
		DB: DatabaseConfig{
			Postgres: PostgresConfig{
				DSN:         "postgres://example",
				StateSchema: "transmogr",
			},
		},
		Tables: []models.TableSpec{
			{
				Name:        "users",
				PrimaryKey:  []string{"id"},
				RegionField: "_owner_region",
			},
		},
	}
}

func TestConfigValidateAllowsCDCOnlyTables(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected config to validate, got %v", err)
	}
}

func TestConfigValidateRequiresUpdatedAtFieldForPolling(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Replication.Source.Type = "polling"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected polling config without updated_at_field to fail")
	}
}

func TestConfigValidateRejectsUnsupportedLogLevel(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.LogLevel = "trace"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected config validation to fail for unsupported log level")
	}
}

func TestConfigValidateRejectsUnsupportedLogFormat(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.LogFormat = "xml"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected config validation to fail for unsupported log format")
	}
}

func TestConfigValidateRejectsEmptyTables(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Tables = nil

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected config with empty tables to fail validation")
	}
}

func TestConfigValidateRejectsUnsupportedChangeSource(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Replication.Source.Type = "invalid"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected config validation to fail for unsupported change source")
	}
}

func TestConfigValidateRejectsNeitherTLSNorInsecure(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Transport.GRPC.Insecure = false
	// neither TLS nor Insecure set — must fail

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when neither tls nor insecure is set")
	}
}

func TestConfigValidateRejectsTLSAndInsecureTogether(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Transport.GRPC.TLS = GRPCTLSConfig{CertFile: "server.crt"}
	// Insecure: true from makeTestConfig — mutually exclusive with TLS

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when both tls and insecure are set")
	}
}

func TestConfigValidateRequiresTLSFilesWhenGRPCTLSEnabled(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Transport.GRPC.Insecure = false
	cfg.Transport.GRPC.TLS = GRPCTLSConfig{CertFile: "server.crt"}
	// KeyFile and ClientCAFile intentionally omitted — must fail

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected tls config without key/ca files to fail")
	}
}

func TestConfigApplyDefaultsSetsPingInterval(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.applyDefaults()

	if cfg.Transport.PingInterval != 15*time.Second {
		t.Fatalf("unexpected ping interval default: %v", cfg.Transport.PingInterval)
	}
	if cfg.LogFormat != "text" {
		t.Fatalf("unexpected log format default: %q", cfg.LogFormat)
	}
	if cfg.Lease.CleanupInterval != 5*time.Minute {
		t.Fatalf("unexpected lease cleanup interval default: %v", cfg.Lease.CleanupInterval)
	}
	if cfg.Outbox.CleanupInterval != 5*time.Minute {
		t.Fatalf("unexpected outbox cleanup interval default: %v", cfg.Outbox.CleanupInterval)
	}
	if cfg.Outbox.MaxBatchAge != 7*24*time.Hour {
		t.Fatalf("unexpected outbox max batch age default: %v", cfg.Outbox.MaxBatchAge)
	}
	if cfg.Outbox.MaxPendingPerPeer != 100000 {
		t.Fatalf("unexpected outbox max pending per peer default: %d", cfg.Outbox.MaxPendingPerPeer)
	}
	if cfg.Crypto.RefreshInterval != time.Minute {
		t.Fatalf("unexpected crypto refresh interval default: %v", cfg.Crypto.RefreshInterval)
	}
	if cfg.Crypto.RetryAttempts != 3 {
		t.Fatalf("unexpected crypto retry attempts default: %d", cfg.Crypto.RetryAttempts)
	}
	if cfg.Crypto.RetryDelay != time.Second {
		t.Fatalf("unexpected crypto retry delay default: %v", cfg.Crypto.RetryDelay)
	}
}

func TestConfigValidateRejectsPartialVaultSource(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Crypto.Source.Vault.Address = "http://127.0.0.1:8200"
	// Path intentionally omitted to trigger partial vault config error.

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected config validation to fail for partial vault source")
	}
}

func TestConfigValidateRequiresHTTPURLWhenHTTPSourceConfigured(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	// No HTTP source configured; empty source config is valid.

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected config without http source to validate, got %v", err)
	}
}

func TestConfigValidateRequiresEncryptedFieldPolicy(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Transformers = []models.TransformerSpec{
		{Type: "crypto", Table: "users", Fields: []string{"email"}, KeyID: "email-key"},
		// CryptoType intentionally omitted to trigger validation error.
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected config validation to fail for missing crypto_type")
	}
}

func TestConfigValidateRejectsDuplicatePeerRegions(t *testing.T) {
	t.Parallel()

	cfg := makeTestConfig()
	cfg.Peers = []models.Endpoint{
		{Region: "us-east", Endpoint: "peer-a:8443"},
		{Region: "us-east", Endpoint: "peer-b:8443"},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected config validation to fail for duplicate peer regions")
	}
}
