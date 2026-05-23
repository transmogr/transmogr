package protocol

import (
	"strings"
	"testing"

	"github.com/transmogr/transmogr/internal/models"
	peerv1 "github.com/transmogr/transmogr/pkg/proto/transmogr/peerv1"
)

func TestCheckHandshakeCompatibility(t *testing.T) {
	t.Parallel()

	tables := []models.TableSpec{
		{Name: "orders", PrimaryKey: []string{"id"}},
		{Name: "users", PrimaryKey: []string{"id"}},
	}
	cfg := models.BuildHandshakeConfiguration("cdc", tables, nil)

	local := models.HandshakeMessage{Version: "v1", Configuration: cfg}

	remote := func(version, configuration string) *peerv1.Handshake {
		return &peerv1.Handshake{Version: version, Configuration: configuration}
	}

	tests := []struct {
		name        string
		local       models.HandshakeMessage
		remote      *peerv1.Handshake
		wantErr     bool
		errContains string
	}{
		{
			name:   "compatible peers",
			local:  local,
			remote: remote("v1", cfg),
		},
		{
			name:  "table order does not matter",
			local: local,
			remote: remote("v1", models.BuildHandshakeConfiguration("cdc", []models.TableSpec{
				{Name: "users", PrimaryKey: []string{"id"}},
				{Name: "orders", PrimaryKey: []string{"id"}},
			}, nil)),
		},
		{
			name:        "version mismatch",
			local:       local,
			remote:      remote("v2", cfg),
			wantErr:     true,
			errContains: "peer version mismatch",
		},
		{
			name:  "remote has extra table",
			local: local,
			remote: remote("v1", models.BuildHandshakeConfiguration("cdc", append(tables,
				models.TableSpec{Name: "payments", PrimaryKey: []string{"id"}},
			), nil)),
			wantErr:     true,
			errContains: "peer table set does not match",
		},
		{
			name: "local has extra table",
			local: models.HandshakeMessage{
				Version: "v1",
				Configuration: models.BuildHandshakeConfiguration("cdc", append(tables,
					models.TableSpec{Name: "payments", PrimaryKey: []string{"id"}},
				), nil),
			},
			remote:      remote("v1", cfg),
			wantErr:     true,
			errContains: "peer table set does not match",
		},
		{
			name:        "invalid remote configuration",
			local:       local,
			remote:      remote("v1", "not-valid-json"),
			wantErr:     true,
			errContains: "invalid peer configuration",
		},
		{
			name:   "both sides have no tables",
			local:  models.HandshakeMessage{Version: "v1", Configuration: models.BuildHandshakeConfiguration("cdc", nil, nil)},
			remote: remote("v1", models.BuildHandshakeConfiguration("cdc", nil, nil)),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := CheckHandshakeCompatibility(tc.local, tc.remote)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
