package models

import (
	"testing"
)

func TestBuildHandshakeConfigurationNormalises(t *testing.T) {
	t.Parallel()

	tables := []TableSpec{
		{Name: "orders", PrimaryKey: []string{"id"}, RegionField: "region"},
		{Name: "accounts", PrimaryKey: []string{"id"}, RegionField: "region"},
	}
	cols := map[string][]ColumnInfo{
		"orders": {
			{Name: "total", DataType: "numeric"},
			{Name: "id", DataType: "bigint"},
		},
		"accounts": {
			{Name: "name", DataType: "text"},
			{Name: "id", DataType: "bigint"},
		},
	}

	// Reversed table and column order — should produce the same JSON.
	tablesRev := []TableSpec{tables[1], tables[0]}
	colsRev := map[string][]ColumnInfo{
		"orders":   {cols["orders"][1], cols["orders"][0]},
		"accounts": {cols["accounts"][1], cols["accounts"][0]},
	}

	got1 := BuildHandshakeConfiguration("cdc", tables, cols)
	got2 := BuildHandshakeConfiguration("cdc", tablesRev, colsRev)

	if got1 != got2 {
		t.Fatalf("expected identical normalised output\ngot1: %s\ngot2: %s", got1, got2)
	}

	// Verify tables appear in sorted order.
	cfg, err := ParseHandshakeConfiguration(got1)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Tables[0].Name != "accounts" || cfg.Tables[1].Name != "orders" {
		t.Fatalf("expected tables sorted by name, got %v", cfg.Tables)
	}
	if cfg.Tables[1].Columns[0].Name != "id" || cfg.Tables[1].Columns[1].Name != "total" {
		t.Fatalf("expected columns sorted by name, got %v", cfg.Tables[1].Columns)
	}
}
