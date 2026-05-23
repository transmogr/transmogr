package models

// TableSpec describes a replicated application table from configuration.
type TableSpec struct {
	Name           string   `yaml:"name"`
	PrimaryKey     []string `yaml:"primary_key"`
	UpdatedAtField string   `yaml:"updated_at_field"`
	RegionField    string   `yaml:"region_field"`
}

// ColumnInfo describes one column as reported by the database schema.
type ColumnInfo struct {
	Name     string `json:"name"`
	DataType string `json:"data_type"`
}

// TransformerSpec describes one transport transformer applied to one table.
type TransformerSpec struct {
	Type       string   `yaml:"type"`
	Table      string   `yaml:"table"`
	Fields     []string `yaml:"fields"`
	KeyID      string   `yaml:"key_id"`
	CryptoType string   `yaml:"crypto_type"`
}
