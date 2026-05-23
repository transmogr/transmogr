package migrations

import "testing"

func TestMigrationPaths(t *testing.T) {
	t.Parallel()

	upPaths, err := migrationPaths("*.up.sql")
	if err != nil {
		t.Fatalf("migrationPaths(up): %v", err)
	}
	if len(upPaths) == 0 {
		t.Fatal("expected at least one up migration")
	}

	downPaths, err := migrationPaths("*.down.sql")
	if err != nil {
		t.Fatalf("migrationPaths(down): %v", err)
	}
	if len(downPaths) == 0 {
		t.Fatal("expected at least one down migration")
	}
}
