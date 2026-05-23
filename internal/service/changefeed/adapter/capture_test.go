package adapter

import (
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/transmogr/transmogr/internal/models"
)

func TestBuildWriteChangeUsesTuplePayload(t *testing.T) {
	t.Parallel()

	adapter := &ChangeDataCapture{localRegion: "eu"}
	relation := postgresRelation{
		tableName:  "users",
		primaryKey: []string{"id"},
		primaryKeyTypes: map[string]models.PrimaryKeyType{
			"id": models.PrimaryKeyTypeString,
		},
		columnByName: map[string]int{
			"id":     0,
			"region": 1,
			"name":   2,
		},
		regionField: "region",
	}
	tuple := &pglogrepl.TupleData{
		ColumnNum: 3,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("u-1")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("eu")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("Alice")},
		},
	}

	change, ok, err := adapter.buildWriteChange(
		map[uint32]postgresRelation{7: relation},
		7,
		tuple,
		pglogrepl.LSN(42),
		models.ReplicationOperationInsert,
	)
	if err != nil {
		t.Fatalf("buildWriteChange returned error: %v", err)
	}
	if !ok {
		t.Fatal("buildWriteChange filtered local tuple")
	}
	if change.Operation != models.ReplicationOperationInsert {
		t.Fatalf("unexpected operation: %s", change.Operation)
	}
	if change.Version != 42 {
		t.Fatalf("unexpected version: %d", change.Version)
	}

	fields, err := models.PayloadToFields(change.Payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := string(fields["id"]); got != `"u-1"` {
		t.Fatalf("unexpected id field: %s", got)
	}
	if got := string(fields["region"]); got != `"eu"` {
		t.Fatalf("unexpected region field: %s", got)
	}
	if got := string(fields["name"]); got != `"Alice"` {
		t.Fatalf("unexpected name field: %s", got)
	}
}

func TestBuildWriteChangeRejectsToastPlaceholder(t *testing.T) {
	t.Parallel()

	adapter := &ChangeDataCapture{localRegion: "eu"}
	relation := postgresRelation{
		tableName:  "users",
		primaryKey: []string{"id"},
		primaryKeyTypes: map[string]models.PrimaryKeyType{
			"id": models.PrimaryKeyTypeString,
		},
		columnByName: map[string]int{
			"id":      0,
			"region":  1,
			"profile": 2,
		},
		regionField: "region",
	}
	tuple := &pglogrepl.TupleData{
		ColumnNum: 3,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("u-1")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("eu")},
			{DataType: pglogrepl.TupleDataTypeToast},
		},
	}

	_, _, err := adapter.buildWriteChange(
		map[uint32]postgresRelation{7: relation},
		7,
		tuple,
		pglogrepl.LSN(42),
		models.ReplicationOperationUpdate,
	)
	if err == nil {
		t.Fatal("expected TOAST placeholder error")
	}
	if !strings.Contains(err.Error(), "unchanged TOAST value") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildUpdateDiffChangeUsesWalTuples(t *testing.T) {
	t.Parallel()

	adapter := &ChangeDataCapture{localRegion: "eu", updateDiff: true}
	relation := postgresRelation{
		tableName:  "users",
		primaryKey: []string{"id"},
		primaryKeyTypes: map[string]models.PrimaryKeyType{
			"id": models.PrimaryKeyTypeString,
		},
		columnByName: map[string]int{
			"id":     0,
			"region": 1,
			"name":   2,
		},
		regionField: "region",
	}
	oldTuple := &pglogrepl.TupleData{
		ColumnNum: 3,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("u-1")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("eu")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("Alice")},
		},
	}
	newTuple := &pglogrepl.TupleData{
		ColumnNum: 3,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("u-1")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("eu")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("Bob")},
		},
	}

	change, ok, err := adapter.buildUpdateChange(
		map[uint32]postgresRelation{7: relation},
		7,
		oldTuple,
		newTuple,
		pglogrepl.LSN(42),
	)
	if err != nil {
		t.Fatalf("buildUpdateChange returned error: %v", err)
	}
	if !ok {
		t.Fatal("buildUpdateChange filtered local tuple")
	}

	fields, err := models.PayloadToFields(change.Payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("unexpected changed field count: %d", len(fields))
	}
	if got := string(fields["id"]); got != `"u-1"` {
		t.Fatalf("unexpected id field: %s", got)
	}
	if got := string(fields["name"]); got != `"Bob"` {
		t.Fatalf("unexpected name field: %s", got)
	}
}
