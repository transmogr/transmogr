package changefeed

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/transmogr/transmogr/internal/models"
)

func TestProducerRunUsesLeaseService(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	peers := NewMockPeersService(ctrl)
	adapter := NewMockAdapter(ctrl)
	leases := NewMockLeaseService(ctrl)
	box := NewMockOutbox(ctrl)
	stateStore := NewMockStateStore(ctrl)

	producer, err := NewService(ServiceConfig{
		Source:         "polling",
		LocalRegion:    "local",
		InstanceID:     "pod-1",
		LeaseTTL:       time.Second,
		RenewInterval:  100 * time.Millisecond,
		MaxBatchEvents: 1000,
	}, peers, adapter, leases, box, stateStore, nil)
	if err != nil {
		t.Fatal(err)
	}

	leases.EXPECT().
		RunWithLease(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		})
	adapter.EXPECT().
		CursorTables().
		Return([]string{"test"})
	stateStore.EXPECT().
		GetChangefeedCursor(gomock.Any(), "test").
		Return(models.ChangefeedCursor{Table: "test"}, nil)
	stateStore.EXPECT().
		SaveChangefeedCursor(gomock.Any(), gomock.Any()).
		Return(nil)
	adapter.EXPECT().
		RestoreCursors(gomock.Any(), gomock.Any()).
		Return(nil)
	adapter.EXPECT().
		Run(gomock.Any(), gomock.Any()).
		Return(errors.New("stop"))

	err = producer.Run(context.Background())
	if err == nil || err.Error() != "stop" {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestHandleChangePublishesToAllPeers(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	peers := NewMockPeersService(ctrl)
	adapter := NewMockAdapter(ctrl)
	leases := NewMockLeaseService(ctrl)
	box := NewMockOutbox(ctrl)

	producer, err := NewService(
		ServiceConfig{Source: "polling", LocalRegion: "local", MaxBatchEvents: 1000},
		peers, adapter, leases, box,
		&stubStateStore{cursors: map[string]models.ChangefeedCursor{}},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	change := models.ChangefeedChange{
		Table:      "users",
		PrimaryKey: models.PrimaryKeyString("id", "u-1"),
		Operation:  models.ReplicationOperationUpsert,
		Version:    1234,
		Payload:    []byte(`{"id":"u-1"}`),
	}

	peers.EXPECT().Peers().Return([]models.Peer{
		{Region: "eu"},
		{Region: "us"},
	})
	box.EXPECT().NextSequence(gomock.Any(), "eu", "users").Return(uint64(1), nil)
	box.EXPECT().
		Enqueue(gomock.Any(), batchForPeer("eu")).
		DoAndReturn(assertSingleBatch(
			"eu", "users", 1, models.ReplicationOperationUpsert,
			1234, models.PrimaryKeyString("id", "u-1"),
		))
	box.EXPECT().NextSequence(gomock.Any(), "us", "users").Return(uint64(1), nil)
	box.EXPECT().
		Enqueue(gomock.Any(), batchForPeer("us")).
		DoAndReturn(assertSingleBatch(
			"us", "users", 1, models.ReplicationOperationUpsert,
			1234, models.PrimaryKeyString("id", "u-1"),
		))

	batch := models.ChangefeedBatch{Changes: []models.ChangefeedChange{change}}
	if err := producer.HandleBatch(context.Background(), batch); err != nil {
		t.Fatalf("handle change: %v", err)
	}
}

func TestHandleDeleteChangePublishesDelete(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	peers := NewMockPeersService(ctrl)
	adapter := NewMockAdapter(ctrl)
	leases := NewMockLeaseService(ctrl)
	box := NewMockOutbox(ctrl)

	producer, err := NewService(
		ServiceConfig{Source: "polling", LocalRegion: "local", MaxBatchEvents: 1000},
		peers, adapter, leases, box,
		&stubStateStore{cursors: map[string]models.ChangefeedCursor{}},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	change := models.ChangefeedChange{
		Table:      "users",
		PrimaryKey: models.PrimaryKeyString("id", "u-9"),
		Operation:  models.ReplicationOperationDelete,
		Version:    2048,
	}

	peers.EXPECT().Peers().Return([]models.Peer{{Region: "eu"}})
	box.EXPECT().NextSequence(gomock.Any(), "eu", "users").Return(uint64(1), nil)
	box.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(assertSingleBatch(
			"eu", "users", 1, models.ReplicationOperationDelete,
			2048, models.PrimaryKeyString("id", "u-9"),
		))

	batch := models.ChangefeedBatch{Changes: []models.ChangefeedChange{change}}
	if err := producer.HandleBatch(context.Background(), batch); err != nil {
		t.Fatalf("handle delete change: %v", err)
	}
}

func TestProducerRunRestoresPollingCursorFromState(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	peers := NewMockPeersService(ctrl)
	leases := NewMockLeaseService(ctrl)
	box := NewMockOutbox(ctrl)
	state := &stubStateStore{
		cursors: map[string]models.ChangefeedCursor{
			"users": {
				Table:          "users",
				Source:         "polling",
				LastVersion:    42,
				LastPrimaryKey: models.PrimaryKeyString("id", "u-42"),
			},
		},
	}
	adapter := &stubPollingAdapter{}

	producer, err := NewService(
		ServiceConfig{Source: "polling", LocalRegion: "local", MaxBatchEvents: 1000},
		peers, adapter, leases, box, state, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	leases.EXPECT().
		RunWithLease(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		})

	err = producer.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	expected := models.PrimaryKeyString("id", "u-42")
	if got := adapter.cursor("users"); got.version != 42 ||
		models.ComparePrimaryKeys(got.primaryKey, expected) != 0 {
		t.Fatalf("unexpected restored cursor: %+v", got)
	}
}

func TestHandleBatchSavesCursorToState(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	peers := NewMockPeersService(ctrl)
	adapter := NewMockAdapter(ctrl)
	leases := NewMockLeaseService(ctrl)
	box := NewMockOutbox(ctrl)
	state := &stubStateStore{cursors: map[string]models.ChangefeedCursor{}}

	producer, err := NewService(
		ServiceConfig{Source: "polling", LocalRegion: "local", MaxBatchEvents: 1000},
		peers, adapter, leases, box, state, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	change := models.ChangefeedChange{
		Table:      "users",
		PrimaryKey: models.PrimaryKeyString("id", "u-1"),
		Operation:  models.ReplicationOperationUpsert,
		Version:    1234,
		Payload:    []byte(`{"id":"u-1"}`),
	}

	peers.EXPECT().Peers().Return([]models.Peer{{Region: "eu"}})
	box.EXPECT().NextSequence(gomock.Any(), "eu", "users").Return(uint64(1), nil)
	box.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(assertSingleBatch(
			"eu", "users", 1, models.ReplicationOperationUpsert,
			1234, models.PrimaryKeyString("id", "u-1"),
		))

	batch := models.ChangefeedBatch{Changes: []models.ChangefeedChange{change}}
	if err := producer.HandleBatch(context.Background(), batch); err != nil {
		t.Fatalf("handle batch: %v", err)
	}

	cursor, ok := state.cursors["users"]
	if !ok {
		t.Fatal("expected polling cursor to be saved")
	}
	wantKey := models.PrimaryKeyString("id", "u-1")
	if cursor.LastVersion != 1234 ||
		models.ComparePrimaryKeys(cursor.LastPrimaryKey, wantKey) != 0 {
		t.Fatalf("unexpected saved cursor: %+v", cursor)
	}
	if cursor.Source != "polling" {
		t.Fatalf("unexpected saved source: %q", cursor.Source)
	}
}

func TestHandleBatchSavesCDCCursorToState(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	peers := NewMockPeersService(ctrl)
	adapter := NewMockAdapter(ctrl)
	leases := NewMockLeaseService(ctrl)
	box := NewMockOutbox(ctrl)
	state := &stubStateStore{cursors: map[string]models.ChangefeedCursor{}}

	producer, err := NewService(
		ServiceConfig{Source: "cdc", LocalRegion: "local", MaxBatchEvents: 1000},
		peers, adapter, leases, box, state, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	first := models.ChangefeedChange{
		Table:      "users",
		PrimaryKey: models.PrimaryKeyString("id", "u-1"),
		Operation:  models.ReplicationOperationUpsert,
		Version:    1234,
		Payload:    []byte(`{"id":"u-1"}`),
	}
	second := models.ChangefeedChange{
		Table:      "users",
		PrimaryKey: models.PrimaryKeyString("id", "u-2"),
		Operation:  models.ReplicationOperationUpsert,
		Version:    1235,
		Payload:    []byte(`{"id":"u-2"}`),
	}

	peers.EXPECT().Peers().Return([]models.Peer{{Region: "eu"}})
	box.EXPECT().NextSequence(gomock.Any(), "eu", "").Return(uint64(1), nil)
	box.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil)

	if err := producer.HandleBatch(context.Background(), models.ChangefeedBatch{
		TxID:    "lsn-1",
		Changes: []models.ChangefeedChange{first, second},
	}); err != nil {
		t.Fatalf("handle batch: %v", err)
	}

	cursor, ok := state.cursors["users"]
	if !ok {
		t.Fatal("expected cdc cursor to be saved")
	}
	wantKey2 := models.PrimaryKeyString("id", "u-2")
	if cursor.Source != "cdc" || cursor.LastVersion != 1235 ||
		models.ComparePrimaryKeys(cursor.LastPrimaryKey, wantKey2) != 0 {
		t.Fatalf("unexpected saved cursor: %+v", cursor)
	}
}

func TestProducerRunFailsOnCursorSourceMismatch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	peers := NewMockPeersService(ctrl)
	leases := NewMockLeaseService(ctrl)
	box := NewMockOutbox(ctrl)
	state := &stubStateStore{
		cursors: map[string]models.ChangefeedCursor{
			"users": {
				Table:          "users",
				Source:         "cdc",
				LastVersion:    42,
				LastPrimaryKey: models.PrimaryKeyString("id", "u-42"),
			},
		},
	}
	adapter := &stubPollingAdapter{}

	producer, err := NewService(
		ServiceConfig{Source: "polling", LocalRegion: "local", MaxBatchEvents: 1000},
		peers, adapter, leases, box, state, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	leases.EXPECT().
		RunWithLease(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		})

	err = producer.Run(context.Background())
	if err == nil || err.Error() != "changefeed cursor source mismatch\nfor users: stored=cdc current=polling" {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestProducerRunInitializesMissingCursorWithSource(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	peers := NewMockPeersService(ctrl)
	leases := NewMockLeaseService(ctrl)
	box := NewMockOutbox(ctrl)
	state := &stubStateStore{cursors: map[string]models.ChangefeedCursor{}}
	adapter := &stubPollingAdapter{}

	producer, err := NewService(
		ServiceConfig{Source: "cdc", LocalRegion: "local", MaxBatchEvents: 1000},
		peers, adapter, leases, box, state, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	leases.EXPECT().
		RunWithLease(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		})

	err = producer.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	cursor, ok := state.cursors["users"]
	if !ok {
		t.Fatal("expected cursor to be initialized")
	}
	if cursor.Source != "cdc" || cursor.LastVersion != 0 || len(cursor.LastPrimaryKey) != 0 {
		t.Fatalf("unexpected initialized cursor: %+v", cursor)
	}
}

func TestEventIDFromChangeIsBinarySafeAndCollisionResistant(t *testing.T) {
	t.Parallel()

	first := eventIDFromChange(models.ChangefeedChange{
		Table:      "t",
		PrimaryKey: models.PrimaryKeyString("id", "foo:1"),
		Operation:  models.ReplicationOperationUpsert,
		Version:    42,
	})
	second := eventIDFromChange(models.ChangefeedChange{
		Table:      "t:1",
		PrimaryKey: models.PrimaryKeyString("id", "foo"),
		Operation:  models.ReplicationOperationUpsert,
		Version:    42,
	})
	if first == second {
		t.Fatal("expected distinct event ids for distinct change tuples")
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Fatalf("expected hex event id, got %q: %v", first, err)
	}

	binaryKey := eventIDFromChange(models.ChangefeedChange{
		Table:      "users",
		PrimaryKey: []models.PrimaryKeyPart{{Column: "id", Value: []byte(`"\u0000\ufffd:"`)}},
		Operation:  models.ReplicationOperationDelete,
		Version:    7,
	})
	if _, err := hex.DecodeString(binaryKey); err != nil {
		t.Fatalf("expected binary-safe event id, got %q: %v", binaryKey, err)
	}
}

type batchPeerMatcher struct{ region string }

func batchForPeer(region string) gomock.Matcher { return batchPeerMatcher{region} }
func (m batchPeerMatcher) Matches(x any) bool {
	b, ok := x.(models.OutboxBatch)
	return ok && b.PeerRegion == m.region
}
func (m batchPeerMatcher) String() string { return "batch for peer " + m.region }

func assertSingleBatch(
	wantPeer string,
	wantTable string,
	wantSequence uint64,
	wantOp models.ReplicationOperation,
	wantVersion int64,
	wantPrimaryKey []models.PrimaryKeyPart,
) func(context.Context, models.OutboxBatch) error {
	return func(_ context.Context, batch models.OutboxBatch) error {
		if batch.PeerRegion != wantPeer {
			return errors.New("unexpected peer region")
		}
		if batch.Table != wantTable || batch.Sequence != wantSequence || len(batch.Events) != 1 {
			return errors.New("unexpected event batch")
		}
		event := batch.Events[0]
		if event.Operation != wantOp ||
			event.Version != wantVersion ||
			models.ComparePrimaryKeys(event.PrimaryKey, wantPrimaryKey) != 0 {
			return errors.New("unexpected event")
		}
		if wantOp != models.ReplicationOperationDelete && string(event.Fields["id"]) != `"u-1"` {
			return errors.New("unexpected event fields")
		}
		return nil
	}
}

type stubPollingAdapter struct {
	cursors map[string]stubCursor
}

type stubCursor struct {
	version    int64
	primaryKey []models.PrimaryKeyPart
}

func (a *stubPollingAdapter) Run(context.Context, func(context.Context, models.ChangefeedBatch) error) error {
	return nil
}

func (a *stubPollingAdapter) CursorTables() []string {
	return []string{"users"}
}

func (a *stubPollingAdapter) RestoreCursors(
	_ context.Context,
	load func(table string) (models.ChangefeedCursor, error),
) error {
	for _, table := range a.CursorTables() {
		cursor, err := load(table)
		if err != nil {
			return err
		}
		if a.cursors == nil {
			a.cursors = make(map[string]stubCursor)
		}
		a.cursors[table] = stubCursor{
			version:    cursor.LastVersion,
			primaryKey: models.ClonePrimaryKeyParts(cursor.LastPrimaryKey),
		}
	}
	return nil
}

func (a *stubPollingAdapter) cursor(table string) stubCursor {
	return a.cursors[table]
}

type stubStateStore struct {
	cursors map[string]models.ChangefeedCursor
}

func (s *stubStateStore) GetChangefeedCursor(_ context.Context, table string) (models.ChangefeedCursor, error) {
	if cursor, ok := s.cursors[table]; ok {
		return cursor, nil
	}
	return models.ChangefeedCursor{Table: table}, nil
}

func (s *stubStateStore) SaveChangefeedCursor(_ context.Context, cursor models.ChangefeedCursor) error {
	if s.cursors == nil {
		s.cursors = make(map[string]models.ChangefeedCursor)
	}
	s.cursors[cursor.Table] = cursor
	return nil
}
