// This file verifies memory-stability properties for the main replication paths:
// repeated send-side enqueue into the durable outbox must not cause unbounded
// heap growth, repeated receive/apply cycles on the destination must stay
// within a bounded steady-state envelope after warmup, and longer multi-wave
// soak scenarios must plateau rather than trend upward linearly over time.
package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// steadyStateHeapGrowthLimit bounds retained Go heap growth between adjacent
	// post-warmup load waves. The limit is intentionally above small runtime and
	// allocator noise, but low enough to catch obvious buffering leaks.
	steadyStateHeapGrowthLimit = 12 << 20 // 12 MiB

	// steadyStateRSSGrowthLimit bounds resident-memory growth between adjacent
	// post-warmup load waves. RSS is noisier than heap_alloc because it includes
	// allocator arenas and process-level effects, so the limit is looser.
	steadyStateRSSGrowthLimit = 24 << 20 // 24 MiB

	// longRunHeapEnvelopeLimit bounds the spread between the minimum and maximum
	// heap samples during the measured part of a long soak. This allows normal
	// allocator jitter while still rejecting tests that trend upward over many
	// waves instead of flattening after warmup.
	longRunHeapEnvelopeLimit = 24 << 20 // 24 MiB

	// longRunRSSEnvelopeLimit bounds the RSS spread during a long soak. RSS is
	// noisier because spans and arenas are returned to the OS less predictably,
	// so the acceptable envelope is wider than the heap envelope.
	longRunRSSEnvelopeLimit = 48 << 20 // 48 MiB

	longRunWaveCount = 6
)

type memorySample struct {
	heapBytes uint64
	rssBytes  uint64
}

func TestSlowSenderHeapStaysBoundedDuringRepeatedOutboxEnqueue(t *testing.T) {
	fixture := setupPollingFixture(t)
	usCfg, _ := fixture.pollingConfigs()
	usCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	usNode := startNode(t, usCfg)
	t.Cleanup(func() {
		stopNodes(t, usNode)
	})

	eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		count, err := leaseCount(t, fixture.usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected sender producer lease to be active, got %d", count)
		}
		return nil
	})

	insertUsersBatch(t, fixture.usDB, "sender-warm", 0, 400, "us")
	eventually(t, 15*time.Second, 200*time.Millisecond, func() error {
		if pendingOutboxCount(t, fixture.usDB, "eu") == 0 {
			return fmt.Errorf("expected sender warmup to enqueue pending outbox data")
		}
		return nil
	})

	beforeSecondWave := pendingOutboxCount(t, fixture.usDB, "eu")
	insertUsersBatch(t, fixture.usDB, "sender-measure-a", 0, 400, "us")
	eventually(t, 15*time.Second, 200*time.Millisecond, func() error {
		pending := pendingOutboxCount(t, fixture.usDB, "eu")
		if pending <= beforeSecondWave {
			return fmt.Errorf(
				"expected second sender wave to increase pending outbox backlog: before=%d after=%d",
				beforeSecondWave,
				pending,
			)
		}
		return nil
	})
	memAfterSecondWave := sampleMemoryMetrics(t, usCfg.Transport.Metrics.ListenAddr)

	beforeThirdWave := pendingOutboxCount(t, fixture.usDB, "eu")
	insertUsersBatch(t, fixture.usDB, "sender-measure-b", 0, 400, "us")
	eventually(t, 15*time.Second, 200*time.Millisecond, func() error {
		pending := pendingOutboxCount(t, fixture.usDB, "eu")
		if pending <= beforeThirdWave {
			return fmt.Errorf(
				"expected third sender wave to increase pending outbox backlog: before=%d after=%d",
				beforeThirdWave,
				pending,
			)
		}
		return nil
	})
	memAfterThirdWave := sampleMemoryMetrics(t, usCfg.Transport.Metrics.ListenAddr)

	assertMetricGrowthWithin(
		t,
		"sender enqueue steady-state heap growth",
		memAfterSecondWave.heapBytes,
		memAfterThirdWave.heapBytes,
		steadyStateHeapGrowthLimit,
	)
	assertMetricGrowthWithin(
		t,
		"sender enqueue steady-state rss growth",
		memAfterSecondWave.rssBytes,
		memAfterThirdWave.rssBytes,
		steadyStateRSSGrowthLimit,
	)
}

func TestSlowReceiverHeapStaysBoundedDuringRepeatedApplyCycles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fixture := setupPollingFixture(t)
	usCfg, euCfg := fixture.pollingConfigs()
	euCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})

	insertUsersBatch(t, fixture.usDB, "recv-warm", 0, 400, "us")
	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := fixture.euDB.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
			return fmt.Errorf("count receiver warmup rows: %w", err)
		}
		if count != 400 {
			return fmt.Errorf("receiver warmup not fully applied yet: count=%d", count)
		}
		return nil
	})

	insertUsersBatch(t, fixture.usDB, "recv-measure-a", 0, 400, "us")
	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := fixture.euDB.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
			return fmt.Errorf("count receiver rows after second wave: %w", err)
		}
		if count != 800 {
			return fmt.Errorf("receiver second wave not fully applied yet: count=%d", count)
		}
		if pendingOutboxCount(t, fixture.usDB, "eu") != 0 {
			return fmt.Errorf("expected sender outbox to be fully acked before heap sample")
		}
		return nil
	})
	memAfterSecondWave := sampleMemoryMetrics(t, euCfg.Transport.Metrics.ListenAddr)

	insertUsersBatch(t, fixture.usDB, "recv-measure-b", 0, 400, "us")
	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := fixture.euDB.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
			return fmt.Errorf("count receiver rows after third wave: %w", err)
		}
		if count != 1200 {
			return fmt.Errorf("receiver third wave not fully applied yet: count=%d", count)
		}
		if pendingOutboxCount(t, fixture.usDB, "eu") != 0 {
			return fmt.Errorf("expected sender outbox to be fully acked before final heap sample")
		}
		return nil
	})
	memAfterThirdWave := sampleMemoryMetrics(t, euCfg.Transport.Metrics.ListenAddr)

	assertMetricGrowthWithin(
		t,
		"receiver apply steady-state heap growth",
		memAfterSecondWave.heapBytes,
		memAfterThirdWave.heapBytes,
		steadyStateHeapGrowthLimit,
	)
	assertMetricGrowthWithin(
		t,
		"receiver apply steady-state rss growth",
		memAfterSecondWave.rssBytes,
		memAfterThirdWave.rssBytes,
		steadyStateRSSGrowthLimit,
	)
}

func TestSlowCDCSenderHeapStaysBoundedDuringRepeatedOutboxEnqueue(t *testing.T) {
	fixture := setupCDCFixture(t)
	usCfg, _ := fixture.cdcConfigs()
	usCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	usNode := startNode(t, usCfg)
	t.Cleanup(func() {
		stopNodes(t, usNode)
	})

	eventually(t, 15*time.Second, 200*time.Millisecond, func() error {
		count, err := leaseCount(t, fixture.usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected cdc sender producer lease to be active, got %d", count)
		}
		return nil
	})
	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		active, err := replicationSlotActive(t, fixture.usDB, usCfg.Replication.Source.CDC.Slot)
		if err != nil {
			return err
		}
		if !active {
			return fmt.Errorf("expected cdc sender replication slot %s to be active", usCfg.Replication.Source.CDC.Slot)
		}
		return nil
	})

	insertUsersBatchStatements(t, fixture.usDB, "cdc-sender-warm", 0, 300, "us")
	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		if pendingOutboxCount(t, fixture.usDB, "eu") == 0 {
			return fmt.Errorf("expected cdc sender warmup to enqueue pending outbox data")
		}
		return nil
	})

	beforeSecondWave := pendingOutboxCount(t, fixture.usDB, "eu")
	insertUsersBatchStatements(t, fixture.usDB, "cdc-sender-measure-a", 0, 300, "us")
	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		pending := pendingOutboxCount(t, fixture.usDB, "eu")
		if pending <= beforeSecondWave {
			return fmt.Errorf(
				"expected cdc second sender wave to increase pending outbox backlog: before=%d after=%d",
				beforeSecondWave,
				pending,
			)
		}
		return nil
	})
	memAfterSecondWave := sampleMemoryMetrics(t, usCfg.Transport.Metrics.ListenAddr)

	beforeThirdWave := pendingOutboxCount(t, fixture.usDB, "eu")
	insertUsersBatchStatements(t, fixture.usDB, "cdc-sender-measure-b", 0, 300, "us")
	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		pending := pendingOutboxCount(t, fixture.usDB, "eu")
		if pending <= beforeThirdWave {
			return fmt.Errorf(
				"expected cdc third sender wave to increase pending outbox backlog: before=%d after=%d",
				beforeThirdWave,
				pending,
			)
		}
		return nil
	})
	memAfterThirdWave := sampleMemoryMetrics(t, usCfg.Transport.Metrics.ListenAddr)

	assertMetricGrowthWithin(
		t,
		"cdc sender enqueue steady-state heap growth",
		memAfterSecondWave.heapBytes,
		memAfterThirdWave.heapBytes,
		steadyStateHeapGrowthLimit,
	)
	assertMetricGrowthWithin(
		t,
		"cdc sender enqueue steady-state rss growth",
		memAfterSecondWave.rssBytes,
		memAfterThirdWave.rssBytes,
		steadyStateRSSGrowthLimit,
	)
}

func TestSlowCDCReceiverHeapStaysBoundedDuringRepeatedApplyCycles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fixture := setupCDCFixture(t)
	usCfg, euCfg := fixture.cdcConfigs()
	euCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})
	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		active, err := replicationSlotActive(t, fixture.usDB, usCfg.Replication.Source.CDC.Slot)
		if err != nil {
			return err
		}
		if !active {
			return fmt.Errorf("expected cdc source replication slot %s to be active", usCfg.Replication.Source.CDC.Slot)
		}
		return nil
	})

	insertUsersBatchStatements(t, fixture.usDB, "cdc-recv-ready", 0, 1, "us")
	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id LIKE 'cdc-recv-ready-%'`,
		).Scan(&count); err != nil {
			return fmt.Errorf("count cdc receiver readiness rows: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("cdc receiver readiness row not applied yet: count=%d", count)
		}
		if pendingOutboxCount(t, fixture.usDB, "eu") != 0 {
			return fmt.Errorf("expected cdc readiness outbox to be fully acked before warmup")
		}
		return nil
	})

	insertUsersBatchStatements(t, fixture.usDB, "cdc-recv-warm", 0, 300, "us")
	eventually(t, 40*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id LIKE 'cdc-recv-warm-%'`,
		).Scan(&count); err != nil {
			return fmt.Errorf("count cdc receiver warmup rows: %w", err)
		}
		if count != 300 {
			return fmt.Errorf("cdc receiver warmup not fully applied yet: count=%d", count)
		}
		return nil
	})

	insertUsersBatchStatements(t, fixture.usDB, "cdc-recv-measure-a", 0, 300, "us")
	eventually(t, 40*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id LIKE 'cdc-recv-measure-a-%'`,
		).Scan(&count); err != nil {
			return fmt.Errorf("count cdc receiver rows after second wave: %w", err)
		}
		if count != 300 {
			return fmt.Errorf("cdc receiver second wave not fully applied yet: count=%d", count)
		}
		if pendingOutboxCount(t, fixture.usDB, "eu") != 0 {
			return fmt.Errorf("expected cdc sender outbox to be fully acked before heap sample")
		}
		return nil
	})
	memAfterSecondWave := sampleMemoryMetrics(t, euCfg.Transport.Metrics.ListenAddr)

	insertUsersBatchStatements(t, fixture.usDB, "cdc-recv-measure-b", 0, 300, "us")
	eventually(t, 40*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id LIKE 'cdc-recv-measure-b-%'`,
		).Scan(&count); err != nil {
			return fmt.Errorf("count cdc receiver rows after third wave: %w", err)
		}
		if count != 300 {
			return fmt.Errorf("cdc receiver third wave not fully applied yet: count=%d", count)
		}
		if pendingOutboxCount(t, fixture.usDB, "eu") != 0 {
			return fmt.Errorf("expected cdc sender outbox to be fully acked before final heap sample")
		}
		return nil
	})
	memAfterThirdWave := sampleMemoryMetrics(t, euCfg.Transport.Metrics.ListenAddr)

	assertMetricGrowthWithin(
		t,
		"cdc receiver apply steady-state heap growth",
		memAfterSecondWave.heapBytes,
		memAfterThirdWave.heapBytes,
		steadyStateHeapGrowthLimit,
	)
	assertMetricGrowthWithin(
		t,
		"cdc receiver apply steady-state rss growth",
		memAfterSecondWave.rssBytes,
		memAfterThirdWave.rssBytes,
		steadyStateRSSGrowthLimit,
	)
}

func TestSlowPollingSenderMemoryPlateausAcrossLongOfflineBacklogRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	fixture := setupPollingFixture(t)
	usCfg, _ := fixture.pollingConfigs()
	usCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	usNode := startNode(t, usCfg)
	t.Cleanup(func() {
		stopNodes(t, usNode)
	})

	eventually(t, 15*time.Second, 200*time.Millisecond, func() error {
		count, err := leaseCount(t, fixture.usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected polling soak producer lease to be active, got %d", count)
		}
		return nil
	})

	samples := make([]memorySample, 0, longRunWaveCount-1)
	lastPending := 0
	for wave := 0; wave < longRunWaveCount; wave++ {
		prefix := fmt.Sprintf("polling-soak-sender-%d", wave)
		insertUsersBatch(t, fixture.usDB, prefix, 0, 350, "us")

		eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
			pending := pendingOutboxCount(t, fixture.usDB, "eu")
			if pending <= lastPending {
				return fmt.Errorf(
					"expected sender soak wave %d to increase pending backlog: before=%d after=%d",
					wave,
					lastPending,
					pending,
				)
			}
			return nil
		})
		lastPending = pendingOutboxCount(t, fixture.usDB, "eu")

		var inserted int
		if err := fixture.usDB.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id LIKE $1`,
			prefix+"-%",
		).Scan(&inserted); err != nil {
			t.Fatalf("count sender soak source rows for wave %d: %v", wave, err)
		}
		if inserted != 350 {
			t.Fatalf("expected 350 sender soak source rows for wave %d, got %d", wave, inserted)
		}

		if wave == 0 {
			continue
		}
		samples = append(samples, sampleMemoryMetrics(t, usCfg.Transport.Metrics.ListenAddr))
	}

	assertPlateau(
		t,
		"polling sender soak heap",
		samples,
		steadyStateHeapGrowthLimit,
		longRunHeapEnvelopeLimit,
		func(s memorySample) uint64 { return s.heapBytes },
	)
	assertPlateau(
		t,
		"polling sender soak rss",
		samples,
		steadyStateRSSGrowthLimit,
		longRunRSSEnvelopeLimit,
		func(s memorySample) uint64 { return s.rssBytes },
	)
}

func TestSlowCDCReceiverMemoryPlateausAcrossLongApplyRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	fixture := setupCDCFixture(t)
	usCfg, euCfg := fixture.cdcConfigs()
	euCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		active, err := replicationSlotActive(t, fixture.usDB, usCfg.Replication.Source.CDC.Slot)
		if err != nil {
			return err
		}
		if !active {
			return fmt.Errorf("expected cdc soak source replication slot %s to be active", usCfg.Replication.Source.CDC.Slot)
		}
		return nil
	})

	insertUsersBatchStatements(t, fixture.usDB, "cdc-soak-ready", 0, 1, "us")
	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := fixture.euDB.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id LIKE 'cdc-soak-ready-%'`,
		).Scan(&count); err != nil {
			return fmt.Errorf("count cdc soak readiness rows: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("cdc soak readiness row not applied yet: count=%d", count)
		}
		if pendingOutboxCount(t, fixture.usDB, "eu") != 0 {
			return fmt.Errorf("expected cdc soak readiness outbox to be fully acked before measurements")
		}
		return nil
	})

	samples := make([]memorySample, 0, longRunWaveCount-1)
	for wave := 0; wave < longRunWaveCount; wave++ {
		prefix := fmt.Sprintf("cdc-soak-recv-%d", wave)
		insertUsersBatchStatements(t, fixture.usDB, prefix, 0, 250, "us")

		eventually(t, 40*time.Second, 200*time.Millisecond, func() error {
			var count int
			if err := fixture.euDB.Pool.QueryRow(
				ctx,
				`SELECT COUNT(*) FROM users WHERE id LIKE $1`,
				prefix+"-%",
			).Scan(&count); err != nil {
				return fmt.Errorf("count cdc soak receiver rows for wave %d: %w", wave, err)
			}
			if count != 250 {
				return fmt.Errorf("cdc soak wave %d not fully applied yet: count=%d", wave, count)
			}
			if pendingOutboxCount(t, fixture.usDB, "eu") != 0 {
				return fmt.Errorf("expected cdc soak sender outbox to be fully acked before sampling")
			}
			return nil
		})

		if wave == 0 {
			continue
		}
		samples = append(samples, sampleMemoryMetrics(t, euCfg.Transport.Metrics.ListenAddr))
	}

	assertPlateau(
		t,
		"cdc receiver soak heap",
		samples,
		steadyStateHeapGrowthLimit,
		longRunHeapEnvelopeLimit,
		func(s memorySample) uint64 { return s.heapBytes },
	)
	assertPlateau(
		t,
		"cdc receiver soak rss",
		samples,
		steadyStateRSSGrowthLimit,
		longRunRSSEnvelopeLimit,
		func(s memorySample) uint64 { return s.rssBytes },
	)
}

func insertUsersBatch(t *testing.T, db *postgresInstance, prefix string, start, count int, owner string) {
	t.Helper()

	rows := make([][]any, 0, count)
	for i := 0; i < count; i++ {
		rows = append(rows, []any{
			fmt.Sprintf("%s-%04d", prefix, start+i),
			strings.Repeat("m", 4096),
			time.Now().UTC().Add(time.Duration(i) * time.Microsecond),
			owner,
		})
	}
	seedUsersCopy(t, db, rows)
}

// insertUsersBatchStatements writes one INSERT per row through a single pgx batch.
// CDC tests use this path so logical decoding sees the same row-level change shape
// as ordinary OLTP writes instead of COPY-specific WAL behavior.
func insertUsersBatchStatements(t *testing.T, db *postgresInstance, prefix string, start, count int, owner string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch := &pgx.Batch{}
	baseTime := time.Now().UTC()
	for i := 0; i < count; i++ {
		batch.Queue(
			`INSERT INTO users (id, name, updated_at, _owner_region) VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("%s-%04d", prefix, start+i),
			strings.Repeat("m", 4096),
			baseTime.Add(time.Duration(i)*time.Microsecond),
			owner,
		)
	}

	results := db.Pool.SendBatch(ctx, batch)
	for i := 0; i < count; i++ {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			t.Fatalf("insert batched users row %d: %v", i, err)
		}
	}
	if err := results.Close(); err != nil {
		t.Fatalf("close users insert batch: %v", err)
	}
}

func sampleMemoryMetrics(t *testing.T, metricsAddr string) memorySample {
	t.Helper()

	// These samples intentionally observe exported process/runtime metrics without
	// forcing a GC cycle from the application. The limits above are therefore set
	// with normal GC scheduling noise in mind rather than assuming a perfectly
	// quiesced heap between waves.
	var sample memorySample
	eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		heapValue, err := scrapePrometheusGauge(metricsAddr, "go_memstats_heap_alloc_bytes")
		if err != nil {
			return err
		}
		rssValue, err := scrapePrometheusGauge(metricsAddr, "process_resident_memory_bytes")
		if err != nil {
			return err
		}
		sample.heapBytes = uint64(heapValue)
		sample.rssBytes = uint64(rssValue)
		return nil
	})

	return sample
}

func scrapePrometheusGauge(metricsAddr, metricName string) (float64, error) {
	resp, err := http.Get("http://" + metricsAddr + "/metrics") // #nosec G107 -- test-only loopback scrape.
	if err != nil {
		return 0, fmt.Errorf("scrape metrics endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("scrape metrics endpoint: unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read metrics response: %w", err)
	}

	prefix := metricName + " "
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}

		var value float64
		if _, err := fmt.Sscanf(strings.TrimPrefix(line, prefix), "%f", &value); err != nil {
			return 0, fmt.Errorf("parse metric %s: %w", metricName, err)
		}
		return value, nil
	}

	return 0, fmt.Errorf("metric %s not found", metricName)
}

func assertMetricGrowthWithin(t *testing.T, label string, before, after uint64, limit uint64) {
	t.Helper()

	if after <= before {
		return
	}

	if growth := after - before; growth > limit {
		t.Fatalf("%s exceeded limit: before=%d after=%d growth=%d limit=%d", label, before, after, growth, limit)
	}
}

func assertPlateau[T any](
	t *testing.T,
	label string,
	samples []T,
	adjacentLimit uint64,
	envelopeLimit uint64,
	value func(T) uint64,
) {
	t.Helper()

	if len(samples) < 2 {
		t.Fatalf("%s requires at least two samples, got %d", label, len(samples))
	}

	minValue := value(samples[0])
	maxValue := minValue
	for i := 1; i < len(samples); i++ {
		before := value(samples[i-1])
		after := value(samples[i])
		assertMetricGrowthWithin(
			t,
			fmt.Sprintf("%s adjacent growth sample %d->%d", label, i-1, i),
			before,
			after,
			adjacentLimit,
		)

		if after < minValue {
			minValue = after
		}
		if after > maxValue {
			maxValue = after
		}
	}

	if envelope := maxValue - minValue; envelope > envelopeLimit {
		t.Fatalf(
			"%s exceeded envelope: min=%d max=%d envelope=%d limit=%d",
			label,
			minValue,
			maxValue,
			envelope,
			envelopeLimit,
		)
	}
}
