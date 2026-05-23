//go:build soak

// This file contains long-running memory soak scenarios that intentionally sit
// outside the default e2e suite behind the `soak` build tag. These tests focus
// on the memory properties that matter for long-lived production runs:
// senders accumulating a large backlog while the peer is offline, and full
// receiver lifecycles that cover snapshot bootstrap followed by many live apply
// waves for both polling and CDC. The assertions are stricter than a single before/after sample:
// consecutive samples must stay within a bounded growth step, the whole measured
// window must stay within an envelope, goroutine counts must plateau, and the
// system must continue making observable replication progress while the memory
// shape remains stable.
package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

const (
	soakWaveCount = 10

	// Soak tests run longer and therefore tolerate a slightly wider memory
	// envelope than the default slow tests while still rejecting clear upward
	// trends that would become leaks during multi-hour production runs.
	soakHeapEnvelopeLimit = 32 << 20 // 32 MiB
	soakRSSEnvelopeLimit  = 64 << 20 // 64 MiB

	// Goroutine counts should stay essentially flat after warmup. A wider limit
	// would hide leaked reconnect loops, stream handlers, or metrics workers.
	soakGoroutineAdjacentLimit = 8
	soakGoroutineEnvelopeLimit = 16
)

type soakSample struct {
	memorySample
	goroutines uint64
}

func TestSoakPollingSenderMemoryPlateausDuringExtendedOfflineBacklogRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fixture := setupPollingFixture(t)
	usCfg, _ := fixture.pollingConfigs()
	usCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	usNode := startNode(t, usCfg)
	t.Cleanup(func() {
		stopNodes(t, usNode)
	})

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		count, err := leaseCount(t, fixture.usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected polling soak sender producer lease to be active, got %d", count)
		}
		return nil
	})

	samples := make([]soakSample, 0, soakWaveCount-1)
	lastPending := uint64(0)
	for wave := 0; wave < soakWaveCount; wave++ {
		prefix := fmt.Sprintf("polling-soak-backlog-%d", wave)
		insertUsersBatch(t, fixture.usDB, prefix, 0, 350, "us")

		lastPending = awaitPendingIncrease(t, fixture.usDB, "eu", lastPending, wave)
		awaitSourcePrefixCount(t, ctx, fixture.usDB, prefix, 350)

		if wave == 0 {
			continue
		}
		samples = append(samples, sampleSoakMetrics(t, usCfg.Transport.Metrics.ListenAddr))
	}

	assertPlateau(t, "polling soak sender heap", samples, steadyStateHeapGrowthLimit, soakHeapEnvelopeLimit, func(s soakSample) uint64 {
		return s.heapBytes
	})
	assertPlateau(t, "polling soak sender rss", samples, steadyStateRSSGrowthLimit, soakRSSEnvelopeLimit, func(s soakSample) uint64 {
		return s.rssBytes
	})
	assertPlateau(t, "polling soak sender goroutines", samples, soakGoroutineAdjacentLimit, soakGoroutineEnvelopeLimit, func(s soakSample) uint64 {
		return s.goroutines
	})
}

func TestSoakCDCSenderMemoryPlateausDuringExtendedOfflineBacklogRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fixture := setupCDCFixture(t)
	usCfg, _ := fixture.cdcConfigs()
	usCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	usNode := startNode(t, usCfg)
	t.Cleanup(func() {
		stopNodes(t, usNode)
	})

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		count, err := leaseCount(t, fixture.usDB, "producer", "us", "", "")
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected cdc soak sender producer lease to be active, got %d", count)
		}
		return nil
	})
	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		active, err := replicationSlotActive(t, fixture.usDB, usCfg.Replication.Source.CDC.Slot)
		if err != nil {
			return err
		}
		if !active {
			return fmt.Errorf("expected cdc soak sender replication slot %s to be active", usCfg.Replication.Source.CDC.Slot)
		}
		return nil
	})

	samples := make([]soakSample, 0, soakWaveCount-1)
	lastPending := uint64(0)
	for wave := 0; wave < soakWaveCount; wave++ {
		prefix := fmt.Sprintf("cdc-soak-backlog-%d", wave)
		insertUsersBatchStatements(t, fixture.usDB, prefix, 0, 300, "us")

		lastPending = awaitPendingIncrease(t, fixture.usDB, "eu", lastPending, wave)
		awaitSourcePrefixCount(t, ctx, fixture.usDB, prefix, 300)

		if wave == 0 {
			continue
		}
		samples = append(samples, sampleSoakMetrics(t, usCfg.Transport.Metrics.ListenAddr))
	}

	assertPlateau(t, "cdc soak sender heap", samples, steadyStateHeapGrowthLimit, soakHeapEnvelopeLimit, func(s soakSample) uint64 {
		return s.heapBytes
	})
	assertPlateau(t, "cdc soak sender rss", samples, steadyStateRSSGrowthLimit, soakRSSEnvelopeLimit, func(s soakSample) uint64 {
		return s.rssBytes
	})
	assertPlateau(t, "cdc soak sender goroutines", samples, soakGoroutineAdjacentLimit, soakGoroutineEnvelopeLimit, func(s soakSample) uint64 {
		return s.goroutines
	})
}

func TestSoakPollingReceiverMemoryPlateausAcrossSnapshotBootstrapAndExtendedLiveApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fixture := setupPollingFixture(t)
	usCfg, euCfg := fixture.pollingConfigs()
	usCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)
	euCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	insertUsersBatch(t, fixture.usDB, "polling-soak-snapshot", 0, 500, "us")

	usNode := startNode(t, usCfg)
	euNode := startNode(t, euCfg)
	t.Cleanup(func() {
		stopNodes(t, euNode, usNode)
	})

	awaitReplicatedPrefixCount(ctx, t, fixture.euDB, "polling-soak-snapshot", 500, 60*time.Second)
	awaitOutboxDrained(t, fixture.usDB, "eu", 30*time.Second)

	samples := make([]soakSample, 0, soakWaveCount-1)
	for wave := 0; wave < soakWaveCount; wave++ {
		prefix := fmt.Sprintf("polling-soak-live-%d", wave)
		insertUsersBatch(t, fixture.usDB, prefix, 0, 250, "us")
		awaitReplicatedPrefixCount(ctx, t, fixture.euDB, prefix, 250, 60*time.Second)
		awaitOutboxDrained(t, fixture.usDB, "eu", 30*time.Second)

		after := sampleSoakMetrics(t, euCfg.Transport.Metrics.ListenAddr)
		if wave == 0 {
			continue
		}
		samples = append(samples, after)
	}

	assertPlateau(t, "polling soak receiver heap", samples, steadyStateHeapGrowthLimit, soakHeapEnvelopeLimit, func(s soakSample) uint64 {
		return s.heapBytes
	})
	assertPlateau(t, "polling soak receiver rss", samples, steadyStateRSSGrowthLimit, soakRSSEnvelopeLimit, func(s soakSample) uint64 {
		return s.rssBytes
	})
	assertPlateau(t, "polling soak receiver goroutines", samples, soakGoroutineAdjacentLimit, soakGoroutineEnvelopeLimit, func(s soakSample) uint64 {
		return s.goroutines
	})
}

func TestSoakCDCMemoryPlateausAcrossSnapshotBootstrapAndExtendedLiveApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fixture := setupCDCFixture(t)
	usCfg, euCfg := fixture.cdcConfigs()
	usCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)
	euCfg.Transport.Metrics.ListenAddr = reserveListenAddr(t)

	insertUsersBatchStatements(t, fixture.usDB, "cdc-soak-snapshot", 0, 500, "us")

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
			return fmt.Errorf("expected cdc soak replication slot %s to be active", usCfg.Replication.Source.CDC.Slot)
		}
		return nil
	})

	awaitReplicatedPrefixCount(ctx, t, fixture.euDB, "cdc-soak-snapshot", 500, 60*time.Second)
	awaitOutboxDrained(t, fixture.usDB, "eu", 30*time.Second)

	samples := make([]soakSample, 0, soakWaveCount-1)
	for wave := 0; wave < soakWaveCount; wave++ {
		prefix := fmt.Sprintf("cdc-soak-live-%d", wave)
		insertUsersBatchStatements(t, fixture.usDB, prefix, 0, 250, "us")
		awaitReplicatedPrefixCount(ctx, t, fixture.euDB, prefix, 250, 60*time.Second)
		awaitOutboxDrained(t, fixture.usDB, "eu", 30*time.Second)

		after := sampleSoakMetrics(t, euCfg.Transport.Metrics.ListenAddr)

		if wave == 0 {
			continue
		}
		samples = append(samples, after)
	}

	assertPlateau(t, "cdc soak receiver heap", samples, steadyStateHeapGrowthLimit, soakHeapEnvelopeLimit, func(s soakSample) uint64 {
		return s.heapBytes
	})
	assertPlateau(t, "cdc soak receiver rss", samples, steadyStateRSSGrowthLimit, soakRSSEnvelopeLimit, func(s soakSample) uint64 {
		return s.rssBytes
	})
	assertPlateau(t, "cdc soak receiver goroutines", samples, soakGoroutineAdjacentLimit, soakGoroutineEnvelopeLimit, func(s soakSample) uint64 {
		return s.goroutines
	})
}

func sampleSoakMetrics(t *testing.T, metricsAddr string) soakSample {
	t.Helper()

	var sample soakSample
	eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		heapValue, err := scrapePrometheusGauge(metricsAddr, "go_memstats_heap_alloc_bytes")
		if err != nil {
			return err
		}
		rssValue, err := scrapePrometheusGauge(metricsAddr, "process_resident_memory_bytes")
		if err != nil {
			return err
		}
		goroutines, err := scrapePrometheusGauge(metricsAddr, "go_goroutines")
		if err != nil {
			return err
		}
		sample.heapBytes = uint64(heapValue)
		sample.rssBytes = uint64(rssValue)
		sample.goroutines = uint64(goroutines)
		return nil
	})

	return sample
}

func awaitSourcePrefixCount(t *testing.T, ctx context.Context, db *postgresInstance, prefix string, expected int) {
	t.Helper()

	eventually(t, 20*time.Second, 200*time.Millisecond, func() error {
		var count int
		if err := db.Pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM users WHERE id LIKE $1`,
			prefix+"-%",
		).Scan(&count); err != nil {
			return fmt.Errorf("count source rows for prefix %s: %w", prefix, err)
		}
		if count != expected {
			return fmt.Errorf("source rows for prefix %s not complete yet: count=%d expected=%d", prefix, count, expected)
		}
		return nil
	})
}

func awaitPendingIncrease(t *testing.T, db *postgresInstance, peerRegion string, previous uint64, wave int) uint64 {
	t.Helper()

	var current uint64
	eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
		current = uint64(pendingOutboxCount(t, db, peerRegion))
		if current <= previous {
			return fmt.Errorf("expected pending outbox to increase for wave %d: before=%d after=%d", wave, previous, current)
		}
		return nil
	})

	return current
}
