package ingot

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"git.dvdt.dev/david/ingot/labels"
)

// TestSoak simulates 48h of ingestion at 15s intervals across 10k series,
// validating that compaction, retention, and concurrent queries all work
// correctly under sustained load.
//
// Skipped with -short. Expected runtime: 1-5 minutes.
func TestSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in -short mode")
	}

	const (
		numSeries      = 10_000
		canaryCount    = 10 // series tracked in the oracle for query validation
		scrapeInterval = 15_000 // 15s in ms
		simDuration    = 48 * 3600 * 1000 // 48h in ms
		blockDuration  = 2 * 3600 * 1000 // 2h in ms
		retentionMs    = 24 * 3600 * 1000 // 24h in ms
		flushInterval  = 5 * 60 * 1000 // flush every 5 simulated minutes
		compactInterval = 30 * 60 * 1000 // compact every 30 simulated minutes
		queryInterval  = 10 * 60 * 1000 // validate queries every 10 simulated minutes
	)

	var now atomic.Int64
	startTime := int64(1_000_000_000) // arbitrary start: ~11.5 days in ms
	now.Store(startTime)
	clock := func() int64 { return now.Load() }

	dir := t.TempDir()
	db, err := Open(dir, Options{
		Retention:     time.Duration(retentionMs) * time.Millisecond,
		BlockDuration: time.Duration(blockDuration) * time.Millisecond,
		Clock:         clock,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer db.Close()

	// Generate series labels.
	seriesLabels := make([][]labels.Label, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesLabels[i] = labels.FromStrings(
			"__name__", fmt.Sprintf("metric_%d", i/100),
			"instance", fmt.Sprintf("inst_%d", i%100),
		)
	}

	// Oracle tracks canary series only.
	oracle := newOracle()
	refs := make([]uint64, numSeries)

	// Track stats.
	var totalSamples int64
	var queryErrors int64
	var maxHeapMB uint64

	endTime := startTime + simDuration
	for ts := startTime; ts < endTime; ts += scrapeInterval {
		now.Store(ts)

		// Append samples for all series.
		app := db.Appender()
		for i := 0; i < numSeries; i++ {
			var ls []labels.Label
			if refs[i] == 0 {
				ls = seriesLabels[i]
			}
			r, err := app.Append(refs[i], ls, ts, float64(ts+int64(i)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			refs[i] = r
			if i < canaryCount {
				if ls != nil {
					oracle.addSeries(r, seriesLabels[i])
				}
				oracle.addSample(r, ts, float64(ts+int64(i)))
			}
		}
		if err := app.Commit(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		totalSamples += int64(numSeries)

		// Periodic flush.
		elapsed := ts - startTime
		if elapsed > 0 && elapsed%flushInterval == 0 {
			_, err := db.FlushOlderThan(ts - int64(blockDuration))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// Periodic compaction + retention.
		if elapsed > 0 && elapsed%compactInterval == 0 {
			err := db.RunCompaction()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			db.ApplyRetention()
		}

		// Periodic query validation.
		if elapsed > 0 && elapsed%queryInterval == 0 {
			if !validateCanaries(t, db, oracle, ts, retentionMs, clock) {
				atomic.AddInt64(&queryErrors, 1)
			}

			// Check heap allocation.
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			heapMB := ms.HeapAlloc / (1024 * 1024)
			if heapMB > maxHeapMB {
				maxHeapMB = heapMB
			}
		}
	}

	// Final validation.
	t.Logf("Total samples written: %d", totalSamples)
	t.Logf("Peak heap: %d MiB", maxHeapMB)

	// Assert zero query errors.
	if got := atomic.LoadInt64(&queryErrors); got != int64(0) {
		t.Errorf("query errors during soak: got %v, want %v", got, int64(0))
	}

	// Assert bounded memory (should stay well under 1 GiB with 10k series).
	if !(maxHeapMB < uint64(1024)) {
		t.Errorf("heap should stay under 1 GiB: got %v, want < %v", maxHeapMB, uint64(1024))
	}

	// Assert bounded disk: count block directories.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	blockCount := 0
	for _, e := range entries {
		if e.IsDir() && e.Name() != "wal" {
			if _, err := os.Stat(filepath.Join(dir, e.Name(), "meta.json")); err == nil {
				blockCount++
			}
		}
	}
	t.Logf("Block count at end: %d", blockCount)
	// With 24h retention and 2h blocks, expect roughly 12 raw + some compacted.
	// Should be well under 50.
	if !(blockCount < 50) {
		t.Errorf("block count should be bounded by retention: got %v, want < %v", blockCount, 50)
	}

	// Final canary validation.
	finalNow := now.Load()
	validateCanaries(t, db, oracle, finalNow, retentionMs, clock)

	// Live query during compaction: start query, compact, finish query.
	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ss := q.Select(labels.MustNewMatcher(labels.MatchRegexp, "__name__", "metric_0.*"))
	// Trigger compaction while query is open.
	db.RunCompaction()
	// Drain the query — should not error.
	for ss.Next() {
		it := ss.At().Iterator()
		for it.Next() {
		}
		if err := it.Err(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if err := ss.Err(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// validateCanaries queries each canary series and compares with the oracle.
// Returns true if all validations pass. Every assertion runs for every canary.
func validateCanaries(t *testing.T, db *DB, o *oracle, now int64, retentionMs int64, clock func() int64) bool {
	t.Helper()
	pass := true

	// Query window: last 1h or from retention cutoff, whichever is later.
	maxt := now
	mint := now - 3600*1000
	retentionCutoff := clock() - retentionMs
	if mint < retentionCutoff {
		mint = retentionCutoff
	}

	for ref, ls := range o.labels {
		// Build matchers from labels.
		var matchers []*labels.Matcher
		for _, l := range ls {
			matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, l.Name, l.Value))
		}

		// Query.
		q, err := db.Querier(mint, maxt)
		if err != nil {
			t.Fatalf("querier for canary ref %d: unexpected error: %v", ref, err)
		}

		ss := q.Select(matchers...)
		var gotSamples []sample
		var iterErr error
		var ssErr error
		for ss.Next() {
			it := ss.At().Iterator()
			for it.Next() {
				st, sv := it.At()
				gotSamples = append(gotSamples, sample{st, sv})
			}
			iterErr = it.Err()
		}
		ssErr = ss.Err()
		q.Close()

		// All assertions run unconditionally for every canary.
		if iterErr != nil {
			t.Errorf("canary ref %d: iterator error: %v", ref, iterErr)
			pass = false
		}
		if ssErr != nil {
			t.Errorf("canary ref %d: series set error: %v", ref, ssErr)
			pass = false
		}

		// Compare with oracle.
		wantByRef := o.query(mint, maxt, matchers...)
		var wantSamples []sample
		if s, ok := wantByRef[ref]; ok {
			wantSamples = s
		}

		if len(wantSamples) != len(gotSamples) {
			t.Errorf("canary ref %d sample count (mint=%d maxt=%d): got %v, want %v", ref, mint, maxt, len(gotSamples), len(wantSamples))
			pass = false
		}
	}
	return pass
}
