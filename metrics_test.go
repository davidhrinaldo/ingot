package ingot

import (
	"math"
	"testing"

	"git.dvdt.dev/david/ingot/labels"
)

func TestCollectMetrics(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(t *testing.T, db *DB)
		collectCalls    int
		wantHeadSeries  float64 // expected value after last collectMetrics call
		wantBlocksTotal float64
		wantCompactions float64
		wantMetricCount int // total ingot_* series
	}{
		{
			name:  "empty_db",
			setup: func(t *testing.T, db *DB) {},
			// First call snapshots 0 series, writes 5 metric series.
			// Second call snapshots 5 series (the metrics themselves).
			collectCalls:    2,
			wantHeadSeries:  5,
			wantBlocksTotal: 0,
			wantCompactions: 0,
			wantMetricCount: 5,
		},
		{
			name: "with_user_series",
			setup: func(t *testing.T, db *DB) {
				app := db.Appender()
				_, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "a"), 1000, 1.0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				_, err = app.Append(0, labels.FromStrings("__name__", "temp", "room", "b"), 1000, 2.0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			// 2 user series + 5 metric series = 7
			collectCalls:    2,
			wantHeadSeries:  7,
			wantBlocksTotal: 0,
			wantCompactions: 0,
			wantMetricCount: 5,
		},
		{
			name: "with_block",
			setup: func(t *testing.T, db *DB) {
				app := db.Appender()
				ref, err := app.Append(0, labels.FromStrings("__name__", "temp"), 0, 0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				_, err = db.FlushOlderThan(math.MaxInt64)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			collectCalls:    2,
			wantHeadSeries:  6, // 1 user + 5 metric series
			wantBlocksTotal: 1,
			wantCompactions: 0,
			wantMetricCount: 5,
		},
		{
			name:            "idempotent_five_calls",
			setup:           func(t *testing.T, db *DB) {},
			collectCalls:    5,
			wantHeadSeries:  5,
			wantBlocksTotal: 0,
			wantCompactions: 0,
			wantMetricCount: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := &testClock{now: 100_000}
			db, err := Open(t.TempDir(), Options{Clock: clock.fn()})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Cleanup(func() { db.Close() })

			tc.setup(t, db)

			for i := 0; i < tc.collectCalls; i++ {
				clock.now = int64(200_000 + i*1000)
				db.collectMetrics()
			}

			// Query each expected metric.
			lastValue := func(name string) (float64, bool) {
				q, err := db.Querier(math.MinInt64, math.MaxInt64)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				defer q.Close()
				ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", name))
				found := false
				var last float64
				for ss.Next() {
					it := ss.At().Iterator()
					for it.Next() {
						_, last = it.At()
					}
					if err := it.Err(); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					found = true
				}
				if err := ss.Err(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return last, found
			}

			v, found := lastValue(MetricHeadSeries)
			if !found {
				t.Errorf("ingot_head_series not found")
			}
			if v != tc.wantHeadSeries {
				t.Errorf("ingot_head_series: got %v, want %v", v, tc.wantHeadSeries)
			}

			v, found = lastValue(MetricBlocksTotal)
			if !found {
				t.Errorf("ingot_blocks_total not found")
			}
			if v != tc.wantBlocksTotal {
				t.Errorf("ingot_blocks_total: got %v, want %v", v, tc.wantBlocksTotal)
			}

			v, found = lastValue(MetricCompactionsTotal)
			if !found {
				t.Errorf("ingot_compactions_total not found")
			}
			if v != tc.wantCompactions {
				t.Errorf("ingot_compactions_total: got %v, want %v", v, tc.wantCompactions)
			}

			// Count total ingot_* series.
			q, err := db.Querier(math.MinInt64, math.MaxInt64)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer q.Close()
			ss := q.Select(labels.MustNewMatcher(labels.MatchRegexp, "__name__", "ingot_.*"))
			count := 0
			for ss.Next() {
				count++
			}
			if err := ss.Err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != tc.wantMetricCount {
				t.Errorf("metric series count: got %v, want %v", count, tc.wantMetricCount)
			}
		})
	}
}

type testClock struct {
	now int64
}

func (c *testClock) fn() func() int64 {
	return func() int64 { return c.now }
}
