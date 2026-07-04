package ingot

import (
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"git.dvdt.dev/david/ingot/labels"
)

// sample is a test convenience type.
type sample struct {
	t int64
	v float64
}

// oracle is a naive reference implementation for query comparison.
type oracle struct {
	series map[uint64][]sample      // ref -> samples in order
	labels map[uint64][]labels.Label // ref -> labels
}

func newOracle() *oracle {
	return &oracle{
		series: make(map[uint64][]sample),
		labels: make(map[uint64][]labels.Label),
	}
}

func (o *oracle) addSeries(ref uint64, ls []labels.Label) {
	if _, ok := o.labels[ref]; !ok {
		o.labels[ref] = ls
	}
}

func (o *oracle) addSample(ref uint64, t int64, v float64) {
	o.series[ref] = append(o.series[ref], sample{t, v})
}

func (o *oracle) query(mint, maxt int64, matchers ...*labels.Matcher) map[uint64][]sample {
	result := make(map[uint64][]sample)
	for ref, ls := range o.labels {
		if !matchesAll(ls, matchers) {
			continue
		}
		var filtered []sample
		for _, s := range o.series[ref] {
			if s.t >= mint && s.t <= maxt {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) > 0 {
			result[ref] = filtered
		}
	}
	return result
}

func matchesAll(ls []labels.Label, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		val := ""
		for _, l := range ls {
			if l.Name == m.Name {
				val = l.Value
				break
			}
		}
		if !m.Matches(val) {
			return false
		}
	}
	return true
}

// collectSeriesSet drains a SeriesSet into a map of ref -> samples.
func collectSeriesSet(t *testing.T, ss SeriesSet) map[uint64][]sample {
	t.Helper()
	result := make(map[uint64][]sample)
	for ss.Next() {
		s := ss.At()
		ls := s.Labels()
		// Determine ref by looking at labels hash — we need a stable key.
		// Use the series ref which is the map key from the querier.
		// Actually, we need to match by labels since the oracle uses refs.
		// Let's collect by label hash.
		it := s.Iterator()
		var samples []sample
		for it.Next() {
			st, sv := it.At()
			samples = append(samples, sample{st, sv})
		}
		if err := it.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(samples) > 0 {
			h := labels.Hash(ls)
			result[h] = samples
		}
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return result
}

// oracleByLabelHash re-keys oracle results by label hash for comparison.
func oracleByLabelHash(o *oracle, mint, maxt int64, matchers ...*labels.Matcher) map[uint64][]sample {
	byRef := o.query(mint, maxt, matchers...)
	result := make(map[uint64][]sample, len(byRef))
	for ref, samples := range byRef {
		ls := o.labels[ref]
		h := labels.Hash(ls)
		result[h] = samples
	}
	return result
}

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestQueryOracle(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, db *DB, o *oracle)
		queries  []queryCase
	}{
		{
			name: "head_only",
			setup: func(t *testing.T, db *DB, o *oracle) {
				app := db.Appender()
				ref, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), 1000, 71.3)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				o.addSeries(ref, labels.FromStrings("__name__", "temp", "room", "office"))
				o.addSample(ref, 1000, 71.3)

				_, err = app.Append(ref, nil, 2000, 71.4)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				o.addSample(ref, 2000, 71.4)

				ref2, err := app.Append(0, labels.FromStrings("__name__", "humidity", "room", "office"), 1000, 55.0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				o.addSeries(ref2, labels.FromStrings("__name__", "humidity", "room", "office"))
				o.addSample(ref2, 1000, 55.0)

				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			queries: []queryCase{
				{
					name:     "match_all_by_room",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "room", "office")},
				},
				{
					name:     "match_temp_only",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
				{
					name:     "match_not_equal",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "__name__", "temp")},
				},
				{
					name:     "match_regex",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", "te.*")},
				},
				{
					name:     "match_not_regex",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotRegexp, "__name__", "te.*")},
				},
				{
					name:     "time_range_filter",
					mint:     1500,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
				{
					name:     "no_match",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "pressure")},
				},
			},
		},
		{
			name: "block_only",
			setup: func(t *testing.T, db *DB, o *oracle) {
				app := db.Appender()
				// Write enough samples to seal chunks (need >120 for a sealed chunk).
				ref, err := app.Append(0, labels.FromStrings("__name__", "temp"), 0, 0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				o.addSeries(ref, labels.FromStrings("__name__", "temp"))
				o.addSample(ref, 0, 0)
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					o.addSample(ref, int64(i*15000), float64(i))
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// Flush to block.
				_, err = db.FlushOlderThan(math.MaxInt64)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			queries: []queryCase{
				{
					name:     "query_all",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
				{
					name:     "query_subset",
					mint:     100000,
					maxt:     200000,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
			},
		},
		{
			name: "head_block_merge_seam",
			setup: func(t *testing.T, db *DB, o *oracle) {
				app := db.Appender()
				ref, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), 0, 0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				o.addSeries(ref, labels.FromStrings("__name__", "temp", "room", "office"))
				o.addSample(ref, 0, 0)

				// Write 250 samples (2 sealed chunks + 10 active).
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					o.addSample(ref, int64(i*15000), float64(i))
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// Flush sealed chunks to block.
				_, err = db.FlushOlderThan(math.MaxInt64)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// Append more samples to head (after flush).
				app = db.Appender()
				for i := 250; i < 260; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					o.addSample(ref, int64(i*15000), float64(i))
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			queries: []queryCase{
				{
					name:     "full_range",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
				{
					name:     "block_portion_only",
					mint:     0,
					maxt:     120 * 15000,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
				{
					name:     "head_portion_only",
					mint:     250 * 15000,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
				{
					name:     "overlap_seam",
					mint:     230 * 15000,
					maxt:     255 * 15000,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "room", "office")},
				},
			},
		},
		{
			name: "multiple_series_with_matchers",
			setup: func(t *testing.T, db *DB, o *oracle) {
				series := []struct {
					ls []labels.Label
				}{
					{ls: labels.FromStrings("__name__", "temp", "room", "office")},
					{ls: labels.FromStrings("__name__", "temp", "room", "kitchen")},
					{ls: labels.FromStrings("__name__", "humidity", "room", "office")},
					{ls: labels.FromStrings("__name__", "pressure", "room", "lab")},
				}

				app := db.Appender()
				for _, s := range series {
					ref, err := app.Append(0, s.ls, 1000, 1.0)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					o.addSeries(ref, s.ls)
					o.addSample(ref, 1000, 1.0)

					_, err = app.Append(ref, nil, 2000, 2.0)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					o.addSample(ref, 2000, 2.0)
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			queries: []queryCase{
				{
					name:     "equal_name_temp",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
				{
					name:     "equal_room_office",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "room", "office")},
				},
				{
					name:     "combined_matchers",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp"),
						labels.MustNewMatcher(labels.MatchEqual, "room", "office"),
					},
				},
				{
					name:     "regex_name",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", "temp|humidity")},
				},
				{
					name:     "not_equal_room",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "room", "office")},
				},
				{
					name:     "not_regex_room",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotRegexp, "room", "off.*")},
				},
			},
		},
		{
			name: "multiple_blocks",
			setup: func(t *testing.T, db *DB, o *oracle) {
				app := db.Appender()
				ref, err := app.Append(0, labels.FromStrings("__name__", "temp"), 0, 0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				o.addSeries(ref, labels.FromStrings("__name__", "temp"))
				o.addSample(ref, 0, 0)
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					o.addSample(ref, int64(i*15000), float64(i))
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// First flush.
				_, err = db.FlushOlderThan(math.MaxInt64)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// More data -> second flush.
				app = db.Appender()
				for i := 250; i < 500; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					o.addSample(ref, int64(i*15000), float64(i))
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				_, err = db.FlushOlderThan(math.MaxInt64)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// Some data in head.
				app = db.Appender()
				for i := 500; i < 510; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					o.addSample(ref, int64(i*15000), float64(i))
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			queries: []queryCase{
				{
					name:     "full_range",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
				{
					name:     "second_block_range",
					mint:     300 * 15000,
					maxt:     400 * 15000,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
			},
		},
		{
			name: "empty_results",
			setup: func(t *testing.T, db *DB, o *oracle) {
				app := db.Appender()
				ref, err := app.Append(0, labels.FromStrings("__name__", "temp"), 1000, 1.0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				o.addSeries(ref, labels.FromStrings("__name__", "temp"))
				o.addSample(ref, 1000, 1.0)
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			queries: []queryCase{
				{
					name:     "no_matching_series",
					mint:     math.MinInt64,
					maxt:     math.MaxInt64,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "nonexistent")},
				},
				{
					name:     "time_range_miss",
					mint:     5000,
					maxt:     6000,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			o := newOracle()

			tc.setup(t, db, o)

			for _, qc := range tc.queries {
				t.Run(qc.name, func(t *testing.T) {
					q, err := db.Querier(qc.mint, qc.maxt)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					defer q.Close()

					ss := q.Select(qc.matchers...)
					got := collectSeriesSet(t, ss)
					want := oracleByLabelHash(o, qc.mint, qc.maxt, qc.matchers...)

					if len(got) != len(want) {
						t.Errorf("series count mismatch: got %v, want %v", len(got), len(want))
					}
					for h, wantSamples := range want {
						gotSamples, ok := got[h]
						if !ok {
							t.Errorf("missing series with hash %d", h)
						}
						if !reflect.DeepEqual(gotSamples, wantSamples) {
							t.Errorf("sample mismatch for hash %d: got %v, want %v", h, gotSamples, wantSamples)
						}
					}
				})
			}
		})
	}
}

type queryCase struct {
	name     string
	mint     int64
	maxt     int64
	matchers []*labels.Matcher
}

func TestDBLifecycle(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(t *testing.T, dir string) *DB
		wantSampleCount int
		wantSeriesCount int
		matchers        []*labels.Matcher
		mint            int64
		maxt            int64
	}{
		{
			name: "append_and_query_back",
			setup: func(t *testing.T, dir string) *DB {
				db, err := Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				app := db.Appender()
				ref, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), 1000, 71.3)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				_, err = app.Append(ref, nil, 2000, 71.4)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return db
			},
			wantSampleCount: 2,
			wantSeriesCount: 1,
			matchers:        []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "room", "office")},
			mint:            math.MinInt64,
			maxt:            math.MaxInt64,
		},
		{
			name: "reopen_with_blocks",
			setup: func(t *testing.T, dir string) *DB {
				db, err := Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
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
				if err := db.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				db2, err := Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return db2
			},
			wantSampleCount: 250,
			wantSeriesCount: 1,
			matchers:        []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
			mint:            math.MinInt64,
			maxt:            math.MaxInt64,
		},
		{
			name: "compacted_blocks_queryable",
			setup: func(t *testing.T, dir string) *DB {
				db, err := Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				const samplesPerBlock = 130
				ref := uint64(0)
				for b := 0; b < 4; b++ {
					app := db.Appender()
					for i := 0; i < samplesPerBlock; i++ {
						ts := int64((b*samplesPerBlock + i) * 15000)
						r, err := app.Append(ref, labels.FromStrings("__name__", "temp"), ts, float64(ts))
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						ref = r
					}
					if err := app.Commit(); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					_, err := db.FlushOlderThan(math.MaxInt64)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				}
				if err := db.RunCompaction(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return db
			},
			wantSampleCount: 520,
			wantSeriesCount: 1,
			matchers:        []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp")},
			mint:            math.MinInt64,
			maxt:            math.MaxInt64,
		},
		{
			name: "retention_drops_old_keeps_recent",
			setup: func(t *testing.T, dir string) *DB {
				now := int64(100 * 3600 * 1000)
				db, err := Open(dir, Options{
					Retention: 24 * time.Hour,
					Clock:     func() int64 { return now },
				})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				// Old data (50 hours ago).
				app := db.Appender()
				ref, err := app.Append(0, labels.FromStrings("__name__", "old"), 50*3600*1000, 1.0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(50*3600*1000+i*15000), float64(i))
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
				// Recent data (1 hour ago).
				app = db.Appender()
				ref2, err := app.Append(0, labels.FromStrings("__name__", "recent"), 99*3600*1000, 1.0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref2, nil, int64(99*3600*1000+i*15000), float64(i))
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
				db.ApplyRetention()
				return db
			},
			wantSampleCount: 250,
			wantSeriesCount: 1,
			matchers:        []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "recent")},
			mint:            math.MinInt64,
			maxt:            math.MaxInt64,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := tc.setup(t, dir)
			defer db.Close()

			q, err := db.Querier(tc.mint, tc.maxt)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer q.Close()

			ss := q.Select(tc.matchers...)
			seriesCount := 0
			sampleCount := 0
			for ss.Next() {
				seriesCount++
				it := ss.At().Iterator()
				for it.Next() {
					sampleCount++
				}
				if err := it.Err(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			if err := ss.Err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if seriesCount != tc.wantSeriesCount {
				t.Errorf("series count: got %v, want %v", seriesCount, tc.wantSeriesCount)
			}
			if sampleCount != tc.wantSampleCount {
				t.Errorf("sample count: got %v, want %v", sampleCount, tc.wantSampleCount)
			}
		})
	}
}

func TestQueryDuringCompaction(t *testing.T) {
	tests := []struct {
		name            string
		numBlocks       int
		samplesPerBlock int
	}{
		{
			name:            "four_blocks",
			numBlocks:       4,
			samplesPerBlock: 130,
		},
		{
			name:            "two_blocks",
			numBlocks:       2,
			samplesPerBlock: 130,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, Options{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer db.Close()

			ref := uint64(0)
			for b := 0; b < tc.numBlocks; b++ {
				app := db.Appender()
				for i := 0; i < tc.samplesPerBlock; i++ {
					ts := int64((b*tc.samplesPerBlock + i) * 15000)
					r, err := app.Append(ref, labels.FromStrings("__name__", "temp"), ts, float64(ts))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					ref = r
				}
				if err := app.Commit(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				_, err := db.FlushOlderThan(math.MaxInt64)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			// Snapshot source block dirs.
			db.mu.RLock()
			sourceDirs := make([]string, len(db.blocks))
			for i, b := range db.blocks {
				sourceDirs[i] = b.Dir()
			}
			db.mu.RUnlock()

			// Start query holding refs on all blocks.
			q, err := db.Querier(math.MinInt64, math.MaxInt64)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp"))
			if !ss.Next() {
				t.Fatalf("expected true")
			}
			it := ss.At().Iterator()
			if !it.Next() {
				t.Fatalf("expected true")
			}

			// Compact while query is open.
			err = db.RunCompaction()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Source dirs still exist (query holds refs).
			for _, d := range sourceDirs {
				_, statErr := os.Stat(d)
				if statErr != nil {
					t.Errorf("source dir should exist while query holds ref: %v", statErr)
				}
			}

			// Finish iterating — all data still readable.
			count := 1
			for it.Next() {
				count++
			}
			if err := it.Err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != tc.samplesPerBlock*tc.numBlocks {
				t.Errorf("all samples readable during compaction: got %v, want %v", count, tc.samplesPerBlock*tc.numBlocks)
			}

			// Close querier — source dirs should be deleted.
			if err := q.Close(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, d := range sourceDirs {
				_, statErr := os.Stat(d)
				if !os.IsNotExist(statErr) {
					t.Errorf("source dir should be deleted after query close")
				}
			}

			// Compacted block still queryable.
			q2, err := db.Querier(math.MinInt64, math.MaxInt64)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer q2.Close()
			ss2 := q2.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp"))
			count = 0
			for ss2.Next() {
				it2 := ss2.At().Iterator()
				for it2.Next() {
					count++
				}
				if err := it2.Err(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			if err := ss2.Err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if count != tc.samplesPerBlock*tc.numBlocks {
				t.Errorf("compacted block has all samples: got %v, want %v", count, tc.samplesPerBlock*tc.numBlocks)
			}
		})
	}
}
