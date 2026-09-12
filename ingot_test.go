package ingot

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/davidhrinaldo/ingot/internal/block"
	"github.com/davidhrinaldo/ingot/internal/wal"
	"github.com/davidhrinaldo/ingot/labels"
)

// sample is a test convenience type.
type sample struct {
	t int64
	v float64
}

// oracle is a naive reference implementation for query comparison.
type oracle struct {
	series map[uint64][]sample       // ref -> samples in order
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

func TestAppenderRejectsStaleCommit(t *testing.T) {
	db := openTestDB(t)
	firstLabels := labels.FromStrings("__name__", "first")
	secondLabels := labels.FromStrings("__name__", "second")

	seed := db.Appender()
	firstRef, err := seed.Append(0, firstLabels, 1, 1)
	if err != nil {
		t.Fatalf("append first seed sample: %v", err)
	}
	secondRef, err := seed.Append(0, secondLabels, 1, 1)
	if err != nil {
		t.Fatalf("append second seed sample: %v", err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatalf("commit seed samples: %v", err)
	}

	stale := db.Appender()
	if _, err := stale.Append(firstRef, nil, 2, 2); err != nil {
		t.Fatalf("append non-conflicting sample: %v", err)
	}
	if _, err := stale.Append(secondRef, nil, 2, 2); err != nil {
		t.Fatalf("append stale sample: %v", err)
	}
	winner := db.Appender()
	if _, err := winner.Append(secondRef, nil, 3, 3); err != nil {
		t.Fatalf("append winning sample: %v", err)
	}
	if err := winner.Commit(); err != nil {
		t.Fatalf("commit winning sample: %v", err)
	}
	if err := stale.Commit(); err == nil {
		t.Fatal("stale commit succeeded")
	}

	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("open querier: %v", err)
	}
	defer q.Close()
	got := collectSeriesSet(t, q.Select())
	want := map[uint64][]sample{
		labels.Hash(firstLabels):  {{t: 1, v: 1}},
		labels.Hash(secondLabels): {{t: 1, v: 1}, {t: 3, v: 3}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("samples after rejected commit: got %v, want %v", got, want)
	}
}

func TestAppenderRejectsAppendAfterClose(t *testing.T) {
	for _, closeAppender := range []struct {
		name string
		fn   func(*Appender) error
	}{
		{name: "commit", fn: func(app *Appender) error { return app.Commit() }},
		{name: "rollback", fn: func(app *Appender) error { return app.Rollback() }},
	} {
		t.Run(closeAppender.name, func(t *testing.T) {
			db := openTestDB(t)
			app := db.Appender()
			if err := closeAppender.fn(app); err != nil {
				t.Fatalf("close appender: %v", err)
			}
			if _, err := app.Append(0, labels.FromStrings("__name__", "closed"), 1, 1); err == nil {
				t.Fatal("append after close succeeded")
			}
			if got := db.Stats().HeadSeries; got != 0 {
				t.Fatalf("registered series after close: got %d, want 0", got)
			}
		})
	}
}

func TestSyncPolicyOptions(t *testing.T) {
	tests := []struct {
		name         string
		opts         Options
		wantPolicy   wal.SyncPolicy
		wantInterval time.Duration
		wantErr      bool
	}{
		{name: "default_is_sync_on_commit", opts: Options{}, wantPolicy: wal.SyncOnCommit},
		{
			name:         "periodic_default_interval",
			opts:         Options{SyncPolicy: SyncPeriodic},
			wantPolicy:   wal.SyncPeriodic,
			wantInterval: time.Second,
		},
		{
			name:         "periodic_custom_interval",
			opts:         Options{SyncPolicy: SyncPeriodic, SyncInterval: 25 * time.Millisecond},
			wantPolicy:   wal.SyncPeriodic,
			wantInterval: 25 * time.Millisecond,
		},
		{name: "commit_rejects_interval", opts: Options{SyncInterval: time.Second}, wantErr: true},
		{name: "periodic_rejects_negative_interval", opts: Options{SyncPolicy: SyncPeriodic, SyncInterval: -1}, wantErr: true},
		{name: "rejects_unknown_policy", opts: Options{SyncPolicy: SyncPolicy(99)}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.opts.walOptions()
			if (err != nil) != tc.wantErr {
				t.Fatalf("wal options error: got %v, want error=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got.SyncPolicy != tc.wantPolicy {
				t.Errorf("sync policy: got %v, want %v", got.SyncPolicy, tc.wantPolicy)
			}
			if got.SyncInterval != tc.wantInterval {
				t.Errorf("sync interval: got %v, want %v", got.SyncInterval, tc.wantInterval)
			}
		})
	}
}

func TestQueryOracle(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, db *DB, o *oracle)
		queries []queryCase
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
					name: "combined_matchers",
					mint: math.MinInt64,
					maxt: math.MaxInt64,
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

func TestMatcherAbsentLabelSemantics(t *testing.T) {
	tests := []struct {
		name    string
		matcher *labels.Matcher
		want    []string
	}{
		{name: "equal_empty", matcher: labels.MustNewMatcher(labels.MatchEqual, "state", ""), want: []string{"explicit-empty", "missing"}},
		{name: "equal_nonempty", matcher: labels.MustNewMatcher(labels.MatchEqual, "state", "up"), want: []string{"nonempty"}},
		{name: "not_equal_empty", matcher: labels.MustNewMatcher(labels.MatchNotEqual, "state", ""), want: []string{"nonempty"}},
		{name: "not_equal_nonempty", matcher: labels.MustNewMatcher(labels.MatchNotEqual, "state", "up"), want: []string{"explicit-empty", "missing"}},
		{name: "regexp_matches_empty", matcher: labels.MustNewMatcher(labels.MatchRegexp, "state", ".*"), want: []string{"explicit-empty", "missing", "nonempty"}},
		{name: "regexp_rejects_empty", matcher: labels.MustNewMatcher(labels.MatchRegexp, "state", ".+"), want: []string{"nonempty"}},
		{name: "not_regexp_rejects_empty", matcher: labels.MustNewMatcher(labels.MatchNotRegexp, "state", ".*"), want: nil},
		{name: "not_regexp_matches_empty", matcher: labels.MustNewMatcher(labels.MatchNotRegexp, "state", ".+"), want: []string{"explicit-empty", "missing"}},
		{name: "nil", matcher: nil, want: nil},
		{name: "unsupported", matcher: &labels.Matcher{Type: labels.MatchType(99), Name: "state"}, want: nil},
		{name: "uncompiled_regexp", matcher: &labels.Matcher{Type: labels.MatchRegexp, Name: "state", Value: ".*"}, want: nil},
	}

	for _, blockOnly := range []bool{false, true} {
		source := "head"
		if blockOnly {
			source = "block_reopen"
		}
		t.Run(source, func(t *testing.T) {
			dataDir := t.TempDir()
			db, err := Open(dataDir, Options{})
			if err != nil {
				t.Fatalf("open DB: %v", err)
			}

			series := [][]labels.Label{
				labels.FromStrings("__name__", "missing"),
				labels.FromStrings("__name__", "explicit-empty", "state", ""),
				labels.FromStrings("__name__", "nonempty", "state", "up"),
			}
			app := db.Appender()
			for _, ls := range series {
				ref, err := app.Append(0, ls, 0, 0)
				if err != nil {
					db.Close()
					t.Fatalf("append series: %v", err)
				}
				if blockOnly {
					for i := 1; i <= 120; i++ {
						if _, err := app.Append(ref, nil, int64(i), float64(i)); err != nil {
							db.Close()
							t.Fatalf("append sample: %v", err)
						}
					}
				}
			}
			if err := app.Commit(); err != nil {
				db.Close()
				t.Fatalf("commit: %v", err)
			}

			if blockOnly {
				if _, err := db.FlushOlderThan(math.MaxInt64); err != nil {
					db.Close()
					t.Fatalf("flush: %v", err)
				}
				if err := db.Close(); err != nil {
					t.Fatalf("close before reopen: %v", err)
				}
				if err := os.RemoveAll(filepath.Join(dataDir, "wal")); err != nil {
					t.Fatalf("remove WAL: %v", err)
				}
				db, err = Open(dataDir, Options{})
				if err != nil {
					t.Fatalf("reopen DB: %v", err)
				}
			}
			defer db.Close()

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					q, err := db.Querier(math.MinInt64, math.MaxInt64)
					if err != nil {
						t.Fatalf("querier: %v", err)
					}
					ss := q.Select(tc.matcher)
					var got []string
					for ss.Next() {
						for _, l := range ss.At().Labels() {
							if l.Name == "__name__" {
								got = append(got, l.Value)
							}
						}
					}
					if err := ss.Err(); err != nil {
						q.Close()
						t.Fatalf("select: %v", err)
					}
					if err := q.Close(); err != nil {
						t.Fatalf("close querier: %v", err)
					}
					sort.Strings(got)
					if !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("series: got %v, want %v", got, tc.want)
					}
				})
			}

			q, err := db.Querier(math.MinInt64, math.MaxInt64)
			if err != nil {
				t.Fatalf("querier for mutation test: %v", err)
			}
			ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", "missing"))
			if !ss.Next() {
				q.Close()
				t.Fatal("series for mutation test not found")
			}
			result := ss.At()
			got := result.Labels()
			got[0].Value = "changed"
			if got = result.Labels(); got[0].Value != "missing" {
				q.Close()
				t.Fatalf("result labels changed through returned slice: %v", got)
			}
			if err := q.Close(); err != nil {
				t.Fatalf("close mutation querier: %v", err)
			}
		})
	}
}

func TestQuerierSnapshotsHeadAcrossFlush(t *testing.T) {
	tests := []struct {
		name         string
		selectBefore bool
	}{
		{name: "select_and_iterator_after_flush"},
		{name: "iterator_after_flush", selectBefore: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			app := db.Appender()
			ref, err := app.Append(0, labels.FromStrings("__name__", "temp"), 0, 0)
			if err != nil {
				t.Fatalf("append first sample: %v", err)
			}
			for i := 1; i < 250; i++ {
				if _, err := app.Append(ref, nil, int64(i), float64(i)); err != nil {
					t.Fatalf("append sample %d: %v", i, err)
				}
			}
			if err := app.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}

			q, err := db.Querier(math.MinInt64, math.MaxInt64)
			if err != nil {
				t.Fatalf("create querier: %v", err)
			}
			defer q.Close()

			var ss SeriesSet
			if tc.selectBefore {
				ss = q.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp"))
			}
			if _, err := db.FlushOlderThan(math.MaxInt64); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if !tc.selectBefore {
				ss = q.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp"))
			}

			if !ss.Next() {
				t.Fatalf("missing series after flush: %v", ss.Err())
			}
			it := ss.At().Iterator()
			for i := 0; i < 250; i++ {
				if !it.Next() {
					t.Fatalf("sample count: got %d, want 250: %v", i, it.Err())
				}
				gotT, gotV := it.At()
				if gotT != int64(i) || gotV != float64(i) {
					t.Fatalf("sample %d: got (%d, %v), want (%d, %d)", i, gotT, gotV, i, i)
				}
			}
			if it.Next() {
				t.Fatal("iterator returned more than 250 samples")
			}
			if err := it.Err(); err != nil {
				t.Fatalf("iterate: %v", err)
			}
			if ss.Next() {
				t.Fatal("series set returned more than one series")
			}
			if err := ss.Err(); err != nil {
				t.Fatalf("select: %v", err)
			}
		})
	}
}

func TestQuerierExcludesCommitsAfterCreation(t *testing.T) {
	db := openTestDB(t)
	app := db.Appender()
	ref, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), 1, 1)
	if err != nil {
		t.Fatalf("append initial sample: %v", err)
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("commit initial sample: %v", err)
	}

	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("create querier: %v", err)
	}
	defer q.Close()

	app = db.Appender()
	if _, err := app.Append(ref, nil, 2, 2); err != nil {
		t.Fatalf("append to existing series: %v", err)
	}
	if _, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "kitchen"), 1, 10); err != nil {
		t.Fatalf("append new series: %v", err)
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("commit later samples: %v", err)
	}

	ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp"))
	if !ss.Next() {
		t.Fatalf("missing snapshot series: %v", ss.Err())
	}
	it := ss.At().Iterator()
	if !it.Next() {
		t.Fatalf("missing snapshot sample: %v", it.Err())
	}
	if gotT, gotV := it.At(); gotT != 1 || gotV != 1 {
		t.Fatalf("snapshot sample: got (%d, %v), want (1, 1)", gotT, gotV)
	}
	if it.Next() {
		t.Fatal("post-creation sample leaked into querier")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if ss.Next() {
		t.Fatal("post-creation series leaked into querier")
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("select: %v", err)
	}
}

func TestQuerierCloseReleasesHeadSnapshot(t *testing.T) {
	db := openTestDB(t)
	app := db.Appender()
	if _, err := app.Append(0, labels.FromStrings("__name__", "temp"), 1, 1); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("create querier: %v", err)
	}
	if q.head == nil {
		t.Fatal("querier has no head snapshot")
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close querier: %v", err)
	}
	if q.head != nil {
		t.Fatal("closed querier retained its head snapshot")
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close querier again: %v", err)
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

func TestRestartReconcilesCompactionLineage(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writeTestBlocks(t, db, []int64{0, 4 * 3600 * 1000})

	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("open querier: %v", err)
	}
	if err := db.RunCompaction(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if got := db.Stats().Blocks; got != 1 {
		t.Fatalf("blocks after compaction: got %d, want 1", got)
	}
	if got := blockDirectoryCount(t, dir); got != 3 {
		t.Fatalf("block directories while sources are pinned: got %d, want 3", got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer db.Close()
	if got := db.Stats().Blocks; got != 1 {
		t.Fatalf("blocks after restart: got %d, want 1", got)
	}
	if got := blockDirectoryCount(t, dir); got != 1 {
		t.Fatalf("block directories after restart: got %d, want 1", got)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close lingering querier: %v", err)
	}
}

func TestRestartReconcilesFlattenedCompactionLineage(t *testing.T) {
	const hour = int64(3600 * 1000)
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writeTestBlocks(t, db, []int64{0, 4 * hour, 12 * hour, 16 * hour})

	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("open querier: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := db.RunCompaction(); err != nil {
			t.Fatalf("compaction %d: %v", i+1, err)
		}
	}
	if got := db.Stats().Blocks; got != 1 {
		t.Fatalf("blocks after cascading compaction: got %d, want 1", got)
	}
	db.mu.RLock()
	sourceCount := len(db.blocks[0].Meta.Compaction.Sources)
	db.mu.RUnlock()
	if sourceCount != 4 {
		t.Fatalf("persisted original sources: got %d, want 4", sourceCount)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer db.Close()
	if got := db.Stats().Blocks; got != 1 {
		t.Fatalf("blocks after restart: got %d, want 1", got)
	}
	if got := blockDirectoryCount(t, dir); got != 1 {
		t.Fatalf("block directories after restart: got %d, want 1", got)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close lingering querier: %v", err)
	}
}

func TestRestartPreservesSourcesWhenCompactionReplacementIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writeTestBlocks(t, db, []int64{0, 4 * 3600 * 1000})

	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("open querier: %v", err)
	}
	if err := db.RunCompaction(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	db.mu.RLock()
	replacementDir := db.blocks[0].Dir()
	db.mu.RUnlock()
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}
	corruptLastChunkCRC(t, replacementDir)

	if _, err := Open(dir, Options{}); !errors.Is(err, block.ErrCorruptChunk) {
		t.Fatalf("open with corrupt replacement: got %v, want %v", err, block.ErrCorruptChunk)
	}
	if got := blockDirectoryCount(t, dir); got != 3 {
		t.Fatalf("block directories after rejected replacement: got %d, want 3", got)
	}

	if err := os.RemoveAll(replacementDir); err != nil {
		t.Fatalf("remove corrupt replacement: %v", err)
	}
	recovered, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open healthy sources: %v", err)
	}
	if got := recovered.Stats().Blocks; got != 2 {
		t.Fatalf("healthy source blocks after recovery: got %d, want 2", got)
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("close recovered DB: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close lingering querier: %v", err)
	}
}

func TestConcurrentCompactionsAndRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writeTestBlocks(t, db, []int64{0, 4 * 3600 * 1000})

	db.lifecycleMu.Lock()
	started := make(chan struct{}, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			started <- struct{}{}
			errs <- db.RunCompaction()
		}()
	}
	<-started
	<-started
	db.lifecycleMu.Unlock()

	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent compaction: %v", err)
		}
	}
	if got := db.Stats(); got.Blocks != 1 || got.Compactions != 1 {
		t.Fatalf("stats after concurrent compactions: got %+v, want 1 block and 1 compaction", got)
	}
	if got := blockDirectoryCount(t, dir); got != 1 {
		t.Fatalf("block directories after concurrent compactions: got %d, want 1", got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	defer db.Close()
	if got := db.Stats().Blocks; got != 1 {
		t.Fatalf("blocks after restart: got %d, want 1", got)
	}
	if got := blockDirectoryCount(t, dir); got != 1 {
		t.Fatalf("block directories after restart: got %d, want 1", got)
	}
}

func TestConcurrentCompactionAndRetention(t *testing.T) {
	const hour = int64(3600 * 1000)
	db, err := Open(t.TempDir(), Options{
		Retention: time.Hour,
		Clock:     func() int64 { return 100 * hour },
	})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	defer db.Close()
	writeTestBlocks(t, db, []int64{0, 4 * hour})

	db.lifecycleMu.Lock()
	started := make(chan struct{}, 2)
	errs := make(chan error, 2)
	go func() {
		started <- struct{}{}
		errs <- db.RunCompaction()
	}()
	go func() {
		started <- struct{}{}
		errs <- db.RunRetention()
	}()
	<-started
	<-started
	db.lifecycleMu.Unlock()

	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("maintenance operation: %v", err)
		}
	}
	if got := db.Stats().Blocks; got != 0 {
		t.Fatalf("blocks after retention: got %d, want 0", got)
	}
}

func TestLifecycleCallsRejectedWhileClosingAndAfterClose(t *testing.T) {
	db, err := Open(t.TempDir(), Options{Retention: time.Hour})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writeTestBlocks(t, db, []int64{0, 4 * 3600 * 1000})
	wantBlocks := db.Stats().Blocks

	db.compactWg.Add(1)
	closeErr := make(chan error, 1)
	go func() {
		closeErr <- db.Close()
	}()
	waitForClosing(t, db)
	secondCloseErr := make(chan error, 1)
	go func() {
		secondCloseErr <- db.Close()
	}()

	assertLifecycleClosed(t, db)
	db.ApplyRetention()
	if got := db.Stats().Blocks; got != wantBlocks {
		t.Fatalf("ApplyRetention changed blocks while closing: got %d, want %d", got, wantBlocks)
	}

	db.compactWg.Done()
	if err := <-closeErr; err != nil {
		t.Fatalf("close DB: %v", err)
	}
	if err := <-secondCloseErr; err != nil {
		t.Fatalf("concurrent close DB: %v", err)
	}
	assertLifecycleClosed(t, db)
	db.ApplyRetention()
	if got := db.Stats().Blocks; got != wantBlocks {
		t.Fatalf("ApplyRetention changed blocks after close: got %d, want %d", got, wantBlocks)
	}
}

func TestApplyRetentionReportsDeletionFailureOnClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{
		Retention: time.Hour,
		Clock:     func() int64 { return 100 * 3600 * 1000 },
	})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writeTestBlocks(t, db, []int64{0})
	db.removeBlockDir = func(string) error { return errors.New("injected deletion failure") }
	db.ApplyRetention()
	if err := db.Close(); err == nil {
		t.Fatal("Close did not report the retention deletion failure")
	}
}

func TestRetentionDeletionFailureRetries(t *testing.T) {
	dir := t.TempDir()
	db, blockName := openExpiredTestBlock(t, dir)
	removeErr := errors.New("injected deletion failure")
	db.removeBlockDir = func(string) error { return removeErr }
	if err := db.RunRetention(); !errors.Is(err, removeErr) {
		t.Fatalf("retention deletion: got %v, want %v", err, removeErr)
	}
	if got := db.Stats().Blocks; got != 0 {
		t.Fatalf("active blocks after retention: got %d, want 0", got)
	}
	assertRetentionFiles(t, dir, blockName, true)

	db.removeBlockDir = removeBlockDirectory
	if err := db.RunRetention(); err != nil {
		t.Fatalf("retry retention deletion: %v", err)
	}
	assertRetentionFiles(t, dir, blockName, false)
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}
}

func TestRetentionTombstonePreventsRestartResurrection(t *testing.T) {
	dir := t.TempDir()
	db, blockName := openExpiredTestBlock(t, dir)
	removeErr := errors.New("injected deletion failure")
	db.removeBlockDir = func(string) error { return removeErr }
	if err := db.RunRetention(); !errors.Is(err, removeErr) {
		t.Fatalf("retention deletion: got %v, want %v", err, removeErr)
	}
	assertRetentionFiles(t, dir, blockName, true)
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}

	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	if got := db.Stats().Blocks; got != 0 {
		t.Fatalf("blocks after tombstone recovery: got %d, want 0", got)
	}
	assertRetentionFiles(t, dir, blockName, false)
	if err := db.Close(); err != nil {
		t.Fatalf("close reopened DB: %v", err)
	}
}

func TestRetentionTombstonesCompactionSources(t *testing.T) {
	const hour = int64(3600 * 1000)
	dir := t.TempDir()
	db, err := Open(dir, Options{
		Retention: time.Hour,
		Clock:     func() int64 { return 100 * hour },
	})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writeTestBlocks(t, db, []int64{0, 4 * hour})
	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("open querier: %v", err)
	}
	db.mu.RLock()
	var sourceDirs []string
	for _, b := range db.blocks {
		sourceDirs = append(sourceDirs, b.Dir())
	}
	db.mu.RUnlock()
	if err := db.RunCompaction(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	db.mu.RLock()
	replacementDir := db.blocks[0].Dir()
	db.mu.RUnlock()
	removeErr := errors.New("injected source deletion failure")
	db.removeBlockDir = func(dir string) error {
		if dir == replacementDir {
			return removeBlockDirectory(dir)
		}
		return removeErr
	}
	if err := db.RunRetention(); !errors.Is(err, removeErr) {
		t.Fatalf("apply retention: got %v, want %v", err, removeErr)
	}
	if got := blockDirectoryCount(t, dir); got != 2 {
		t.Fatalf("query-pinned source directories: got %d, want 2", got)
	}
	for _, sourceDir := range sourceDirs {
		if _, err := os.Stat(sourceDir); err != nil {
			t.Fatalf("retained source directory %s: %v", sourceDir, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, retentionTombstoneDir, filepath.Base(replacementDir))); err != nil {
		t.Fatalf("retention lineage tombstone: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}

	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	if got := db.Stats().Blocks; got != 0 {
		t.Fatalf("blocks after retention restart: got %d, want 0", got)
	}
	if got := blockDirectoryCount(t, dir); got != 0 {
		t.Fatalf("block directories after retention restart: got %d, want 0", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close reopened DB: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close lingering querier: %v", err)
	}
}

func openExpiredTestBlock(t *testing.T, dir string) (*DB, string) {
	t.Helper()
	db, err := Open(dir, Options{
		Retention: time.Hour,
		Clock:     func() int64 { return 100 * 3600 * 1000 },
	})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	writeTestBlocks(t, db, []int64{0})
	blockName := filepath.Base(db.blocks[0].Dir())
	return db, blockName
}

func assertRetentionFiles(t *testing.T, dir, blockName string, want bool) {
	t.Helper()
	for _, path := range []string{
		filepath.Join(dir, blockName),
		filepath.Join(dir, retentionTombstoneDir, blockName),
	} {
		_, err := os.Stat(path)
		if want && err != nil {
			t.Errorf("expected %s to exist: %v", path, err)
		}
		if !want && !os.IsNotExist(err) {
			t.Errorf("expected %s to be absent: %v", path, err)
		}
	}
}

func assertLifecycleClosed(t *testing.T, db *DB) {
	t.Helper()
	if _, err := db.FlushOlderThan(math.MaxInt64); !errors.Is(err, ErrClosed) {
		t.Errorf("FlushOlderThan error: got %v, want %v", err, ErrClosed)
	}
	if err := db.RunCompaction(); !errors.Is(err, ErrClosed) {
		t.Errorf("RunCompaction error: got %v, want %v", err, ErrClosed)
	}
	if err := db.RunRetention(); !errors.Is(err, ErrClosed) {
		t.Errorf("RunRetention error: got %v, want %v", err, ErrClosed)
	}
}

func waitForClosing(t *testing.T, db *DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		db.lifecycleMu.Lock()
		closing := db.closing
		db.lifecycleMu.Unlock()
		if closing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("DB did not enter closing state")
}

func corruptLastChunkCRC(t *testing.T, blockDir string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(blockDir, "chunks"))
	if err != nil {
		t.Fatalf("read chunk directory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("replacement has no chunk segments")
	}
	chunkPath := filepath.Join(blockDir, "chunks", entries[len(entries)-1].Name())
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		t.Fatalf("read chunk segment: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(chunkPath, data, 0644); err != nil {
		t.Fatalf("corrupt chunk CRC: %v", err)
	}
}

func writeTestBlocks(t *testing.T, db *DB, starts []int64) {
	t.Helper()
	for _, start := range starts {
		var ref uint64
		app := db.Appender()
		for i := 0; i < 130; i++ {
			ls := []labels.Label(nil)
			if ref == 0 {
				ls = labels.FromStrings("__name__", "lifecycle_test", "block", strconv.FormatInt(start, 10))
			}
			var err error
			ref, err = app.Append(ref, ls, start+int64(i)*15000, float64(i))
			if err != nil {
				t.Fatalf("append sample: %v", err)
			}
		}
		if err := app.Commit(); err != nil {
			t.Fatalf("commit samples: %v", err)
		}
		if _, err := db.FlushOlderThan(math.MaxInt64); err != nil {
			t.Fatalf("flush block: %v", err)
		}
	}
}

func blockDirectoryCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read data directory: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "wal" {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, entry.Name(), "meta.json")); err == nil {
			count++
		}
	}
	return count
}
