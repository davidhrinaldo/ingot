package ingot

import (
	"math"
	"testing"

	"git.dvdt.dev/david/ingot/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, it.Err())
		if len(samples) > 0 {
			h := labels.Hash(ls)
			result[h] = samples
		}
	}
	require.NoError(t, ss.Err())
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
	require.NoError(t, err)
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
				require.NoError(t, err)
				o.addSeries(ref, labels.FromStrings("__name__", "temp", "room", "office"))
				o.addSample(ref, 1000, 71.3)

				_, err = app.Append(ref, nil, 2000, 71.4)
				require.NoError(t, err)
				o.addSample(ref, 2000, 71.4)

				ref2, err := app.Append(0, labels.FromStrings("__name__", "humidity", "room", "office"), 1000, 55.0)
				require.NoError(t, err)
				o.addSeries(ref2, labels.FromStrings("__name__", "humidity", "room", "office"))
				o.addSample(ref2, 1000, 55.0)

				require.NoError(t, app.Commit())
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
				require.NoError(t, err)
				o.addSeries(ref, labels.FromStrings("__name__", "temp"))
				o.addSample(ref, 0, 0)
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					require.NoError(t, err)
					o.addSample(ref, int64(i*15000), float64(i))
				}
				require.NoError(t, app.Commit())

				// Flush to block.
				_, err = db.FlushOlderThan(math.MaxInt64)
				require.NoError(t, err)
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
				require.NoError(t, err)
				o.addSeries(ref, labels.FromStrings("__name__", "temp", "room", "office"))
				o.addSample(ref, 0, 0)

				// Write 250 samples (2 sealed chunks + 10 active).
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					require.NoError(t, err)
					o.addSample(ref, int64(i*15000), float64(i))
				}
				require.NoError(t, app.Commit())

				// Flush sealed chunks to block.
				_, err = db.FlushOlderThan(math.MaxInt64)
				require.NoError(t, err)

				// Append more samples to head (after flush).
				app = db.Appender()
				for i := 250; i < 260; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					require.NoError(t, err)
					o.addSample(ref, int64(i*15000), float64(i))
				}
				require.NoError(t, app.Commit())
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
					require.NoError(t, err)
					o.addSeries(ref, s.ls)
					o.addSample(ref, 1000, 1.0)

					_, err = app.Append(ref, nil, 2000, 2.0)
					require.NoError(t, err)
					o.addSample(ref, 2000, 2.0)
				}
				require.NoError(t, app.Commit())
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
				require.NoError(t, err)
				o.addSeries(ref, labels.FromStrings("__name__", "temp"))
				o.addSample(ref, 0, 0)
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					require.NoError(t, err)
					o.addSample(ref, int64(i*15000), float64(i))
				}
				require.NoError(t, app.Commit())

				// First flush.
				_, err = db.FlushOlderThan(math.MaxInt64)
				require.NoError(t, err)

				// More data -> second flush.
				app = db.Appender()
				for i := 250; i < 500; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					require.NoError(t, err)
					o.addSample(ref, int64(i*15000), float64(i))
				}
				require.NoError(t, app.Commit())
				_, err = db.FlushOlderThan(math.MaxInt64)
				require.NoError(t, err)

				// Some data in head.
				app = db.Appender()
				for i := 500; i < 510; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					require.NoError(t, err)
					o.addSample(ref, int64(i*15000), float64(i))
				}
				require.NoError(t, app.Commit())
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
				require.NoError(t, err)
				o.addSeries(ref, labels.FromStrings("__name__", "temp"))
				o.addSample(ref, 1000, 1.0)
				require.NoError(t, app.Commit())
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
					require.NoError(t, err)
					defer q.Close()

					ss := q.Select(qc.matchers...)
					got := collectSeriesSet(t, ss)
					want := oracleByLabelHash(o, qc.mint, qc.maxt, qc.matchers...)

					assert.Equal(t, len(want), len(got), "series count mismatch")
					for h, wantSamples := range want {
						gotSamples, ok := got[h]
						assert.True(t, ok, "missing series with hash %d", h)
						assert.Equal(t, wantSamples, gotSamples, "sample mismatch for hash %d", h)
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

func TestDBAppenderAPI(t *testing.T) {
	db := openTestDB(t)

	app := db.Appender()
	ref, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), 1000, 71.3)
	require.NoError(t, err)
	assert.NotZero(t, ref)

	_, err = app.Append(ref, nil, 2000, 71.4)
	require.NoError(t, err)

	require.NoError(t, app.Commit())

	// Query back.
	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	defer q.Close()

	ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "room", "office"))
	require.True(t, ss.Next())
	s := ss.At()
	assert.Equal(t, labels.FromStrings("__name__", "temp", "room", "office"), s.Labels())

	it := s.Iterator()
	require.True(t, it.Next())
	st, sv := it.At()
	assert.Equal(t, int64(1000), st)
	assert.Equal(t, 71.3, sv)

	require.True(t, it.Next())
	st, sv = it.At()
	assert.Equal(t, int64(2000), st)
	assert.Equal(t, 71.4, sv)

	assert.False(t, it.Next())
	assert.False(t, ss.Next())
}

func TestDBReopenWithBlocks(t *testing.T) {
	dir := t.TempDir()

	// Write data, flush, close.
	db, err := Open(dir, Options{})
	require.NoError(t, err)

	app := db.Appender()
	ref, err := app.Append(0, labels.FromStrings("__name__", "temp"), 0, 0)
	require.NoError(t, err)
	for i := 1; i < 250; i++ {
		_, err = app.Append(ref, nil, int64(i*15000), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	_, err = db.FlushOlderThan(math.MaxInt64)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopen.
	db2, err := Open(dir, Options{})
	require.NoError(t, err)
	defer db2.Close()

	// Should find data in block.
	q, err := db2.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	defer q.Close()

	ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", "temp"))
	require.True(t, ss.Next())
	it := ss.At().Iterator()
	count := 0
	for it.Next() {
		count++
	}
	require.NoError(t, it.Err())
	assert.Equal(t, 250, count, "should find all samples: 240 from block + 10 from head WAL replay")
}
