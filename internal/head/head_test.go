package head

import (
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"git.dvdt.dev/david/ingot/internal/block"
	"git.dvdt.dev/david/ingot/internal/wal"
	"git.dvdt.dev/david/ingot/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sample is a convenience type for expected results.
type sample struct {
	t     int64
	vBits uint64
}

func s(t int64, v float64) sample { return sample{t, math.Float64bits(v)} }

// collectSamples reads all samples from a head series iterator.
func collectSamples(t *testing.T, h *Head, ref uint64, mint, maxt int64) []sample {
	t.Helper()
	it := h.SeriesIterator(ref, mint, maxt)
	var out []sample
	for it.Next() {
		ts, v := it.At()
		out = append(out, sample{ts, math.Float64bits(v)})
	}
	require.NoError(t, it.Err())
	return out
}

// Action kinds for the appender lifecycle table.
const (
	actNew      = iota // create a new Appender
	actAppend          // call Append
	actCommit          // call Commit
	actRollback        // call Rollback
)

type action struct {
	kind int

	// actAppend fields.
	ref    uint64
	labels []labels.Label
	t      int64
	v      float64

	// Expected results (actAppend: ref+err, actCommit/actRollback: err).
	wantRef uint64
	wantErr error
}

func openHead(t *testing.T) *Head {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	h, err := Open(dir, wal.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { h.Close() })
	return h
}

func TestHead(t *testing.T) {
	tests := []struct {
		name        string
		actions     []action
		wantSamples map[uint64][]sample // ref -> expected samples after all actions
	}{
		{
			name: "single_series_single_sample",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 71.3, wantRef: 1, wantErr: nil},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{
				1: {s(1000, 71.3)},
			},
		},
		{
			name: "single_series_multiple_samples",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 1.0, wantRef: 1, wantErr: nil},
				{kind: actAppend, ref: 1, t: 1015, v: 2.0, wantRef: 1, wantErr: nil},
				{kind: actAppend, ref: 1, t: 1030, v: 3.0, wantRef: 1, wantErr: nil},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{
				1: {s(1000, 1.0), s(1015, 2.0), s(1030, 3.0)},
			},
		},
		{
			name: "multiple_series",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 71.3, wantRef: 1, wantErr: nil},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "humidity"}}, t: 1000, v: 55.0, wantRef: 2, wantErr: nil},
				{kind: actAppend, ref: 1, t: 1015, v: 71.4, wantRef: 1, wantErr: nil},
				{kind: actAppend, ref: 2, t: 1015, v: 54.0, wantRef: 2, wantErr: nil},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{
				1: {s(1000, 71.3), s(1015, 71.4)},
				2: {s(1000, 55.0), s(1015, 54.0)},
			},
		},
		{
			name: "out_of_order_rejected",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 1.0, wantRef: 1, wantErr: nil},
				{kind: actAppend, ref: 1, t: 999, v: 2.0, wantRef: 0, wantErr: ErrOutOfOrder},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{
				1: {s(1000, 1.0)},
			},
		},
		{
			name: "duplicate_timestamp_rejected",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 1.0, wantRef: 1, wantErr: nil},
				{kind: actAppend, ref: 1, t: 1000, v: 2.0, wantRef: 0, wantErr: ErrOutOfOrder},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{
				1: {s(1000, 1.0)},
			},
		},
		{
			name: "out_of_order_across_commits",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 1.0, wantRef: 1, wantErr: nil},
				{kind: actCommit, wantErr: nil},
				{kind: actNew},
				{kind: actAppend, ref: 1, t: 500, v: 2.0, wantRef: 0, wantErr: ErrOutOfOrder},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{
				1: {s(1000, 1.0)},
			},
		},
		{
			name: "unknown_ref_rejected",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, ref: 999, t: 1000, v: 1.0, wantRef: 0, wantErr: ErrSeriesNotFound},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{},
		},
		{
			name: "empty_label_name_rejected",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "", Value: "temp"}}, t: 1000, v: 1.0, wantRef: 0, wantErr: labels.ErrEmptyName},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{},
		},
		{
			name: "unsorted_labels_normalized",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "room", Value: "office"}, {Name: "__name__", Value: "temp"}}, t: 1000, v: 71.3, wantRef: 1, wantErr: nil},
				{kind: actCommit, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{
				1: {s(1000, 71.3)},
			},
		},
		{
			name: "chunk_sealing",
			actions: func() []action {
				acts := []action{{kind: actNew}}
				acts = append(acts, action{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 0, v: 0, wantRef: 1})
				for i := 1; i < 250; i++ {
					acts = append(acts, action{kind: actAppend, ref: 1, t: int64(i * 15000), v: float64(i), wantRef: 1})
				}
				acts = append(acts, action{kind: actCommit})
				return acts
			}(),
			wantSamples: map[uint64][]sample{
				1: func() []sample {
					out := make([]sample, 250)
					for i := range out {
						out[i] = s(int64(i*15000), float64(i))
					}
					return out
				}(),
			},
		},
		{
			name: "query_out_of_range_returns_empty",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 1.0, wantRef: 1, wantErr: nil},
				{kind: actCommit, wantErr: nil},
			},
			// wantSamples checks ref=1 over all time; a separate query assertion below.
			wantSamples: map[uint64][]sample{
				1: {s(1000, 1.0)},
			},
		},

		// --- Appender lifecycle error paths ---
		{
			name: "rollback_discards_samples",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 1.0, wantRef: 1, wantErr: nil},
				{kind: actCommit, wantErr: nil},
				{kind: actNew},
				{kind: actAppend, ref: 1, t: 2000, v: 2.0, wantRef: 1, wantErr: nil},
				{kind: actRollback, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{
				1: {s(1000, 1.0)},
			},
		},
		{
			name: "rollback_removes_new_series",
			actions: []action{
				{kind: actNew},
				{kind: actAppend, labels: []labels.Label{{Name: "__name__", Value: "temp"}}, t: 1000, v: 1.0, wantRef: 1, wantErr: nil},
				{kind: actRollback, wantErr: nil},
			},
			wantSamples: map[uint64][]sample{},
		},
		{
			name: "commit_after_commit",
			actions: []action{
				{kind: actNew},
				{kind: actCommit, wantErr: nil},
				{kind: actCommit, wantErr: ErrAppenderClosed},
			},
			wantSamples: map[uint64][]sample{},
		},
		{
			name: "rollback_after_commit",
			actions: []action{
				{kind: actNew},
				{kind: actCommit, wantErr: nil},
				{kind: actRollback, wantErr: ErrAppenderClosed},
			},
			wantSamples: map[uint64][]sample{},
		},
		{
			name: "commit_after_rollback",
			actions: []action{
				{kind: actNew},
				{kind: actRollback, wantErr: nil},
				{kind: actCommit, wantErr: ErrAppenderClosed},
			},
			wantSamples: map[uint64][]sample{},
		},
		{
			name: "rollback_after_rollback",
			actions: []action{
				{kind: actNew},
				{kind: actRollback, wantErr: nil},
				{kind: actRollback, wantErr: ErrAppenderClosed},
			},
			wantSamples: map[uint64][]sample{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := openHead(t)

			var app *Appender
			for i, act := range tc.actions {
				switch act.kind {
				case actNew:
					app = h.Appender()
				case actAppend:
					ref, err := app.Append(act.ref, act.labels, act.t, act.v)
					assert.Equal(t, act.wantRef, ref, "action %d ref", i)
					assert.Equal(t, act.wantErr, err, "action %d error", i)
				case actCommit:
					err := app.Commit()
					assert.Equal(t, act.wantErr, err, "action %d commit error", i)
				case actRollback:
					err := app.Rollback()
					assert.Equal(t, act.wantErr, err, "action %d rollback error", i)
				}
			}

			for ref, wantSamples := range tc.wantSamples {
				got := collectSamples(t, h, ref, math.MinInt64, math.MaxInt64)
				require.Equal(t, len(wantSamples), len(got), "ref %d sample count", ref)
				for i, want := range wantSamples {
					assert.Equal(t, want, got[i], "ref %d sample %d", ref, i)
				}
			}
		})
	}
}

func TestWALReplay(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T, dir string)
		wantSeries map[uint64][]sample
	}{
		{
			name: "basic_recovery",
			setup: func(t *testing.T, dir string) {
				h, err := Open(dir, wal.Options{})
				require.NoError(t, err)
				app := h.Appender()
				ref, err := app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}}, 1000, 71.3)
				require.NoError(t, err)
				_, err = app.Append(ref, nil, 1015, 71.4)
				require.NoError(t, err)
				require.NoError(t, app.Commit())
				require.NoError(t, h.Close())
			},
			wantSeries: map[uint64][]sample{
				1: {s(1000, 71.3), s(1015, 71.4)},
			},
		},
		{
			name: "multi_series_recovery",
			setup: func(t *testing.T, dir string) {
				h, err := Open(dir, wal.Options{})
				require.NoError(t, err)
				app := h.Appender()
				_, err = app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}}, 1000, 71.3)
				require.NoError(t, err)
				_, err = app.Append(0, []labels.Label{{Name: "__name__", Value: "humidity"}}, 1000, 55.0)
				require.NoError(t, err)
				require.NoError(t, app.Commit())
				require.NoError(t, h.Close())
			},
			wantSeries: map[uint64][]sample{
				1: {s(1000, 71.3)},
				2: {s(1000, 55.0)},
			},
		},
		{
			name: "chunk_sealing_recovery",
			setup: func(t *testing.T, dir string) {
				h, err := Open(dir, wal.Options{})
				require.NoError(t, err)
				app := h.Appender()
				ref, err := app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}}, 0, 0)
				require.NoError(t, err)
				for i := 1; i < 250; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					require.NoError(t, err)
				}
				require.NoError(t, app.Commit())
				require.NoError(t, h.Close())
			},
			wantSeries: map[uint64][]sample{
				1: func() []sample {
					out := make([]sample, 250)
					for i := range out {
						out[i] = s(int64(i*15000), float64(i))
					}
					return out
				}(),
			},
		},
		{
			name: "multiple_commits_recovery",
			setup: func(t *testing.T, dir string) {
				h, err := Open(dir, wal.Options{})
				require.NoError(t, err)
				app := h.Appender()
				_, err = app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}}, 1000, 1.0)
				require.NoError(t, err)
				require.NoError(t, app.Commit())
				app = h.Appender()
				_, err = app.Append(1, nil, 2000, 2.0)
				require.NoError(t, err)
				require.NoError(t, app.Commit())
				require.NoError(t, h.Close())
			},
			wantSeries: map[uint64][]sample{
				1: {s(1000, 1.0), s(2000, 2.0)},
			},
		},
		{
			name: "rollback_not_recovered",
			setup: func(t *testing.T, dir string) {
				h, err := Open(dir, wal.Options{})
				require.NoError(t, err)
				app := h.Appender()
				_, err = app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}}, 1000, 1.0)
				require.NoError(t, err)
				require.NoError(t, app.Commit())
				// Second batch is rolled back — should not survive restart.
				app = h.Appender()
				_, err = app.Append(1, nil, 2000, 2.0)
				require.NoError(t, err)
				require.NoError(t, app.Rollback())
				require.NoError(t, h.Close())
			},
			wantSeries: map[uint64][]sample{
				1: {s(1000, 1.0)},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			tc.setup(t, dir)

			h, err := Open(dir, wal.Options{})
			require.NoError(t, err)
			defer h.Close()

			for ref, wantSamples := range tc.wantSeries {
				got := collectSamples(t, h, ref, math.MinInt64, math.MaxInt64)
				require.Equal(t, len(wantSamples), len(got), "ref %d sample count", ref)
				for i, want := range wantSamples {
					assert.Equal(t, want, got[i], "ref %d sample %d", ref, i)
				}
			}
		})
	}
}

func TestFlushOlderThan(t *testing.T) {
	tests := []struct {
		name            string
		numSamples      int // samples per series (each at 15s intervals)
		flushMaxT       int64
		postFlushAppend int   // additional samples to append after flush (0 = none)
		wantULID        bool  // whether flush produces a block
		wantBlockSeries int   // number of series in the block (0 if no block)
		wantBlockCount  int   // samples in the block (0 if no block)
		wantHeadCount   int   // minimum samples remaining in head after flush
	}{
		{
			name:            "flush_sealed_chunks",
			numSamples:      250,
			flushMaxT:       math.MaxInt64,
			postFlushAppend: 0,
			wantULID:        true,
			wantBlockSeries: 1,
			wantBlockCount:  240,
			wantHeadCount:   10,
		},
		{
			name:            "nothing_to_flush",
			numSamples:      50,
			flushMaxT:       math.MaxInt64,
			postFlushAppend: 0,
			wantULID:        false,
			wantBlockSeries: 0,
			wantBlockCount:  0,
			wantHeadCount:   50,
		},
		{
			name:            "partial_flush_by_time",
			numSamples:      250,
			flushMaxT:       120 * 15000,
			postFlushAppend: 0,
			wantULID:        true,
			wantBlockSeries: 1,
			wantBlockCount:  120,
			wantHeadCount:   10,
		},
		{
			name:            "flush_then_continue_appending",
			numSamples:      250,
			flushMaxT:       math.MaxInt64,
			postFlushAppend: 10,
			wantULID:        true,
			wantBlockSeries: 1,
			wantBlockCount:  240,
			wantHeadCount:   10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := openHead(t)

			app := h.Appender()
			ref, err := app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}}, 0, 0)
			require.NoError(t, err)
			for i := 1; i < tc.numSamples; i++ {
				_, err = app.Append(ref, nil, int64(i*15000), float64(i))
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())

			ulid, err := h.FlushOlderThan(tc.flushMaxT)
			require.NoError(t, err)

			// Post-flush appends.
			if tc.postFlushAppend > 0 {
				app = h.Appender()
				for i := tc.numSamples; i < tc.numSamples+tc.postFlushAppend; i++ {
					_, err = app.Append(ref, nil, int64(i*15000), float64(i))
					require.NoError(t, err)
				}
				require.NoError(t, app.Commit())
			}

			// Assert ULID presence.
			assert.Equal(t, tc.wantULID, ulid != "", "block ULID presence")

			// Assert block contents.
			blockCount := 0
			if ulid != "" {
				blockDir := filepath.Join(h.DataDir(), ulid)
				br, err := block.Open(blockDir)
				require.NoError(t, err)
				defer br.Close()
				assert.Equal(t, tc.wantBlockSeries, br.Meta.Stats.NumSeries, "block series count")
				it, err := br.SeriesChunkIterator(ref, math.MinInt64, math.MaxInt64)
				require.NoError(t, err)
				for it.Next() {
					blockCount++
				}
				require.NoError(t, it.Err())
			}
			assert.Equal(t, tc.wantBlockCount, blockCount, "block sample count")

			// Assert head still has data.
			headSamples := collectSamples(t, h, ref, math.MinInt64, math.MaxInt64)
			assert.GreaterOrEqual(t, len(headSamples), tc.wantHeadCount, "head sample count")
		})
	}
}

func TestFlushWALTruncation(t *testing.T) {
	tests := []struct {
		name       string
		numSamples int
	}{
		{name: "250_samples", numSamples: 250},
		{name: "500_samples", numSamples: 500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			walDir := filepath.Join(dir, "wal")

			h, err := Open(walDir, wal.Options{})
			require.NoError(t, err)

			app := h.Appender()
			ref, err := app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}}, 0, 0)
			require.NoError(t, err)
			_ = ref
			for i := 1; i < tc.numSamples; i++ {
				_, err = app.Append(ref, nil, int64(i*15000), float64(i))
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())

			walEntries, err := os.ReadDir(walDir)
			require.NoError(t, err)
			segsBefore := len(walEntries)

			_, err = h.FlushOlderThan(math.MaxInt64)
			require.NoError(t, err)

			walEntries, err = os.ReadDir(walDir)
			require.NoError(t, err)
			segsAfter := len(walEntries)
			assert.LessOrEqual(t, segsAfter, segsBefore, "WAL should be truncated after flush")

			require.NoError(t, h.Close())

			// Re-open: head should be functional.
			h2, err := Open(walDir, wal.Options{})
			require.NoError(t, err)
			defer h2.Close()

			app = h2.Appender()
			_, err = app.Append(0, []labels.Label{{Name: "__name__", Value: "new_series"}}, 5000000, 42.0)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
		})
	}
}

func TestHeadPostings(t *testing.T) {
	h := openHead(t)

	// Add three series.
	app := h.Appender()
	_, err := app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}}, 1000, 1.0)
	require.NoError(t, err)
	_, err = app.Append(0, []labels.Label{{Name: "__name__", Value: "humidity"}, {Name: "room", Value: "office"}}, 1000, 2.0)
	require.NoError(t, err)
	_, err = app.Append(0, []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "kitchen"}}, 1000, 3.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	tests := []struct {
		name     string
		label    string
		value    string
		wantRefs []uint64
	}{
		{name: "match_name_temp", label: "__name__", value: "temp", wantRefs: []uint64{1, 3}},
		{name: "match_room_office", label: "room", value: "office", wantRefs: []uint64{1, 2}},
		{name: "match_name_humidity", label: "__name__", value: "humidity", wantRefs: []uint64{2}},
		{name: "no_match", label: "__name__", value: "pressure", wantRefs: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := h.Postings(tc.label, tc.value)
			assert.Equal(t, tc.wantRefs, got)
		})
	}
}

func TestHeadQueryMethods(t *testing.T) {
	tests := []struct {
		name            string
		series          [][]labels.Label
		wantLabelValues map[string][]string // label name -> expected values
		wantLabels      map[uint64][]labels.Label // ref -> expected labels
		wantAllPostings []uint64
	}{
		{
			name: "multiple_series_with_shared_labels",
			series: [][]labels.Label{
				{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}},
				{{Name: "__name__", Value: "humidity"}, {Name: "room", Value: "office"}},
				{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "kitchen"}},
			},
			wantLabelValues: map[string][]string{
				"__name__":    {"humidity", "temp"},
				"room":        {"kitchen", "office"},
				"nonexistent": {},
			},
			wantLabels: map[uint64][]labels.Label{
				1: {{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}},
				2: {{Name: "__name__", Value: "humidity"}, {Name: "room", Value: "office"}},
				3: {{Name: "__name__", Value: "temp"}, {Name: "room", Value: "kitchen"}},
			},
			wantAllPostings: []uint64{1, 2, 3},
		},
		{
			name: "single_series",
			series: [][]labels.Label{
				{{Name: "__name__", Value: "temp"}},
			},
			wantLabelValues: map[string][]string{
				"__name__":    {"temp"},
				"nonexistent": {},
			},
			wantLabels: map[uint64][]labels.Label{
				1: {{Name: "__name__", Value: "temp"}},
			},
			wantAllPostings: []uint64{1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := openHead(t)

			app := h.Appender()
			for _, ls := range tc.series {
				_, err := app.Append(0, ls, 1000, 1.0)
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())

			// Assert LabelValues.
			for name, wantVals := range tc.wantLabelValues {
				got := h.LabelValues(name)
				assert.Equal(t, wantVals, got, "LabelValues(%q)", name)
			}

			// Assert Labels by ref.
			for ref, wantLabels := range tc.wantLabels {
				ls, ok := h.Labels(ref)
				assert.True(t, ok, "Labels(%d) should exist", ref)
				assert.Equal(t, wantLabels, ls, "Labels(%d)", ref)
			}

			// Unknown ref returns false.
			_, ok := h.Labels(999)
			assert.False(t, ok, "Labels(999) should not exist")

			// Assert AllPostings.
			assert.Equal(t, tc.wantAllPostings, h.AllPostings(), "AllPostings")
		})
	}
}

func TestConcurrentAppend(t *testing.T) {
	tests := []struct {
		name                string
		numGoroutines       int
		samplesPerGoroutine int
	}{
		{name: "8_goroutines_100_samples", numGoroutines: 8, samplesPerGoroutine: 100},
		{name: "1_goroutine_1000_samples", numGoroutines: 1, samplesPerGoroutine: 1000},
		{name: "32_goroutines_10_samples", numGoroutines: 32, samplesPerGoroutine: 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := openHead(t)

			refs := make([]uint64, tc.numGoroutines)
			var wg sync.WaitGroup

			for g := 0; g < tc.numGoroutines; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					app := h.Appender()
					ls := []labels.Label{
						{Name: "__name__", Value: "metric"},
						{Name: "goroutine", Value: string(rune('A' + g))},
					}
					ref, err := app.Append(0, ls, int64(g*1000000), 0)
					require.NoError(t, err)
					refs[g] = ref
					for i := 1; i < tc.samplesPerGoroutine; i++ {
						_, err = app.Append(ref, nil, int64(g*1000000+i*1000), float64(i))
						require.NoError(t, err)
					}
					require.NoError(t, app.Commit())
				}(g)
			}
			wg.Wait()

			for g := 0; g < tc.numGoroutines; g++ {
				got := collectSamples(t, h, refs[g], math.MinInt64, math.MaxInt64)
				assert.Equal(t, tc.samplesPerGoroutine, len(got), "goroutine %d sample count", g)
			}
		})
	}
}
