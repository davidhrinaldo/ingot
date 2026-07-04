package wal

import (
	"os"
	"path/filepath"
	"testing"

	"git.dvdt.dev/david/ingot/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectRecords replays all records from a WAL directory.
func collectRecords(t *testing.T, dir string) []Record {
	t.Helper()
	r, err := NewReader(dir)
	require.NoError(t, err)
	defer r.Close()

	var recs []Record
	for r.Next() {
		recs = append(recs, r.Record())
	}
	require.NoError(t, r.Err())
	return recs
}

func TestWAL(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(t *testing.T, dir string) // write to WAL, close it, optionally corrupt
		wantRecords int
		wantMinSegs int // assert segment count >= this
	}{
		{
			name: "write_and_replay",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				require.NoError(t, err)
				require.NoError(t, w.LogSeries([]SeriesRecord{
					{Ref: 1, Labels: []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}}},
				}))
				require.NoError(t, w.LogSamples([]RefSample{
					{Ref: 1, T: 1000, V: 71.3},
					{Ref: 1, T: 1015, V: 71.4},
				}))
				require.NoError(t, w.Close())
			},
			wantRecords: 2,
			wantMinSegs: 1,
		},
		{
			name: "empty",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				require.NoError(t, err)
				require.NoError(t, w.Close())
			},
			wantRecords: 0,
			wantMinSegs: 1,
		},
		{
			name: "reopen_and_append",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				require.NoError(t, err)
				require.NoError(t, w.LogSamples([]RefSample{{Ref: 1, T: 1000, V: 1.0}}))
				require.NoError(t, w.Close())

				w, err = Open(dir, Options{})
				require.NoError(t, err)
				require.NoError(t, w.LogSamples([]RefSample{{Ref: 2, T: 2000, V: 2.0}}))
				require.NoError(t, w.Close())
			},
			wantRecords: 2,
			wantMinSegs: 1,
		},
		{
			name: "segment_rotation",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{SegmentMaxSize: 50})
				require.NoError(t, err)
				for i := 0; i < 10; i++ {
					require.NoError(t, w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i * 1000), V: float64(i)}}))
				}
				require.NoError(t, w.Close())
			},
			wantRecords: 10,
			wantMinSegs: 2,
		},
		{
			name: "truncate_old_segments",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{SegmentMaxSize: 50})
				require.NoError(t, err)
				for i := 0; i < 10; i++ {
					require.NoError(t, w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i), V: float64(i)}}))
				}
				lastSeg := w.LastSegment()
				require.NoError(t, w.Truncate(lastSeg))
				require.NoError(t, w.Close())
			},
			wantRecords: 1,  // only the last segment's record(s) survive
			wantMinSegs: 1,
		},
		{
			name: "recovery_truncates_trailing_garbage",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				require.NoError(t, err)
				for i := 0; i < 3; i++ {
					require.NoError(t, w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i), V: float64(i)}}))
				}
				require.NoError(t, w.Close())

				// Append garbage after valid records.
				segs, err := listSegments(dir)
				require.NoError(t, err)
				f, err := os.OpenFile(segmentPath(dir, segs[0]), os.O_WRONLY|os.O_APPEND, 0644)
				require.NoError(t, err)
				_, err = f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
				require.NoError(t, err)
				require.NoError(t, f.Close())

				// Reopen triggers recovery.
				w, err = Open(dir, Options{})
				require.NoError(t, err)
				require.NoError(t, w.Close())
			},
			wantRecords: 3,
			wantMinSegs: 1,
		},
		{
			name: "recovery_truncates_corrupt_mid_record",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				require.NoError(t, err)
				for i := 0; i < 3; i++ {
					require.NoError(t, w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i), V: float64(i)}}))
				}
				require.NoError(t, w.Close())

				// Write a valid header but truncated payload (looks like a torn write).
				segs, err := listSegments(dir)
				require.NoError(t, err)
				f, err := os.OpenFile(segmentPath(dir, segs[0]), os.O_WRONLY|os.O_APPEND, 0644)
				require.NoError(t, err)
				// type=1, len=100 (big), but no payload follows.
				_, err = f.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x64})
				require.NoError(t, err)
				require.NoError(t, f.Close())

				w, err = Open(dir, Options{})
				require.NoError(t, err)
				require.NoError(t, w.Close())
			},
			wantRecords: 3,
			wantMinSegs: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			tc.setup(t, dir)

			recs := collectRecords(t, dir)
			assert.Equal(t, tc.wantRecords, len(recs), "record count")

			segs, err := listSegments(dir)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(segs), tc.wantMinSegs, "segment count")
		})
	}
}

// TestTornWriteRecovery is the headline test from DESIGN.md: for every possible
// byte offset, truncate the WAL there and verify recovery produces a valid
// prefix of the original record sequence.
func TestTornWriteRecovery(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		recs func(t *testing.T, w *WAL) // write records to the WAL
	}{
		{
			name: "single_segment_mixed_records",
			opts: Options{},
			recs: func(t *testing.T, w *WAL) {
				require.NoError(t, w.LogSeries([]SeriesRecord{
					{Ref: 1, Labels: []labels.Label{{Name: "__name__", Value: "temp"}}},
				}))
				require.NoError(t, w.LogSamples([]RefSample{{Ref: 1, T: 1000, V: 71.3}}))
				require.NoError(t, w.LogSeries([]SeriesRecord{
					{Ref: 2, Labels: []labels.Label{{Name: "__name__", Value: "humidity"}, {Name: "room", Value: "lab"}}},
				}))
				require.NoError(t, w.LogSamples([]RefSample{
					{Ref: 1, T: 1015, V: 71.4},
					{Ref: 2, T: 1000, V: 55.0},
				}))
			},
		},
		{
			name: "multi_segment",
			opts: Options{SegmentMaxSize: 50},
			recs: func(t *testing.T, w *WAL) {
				for i := 0; i < 10; i++ {
					require.NoError(t, w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i * 1000), V: float64(i)}}))
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Write the reference WAL.
			srcDir := filepath.Join(t.TempDir(), "src")
			w, err := Open(srcDir, tc.opts)
			require.NoError(t, err)
			tc.recs(t, w)
			require.NoError(t, w.Close())

			origRecs := collectRecords(t, srcDir)
			require.Greater(t, len(origRecs), 0)

			segs, err := listSegments(srcDir)
			require.NoError(t, err)

			// Read all segment data.
			segData := make(map[int][]byte)
			for _, idx := range segs {
				data, err := os.ReadFile(segmentPath(srcDir, idx))
				require.NoError(t, err)
				segData[idx] = data
			}

			// Truncate the last segment at every byte offset.
			lastSeg := segs[len(segs)-1]
			lastData := segData[lastSeg]

			for cutoff := 0; cutoff <= len(lastData); cutoff++ {
				walDir := filepath.Join(t.TempDir(), "wal")
				require.NoError(t, os.MkdirAll(walDir, 0755))

				// Copy earlier segments intact.
				for _, idx := range segs[:len(segs)-1] {
					require.NoError(t, os.WriteFile(segmentPath(walDir, idx), segData[idx], 0644))
				}
				// Write truncated last segment.
				require.NoError(t, os.WriteFile(segmentPath(walDir, lastSeg), lastData[:cutoff], 0644))

				w2, err := Open(walDir, tc.opts)
				require.NoError(t, err, "cutoff=%d", cutoff)
				recovered := collectRecords(t, walDir)
				require.NoError(t, w2.Close(), "cutoff=%d", cutoff)

				// Must be a valid prefix.
				assert.LessOrEqual(t, len(recovered), len(origRecs), "cutoff=%d count", cutoff)
				for i, rec := range recovered {
					assert.Equal(t, origRecs[i].Type, rec.Type, "cutoff=%d rec=%d type", cutoff, i)
					assert.Equal(t, origRecs[i].Data, rec.Data, "cutoff=%d rec=%d data", cutoff, i)
				}
			}
		})
	}
}
