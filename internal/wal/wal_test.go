package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"git.dvdt.dev/david/ingot/labels"
)

// collectRecords replays all records from a WAL directory.
func collectRecords(t *testing.T, dir string) []Record {
	t.Helper()
	r, err := NewReader(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer r.Close()

	var recs []Record
	for r.Next() {
		recs = append(recs, r.Record())
	}
	if r.Err() != nil {
		t.Fatalf("unexpected error: %v", r.Err())
	}
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
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.LogSeries([]SeriesRecord{
					{Ref: 1, Labels: []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}}},
				}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.LogSamples([]RefSample{
					{Ref: 1, T: 1000, V: 71.3},
					{Ref: 1, T: 1015, V: 71.4},
				}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			wantRecords: 2,
			wantMinSegs: 1,
		},
		{
			name: "empty",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			wantRecords: 0,
			wantMinSegs: 1,
		},
		{
			name: "reopen_and_append",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.LogSamples([]RefSample{{Ref: 1, T: 1000, V: 1.0}}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				w, err = Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.LogSamples([]RefSample{{Ref: 2, T: 2000, V: 2.0}}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			wantRecords: 2,
			wantMinSegs: 1,
		},
		{
			name: "segment_rotation",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{SegmentMaxSize: 50})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				for i := 0; i < 10; i++ {
					if err := w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i * 1000), V: float64(i)}}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			wantRecords: 10,
			wantMinSegs: 2,
		},
		{
			name: "truncate_old_segments",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{SegmentMaxSize: 50})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				for i := 0; i < 10; i++ {
					if err := w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i), V: float64(i)}}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				}
				lastSeg := w.LastSegment()
				if err := w.Truncate(lastSeg); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			wantRecords: 1,  // only the last segment's record(s) survive
			wantMinSegs: 1,
		},
		{
			name: "recovery_truncates_trailing_garbage",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				for i := 0; i < 3; i++ {
					if err := w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i), V: float64(i)}}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// Append garbage after valid records.
				segs, err := listSegments(dir)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				f, err := os.OpenFile(segmentPath(dir, segs[0]), os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := f.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// Reopen triggers recovery.
				w, err = Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			wantRecords: 3,
			wantMinSegs: 1,
		},
		{
			name: "recovery_truncates_corrupt_mid_record",
			setup: func(t *testing.T, dir string) {
				w, err := Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				for i := 0; i < 3; i++ {
					if err := w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i), V: float64(i)}}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				// Write a valid header but truncated payload (looks like a torn write).
				segs, err := listSegments(dir)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				f, err := os.OpenFile(segmentPath(dir, segs[0]), os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				// type=1, len=100 (big), but no payload follows.
				if _, err := f.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x64}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := f.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				w, err = Open(dir, Options{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
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
			if len(recs) != tc.wantRecords {
				t.Errorf("record count: got %v, want %v", len(recs), tc.wantRecords)
			}

			segs, err := listSegments(dir)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !(len(segs) >= tc.wantMinSegs) {
				t.Errorf("segment count: got %v, want >= %v", len(segs), tc.wantMinSegs)
			}
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
				if err := w.LogSeries([]SeriesRecord{
					{Ref: 1, Labels: []labels.Label{{Name: "__name__", Value: "temp"}}},
				}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.LogSamples([]RefSample{{Ref: 1, T: 1000, V: 71.3}}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.LogSeries([]SeriesRecord{
					{Ref: 2, Labels: []labels.Label{{Name: "__name__", Value: "humidity"}, {Name: "room", Value: "lab"}}},
				}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if err := w.LogSamples([]RefSample{
					{Ref: 1, T: 1015, V: 71.4},
					{Ref: 2, T: 1000, V: 55.0},
				}); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
		},
		{
			name: "multi_segment",
			opts: Options{SegmentMaxSize: 50},
			recs: func(t *testing.T, w *WAL) {
				for i := 0; i < 10; i++ {
					if err := w.LogSamples([]RefSample{{Ref: uint64(i), T: int64(i * 1000), V: float64(i)}}); err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Write the reference WAL.
			srcDir := filepath.Join(t.TempDir(), "src")
			w, err := Open(srcDir, tc.opts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tc.recs(t, w)
			if err := w.Close(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			origRecs := collectRecords(t, srcDir)
			if !(len(origRecs) > 0) {
				t.Fatalf("got %v records, want > 0", len(origRecs))
			}

			segs, err := listSegments(srcDir)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Read all segment data.
			segData := make(map[int][]byte)
			for _, idx := range segs {
				data, err := os.ReadFile(segmentPath(srcDir, idx))
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				segData[idx] = data
			}

			// Truncate the last segment at every byte offset.
			lastSeg := segs[len(segs)-1]
			lastData := segData[lastSeg]

			for cutoff := 0; cutoff <= len(lastData); cutoff++ {
				walDir := filepath.Join(t.TempDir(), "wal")
				if err := os.MkdirAll(walDir, 0755); err != nil {
					t.Fatalf("cutoff=%d: unexpected error: %v", cutoff, err)
				}

				// Copy earlier segments intact.
				for _, idx := range segs[:len(segs)-1] {
					if err := os.WriteFile(segmentPath(walDir, idx), segData[idx], 0644); err != nil {
						t.Fatalf("cutoff=%d: unexpected error: %v", cutoff, err)
					}
				}
				// Write truncated last segment.
				if err := os.WriteFile(segmentPath(walDir, lastSeg), lastData[:cutoff], 0644); err != nil {
					t.Fatalf("cutoff=%d: unexpected error: %v", cutoff, err)
				}

				w2, err := Open(walDir, tc.opts)
				if err != nil {
					t.Fatalf("cutoff=%d: unexpected error: %v", cutoff, err)
				}
				recovered := collectRecords(t, walDir)
				if err := w2.Close(); err != nil {
					t.Fatalf("cutoff=%d: unexpected error: %v", cutoff, err)
				}

				// Must be a valid prefix.
				if !(len(recovered) <= len(origRecs)) {
					t.Errorf("cutoff=%d: got %d records, want <= %d", cutoff, len(recovered), len(origRecs))
				}
				for i, rec := range recovered {
					if origRecs[i].Type != rec.Type {
						t.Errorf("cutoff=%d rec=%d type: got %v, want %v", cutoff, i, rec.Type, origRecs[i].Type)
					}
					if !reflect.DeepEqual(origRecs[i].Data, rec.Data) {
						t.Errorf("cutoff=%d rec=%d data: got %v, want %v", cutoff, i, rec.Data, origRecs[i].Data)
					}
				}
			}
		})
	}
}
