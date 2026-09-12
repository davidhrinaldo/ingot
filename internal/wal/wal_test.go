package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/davidhrinaldo/ingot/labels"
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

func TestLogSeriesRejectsOversizedBatchBeforeWriting(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := Open(dir, Options{SyncInterval: -1})
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	recs := []SeriesRecord{
		{Ref: 1, Labels: labels.FromStrings("__name__", "valid")},
		{Ref: 2, Labels: labels.FromStrings("__name__", strings.Repeat("v", 1<<16))},
	}
	if err := w.LogSeries(recs); !errors.Is(err, labels.ErrLabelTooLong) {
		t.Fatalf("log series error: got %v, want %v", err, labels.ErrLabelTooLong)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
	if got := collectRecords(t, dir); len(got) != 0 {
		t.Fatalf("wrote %d records before rejecting oversized label", len(got))
	}
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
			wantRecords: 1, // only the last segment's record(s) survive
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

// TestTornWriteRecovery appends every possible partial prefix of a new final
// record. Recovery must remove only that unacknowledged tail and preserve every
// complete record that preceded it.
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
			if err := w.Commit(); err != nil {
				t.Fatalf("commit reference WAL: %v", err)
			}
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

			// Append a partial record at every incomplete byte offset.
			lastSeg := segs[len(segs)-1]
			lastData := segData[lastSeg]
			tail := EncodeRecord(nil, RecordSamples, EncodeSamplesRecord(nil, []RefSample{{Ref: 999, T: 999, V: 999}}))

			for cutoff := 0; cutoff < len(tail); cutoff++ {
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
				// Keep every complete record and append only the torn tail.
				data := append([]byte(nil), lastData...)
				data = append(data, tail[:cutoff]...)
				if err := os.WriteFile(segmentPath(walDir, lastSeg), data, 0644); err != nil {
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

				if len(recovered) != len(origRecs) {
					t.Errorf("cutoff=%d: got %d records, want %d", cutoff, len(recovered), len(origRecs))
				}
				for i, rec := range recovered {
					if origRecs[i].Type != rec.Type {
						t.Errorf("cutoff=%d rec=%d type: got %v, want %v", cutoff, i, rec.Type, origRecs[i].Type)
					}
					if !reflect.DeepEqual(origRecs[i].Data, rec.Data) {
						t.Errorf("cutoff=%d rec=%d data: got %v, want %v", cutoff, i, rec.Data, origRecs[i].Data)
					}
				}
				repaired, err := os.ReadFile(segmentPath(walDir, lastSeg))
				if err != nil {
					t.Fatalf("cutoff=%d: read repaired tail: %v", cutoff, err)
				}
				if !reflect.DeepEqual(repaired, lastData) {
					t.Fatalf("cutoff=%d: repaired segment differs from acknowledged baseline", cutoff)
				}
			}
		})
	}
}

// TestExternallyTruncatedDurableFinalRecordLimit documents the ambiguity that
// remains without a persisted durable-offset watermark. An externally
// truncated durable final frame is indistinguishable from an interrupted append.
func TestExternallyTruncatedDurableFinalRecordLimit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := w.LogSamples([]RefSample{{Ref: 1, T: int64(i), V: float64(i)}}); err != nil {
			t.Fatalf("log sample %d: %v", i, err)
		}
		if err := w.Commit(); err != nil {
			t.Fatalf("commit sample %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	path := segmentPath(dir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}
	if err := os.WriteFile(path, data[:len(data)-1], 0644); err != nil {
		t.Fatalf("truncate durable final record: %v", err)
	}

	w, err = Open(dir, Options{})
	if err != nil {
		t.Fatalf("recover externally truncated final record: %v", err)
	}
	recovered := collectRecords(t, dir)
	if len(recovered) != 1 {
		t.Fatalf("recovered records: got %d, want 1", len(recovered))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close recovered WAL: %v", err)
	}
}

func TestFinalSegmentCorruptionLeavesWALIntact(t *testing.T) {
	tests := []struct {
		name        string
		recordIndex int
		corrupt     func([]byte, int, int)
		wantErr     error
	}{
		{
			name:        "checksum_before_later_record",
			recordIndex: 1,
			corrupt:     func(data []byte, off, size int) { data[off+size-1] ^= 0xff },
			wantErr:     ErrCorruptRecord,
		},
		{
			name:        "checksum_at_tail",
			recordIndex: 2,
			corrupt:     func(data []byte, off, size int) { data[off+size-1] ^= 0xff },
			wantErr:     ErrCorruptRecord,
		},
		{
			name:        "invalid_length_before_later_record",
			recordIndex: 1,
			corrupt: func(data []byte, off, _ int) {
				copy(data[off+1:off+recordHeaderSize], []byte{0xff, 0xff, 0xff, 0xff})
			},
			wantErr: ErrInvalidRecord,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			w, err := Open(dir, Options{})
			if err != nil {
				t.Fatalf("open WAL: %v", err)
			}
			for i := 0; i < 3; i++ {
				if err := w.LogSamples([]RefSample{{Ref: 1, T: int64(i), V: float64(i)}}); err != nil {
					t.Fatalf("log sample %d: %v", i, err)
				}
			}
			if err := w.Commit(); err != nil {
				t.Fatalf("commit WAL: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close WAL: %v", err)
			}

			path := segmentPath(dir, 1)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read WAL: %v", err)
			}
			off := 0
			for i := 0; i <= tc.recordIndex; i++ {
				_, _, consumed, err := DecodeRecord(data[off:])
				if err != nil {
					t.Fatalf("decode record %d: %v", i, err)
				}
				if i == tc.recordIndex {
					tc.corrupt(data, off, consumed)
					break
				}
				off += consumed
			}
			if err := os.WriteFile(path, data, 0644); err != nil {
				t.Fatalf("corrupt final segment: %v", err)
			}

			before := append([]byte(nil), data...)
			if _, err := Open(dir, Options{}); !errors.Is(err, tc.wantErr) {
				t.Fatalf("reopen error: got %v, want %v", err, tc.wantErr)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read final segment after failed recovery: %v", err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatal("final segment changed during failed recovery")
			}
		})
	}
}

func TestClosedSegmentCorruptionLeavesWALIntact(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func([]byte) []byte
		wantErr error
	}{
		{
			name: "checksum",
			corrupt: func(data []byte) []byte {
				data[len(data)-1] ^= 0xff
				return data
			},
			wantErr: ErrCorruptRecord,
		},
		{
			name: "truncated",
			corrupt: func(data []byte) []byte {
				return data[:len(data)-1]
			},
			wantErr: ErrInvalidRecord,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			opts := Options{SegmentMaxSize: 50}
			w, err := Open(dir, opts)
			if err != nil {
				t.Fatalf("open WAL: %v", err)
			}
			for i := 0; i < 4; i++ {
				if err := w.LogSamples([]RefSample{{Ref: 1, T: int64(i), V: float64(i)}}); err != nil {
					t.Fatalf("log sample %d: %v", i, err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close WAL: %v", err)
			}

			segs, err := listSegments(dir)
			if err != nil {
				t.Fatalf("list segments: %v", err)
			}
			if len(segs) < 3 {
				t.Fatalf("segment count: got %d, want at least 3", len(segs))
			}
			closed := segs[0]
			path := segmentPath(dir, closed)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read closed segment: %v", err)
			}
			if err := os.WriteFile(path, tc.corrupt(data), 0644); err != nil {
				t.Fatalf("corrupt closed segment: %v", err)
			}

			before := make(map[int][]byte, len(segs))
			for _, idx := range segs {
				before[idx], err = os.ReadFile(segmentPath(dir, idx))
				if err != nil {
					t.Fatalf("snapshot segment %d: %v", idx, err)
				}
			}

			if _, err := Open(dir, opts); !errors.Is(err, tc.wantErr) {
				t.Fatalf("reopen error: got %v, want %v", err, tc.wantErr)
			}
			afterSegs, err := listSegments(dir)
			if err != nil {
				t.Fatalf("list segments after failed recovery: %v", err)
			}
			if !reflect.DeepEqual(afterSegs, segs) {
				t.Fatalf("segments changed: got %v, want %v", afterSegs, segs)
			}
			for _, idx := range segs {
				after, err := os.ReadFile(segmentPath(dir, idx))
				if err != nil {
					t.Fatalf("read segment %d after failed recovery: %v", idx, err)
				}
				if !reflect.DeepEqual(after, before[idx]) {
					t.Fatalf("segment %d changed during failed recovery", idx)
				}
			}
		})
	}
}

func TestCommitSyncPolicy(t *testing.T) {
	tests := []struct {
		name            string
		opts            Options
		wantCommitSyncs int
	}{
		{name: "default_syncs_on_commit", opts: Options{}, wantCommitSyncs: 1},
		{
			name:            "periodic_commit_does_not_sync",
			opts:            Options{SyncPolicy: SyncPeriodic, SyncInterval: time.Hour},
			wantCommitSyncs: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var syncs int
			tc.opts.sync = func(*os.File) error {
				syncs++
				return nil
			}
			w, err := Open(filepath.Join(t.TempDir(), "wal"), tc.opts)
			if err != nil {
				t.Fatalf("open WAL: %v", err)
			}
			if err := w.LogSamples([]RefSample{{Ref: 1, T: 1, V: 1}}); err != nil {
				t.Fatalf("log sample: %v", err)
			}
			if err := w.Commit(); err != nil {
				t.Fatalf("commit WAL: %v", err)
			}
			if syncs != tc.wantCommitSyncs {
				t.Fatalf("sync count after commit: got %d, want %d", syncs, tc.wantCommitSyncs)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close WAL: %v", err)
			}
		})
	}
}

func TestCheckpointActivationRequiresSourceValidation(t *testing.T) {
	validationFailure := errors.New("injected source validation failure")
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	cp, err := w.Checkpoint("source-block", nil, nil)
	if err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	if err := w.ActivateCheckpoint(cp, func() error { return validationFailure }); !errors.Is(err, validationFailure) {
		t.Fatalf("activation validation error: got %v, want %v", err, validationFailure)
	}
	if err := w.ActivateCheckpoint(cp, nil); err == nil {
		t.Fatal("activation without validation succeeded")
	}
	for _, rec := range collectRecords(t, dir) {
		if rec.Type == RecordCheckpointActivate {
			t.Fatal("failed validation wrote a checkpoint activation record")
		}
	}

	if err := w.ActivateCheckpoint(cp, func() error { return nil }); err != nil {
		t.Fatalf("activate validated checkpoint: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
}

func TestCommitSyncFailureIsSticky(t *testing.T) {
	syncFailure := errors.New("injected fsync failure")
	var syncs int
	opts := Options{sync: func(*os.File) error {
		syncs++
		if syncs == 1 {
			return syncFailure
		}
		return nil
	}}
	w, err := Open(filepath.Join(t.TempDir(), "wal"), opts)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := w.LogSamples([]RefSample{{Ref: 1, T: 1, V: 1}}); err != nil {
		t.Fatalf("log sample: %v", err)
	}
	if err := w.Commit(); !errors.Is(err, syncFailure) {
		t.Fatalf("commit error: got %v, want %v", err, syncFailure)
	}
	if err := w.Commit(); !errors.Is(err, syncFailure) {
		t.Fatalf("second commit error: got %v, want %v", err, syncFailure)
	}
	if err := w.Close(); !errors.Is(err, syncFailure) {
		t.Fatalf("close error: got %v, want %v", err, syncFailure)
	}
	if syncs != 2 {
		t.Fatalf("sync count: got %d, want 2", syncs)
	}
}

func TestWriteFailuresPoisonWAL(t *testing.T) {
	writeFailure := errors.New("injected write failure")
	tests := []struct {
		name    string
		write   func(*os.File, []byte) (int, error)
		wantErr error
	}{
		{
			name: "write_error",
			write: func(*os.File, []byte) (int, error) {
				return 0, writeFailure
			},
			wantErr: writeFailure,
		},
		{
			name: "short_write",
			write: func(f *os.File, b []byte) (int, error) {
				return f.Write(b[:len(b)/2])
			},
			wantErr: io.ErrShortWrite,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, err := Open(filepath.Join(t.TempDir(), "wal"), Options{write: tc.write})
			if err != nil {
				t.Fatalf("open WAL: %v", err)
			}
			if err := w.LogSamples([]RefSample{{Ref: 1, T: 1, V: 1}}); !errors.Is(err, tc.wantErr) {
				t.Fatalf("write error: got %v, want %v", err, tc.wantErr)
			}
			if err := w.Commit(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("commit after write failure: got %v, want %v", err, tc.wantErr)
			}
			if err := w.LogSamples([]RefSample{{Ref: 1, T: 2, V: 2}}); !errors.Is(err, tc.wantErr) {
				t.Fatalf("write after write failure: got %v, want %v", err, tc.wantErr)
			}
			if err := w.Close(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("close after write failure: got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestRotationFailuresPoisonWAL(t *testing.T) {
	rotationFailure := errors.New("injected rotation failure")
	tests := []struct {
		name string
		opts func() Options
	}{
		{
			name: "segment_sync",
			opts: func() Options {
				calls := 0
				return Options{sync: func(f *os.File) error {
					calls++
					if calls == 1 {
						return rotationFailure
					}
					return f.Sync()
				}}
			},
		},
		{
			name: "segment_close",
			opts: func() Options {
				calls := 0
				return Options{close: func(f *os.File) error {
					calls++
					if calls == 1 {
						return rotationFailure
					}
					return f.Close()
				}}
			},
		},
		{
			name: "segment_create",
			opts: func() Options {
				calls := 0
				return Options{createSegment: func(dir string, index int) (*os.File, error) {
					calls++
					if calls == 2 {
						return nil, rotationFailure
					}
					return createSegment(dir, index)
				}}
			},
		},
		{
			name: "directory_sync",
			opts: func() Options {
				calls := 0
				return Options{syncDir: func(dir string) error {
					calls++
					if calls == 3 {
						return rotationFailure
					}
					return syncDir(dir)
				}}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.opts()
			opts.SegmentMaxSize = 1
			w, err := Open(filepath.Join(t.TempDir(), "wal"), opts)
			if err != nil {
				t.Fatalf("open WAL: %v", err)
			}
			if err := w.LogSamples([]RefSample{{Ref: 1, T: 1, V: 1}}); !errors.Is(err, rotationFailure) {
				t.Fatalf("rotation error: got %v, want %v", err, rotationFailure)
			}
			if err := w.Commit(); !errors.Is(err, rotationFailure) {
				t.Fatalf("commit after rotation failure: got %v, want %v", err, rotationFailure)
			}
			if err := w.Close(); !errors.Is(err, rotationFailure) {
				t.Fatalf("close after rotation failure: got %v, want %v", err, rotationFailure)
			}
		})
	}
}

func TestTruncationDirectorySyncFailurePoisonsWAL(t *testing.T) {
	dirSyncFailure := errors.New("injected directory sync failure")
	w, err := Open(filepath.Join(t.TempDir(), "wal"), Options{SegmentMaxSize: 50})
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := w.LogSamples([]RefSample{{Ref: 1, T: int64(i), V: float64(i)}}); err != nil {
			t.Fatalf("log sample %d: %v", i, err)
		}
	}
	w.opts.syncDir = func(string) error { return dirSyncFailure }
	if err := w.Truncate(w.LastSegment()); !errors.Is(err, dirSyncFailure) {
		t.Fatalf("truncate error: got %v, want %v", err, dirSyncFailure)
	}
	if err := w.Commit(); !errors.Is(err, dirSyncFailure) {
		t.Fatalf("commit after directory sync failure: got %v, want %v", err, dirSyncFailure)
	}
	if err := w.Close(); !errors.Is(err, dirSyncFailure) {
		t.Fatalf("close after directory sync failure: got %v, want %v", err, dirSyncFailure)
	}
}

func TestBackgroundSyncErrorIsReported(t *testing.T) {
	syncFailure := errors.New("injected fsync failure")

	for _, operation := range []string{"commit", "close"} {
		t.Run(operation, func(t *testing.T) {
			called := make(chan struct{})
			var once sync.Once
			opts := Options{
				SyncPolicy:   SyncPeriodic,
				SyncInterval: time.Millisecond,
				sync: func(*os.File) error {
					once.Do(func() { close(called) })
					return syncFailure
				},
			}
			w, err := Open(filepath.Join(t.TempDir(), "wal"), opts)
			if err != nil {
				t.Fatalf("open WAL: %v", err)
			}
			if err := w.LogSamples([]RefSample{{Ref: 1, T: 1, V: 1}}); err != nil {
				t.Fatalf("log sample: %v", err)
			}
			select {
			case <-called:
			case <-time.After(time.Second):
				t.Fatal("background sync did not run")
			}

			if operation == "commit" {
				if err := w.Commit(); !errors.Is(err, syncFailure) {
					t.Fatalf("commit error: got %v, want %v", err, syncFailure)
				}
			}
			if err := w.Close(); !errors.Is(err, syncFailure) {
				t.Fatalf("close error: got %v, want %v", err, syncFailure)
			}
		})
	}
}
