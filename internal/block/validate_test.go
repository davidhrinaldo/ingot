package block

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/davidhrinaldo/ingot/labels"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T) string // returns block dir
		wantErrors int
		wantMatch  string // substring to find in concatenated errors; "" matches everything
	}{
		{
			name: "valid_block",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				series := []SeriesFlush{
					{
						Ref:    1,
						Labels: labels.FromStrings("__name__", "temp"),
						Chunks: []ChunkData{
							{MinT: 0, MaxT: 1000, Data: makeTestChunk(t)},
						},
					},
				}
				ulid, err := Flush(dir, series)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return filepath.Join(dir, ulid)
			},
			wantErrors: 0,
			wantMatch:  "",
		},
		{
			name: "missing_meta",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				series := []SeriesFlush{
					{
						Ref:    1,
						Labels: labels.FromStrings("__name__", "temp"),
						Chunks: []ChunkData{
							{MinT: 0, MaxT: 1000, Data: makeTestChunk(t)},
						},
					},
				}
				ulid, err := Flush(dir, series)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				blockDir := filepath.Join(dir, ulid)
				os.Remove(filepath.Join(blockDir, "meta.json"))
				return blockDir
			},
			wantErrors: 1,
			wantMatch:  "meta",
		},
		{
			name: "corrupt_chunk_crc",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				series := []SeriesFlush{
					{
						Ref:    1,
						Labels: labels.FromStrings("__name__", "temp"),
						Chunks: []ChunkData{
							{MinT: 0, MaxT: 1000, Data: makeTestChunk(t)},
						},
					},
				}
				ulid, err := Flush(dir, series)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				blockDir := filepath.Join(dir, ulid)

				// Corrupt a byte in the chunk data.
				chunkPath := filepath.Join(blockDir, "chunks", "000001")
				data, err := os.ReadFile(chunkPath)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				data[chunkHeaderLen+chunkEntryHeaderLen+2] ^= 0xFF
				if err := os.WriteFile(chunkPath, data, 0644); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return blockDir
			},
			wantErrors: 1,
			wantMatch:  "CRC mismatch",
		},
		{
			name: "corrupt_chunk_magic",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				series := []SeriesFlush{
					{
						Ref:    1,
						Labels: labels.FromStrings("__name__", "temp"),
						Chunks: []ChunkData{
							{MinT: 0, MaxT: 1000, Data: makeTestChunk(t)},
						},
					},
				}
				ulid, err := Flush(dir, series)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				blockDir := filepath.Join(dir, ulid)

				chunkPath := filepath.Join(blockDir, "chunks", "000001")
				data, err := os.ReadFile(chunkPath)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				binary.BigEndian.PutUint32(data[:4], 0xDEADBEEF)
				if err := os.WriteFile(chunkPath, data, 0644); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return blockDir
			},
			wantErrors: 1,
			wantMatch:  "invalid magic",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blockDir := tc.setup(t)
			errs := Validate(blockDir)
			if len(errs) != tc.wantErrors {
				t.Errorf("error count: got %d, want %d: %v", len(errs), tc.wantErrors, errs)
			}

			// Concatenate all error strings; "" is contained in everything.
			var combined strings.Builder
			for _, e := range errs {
				combined.WriteString(e.Error())
				combined.WriteByte('\n')
			}
			if !strings.Contains(combined.String(), tc.wantMatch) {
				t.Errorf("got %q, want substring %q", combined.String(), tc.wantMatch)
			}
		})
	}
}

func TestReadMeta(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T) (string, string) // returns (dir, ulid)
		wantULID  bool                                 // true = ULID should match
		wantMinT  int64
		wantMaxT  int64
		wantNSer  int
		wantNChk  int
		wantErr   error
	}{
		{
			name: "valid_block",
			setup: func(t *testing.T) (string, string) {
				dir := t.TempDir()
				ulid, err := Flush(dir, []SeriesFlush{
					{
						Ref:    1,
						Labels: labels.FromStrings("__name__", "test"),
						Chunks: []ChunkData{
							{MinT: 100, MaxT: 200, Data: makeTestChunk(t)},
						},
					},
				})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return dir, ulid
			},
			wantULID: true,
			wantMinT: 100,
			wantMaxT: 200,
			wantNSer: 1,
			wantNChk: 1,
			wantErr:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, ulid := tc.setup(t)
			meta, err := ReadMeta(filepath.Join(dir, ulid))
			if err != tc.wantErr {
				t.Errorf("error: got %v, want %v", err, tc.wantErr)
			}
			if (meta.ULID == ulid) != tc.wantULID {
				t.Errorf("ULID match: got %v, want %v", meta.ULID == ulid, tc.wantULID)
			}
			if meta.MinTime != tc.wantMinT {
				t.Errorf("MinTime: got %v, want %v", meta.MinTime, tc.wantMinT)
			}
			if meta.MaxTime != tc.wantMaxT {
				t.Errorf("MaxTime: got %v, want %v", meta.MaxTime, tc.wantMaxT)
			}
			if meta.Stats.NumSeries != tc.wantNSer {
				t.Errorf("NumSeries: got %v, want %v", meta.Stats.NumSeries, tc.wantNSer)
			}
			if meta.Stats.NumChunks != tc.wantNChk {
				t.Errorf("NumChunks: got %v, want %v", meta.Stats.NumChunks, tc.wantNChk)
			}
		})
	}
}

func makeTestChunk(t *testing.T) []byte {
	t.Helper()
	return makeChunk(t, []sample{s(0, 1.0), s(15000, 2.0), s(30000, 3.0)})
}
