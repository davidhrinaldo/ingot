package block

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"git.dvdt.dev/david/ingot/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				require.NoError(t, err)
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
				require.NoError(t, err)
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
				require.NoError(t, err)
				blockDir := filepath.Join(dir, ulid)

				// Corrupt a byte in the chunk data.
				chunkPath := filepath.Join(blockDir, "chunks", "000001")
				data, err := os.ReadFile(chunkPath)
				require.NoError(t, err)
				data[chunkHeaderLen+chunkEntryHeaderLen+2] ^= 0xFF
				require.NoError(t, os.WriteFile(chunkPath, data, 0644))
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
				require.NoError(t, err)
				blockDir := filepath.Join(dir, ulid)

				chunkPath := filepath.Join(blockDir, "chunks", "000001")
				data, err := os.ReadFile(chunkPath)
				require.NoError(t, err)
				binary.BigEndian.PutUint32(data[:4], 0xDEADBEEF)
				require.NoError(t, os.WriteFile(chunkPath, data, 0644))
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
			assert.Equal(t, tc.wantErrors, len(errs), "error count: %v", errs)

			// Concatenate all error strings; "" is contained in everything.
			var combined strings.Builder
			for _, e := range errs {
				combined.WriteString(e.Error())
				combined.WriteByte('\n')
			}
			assert.Contains(t, combined.String(), tc.wantMatch)
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
				require.NoError(t, err)
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
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantULID, meta.ULID == ulid)
			assert.Equal(t, tc.wantMinT, meta.MinTime)
			assert.Equal(t, tc.wantMaxT, meta.MaxTime)
			assert.Equal(t, tc.wantNSer, meta.Stats.NumSeries)
			assert.Equal(t, tc.wantNChk, meta.Stats.NumChunks)
		})
	}
}

func makeTestChunk(t *testing.T) []byte {
	t.Helper()
	return makeChunk(t, []sample{s(0, 1.0), s(15000, 2.0), s(30000, 3.0)})
}
