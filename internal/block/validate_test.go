package block

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
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
							{MinT: 0, MaxT: 30000, Data: makeTestChunk(t)},
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
							{MinT: 0, MaxT: 30000, Data: makeTestChunk(t)},
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
							{MinT: 0, MaxT: 30000, Data: makeTestChunk(t)},
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
							{MinT: 0, MaxT: 30000, Data: makeTestChunk(t)},
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
		name     string
		setup    func(t *testing.T) (string, string) // returns (dir, ulid)
		wantULID bool                                // true = ULID should match
		wantMinT int64
		wantMaxT int64
		wantNSer int
		wantNChk int
		wantErr  error
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
							{MinT: 100, MaxT: 200, Data: makeChunk(t, []sample{s(100, 1), s(200, 2)})},
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

func TestValidateSemanticCorruption(t *testing.T) {
	tests := []struct {
		name      string
		corrupt   func(t *testing.T, blockDir string)
		wantMatch string
	}{
		{
			name: "indexed_max_time",
			corrupt: func(t *testing.T, blockDir string) {
				mutateIndex(t, blockDir, func(data []byte, layout testIndexLayout) {
					binary.BigEndian.PutUint64(data[layout.series[0].chunks[0].maxT:], uint64(201))
				})
			},
			wantMatch: "do not match decoded samples",
		},
		{
			name: "chunk_ref_not_at_entry_boundary",
			corrupt: func(t *testing.T, blockDir string) {
				mutateIndex(t, blockDir, func(data []byte, layout testIndexLayout) {
					refOffset := layout.series[0].chunks[0].ref
					ref := binary.BigEndian.Uint64(data[refOffset:])
					binary.BigEndian.PutUint64(data[refOffset:], ref+1)
				})
			},
			wantMatch: "not a chunk entry boundary",
		},
		{
			name: "duplicate_chunk_ref",
			corrupt: func(t *testing.T, blockDir string) {
				mutateIndex(t, blockDir, func(data []byte, layout testIndexLayout) {
					first := layout.series[0].chunks[0].ref
					second := layout.series[0].chunks[1].ref
					copy(data[second:second+8], data[first:first+8])
				})
			},
			wantMatch: "duplicate chunk ref",
		},
		{
			name: "duplicate_series_ref",
			corrupt: func(t *testing.T, blockDir string) {
				mutateIndex(t, blockDir, func(data []byte, layout testIndexLayout) {
					copy(data[layout.series[1].ref:layout.series[1].ref+8], data[layout.series[0].ref:layout.series[0].ref+8])
				})
			},
			wantMatch: "duplicate series ref",
		},
		{
			name: "postings_missing_series",
			corrupt: func(t *testing.T, blockDir string) {
				mutateIndex(t, blockDir, func(data []byte, layout testIndexLayout) {
					binary.BigEndian.PutUint64(data[layout.postingRefs[0]:], 999)
				})
			},
			wantMatch: "reference missing series",
		},
		{
			name: "postings_wrong_series",
			corrupt: func(t *testing.T, blockDir string) {
				mutateIndex(t, blockDir, func(data []byte, layout testIndexLayout) {
					binary.BigEndian.PutUint64(data[layout.postingRefs[0]:], 2)
				})
			},
			wantMatch: "reference wrong series",
		},
		{
			name: "unsorted_chunks",
			corrupt: func(t *testing.T, blockDir string) {
				mutateIndex(t, blockDir, func(data []byte, layout testIndexLayout) {
					first := layout.series[0].chunks[0].minT
					second := layout.series[0].chunks[1].minT
					firstMeta := append([]byte(nil), data[first:first+24]...)
					copy(data[first:first+24], data[second:second+24])
					copy(data[second:second+24], firstMeta)
				})
			},
			wantMatch: "unsorted or overlap",
		},
		{
			name: "unsupported_chunk_encoding_with_valid_crc",
			corrupt: func(t *testing.T, blockDir string) {
				path := filepath.Join(blockDir, chunksDirName, "000001")
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				off := chunkHeaderLen
				dataLen := int(binary.BigEndian.Uint32(data[off : off+4]))
				data[off+4] = 99
				checksum := crc32.New(castagnoliTable)
				checksum.Write(data[off+4 : off+5])
				checksum.Write(data[off+chunkEntryHeaderLen : off+chunkEntryHeaderLen+dataLen])
				binary.BigEndian.PutUint32(data[off+chunkEntryHeaderLen+dataLen:], checksum.Sum32())
				if err := os.WriteFile(path, data, 0644); err != nil {
					t.Fatal(err)
				}
			},
			wantMatch: "unsupported encoding 99",
		},
		{
			name: "unsupported_meta_version",
			corrupt: func(t *testing.T, blockDir string) {
				mutateMeta(t, blockDir, func(meta *BlockMeta) { meta.Version++ })
			},
			wantMatch: "unsupported version 2",
		},
		{
			name: "meta_max_time",
			corrupt: func(t *testing.T, blockDir string) {
				mutateMeta(t, blockDir, func(meta *BlockMeta) { meta.MaxTime++ })
			},
			wantMatch: "bounds [100,601] do not match",
		},
		{
			name: "meta_stats",
			corrupt: func(t *testing.T, blockDir string) {
				mutateMeta(t, blockDir, func(meta *BlockMeta) { meta.Stats.NumSamples++ })
			},
			wantMatch: "numsamples 7 does not match decoded count 6",
		},
		{
			name: "meta_ulid",
			corrupt: func(t *testing.T, blockDir string) {
				mutateMeta(t, blockDir, func(meta *BlockMeta) { meta.ULID = "00000000000000000000000000" })
			},
			wantMatch: "does not match directory",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blockDir := semanticTestBlock(t)
			tc.corrupt(t, blockDir)

			validationErrs := Validate(blockDir)
			var combined strings.Builder
			for _, err := range validationErrs {
				combined.WriteString(err.Error())
				combined.WriteByte('\n')
			}
			if !strings.Contains(strings.ToLower(combined.String()), strings.ToLower(tc.wantMatch)) {
				t.Fatalf("Validate errors %q do not contain %q", combined.String(), tc.wantMatch)
			}

			reader, err := Open(blockDir)
			if reader != nil {
				reader.Close()
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.wantMatch)) {
				t.Fatalf("Open error %q does not contain %q", err, tc.wantMatch)
			}
		})
	}
}

func TestValidateRejectsOverlappingChunks(t *testing.T) {
	dataDir := t.TempDir()
	ulid, err := Flush(dataDir, []SeriesFlush{{
		Ref:    1,
		Labels: labels.FromStrings("__name__", "overlap"),
		Chunks: []ChunkData{
			{MinT: 100, MaxT: 300, Data: makeChunk(t, []sample{s(100, 1), s(300, 2)})},
			{MinT: 200, MaxT: 400, Data: makeChunk(t, []sample{s(200, 3), s(400, 4)})},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	blockDir := filepath.Join(dataDir, ulid)
	errs := Validate(blockDir)
	if len(errs) == 0 || !strings.Contains(errs[0].Error(), "unsorted or overlap") {
		t.Fatalf("got %v, want overlapping chunks error", errs)
	}
	if _, err := Open(blockDir); err == nil {
		t.Fatal("Open accepted overlapping chunks")
	}
}

func semanticTestBlock(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	ulid, err := Flush(dataDir, []SeriesFlush{
		{
			Ref:    1,
			Labels: labels.FromStrings("__name__", "a"),
			Chunks: []ChunkData{
				{MinT: 100, MaxT: 200, Data: makeChunk(t, []sample{s(100, 1), s(200, 2)})},
				{MinT: 300, MaxT: 400, Data: makeChunk(t, []sample{s(300, 3), s(400, 4)})},
			},
		},
		{
			Ref:    2,
			Labels: labels.FromStrings("__name__", "b"),
			Chunks: []ChunkData{
				{MinT: 500, MaxT: 600, Data: makeChunk(t, []sample{s(500, 5), s(600, 6)})},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dataDir, ulid)
}

type testChunkOffsets struct {
	minT int
	maxT int
	ref  int
}

type testSeriesOffsets struct {
	ref    int
	chunks []testChunkOffsets
}

type testIndexLayout struct {
	series      []testSeriesOffsets
	postingRefs []int
}

func mutateIndex(t *testing.T, blockDir string, mutate func([]byte, testIndexLayout)) {
	t.Helper()
	path := filepath.Join(blockDir, "index")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	layout := parseTestIndexLayout(t, data)
	mutate(data, layout)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func parseTestIndexLayout(t *testing.T, data []byte) testIndexLayout {
	t.Helper()
	toc := data[len(data)-28:]
	seriesOffset := int(binary.BigEndian.Uint64(toc[8:16]))
	postingsOffset := int(binary.BigEndian.Uint64(toc[16:24]))

	var layout testIndexLayout
	off := seriesOffset
	numSeries := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	for i := 0; i < numSeries; i++ {
		series := testSeriesOffsets{ref: off}
		off += 8
		numLabels := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2 + numLabels*8
		numChunks := int(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4
		for j := 0; j < numChunks; j++ {
			series.chunks = append(series.chunks, testChunkOffsets{minT: off, maxT: off + 8, ref: off + 16})
			off += 24
		}
		layout.series = append(layout.series, series)
	}

	off = postingsOffset
	numPostings := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	for i := 0; i < numPostings; i++ {
		numRefs := int(binary.BigEndian.Uint32(data[off+8 : off+12]))
		off += 12
		for j := 0; j < numRefs; j++ {
			layout.postingRefs = append(layout.postingRefs, off)
			off += 8
		}
	}
	return layout
}

func mutateMeta(t *testing.T, blockDir string, mutate func(*BlockMeta)) {
	t.Helper()
	meta, err := readMeta(blockDir)
	if err != nil {
		t.Fatal(err)
	}
	mutate(&meta)
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blockDir, metaFilename), data, 0644); err != nil {
		t.Fatal(err)
	}
}

func makeTestChunk(t *testing.T) []byte {
	t.Helper()
	return makeChunk(t, []sample{s(0, 1.0), s(15000, 2.0), s(30000, 3.0)})
}
