package block

import (
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"git.dvdt.dev/david/ingot/internal/chunkenc"
	"git.dvdt.dev/david/ingot/internal/index"
	"git.dvdt.dev/david/ingot/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sample struct {
	t     int64
	vBits uint64
}

func s(t int64, v float64) sample { return sample{t, math.Float64bits(v)} }

// makeChunk creates a chunk with the given samples and returns its raw bytes.
func makeChunk(t *testing.T, samples []sample) []byte {
	t.Helper()
	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	require.NoError(t, err)
	for _, s := range samples {
		a.Append(s.t, math.Float64frombits(s.vBits))
	}
	return append([]byte(nil), c.Bytes()...)
}

// collectIterator reads all samples from a ChunkIterator.
func collectIterator(t *testing.T, it chunkenc.ChunkIterator) []sample {
	t.Helper()
	var out []sample
	for it.Next() {
		ts, v := it.At()
		out = append(out, sample{ts, math.Float64bits(v)})
	}
	require.NoError(t, it.Err())
	return out
}

func TestBlockRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		series []struct {
			ref     uint64
			labels  []labels.Label
			samples []sample
		}
	}{
		{
			name: "single_series_single_chunk",
			series: []struct {
				ref     uint64
				labels  []labels.Label
				samples []sample
			}{
				{
					ref:    1,
					labels: []labels.Label{{Name: "__name__", Value: "temp"}},
					samples: []sample{
						s(1000, 71.3), s(1015, 71.4), s(1030, 71.5),
					},
				},
			},
		},
		{
			name: "multiple_series",
			series: []struct {
				ref     uint64
				labels  []labels.Label
				samples []sample
			}{
				{
					ref:    1,
					labels: []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}},
					samples: []sample{
						s(1000, 71.3), s(1015, 71.4), s(1030, 71.5),
					},
				},
				{
					ref:    2,
					labels: []labels.Label{{Name: "__name__", Value: "humidity"}, {Name: "room", Value: "office"}},
					samples: []sample{
						s(1000, 55.0), s(1015, 54.5), s(1030, 54.0),
					},
				},
			},
		},
		{
			name: "large_chunk",
			series: []struct {
				ref     uint64
				labels  []labels.Label
				samples []sample
			}{
				{
					ref:    1,
					labels: []labels.Label{{Name: "__name__", Value: "counter"}},
					samples: func() []sample {
						rnd := rand.New(rand.NewSource(42))
						out := make([]sample, 120)
						ts, v := int64(0), 0.0
						for i := range out {
							out[i] = s(ts, v)
							ts += 15000 + int64(rnd.Intn(100)) - 50
							v += rnd.Float64()
						}
						return out
					}(),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()

			// Build SeriesFlush data.
			var flushData []SeriesFlush
			for _, s := range tc.series {
				data := makeChunk(t, s.samples)
				minT, maxT := s.samples[0].t, s.samples[len(s.samples)-1].t
				flushData = append(flushData, SeriesFlush{
					Ref:    s.ref,
					Labels: s.labels,
					Chunks: []ChunkData{{MinT: minT, MaxT: maxT, Data: data}},
				})
			}

			// Write block.
			ulid, err := Flush(dataDir, flushData)
			require.NoError(t, err)
			require.NotEmpty(t, ulid)

			// Open block for reading.
			blockDir := filepath.Join(dataDir, ulid)
			r, err := Open(blockDir)
			require.NoError(t, err)
			defer r.Close()

			// Verify meta.
			assert.Equal(t, ulid, r.Meta.ULID)
			assert.Equal(t, 1, r.Meta.Version)
			assert.Equal(t, len(tc.series), r.Meta.Stats.NumSeries)
			assert.Equal(t, len(tc.series), r.Meta.Stats.NumChunks)

			// Verify each series' data via iteration.
			for _, s := range tc.series {
				it, err := r.SeriesChunkIterator(s.ref, math.MinInt64, math.MaxInt64)
				require.NoError(t, err)
				got := collectIterator(t, it)
				require.Equal(t, len(s.samples), len(got), "ref %d sample count", s.ref)
				for i, want := range s.samples {
					assert.Equal(t, want.t, got[i].t, "ref %d sample %d t", s.ref, i)
					assert.Equal(t, want.vBits, got[i].vBits, "ref %d sample %d v", s.ref, i)
				}
			}

			// Verify postings.
			for _, s := range tc.series {
				for _, l := range s.labels {
					refs := r.Postings(l.Name, l.Value)
					assert.Contains(t, refs, s.ref, "postings for %s=%s", l.Name, l.Value)
				}
			}

			// Verify labels lookup.
			for _, s := range tc.series {
				ls, ok := r.Labels(s.ref)
				assert.True(t, ok)
				assert.Equal(t, s.labels, ls)
			}
		})
	}
}

func TestBlockSeriesChunkIterator(t *testing.T) {
	chunk1Samples := []sample{s(1000, 1.0), s(1015, 2.0), s(1030, 3.0)}
	chunk2Samples := []sample{s(2000, 4.0), s(2015, 5.0), s(2030, 6.0)}

	tests := []struct {
		name        string
		ref         uint64
		mint        int64
		maxt        int64
		wantSamples []sample
	}{
		{
			name:        "full_range",
			ref:         1,
			mint:        math.MinInt64,
			maxt:        math.MaxInt64,
			wantSamples: append(chunk1Samples, chunk2Samples...),
		},
		{
			name:        "second_chunk_only",
			ref:         1,
			mint:        2000,
			maxt:        3000,
			wantSamples: chunk2Samples,
		},
		{
			name:        "no_overlap",
			ref:         1,
			mint:        5000,
			maxt:        6000,
			wantSamples: nil,
		},
		{
			name:        "unknown_ref",
			ref:         999,
			mint:        math.MinInt64,
			maxt:        math.MaxInt64,
			wantSamples: nil,
		},
	}

	// Setup: create a block with two chunks for series ref=1.
	dataDir := t.TempDir()
	flushData := []SeriesFlush{
		{
			Ref:    1,
			Labels: []labels.Label{{Name: "__name__", Value: "temp"}},
			Chunks: []ChunkData{
				{MinT: 1000, MaxT: 1030, Data: makeChunk(t, chunk1Samples)},
				{MinT: 2000, MaxT: 2030, Data: makeChunk(t, chunk2Samples)},
			},
		},
	}
	ulid, err := Flush(dataDir, flushData)
	require.NoError(t, err)
	r, err := Open(filepath.Join(dataDir, ulid))
	require.NoError(t, err)
	defer r.Close()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			it, err := r.SeriesChunkIterator(tc.ref, tc.mint, tc.maxt)
			require.NoError(t, err)
			got := collectIterator(t, it)
			assert.Equal(t, len(tc.wantSamples), len(got), "sample count")
			for i, want := range tc.wantSamples {
				assert.Equal(t, want.t, got[i].t, "sample %d t", i)
				assert.Equal(t, want.vBits, got[i].vBits, "sample %d v", i)
			}
		})
	}
}

func TestBlockMetaTimeBounds(t *testing.T) {
	tests := []struct {
		name     string
		series   []SeriesFlush
		wantMinT int64
		wantMaxT int64
	}{
		{
			name: "two_series_different_ranges",
			series: []SeriesFlush{
				{Ref: 1, Labels: []labels.Label{{Name: "__name__", Value: "a"}}, Chunks: []ChunkData{{MinT: 500, MaxT: 1000, Data: makeChunkFromPairs([]int64{500, 1000}, []float64{1.0, 2.0})}}},
				{Ref: 2, Labels: []labels.Label{{Name: "__name__", Value: "b"}}, Chunks: []ChunkData{{MinT: 200, MaxT: 800, Data: makeChunkFromPairs([]int64{200, 800}, []float64{3.0, 4.0})}}},
			},
			wantMinT: 200,
			wantMaxT: 1000,
		},
		{
			name: "single_series",
			series: []SeriesFlush{
				{Ref: 1, Labels: []labels.Label{{Name: "__name__", Value: "a"}}, Chunks: []ChunkData{{MinT: 100, MaxT: 500, Data: makeChunkFromPairs([]int64{100, 500}, []float64{1.0, 2.0})}}},
			},
			wantMinT: 100,
			wantMaxT: 500,
		},
		{
			name: "multiple_chunks",
			series: []SeriesFlush{
				{Ref: 1, Labels: []labels.Label{{Name: "__name__", Value: "a"}}, Chunks: []ChunkData{
					{MinT: 100, MaxT: 200, Data: makeChunkFromPairs([]int64{100, 200}, []float64{1.0, 2.0})},
					{MinT: 300, MaxT: 900, Data: makeChunkFromPairs([]int64{300, 900}, []float64{3.0, 4.0})},
				}},
			},
			wantMinT: 100,
			wantMaxT: 900,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			ulid, err := Flush(dataDir, tc.series)
			require.NoError(t, err)
			r, err := Open(filepath.Join(dataDir, ulid))
			require.NoError(t, err)
			defer r.Close()

			assert.Equal(t, tc.wantMinT, r.Meta.MinTime, "MinTime")
			assert.Equal(t, tc.wantMaxT, r.Meta.MaxTime, "MaxTime")
		})
	}
}

func TestULIDRoundTrip(t *testing.T) {
	for i := 0; i < 100; i++ {
		u := newULID()
		assert.Equal(t, 26, len(u), "ULID length")

		// Should parse without error.
		_, err := parseULID(u)
		assert.NoError(t, err, "parse ULID %q", u)
	}
}

func TestBlockCorruption(t *testing.T) {
	tests := []struct {
		name        string
		corruptFunc func(t *testing.T, blockDir string, chunkRef index.ChunkRef)
		wantErr     error
	}{
		{
			name: "corrupt_chunk_data_byte",
			corruptFunc: func(t *testing.T, blockDir string, chunkRef index.ChunkRef) {
				chunkPath := filepath.Join(blockDir, chunksDirName, segmentName(int(chunkRef.Segment())))
				data, err := os.ReadFile(chunkPath)
				require.NoError(t, err)
				off := int(chunkRef.Offset()) + chunkEntryHeaderLen + 1
				data[off] ^= 0xFF
				require.NoError(t, os.WriteFile(chunkPath, data, 0644))
			},
			wantErr: ErrCorruptChunk,
		},
		{
			name: "corrupt_chunk_crc",
			corruptFunc: func(t *testing.T, blockDir string, chunkRef index.ChunkRef) {
				chunkPath := filepath.Join(blockDir, chunksDirName, segmentName(int(chunkRef.Segment())))
				data, err := os.ReadFile(chunkPath)
				require.NoError(t, err)
				// Corrupt the last byte of the CRC.
				off := int(chunkRef.Offset()) + chunkEntryHeaderLen
				// Read dataLen to find CRC position.
				dataLen := int(data[chunkRef.Offset()])*16777216 + int(data[chunkRef.Offset()+1])*65536 +
					int(data[chunkRef.Offset()+2])*256 + int(data[chunkRef.Offset()+3])
				crcOff := off + dataLen + 3 // last byte of CRC
				data[crcOff] ^= 0xFF
				require.NoError(t, os.WriteFile(chunkPath, data, 0644))
			},
			wantErr: ErrCorruptChunk,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			samples := []sample{s(1000, 71.3), s(1015, 71.4)}
			flushData := []SeriesFlush{
				{
					Ref:    1,
					Labels: []labels.Label{{Name: "__name__", Value: "temp"}},
					Chunks: []ChunkData{{MinT: 1000, MaxT: 1015, Data: makeChunk(t, samples)}},
				},
			}
			ulid, err := Flush(dataDir, flushData)
			require.NoError(t, err)

			blockDir := filepath.Join(dataDir, ulid)
			r, err := Open(blockDir)
			require.NoError(t, err)
			series := r.Series()
			require.Equal(t, 1, len(series))
			chunkRef := series[0].Chunks[0].Ref
			r.Close()

			tc.corruptFunc(t, blockDir, chunkRef)

			r, err = Open(blockDir)
			require.NoError(t, err)
			defer r.Close()

			_, err = r.ChunkIterator(chunkRef)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

// makeChunkFromPairs creates a chunk from timestamp/value slices.
func makeChunkFromPairs(ts []int64, vs []float64) []byte {
	c := chunkenc.NewXORChunk()
	a, _ := c.Appender()
	for i := range ts {
		a.Append(ts[i], vs[i])
	}
	return append([]byte(nil), c.Bytes()...)
}
