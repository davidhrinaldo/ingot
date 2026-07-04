package block

import (
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"git.dvdt.dev/david/ingot/internal/chunkenc"
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

func TestBlockMultipleChunksPerSeries(t *testing.T) {
	dataDir := t.TempDir()

	chunk1Samples := []sample{s(1000, 1.0), s(1015, 2.0), s(1030, 3.0)}
	chunk2Samples := []sample{s(2000, 4.0), s(2015, 5.0), s(2030, 6.0)}
	allSamples := append(chunk1Samples, chunk2Samples...)

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

	// Full range.
	it, err := r.SeriesChunkIterator(1, math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	got := collectIterator(t, it)
	require.Equal(t, len(allSamples), len(got))
	for i, want := range allSamples {
		assert.Equal(t, want.t, got[i].t, "sample %d t", i)
		assert.Equal(t, want.vBits, got[i].vBits, "sample %d v", i)
	}

	// Query only second chunk's range.
	it, err = r.SeriesChunkIterator(1, 2000, 3000)
	require.NoError(t, err)
	got = collectIterator(t, it)
	require.Equal(t, len(chunk2Samples), len(got))
	for i, want := range chunk2Samples {
		assert.Equal(t, want.t, got[i].t, "sample %d t", i)
		assert.Equal(t, want.vBits, got[i].vBits, "sample %d v", i)
	}

	// Query with no overlap.
	it, err = r.SeriesChunkIterator(1, 5000, 6000)
	require.NoError(t, err)
	got = collectIterator(t, it)
	assert.Empty(t, got)

	// Unknown ref.
	it, err = r.SeriesChunkIterator(999, math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	assert.False(t, it.Next())
}

func TestBlockMetaTimeBounds(t *testing.T) {
	dataDir := t.TempDir()

	flushData := []SeriesFlush{
		{
			Ref:    1,
			Labels: []labels.Label{{Name: "__name__", Value: "a"}},
			Chunks: []ChunkData{{MinT: 500, MaxT: 1000, Data: makeChunk(t, []sample{s(500, 1.0), s(1000, 2.0)})}},
		},
		{
			Ref:    2,
			Labels: []labels.Label{{Name: "__name__", Value: "b"}},
			Chunks: []ChunkData{{MinT: 200, MaxT: 800, Data: makeChunk(t, []sample{s(200, 3.0), s(800, 4.0)})}},
		},
	}

	ulid, err := Flush(dataDir, flushData)
	require.NoError(t, err)

	r, err := Open(filepath.Join(dataDir, ulid))
	require.NoError(t, err)
	defer r.Close()

	assert.Equal(t, int64(200), r.Meta.MinTime)
	assert.Equal(t, int64(1000), r.Meta.MaxTime)
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

func TestCorruptChunkCRC(t *testing.T) {
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

	// Open block, find the chunk ref, then corrupt the chunk file.
	blockDir := filepath.Join(dataDir, ulid)
	r, err := Open(blockDir)
	require.NoError(t, err)

	series := r.Series()
	require.Equal(t, 1, len(series))
	chunkRef := series[0].Chunks[0].Ref
	r.Close()

	// Corrupt chunk data on disk.
	chunkSeg := chunkRef.Segment()
	chunkPath := filepath.Join(blockDir, chunksDirName, segmentName(int(chunkSeg)))

	// Read, corrupt a data byte, write back.
	chunkFile, err := readFileBytes(chunkPath)
	require.NoError(t, err)
	off := int(chunkRef.Offset()) + chunkEntryHeaderLen + 1 // corrupt a data byte
	if off < len(chunkFile) {
		chunkFile[off] ^= 0xFF
	}
	require.NoError(t, writeFileBytes(chunkPath, chunkFile))

	// Re-open and try to read the corrupt chunk.
	r, err = Open(blockDir)
	require.NoError(t, err)
	defer r.Close()

	_, err = r.ChunkIterator(chunkRef)
	assert.Equal(t, ErrCorruptChunk, err)
}

func readFileBytes(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func writeFileBytes(path string, data []byte) error {
	return os.WriteFile(path, data, 0644)
}
