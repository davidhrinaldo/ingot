package index

import (
	"bytes"
	"testing"

	"git.dvdt.dev/david/ingot/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeIndex(t *testing.T, entries []SeriesEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, e := range entries {
		w.AddSeries(e)
	}
	_, err := w.WriteTo()
	require.NoError(t, err)
	return buf.Bytes()
}

func TestIndexRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		entries []SeriesEntry
	}{
		{
			name: "single_series_single_chunk",
			entries: []SeriesEntry{
				{
					Ref:    1,
					Labels: []labels.Label{{Name: "__name__", Value: "temp"}},
					Chunks: []ChunkMeta{{MinT: 1000, MaxT: 2000, Ref: NewChunkRef(1, 0)}},
				},
			},
		},
		{
			name: "single_series_multiple_chunks",
			entries: []SeriesEntry{
				{
					Ref:    1,
					Labels: []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}},
					Chunks: []ChunkMeta{
						{MinT: 1000, MaxT: 2000, Ref: NewChunkRef(1, 0)},
						{MinT: 2001, MaxT: 3000, Ref: NewChunkRef(1, 500)},
						{MinT: 3001, MaxT: 4000, Ref: NewChunkRef(1, 1000)},
					},
				},
			},
		},
		{
			name: "multiple_series",
			entries: []SeriesEntry{
				{
					Ref:    1,
					Labels: []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}},
					Chunks: []ChunkMeta{{MinT: 1000, MaxT: 2000, Ref: NewChunkRef(1, 0)}},
				},
				{
					Ref:    2,
					Labels: []labels.Label{{Name: "__name__", Value: "humidity"}, {Name: "room", Value: "office"}},
					Chunks: []ChunkMeta{{MinT: 1000, MaxT: 2000, Ref: NewChunkRef(1, 200)}},
				},
				{
					Ref:    3,
					Labels: []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "kitchen"}},
					Chunks: []ChunkMeta{{MinT: 1000, MaxT: 2000, Ref: NewChunkRef(1, 400)}},
				},
			},
		},
		{
			name:    "empty",
			entries: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := writeIndex(t, tc.entries)

			r, err := NewReader(data)
			require.NoError(t, err)

			// Verify series count and content.
			gotSeries := r.Series()
			require.Equal(t, len(tc.entries), len(gotSeries))

			for i, want := range tc.entries {
				got := gotSeries[i]
				assert.Equal(t, want.Ref, got.Ref, "series %d ref", i)
				assert.Equal(t, want.Labels, got.Labels, "series %d labels", i)
				require.Equal(t, len(want.Chunks), len(got.Chunks), "series %d chunk count", i)
				for j, wc := range want.Chunks {
					assert.Equal(t, wc.MinT, got.Chunks[j].MinT, "series %d chunk %d minT", i, j)
					assert.Equal(t, wc.MaxT, got.Chunks[j].MaxT, "series %d chunk %d maxT", i, j)
					assert.Equal(t, wc.Ref, got.Chunks[j].Ref, "series %d chunk %d ref", i, j)
				}

				// SeriesByRef lookup.
				byRef, ok := r.SeriesByRef(want.Ref)
				assert.True(t, ok, "series %d lookup by ref", i)
				assert.Equal(t, want.Ref, byRef.Ref)
			}

			// Verify postings.
			for _, e := range tc.entries {
				for _, l := range e.Labels {
					refs := r.Postings(l.Name, l.Value)
					assert.Contains(t, refs, e.Ref, "postings for %s=%s should contain ref %d", l.Name, l.Value, e.Ref)
				}
			}

			// Verify missing lookups return empty/false.
			_, ok := r.SeriesByRef(999999)
			assert.False(t, ok)
			assert.Nil(t, r.Postings("nonexistent", "value"))
		})
	}
}

func TestIndexPostingsSorted(t *testing.T) {
	entries := []SeriesEntry{
		{Ref: 5, Labels: []labels.Label{{Name: "room", Value: "office"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
		{Ref: 2, Labels: []labels.Label{{Name: "room", Value: "office"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
		{Ref: 8, Labels: []labels.Label{{Name: "room", Value: "office"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
	}

	data := writeIndex(t, entries)
	r, err := NewReader(data)
	require.NoError(t, err)

	refs := r.Postings("room", "office")
	require.Equal(t, 3, len(refs))
	assert.Equal(t, uint64(2), refs[0])
	assert.Equal(t, uint64(5), refs[1])
	assert.Equal(t, uint64(8), refs[2])
}

func TestIndexChunkRefEncoding(t *testing.T) {
	tests := []struct {
		name    string
		segment uint32
		offset  uint32
	}{
		{"zero", 0, 0},
		{"first_segment", 1, 0},
		{"with_offset", 1, 12345},
		{"large_values", 100, 536870912},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref := NewChunkRef(tc.segment, tc.offset)
			assert.Equal(t, tc.segment, ref.Segment())
			assert.Equal(t, tc.offset, ref.Offset())
		})
	}
}

func TestIndexLabelValues(t *testing.T) {
	entries := []SeriesEntry{
		{Ref: 1, Labels: []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
		{Ref: 2, Labels: []labels.Label{{Name: "__name__", Value: "humidity"}, {Name: "room", Value: "office"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
		{Ref: 3, Labels: []labels.Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "kitchen"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
	}

	data := writeIndex(t, entries)
	r, err := NewReader(data)
	require.NoError(t, err)

	tests := []struct {
		name     string
		label    string
		wantVals []string
	}{
		{name: "name_values", label: "__name__", wantVals: []string{"humidity", "temp"}},
		{name: "room_values", label: "room", wantVals: []string{"kitchen", "office"}},
		{name: "missing_label", label: "nonexistent", wantVals: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := r.LabelValues(tc.label)
			assert.Equal(t, tc.wantVals, got)
		})
	}
}

func TestIndexAllPostings(t *testing.T) {
	entries := []SeriesEntry{
		{Ref: 5, Labels: []labels.Label{{Name: "__name__", Value: "a"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
		{Ref: 2, Labels: []labels.Label{{Name: "__name__", Value: "b"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
		{Ref: 8, Labels: []labels.Label{{Name: "__name__", Value: "c"}}, Chunks: []ChunkMeta{{MinT: 0, MaxT: 1, Ref: 0}}},
	}

	data := writeIndex(t, entries)
	r, err := NewReader(data)
	require.NoError(t, err)

	refs := r.AllPostings()
	assert.Equal(t, []uint64{2, 5, 8}, refs)
}

func TestIndexCorruptData(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr error
	}{
		{
			name:    "too_short",
			data:    []byte{1, 2, 3},
			wantErr: ErrTooShort,
		},
		{
			name: "bad_magic",
			data: func() []byte {
				d := writeIndex(t, nil)
				d[0] = 0xFF
				return d
			}(),
			wantErr: ErrInvalidMagic,
		},
		{
			name: "bad_version",
			data: func() []byte {
				d := writeIndex(t, nil)
				d[4] = 99
				return d
			}(),
			wantErr: ErrInvalidVersion,
		},
		{
			name: "corrupt_toc_crc",
			data: func() []byte {
				d := writeIndex(t, nil)
				d[len(d)-1] ^= 0xFF // flip CRC bits
				return d
			}(),
			wantErr: ErrCorruptTOC,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReader(tc.data)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
