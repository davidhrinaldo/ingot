package index

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/davidhrinaldo/ingot/labels"
)

func writeIndex(t *testing.T, entries []SeriesEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, e := range entries {
		w.AddSeries(e)
	}
	_, err := w.WriteTo()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify series count and content.
			gotSeries := r.Series()
			if len(gotSeries) != len(tc.entries) {
				t.Fatalf("got %v, want %v", len(gotSeries), len(tc.entries))
			}

			for i, want := range tc.entries {
				got := gotSeries[i]
				if got.Ref != want.Ref {
					t.Errorf("series %d ref: got %v, want %v", i, got.Ref, want.Ref)
				}
				if !reflect.DeepEqual(got.Labels, want.Labels) {
					t.Errorf("series %d labels: got %v, want %v", i, got.Labels, want.Labels)
				}
				if len(got.Chunks) != len(want.Chunks) {
					t.Fatalf("series %d chunk count: got %v, want %v", i, len(got.Chunks), len(want.Chunks))
				}
				for j, wc := range want.Chunks {
					if got.Chunks[j].MinT != wc.MinT {
						t.Errorf("series %d chunk %d minT: got %v, want %v", i, j, got.Chunks[j].MinT, wc.MinT)
					}
					if got.Chunks[j].MaxT != wc.MaxT {
						t.Errorf("series %d chunk %d maxT: got %v, want %v", i, j, got.Chunks[j].MaxT, wc.MaxT)
					}
					if got.Chunks[j].Ref != wc.Ref {
						t.Errorf("series %d chunk %d ref: got %v, want %v", i, j, got.Chunks[j].Ref, wc.Ref)
					}
				}

				// SeriesByRef lookup.
				byRef, ok := r.SeriesByRef(want.Ref)
				if !ok {
					t.Errorf("series %d lookup by ref: got false, want true", i)
				}
				if byRef.Ref != want.Ref {
					t.Errorf("got %v, want %v", byRef.Ref, want.Ref)
				}
			}

			// Verify postings.
			for _, e := range tc.entries {
				for _, l := range e.Labels {
					refs := r.Postings(l.Name, l.Value)
					found := false
					for _, ref := range refs {
						if ref == e.Ref {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("postings for %s=%s should contain ref %d, got %v", l.Name, l.Value, e.Ref, refs)
					}
				}
			}

			// Verify missing lookups return empty/false.
			_, ok := r.SeriesByRef(999999)
			if ok {
				t.Errorf("got true, want false")
			}
			if refs := r.Postings("nonexistent", "value"); refs != nil {
				t.Errorf("got %v, want nil", refs)
			}
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	refs := r.Postings("room", "office")
	if len(refs) != 3 {
		t.Fatalf("got %v, want %v", len(refs), 3)
	}
	if refs[0] != uint64(2) {
		t.Errorf("got %v, want %v", refs[0], uint64(2))
	}
	if refs[1] != uint64(5) {
		t.Errorf("got %v, want %v", refs[1], uint64(5))
	}
	if refs[2] != uint64(8) {
		t.Errorf("got %v, want %v", refs[2], uint64(8))
	}
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
			if ref.Segment() != tc.segment {
				t.Errorf("got %v, want %v", ref.Segment(), tc.segment)
			}
			if ref.Offset() != tc.offset {
				t.Errorf("got %v, want %v", ref.Offset(), tc.offset)
			}
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
			if !reflect.DeepEqual(got, tc.wantVals) {
				t.Errorf("got %v, want %v", got, tc.wantVals)
			}
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	refs := r.AllPostings()
	if !reflect.DeepEqual(refs, []uint64{2, 5, 8}) {
		t.Errorf("got %v, want %v", refs, []uint64{2, 5, 8})
	}
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
			if err != tc.wantErr {
				t.Errorf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}
