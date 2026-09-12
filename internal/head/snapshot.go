package head

import (
	"sort"

	"github.com/davidhrinaldo/ingot/internal/chunkenc"
	"github.com/davidhrinaldo/ingot/labels"
)

// Snapshot is an immutable view of the head at query creation time.
type Snapshot struct {
	series map[uint64]*memSeries
}

// Snapshot captures the head while commits and flushes are paused. capture is
// called under the same lock so callers can snapshot related state atomically.
func (h *Head) Snapshot(capture func()) *Snapshot {
	h.commitMu.Lock()
	defer h.commitMu.Unlock()

	if capture != nil {
		capture()
	}

	snapshot := &Snapshot{series: make(map[uint64]*memSeries)}
	h.series.forEach(func(s *memSeries) {
		if !s.walLogged {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()

		series := &memSeries{
			ref:    s.ref,
			labels: copyLabels(s.labels),
			sealed: append([]chunkMeta(nil), s.sealed...),
		}
		if s.chunk != nil && s.chunk.NumSamples() > 0 {
			series.sealed = append(series.sealed, chunkMeta{
				chunk: chunkenc.XORChunkFromBytes(s.chunk.Bytes()),
				minT:  s.chunkMinT,
				maxT:  s.lastT,
			})
		}
		snapshot.series[s.ref] = series
	})
	return snapshot
}

// SeriesIterator returns an iterator over the snapshot samples in [mint, maxt].
func (s *Snapshot) SeriesIterator(ref uint64, mint, maxt int64) chunkenc.ChunkIterator {
	series := s.series[ref]
	if series == nil {
		return &emptyIterator{}
	}
	return series.iterator(mint, maxt)
}

// Postings returns sorted series refs where the series has label name=value.
func (s *Snapshot) Postings(name, value string) []uint64 {
	var refs []uint64
	for _, series := range s.series {
		for _, l := range series.labels {
			if l.Name == name && l.Value == value {
				refs = append(refs, series.ref)
				break
			}
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	return refs
}

// LabelValues returns sorted unique values for the given label name.
func (s *Snapshot) LabelValues(name string) []string {
	seen := make(map[string]struct{})
	for _, series := range s.series {
		for _, l := range series.labels {
			if l.Name == name {
				seen[l.Value] = struct{}{}
				break
			}
		}
	}
	vals := make([]string, 0, len(seen))
	for v := range seen {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	return vals
}

// Labels returns the labels for a series by ref.
func (s *Snapshot) Labels(ref uint64) ([]labels.Label, bool) {
	series := s.series[ref]
	if series == nil {
		return nil, false
	}
	return series.labels, true
}

// AllPostings returns all series refs in sorted order.
func (s *Snapshot) AllPostings() []uint64 {
	refs := make([]uint64, 0, len(s.series))
	for ref := range s.series {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	return refs
}
