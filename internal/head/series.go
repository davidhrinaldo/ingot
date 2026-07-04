package head

import (
	"sync"

	"github.com/davidhrinaldo/ingot/internal/chunkenc"
	"github.com/davidhrinaldo/ingot/labels"
)

const samplesPerChunk = 120

type chunkMeta struct {
	chunk *chunkenc.XORChunk
	minT  int64
	maxT  int64
}

type memSeries struct {
	mu     sync.Mutex
	ref    uint64
	labels []labels.Label

	// Active chunk and its appender.
	chunk    *chunkenc.XORChunk
	chunkApp chunkenc.ChunkAppender
	chunkMinT int64

	// Sealed chunks awaiting block flush.
	sealed []chunkMeta

	// Timestamp of last appended sample (for OOO rejection).
	lastT    int64
	hasData  bool // false until the first sample is appended
}

// append adds a sample to the series. Caller must hold s.mu.
func (s *memSeries) append(t int64, v float64) {
	if s.chunk == nil || s.chunk.NumSamples() >= samplesPerChunk {
		s.cutNewChunk(t)
	}
	if !s.hasData {
		s.chunkMinT = t
		s.hasData = true
	}
	s.chunkApp.Append(t, v)
	s.lastT = t
}

// cutNewChunk seals the current chunk (if any) and starts a fresh one.
func (s *memSeries) cutNewChunk(t int64) {
	if s.chunk != nil {
		s.sealed = append(s.sealed, chunkMeta{
			chunk: s.chunk,
			minT:  s.chunkMinT,
			maxT:  s.lastT,
		})
	}
	s.chunk = chunkenc.NewXORChunk()
	s.chunkApp, _ = s.chunk.Appender() // always succeeds on a fresh chunk
	s.chunkMinT = t
}

// iterator returns an iterator over all samples in [mint, maxt].
// Caller must hold s.mu.
func (s *memSeries) iterator(mint, maxt int64) chunkenc.ChunkIterator {
	var iters []chunkenc.ChunkIterator

	for _, cm := range s.sealed {
		if cm.maxT < mint || cm.minT > maxt {
			continue
		}
		iters = append(iters, cm.chunk.Iterator())
	}

	if s.chunk != nil && s.chunk.NumSamples() > 0 && s.chunkMinT <= maxt && s.lastT >= mint {
		// Snapshot the active chunk bytes for safe concurrent iteration.
		snap := &chunkenc.XORChunk{}
		*snap = *s.chunk
		iters = append(iters, snap.Iterator())
	}

	if len(iters) == 0 {
		return &emptyIterator{}
	}
	if len(iters) == 1 {
		return iters[0]
	}
	return &multiIterator{iters: iters}
}

// multiIterator chains multiple ChunkIterators in order.
type multiIterator struct {
	iters []chunkenc.ChunkIterator
	cur   int
}

func (m *multiIterator) Next() bool {
	for m.cur < len(m.iters) {
		if m.iters[m.cur].Next() {
			return true
		}
		if m.iters[m.cur].Err() != nil {
			return false
		}
		m.cur++
	}
	return false
}

func (m *multiIterator) At() (int64, float64) {
	return m.iters[m.cur].At()
}

func (m *multiIterator) Err() error {
	if m.cur < len(m.iters) {
		return m.iters[m.cur].Err()
	}
	return nil
}

// emptyIterator is returned when a series has no data in the requested range.
type emptyIterator struct{}

func (e *emptyIterator) Next() bool        { return false }
func (e *emptyIterator) At() (int64, float64) { return 0, 0 }
func (e *emptyIterator) Err() error        { return nil }
