package block

import (
	"container/heap"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/davidhrinaldo/ingot/internal/chunkenc"
	"github.com/davidhrinaldo/ingot/internal/index"
	"github.com/davidhrinaldo/ingot/labels"
)

// Reader provides read access to an immutable on-disk block.
type Reader struct {
	dir       string
	Meta      BlockMeta
	idx       *index.Reader
	chunks    *chunkReader
	refs      atomic.Int32
	condemned atomic.Bool
}

// Open opens a block directory for reading. Chunk files are mmap'd.
func Open(dir string) (*Reader, error) {
	meta, err := readMeta(dir)
	if err != nil {
		return nil, err
	}

	// Read the index file into memory.
	indexData, err := os.ReadFile(filepath.Join(dir, "index"))
	if err != nil {
		return nil, err
	}
	idx, err := index.NewReader(indexData)
	if err != nil {
		return nil, err
	}

	cr, err := newChunkReader(dir)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		dir:    dir,
		Meta:   meta,
		idx:    idx,
		chunks: cr,
	}
	r.refs.Store(1) // DB's ownership ref
	return r, nil
}

// Series returns copies of all series entries from the index.
func (r *Reader) Series() []index.SeriesEntry {
	entries := r.idx.Series()
	result := make([]index.SeriesEntry, len(entries))
	for i, entry := range entries {
		result[i] = cloneSeriesEntry(entry)
	}
	return result
}

// SeriesByRef looks up a series by ref and returns a copy.
func (r *Reader) SeriesByRef(ref uint64) (index.SeriesEntry, bool) {
	entry, ok := r.idx.SeriesByRef(ref)
	if !ok {
		return index.SeriesEntry{}, false
	}
	return cloneSeriesEntry(entry), true
}

func cloneSeriesEntry(entry index.SeriesEntry) index.SeriesEntry {
	entry.Labels = append([]labels.Label(nil), entry.Labels...)
	entry.Chunks = append([]index.ChunkMeta(nil), entry.Chunks...)
	return entry
}

// Postings returns sorted series refs matching label name=value.
func (r *Reader) Postings(name, value string) []uint64 {
	return r.idx.Postings(name, value)
}

// ChunkIterator returns an iterator for a chunk at the given ref.
func (r *Reader) ChunkIterator(ref index.ChunkRef) (chunkenc.ChunkIterator, error) {
	return r.chunks.chunkIterator(ref)
}

// SeriesChunkIterator returns a chronological iterator over all chunks for a
// series in a time range. Earlier chunk entries win duplicate timestamps.
func (r *Reader) SeriesChunkIterator(ref uint64, mint, maxt int64) (chunkenc.ChunkIterator, error) {
	entry, ok := r.idx.SeriesByRef(ref)
	if !ok {
		return &emptyIterator{}, nil
	}

	var iters []chunkenc.ChunkIterator
	for _, cm := range entry.Chunks {
		if cm.MaxT < mint || cm.MinT > maxt {
			continue
		}
		it, err := r.chunks.chunkIterator(cm.Ref)
		if err != nil {
			return nil, err
		}
		iters = append(iters, it)
	}

	if len(iters) == 0 {
		return &emptyIterator{}, nil
	}
	return &multiIterator{iters: iters, mint: mint, maxt: maxt}, nil
}

// Labels returns the labels for a series by ref.
func (r *Reader) Labels(ref uint64) ([]labels.Label, bool) {
	entry, ok := r.idx.SeriesByRef(ref)
	if !ok {
		return nil, false
	}
	return append([]labels.Label(nil), entry.Labels...), true
}

// LabelValues returns sorted unique values for the given label name.
func (r *Reader) LabelValues(name string) []string {
	return r.idx.LabelValues(name)
}

// AllPostings returns sorted refs for all series in the block.
func (r *Reader) AllPostings() []uint64 {
	return r.idx.AllPostings()
}

// Dir returns the block directory path.
func (r *Reader) Dir() string { return r.dir }

// Ref increments the refcount. Called by Querier on snapshot.
func (r *Reader) Ref() { r.refs.Add(1) }

// Release decrements the refcount. Returns true if the refcount hit zero
// and the block is condemned (caller should delete the directory).
func (r *Reader) Release() bool {
	if r.refs.Add(-1) == 0 {
		r.Close()
		return r.condemned.Load()
	}
	return false
}

// Condemn marks the block for directory deletion when refcount reaches zero.
func (r *Reader) Condemn() { r.condemned.Store(true) }

// RawChunkData returns the raw chunk bytes at the given ref (for compaction).
func (r *Reader) RawChunkData(ref index.ChunkRef) ([]byte, error) {
	return r.chunks.chunkData(ref)
}

// Close releases all resources (munmaps chunk files).
func (r *Reader) Close() error {
	return r.chunks.close()
}

// multiIterator merges chunks by timestamp. The chunk's index in iters is its
// precedence, so duplicate values from earlier chunks win.
type multiIterator struct {
	iters       []chunkenc.ChunkIterator
	mint        int64
	maxt        int64
	heap        chunkIteratorHeap
	curT        int64
	curV        float64
	initialized bool
	err         error
}

func (m *multiIterator) Next() bool {
	if m.err != nil {
		return false
	}
	if !m.initialized {
		m.initialized = true
		heap.Init(&m.heap)
		for source := range m.iters {
			m.advance(source)
		}
		if m.err != nil {
			return false
		}
	}
	if len(m.heap) == 0 {
		return false
	}

	next := heap.Pop(&m.heap).(chunkIteratorHead)
	m.curT, m.curV = next.t, next.v
	m.advance(next.source)

	for len(m.heap) > 0 && m.heap[0].t == m.curT {
		duplicate := heap.Pop(&m.heap).(chunkIteratorHead)
		m.advance(duplicate.source)
	}
	return true
}

func (m *multiIterator) advance(source int) {
	it := m.iters[source]
	for it.Next() {
		t, v := it.At()
		if t < m.mint {
			continue
		}
		if t > m.maxt {
			return
		}
		heap.Push(&m.heap, chunkIteratorHead{source: source, t: t, v: v})
		return
	}
	if err := it.Err(); err != nil {
		m.err = err
	}
}

func (m *multiIterator) At() (int64, float64) {
	return m.curT, m.curV
}

func (m *multiIterator) Err() error {
	return m.err
}

type chunkIteratorHead struct {
	source int
	t      int64
	v      float64
}

type chunkIteratorHeap []chunkIteratorHead

func (h chunkIteratorHeap) Len() int { return len(h) }

func (h chunkIteratorHeap) Less(i, j int) bool {
	if h[i].t != h[j].t {
		return h[i].t < h[j].t
	}
	return h[i].source < h[j].source
}

func (h chunkIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *chunkIteratorHeap) Push(x any) {
	*h = append(*h, x.(chunkIteratorHead))
}

func (h *chunkIteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type emptyIterator struct{}

func (e *emptyIterator) Next() bool           { return false }
func (e *emptyIterator) At() (int64, float64) { return 0, 0 }
func (e *emptyIterator) Err() error           { return nil }
