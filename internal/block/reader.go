package block

import (
	"os"
	"path/filepath"

	"git.dvdt.dev/david/ingot/internal/chunkenc"
	"git.dvdt.dev/david/ingot/internal/index"
	"git.dvdt.dev/david/ingot/labels"
)

// Reader provides read access to an immutable on-disk block.
type Reader struct {
	dir    string
	Meta   BlockMeta
	idx    *index.Reader
	chunks *chunkReader
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

	return &Reader{
		dir:    dir,
		Meta:   meta,
		idx:    idx,
		chunks: cr,
	}, nil
}

// Series returns all series entries from the index.
func (r *Reader) Series() []index.SeriesEntry {
	return r.idx.Series()
}

// SeriesByRef looks up a series by ref.
func (r *Reader) SeriesByRef(ref uint64) (index.SeriesEntry, bool) {
	return r.idx.SeriesByRef(ref)
}

// Postings returns sorted series refs matching label name=value.
func (r *Reader) Postings(name, value string) []uint64 {
	return r.idx.Postings(name, value)
}

// ChunkIterator returns an iterator for a chunk at the given ref.
func (r *Reader) ChunkIterator(ref index.ChunkRef) (chunkenc.ChunkIterator, error) {
	return r.chunks.chunkIterator(ref)
}

// SeriesChunkIterator returns an iterator over all chunks for a series in a
// time range. Chunks are iterated in order.
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
	if len(iters) == 1 {
		return iters[0], nil
	}
	return &multiIterator{iters: iters}, nil
}

// Labels returns the labels for a series by ref.
func (r *Reader) Labels(ref uint64) ([]labels.Label, bool) {
	entry, ok := r.idx.SeriesByRef(ref)
	if !ok {
		return nil, false
	}
	return entry.Labels, true
}

// Close releases all resources (munmaps chunk files).
func (r *Reader) Close() error {
	return r.chunks.close()
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

type emptyIterator struct{}

func (e *emptyIterator) Next() bool        { return false }
func (e *emptyIterator) At() (int64, float64) { return 0, 0 }
func (e *emptyIterator) Err() error        { return nil }
