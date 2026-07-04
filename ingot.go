// Package ingot is an embedded time-series database library for Go.
package ingot

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"git.dvdt.dev/david/ingot/internal/block"
	"git.dvdt.dev/david/ingot/internal/chunkenc"
	"git.dvdt.dev/david/ingot/internal/head"
	"git.dvdt.dev/david/ingot/internal/postings"
	"git.dvdt.dev/david/ingot/internal/wal"
	"git.dvdt.dev/david/ingot/labels"
)

// DB is an embedded time-series database.
type DB struct {
	dataDir string
	opts    Options
	head    *head.Head
	blocks  []*block.Reader // sorted by MinTime
	mu      sync.RWMutex    // protects blocks slice
}

// Options configures a DB.
type Options struct {
	Retention     time.Duration
	BlockDuration time.Duration
}

// Open opens or creates a DB at the given directory.
func Open(dataDir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("ingot: create data dir: %w", err)
	}

	walDir := filepath.Join(dataDir, "wal")
	h, err := head.Open(walDir, wal.Options{})
	if err != nil {
		return nil, fmt.Errorf("ingot: open head: %w", err)
	}

	db := &DB{
		dataDir: dataDir,
		opts:    opts,
		head:    h,
	}

	if err := db.loadBlocks(); err != nil {
		h.Close()
		return nil, fmt.Errorf("ingot: load blocks: %w", err)
	}

	return db, nil
}

// loadBlocks scans dataDir for block directories and opens them.
func (db *DB) loadBlocks() error {
	entries, err := os.ReadDir(db.dataDir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if !e.IsDir() || e.Name() == "wal" {
			continue
		}
		// Try to open as a block — skip if meta.json is missing.
		dir := filepath.Join(db.dataDir, e.Name())
		if _, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil {
			continue
		}
		br, err := block.Open(dir)
		if err != nil {
			return fmt.Errorf("open block %s: %w", e.Name(), err)
		}
		db.blocks = append(db.blocks, br)
	}

	sort.Slice(db.blocks, func(i, j int) bool {
		return db.blocks[i].Meta.MinTime < db.blocks[j].Meta.MinTime
	})

	return nil
}

// Appender returns a new Appender for batching writes.
func (db *DB) Appender() *Appender {
	return &Appender{inner: db.head.Appender()}
}

// Querier returns a Querier over [mint, maxt].
func (db *DB) Querier(mint, maxt int64) (*Querier, error) {
	db.mu.RLock()
	var overlapping []*block.Reader
	for _, b := range db.blocks {
		if b.Meta.MaxTime >= mint && b.Meta.MinTime <= maxt {
			overlapping = append(overlapping, b)
		}
	}
	db.mu.RUnlock()

	return &Querier{
		mint:   mint,
		maxt:   maxt,
		head:   db.head,
		blocks: overlapping,
	}, nil
}

// FlushOlderThan flushes sealed head chunks to an immutable block.
func (db *DB) FlushOlderThan(maxT int64) (string, error) {
	ulid, err := db.head.FlushOlderThan(maxT)
	if err != nil {
		return "", err
	}
	if ulid == "" {
		return "", nil
	}

	// Open the new block and add it to the block list.
	dir := filepath.Join(db.dataDir, ulid)
	br, err := block.Open(dir)
	if err != nil {
		return ulid, fmt.Errorf("ingot: open flushed block: %w", err)
	}

	db.mu.Lock()
	db.blocks = append(db.blocks, br)
	sort.Slice(db.blocks, func(i, j int) bool {
		return db.blocks[i].Meta.MinTime < db.blocks[j].Meta.MinTime
	})
	db.mu.Unlock()

	return ulid, nil
}

// Close closes the DB, releasing all resources.
func (db *DB) Close() error {
	var firstErr error
	if err := db.head.Close(); err != nil {
		firstErr = err
	}
	for _, b := range db.blocks {
		if err := b.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Appender buffers samples and new series for atomic commit.
type Appender struct {
	inner *head.Appender
}

// Append adds a sample. If ref is 0, the series is resolved (or created) from ls.
func (a *Appender) Append(ref uint64, ls []labels.Label, t int64, v float64) (uint64, error) {
	return a.inner.Append(ref, ls, t, v)
}

// Commit writes the batch to the WAL and applies it to the head.
func (a *Appender) Commit() error {
	return a.inner.Commit()
}

// Rollback discards the batch.
func (a *Appender) Rollback() error {
	return a.inner.Rollback()
}

// Querier queries the DB over a time range.
type Querier struct {
	mint, maxt int64
	head       *head.Head
	blocks     []*block.Reader
}

// Select returns a SeriesSet matching the given matchers.
func (q *Querier) Select(matchers ...*labels.Matcher) SeriesSet {
	// Collect refs from all sources, keyed by ref.
	type seriesSource struct {
		labels []labels.Label
		ref    uint64
	}
	refSet := make(map[uint64]seriesSource)

	// Resolve from each block.
	for _, b := range q.blocks {
		refs := resolveBlockPostings(b, matchers)
		for _, ref := range refs {
			if _, ok := refSet[ref]; !ok {
				ls, ok := b.Labels(ref)
				if !ok {
					continue
				}
				refSet[ref] = seriesSource{labels: ls, ref: ref}
			}
		}
	}

	// Resolve from head.
	headRefs := resolveHeadPostings(q.head, matchers)
	for _, ref := range headRefs {
		if _, ok := refSet[ref]; !ok {
			ls, ok := q.head.Labels(ref)
			if !ok {
				continue
			}
			refSet[ref] = seriesSource{labels: ls, ref: ref}
		}
	}

	// Build sorted series list by ref.
	sorted := make([]seriesSource, 0, len(refSet))
	for _, ss := range refSet {
		sorted = append(sorted, ss)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ref < sorted[j].ref })

	// Build series entries.
	entries := make([]resultSeries, 0, len(sorted))
	for _, ss := range sorted {
		entries = append(entries, resultSeries{
			labels:  ss.labels,
			ref:     ss.ref,
			querier: q,
		})
	}

	return &sliceSeriesSet{series: entries}
}

// Close is a no-op for now (refcounting is M5).
func (q *Querier) Close() error {
	return nil
}

func resolveBlockPostings(b *block.Reader, matchers []*labels.Matcher) []uint64 {
	if len(matchers) == 0 {
		return b.AllPostings()
	}
	var lists [][]uint64
	for _, m := range matchers {
		var refs []uint64
		switch m.Type {
		case labels.MatchEqual:
			refs = b.Postings(m.Name, m.Value)
		case labels.MatchNotEqual:
			refs = postings.Without(b.AllPostings(), b.Postings(m.Name, m.Value))
		case labels.MatchRegexp:
			var parts [][]uint64
			for _, v := range b.LabelValues(m.Name) {
				if m.Matches(v) {
					parts = append(parts, b.Postings(m.Name, v))
				}
			}
			refs = postings.Union(parts...)
		case labels.MatchNotRegexp:
			var matching [][]uint64
			for _, v := range b.LabelValues(m.Name) {
				if !m.Matches(v) { // m.Matches returns false for values matching the regex
					matching = append(matching, b.Postings(m.Name, v))
				}
			}
			refs = postings.Without(b.AllPostings(), postings.Union(matching...))
		}
		lists = append(lists, refs)
	}
	return postings.Intersect(lists...)
}

func resolveHeadPostings(h *head.Head, matchers []*labels.Matcher) []uint64 {
	if len(matchers) == 0 {
		return h.AllPostings()
	}
	var lists [][]uint64
	for _, m := range matchers {
		var refs []uint64
		switch m.Type {
		case labels.MatchEqual:
			refs = h.Postings(m.Name, m.Value)
		case labels.MatchNotEqual:
			refs = postings.Without(h.AllPostings(), h.Postings(m.Name, m.Value))
		case labels.MatchRegexp:
			var parts [][]uint64
			for _, v := range h.LabelValues(m.Name) {
				if m.Matches(v) {
					parts = append(parts, h.Postings(m.Name, v))
				}
			}
			refs = postings.Union(parts...)
		case labels.MatchNotRegexp:
			var matching [][]uint64
			for _, v := range h.LabelValues(m.Name) {
				if !m.Matches(v) {
					matching = append(matching, h.Postings(m.Name, v))
				}
			}
			refs = postings.Without(h.AllPostings(), postings.Union(matching...))
		}
		lists = append(lists, refs)
	}
	return postings.Intersect(lists...)
}

// SeriesSet iterates over query results.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Series represents a single time series.
type Series interface {
	Labels() []labels.Label
	Iterator() SampleIterator
}

// SampleIterator iterates over samples.
type SampleIterator interface {
	Next() bool
	At() (int64, float64)
	Err() error
}

// --- concrete implementations ---

type sliceSeriesSet struct {
	series []resultSeries
	cur    int
}

func (s *sliceSeriesSet) Next() bool {
	if s.cur >= len(s.series) {
		return false
	}
	s.cur++
	return s.cur <= len(s.series)
}

func (s *sliceSeriesSet) At() Series {
	return &s.series[s.cur-1]
}

func (s *sliceSeriesSet) Err() error { return nil }

type resultSeries struct {
	labels  []labels.Label
	ref     uint64
	querier *Querier
}

func (s *resultSeries) Labels() []labels.Label {
	return s.labels
}

func (s *resultSeries) Iterator() SampleIterator {
	var iters []chunkenc.ChunkIterator

	// Blocks first (in minTime order) — block values win on duplicate timestamps.
	for _, b := range s.querier.blocks {
		it, err := b.SeriesChunkIterator(s.ref, s.querier.mint, s.querier.maxt)
		if err != nil {
			return &errIterator{err: err}
		}
		iters = append(iters, it)
	}

	// Head last.
	iters = append(iters, s.querier.head.SeriesIterator(s.ref, s.querier.mint, s.querier.maxt))

	return &mergedSampleIterator{
		iters: iters,
		mint:  s.querier.mint,
		maxt:  s.querier.maxt,
	}
}

// mergedSampleIterator merges multiple ChunkIterators in order, deduplicating
// timestamps. Earlier iterators (blocks) win over later ones (head).
type mergedSampleIterator struct {
	iters  []chunkenc.ChunkIterator
	mint   int64
	maxt   int64
	cur    int
	lastT  int64
	curT   int64
	curV   float64
	started bool
	err    error
}

func (m *mergedSampleIterator) Next() bool {
	for {
		if m.err != nil {
			return false
		}
		// Try to advance the current iterator.
		for m.cur < len(m.iters) {
			if m.iters[m.cur].Next() {
				t, v := m.iters[m.cur].At()
				// Filter to [mint, maxt].
				if t < m.mint {
					continue
				}
				if t > m.maxt {
					// This iterator is past our range; move to next.
					m.cur++
					continue
				}
				// Dedup: skip if we've already emitted this timestamp.
				if m.started && t <= m.lastT {
					continue
				}
				m.curT = t
				m.curV = v
				m.lastT = t
				m.started = true
				return true
			}
			if err := m.iters[m.cur].Err(); err != nil {
				m.err = err
				return false
			}
			m.cur++
		}
		return false
	}
}

func (m *mergedSampleIterator) At() (int64, float64) {
	return m.curT, m.curV
}

func (m *mergedSampleIterator) Err() error {
	return m.err
}

type errIterator struct {
	err error
}

func (e *errIterator) Next() bool            { return false }
func (e *errIterator) At() (int64, float64)  { return 0, 0 }
func (e *errIterator) Err() error            { return e.err }

// ensure interfaces are satisfied.
var (
	_ SeriesSet      = (*sliceSeriesSet)(nil)
	_ Series         = (*resultSeries)(nil)
	_ SampleIterator = (*mergedSampleIterator)(nil)
	_ SampleIterator = (*errIterator)(nil)
)
