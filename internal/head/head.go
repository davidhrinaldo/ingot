// Package head implements the in-memory series store for the active
// write window, backed by a write-ahead log for crash safety.
package head

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync/atomic"

	"git.dvdt.dev/david/ingot/internal/block"
	"git.dvdt.dev/david/ingot/internal/chunkenc"
	"git.dvdt.dev/david/ingot/internal/wal"
	"git.dvdt.dev/david/ingot/labels"
)

// Head is the in-memory store for active series and their chunks.
type Head struct {
	dataDir string // parent directory containing WAL and block dirs
	series  *seriesMap
	wal     *wal.WAL
	nextRef atomic.Uint64

	minTime atomic.Int64
	maxTime atomic.Int64
	minSet  atomic.Bool
}

// Open creates or recovers a Head backed by a WAL in walDir.
// The dataDir (parent of walDir) is used for writing blocks.
func Open(walDir string, walOpts wal.Options) (*Head, error) {
	w, err := wal.Open(walDir, walOpts)
	if err != nil {
		return nil, err
	}

	h := &Head{
		dataDir: filepath.Dir(walDir),
		series:  newSeriesMap(),
		wal:     w,
	}

	if err := h.replay(); err != nil {
		w.Close()
		return nil, err
	}

	return h, nil
}

// replay reads all WAL records and rebuilds in-memory state.
func (h *Head) replay() error {
	r, err := h.wal.Replay()
	if err != nil {
		return err
	}
	defer r.Close()

	for r.Next() {
		rec := r.Record()
		switch rec.Type {
		case wal.RecordSeries:
			sr, err := wal.DecodeSeriesRecord(rec.Data)
			if err != nil {
				return fmt.Errorf("head: replay series: %w", err)
			}
			h.replaySeries(sr)
		case wal.RecordSamples:
			samples, err := wal.DecodeSamplesRecord(rec.Data)
			if err != nil {
				return fmt.Errorf("head: replay samples: %w", err)
			}
			for _, s := range samples {
				h.applySample(s.Ref, s.T, s.V)
			}
		}
	}
	return r.Err()
}

func (h *Head) replaySeries(sr wal.SeriesRecord) {
	// Update nextRef so new series don't collide.
	for {
		cur := h.nextRef.Load()
		if sr.Ref < cur {
			break
		}
		if h.nextRef.CompareAndSwap(cur, sr.Ref+1) {
			break
		}
	}

	// Skip if already exists (idempotent replay).
	hash := labels.Hash(sr.Labels)
	if h.series.getByHash(hash, sr.Labels) != nil {
		return
	}

	s := &memSeries{
		ref:    sr.Ref,
		labels: sr.Labels,
	}
	h.series.set(hash, s)
}

// applySample appends a sample to the series' active chunk.
func (h *Head) applySample(ref uint64, t int64, v float64) {
	s := h.series.getByRef(ref)
	if s == nil {
		return // series not found; skip during replay of partial WAL
	}

	s.mu.Lock()
	s.append(t, v)
	s.mu.Unlock()

	// Update head time bounds.
	if !h.minSet.Load() || t < h.minTime.Load() {
		h.minTime.Store(t)
		h.minSet.Store(true)
	}
	if t > h.maxTime.Load() {
		h.maxTime.Store(t)
	}
}

// Appender returns a new Appender for batching writes.
func (h *Head) Appender() *Appender {
	return &Appender{head: h}
}

// SeriesIterator returns an iterator over all samples in [mint, maxt]
// for the given series ref.
func (h *Head) SeriesIterator(ref uint64, mint, maxt int64) chunkenc.ChunkIterator {
	s := h.series.getByRef(ref)
	if s == nil {
		return &emptyIterator{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.iterator(mint, maxt)
}

// MinTime returns the earliest sample timestamp in the head.
func (h *Head) MinTime() int64 { return h.minTime.Load() }

// MaxTime returns the latest sample timestamp in the head.
func (h *Head) MaxTime() int64 { return h.maxTime.Load() }

// Close syncs and closes the WAL.
func (h *Head) Close() error {
	return h.wal.Close()
}

// FlushOlderThan collects all sealed chunks with maxT <= threshold from all
// series, writes them to an immutable block, and truncates the WAL.
//
// The ordering invariant is enforced: block fsync -> meta.json write -> WAL truncate.
// Returns the block ULID (empty string if nothing to flush) and any error.
func (h *Head) FlushOlderThan(maxT int64) (string, error) {
	var flushData []block.SeriesFlush

	h.series.forEach(func(s *memSeries) {
		s.mu.Lock()
		defer s.mu.Unlock()

		var toFlush []chunkMeta
		var remaining []chunkMeta
		for _, cm := range s.sealed {
			if cm.maxT <= maxT {
				toFlush = append(toFlush, cm)
			} else {
				remaining = append(remaining, cm)
			}
		}
		if len(toFlush) == 0 {
			return
		}

		sf := block.SeriesFlush{
			Ref:    s.ref,
			Labels: s.labels,
		}
		for _, cm := range toFlush {
			sf.Chunks = append(sf.Chunks, block.ChunkData{
				MinT: cm.minT,
				MaxT: cm.maxT,
				Data: append([]byte(nil), cm.chunk.Bytes()...),
			})
		}
		flushData = append(flushData, sf)

		// Clear flushed chunks from the series.
		s.sealed = remaining
	})

	if len(flushData) == 0 {
		return "", nil
	}

	// Write block. Flush handles: chunk files + index + fsync + meta.json.
	ulid, err := block.Flush(h.dataDir, flushData)
	if err != nil {
		return "", fmt.Errorf("head: flush block: %w", err)
	}

	// WAL truncation: safe because the block is fully fsynced.
	// Truncate all segments below the current one — the flushed data is now
	// in the block and doesn't need WAL replay.
	lastSeg := h.wal.LastSegment()
	if err := h.wal.Truncate(lastSeg); err != nil {
		return ulid, fmt.Errorf("head: truncate WAL: %w", err)
	}

	return ulid, nil
}

// Postings returns sorted series refs where the series has label name=value.
func (h *Head) Postings(name, value string) []uint64 {
	var refs []uint64
	h.series.forEach(func(s *memSeries) {
		for _, l := range s.labels {
			if l.Name == name && l.Value == value {
				refs = append(refs, s.ref)
				break
			}
		}
	})
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	return refs
}

// LabelValues returns sorted unique values for the given label name.
func (h *Head) LabelValues(name string) []string {
	seen := make(map[string]struct{})
	h.series.forEach(func(s *memSeries) {
		for _, l := range s.labels {
			if l.Name == name {
				seen[l.Value] = struct{}{}
				break
			}
		}
	})
	vals := make([]string, 0, len(seen))
	for v := range seen {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	return vals
}

// Labels returns the labels for a series by ref.
func (h *Head) Labels(ref uint64) ([]labels.Label, bool) {
	s := h.series.getByRef(ref)
	if s == nil {
		return nil, false
	}
	return s.labels, true
}

// AllPostings returns sorted refs for all series in the head.
func (h *Head) AllPostings() []uint64 {
	var refs []uint64
	h.series.forEach(func(s *memSeries) {
		refs = append(refs, s.ref)
	})
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	return refs
}

// DataDir returns the data directory (parent of WAL dir).
func (h *Head) DataDir() string {
	return h.dataDir
}

// Stats returns a snapshot of head statistics.
func (h *Head) Stats() HeadStats {
	var s HeadStats
	h.series.forEach(func(ms *memSeries) {
		s.NumSeries++
		ms.mu.Lock()
		if ms.chunk != nil && ms.chunk.NumSamples() > 0 {
			s.NumActiveChunks++
		}
		s.NumActiveChunks += len(ms.sealed)
		ms.mu.Unlock()
	})
	return s
}

// HeadStats holds a snapshot of head statistics.
type HeadStats struct {
	NumSeries      int
	NumActiveChunks int
}

// WALSyncDuration returns the duration of the most recent WAL fsync in seconds.
func (h *Head) WALSyncDuration() float64 {
	return h.wal.LastSyncDuration()
}
