// Package head implements the in-memory series store for the active
// write window, backed by a write-ahead log for crash safety.
package head

import (
	"fmt"
	"sync/atomic"

	"git.dvdt.dev/david/ingot/internal/chunkenc"
	"git.dvdt.dev/david/ingot/internal/wal"
	"git.dvdt.dev/david/ingot/labels"
)

// Head is the in-memory store for active series and their chunks.
type Head struct {
	series  *seriesMap
	wal     *wal.WAL
	nextRef atomic.Uint64

	minTime atomic.Int64
	maxTime atomic.Int64
	minSet  atomic.Bool
}

// Open creates or recovers a Head backed by a WAL in walDir.
func Open(walDir string, walOpts wal.Options) (*Head, error) {
	w, err := wal.Open(walDir, walOpts)
	if err != nil {
		return nil, err
	}

	h := &Head{
		series: newSeriesMap(),
		wal:    w,
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
