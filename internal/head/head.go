// Package head implements the in-memory series store for the active
// write window, backed by a write-ahead log for crash safety.
package head

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/davidhrinaldo/ingot/internal/block"
	"github.com/davidhrinaldo/ingot/internal/chunkenc"
	"github.com/davidhrinaldo/ingot/internal/wal"
	"github.com/davidhrinaldo/ingot/labels"
)

// Head is the in-memory store for active series and their chunks.
type Head struct {
	dataDir  string // parent directory containing WAL and block dirs
	series   *seriesMap
	wal      *wal.WAL
	nextRef  atomic.Uint64
	commitMu sync.Mutex

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

	var acceptedCheckpoint int
	var checkpoint struct {
		marker    wal.Checkpoint
		series    []wal.SeriesRecord
		samples   [][]wal.RefSample
		active    bool
		committed bool
		applied   bool
	}
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
		case wal.RecordCheckpointBegin:
			cp, err := wal.DecodeCheckpoint(rec.Data)
			if err != nil {
				return fmt.Errorf("head: replay checkpoint begin: %w", err)
			}
			checkpoint.marker = cp
			checkpoint.series = nil
			checkpoint.samples = nil
			checkpoint.active = true
			checkpoint.committed = false
			checkpoint.applied = false
		case wal.RecordCheckpointSeries:
			if !checkpoint.active {
				continue
			}
			sr, err := wal.DecodeSeriesRecord(rec.Data)
			if err != nil {
				return fmt.Errorf("head: replay checkpoint series: %w", err)
			}
			checkpoint.series = append(checkpoint.series, sr)
		case wal.RecordCheckpointSamples:
			if !checkpoint.active {
				continue
			}
			samples, err := wal.DecodeSamplesRecord(rec.Data)
			if err != nil {
				return fmt.Errorf("head: replay checkpoint samples: %w", err)
			}
			checkpoint.samples = append(checkpoint.samples, samples)
		case wal.RecordCheckpointCommit:
			cp, err := wal.DecodeCheckpoint(rec.Data)
			if err != nil {
				return fmt.Errorf("head: replay checkpoint commit: %w", err)
			}
			if !checkpoint.active || cp != checkpoint.marker {
				continue
			}
			checkpoint.active = false
			checkpoint.committed = true
			valid, err := h.checkpointValid(cp)
			// A later activation record is authoritative even if the source
			// block has since been compacted, retained, or cannot be opened.
			if err != nil {
				valid = false
			}
			if valid {
				h.applyReplayCheckpoint(checkpoint.series, checkpoint.samples)
				checkpoint.applied = true
				acceptedCheckpoint = cp.StartSegment
			}
		case wal.RecordCheckpointActivate:
			cp, err := wal.DecodeCheckpoint(rec.Data)
			if err != nil {
				return fmt.Errorf("head: replay checkpoint activation: %w", err)
			}
			if !checkpoint.committed || checkpoint.applied || cp != checkpoint.marker {
				continue
			}
			h.applyReplayCheckpoint(checkpoint.series, checkpoint.samples)
			checkpoint.applied = true
			acceptedCheckpoint = cp.StartSegment
		}
	}
	if err := r.Err(); err != nil {
		r.Close()
		return err
	}
	if err := r.Close(); err != nil {
		return err
	}
	if acceptedCheckpoint != 0 {
		return h.wal.Truncate(acceptedCheckpoint)
	}
	return nil
}

func (h *Head) applyReplayCheckpoint(series []wal.SeriesRecord, samples [][]wal.RefSample) {
	h.resetReplayState()
	for _, sr := range series {
		h.replaySeries(sr)
	}
	for _, batch := range samples {
		for _, s := range batch {
			h.applySample(s.Ref, s.T, s.V)
		}
	}
}

func (h *Head) checkpointValid(cp wal.Checkpoint) (bool, error) {
	hasOlder, err := h.wal.HasSegmentBefore(cp.StartSegment)
	if err != nil {
		return false, err
	}
	if !hasOlder {
		return true, nil
	}
	blockDir := filepath.Join(h.dataDir, cp.BlockULID)
	br, err := block.Open(blockDir)
	if err == nil {
		if br.Meta.ULID != cp.BlockULID {
			br.Close()
			return false, nil
		}
		for _, entry := range br.Series() {
			for _, chunk := range entry.Chunks {
				if _, err := br.RawChunkData(chunk.Ref); err != nil {
					br.Close()
					return false, err
				}
			}
		}
		return true, br.Close()
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (h *Head) resetReplayState() {
	h.series = newSeriesMap()
	h.nextRef.Store(0)
	h.minTime.Store(0)
	h.maxTime.Store(0)
	h.minSet.Store(false)
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
		ref:       sr.Ref,
		labels:    sr.Labels,
		walLogged: true,
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
// series, writes them to an immutable block, and checkpoints the remaining head.
//
// The ordering invariant is enforced: block data fsync -> WAL checkpoint fsync ->
// meta.json publication -> WAL truncate.
// Returns the block ULID (empty string if nothing to flush) and any error.
func (h *Head) FlushOlderThan(maxT int64) (string, error) {
	return h.flushOlderThan(maxT, nil)
}

// FlushOlderThanAndInstall installs a published block before its chunks are
// evicted from the head, preventing a gap for concurrent DB queries.
func (h *Head) FlushOlderThanAndInstall(maxT int64, install func(string) error) (string, error) {
	return h.flushOlderThan(maxT, install)
}

func (h *Head) flushOlderThan(maxT int64, install func(string) error) (string, error) {
	var flushData []block.SeriesFlush
	var checkpointSeries []wal.SeriesRecord
	var checkpointSamples [][]wal.RefSample
	var snapshotErr error

	h.commitMu.Lock()
	defer h.commitMu.Unlock()

	h.series.forEach(func(s *memSeries) {
		if snapshotErr != nil {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()

		var toFlush []chunkMeta
		var remainingSamples []wal.RefSample
		for _, cm := range s.sealed {
			if cm.maxT <= maxT {
				toFlush = append(toFlush, cm)
			} else {
				remainingSamples, snapshotErr = appendCheckpointSamples(remainingSamples, s.ref, cm.chunk.Iterator())
			}
		}
		if s.chunk != nil && s.chunk.NumSamples() > 0 {
			remainingSamples, snapshotErr = appendCheckpointSamples(remainingSamples, s.ref, s.chunk.Iterator())
		}
		if snapshotErr != nil {
			return
		}
		if len(remainingSamples) > 0 || s.walLogged {
			checkpointSeries = append(checkpointSeries, wal.SeriesRecord{Ref: s.ref, Labels: s.labels})
		}
		if len(remainingSamples) > 0 {
			checkpointSamples = append(checkpointSamples, remainingSamples)
		}

		if len(toFlush) > 0 {
			sf := block.SeriesFlush{Ref: s.ref, Labels: s.labels}
			for _, cm := range toFlush {
				sf.Chunks = append(sf.Chunks, block.ChunkData{
					MinT: cm.minT,
					MaxT: cm.maxT,
					Data: append([]byte(nil), cm.chunk.Bytes()...),
				})
			}
			flushData = append(flushData, sf)
		}
	})
	if snapshotErr != nil {
		return "", fmt.Errorf("head: snapshot checkpoint: %w", snapshotErr)
	}

	if len(flushData) == 0 {
		return "", nil
	}
	sort.Slice(flushData, func(i, j int) bool { return flushData[i].Ref < flushData[j].Ref })
	sort.Slice(checkpointSeries, func(i, j int) bool { return checkpointSeries[i].Ref < checkpointSeries[j].Ref })

	prepared, err := block.PrepareFlush(h.dataDir, flushData)
	if err != nil {
		return "", fmt.Errorf("head: prepare block: %w", err)
	}

	checkpoint, err := h.wal.Checkpoint(prepared.ULID, checkpointSeries, checkpointSamples)
	if err != nil {
		return "", fmt.Errorf("head: write WAL checkpoint: %w", err)
	}
	if err := prepared.Publish(); err != nil {
		return "", fmt.Errorf("head: publish block: %w", err)
	}
	if err := h.wal.ActivateCheckpoint(checkpoint); err != nil {
		return prepared.ULID, fmt.Errorf("head: activate WAL checkpoint: %w", err)
	}
	if install != nil {
		if err := install(prepared.ULID); err != nil {
			return prepared.ULID, err
		}
	}

	// The published block makes the checkpoint authoritative. Remove the
	// flushed chunks in memory before cleaning up now-redundant WAL segments.
	h.series.forEach(func(s *memSeries) {
		s.mu.Lock()
		remaining := s.sealed[:0]
		for _, cm := range s.sealed {
			if cm.maxT > maxT {
				remaining = append(remaining, cm)
			}
		}
		s.sealed = remaining
		s.mu.Unlock()
	})
	if err := h.wal.Truncate(checkpoint.StartSegment); err != nil {
		return prepared.ULID, fmt.Errorf("head: truncate WAL: %w", err)
	}

	return prepared.ULID, nil
}

func appendCheckpointSamples(dst []wal.RefSample, ref uint64, it chunkenc.ChunkIterator) ([]wal.RefSample, error) {
	for it.Next() {
		t, v := it.At()
		dst = append(dst, wal.RefSample{Ref: ref, T: t, V: v})
	}
	return dst, it.Err()
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
	NumSeries       int
	NumActiveChunks int
}

// WALSyncDuration returns the duration of the most recent WAL fsync in seconds.
func (h *Head) WALSyncDuration() float64 {
	return h.wal.LastSyncDuration()
}
