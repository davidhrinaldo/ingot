package head

import (
	"errors"

	"github.com/davidhrinaldo/ingot/internal/wal"
	"github.com/davidhrinaldo/ingot/labels"
)

var (
	ErrOutOfOrder     = errors.New("head: out-of-order sample")
	ErrSeriesNotFound = errors.New("head: unknown series ref")
	ErrAppenderClosed = errors.New("head: appender already closed")
)

// Appender buffers samples and new series, then atomically commits them
// to the WAL and applies them to the head.
type Appender struct {
	head *Head

	// Unlogged series used by this batch, with their label hashes for cleanup.
	pendingSeries map[*memSeries]uint64

	// Buffered samples.
	samples []wal.RefSample

	// Last timestamp seen per series in this batch, for OOO rejection
	// of samples that haven't been committed yet.
	batchLastT map[uint64]int64

	closed bool
}

// Append adds a sample to the batch. If ref is 0, the series is resolved
// (or created) from ls. Returns the series ref for fast-path reuse.
func (a *Appender) Append(ref uint64, ls []labels.Label, t int64, v float64) (uint64, error) {
	if a.closed {
		return 0, ErrAppenderClosed
	}
	if ref == 0 {
		return a.appendByLabels(ls, t, v)
	}
	return a.appendByRef(ref, t, v)
}

func (a *Appender) appendByLabels(ls []labels.Label, t int64, v float64) (uint64, error) {
	ls = labels.Sort(ls)
	if err := labels.Validate(ls); err != nil {
		return 0, err
	}

	hash := labels.Hash(ls)
	a.head.commitMu.Lock()
	defer a.head.commitMu.Unlock()
	s := a.head.series.getByHash(hash, ls)

	if s == nil {
		// New series.
		ref := a.head.nextRef.Add(1)
		s = &memSeries{
			ref:    ref,
			labels: copyLabels(ls),
		}
		a.head.series.set(hash, s)
	}

	if err := a.checkTimestamp(s.ref, t); err != nil {
		return 0, err
	}

	a.samples = append(a.samples, wal.RefSample{Ref: s.ref, T: t, V: v})
	a.trackPendingSeries(s, hash)
	return s.ref, nil
}

func (a *Appender) appendByRef(ref uint64, t int64, v float64) (uint64, error) {
	a.head.commitMu.Lock()
	defer a.head.commitMu.Unlock()

	s := a.head.series.getByRef(ref)
	if s == nil {
		return 0, ErrSeriesNotFound
	}

	if err := a.checkTimestamp(ref, t); err != nil {
		return 0, err
	}

	a.samples = append(a.samples, wal.RefSample{Ref: ref, T: t, V: v})
	a.trackPendingSeries(s, labels.Hash(s.labels))
	return ref, nil
}

// checkTimestamp validates that t is strictly after the last sample for
// this series, both in committed state and in the current batch.
func (a *Appender) checkTimestamp(ref uint64, t int64) error {
	// Check committed state.
	s := a.head.series.getByRef(ref)
	if s != nil {
		s.mu.Lock()
		lastT, hasData := s.lastT, s.hasData
		s.mu.Unlock()
		if hasData && t <= lastT {
			return ErrOutOfOrder
		}
	}
	// Check batch state.
	if a.batchLastT == nil {
		a.batchLastT = make(map[uint64]int64)
	}
	if prev, ok := a.batchLastT[ref]; ok && t <= prev {
		return ErrOutOfOrder
	}
	a.batchLastT[ref] = t
	return nil
}

// Commit writes the batch to the WAL, then applies it to the head.
func (a *Appender) Commit() error {
	if a.closed {
		return ErrAppenderClosed
	}
	a.closed = true
	a.head.commitMu.Lock()
	defer a.head.commitMu.Unlock()
	defer a.releasePendingSeries()

	if err := a.validateBatch(); err != nil {
		return err
	}

	// A series may have been created by another appender that has not committed.
	// Log unresolved definitions here so samples never precede their series.
	seen := make(map[uint64]struct{}, len(a.samples))
	for _, sample := range a.samples {
		if _, ok := seen[sample.Ref]; ok {
			continue
		}
		seen[sample.Ref] = struct{}{}
		s := a.head.series.getByRef(sample.Ref)
		if s != nil && !s.walLogged {
			if err := a.head.wal.LogSeries([]wal.SeriesRecord{{Ref: s.ref, Labels: s.labels}}); err != nil {
				return err
			}
			s.walLogged = true
		}
	}
	if len(a.samples) > 0 {
		if err := a.head.wal.LogSamples(a.samples); err != nil {
			return err
		}
	}

	// Apply samples to head.
	a.head.applyMu.Lock()
	for _, s := range a.samples {
		a.head.applySample(s.Ref, s.T, s.V)
	}
	a.head.applyMu.Unlock()

	return nil
}

// validateBatch checks committed timestamps again while commits are serialized.
func (a *Appender) validateBatch() error {
	lastT := make(map[uint64]int64)
	for _, sample := range a.samples {
		if prev, ok := lastT[sample.Ref]; ok {
			if sample.T <= prev {
				return ErrOutOfOrder
			}
			lastT[sample.Ref] = sample.T
			continue
		}

		s := a.head.series.getByRef(sample.Ref)
		if s == nil {
			return ErrSeriesNotFound
		}
		s.mu.Lock()
		committedT, hasData := s.lastT, s.hasData
		s.mu.Unlock()
		if hasData && sample.T <= committedT {
			return ErrOutOfOrder
		}
		lastT[sample.Ref] = sample.T
	}
	return nil
}

// Rollback discards the batch. Unlogged series are removed when their last
// participating appender closes.
func (a *Appender) Rollback() error {
	if a.closed {
		return ErrAppenderClosed
	}
	a.closed = true
	a.head.commitMu.Lock()
	defer a.head.commitMu.Unlock()

	a.releasePendingSeries()
	a.samples = nil
	return nil
}

func (a *Appender) trackPendingSeries(s *memSeries, hash uint64) {
	if s.walLogged {
		return
	}
	if a.pendingSeries == nil {
		a.pendingSeries = make(map[*memSeries]uint64)
	}
	if _, ok := a.pendingSeries[s]; ok {
		return
	}
	a.pendingSeries[s] = hash
	s.pendingAppenders++
}

// releasePendingSeries drops this appender's claims and removes an unlogged
// series after its last participating appender closes.
func (a *Appender) releasePendingSeries() {
	for s, hash := range a.pendingSeries {
		s.pendingAppenders--
		if s.pendingAppenders != 0 || s.walLogged {
			continue
		}
		s.mu.Lock()
		hasData := s.hasData
		s.mu.Unlock()
		if !hasData {
			a.head.series.remove(hash, s)
		}
	}
	a.pendingSeries = nil
}

func copyLabels(ls []labels.Label) []labels.Label {
	c := make([]labels.Label, len(ls))
	copy(c, ls)
	return c
}
