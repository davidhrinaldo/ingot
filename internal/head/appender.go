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

	// New series created during this batch (not yet in WAL).
	newSeries []wal.SeriesRecord
	newHashes []uint64 // parallel to newSeries, for rollback

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
		a.newSeries = append(a.newSeries, wal.SeriesRecord{Ref: ref, Labels: s.labels})
		a.newHashes = append(a.newHashes, hash)
	} else if !s.walLogged && !a.ownsSeries(s.ref) {
		s.sharedPending = true
	}

	if err := a.checkTimestamp(s.ref, t); err != nil {
		return 0, err
	}

	a.samples = append(a.samples, wal.RefSample{Ref: s.ref, T: t, V: v})
	return s.ref, nil
}

func (a *Appender) appendByRef(ref uint64, t int64, v float64) (uint64, error) {
	a.head.commitMu.Lock()
	s := a.head.series.getByRef(ref)
	if s == nil {
		a.head.commitMu.Unlock()
		return 0, ErrSeriesNotFound
	}
	if !s.walLogged && !a.ownsSeries(ref) {
		s.sharedPending = true
	}
	a.head.commitMu.Unlock()

	if err := a.checkTimestamp(ref, t); err != nil {
		return 0, err
	}

	a.samples = append(a.samples, wal.RefSample{Ref: ref, T: t, V: v})
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
	if err := a.head.wal.Commit(); err != nil {
		return err
	}

	// Apply samples to head.
	for _, s := range a.samples {
		a.head.applySample(s.Ref, s.T, s.V)
	}

	return nil
}

// Rollback discards the batch. Any new series created during this batch
// that have no data are removed.
func (a *Appender) Rollback() error {
	if a.closed {
		return ErrAppenderClosed
	}
	a.closed = true
	a.head.commitMu.Lock()
	defer a.head.commitMu.Unlock()

	// Remove new series that were registered but never committed.
	for i, rec := range a.newSeries {
		s := a.head.series.getByRef(rec.Ref)
		if s == nil {
			continue
		}
		s.mu.Lock()
		hasData := s.hasData
		s.mu.Unlock()
		if !hasData && !s.walLogged && !s.sharedPending {
			a.head.series.remove(a.newHashes[i], s)
		}
	}

	a.newSeries = nil
	a.newHashes = nil
	a.samples = nil
	return nil
}

func (a *Appender) ownsSeries(ref uint64) bool {
	for _, rec := range a.newSeries {
		if rec.Ref == ref {
			return true
		}
	}
	return false
}

func copyLabels(ls []labels.Label) []labels.Label {
	c := make([]labels.Label, len(ls))
	copy(c, ls)
	return c
}
