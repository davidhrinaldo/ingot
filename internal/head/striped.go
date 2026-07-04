package head

import (
	"sync"

	"git.dvdt.dev/david/ingot/labels"
)

const numStripes = 128

// seriesMap is a concurrent map of series, striped by label hash for hash
// lookups and by ref for ref lookups.
type seriesMap struct {
	hashStripes [numStripes]hashStripe
	refStripes  [numStripes]refStripe
}

type hashStripe struct {
	mu sync.RWMutex
	m  map[uint64][]*memSeries // label hash -> series (slice for collision)
}

type refStripe struct {
	mu sync.RWMutex
	m  map[uint64]*memSeries
}

func newSeriesMap() *seriesMap {
	sm := &seriesMap{}
	for i := range sm.hashStripes {
		sm.hashStripes[i].m = make(map[uint64][]*memSeries)
	}
	for i := range sm.refStripes {
		sm.refStripes[i].m = make(map[uint64]*memSeries)
	}
	return sm
}

// getByRef returns the series with the given ref, or nil.
func (sm *seriesMap) getByRef(ref uint64) *memSeries {
	s := &sm.refStripes[ref%numStripes]
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m[ref]
}

// getByHash returns the series with the given hash and labels, or nil.
func (sm *seriesMap) getByHash(hash uint64, ls []labels.Label) *memSeries {
	s := &sm.hashStripes[hash%numStripes]
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, series := range s.m[hash] {
		if labels.Equal(series.labels, ls) {
			return series
		}
	}
	return nil
}

// set inserts a series into both maps. Caller must ensure the ref is unique.
func (sm *seriesMap) set(hash uint64, s *memSeries) {
	hs := &sm.hashStripes[hash%numStripes]
	hs.mu.Lock()
	hs.m[hash] = append(hs.m[hash], s)
	hs.mu.Unlock()

	rs := &sm.refStripes[s.ref%numStripes]
	rs.mu.Lock()
	rs.m[s.ref] = s
	rs.mu.Unlock()
}

// remove deletes a series from both maps.
func (sm *seriesMap) remove(hash uint64, s *memSeries) {
	hs := &sm.hashStripes[hash%numStripes]
	hs.mu.Lock()
	bucket := hs.m[hash]
	for i, existing := range bucket {
		if existing == s {
			hs.m[hash] = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}
	if len(hs.m[hash]) == 0 {
		delete(hs.m, hash)
	}
	hs.mu.Unlock()

	rs := &sm.refStripes[s.ref%numStripes]
	rs.mu.Lock()
	delete(rs.m, s.ref)
	rs.mu.Unlock()
}
