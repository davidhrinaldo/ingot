// Package ingot is an embedded time-series database library for Go.
package ingot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidhrinaldo/ingot/internal/block"
	"github.com/davidhrinaldo/ingot/internal/chunkenc"
	"github.com/davidhrinaldo/ingot/internal/compact"
	"github.com/davidhrinaldo/ingot/internal/head"
	"github.com/davidhrinaldo/ingot/internal/postings"
	"github.com/davidhrinaldo/ingot/internal/wal"
	"github.com/davidhrinaldo/ingot/labels"
)

// Default compaction level durations in milliseconds.
var defaultLevels = []int64{
	2 * 3600 * 1000,  // 2h
	8 * 3600 * 1000,  // 8h
	32 * 3600 * 1000, // 32h
}

// DB is an embedded time-series database.
type DB struct {
	dataDir        string
	opts           Options
	head           *head.Head
	blocks         []*block.Reader // sorted by MinTime
	mu             sync.RWMutex    // protects blocks slice
	compactor      *compact.Compactor
	compactCtx     context.Context
	compactCancel  context.CancelFunc
	compactWg      sync.WaitGroup
	lifecycleMu    sync.Mutex // serializes flush, compaction, retention, and close
	maintenanceMu  sync.Mutex
	maintenanceErr error

	compactionCount atomic.Int64 // incremented on each successful compaction
	metricsR        metricsRefs  // cached series refs for self-instrumentation
}

// Options configures a DB.
type Options struct {
	Retention     time.Duration
	BlockDuration time.Duration
	// Clock returns the current time in milliseconds. Defaults to
	// time.Now().UnixMilli(). Injected for testing with simulated time.
	Clock func() int64
}

func (o *Options) clock() func() int64 {
	if o.Clock != nil {
		return o.Clock
	}
	return func() int64 { return time.Now().UnixMilli() }
}

func (o *Options) blockDurationMs() int64 {
	if o.BlockDuration == 0 {
		return defaultLevels[0] // 2h default
	}
	return o.BlockDuration.Milliseconds()
}

func (o *Options) retentionMs() int64 {
	return o.Retention.Milliseconds()
}

// Open opens or creates a DB at the given directory.
func Open(dataDir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("ingot: create data dir: %w", err)
	}
	if err := syncDirectory(filepath.Dir(dataDir)); err != nil {
		return nil, fmt.Errorf("ingot: sync data dir parent: %w", err)
	}

	walDir := filepath.Join(dataDir, "wal")
	h, err := head.Open(walDir, wal.Options{})
	if err != nil {
		return nil, fmt.Errorf("ingot: open head: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	db := &DB{
		dataDir:       dataDir,
		opts:          opts,
		head:          h,
		compactCtx:    ctx,
		compactCancel: cancel,
	}

	db.compactor = compact.New(dataDir, defaultLevels, opts.retentionMs(), opts.clock())

	if err := db.loadBlocks(); err != nil {
		cancel()
		h.Close()
		return nil, fmt.Errorf("ingot: load blocks: %w", err)
	}

	// Start background compaction goroutine.
	db.compactWg.Add(1)
	go db.compactLoop()

	return db, nil
}

// loadBlocks scans dataDir for block directories and opens them.
func (db *DB) loadBlocks() error {
	entries, err := os.ReadDir(db.dataDir)
	if err != nil {
		return err
	}

	var opened []*block.Reader
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
			return errors.Join(fmt.Errorf("open block %s: %w", e.Name(), err), closeBlockReaders(opened))
		}
		opened = append(opened, br)
	}

	active, replaced := reconcileBlockLineage(opened)
	var cleanupErr error
	for _, b := range replaced {
		dir := b.Dir()
		b.Condemn()
		if b.Release() {
			cleanupErr = errors.Join(cleanupErr, removeBlockDirectory(dir))
		}
	}
	if cleanupErr != nil {
		return errors.Join(fmt.Errorf("remove replaced blocks: %w", cleanupErr), closeBlockReaders(active))
	}

	sort.Slice(active, func(i, j int) bool {
		return active[i].Meta.MinTime < active[j].Meta.MinTime
	})
	db.blocks = active

	return nil
}

// reconcileBlockLineage keeps one maximal block for each source lineage.
// A compacted block replaces every block whose transitive sources are a subset
// of its sources.
func reconcileBlockLineage(blocks []*block.Reader) (active, replaced []*block.Reader) {
	byULID := make(map[string]*block.Reader, len(blocks))
	for _, b := range blocks {
		byULID[b.Meta.ULID] = b
	}

	lineages := make(map[string]map[string]struct{}, len(blocks))
	var lineage func(*block.Reader, map[string]bool) map[string]struct{}
	lineage = func(b *block.Reader, visiting map[string]bool) map[string]struct{} {
		if cached := lineages[b.Meta.ULID]; cached != nil {
			return cached
		}
		if visiting[b.Meta.ULID] {
			return map[string]struct{}{b.Meta.ULID: {}}
		}
		visiting[b.Meta.ULID] = true
		sources := b.Meta.Compaction.Sources
		if len(sources) == 0 {
			sources = []string{b.Meta.ULID}
		}
		result := make(map[string]struct{})
		for _, source := range sources {
			if source == b.Meta.ULID {
				result[source] = struct{}{}
				continue
			}
			if parent := byULID[source]; parent != nil {
				for original := range lineage(parent, visiting) {
					result[original] = struct{}{}
				}
				continue
			}
			result[source] = struct{}{}
		}
		delete(visiting, b.Meta.ULID)
		lineages[b.Meta.ULID] = result
		return result
	}

	for _, candidate := range blocks {
		candidateSources := lineage(candidate, make(map[string]bool))
		obsolete := false
		for _, replacement := range blocks {
			if candidate == replacement {
				continue
			}
			replacementSources := lineage(replacement, make(map[string]bool))
			if !sourceSubset(candidateSources, replacementSources) {
				continue
			}
			if len(candidateSources) < len(replacementSources) || candidate.Meta.ULID < replacement.Meta.ULID {
				obsolete = true
				break
			}
		}
		if obsolete {
			replaced = append(replaced, candidate)
		} else {
			active = append(active, candidate)
		}
	}
	return active, replaced
}

func sourceSubset(a, b map[string]struct{}) bool {
	if len(a) > len(b) {
		return false
	}
	for source := range a {
		if _, ok := b[source]; !ok {
			return false
		}
	}
	return true
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
			b.Ref()
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
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()

	return db.head.FlushOlderThanAndInstall(maxT, func(ulid string) error {
		br, err := block.Open(filepath.Join(db.dataDir, ulid))
		if err != nil {
			return fmt.Errorf("ingot: open flushed block: %w", err)
		}
		db.mu.Lock()
		db.blocks = append(db.blocks, br)
		sort.Slice(db.blocks, func(i, j int) bool {
			return db.blocks[i].Meta.MinTime < db.blocks[j].Meta.MinTime
		})
		db.mu.Unlock()
		return nil
	})
}

// RunCompaction performs a single compaction cycle. Exported for testing.
func (db *DB) RunCompaction() error {
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()

	db.mu.RLock()
	snapshot := make([]*block.Reader, len(db.blocks))
	copy(snapshot, db.blocks)
	db.mu.RUnlock()

	group := db.compactor.Plan(snapshot)
	if group == nil {
		return nil
	}

	newULID, err := db.compactor.Compact(group.Sources)
	if err != nil {
		var cleanupErr error
		if newULID != "" {
			cleanupErr = removeBlockDirectory(filepath.Join(db.dataDir, newULID))
		}
		return errors.Join(fmt.Errorf("ingot: compact: %w", err), cleanupErr)
	}

	newBlock, err := block.Open(filepath.Join(db.dataDir, newULID))
	if err != nil {
		cleanupErr := removeBlockDirectory(filepath.Join(db.dataDir, newULID))
		return errors.Join(fmt.Errorf("ingot: open compacted block: %w", err), cleanupErr)
	}

	// Build a set of source ULIDs for fast lookup.
	sourceSet := make(map[string]struct{}, len(group.Sources))
	for _, s := range group.Sources {
		sourceSet[s.Meta.ULID] = struct{}{}
	}

	// Swap blocks under short lock.
	db.mu.Lock()
	var remaining []*block.Reader
	for _, b := range db.blocks {
		if _, ok := sourceSet[b.Meta.ULID]; !ok {
			remaining = append(remaining, b)
		}
	}
	remaining = append(remaining, newBlock)
	sort.Slice(remaining, func(i, j int) bool {
		return remaining[i].Meta.MinTime < remaining[j].Meta.MinTime
	})
	db.blocks = remaining
	db.mu.Unlock()

	// Condemn and release source blocks.
	var cleanupErr error
	for _, src := range group.Sources {
		dir := src.Dir()
		src.Condemn()
		if src.Release() {
			cleanupErr = errors.Join(cleanupErr, removeBlockDirectory(dir))
		}
	}

	db.compactionCount.Add(1)
	if cleanupErr != nil {
		return fmt.Errorf("ingot: remove compacted source blocks: %w", cleanupErr)
	}
	return nil
}

// ApplyRetention drops blocks whose data is older than the retention window.
// Exported for testing.
func (db *DB) ApplyRetention() error {
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()

	if db.opts.Retention == 0 {
		return nil
	}

	db.mu.RLock()
	snapshot := make([]*block.Reader, len(db.blocks))
	copy(snapshot, db.blocks)
	db.mu.RUnlock()

	expired := db.compactor.Expired(snapshot)
	if len(expired) == 0 {
		return nil
	}

	expiredSet := make(map[string]struct{}, len(expired))
	for _, b := range expired {
		expiredSet[b.Meta.ULID] = struct{}{}
	}

	db.mu.Lock()
	var remaining []*block.Reader
	for _, b := range db.blocks {
		if _, ok := expiredSet[b.Meta.ULID]; !ok {
			remaining = append(remaining, b)
		}
	}
	db.blocks = remaining
	db.mu.Unlock()

	var cleanupErr error
	for _, b := range expired {
		dir := b.Dir()
		b.Condemn()
		if b.Release() {
			cleanupErr = errors.Join(cleanupErr, removeBlockDirectory(dir))
		}
	}
	if cleanupErr != nil {
		return fmt.Errorf("ingot: remove expired blocks: %w", cleanupErr)
	}
	return nil
}

// compactLoop runs in a background goroutine, periodically flushing the
// head and compacting blocks.
func (db *DB) compactLoop() {
	defer db.compactWg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-db.compactCtx.Done():
			return
		case <-ticker.C:
			db.collectMetrics()
			if err := db.autoFlush(); err != nil {
				db.recordMaintenanceError(err)
			}
			if err := db.RunCompaction(); err != nil {
				db.recordMaintenanceError(err)
			}
			if err := db.ApplyRetention(); err != nil {
				db.recordMaintenanceError(err)
			}
		}
	}
}

// autoFlush flushes sealed head chunks older than BlockDuration.
func (db *DB) autoFlush() error {
	now := db.opts.clock()()
	cutoff := now - db.opts.blockDurationMs()
	_, err := db.FlushOlderThan(cutoff)
	return err
}

func (db *DB) recordMaintenanceError(err error) {
	db.maintenanceMu.Lock()
	if db.maintenanceErr == nil {
		db.maintenanceErr = err
	}
	db.maintenanceMu.Unlock()
}

// DBStats holds summary statistics for the database.
type DBStats struct {
	HeadSeries  int
	HeadChunks  int
	Blocks      int
	Compactions int
}

// Stats returns a snapshot of database statistics.
func (db *DB) Stats() DBStats {
	hs := db.head.Stats()
	db.mu.RLock()
	numBlocks := len(db.blocks)
	db.mu.RUnlock()
	return DBStats{
		HeadSeries:  hs.NumSeries,
		HeadChunks:  hs.NumActiveChunks,
		Blocks:      numBlocks,
		Compactions: int(db.compactionCount.Load()),
	}
}

// Close closes the DB, releasing all resources.
func (db *DB) Close() error {
	db.compactCancel()
	db.compactWg.Wait()
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()

	var firstErr error
	if err := db.head.Close(); err != nil {
		firstErr = err
	}
	for _, b := range db.blocks {
		if err := b.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.maintenanceMu.Lock()
	maintenanceErr := db.maintenanceErr
	db.maintenanceMu.Unlock()
	return errors.Join(firstErr, maintenanceErr)
}

func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func removeBlockDirectory(dir string) error {
	removeErr := os.RemoveAll(dir)
	syncErr := syncDirectory(filepath.Dir(dir))
	if removeErr != nil {
		removeErr = fmt.Errorf("remove block directory %s: %w", dir, removeErr)
	}
	if syncErr != nil {
		syncErr = fmt.Errorf("sync block directory parent %s: %w", filepath.Dir(dir), syncErr)
	}
	return errors.Join(removeErr, syncErr)
}

func closeBlockReaders(blocks []*block.Reader) error {
	var err error
	for _, b := range blocks {
		err = errors.Join(err, b.Close())
	}
	return err
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

// Close releases block references held by this querier.
func (q *Querier) Close() error {
	var err error
	for _, b := range q.blocks {
		dir := b.Dir()
		if b.Release() {
			err = errors.Join(err, removeBlockDirectory(dir))
		}
	}
	q.blocks = nil
	return err
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
	iters   []chunkenc.ChunkIterator
	mint    int64
	maxt    int64
	cur     int
	lastT   int64
	curT    int64
	curV    float64
	started bool
	err     error
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

func (e *errIterator) Next() bool           { return false }
func (e *errIterator) At() (int64, float64) { return 0, 0 }
func (e *errIterator) Err() error           { return e.err }

// ensure interfaces are satisfied.
var (
	_ SeriesSet      = (*sliceSeriesSet)(nil)
	_ Series         = (*resultSeries)(nil)
	_ SampleIterator = (*mergedSampleIterator)(nil)
	_ SampleIterator = (*errIterator)(nil)
)
