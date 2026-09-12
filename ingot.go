// Package ingot is an embedded time-series database library for Go.
package ingot

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// ErrClosed indicates that a lifecycle operation was rejected because Close
// has started.
var ErrClosed = errors.New("ingot: database is closing or closed")

const retentionTombstoneDir = ".retention"

// DB is an embedded time-series database.
type DB struct {
	dataDir        string
	opts           Options
	head           *head.Head
	blocks         []*block.Reader // sorted by MinTime, then ULID
	mu             sync.RWMutex    // protects blocks slice
	compactor      *compact.Compactor
	compactCtx     context.Context
	compactCancel  context.CancelFunc
	compactWg      sync.WaitGroup
	lifecycleMu    sync.Mutex // serializes flush, compaction, retention, and close
	closing        bool
	closed         bool
	closeDone      chan struct{}
	closeErr       error
	retentionRetry map[string][]string
	removeBlockDir func(string) error
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
	// SyncPolicy controls WAL fsync behavior. The zero value, SyncOnCommit,
	// makes a successful Commit durable before it returns.
	SyncPolicy SyncPolicy
	// SyncInterval controls fsync frequency for SyncPeriodic. Zero uses 1s.
	SyncInterval time.Duration
}

// SyncPolicy controls when committed WAL records are fsynced.
type SyncPolicy uint8

const (
	// SyncOnCommit fsyncs each appender batch before Commit returns. This is
	// the default.
	SyncOnCommit SyncPolicy = iota
	// SyncPeriodic fsyncs in the background. A process or machine crash may
	// lose commits made since the last successful background fsync.
	SyncPeriodic
)

func (o *Options) walOptions() (wal.Options, error) {
	switch o.SyncPolicy {
	case SyncOnCommit:
		if o.SyncInterval != 0 {
			return wal.Options{}, fmt.Errorf("SyncInterval requires SyncPeriodic")
		}
		return wal.Options{SyncPolicy: wal.SyncOnCommit}, nil
	case SyncPeriodic:
		if o.SyncInterval < 0 {
			return wal.Options{}, fmt.Errorf("SyncInterval must not be negative")
		}
		interval := o.SyncInterval
		if interval == 0 {
			interval = time.Second
		}
		return wal.Options{SyncPolicy: wal.SyncPeriodic, SyncInterval: interval}, nil
	default:
		return wal.Options{}, fmt.Errorf("invalid SyncPolicy %d", o.SyncPolicy)
	}
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
	walOpts, err := opts.walOptions()
	if err != nil {
		return nil, fmt.Errorf("ingot: %w", err)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("ingot: create data dir: %w", err)
	}
	if err := syncDirectory(filepath.Dir(dataDir)); err != nil {
		return nil, fmt.Errorf("ingot: sync data dir parent: %w", err)
	}

	walDir := filepath.Join(dataDir, "wal")
	h, err := head.Open(walDir, walOpts)
	if err != nil {
		return nil, fmt.Errorf("ingot: open head: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	db := &DB{
		dataDir:        dataDir,
		opts:           opts,
		head:           h,
		compactCtx:     ctx,
		compactCancel:  cancel,
		closeDone:      make(chan struct{}),
		retentionRetry: make(map[string][]string),
		removeBlockDir: removeBlockDirectory,
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
	tombstones, err := db.loadRetentionTombstones()
	if err != nil {
		return fmt.Errorf("load retention tombstones: %w", err)
	}
	var retryErr error
	blocked := make(map[string]struct{})
	for marker, names := range tombstones {
		if err := db.finishRetentionDelete(marker, names); err != nil {
			db.retentionRetry[marker] = names
			for _, name := range names {
				blocked[name] = struct{}{}
			}
			retryErr = errors.Join(retryErr, err)
		}
	}
	if retryErr != nil {
		db.recordMaintenanceError(fmt.Errorf("retry retained block deletion: %w", retryErr))
	}

	entries, err := os.ReadDir(db.dataDir)
	if err != nil {
		return err
	}

	var opened []*block.Reader
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "wal" {
			continue
		}
		if _, tombstoned := blocked[e.Name()]; tombstoned {
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
	if len(replaced) > 0 {
		for _, replacement := range active {
			if err := validateReferencedChunks(replacement); err != nil {
				return errors.Join(fmt.Errorf("validate replacement block %s: %w", replacement.Meta.ULID, err), closeBlockReaders(opened))
			}
		}
	}
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

	sortBlockReaders(active)
	db.blocks = active

	return nil
}

// reconcileBlockLineage keeps one maximal block for each source lineage.
// A compacted block replaces every block whose transitive sources are a subset
// of its sources. Legacy blocks with immediate-only lineage cannot be traced
// through an intermediate block that was already deleted.
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

func validateReferencedChunks(b *block.Reader) error {
	for _, series := range b.Series() {
		for _, chunk := range series.Chunks {
			if _, err := b.RawChunkData(chunk.Ref); err != nil {
				return fmt.Errorf("read chunk ref %v: %w", chunk.Ref, err)
			}
		}
	}
	return nil
}

// Appender returns a new Appender for batching writes.
func (db *DB) Appender() *Appender {
	return &Appender{inner: db.head.Appender()}
}

// Querier returns a Querier over [mint, maxt].
func (db *DB) Querier(mint, maxt int64) (*Querier, error) {
	var overlapping []*block.Reader
	headSnapshot := db.head.Snapshot(func() {
		db.mu.RLock()
		defer db.mu.RUnlock()
		for _, b := range db.blocks {
			if b.Meta.MaxTime >= mint && b.Meta.MinTime <= maxt {
				b.Ref()
				overlapping = append(overlapping, b)
			}
		}
	})

	return &Querier{
		mint:   mint,
		maxt:   maxt,
		head:   headSnapshot,
		blocks: overlapping,
	}, nil
}

// FlushOlderThan flushes sealed head chunks to an immutable block.
func (db *DB) FlushOlderThan(maxT int64) (string, error) {
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()
	if db.closing || db.closed {
		return "", ErrClosed
	}

	return db.head.FlushOlderThanAndInstall(maxT, func(ulid string) error {
		br, err := block.Open(filepath.Join(db.dataDir, ulid))
		if err != nil {
			return fmt.Errorf("ingot: open flushed block: %w", err)
		}
		db.mu.Lock()
		db.blocks = append(db.blocks, br)
		sortBlockReaders(db.blocks)
		db.mu.Unlock()
		return nil
	})
}

// RunCompaction performs a single compaction cycle. Exported for testing.
func (db *DB) RunCompaction() error {
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()
	if db.closing || db.closed {
		return ErrClosed
	}

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
	sortBlockReaders(remaining)
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
// Errors are retained and returned by Close. Use RunRetention to receive an
// immediate error.
func (db *DB) ApplyRetention() {
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()
	if db.closing || db.closed {
		return
	}
	if err := db.runRetentionLocked(); err != nil {
		db.recordMaintenanceError(err)
	}
}

// RunRetention applies retention and returns any persistence or deletion error.
func (db *DB) RunRetention() error {
	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()
	if db.closing || db.closed {
		return ErrClosed
	}
	return db.runRetentionLocked()
}

func (db *DB) runRetentionLocked() error {
	var retryErr error
	for marker, names := range db.retentionRetry {
		if err := db.finishRetentionDelete(marker, names); err != nil {
			retryErr = errors.Join(retryErr, err)
			continue
		}
		delete(db.retentionRetry, marker)
	}

	if db.opts.Retention == 0 {
		return retryErr
	}

	db.mu.RLock()
	snapshot := make([]*block.Reader, len(db.blocks))
	copy(snapshot, db.blocks)
	db.mu.RUnlock()

	expired := db.compactor.Expired(snapshot)
	if len(expired) == 0 {
		return retryErr
	}

	expiredSet := make(map[string]struct{}, len(expired))
	tombstones := make(map[string][]string, len(expired))
	for _, b := range expired {
		expiredSet[b.Meta.ULID] = struct{}{}
		names := map[string]struct{}{filepath.Base(b.Dir()): {}}
		for _, source := range b.Meta.Compaction.Sources {
			if source == "" || source == "." || source == ".." || filepath.IsAbs(source) || filepath.Base(source) != source {
				return errors.Join(retryErr, fmt.Errorf("ingot: invalid retention source %q", source))
			}
			names[source] = struct{}{}
		}
		blockNames := make([]string, 0, len(names))
		for name := range names {
			blockNames = append(blockNames, name)
		}
		sort.Strings(blockNames)
		marker := filepath.Base(b.Dir())
		if err := db.writeRetentionTombstone(marker, blockNames); err != nil {
			return errors.Join(retryErr, fmt.Errorf("ingot: persist retention tombstone for %s: %w", b.Meta.ULID, err))
		}
		tombstones[marker] = blockNames
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

	for _, b := range expired {
		b.Condemn()
		b.Release()
	}
	// Querier references keep mapped readers alive after POSIX unlink. The
	// manifest must cover source directories even when those readers are pinned.
	var cleanupErr error
	for marker, names := range tombstones {
		if err := db.finishRetentionDelete(marker, names); err != nil {
			db.retentionRetry[marker] = names
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if cleanupErr != nil {
		cleanupErr = fmt.Errorf("ingot: remove expired blocks: %w", cleanupErr)
	}
	return errors.Join(retryErr, cleanupErr)
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
				if errors.Is(err, ErrClosed) {
					return
				}
				db.recordMaintenanceError(err)
			}
			if err := db.RunCompaction(); err != nil {
				if errors.Is(err, ErrClosed) {
					return
				}
				db.recordMaintenanceError(err)
			}
			if err := db.RunRetention(); err != nil {
				if errors.Is(err, ErrClosed) {
					return
				}
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
	db.lifecycleMu.Lock()
	if db.closing || db.closed {
		done := db.closeDone
		db.lifecycleMu.Unlock()
		<-done
		db.lifecycleMu.Lock()
		err := db.closeErr
		db.lifecycleMu.Unlock()
		return err
	}
	db.closing = true
	db.lifecycleMu.Unlock()

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
	db.closeErr = errors.Join(firstErr, maintenanceErr)
	db.closed = true
	close(db.closeDone)
	return db.closeErr
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

func (db *DB) loadRetentionTombstones() (map[string][]string, error) {
	dir := filepath.Join(db.dataDir, retentionTombstoneDir)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	tombstones := make(map[string][]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".tmp-") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		names := strings.Fields(string(data))
		if len(names) == 0 {
			return nil, fmt.Errorf("empty retention tombstone %s", entry.Name())
		}
		for _, name := range names {
			if name == "." || name == ".." || filepath.IsAbs(name) || filepath.Base(name) != name {
				return nil, fmt.Errorf("invalid block name %q in retention tombstone %s", name, entry.Name())
			}
		}
		tombstones[entry.Name()] = names
	}
	return tombstones, nil
}

func (db *DB) writeRetentionTombstone(marker string, names []string) error {
	dir := filepath.Join(db.dataDir, retentionTombstoneDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := syncDirectory(db.dataDir); err != nil {
		return err
	}
	path := filepath.Join(dir, marker)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err := f.WriteString(strings.Join(names, "\n") + "\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func (db *DB) finishRetentionDelete(marker string, names []string) error {
	var deleteErr error
	for _, name := range names {
		if err := db.removeBlockDir(filepath.Join(db.dataDir, name)); err != nil {
			deleteErr = errors.Join(deleteErr, err)
		}
	}
	if deleteErr != nil {
		return deleteErr
	}
	markerPath := filepath.Join(db.dataDir, retentionTombstoneDir, marker)
	err := os.Remove(markerPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove retention tombstone %s: %w", markerPath, err)
	}
	if err := syncDirectory(filepath.Dir(markerPath)); err != nil {
		return fmt.Errorf("sync retention tombstone directory: %w", err)
	}
	return nil
}

func closeBlockReaders(blocks []*block.Reader) error {
	var err error
	for _, b := range blocks {
		err = errors.Join(err, b.Close())
	}
	return err
}

func sortBlockReaders(readers []*block.Reader) {
	sort.Slice(readers, func(i, j int) bool {
		if readers[i].Meta.MinTime != readers[j].Meta.MinTime {
			return readers[i].Meta.MinTime < readers[j].Meta.MinTime
		}
		return readers[i].Meta.ULID < readers[j].Meta.ULID
	})
}

// Appender buffers samples and new series for atomic commit.
type Appender struct {
	inner *head.Appender
}

// Append adds a sample. If ref is 0, the series is resolved (or created) from ls.
func (a *Appender) Append(ref uint64, ls []labels.Label, t int64, v float64) (uint64, error) {
	return a.inner.Append(ref, ls, t, v)
}

// Commit writes the batch to the WAL according to the configured sync policy,
// then applies it to the head. With the default SyncOnCommit policy, success
// means the batch is durable on disk.
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
	head       *head.Snapshot
	blocks     []*block.Reader
}

// Select returns a SeriesSet matching the given matchers. A nil matcher,
// unsupported match type, or uninitialized regexp matcher produces an empty set.
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

// Close releases snapshot references held by this querier.
func (q *Querier) Close() error {
	var err error
	for _, b := range q.blocks {
		dir := b.Dir()
		if b.Release() {
			err = errors.Join(err, removeBlockDirectory(dir))
		}
	}
	q.blocks = nil
	q.head = nil
	return err
}

func resolveBlockPostings(b *block.Reader, matchers []*labels.Matcher) []uint64 {
	return resolvePostings(b, matchers)
}

func resolveHeadPostings(h *head.Snapshot, matchers []*labels.Matcher) []uint64 {
	return resolvePostings(h, matchers)
}

type labelPostings interface {
	Postings(name, value string) []uint64
	LabelValues(name string) []string
	AllPostings() []uint64
}

func resolvePostings(src labelPostings, matchers []*labels.Matcher) []uint64 {
	if len(matchers) == 0 {
		return src.AllPostings()
	}
	all := src.AllPostings()
	var lists [][]uint64
	for _, m := range matchers {
		if m == nil {
			return nil
		}
		switch m.Type {
		case labels.MatchEqual, labels.MatchNotEqual, labels.MatchRegexp, labels.MatchNotRegexp:
		default:
			return nil
		}

		var present, matching [][]uint64
		for _, value := range src.LabelValues(m.Name) {
			refs := src.Postings(m.Name, value)
			present = append(present, refs)
			if m.Matches(value) {
				matching = append(matching, refs)
			}
		}
		refs := postings.Union(matching...)
		if m.Matches("") {
			absent := postings.Without(all, postings.Union(present...))
			refs = postings.Union(refs, absent)
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
	return append([]labels.Label(nil), s.labels...)
}

func (s *resultSeries) Iterator() SampleIterator {
	var iters []chunkenc.ChunkIterator

	// Blocks first in MinTime and ULID order, then head. Earlier sources win
	// duplicates for the current block set; only block-over-head precedence is
	// stable when compaction replaces blocks.
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

// mergedSampleIterator performs a timestamp merge across ChunkIterators and
// deduplicates timestamps. Earlier iterators win over later ones.
type mergedSampleIterator struct {
	iters       []chunkenc.ChunkIterator
	mint        int64
	maxt        int64
	heap        sampleIteratorHeap
	curT        int64
	curV        float64
	initialized bool
	err         error
}

func (m *mergedSampleIterator) Next() bool {
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

	next := heap.Pop(&m.heap).(sampleIteratorHead)
	m.curT, m.curV = next.t, next.v
	m.advance(next.source)

	// Consume every lower-precedence copy of this timestamp. advance may push
	// another copy from the same source, so inspect the heap after each push.
	for len(m.heap) > 0 && m.heap[0].t == m.curT {
		duplicate := heap.Pop(&m.heap).(sampleIteratorHead)
		m.advance(duplicate.source)
	}
	return true
}

func (m *mergedSampleIterator) advance(source int) {
	it := m.iters[source]
	for it.Next() {
		t, v := it.At()
		if t < m.mint {
			continue
		}
		if t > m.maxt {
			return
		}
		heap.Push(&m.heap, sampleIteratorHead{source: source, t: t, v: v})
		return
	}
	if err := it.Err(); err != nil {
		m.err = err
	}
}

func (m *mergedSampleIterator) At() (int64, float64) {
	return m.curT, m.curV
}

func (m *mergedSampleIterator) Err() error {
	return m.err
}

type sampleIteratorHead struct {
	source int
	t      int64
	v      float64
}

type sampleIteratorHeap []sampleIteratorHead

func (h sampleIteratorHeap) Len() int { return len(h) }

func (h sampleIteratorHeap) Less(i, j int) bool {
	if h[i].t != h[j].t {
		return h[i].t < h[j].t
	}
	return h[i].source < h[j].source
}

func (h sampleIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *sampleIteratorHeap) Push(x any) {
	*h = append(*h, x.(sampleIteratorHead))
}

func (h *sampleIteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
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
