package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// SyncPolicy controls when WAL writes are made durable.
type SyncPolicy uint8

const (
	// SyncOnCommit fsyncs the WAL when a commit completes.
	SyncOnCommit SyncPolicy = iota
	// SyncPeriodic fsyncs the WAL in a background goroutine.
	SyncPeriodic
)

// Options configures WAL behavior.
type Options struct {
	// SegmentMaxSize is the maximum size of a single segment file in bytes.
	// A new segment is created when the current one would exceed this.
	// Default: 128 MiB.
	SegmentMaxSize int

	// SyncPolicy controls whether commits or a background goroutine fsync.
	// The default is SyncOnCommit.
	SyncPolicy SyncPolicy

	// SyncInterval controls background fsync frequency when SyncPolicy is
	// SyncPeriodic. The default is 1s.
	SyncInterval time.Duration

	// sync is overridden by tests that exercise fsync failures.
	sync func(*os.File) error
}

func (o *Options) segmentMaxSize() int {
	if o.SegmentMaxSize > 0 {
		return o.SegmentMaxSize
	}
	return defaultSegmentMaxSize
}

func (o *Options) syncInterval() time.Duration {
	if o.SyncPolicy != SyncPeriodic {
		return 0
	}
	if o.SyncInterval <= 0 {
		return time.Second
	}
	return o.SyncInterval
}

func (o *Options) validate() error {
	if o.SyncPolicy != SyncOnCommit && o.SyncPolicy != SyncPeriodic {
		return fmt.Errorf("wal: invalid sync policy %d", o.SyncPolicy)
	}
	if o.SyncInterval < 0 {
		return fmt.Errorf("wal: sync interval must not be negative")
	}
	if o.SyncPolicy == SyncOnCommit && o.SyncInterval != 0 {
		return fmt.Errorf("wal: sync interval requires periodic sync policy")
	}
	return nil
}

func (o *Options) fsync(f *os.File) error {
	if o.sync != nil {
		return o.sync(f)
	}
	return f.Sync()
}

// WAL is a segmented write-ahead log.
type WAL struct {
	dir  string
	opts Options

	mu         sync.Mutex
	segment    *os.File
	segmentIdx int
	segmentOff int64
	buf        []byte
	syncErr    error

	lastSyncDur atomic.Int64 // nanoseconds of last fsync

	done chan struct{}
	wg   sync.WaitGroup
}

// Open opens or creates a WAL in dir. Recovery repairs an incomplete final
// segment and rejects corruption in any closed segment.
func Open(dir string, opts Options) (*WAL, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return nil, err
	}

	// Recover existing segments.
	if err := recover(dir); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:  dir,
		opts: opts,
		done: make(chan struct{}),
	}

	// Open or create the active segment.
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	if len(segs) == 0 {
		// Fresh WAL.
		w.segmentIdx = 1
		f, err := createSegment(dir, 1)
		if err != nil {
			return nil, err
		}
		w.segment = f
		if err := syncDir(dir); err != nil {
			f.Close()
			return nil, err
		}
	} else {
		// Append to the last segment.
		idx := segs[len(segs)-1]
		w.segmentIdx = idx
		f, err := os.OpenFile(segmentPath(dir, idx), os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		w.segment = f
		w.segmentOff = info.Size()
	}

	// Start background syncer.
	if interval := opts.syncInterval(); interval > 0 {
		w.wg.Add(1)
		go w.syncLoop(interval)
	}

	return w, nil
}

// Log writes a framed record to the WAL. The payload is wrapped with
// the record envelope (type + length + CRC).
func (w *WAL) Log(typ RecordType, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return fmt.Errorf("wal: previous fsync failed: %w", w.syncErr)
	}

	w.buf = EncodeRecord(w.buf[:0], typ, payload)

	// Rotate if this write would exceed the segment size limit.
	if w.segmentOff+int64(len(w.buf)) > int64(w.opts.segmentMaxSize()) {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	n, err := w.segment.Write(w.buf)
	w.segmentOff += int64(n)
	if err != nil {
		return err
	}

	return nil
}

// LogSeries encodes and writes a series record.
func (w *WAL) LogSeries(recs []SeriesRecord) error {
	for _, rec := range recs {
		payload := EncodeSeriesRecord(nil, rec)
		if err := w.Log(RecordSeries, payload); err != nil {
			return err
		}
	}
	return nil
}

// LogSamples encodes and writes a samples record.
func (w *WAL) LogSamples(samples []RefSample) error {
	payload := EncodeSamplesRecord(nil, samples)
	return w.Log(RecordSamples, payload)
}

// Checkpoint writes and fsyncs a complete snapshot of the live head into new
// WAL segments. Older segments must remain in place until the block identified
// by blockULID has been published durably.
func (w *WAL) Checkpoint(blockULID string, series []SeriesRecord, samples [][]RefSample) (Checkpoint, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return Checkpoint{}, fmt.Errorf("wal: previous fsync failed: %w", w.syncErr)
	}

	if err := w.rotate(); err != nil {
		return Checkpoint{}, err
	}
	startSegment := w.segmentIdx
	cp := Checkpoint{StartSegment: startSegment, BlockULID: blockULID}
	marker := EncodeCheckpoint(nil, cp)
	if err := w.logLocked(RecordCheckpointBegin, marker); err != nil {
		return Checkpoint{}, err
	}
	for _, rec := range series {
		if err := w.logLocked(RecordCheckpointSeries, EncodeSeriesRecord(nil, rec)); err != nil {
			return Checkpoint{}, err
		}
	}
	for _, batch := range samples {
		if len(batch) == 0 {
			continue
		}
		if err := w.logLocked(RecordCheckpointSamples, EncodeSamplesRecord(nil, batch)); err != nil {
			return Checkpoint{}, err
		}
	}
	if err := w.logLocked(RecordCheckpointCommit, marker); err != nil {
		return Checkpoint{}, err
	}
	if err := w.timedSync(); err != nil {
		return Checkpoint{}, err
	}
	if err := syncDir(w.dir); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}

// ActivateCheckpoint records that the checkpoint's block was published. Once
// this record is durable, recovery does not depend on the source block existing.
func (w *WAL) ActivateCheckpoint(cp Checkpoint) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return fmt.Errorf("wal: previous fsync failed: %w", w.syncErr)
	}

	if err := w.logLocked(RecordCheckpointActivate, EncodeCheckpoint(nil, cp)); err != nil {
		return err
	}
	return w.timedSync()
}

// Commit completes the current WAL commit according to the sync policy.
func (w *WAL) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return fmt.Errorf("wal: previous fsync failed: %w", w.syncErr)
	}
	if w.opts.SyncPolicy == SyncOnCommit {
		return w.timedSync()
	}
	return nil
}

// Replay returns a Reader over all WAL segments. The caller must Close
// the reader when done.
func (w *WAL) Replay() (*Reader, error) {
	return NewReader(w.dir)
}

// Sync forces an fsync of the current segment.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		return fmt.Errorf("wal: previous fsync failed: %w", w.syncErr)
	}
	return w.timedSync()
}

// LastSyncDuration returns the duration of the most recent fsync in seconds.
func (w *WAL) LastSyncDuration() float64 {
	ns := w.lastSyncDur.Load()
	return float64(ns) / 1e9
}

// timedSync fsyncs the segment and records the duration. Caller must hold w.mu.
func (w *WAL) timedSync() error {
	start := time.Now()
	err := w.opts.fsync(w.segment)
	w.lastSyncDur.Store(int64(time.Since(start)))
	if err != nil && w.syncErr == nil {
		w.syncErr = err
	}
	return err
}

// Truncate deletes all segments with index less than below.
func (w *WAL) Truncate(below int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}

	for _, idx := range segs {
		if idx >= below {
			break
		}
		if err := os.Remove(segmentPath(w.dir, idx)); err != nil {
			return err
		}
	}
	return syncDir(w.dir)
}

// LastSegment returns the index of the current active segment.
func (w *WAL) LastSegment() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentIdx
}

// Close stops the background syncer, fsyncs, and closes the active segment.
func (w *WAL) Close() error {
	close(w.done)
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.segment == nil {
		return w.syncErr
	}
	previousSyncErr := w.syncErr
	syncErr := w.timedSync()
	closeErr := w.segment.Close()
	w.segment = nil
	return errors.Join(previousSyncErr, syncErr, closeErr)
}

// rotate fsyncs the current segment, closes it, and creates a new one.
// Caller must hold w.mu.
func (w *WAL) rotate() error {
	if err := w.timedSync(); err != nil {
		return err
	}
	if err := w.segment.Close(); err != nil {
		return err
	}

	w.segmentIdx++
	f, err := createSegment(w.dir, w.segmentIdx)
	if err != nil {
		return err
	}
	w.segment = f
	w.segmentOff = 0
	return syncDir(w.dir)
}

// logLocked writes a record while w.mu is held.
func (w *WAL) logLocked(typ RecordType, payload []byte) error {
	w.buf = EncodeRecord(w.buf[:0], typ, payload)
	if w.segmentOff+int64(len(w.buf)) > int64(w.opts.segmentMaxSize()) {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	n, err := w.segment.Write(w.buf)
	w.segmentOff += int64(n)
	return err
}

func (w *WAL) syncLoop(interval time.Duration) {
	defer w.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.mu.Lock()
			if w.syncErr == nil {
				w.timedSync()
			}
			w.mu.Unlock()
		}
	}
}

func syncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
