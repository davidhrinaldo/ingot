package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidhrinaldo/ingot/labels"
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
	// File operations below are overridden by fault-injection tests.
	write         func(*os.File, []byte) (int, error)
	close         func(*os.File) error
	createSegment func(string, int) (*os.File, error)
	syncDir       func(string) error
	remove        func(string) error
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

func (o *Options) writeTo(f *os.File, b []byte) (int, error) {
	if o.write != nil {
		return o.write(f, b)
	}
	return f.Write(b)
}

func (o *Options) closeFile(f *os.File) error {
	if o.close != nil {
		return o.close(f)
	}
	return f.Close()
}

func (o *Options) create(dir string, index int) (*os.File, error) {
	if o.createSegment != nil {
		return o.createSegment(dir, index)
	}
	return createSegment(dir, index)
}

func (o *Options) syncDirectory(dir string) error {
	if o.syncDir != nil {
		return o.syncDir(dir)
	}
	return syncDir(dir)
}

func (o *Options) removeFile(path string) error {
	if o.remove != nil {
		return o.remove(path)
	}
	return os.Remove(path)
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
	poisonErr  error

	lastSyncDur atomic.Int64 // nanoseconds of last fsync

	done chan struct{}
	wg   sync.WaitGroup
}

// Open opens or creates a WAL in dir. Recovery repairs an incomplete final
// record and rejects CRC corruption in every segment.
func Open(dir string, opts Options) (*WAL, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	if err := opts.syncDirectory(filepath.Dir(dir)); err != nil {
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
		f, err := opts.create(dir, 1)
		if err != nil {
			return nil, err
		}
		w.segment = f
		if err := opts.syncDirectory(dir); err != nil {
			opts.closeFile(f)
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
	if err := w.previousFailure(); err != nil {
		return err
	}

	w.buf = EncodeRecord(w.buf[:0], typ, payload)

	// Rotate if this write would exceed the segment size limit.
	if w.segmentOff+int64(len(w.buf)) > int64(w.opts.segmentMaxSize()) {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	return w.writeRecordLocked()
}

// LogSeries encodes and writes a series record.
func (w *WAL) LogSeries(recs []SeriesRecord) error {
	for _, rec := range recs {
		if err := labels.Validate(rec.Labels); err != nil {
			return err
		}
	}
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
	for _, rec := range series {
		if err := labels.Validate(rec.Labels); err != nil {
			return Checkpoint{}, err
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.previousFailure(); err != nil {
		return Checkpoint{}, err
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
	if err := w.opts.syncDirectory(w.dir); err != nil {
		return Checkpoint{}, w.poison(fmt.Errorf("wal: sync directory after checkpoint: %w", err))
	}
	return cp, nil
}

// ActivateCheckpoint records that the checkpoint's block was published. It
// validates the source while holding the WAL lock so callers cannot append the
// activation record before validation succeeds.
func (w *WAL) ActivateCheckpoint(cp Checkpoint, validateSource func() error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.previousFailure(); err != nil {
		return err
	}
	if validateSource == nil {
		return fmt.Errorf("wal: checkpoint source validator is required")
	}
	if err := validateSource(); err != nil {
		return fmt.Errorf("wal: validate checkpoint source: %w", err)
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
	if err := w.previousFailure(); err != nil {
		return err
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
	if err := w.previousFailure(); err != nil {
		return err
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
	if err != nil {
		return w.poison(fmt.Errorf("wal: sync segment %08d: %w", w.segmentIdx, err))
	}
	return nil
}

// Truncate deletes all segments with index less than below.
func (w *WAL) Truncate(below int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.previousFailure(); err != nil {
		return err
	}

	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}

	for _, idx := range segs {
		if idx >= below {
			break
		}
		if err := w.opts.removeFile(segmentPath(w.dir, idx)); err != nil {
			return w.poison(fmt.Errorf("wal: remove segment %08d: %w", idx, err))
		}
	}
	if err := w.opts.syncDirectory(w.dir); err != nil {
		return w.poison(fmt.Errorf("wal: sync directory after truncation: %w", err))
	}
	return nil
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
		return w.poisonErr
	}
	previousErr := w.poisonErr
	syncErr := w.timedSync()
	closeErr := w.opts.closeFile(w.segment)
	if closeErr != nil {
		closeErr = w.poison(fmt.Errorf("wal: close segment %08d: %w", w.segmentIdx, closeErr))
	}
	w.segment = nil
	return errors.Join(previousErr, syncErr, closeErr)
}

// rotate fsyncs the current segment, closes it, and creates a new one.
// Caller must hold w.mu.
func (w *WAL) rotate() error {
	if err := w.timedSync(); err != nil {
		return err
	}
	if err := w.opts.closeFile(w.segment); err != nil {
		return w.poison(fmt.Errorf("wal: close segment %08d during rotation: %w", w.segmentIdx, err))
	}

	next := w.segmentIdx + 1
	f, err := w.opts.create(w.dir, next)
	if err != nil {
		return w.poison(fmt.Errorf("wal: create segment %08d during rotation: %w", next, err))
	}
	w.segment = f
	w.segmentIdx = next
	w.segmentOff = 0
	if err := w.opts.syncDirectory(w.dir); err != nil {
		return w.poison(fmt.Errorf("wal: sync directory after rotation: %w", err))
	}
	return nil
}

// logLocked writes a record while w.mu is held.
func (w *WAL) logLocked(typ RecordType, payload []byte) error {
	w.buf = EncodeRecord(w.buf[:0], typ, payload)
	if w.segmentOff+int64(len(w.buf)) > int64(w.opts.segmentMaxSize()) {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	return w.writeRecordLocked()
}

func (w *WAL) writeRecordLocked() error {
	n, err := w.opts.writeTo(w.segment, w.buf)
	if n > 0 {
		w.segmentOff += int64(n)
	}
	if err != nil {
		return w.poison(fmt.Errorf("wal: write segment %08d: %w", w.segmentIdx, err))
	}
	if n != len(w.buf) {
		return w.poison(fmt.Errorf("wal: write segment %08d: %w: wrote %d of %d bytes", w.segmentIdx, io.ErrShortWrite, n, len(w.buf)))
	}
	return nil
}

func (w *WAL) previousFailure() error {
	if w.poisonErr == nil {
		return nil
	}
	return fmt.Errorf("wal: unusable after previous failure: %w", w.poisonErr)
}

func (w *WAL) poison(err error) error {
	if err != nil && w.poisonErr == nil {
		w.poisonErr = err
	}
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
			if w.poisonErr == nil {
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
