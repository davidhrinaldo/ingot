package wal

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures WAL behavior.
type Options struct {
	// SegmentMaxSize is the maximum size of a single segment file in bytes.
	// A new segment is created when the current one would exceed this.
	// Default: 128 MiB.
	SegmentMaxSize int

	// SyncInterval controls background fsync frequency.
	// Default (zero): 1s. Negative: sync on every Log call.
	SyncInterval time.Duration
}

func (o *Options) segmentMaxSize() int {
	if o.SegmentMaxSize > 0 {
		return o.SegmentMaxSize
	}
	return defaultSegmentMaxSize
}

func (o *Options) syncInterval() time.Duration {
	if o.SyncInterval < 0 {
		return -1 // sync-per-write sentinel
	}
	if o.SyncInterval == 0 {
		return time.Second
	}
	return o.SyncInterval
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

	lastSyncDur atomic.Int64 // nanoseconds of last fsync

	done chan struct{}
	wg   sync.WaitGroup
}

// Open opens or creates a WAL in dir. If segments already exist, it runs
// recovery (truncating at the first corrupt record) before returning.
func Open(dir string, opts Options) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
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

	// Sync-per-write mode.
	if w.opts.syncInterval() < 0 {
		return w.segment.Sync()
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

// Replay returns a Reader over all WAL segments. The caller must Close
// the reader when done.
func (w *WAL) Replay() (*Reader, error) {
	return NewReader(w.dir)
}

// Sync forces an fsync of the current segment.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
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
	err := w.segment.Sync()
	w.lastSyncDur.Store(int64(time.Since(start)))
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
		return nil
	}
	if err := w.segment.Sync(); err != nil {
		w.segment.Close()
		return err
	}
	return w.segment.Close()
}

// rotate fsyncs the current segment, closes it, and creates a new one.
// Caller must hold w.mu.
func (w *WAL) rotate() error {
	if err := w.segment.Sync(); err != nil {
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
	return nil
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
			w.timedSync()
			w.mu.Unlock()
		}
	}
}
