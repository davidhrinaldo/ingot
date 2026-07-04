package ingot

import (
	"git.dvdt.dev/david/ingot/labels"
)

// Self-instrumentation metric names.
const (
	MetricHeadSeries         = "ingot_head_series"
	MetricHeadChunksActive   = "ingot_head_chunks_active"
	MetricBlocksTotal        = "ingot_blocks_total"
	MetricCompactionsTotal   = "ingot_compactions_total"
	MetricWALFsyncDurationS  = "ingot_wal_fsync_duration_seconds"
)

// metricsRefs caches series refs for self-instrumentation metrics.
type metricsRefs struct {
	headSeries       uint64
	headChunksActive uint64
	blocksTotal      uint64
	compactionsTotal uint64
	walFsyncDuration uint64
}

// collectMetrics snapshots the DB's internal stats and writes them as
// ingot series via the normal Appender path. Called periodically from
// the compact loop.
func (db *DB) collectMetrics() {
	now := db.opts.clock()()

	hs := db.head.Stats()
	db.mu.RLock()
	numBlocks := len(db.blocks)
	db.mu.RUnlock()
	compactions := db.compactionCount.Load()
	walFsync := db.head.WALSyncDuration()

	type metric struct {
		name  string
		value float64
		ref   *uint64
	}
	metrics := []metric{
		{MetricHeadSeries, float64(hs.NumSeries), &db.metricsR.headSeries},
		{MetricHeadChunksActive, float64(hs.NumActiveChunks), &db.metricsR.headChunksActive},
		{MetricBlocksTotal, float64(numBlocks), &db.metricsR.blocksTotal},
		{MetricCompactionsTotal, float64(compactions), &db.metricsR.compactionsTotal},
		{MetricWALFsyncDurationS, walFsync, &db.metricsR.walFsyncDuration},
	}

	app := db.Appender()
	for _, m := range metrics {
		ref := *m.ref
		var ls []labels.Label
		if ref == 0 {
			ls = labels.FromStrings("__name__", m.name)
		}
		newRef, err := app.Append(ref, ls, now, m.value)
		if err != nil {
			// OOO rejection or other transient error — skip this cycle.
			app.Rollback()
			return
		}
		*m.ref = newRef
	}
	app.Commit()
}

