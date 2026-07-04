// Package compact implements levelled compaction and retention for ingot blocks.
package compact

import (
	"fmt"
	"sort"

	"github.com/davidhrinaldo/ingot/internal/block"
	"github.com/davidhrinaldo/ingot/labels"
)

// Clock returns the current time in milliseconds since epoch.
type Clock func() int64

// Compactor manages levelled compaction and retention.
type Compactor struct {
	dataDir   string
	levels    []int64 // level durations in ms, e.g. [2h, 8h, 32h]
	retention int64   // retention window in ms (0 = disabled)
	clock     Clock
}

// New creates a Compactor. levels are the compaction level durations in
// ascending order (e.g. 2h, 8h, 32h in milliseconds). retention is the
// maximum age of data in milliseconds (0 disables retention).
func New(dataDir string, levels []int64, retention int64, clock Clock) *Compactor {
	return &Compactor{
		dataDir:   dataDir,
		levels:    levels,
		retention: retention,
		clock:     clock,
	}
}

// CompactionGroup describes a set of source blocks to compact.
type CompactionGroup struct {
	Sources []*block.Reader
	Level   int // resulting compaction level
}

// Plan returns the first eligible compaction group, or nil if no compaction
// is needed. Lower levels are prioritized. A group requires at least 2
// blocks at the same compaction level whose combined time span fits within
// the next level's duration.
func (c *Compactor) Plan(blocks []*block.Reader) *CompactionGroup {
	if len(blocks) < 2 {
		return nil
	}

	// Group blocks by compaction level.
	byLevel := make(map[int][]*block.Reader)
	for _, b := range blocks {
		lvl := b.Meta.Compaction.Level
		byLevel[lvl] = append(byLevel[lvl], b)
	}

	// Sort levels ascending.
	var levels []int
	for lvl := range byLevel {
		levels = append(levels, lvl)
	}
	sort.Ints(levels)

	for _, lvl := range levels {
		group := c.planLevel(byLevel[lvl], lvl)
		if group != nil {
			return group
		}
	}
	return nil
}

// planLevel finds a compactable group within blocks at the same level.
func (c *Compactor) planLevel(blocks []*block.Reader, level int) *CompactionGroup {
	if len(blocks) < 2 {
		return nil
	}

	// Sort by MinTime.
	sorted := make([]*block.Reader, len(blocks))
	copy(sorted, blocks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Meta.MinTime < sorted[j].Meta.MinTime
	})

	// Determine the max span for the next level.
	var maxSpan int64
	if level-1 < len(c.levels) && level >= 1 {
		// Find the next level's duration. Level 1 blocks should compact
		// into level 2 when they span up to levels[1] (8h), etc.
		if level < len(c.levels) {
			maxSpan = c.levels[level]
		}
	}
	if maxSpan == 0 {
		// No higher level defined, or level 0 — use the second level duration.
		if level < len(c.levels) {
			maxSpan = c.levels[level]
		} else {
			return nil // already at max level
		}
	}

	// Find the first group of consecutive blocks whose span fits maxSpan.
	for i := 0; i < len(sorted)-1; i++ {
		group := []*block.Reader{sorted[i]}
		for j := i + 1; j < len(sorted); j++ {
			span := sorted[j].Meta.MaxTime - sorted[i].Meta.MinTime
			if span > maxSpan {
				break
			}
			group = append(group, sorted[j])
		}
		if len(group) >= 2 {
			return &CompactionGroup{
				Sources: group,
				Level:   level + 1,
			}
		}
	}
	return nil
}

// Compact merges source blocks into a single new block. Returns the new
// block's ULID. The caller is responsible for swapping the block set and
// releasing source blocks.
func (c *Compactor) Compact(sources []*block.Reader) (string, error) {
	if len(sources) == 0 {
		return "", fmt.Errorf("compact: no source blocks")
	}

	merged := make(map[uint64]*mergedEntry)

	for _, src := range sources {
		for _, entry := range src.Series() {
			me, ok := merged[entry.Ref]
			if !ok {
				me = &mergedEntry{
					ref:    entry.Ref,
					labels: entry.Labels,
				}
				merged[entry.Ref] = me
			}
			for _, cm := range entry.Chunks {
				raw, err := src.RawChunkData(cm.Ref)
				if err != nil {
					return "", fmt.Errorf("compact: read chunk ref %v from %s: %w",
						cm.Ref, src.Meta.ULID, err)
				}
				// Copy the raw bytes since the source may be munmapped later.
				data := make([]byte, len(raw))
				copy(data, raw)
				me.chunks = append(me.chunks, block.ChunkData{
					MinT: cm.MinT,
					MaxT: cm.MaxT,
					Data: data,
				})
			}
		}
	}

	// Build flush data sorted by ref for deterministic output.
	flushData := make([]block.SeriesFlush, 0, len(merged))
	for _, me := range merged {
		flushData = append(flushData, block.SeriesFlush{
			Ref:    me.ref,
			Labels: me.labels,
			Chunks: me.chunks,
		})
	}
	sort.Slice(flushData, func(i, j int) bool { return flushData[i].Ref < flushData[j].Ref })

	// Determine new compaction level and collect source ULIDs.
	maxLevel := 0
	sourceULIDs := make([]string, 0, len(sources))
	for _, src := range sources {
		if src.Meta.Compaction.Level > maxLevel {
			maxLevel = src.Meta.Compaction.Level
		}
		sourceULIDs = append(sourceULIDs, src.Meta.ULID)
	}

	return block.FlushCompacted(c.dataDir, flushData, maxLevel+1, sourceULIDs)
}

// Expired returns blocks whose MaxTime is older than the retention window.
// Returns nil if retention is disabled (zero).
func (c *Compactor) Expired(blocks []*block.Reader) []*block.Reader {
	if c.retention == 0 {
		return nil
	}
	cutoff := c.clock() - c.retention
	var expired []*block.Reader
	for _, b := range blocks {
		if b.Meta.MaxTime < cutoff {
			expired = append(expired, b)
		}
	}
	return expired
}

type mergedEntry struct {
	ref    uint64
	labels []labels.Label
	chunks []block.ChunkData
}
