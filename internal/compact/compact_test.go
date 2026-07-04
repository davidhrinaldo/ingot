package compact

import (
	"math"
	"path/filepath"
	"testing"

	"git.dvdt.dev/david/ingot/internal/block"
	"git.dvdt.dev/david/ingot/internal/chunkenc"
	"git.dvdt.dev/david/ingot/labels"
)

const (
	hour = 3600 * 1000 // 1 hour in ms
)

// makeChunk creates a chunk with the given samples and returns its raw bytes.
func makeChunk(t *testing.T, timestamps []int64, values []float64) []byte {
	t.Helper()
	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := range timestamps {
		a.Append(timestamps[i], values[i])
	}
	return append([]byte(nil), c.Bytes()...)
}

// flushTestBlock creates a block in dataDir and returns an opened Reader.
func flushTestBlock(t *testing.T, dataDir string, series []block.SeriesFlush, level int, sources []string) *block.Reader {
	t.Helper()
	var ulid string
	var err error
	if level == 1 && sources == nil {
		ulid, err = block.Flush(dataDir, series)
	} else {
		ulid, err = block.FlushCompacted(dataDir, series, level, sources)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r, err := block.Open(filepath.Join(dataDir, ulid))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return r
}

// collectBlockSamples reads all samples for a series ref from a block.
func collectBlockSamples(t *testing.T, r *block.Reader, ref uint64) []sample {
	t.Helper()
	it, err := r.SeriesChunkIterator(ref, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var out []sample
	for it.Next() {
		ts, v := it.At()
		out = append(out, sample{ts, v})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return out
}

type sample struct {
	t int64
	v float64
}

func TestPlan(t *testing.T) {
	tests := []struct {
		name      string
		blocks    []blockSpec // blocks to create
		wantGroup bool        // expect a compaction group
		wantLevel int         // expected resulting level
		wantCount int         // expected number of sources in group
	}{
		{
			name:      "no_blocks",
			blocks:    nil,
			wantGroup: false,
		},
		{
			name: "single_block",
			blocks: []blockSpec{
				{minT: 0, maxT: 2 * hour, level: 1},
			},
			wantGroup: false,
		},
		{
			name: "two_level1_blocks_within_8h",
			blocks: []blockSpec{
				{minT: 0, maxT: 2 * hour, level: 1},
				{minT: 2 * hour, maxT: 4 * hour, level: 1},
			},
			wantGroup: true,
			wantLevel: 2,
			wantCount: 2,
		},
		{
			name: "four_level1_blocks",
			blocks: []blockSpec{
				{minT: 0, maxT: 2 * hour, level: 1},
				{minT: 2 * hour, maxT: 4 * hour, level: 1},
				{minT: 4 * hour, maxT: 6 * hour, level: 1},
				{minT: 6 * hour, maxT: 8 * hour, level: 1},
			},
			wantGroup: true,
			wantLevel: 2,
			wantCount: 4,
		},
		{
			name: "level1_blocks_exceed_8h_span",
			blocks: []blockSpec{
				{minT: 0, maxT: 2 * hour, level: 1},
				{minT: 7 * hour, maxT: 9 * hour, level: 1},
			},
			wantGroup: false,
		},
		{
			name: "two_level2_blocks_within_32h",
			blocks: []blockSpec{
				{minT: 0, maxT: 8 * hour, level: 2},
				{minT: 8 * hour, maxT: 16 * hour, level: 2},
			},
			wantGroup: true,
			wantLevel: 3,
			wantCount: 2,
		},
		{
			name: "max_level_blocks_not_compacted",
			blocks: []blockSpec{
				{minT: 0, maxT: 32 * hour, level: 3},
				{minT: 32 * hour, maxT: 64 * hour, level: 3},
			},
			wantGroup: false,
		},
		{
			name: "mixed_levels_lower_prioritized",
			blocks: []blockSpec{
				{minT: 0, maxT: 2 * hour, level: 1},
				{minT: 2 * hour, maxT: 4 * hour, level: 1},
				{minT: 10 * hour, maxT: 18 * hour, level: 2},
				{minT: 18 * hour, maxT: 26 * hour, level: 2},
			},
			wantGroup: true,
			wantLevel: 2, // level-1 group found first
			wantCount: 2,
		},
	}

	levels := []int64{2 * hour, 8 * hour, 32 * hour}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			c := New(dataDir, levels, 0, nil)

			var blocks []*block.Reader
			for _, spec := range tc.blocks {
				r := createBlockWithMeta(t, dataDir, spec)
				blocks = append(blocks, r)
				t.Cleanup(func() { r.Close() })
			}

			group := c.Plan(blocks)

			if !tc.wantGroup {
				if group != nil {
					t.Errorf("expected no compaction group, got %v", group)
				}
				return
			}
			if group == nil {
				t.Fatalf("expected a compaction group")
			}
			if got, want := group.Level, tc.wantLevel; got != want {
				t.Errorf("compaction level: got %v, want %v", got, want)
			}
			if got, want := len(group.Sources), tc.wantCount; got != want {
				t.Errorf("source count: got %v, want %v", got, want)
			}
		})
	}
}

func TestCompact(t *testing.T) {
	tests := []struct {
		name           string
		sourceBlocks   []sourceBlock
		wantSeriesRefs []uint64
		wantSamples    map[uint64][]sample
		wantLevel      int
	}{
		{
			name: "merge_two_blocks_disjoint_series",
			sourceBlocks: []sourceBlock{
				{
					level: 1,
					series: []seriesData{
						{ref: 1, labels: labels.FromStrings("__name__", "a"), samples: []sample{{1000, 1.0}, {2000, 2.0}}},
					},
				},
				{
					level: 1,
					series: []seriesData{
						{ref: 2, labels: labels.FromStrings("__name__", "b"), samples: []sample{{1000, 3.0}, {2000, 4.0}}},
					},
				},
			},
			wantSeriesRefs: []uint64{1, 2},
			wantSamples: map[uint64][]sample{
				1: {{1000, 1.0}, {2000, 2.0}},
				2: {{1000, 3.0}, {2000, 4.0}},
			},
			wantLevel: 2,
		},
		{
			name: "merge_two_blocks_same_series",
			sourceBlocks: []sourceBlock{
				{
					level: 1,
					series: []seriesData{
						{ref: 1, labels: labels.FromStrings("__name__", "temp"), samples: []sample{{1000, 1.0}, {2000, 2.0}}},
					},
				},
				{
					level: 1,
					series: []seriesData{
						{ref: 1, labels: labels.FromStrings("__name__", "temp"), samples: []sample{{3000, 3.0}, {4000, 4.0}}},
					},
				},
			},
			wantSeriesRefs: []uint64{1},
			wantSamples: map[uint64][]sample{
				1: {{1000, 1.0}, {2000, 2.0}, {3000, 3.0}, {4000, 4.0}},
			},
			wantLevel: 2,
		},
		{
			name: "merge_level2_blocks",
			sourceBlocks: []sourceBlock{
				{
					level: 2,
					series: []seriesData{
						{ref: 1, labels: labels.FromStrings("__name__", "a"), samples: []sample{{1000, 1.0}}},
					},
				},
				{
					level: 2,
					series: []seriesData{
						{ref: 1, labels: labels.FromStrings("__name__", "a"), samples: []sample{{5000, 5.0}}},
					},
				},
			},
			wantSeriesRefs: []uint64{1},
			wantSamples: map[uint64][]sample{
				1: {{1000, 1.0}, {5000, 5.0}},
			},
			wantLevel: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			c := New(dataDir, []int64{2 * hour, 8 * hour, 32 * hour}, 0, nil)

			// Create source blocks.
			var sources []*block.Reader
			for _, sb := range tc.sourceBlocks {
				r := createSourceBlock(t, dataDir, sb)
				sources = append(sources, r)
			}

			// Compact.
			newULID, err := c.Compact(sources)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if newULID == "" {
				t.Fatalf("expected non-empty ULID")
			}

			// Close source blocks.
			for _, s := range sources {
				s.Close()
			}

			// Open compacted block.
			compacted, err := block.Open(filepath.Join(dataDir, newULID))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer compacted.Close()

			// Verify compaction level.
			if got, want := compacted.Meta.Compaction.Level, tc.wantLevel; got != want {
				t.Errorf("compaction level: got %v, want %v", got, want)
			}
			if got, want := len(compacted.Meta.Compaction.Sources), len(tc.sourceBlocks); got != want {
				t.Errorf("source count: got %v, want %v", got, want)
			}

			// Verify series count.
			if got, want := compacted.Meta.Stats.NumSeries, len(tc.wantSeriesRefs); got != want {
				t.Errorf("series count: got %v, want %v", got, want)
			}

			// Verify each series' samples.
			for _, ref := range tc.wantSeriesRefs {
				got := collectBlockSamples(t, compacted, ref)
				want := tc.wantSamples[ref]
				if len(got) != len(want) {
					t.Fatalf("sample count for ref %d: got %v, want %v", ref, len(got), len(want))
				}
				for i := range want {
					if got[i].t != want[i].t {
						t.Errorf("ref %d sample %d t: got %v, want %v", ref, i, got[i].t, want[i].t)
					}
					if got[i].v != want[i].v {
						t.Errorf("ref %d sample %d v: got %v, want %v", ref, i, got[i].v, want[i].v)
					}
				}
			}
		})
	}
}

func TestExpired(t *testing.T) {
	tests := []struct {
		name      string
		blocks    []blockSpec
		now       int64
		retention int64
		wantCount int
	}{
		{
			name:      "no_retention",
			blocks:    []blockSpec{{minT: 0, maxT: 1000}},
			now:       100000,
			retention: 0,
			wantCount: 0,
		},
		{
			name: "all_within_retention",
			blocks: []blockSpec{
				{minT: 80000, maxT: 90000},
			},
			now:       100000,
			retention: 50000,
			wantCount: 0,
		},
		{
			name: "one_expired",
			blocks: []blockSpec{
				{minT: 0, maxT: 10000},
				{minT: 80000, maxT: 90000},
			},
			now:       100000,
			retention: 50000,
			wantCount: 1,
		},
		{
			name: "all_expired",
			blocks: []blockSpec{
				{minT: 0, maxT: 10000},
				{minT: 20000, maxT: 30000},
			},
			now:       100000,
			retention: 50000,
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			clock := func() int64 { return tc.now }
			c := New(dataDir, nil, tc.retention, clock)

			var blocks []*block.Reader
			for _, spec := range tc.blocks {
				r := createBlockWithMeta(t, dataDir, spec)
				blocks = append(blocks, r)
				t.Cleanup(func() { r.Close() })
			}

			expired := c.Expired(blocks)
			if got, want := len(expired), tc.wantCount; got != want {
				t.Errorf("expired block count: got %v, want %v", got, want)
			}
		})
	}
}

// --- helpers ---

type blockSpec struct {
	minT  int64
	maxT  int64
	level int
}

type sourceBlock struct {
	level  int
	series []seriesData
}

type seriesData struct {
	ref     uint64
	labels  []labels.Label
	samples []sample
}

// createBlockWithMeta creates a minimal block with the given time range and level.
func createBlockWithMeta(t *testing.T, dataDir string, spec blockSpec) *block.Reader {
	t.Helper()
	// Need at least one series with samples spanning [minT, maxT].
	timestamps := []int64{spec.minT, spec.maxT}
	values := []float64{1.0, 2.0}
	data := makeChunk(t, timestamps, values)

	series := []block.SeriesFlush{{
		Ref:    1,
		Labels: labels.FromStrings("__name__", "test"),
		Chunks: []block.ChunkData{{MinT: spec.minT, MaxT: spec.maxT, Data: data}},
	}}

	level := spec.level
	if level == 0 {
		level = 1
	}
	return flushTestBlock(t, dataDir, series, level, []string{"src"})
}

// createSourceBlock creates a block with the given series data.
func createSourceBlock(t *testing.T, dataDir string, sb sourceBlock) *block.Reader {
	t.Helper()
	var flushData []block.SeriesFlush
	for _, sd := range sb.series {
		timestamps := make([]int64, len(sd.samples))
		values := make([]float64, len(sd.samples))
		for i, s := range sd.samples {
			timestamps[i] = s.t
			values[i] = s.v
		}
		data := makeChunk(t, timestamps, values)
		minT, maxT := sd.samples[0].t, sd.samples[len(sd.samples)-1].t
		flushData = append(flushData, block.SeriesFlush{
			Ref:    sd.ref,
			Labels: sd.labels,
			Chunks: []block.ChunkData{{MinT: minT, MaxT: maxT, Data: data}},
		})
	}

	level := sb.level
	if level == 0 {
		level = 1
	}
	return flushTestBlock(t, dataDir, flushData, level, []string{"src"})
}
