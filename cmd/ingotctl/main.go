// Command ingotctl provides CLI tools for inspecting and validating ingot blocks.
//
// Usage:
//
//	ingotctl blocks <datadir>              — list blocks with metadata
//	ingotctl inspect <blockdir>            — detailed block dump
//	ingotctl chunks <blockdir> <series-ref> — decode and print raw samples
//	ingotctl fsck <datadir>                — validate all blocks
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/davidhrinaldo/ingot/internal/block"
	"github.com/davidhrinaldo/ingot/internal/index"
	"github.com/davidhrinaldo/ingot/labels"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "blocks":
		err = cmdBlocks(os.Args[2:])
	case "inspect":
		err = cmdInspect(os.Args[2:])
	case "chunks":
		err = cmdChunks(os.Args[2:])
	case "fsck":
		err = cmdFsck(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage: ingotctl <command> [args]

Commands:
  blocks <datadir>                List blocks with ULID, time range, level, stats
  inspect <blockdir>              Detailed block dump: series, chunks, postings
  chunks <blockdir> <series-ref>  Decode and print raw samples for a series ref
  fsck <datadir>                  Validate all blocks: CRC checks, index integrity`)
}

// cmdBlocks lists all blocks in a data directory.
func cmdBlocks(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ingotctl blocks <datadir>")
	}
	dataDir := args[0]

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}

	type blockInfo struct {
		meta block.BlockMeta
		dir  string
	}
	var blocks []blockInfo

	for _, e := range entries {
		if !e.IsDir() || e.Name() == "wal" {
			continue
		}
		dir := filepath.Join(dataDir, e.Name())
		if _, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil {
			continue
		}
		meta, err := block.ReadMeta(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: %s: %v\n", e.Name(), err)
			continue
		}
		blocks = append(blocks, blockInfo{meta: meta, dir: dir})
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].meta.MinTime < blocks[j].meta.MinTime
	})

	if len(blocks) == 0 {
		fmt.Println("no blocks found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ULID\tMIN TIME\tMAX TIME\tDURATION\tLEVEL\tSERIES\tSAMPLES\tCHUNKS")
	for _, b := range blocks {
		m := b.meta
		dur := time.Duration(m.MaxTime-m.MinTime) * time.Millisecond
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\n",
			m.ULID,
			formatTimestamp(m.MinTime),
			formatTimestamp(m.MaxTime),
			dur.Truncate(time.Second),
			m.Compaction.Level,
			m.Stats.NumSeries,
			m.Stats.NumSamples,
			m.Stats.NumChunks,
		)
	}
	w.Flush()

	fmt.Printf("\n%d block(s) total\n", len(blocks))
	return nil
}

// cmdInspect dumps detailed information about a single block.
func cmdInspect(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ingotctl inspect <blockdir>")
	}
	blockDir := args[0]

	meta, err := block.ReadMeta(blockDir)
	if err != nil {
		return fmt.Errorf("read meta: %w", err)
	}

	// Print meta.
	fmt.Println("=== Block Meta ===")
	metaJSON, _ := json.MarshalIndent(meta, "", "  ")
	fmt.Println(string(metaJSON))

	// Open block for index inspection.
	br, err := block.Open(blockDir)
	if err != nil {
		return fmt.Errorf("open block: %w", err)
	}
	defer br.Close()

	series := br.Series()

	fmt.Printf("\n=== Series (%d) ===\n", len(series))
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "REF\tLABELS\tCHUNKS\tMIN TIME\tMAX TIME")
	for _, s := range series {
		minT, maxT := seriesTimeRange(s.Chunks)
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%s\n",
			s.Ref,
			formatLabels(s.Labels),
			len(s.Chunks),
			formatTimestamp(minT),
			formatTimestamp(maxT),
		)
	}
	w.Flush()

	// Postings stats.
	fmt.Printf("\n=== Postings ===\n")
	postingsStats := collectPostingsStats(br, series)
	pw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(pw, "LABEL\tVALUES\tPOSTINGS")
	for _, ps := range postingsStats {
		fmt.Fprintf(pw, "%s\t%d\t%d\n", ps.name, ps.numValues, ps.totalPostings)
	}
	pw.Flush()

	return nil
}

// cmdChunks decodes and prints raw samples for a series ref in a block.
func cmdChunks(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: ingotctl chunks <blockdir> <series-ref>")
	}
	blockDir := args[0]
	ref, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid series ref %q: %w", args[1], err)
	}

	br, err := block.Open(blockDir)
	if err != nil {
		return fmt.Errorf("open block: %w", err)
	}
	defer br.Close()

	entry, ok := br.SeriesByRef(ref)
	if !ok {
		return fmt.Errorf("series ref %d not found in block", ref)
	}

	fmt.Printf("Series %d: %s\n", ref, formatLabels(entry.Labels))
	fmt.Printf("Chunks: %d\n\n", len(entry.Chunks))

	for i, cm := range entry.Chunks {
		fmt.Printf("--- Chunk %d [%s .. %s] segment=%d offset=%d ---\n",
			i, formatTimestamp(cm.MinT), formatTimestamp(cm.MaxT),
			cm.Ref.Segment(), cm.Ref.Offset())

		it, err := br.ChunkIterator(cm.Ref)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  error reading chunk: %v\n", err)
			continue
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "  TIMESTAMP\tVALUE")
		count := 0
		for it.Next() {
			t, v := it.At()
			fmt.Fprintf(w, "  %s\t%g\n", formatTimestamp(t), v)
			count++
		}
		w.Flush()
		if it.Err() != nil {
			fmt.Fprintf(os.Stderr, "  iterator error: %v\n", it.Err())
		}
		fmt.Printf("  (%d samples)\n\n", count)
	}

	return nil
}

// cmdFsck validates all blocks in a data directory.
func cmdFsck(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ingotctl fsck <datadir>")
	}
	dataDir := args[0]

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}

	var blockDirs []string
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "wal" {
			continue
		}
		dir := filepath.Join(dataDir, e.Name())
		if _, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil {
			continue
		}
		blockDirs = append(blockDirs, dir)
	}

	if len(blockDirs) == 0 {
		fmt.Println("no blocks found")
		return nil
	}

	totalErrors := 0
	for _, dir := range blockDirs {
		name := filepath.Base(dir)
		errs := block.Validate(dir)
		if len(errs) == 0 {
			fmt.Printf("%s: ok\n", name)
		} else {
			for _, e := range errs {
				fmt.Printf("%s\n", e.Error())
				totalErrors++
			}
		}
	}

	fmt.Printf("\n%d block(s) checked, %d error(s)\n", len(blockDirs), totalErrors)
	if totalErrors > 0 {
		return fmt.Errorf("%d integrity error(s) found", totalErrors)
	}
	return nil
}

// --- helpers ---

func formatTimestamp(ms int64) string {
	t := time.UnixMilli(ms)
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func formatLabels(ls []labels.Label) string {
	var parts []string
	for _, l := range ls {
		parts = append(parts, l.Name+"="+strconv.Quote(l.Value))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func seriesTimeRange(chunks []index.ChunkMeta) (int64, int64) {
	if len(chunks) == 0 {
		return 0, 0
	}
	minT := chunks[0].MinT
	maxT := chunks[0].MaxT
	for _, c := range chunks[1:] {
		if c.MinT < minT {
			minT = c.MinT
		}
		if c.MaxT > maxT {
			maxT = c.MaxT
		}
	}
	return minT, maxT
}

type postingsStat struct {
	name          string
	numValues     int
	totalPostings int
}

func collectPostingsStats(br *block.Reader, series []index.SeriesEntry) []postingsStat {
	// Collect all label names.
	nameSet := make(map[string]struct{})
	for _, s := range series {
		for _, l := range s.Labels {
			nameSet[l.Name] = struct{}{}
		}
	}

	names := make([]string, 0, len(nameSet))
	for n := range nameSet {
		names = append(names, n)
	}
	sort.Strings(names)

	var stats []postingsStat
	for _, name := range names {
		values := br.LabelValues(name)
		total := 0
		for _, v := range values {
			total += len(br.Postings(name, v))
		}
		stats = append(stats, postingsStat{
			name:          name,
			numValues:     len(values),
			totalPostings: total,
		})
	}
	return stats
}
