package block

import (
	"os"
	"path/filepath"

	"github.com/davidhrinaldo/ingot/internal/index"
	"github.com/davidhrinaldo/ingot/labels"
)

// ChunkData describes a single chunk to be flushed to a block.
type ChunkData struct {
	MinT int64
	MaxT int64
	Data []byte // raw XOR chunk bytes (including 2-byte sample count header)
}

// SeriesFlush describes a series with its sealed chunks for block writing.
type SeriesFlush struct {
	Ref    uint64
	Labels []labels.Label
	Chunks []ChunkData
}

// Flush writes a new immutable block from the given series data.
// It creates a ULID-named directory under dataDir containing:
//   - chunks/ with segment files
//   - index file
//   - meta.json (written last as the immutability gate)
//
// Returns the block ULID and any error.
func Flush(dataDir string, series []SeriesFlush) (string, error) {
	return flushBlock(dataDir, series, 1, nil)
}

// FlushCompacted writes a new immutable block from compacted series data,
// recording the compaction level and source block ULIDs.
func FlushCompacted(dataDir string, series []SeriesFlush, level int, sources []string) (string, error) {
	return flushBlock(dataDir, series, level, sources)
}

func flushBlock(dataDir string, series []SeriesFlush, level int, sources []string) (string, error) {
	ulid := newULID()
	blockDir := filepath.Join(dataDir, ulid)

	if err := os.MkdirAll(blockDir, 0755); err != nil {
		return "", err
	}

	// Write chunk files and collect index entries.
	cw, err := newChunkWriter(blockDir)
	if err != nil {
		return "", err
	}

	var (
		indexEntries []index.SeriesEntry
		meta         BlockMeta
	)
	meta.ULID = ulid
	meta.Version = 1
	meta.Compaction = CompactionInfo{Level: level}
	if sources != nil {
		meta.Compaction.Sources = sources
	} else {
		meta.Compaction.Sources = []string{ulid}
	}
	meta.MinTime = int64(^uint64(0) >> 1) // max int64
	meta.MaxTime = int64(0)

	for _, sf := range series {
		var chunks []index.ChunkMeta
		for _, cd := range sf.Chunks {
			ref, err := cw.writeChunk(cd.Data)
			if err != nil {
				cw.close()
				return "", err
			}
			chunks = append(chunks, index.ChunkMeta{
				MinT: cd.MinT,
				MaxT: cd.MaxT,
				Ref:  ref,
			})
			if cd.MinT < meta.MinTime {
				meta.MinTime = cd.MinT
			}
			if cd.MaxT > meta.MaxTime {
				meta.MaxTime = cd.MaxT
			}
			meta.Stats.NumChunks++
			// Count samples from the chunk's 2-byte header.
			if len(cd.Data) >= 2 {
				meta.Stats.NumSamples += int(uint16(cd.Data[0])<<8 | uint16(cd.Data[1]))
			}
		}
		indexEntries = append(indexEntries, index.SeriesEntry{
			Ref:    sf.Ref,
			Labels: sf.Labels,
			Chunks: chunks,
		})
		meta.Stats.NumSeries++
	}

	if err := cw.close(); err != nil {
		return "", err
	}

	// Write index file.
	indexPath := filepath.Join(blockDir, "index")
	indexFile, err := os.Create(indexPath)
	if err != nil {
		return "", err
	}

	iw := index.NewWriter(indexFile)
	for _, e := range indexEntries {
		iw.AddSeries(e)
	}
	if _, err := iw.WriteTo(); err != nil {
		indexFile.Close()
		return "", err
	}
	if err := indexFile.Sync(); err != nil {
		indexFile.Close()
		return "", err
	}
	if err := indexFile.Close(); err != nil {
		return "", err
	}

	// Fsync the block directory to ensure all files are durable.
	if err := syncDir(blockDir); err != nil {
		return "", err
	}

	// Write meta.json last — the immutability gate.
	if err := writeMeta(blockDir, meta); err != nil {
		return "", err
	}

	// Fsync the data directory so the block directory entry is visible.
	if err := syncDir(dataDir); err != nil {
		return "", err
	}

	return ulid, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
