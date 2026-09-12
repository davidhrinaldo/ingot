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

// SeriesFlush describes a series with its chunks for block writing.
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
	prepared, err := PrepareFlush(dataDir, series)
	if err != nil {
		return "", err
	}
	if err := prepared.Publish(); err != nil {
		return prepared.ULID, err
	}
	return prepared.ULID, nil
}

// PreparedBlock contains durable block data that is not visible to readers
// until Publish writes meta.json.
type PreparedBlock struct {
	ULID    string
	dataDir string
	dir     string
	meta    BlockMeta
}

// PrepareFlush writes and fsyncs a block without publishing its meta.json.
func PrepareFlush(dataDir string, series []SeriesFlush) (*PreparedBlock, error) {
	return prepareBlock(dataDir, series, 1, nil)
}

// Publish makes a prepared block visible and durable.
func (p *PreparedBlock) Publish() error {
	if err := writeMeta(p.dir, p.meta); err != nil {
		return err
	}
	if err := syncDir(p.dir); err != nil {
		return err
	}
	return syncDir(p.dataDir)
}

// FlushCompacted writes a new immutable block from compacted series data,
// recording the compaction level and source block ULIDs.
func FlushCompacted(dataDir string, series []SeriesFlush, level int, sources []string) (string, error) {
	prepared, err := prepareBlock(dataDir, series, level, sources)
	if err != nil {
		return "", err
	}
	if err := prepared.Publish(); err != nil {
		return prepared.ULID, err
	}
	return prepared.ULID, nil
}

func prepareBlock(dataDir string, series []SeriesFlush, level int, sources []string) (*PreparedBlock, error) {
	ulid := newULID()
	blockDir := filepath.Join(dataDir, ulid)

	if err := os.MkdirAll(blockDir, 0755); err != nil {
		return nil, err
	}

	// Write chunk files and collect index entries.
	cw, err := newChunkWriter(blockDir)
	if err != nil {
		return nil, err
	}

	var (
		indexEntries []index.SeriesEntry
		meta         BlockMeta
		haveChunks   bool
	)
	meta.ULID = ulid
	meta.Version = metaVersion
	meta.Compaction = CompactionInfo{Level: level}
	if sources != nil {
		meta.Compaction.Sources = sources
	} else {
		meta.Compaction.Sources = []string{ulid}
	}
	for _, sf := range series {
		var chunks []index.ChunkMeta
		for _, cd := range sf.Chunks {
			ref, err := cw.writeChunk(cd.Data)
			if err != nil {
				cw.close()
				return nil, err
			}
			chunks = append(chunks, index.ChunkMeta{
				MinT: cd.MinT,
				MaxT: cd.MaxT,
				Ref:  ref,
			})
			if !haveChunks || cd.MinT < meta.MinTime {
				meta.MinTime = cd.MinT
			}
			if !haveChunks || cd.MaxT > meta.MaxTime {
				meta.MaxTime = cd.MaxT
			}
			haveChunks = true
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
	if !haveChunks {
		meta.MinTime = 0
		meta.MaxTime = 0
	}

	if err := cw.close(); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Join(blockDir, chunksDirName)); err != nil {
		return nil, err
	}

	// Write index file.
	indexPath := filepath.Join(blockDir, "index")
	indexFile, err := os.Create(indexPath)
	if err != nil {
		return nil, err
	}

	iw := index.NewWriter(indexFile)
	for _, e := range indexEntries {
		iw.AddSeries(e)
	}
	if _, err := iw.WriteTo(); err != nil {
		indexFile.Close()
		return nil, err
	}
	if err := indexFile.Sync(); err != nil {
		indexFile.Close()
		return nil, err
	}
	if err := indexFile.Close(); err != nil {
		return nil, err
	}

	// Fsync the block directory to ensure all files are durable.
	if err := syncDir(blockDir); err != nil {
		return nil, err
	}
	if err := syncDir(dataDir); err != nil {
		return nil, err
	}

	return &PreparedBlock{ULID: ulid, dataDir: dataDir, dir: blockDir, meta: meta}, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
