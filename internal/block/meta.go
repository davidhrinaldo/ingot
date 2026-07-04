package block

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// BlockMeta describes a block on disk.
type BlockMeta struct {
	ULID       string         `json:"ulid"`
	MinTime    int64          `json:"minTime"`
	MaxTime    int64          `json:"maxTime"`
	Stats      BlockStats     `json:"stats"`
	Compaction CompactionInfo `json:"compaction"`
	Version    int            `json:"version"`
}

// BlockStats holds summary statistics for a block.
type BlockStats struct {
	NumSamples int `json:"numSamples"`
	NumSeries  int `json:"numSeries"`
	NumChunks  int `json:"numChunks"`
}

// CompactionInfo records the block's compaction lineage.
type CompactionInfo struct {
	Level   int      `json:"level"`
	Sources []string `json:"sources"`
}

const metaFilename = "meta.json"

// readMeta reads a block's meta.json from the given block directory.
func readMeta(dir string) (BlockMeta, error) {
	data, err := os.ReadFile(filepath.Join(dir, metaFilename))
	if err != nil {
		return BlockMeta{}, err
	}
	var m BlockMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return BlockMeta{}, err
	}
	return m, nil
}

// writeMeta writes a block's meta.json to the given block directory.
// This is the last file written when creating a block — the immutability gate.
func writeMeta(dir string, m BlockMeta) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, metaFilename), data, 0644)
}
