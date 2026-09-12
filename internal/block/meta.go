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

const (
	metaFilename = "meta.json"
	metaVersion  = 1
)

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
	tmpPath := filepath.Join(dir, metaFilename+".tmp")
	metaPath := filepath.Join(dir, metaFilename)
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	removeTmp := true
	defer func() {
		if removeTmp {
			os.Remove(tmpPath)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, metaPath); err != nil {
		return err
	}
	removeTmp = false
	return nil
}
