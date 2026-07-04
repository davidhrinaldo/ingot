package block

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"git.dvdt.dev/david/ingot/internal/index"
)

// ValidationError describes a single integrity issue found during validation.
type ValidationError struct {
	Block   string // ULID or directory name
	Section string // "meta", "index", "chunks"
	Detail  string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Block, e.Section, e.Detail)
}

// ReadMeta reads and returns the BlockMeta for a block directory.
// Exported for use by ingotctl.
func ReadMeta(dir string) (BlockMeta, error) {
	return readMeta(dir)
}

// Validate performs integrity checks on a single block directory.
// It checks meta.json consistency, index integrity (magic, version, TOC CRC),
// and CRC validation on every chunk entry.
func Validate(dir string) []ValidationError {
	var errs []ValidationError
	blockName := filepath.Base(dir)

	// 1. Validate meta.json.
	meta, err := readMeta(dir)
	if err != nil {
		errs = append(errs, ValidationError{blockName, "meta", err.Error()})
		return errs
	}
	if meta.Version != 1 {
		errs = append(errs, ValidationError{blockName, "meta",
			fmt.Sprintf("unsupported version %d", meta.Version)})
	}
	if meta.MinTime > meta.MaxTime {
		errs = append(errs, ValidationError{blockName, "meta",
			fmt.Sprintf("minTime %d > maxTime %d", meta.MinTime, meta.MaxTime)})
	}

	// 2. Validate index file.
	indexData, err := os.ReadFile(filepath.Join(dir, "index"))
	if err != nil {
		errs = append(errs, ValidationError{blockName, "index", err.Error()})
	} else {
		_, err := index.NewReader(indexData)
		if err != nil {
			errs = append(errs, ValidationError{blockName, "index", err.Error()})
		}
	}

	// 3. Validate all chunk segment files — iterate every entry and check CRC.
	chunksDir := filepath.Join(dir, chunksDirName)
	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		errs = append(errs, ValidationError{blockName, "chunks", err.Error()})
		return errs
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		segIdx := parseSegmentName(e.Name())
		if segIdx < 0 {
			continue
		}

		data, err := os.ReadFile(filepath.Join(chunksDir, e.Name()))
		if err != nil {
			errs = append(errs, ValidationError{blockName, "chunks",
				fmt.Sprintf("segment %s: %s", e.Name(), err)})
			continue
		}

		segErrs := validateChunkSegment(data, e.Name())
		for _, se := range segErrs {
			errs = append(errs, ValidationError{blockName, "chunks", se})
		}
	}

	return errs
}

// validateChunkSegment checks every chunk entry in a segment file for CRC integrity.
func validateChunkSegment(data []byte, name string) []string {
	var errs []string

	if len(data) < chunkHeaderLen {
		return []string{fmt.Sprintf("segment %s: too short for header", name)}
	}

	magic := binary.BigEndian.Uint32(data[:4])
	if magic != chunkMagic {
		return []string{fmt.Sprintf("segment %s: invalid magic %#x", name, magic)}
	}
	if data[4] != chunkVersion {
		return []string{fmt.Sprintf("segment %s: unsupported version %d", name, data[4])}
	}

	off := chunkHeaderLen
	entryIdx := 0
	for off < len(data) {
		if off+chunkEntryHeaderLen > len(data) {
			errs = append(errs, fmt.Sprintf("segment %s entry %d at offset %d: truncated header",
				name, entryIdx, off))
			break
		}

		dataLen := int(binary.BigEndian.Uint32(data[off : off+4]))
		encoding := data[off+4]
		off += chunkEntryHeaderLen

		end := off + dataLen + chunkEntryCRCLen
		if end > len(data) {
			errs = append(errs, fmt.Sprintf("segment %s entry %d: truncated data (need %d bytes, have %d)",
				name, entryIdx, dataLen+chunkEntryCRCLen, len(data)-off))
			break
		}

		chunkBytes := data[off : off+dataLen]
		off += dataLen

		wantCRC := binary.BigEndian.Uint32(data[off : off+4])
		crc := crc32.New(castagnoliTable)
		crc.Write([]byte{encoding})
		crc.Write(chunkBytes)
		if crc.Sum32() != wantCRC {
			errs = append(errs, fmt.Sprintf("segment %s entry %d at offset %d: CRC mismatch",
				name, entryIdx, off-dataLen-chunkEntryHeaderLen))
		}
		off += chunkEntryCRCLen
		entryIdx++
	}

	return errs
}
