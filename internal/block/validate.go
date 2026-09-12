package block

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/davidhrinaldo/ingot/internal/chunkenc"
	"github.com/davidhrinaldo/ingot/internal/index"
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

type scannedChunk struct {
	data  []byte
	valid bool
}

// Validate checks the complete set of relationships in an immutable block.
func Validate(dir string) []ValidationError {
	var errs []ValidationError
	blockName := filepath.Base(filepath.Clean(dir))

	meta, metaErr := readMeta(dir)
	if metaErr != nil {
		errs = append(errs, ValidationError{blockName, "meta", metaErr.Error()})
	} else {
		errs = append(errs, validateMeta(blockName, meta)...)
	}

	var idx *index.Reader
	indexData, err := os.ReadFile(filepath.Join(dir, "index"))
	if err != nil {
		errs = append(errs, ValidationError{blockName, "index", err.Error()})
	} else {
		idx, err = index.NewReader(indexData)
		if err != nil {
			errs = append(errs, ValidationError{blockName, "index", err.Error()})
		}
	}

	chunkEntries := make(map[index.ChunkRef]scannedChunk)
	chunksReadable := true
	chunksDir := filepath.Join(dir, chunksDirName)
	dirEntries, err := os.ReadDir(chunksDir)
	if err != nil {
		errs = append(errs, ValidationError{blockName, "chunks", err.Error()})
		chunksReadable = false
	} else {
		for _, entry := range dirEntries {
			if entry.IsDir() {
				continue
			}
			segment := parseSegmentName(entry.Name())
			if segment < 0 {
				continue
			}

			data, readErr := os.ReadFile(filepath.Join(chunksDir, entry.Name()))
			if readErr != nil {
				errs = append(errs, ValidationError{blockName, "chunks",
					fmt.Sprintf("segment %s: %s", entry.Name(), readErr)})
				chunksReadable = false
				continue
			}
			parsed, complete, segmentErrs := scanChunkSegment(data, entry.Name(), segment)
			if !complete {
				chunksReadable = false
			}
			for _, detail := range segmentErrs {
				errs = append(errs, ValidationError{blockName, "chunks", detail})
			}
			for ref, chunk := range parsed {
				chunkEntries[ref] = chunk
			}
		}
	}

	if metaErr == nil && idx != nil && chunksReadable {
		errs = append(errs, validateRelationships(blockName, meta, idx, chunkEntries)...)
	}
	return errs
}

func validateMeta(blockName string, meta BlockMeta) []ValidationError {
	var errs []ValidationError
	add := func(format string, args ...any) {
		errs = append(errs, ValidationError{blockName, "meta", fmt.Sprintf(format, args...)})
	}
	if meta.Version != metaVersion {
		add("unsupported version %d", meta.Version)
	}
	if meta.MinTime > meta.MaxTime {
		add("minTime %d > maxTime %d", meta.MinTime, meta.MaxTime)
	}
	if !canonicalULID(meta.ULID) {
		add("invalid ULID %q", meta.ULID)
	} else if meta.ULID != blockName {
		add("ULID %q does not match directory %q", meta.ULID, blockName)
	}
	if meta.Compaction.Level < 1 {
		add("invalid compaction level %d", meta.Compaction.Level)
	}
	if len(meta.Compaction.Sources) == 0 {
		add("compaction sources are empty")
	}
	for _, source := range meta.Compaction.Sources {
		if !canonicalULID(source) {
			add("invalid compaction source ULID %q", source)
		}
	}
	return errs
}

func canonicalULID(value string) bool {
	parsed, err := parseULID(value)
	return err == nil && encodeULID(parsed) == value
}

func validateRelationships(blockName string, meta BlockMeta, idx *index.Reader, chunks map[index.ChunkRef]scannedChunk) []ValidationError {
	var errs []ValidationError
	addIndex := func(format string, args ...any) {
		errs = append(errs, ValidationError{blockName, "index", fmt.Sprintf(format, args...)})
	}
	addChunk := func(format string, args ...any) {
		errs = append(errs, ValidationError{blockName, "chunks", fmt.Sprintf(format, args...)})
	}
	addMeta := func(format string, args ...any) {
		errs = append(errs, ValidationError{blockName, "meta", fmt.Sprintf(format, args...)})
	}

	series := idx.Series()
	referenced := make(map[index.ChunkRef]struct{})
	numChunks := 0
	numSamples := 0
	statsComplete := true
	haveSamples := false
	var blockMinT, blockMaxT int64

	for _, entry := range series {
		var previousMaxT int64
		for chunkIndex, chunkMeta := range entry.Chunks {
			numChunks++
			if chunkMeta.MinT > chunkMeta.MaxT {
				addIndex("series %d chunk %d has minT %d > maxT %d", entry.Ref, chunkIndex, chunkMeta.MinT, chunkMeta.MaxT)
			}
			if chunkIndex > 0 && chunkMeta.MinT <= previousMaxT {
				addIndex("series %d chunks are unsorted or overlap at chunk %d", entry.Ref, chunkIndex)
			}
			previousMaxT = chunkMeta.MaxT

			if _, duplicate := referenced[chunkMeta.Ref]; duplicate {
				addIndex("duplicate chunk ref %d", chunkMeta.Ref)
				statsComplete = false
				continue
			}
			referenced[chunkMeta.Ref] = struct{}{}

			chunk, ok := chunks[chunkMeta.Ref]
			if !ok {
				addIndex("series %d chunk %d ref %d is not a chunk entry boundary", entry.Ref, chunkIndex, chunkMeta.Ref)
				statsComplete = false
				continue
			}
			if !chunk.valid {
				statsComplete = false
				continue
			}
			if len(chunk.data) < 2 {
				addChunk("chunk ref %d is too short for an XOR sample count", chunkMeta.Ref)
				statsComplete = false
				continue
			}

			iterator := chunkenc.XORChunkFromBytes(chunk.data).Iterator()
			count := 0
			var firstT, lastT int64
			sorted := true
			for iterator.Next() {
				timestamp, _ := iterator.At()
				if count == 0 {
					firstT = timestamp
				} else if timestamp <= lastT {
					sorted = false
				}
				lastT = timestamp
				count++
			}
			if err := iterator.Err(); err != nil {
				addChunk("chunk ref %d cannot be decoded: %s", chunkMeta.Ref, err)
				statsComplete = false
				continue
			}
			if count == 0 {
				addChunk("chunk ref %d contains no samples", chunkMeta.Ref)
				statsComplete = false
				continue
			}
			if !sorted {
				addChunk("chunk ref %d samples are not strictly increasing", chunkMeta.Ref)
			}
			if chunkMeta.MinT != firstT || chunkMeta.MaxT != lastT {
				addIndex("series %d chunk %d bounds [%d,%d] do not match decoded samples [%d,%d]",
					entry.Ref, chunkIndex, chunkMeta.MinT, chunkMeta.MaxT, firstT, lastT)
			}

			numSamples += count
			if !haveSamples || firstT < blockMinT {
				blockMinT = firstT
			}
			if !haveSamples || lastT > blockMaxT {
				blockMaxT = lastT
			}
			haveSamples = true
		}
	}

	for ref := range chunks {
		if _, ok := referenced[ref]; !ok {
			addChunk("chunk entry ref %d is not referenced by the index", ref)
		}
	}
	if len(series) != meta.Stats.NumSeries {
		addMeta("numSeries %d does not match index count %d", meta.Stats.NumSeries, len(series))
	}
	if numChunks != meta.Stats.NumChunks {
		addMeta("numChunks %d does not match index count %d", meta.Stats.NumChunks, numChunks)
	}
	if numChunks != len(chunks) {
		addIndex("indexed chunk count %d does not match chunk entry count %d", numChunks, len(chunks))
	}
	if statsComplete && numSamples != meta.Stats.NumSamples {
		addMeta("numSamples %d does not match decoded count %d", meta.Stats.NumSamples, numSamples)
	}
	if statsComplete && haveSamples && (meta.MinTime != blockMinT || meta.MaxTime != blockMaxT) {
		addMeta("bounds [%d,%d] do not match decoded samples [%d,%d]", meta.MinTime, meta.MaxTime, blockMinT, blockMaxT)
	}
	if statsComplete && !haveSamples && (meta.MinTime != 0 || meta.MaxTime != 0) {
		addMeta("empty block bounds [%d,%d] must be [0,0]", meta.MinTime, meta.MaxTime)
	}
	return errs
}

// scanChunkSegment checks every entry and returns its exact on-disk boundary.
func scanChunkSegment(data []byte, name string, segment int) (map[index.ChunkRef]scannedChunk, bool, []string) {
	entries := make(map[index.ChunkRef]scannedChunk)
	if len(data) < chunkHeaderLen {
		return entries, false, []string{fmt.Sprintf("segment %s: too short for header", name)}
	}
	magic := binary.BigEndian.Uint32(data[:4])
	if magic != chunkMagic {
		return entries, false, []string{fmt.Sprintf("segment %s: invalid magic %#x", name, magic)}
	}
	if data[4] != chunkVersion {
		return entries, false, []string{fmt.Sprintf("segment %s: unsupported version %d", name, data[4])}
	}

	var errs []string
	complete := true
	off := chunkHeaderLen
	entryIndex := 0
	for off < len(data) {
		entryOffset := off
		if off+chunkEntryHeaderLen > len(data) {
			errs = append(errs, fmt.Sprintf("segment %s entry %d at offset %d: truncated header", name, entryIndex, off))
			complete = false
			break
		}

		dataLen := int(binary.BigEndian.Uint32(data[off : off+4]))
		encoding := data[off+4]
		off += chunkEntryHeaderLen
		if dataLen > len(data)-off-chunkEntryCRCLen {
			errs = append(errs, fmt.Sprintf("segment %s entry %d: truncated data (need %d bytes, have %d)",
				name, entryIndex, dataLen+chunkEntryCRCLen, len(data)-off))
			complete = false
			break
		}

		chunkBytes := data[off : off+dataLen]
		off += dataLen
		wantCRC := binary.BigEndian.Uint32(data[off : off+chunkEntryCRCLen])
		crc := crc32.New(castagnoliTable)
		crc.Write([]byte{encoding})
		crc.Write(chunkBytes)
		crcValid := crc.Sum32() == wantCRC
		if !crcValid {
			errs = append(errs, fmt.Sprintf("segment %s entry %d at offset %d: CRC mismatch", name, entryIndex, entryOffset))
		}
		encodingValid := encoding == encodingXOR
		if !encodingValid {
			errs = append(errs, fmt.Sprintf("segment %s entry %d at offset %d: unsupported encoding %d", name, entryIndex, entryOffset, encoding))
		}

		ref := index.NewChunkRef(uint32(segment), uint32(entryOffset))
		entries[ref] = scannedChunk{data: chunkBytes, valid: crcValid && encodingValid}
		off += chunkEntryCRCLen
		entryIndex++
	}
	return entries, complete, errs
}

// validateChunkSegment is retained for focused segment validation tests.
func validateChunkSegment(data []byte, name string) []string {
	_, _, errs := scanChunkSegment(data, name, parseSegmentName(name))
	return errs
}
