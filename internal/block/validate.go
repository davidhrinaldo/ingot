package block

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"syscall"

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

type chunkScanError struct {
	detail string
	cause  error
}

func (e chunkScanError) Error() string { return e.detail }

func (e chunkScanError) Unwrap() error { return e.cause }

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

	var relationships *relationshipValidator
	if metaErr == nil && idx != nil {
		relationships = newRelationshipValidator(blockName, &meta, idx)
	}
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

			complete, segmentErrs, readErr := validateChunkFile(
				filepath.Join(chunksDir, entry.Name()), entry.Name(), segment, relationships,
			)
			if readErr != nil {
				errs = append(errs, ValidationError{blockName, "chunks",
					fmt.Sprintf("segment %s: %s", entry.Name(), readErr)})
				chunksReadable = false
				continue
			}
			if !complete {
				chunksReadable = false
			}
			for _, scanErr := range segmentErrs {
				errs = append(errs, ValidationError{blockName, "chunks", scanErr.Error()})
			}
		}
	}

	if metaErr == nil && idx != nil && chunksReadable {
		errs = append(errs, relationships.finish()...)
	}
	return errs
}

func validateChunkFile(path, name string, segment int, relationships *relationshipValidator) (bool, []chunkScanError, error) {
	data, err := mmapFile(path)
	if err != nil {
		return false, nil, err
	}
	defer syscall.Munmap(data)

	parsed, complete, errs := scanChunkSegment(data, name, segment)
	if relationships != nil {
		relationships.observe(parsed)
	}
	return complete, errs, nil
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

type chunkExpectation struct {
	seriesRef  uint64
	chunkIndex int
	meta       index.ChunkMeta
}

type relationshipValidator struct {
	blockName string
	meta      *BlockMeta
	errs      []ValidationError

	expected      map[index.ChunkRef]chunkExpectation
	expectedOrder []index.ChunkRef
	observed      map[index.ChunkRef]struct{}
	unexpected    []index.ChunkRef

	numSeries     int
	numChunks     int
	numEntries    int
	numSamples    int
	statsComplete bool
	haveSamples   bool
	blockMinT     int64
	blockMaxT     int64
}

func newRelationshipValidator(blockName string, meta *BlockMeta, idx *index.Reader) *relationshipValidator {
	v := &relationshipValidator{
		blockName:     blockName,
		meta:          meta,
		expected:      make(map[index.ChunkRef]chunkExpectation),
		observed:      make(map[index.ChunkRef]struct{}),
		numSeries:     len(idx.Series()),
		statsComplete: true,
	}
	for _, entry := range idx.Series() {
		for chunkIndex, chunkMeta := range entry.Chunks {
			v.numChunks++
			if chunkMeta.MinT > chunkMeta.MaxT {
				v.addIndex("series %d chunk %d has minT %d > maxT %d", entry.Ref, chunkIndex, chunkMeta.MinT, chunkMeta.MaxT)
			}

			if _, duplicate := v.expected[chunkMeta.Ref]; duplicate {
				v.addIndex("duplicate chunk ref %d", chunkMeta.Ref)
				v.statsComplete = false
				continue
			}
			v.expected[chunkMeta.Ref] = chunkExpectation{entry.Ref, chunkIndex, chunkMeta}
			v.expectedOrder = append(v.expectedOrder, chunkMeta.Ref)
		}
	}
	return v
}

func (v *relationshipValidator) addIndex(format string, args ...any) {
	v.errs = append(v.errs, ValidationError{v.blockName, "index", fmt.Sprintf(format, args...)})
}

func (v *relationshipValidator) addChunk(format string, args ...any) {
	v.errs = append(v.errs, ValidationError{v.blockName, "chunks", fmt.Sprintf(format, args...)})
}

func (v *relationshipValidator) addMeta(format string, args ...any) {
	v.errs = append(v.errs, ValidationError{v.blockName, "meta", fmt.Sprintf(format, args...)})
}

func (v *relationshipValidator) observe(chunks map[index.ChunkRef]scannedChunk) {
	for ref, chunk := range chunks {
		v.numEntries++
		v.observed[ref] = struct{}{}
		expected, ok := v.expected[ref]
		if !ok {
			v.unexpected = append(v.unexpected, ref)
			continue
		}
		if !chunk.valid {
			v.statsComplete = false
			continue
		}
		v.decode(expected, chunk.data)
	}
}

func (v *relationshipValidator) decode(expected chunkExpectation, data []byte) {
	if len(data) < 2 {
		v.addChunk("chunk ref %d is too short for an XOR sample count", expected.meta.Ref)
		v.statsComplete = false
		return
	}

	iterator := chunkenc.XORIteratorFromBytes(data)
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
		v.addChunk("chunk ref %d cannot be decoded: %s", expected.meta.Ref, err)
		v.statsComplete = false
		return
	}
	if count == 0 {
		v.addChunk("chunk ref %d contains no samples", expected.meta.Ref)
		v.statsComplete = false
		return
	}
	if !sorted {
		v.addChunk("chunk ref %d samples are not strictly increasing", expected.meta.Ref)
	}
	if expected.meta.MinT != firstT || expected.meta.MaxT != lastT {
		v.addIndex("series %d chunk %d bounds [%d,%d] do not match decoded samples [%d,%d]",
			expected.seriesRef, expected.chunkIndex, expected.meta.MinT, expected.meta.MaxT, firstT, lastT)
	}

	v.numSamples += count
	if !v.haveSamples || firstT < v.blockMinT {
		v.blockMinT = firstT
	}
	if !v.haveSamples || lastT > v.blockMaxT {
		v.blockMaxT = lastT
	}
	v.haveSamples = true
}

func (v *relationshipValidator) finish() []ValidationError {
	for _, ref := range v.expectedOrder {
		if _, ok := v.observed[ref]; !ok {
			expected := v.expected[ref]
			v.addIndex("series %d chunk %d ref %d is not a chunk entry boundary", expected.seriesRef, expected.chunkIndex, ref)
			v.statsComplete = false
		}
	}
	sort.Slice(v.unexpected, func(i, j int) bool { return v.unexpected[i] < v.unexpected[j] })
	for _, ref := range v.unexpected {
		v.addChunk("chunk entry ref %d is not referenced by the index", ref)
	}
	if v.numSeries != v.meta.Stats.NumSeries {
		v.addMeta("numSeries %d does not match index count %d", v.meta.Stats.NumSeries, v.numSeries)
	}
	if v.numChunks != v.meta.Stats.NumChunks {
		v.addMeta("numChunks %d does not match index count %d", v.meta.Stats.NumChunks, v.numChunks)
	}
	if v.numChunks != v.numEntries {
		v.addIndex("indexed chunk count %d does not match chunk entry count %d", v.numChunks, v.numEntries)
	}
	if v.statsComplete && v.numSamples != v.meta.Stats.NumSamples {
		v.addMeta("numSamples %d does not match decoded count %d", v.meta.Stats.NumSamples, v.numSamples)
	}
	if v.statsComplete && v.haveSamples && (v.meta.MinTime != v.blockMinT || v.meta.MaxTime != v.blockMaxT) {
		if v.meta.Version == 1 && v.meta.MinTime == v.blockMinT && v.meta.MaxTime == 0 && v.blockMaxT < 0 {
			v.meta.MaxTime = v.blockMaxT
		} else {
			v.addMeta("bounds [%d,%d] do not match decoded samples [%d,%d]", v.meta.MinTime, v.meta.MaxTime, v.blockMinT, v.blockMaxT)
		}
	}
	if v.statsComplete && !v.haveSamples && (v.meta.MinTime != 0 || v.meta.MaxTime != 0) {
		v.addMeta("empty block bounds [%d,%d] must be [0,0]", v.meta.MinTime, v.meta.MaxTime)
	}
	return v.errs
}

func validateRelationships(blockName string, meta *BlockMeta, idx *index.Reader, chunks map[index.ChunkRef]scannedChunk) []ValidationError {
	v := newRelationshipValidator(blockName, meta, idx)
	v.observe(chunks)
	return v.finish()
}

// scanChunkSegment checks every entry and returns its exact on-disk boundary.
func scanChunkSegment(data []byte, name string, segment int) (map[index.ChunkRef]scannedChunk, bool, []chunkScanError) {
	entries := make(map[index.ChunkRef]scannedChunk)
	if len(data) < chunkHeaderLen {
		return entries, false, []chunkScanError{{detail: fmt.Sprintf("segment %s: too short for header", name)}}
	}
	magic := binary.BigEndian.Uint32(data[:4])
	if magic != chunkMagic {
		return entries, false, []chunkScanError{{
			detail: fmt.Sprintf("segment %s: invalid magic %#x", name, magic),
			cause:  ErrInvalidChunkMagic,
		}}
	}
	if data[4] != chunkVersion {
		return entries, false, []chunkScanError{{
			detail: fmt.Sprintf("segment %s: unsupported version %d", name, data[4]),
			cause:  ErrInvalidChunkVersion,
		}}
	}

	var errs []chunkScanError
	complete := true
	off := chunkHeaderLen
	entryIndex := 0
	for off < len(data) {
		entryOffset := off
		if off+chunkEntryHeaderLen > len(data) {
			errs = append(errs, chunkScanError{detail: fmt.Sprintf("segment %s entry %d at offset %d: truncated header", name, entryIndex, off)})
			complete = false
			break
		}

		dataLen := int(binary.BigEndian.Uint32(data[off : off+4]))
		encoding := data[off+4]
		off += chunkEntryHeaderLen
		if dataLen > len(data)-off-chunkEntryCRCLen {
			errs = append(errs, chunkScanError{detail: fmt.Sprintf("segment %s entry %d: truncated data (need %d bytes, have %d)",
				name, entryIndex, dataLen+chunkEntryCRCLen, len(data)-off)})
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
			errs = append(errs, chunkScanError{
				detail: fmt.Sprintf("segment %s entry %d at offset %d: CRC mismatch", name, entryIndex, entryOffset),
				cause:  ErrCorruptChunk,
			})
		}
		encodingValid := encoding == encodingXOR
		if !encodingValid {
			errs = append(errs, chunkScanError{
				detail: fmt.Sprintf("segment %s entry %d at offset %d: unsupported encoding %d", name, entryIndex, entryOffset, encoding),
				cause:  ErrInvalidChunkEncoding,
			})
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
	_, _, scanErrs := scanChunkSegment(data, name, parseSegmentName(name))
	errs := make([]string, len(scanErrs))
	for i, err := range scanErrs {
		errs[i] = err.Error()
	}
	return errs
}
