package index

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"slices"
	"sort"

	"github.com/davidhrinaldo/ingot/labels"
)

// Reader reads an index from a byte slice (typically mmap'd).
type Reader struct {
	data []byte

	symbols     []string
	series      []SeriesEntry
	seriesByRef map[uint64]int         // ref -> index into series
	postings    map[labelPair][]uint64 // label pair -> sorted refs
}

// NewReader parses an index from data.
func NewReader(data []byte) (*Reader, error) {
	if len(data) < headerLen+tocLen {
		return nil, ErrTooShort
	}

	// Validate header.
	magic := binary.BigEndian.Uint32(data[:4])
	if magic != indexMagic {
		return nil, ErrInvalidMagic
	}
	if data[4] != indexVersion {
		return nil, ErrInvalidVersion
	}

	// Read TOC from the last 28 bytes.
	tocStart := len(data) - tocLen
	toc := data[tocStart:]

	// Validate TOC CRC.
	wantCRC := binary.BigEndian.Uint32(toc[24:28])
	gotCRC := crc32.Checksum(toc[:24], castagnoliTable)
	if gotCRC != wantCRC {
		return nil, ErrCorruptTOC
	}

	symbolsOffset := binary.BigEndian.Uint64(toc[0:8])
	seriesOffset := binary.BigEndian.Uint64(toc[8:16])
	postingsOffset := binary.BigEndian.Uint64(toc[16:24])
	if symbolsOffset != headerLen ||
		symbolsOffset > seriesOffset || seriesOffset > postingsOffset ||
		postingsOffset > uint64(tocStart) {
		return nil, fmt.Errorf("%w: invalid section offsets", ErrCorruptIndex)
	}
	symbolsOff := int(symbolsOffset)
	seriesOff := int(seriesOffset)
	postingsOff := int(postingsOffset)

	r := &Reader{
		data:        data,
		seriesByRef: make(map[uint64]int),
		postings:    make(map[labelPair][]uint64),
	}

	if err := r.readSymbols(symbolsOff, seriesOff); err != nil {
		return nil, err
	}
	if err := r.readSeries(seriesOff, postingsOff); err != nil {
		return nil, err
	}
	if err := r.readPostings(postingsOff, tocStart); err != nil {
		return nil, err
	}
	if err := r.validatePostings(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Reader) readSymbols(off, limit int) error {
	if off+4 > limit {
		return ErrCorruptIndex
	}
	numSymbols := int(binary.BigEndian.Uint32(r.data[off : off+4]))
	off += 4
	if numSymbols > (limit-off)/2 {
		return ErrCorruptIndex
	}

	for i := 0; i < numSymbols; i++ {
		if off+2 > limit {
			return ErrCorruptIndex
		}
		slen := int(binary.BigEndian.Uint16(r.data[off : off+2]))
		off += 2
		if slen > limit-off {
			return ErrCorruptIndex
		}
		symbol := string(r.data[off : off+slen])
		if i > 0 && r.symbols[i-1] >= symbol {
			return fmt.Errorf("%w: symbols are not unique and sorted", ErrCorruptIndex)
		}
		r.symbols = append(r.symbols, symbol)
		off += slen
	}
	if off != limit {
		return fmt.Errorf("%w: symbol section length mismatch", ErrCorruptIndex)
	}
	return nil
}

func (r *Reader) readSeries(off, limit int) error {
	if off+4 > limit {
		return ErrCorruptIndex
	}
	numSeries := int(binary.BigEndian.Uint32(r.data[off : off+4]))
	off += 4
	if numSeries > (limit-off)/14 {
		return ErrCorruptIndex
	}

	for i := 0; i < numSeries; i++ {
		if off+8 > limit {
			return ErrCorruptIndex
		}
		ref := binary.BigEndian.Uint64(r.data[off : off+8])
		off += 8
		if ref == 0 {
			return fmt.Errorf("%w: invalid series ref 0", ErrCorruptIndex)
		}
		if _, exists := r.seriesByRef[ref]; exists {
			return fmt.Errorf("%w: duplicate series ref %d", ErrCorruptIndex, ref)
		}

		if off+2 > limit {
			return ErrCorruptIndex
		}
		numLabels := int(binary.BigEndian.Uint16(r.data[off : off+2]))
		off += 2
		if numLabels > (limit-off)/8 {
			return ErrCorruptIndex
		}

		var ls []labels.Label
		for j := 0; j < numLabels; j++ {
			if off+8 > limit {
				return ErrCorruptIndex
			}
			nameIdx := int(binary.BigEndian.Uint32(r.data[off : off+4]))
			off += 4
			valueIdx := int(binary.BigEndian.Uint32(r.data[off : off+4]))
			off += 4

			if nameIdx >= len(r.symbols) || valueIdx >= len(r.symbols) {
				return ErrCorruptIndex
			}
			ls = append(ls, labels.Label{Name: r.symbols[nameIdx], Value: r.symbols[valueIdx]})
		}
		if err := labels.Validate(ls); err != nil {
			return fmt.Errorf("%w: series ref %d labels: %v", ErrCorruptIndex, ref, err)
		}

		if off+4 > limit {
			return ErrCorruptIndex
		}
		numChunks := int(binary.BigEndian.Uint32(r.data[off : off+4]))
		off += 4
		if numChunks > (limit-off)/24 {
			return ErrCorruptIndex
		}

		var chunks []ChunkMeta
		for j := 0; j < numChunks; j++ {
			if off+24 > limit {
				return ErrCorruptIndex
			}
			chunk := ChunkMeta{}
			chunk.MinT = int64(binary.BigEndian.Uint64(r.data[off : off+8]))
			off += 8
			chunk.MaxT = int64(binary.BigEndian.Uint64(r.data[off : off+8]))
			off += 8
			chunk.Ref = ChunkRef(binary.BigEndian.Uint64(r.data[off : off+8]))
			off += 8
			chunks = append(chunks, chunk)
		}

		entry := SeriesEntry{Ref: ref, Labels: ls, Chunks: chunks}
		r.seriesByRef[ref] = len(r.series)
		r.series = append(r.series, entry)
	}
	if off != limit {
		return fmt.Errorf("%w: series section length mismatch", ErrCorruptIndex)
	}
	return nil
}

func (r *Reader) readPostings(off, limit int) error {
	if off+4 > limit {
		return ErrCorruptIndex
	}
	numEntries := int(binary.BigEndian.Uint32(r.data[off : off+4]))
	off += 4
	if numEntries > (limit-off)/12 {
		return ErrCorruptIndex
	}

	for i := 0; i < numEntries; i++ {
		if off+12 > limit {
			return ErrCorruptIndex
		}
		nameIdx := int(binary.BigEndian.Uint32(r.data[off : off+4]))
		off += 4
		valueIdx := int(binary.BigEndian.Uint32(r.data[off : off+4]))
		off += 4
		numRefs := int(binary.BigEndian.Uint32(r.data[off : off+4]))
		off += 4

		if nameIdx >= len(r.symbols) || valueIdx >= len(r.symbols) {
			return ErrCorruptIndex
		}

		if numRefs > (limit-off)/8 {
			return ErrCorruptIndex
		}

		var refs []uint64
		for j := 0; j < numRefs; j++ {
			ref := binary.BigEndian.Uint64(r.data[off : off+8])
			off += 8
			if j > 0 && refs[j-1] >= ref {
				return fmt.Errorf("%w: postings refs are not unique and sorted", ErrCorruptIndex)
			}
			refs = append(refs, ref)
		}

		key := labelPair{r.symbols[nameIdx], r.symbols[valueIdx]}
		if _, exists := r.postings[key]; exists {
			return fmt.Errorf("%w: duplicate postings for %s=%q", ErrCorruptIndex, key.name, key.value)
		}
		r.postings[key] = refs
	}
	if off != limit {
		return fmt.Errorf("%w: postings section length mismatch", ErrCorruptIndex)
	}
	return nil
}

func (r *Reader) validatePostings() error {
	expected := make(map[labelPair][]uint64)
	for _, series := range r.series {
		for _, label := range series.Labels {
			key := labelPair{label.Name, label.Value}
			expected[key] = append(expected[key], series.Ref)
		}
	}
	for key := range expected {
		sort.Slice(expected[key], func(i, j int) bool { return expected[key][i] < expected[key][j] })
	}

	for key, refs := range r.postings {
		for _, ref := range refs {
			seriesIdx, ok := r.seriesByRef[ref]
			if !ok {
				return fmt.Errorf("%w: postings for %s=%q reference missing series %d", ErrCorruptIndex, key.name, key.value, ref)
			}
			matched := false
			for _, label := range r.series[seriesIdx].Labels {
				if label.Name == key.name && label.Value == key.value {
					matched = true
					break
				}
			}
			if !matched {
				return fmt.Errorf("%w: postings for %s=%q reference wrong series %d", ErrCorruptIndex, key.name, key.value, ref)
			}
		}
	}

	if len(r.postings) != len(expected) {
		return fmt.Errorf("%w: postings entries do not match series labels", ErrCorruptIndex)
	}
	for key, refs := range expected {
		if !slices.Equal(r.postings[key], refs) {
			return fmt.Errorf("%w: postings for %s=%q do not match series labels", ErrCorruptIndex, key.name, key.value)
		}
	}
	return nil
}

// Symbols returns all symbols in the index.
func (r *Reader) Symbols() []string {
	return r.symbols
}

// Series returns all series entries.
func (r *Reader) Series() []SeriesEntry {
	return r.series
}

// SeriesByRef looks up a series by its ref.
func (r *Reader) SeriesByRef(ref uint64) (SeriesEntry, bool) {
	idx, ok := r.seriesByRef[ref]
	if !ok {
		return SeriesEntry{}, false
	}
	return r.series[idx], true
}

// Postings returns sorted series refs for the given label pair.
func (r *Reader) Postings(name, value string) []uint64 {
	return r.postings[labelPair{name, value}]
}

// LabelValues returns sorted unique values for the given label name.
func (r *Reader) LabelValues(name string) []string {
	seen := make(map[string]struct{})
	for key := range r.postings {
		if key.name == name {
			seen[key.value] = struct{}{}
		}
	}
	vals := make([]string, 0, len(seen))
	for v := range seen {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	return vals
}

// AllPostings returns sorted refs for all series in the index.
func (r *Reader) AllPostings() []uint64 {
	refs := make([]uint64, len(r.series))
	for i, s := range r.series {
		refs[i] = s.Ref
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	return refs
}
