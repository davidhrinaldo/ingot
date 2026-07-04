package index

import (
	"encoding/binary"
	"hash/crc32"

	"git.dvdt.dev/david/ingot/labels"
)

// Reader reads an index from a byte slice (typically mmap'd).
type Reader struct {
	data []byte

	symbols     []string
	series      []SeriesEntry
	seriesByRef map[uint64]int            // ref -> index into series
	postings    map[labelPair][]uint64    // label pair -> sorted refs
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

	symbolsOff := int(binary.BigEndian.Uint64(toc[0:8]))
	seriesOff := int(binary.BigEndian.Uint64(toc[8:16]))
	postingsOff := int(binary.BigEndian.Uint64(toc[16:24]))

	r := &Reader{
		data:        data,
		seriesByRef: make(map[uint64]int),
		postings:    make(map[labelPair][]uint64),
	}

	if err := r.readSymbols(symbolsOff); err != nil {
		return nil, err
	}
	if err := r.readSeries(seriesOff); err != nil {
		return nil, err
	}
	if err := r.readPostings(postingsOff, tocStart); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Reader) readSymbols(off int) error {
	if off+4 > len(r.data) {
		return ErrCorruptIndex
	}
	numSymbols := int(binary.BigEndian.Uint32(r.data[off : off+4]))
	off += 4

	r.symbols = make([]string, 0, numSymbols)
	for i := 0; i < numSymbols; i++ {
		if off+2 > len(r.data) {
			return ErrCorruptIndex
		}
		slen := int(binary.BigEndian.Uint16(r.data[off : off+2]))
		off += 2
		if off+slen > len(r.data) {
			return ErrCorruptIndex
		}
		r.symbols = append(r.symbols, string(r.data[off:off+slen]))
		off += slen
	}
	return nil
}

func (r *Reader) readSeries(off int) error {
	if off+4 > len(r.data) {
		return ErrCorruptIndex
	}
	numSeries := int(binary.BigEndian.Uint32(r.data[off : off+4]))
	off += 4

	r.series = make([]SeriesEntry, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		if off+8 > len(r.data) {
			return ErrCorruptIndex
		}
		ref := binary.BigEndian.Uint64(r.data[off : off+8])
		off += 8

		if off+2 > len(r.data) {
			return ErrCorruptIndex
		}
		numLabels := int(binary.BigEndian.Uint16(r.data[off : off+2]))
		off += 2

		ls := make([]labels.Label, numLabels)
		for j := 0; j < numLabels; j++ {
			if off+8 > len(r.data) {
				return ErrCorruptIndex
			}
			nameIdx := int(binary.BigEndian.Uint32(r.data[off : off+4]))
			off += 4
			valueIdx := int(binary.BigEndian.Uint32(r.data[off : off+4]))
			off += 4

			if nameIdx >= len(r.symbols) || valueIdx >= len(r.symbols) {
				return ErrCorruptIndex
			}
			ls[j] = labels.Label{Name: r.symbols[nameIdx], Value: r.symbols[valueIdx]}
		}

		if off+4 > len(r.data) {
			return ErrCorruptIndex
		}
		numChunks := int(binary.BigEndian.Uint32(r.data[off : off+4]))
		off += 4

		chunks := make([]ChunkMeta, numChunks)
		for j := 0; j < numChunks; j++ {
			if off+24 > len(r.data) {
				return ErrCorruptIndex
			}
			chunks[j].MinT = int64(binary.BigEndian.Uint64(r.data[off : off+8]))
			off += 8
			chunks[j].MaxT = int64(binary.BigEndian.Uint64(r.data[off : off+8]))
			off += 8
			chunks[j].Ref = ChunkRef(binary.BigEndian.Uint64(r.data[off : off+8]))
			off += 8
		}

		entry := SeriesEntry{Ref: ref, Labels: ls, Chunks: chunks}
		r.seriesByRef[ref] = len(r.series)
		r.series = append(r.series, entry)
	}
	return nil
}

func (r *Reader) readPostings(off, limit int) error {
	if off+4 > limit {
		return ErrCorruptIndex
	}
	numEntries := int(binary.BigEndian.Uint32(r.data[off : off+4]))
	off += 4

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

		if off+numRefs*8 > limit {
			return ErrCorruptIndex
		}

		refs := make([]uint64, numRefs)
		for j := 0; j < numRefs; j++ {
			refs[j] = binary.BigEndian.Uint64(r.data[off : off+8])
			off += 8
		}

		key := labelPair{r.symbols[nameIdx], r.symbols[valueIdx]}
		r.postings[key] = refs
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
