package index

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"sort"

	"git.dvdt.dev/david/ingot/labels"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Writer builds an index file from series data.
type Writer struct {
	w   io.Writer
	off int // bytes written so far

	symbols    []string       // ordered symbol list
	symbolIdx  map[string]int // string -> index in symbols
	series     []SeriesEntry
	postings   map[labelPair][]uint64 // label pair -> sorted series refs
}

type labelPair struct {
	name, value string
}

// NewWriter creates a Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:         w,
		symbolIdx: make(map[string]int),
		postings:  make(map[labelPair][]uint64),
	}
}

// AddSeries adds a series to the index. Series must be added before calling
// WriteTo. Labels must be sorted.
func (iw *Writer) AddSeries(entry SeriesEntry) {
	for _, l := range entry.Labels {
		iw.addSymbol(l.Name)
		iw.addSymbol(l.Value)
	}
	iw.series = append(iw.series, entry)

	for _, l := range entry.Labels {
		key := labelPair{l.Name, l.Value}
		iw.postings[key] = append(iw.postings[key], entry.Ref)
	}
}

func (iw *Writer) addSymbol(s string) {
	if _, ok := iw.symbolIdx[s]; ok {
		return
	}
	idx := len(iw.symbols)
	iw.symbols = append(iw.symbols, s)
	iw.symbolIdx[s] = idx
}

// WriteTo writes the complete index file. Returns bytes written and any error.
func (iw *Writer) WriteTo() (int, error) {
	// Sort symbols for deterministic output.
	sort.Strings(iw.symbols)
	iw.symbolIdx = make(map[string]int, len(iw.symbols))
	for i, s := range iw.symbols {
		iw.symbolIdx[s] = i
	}

	iw.off = 0

	// Header.
	if err := iw.writeHeader(); err != nil {
		return iw.off, err
	}

	// Symbol table.
	symbolsOff := iw.off
	if err := iw.writeSymbols(); err != nil {
		return iw.off, err
	}

	// Series.
	seriesOff := iw.off
	if err := iw.writeSeries(); err != nil {
		return iw.off, err
	}

	// Postings.
	postingsOff := iw.off
	if err := iw.writePostings(); err != nil {
		return iw.off, err
	}

	// TOC.
	if err := iw.writeTOC(symbolsOff, seriesOff, postingsOff); err != nil {
		return iw.off, err
	}

	return iw.off, nil
}

func (iw *Writer) write(b []byte) error {
	n, err := iw.w.Write(b)
	iw.off += n
	return err
}

func (iw *Writer) writeHeader() error {
	var buf [headerLen]byte
	binary.BigEndian.PutUint32(buf[:4], indexMagic)
	buf[4] = indexVersion
	return iw.write(buf[:])
}

func (iw *Writer) writeSymbols() error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(iw.symbols)))
	if err := iw.write(buf[:]); err != nil {
		return err
	}

	var lenbuf [2]byte
	for _, s := range iw.symbols {
		binary.BigEndian.PutUint16(lenbuf[:], uint16(len(s)))
		if err := iw.write(lenbuf[:]); err != nil {
			return err
		}
		if err := iw.write([]byte(s)); err != nil {
			return err
		}
	}
	return nil
}

func (iw *Writer) writeSeries() error {
	var buf [8]byte

	// numSeries
	binary.BigEndian.PutUint32(buf[:4], uint32(len(iw.series)))
	if err := iw.write(buf[:4]); err != nil {
		return err
	}

	for _, s := range iw.series {
		// ref
		binary.BigEndian.PutUint64(buf[:8], s.Ref)
		if err := iw.write(buf[:8]); err != nil {
			return err
		}

		// numLabels
		binary.BigEndian.PutUint16(buf[:2], uint16(len(s.Labels)))
		if err := iw.write(buf[:2]); err != nil {
			return err
		}

		// labels as symbol refs
		for _, l := range s.Labels {
			binary.BigEndian.PutUint32(buf[:4], uint32(iw.symbolIdx[l.Name]))
			if err := iw.write(buf[:4]); err != nil {
				return err
			}
			binary.BigEndian.PutUint32(buf[:4], uint32(iw.symbolIdx[l.Value]))
			if err := iw.write(buf[:4]); err != nil {
				return err
			}
		}

		// numChunks
		binary.BigEndian.PutUint32(buf[:4], uint32(len(s.Chunks)))
		if err := iw.write(buf[:4]); err != nil {
			return err
		}

		// chunks
		for _, cm := range s.Chunks {
			binary.BigEndian.PutUint64(buf[:8], uint64(cm.MinT))
			if err := iw.write(buf[:8]); err != nil {
				return err
			}
			binary.BigEndian.PutUint64(buf[:8], uint64(cm.MaxT))
			if err := iw.write(buf[:8]); err != nil {
				return err
			}
			binary.BigEndian.PutUint64(buf[:8], uint64(cm.Ref))
			if err := iw.write(buf[:8]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (iw *Writer) writePostings() error {
	// Sort postings keys for deterministic output.
	keys := make([]labelPair, 0, len(iw.postings))
	for k := range iw.postings {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].value < keys[j].value
	})

	var buf [8]byte

	// numEntries
	binary.BigEndian.PutUint32(buf[:4], uint32(len(keys)))
	if err := iw.write(buf[:4]); err != nil {
		return err
	}

	for _, key := range keys {
		refs := iw.postings[key]

		// nameSymIdx
		binary.BigEndian.PutUint32(buf[:4], uint32(iw.symbolIdx[key.name]))
		if err := iw.write(buf[:4]); err != nil {
			return err
		}
		// valueSymIdx
		binary.BigEndian.PutUint32(buf[:4], uint32(iw.symbolIdx[key.value]))
		if err := iw.write(buf[:4]); err != nil {
			return err
		}

		// Sort refs.
		sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })

		// numRefs
		binary.BigEndian.PutUint32(buf[:4], uint32(len(refs)))
		if err := iw.write(buf[:4]); err != nil {
			return err
		}

		for _, ref := range refs {
			binary.BigEndian.PutUint64(buf[:8], ref)
			if err := iw.write(buf[:8]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (iw *Writer) writeTOC(symbolsOff, seriesOff, postingsOff int) error {
	var buf [tocLen]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(symbolsOff))
	binary.BigEndian.PutUint64(buf[8:16], uint64(seriesOff))
	binary.BigEndian.PutUint64(buf[16:24], uint64(postingsOff))
	crc := crc32.Checksum(buf[:24], castagnoliTable)
	binary.BigEndian.PutUint32(buf[24:28], crc)
	return iw.write(buf[:])
}

// SymbolIndex returns the index of a symbol string, for use in lookups.
func (iw *Writer) SymbolIndex(s string) (int, bool) {
	idx, ok := iw.symbolIdx[s]
	return idx, ok
}

// labelValues extracts the sorted label set for a series, resolving symbol refs.
func resolveLabelNames(ls []labels.Label) []labelPair {
	pairs := make([]labelPair, len(ls))
	for i, l := range ls {
		pairs[i] = labelPair{l.Name, l.Value}
	}
	return pairs
}
