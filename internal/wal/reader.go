package wal

import (
	"io"
	"os"
)

// Record is a decoded WAL record returned by the Reader.
type Record struct {
	Type RecordType
	Data []byte // raw payload; decode with DecodeSeriesRecord / DecodeSamplesRecord
}

// Reader scans WAL segments sequentially, validating CRC on each record.
// It follows the iterator pattern: Next() advances, Record() returns the
// current record, Err() returns any error after Next() returns false.
type Reader struct {
	dir      string
	segments []int // sorted segment indices
	segIdx   int   // position in segments slice
	f        *os.File
	buf      []byte // read buffer, grown as needed
	rec      Record
	err      error
}

// NewReader creates a Reader over all segments in dir.
func NewReader(dir string) (*Reader, error) {
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	return &Reader{dir: dir, segments: segs}, nil
}

// Next advances to the next record. Returns false when no more records
// are available or an error is encountered. After Next returns false,
// call Err() to distinguish clean EOF from corruption.
func (r *Reader) Next() bool {
	for {
		if r.err != nil {
			return false
		}

		// Open the next segment file if needed.
		if r.f == nil {
			if !r.openNextSegment() {
				return false
			}
		}

		// Read the record header (type + len).
		header, ok := r.readExact(recordHeaderSize)
		if !ok {
			// EOF or short read at record boundary.
			if r.err == nil {
				// Clean EOF on this segment — try next.
				r.closeFile()
				continue
			}
			return false
		}

		typ := RecordType(header[0])
		payloadLen := int(header[1])<<24 | int(header[2])<<16 | int(header[3])<<8 | int(header[4])

		// Read payload + CRC trailer.
		body, ok := r.readExact(payloadLen + recordTrailerSize)
		if !ok {
			// Torn write: header was read but payload/CRC is truncated.
			if r.err == nil {
				r.err = ErrInvalidRecord
			}
			return false
		}

		// Validate CRC over header + payload.
		full := append(header, body[:payloadLen]...)
		_, _, _, decErr := DecodeRecord(r.reassemble(header, body, payloadLen))
		if decErr != nil {
			r.err = decErr
			return false
		}
		_ = full // replaced by reassemble

		r.rec = Record{Type: typ, Data: cloneBytes(body[:payloadLen])}
		return true
	}
}

// reassemble reconstructs the full framed record from the separately read
// header and body (payload + CRC) for CRC validation via DecodeRecord.
func (r *Reader) reassemble(header []byte, body []byte, payloadLen int) []byte {
	total := recordHeaderSize + payloadLen + recordTrailerSize
	if cap(r.buf) < total {
		r.buf = make([]byte, total)
	}
	r.buf = r.buf[:total]
	copy(r.buf, header)
	copy(r.buf[recordHeaderSize:], body)
	return r.buf
}

// Record returns the most recently read record.
func (r *Reader) Record() Record {
	return r.rec
}

// Err returns the error encountered during reading, if any.
// A nil error after Next() returns false means all records were read cleanly.
func (r *Reader) Err() error {
	return r.err
}

// Close releases any open file handle.
func (r *Reader) Close() error {
	return r.closeFile()
}

func (r *Reader) openNextSegment() bool {
	if r.segIdx >= len(r.segments) {
		return false
	}
	f, err := os.Open(segmentPath(r.dir, r.segments[r.segIdx]))
	if err != nil {
		r.err = err
		return false
	}
	r.f = f
	r.segIdx++
	return true
}

func (r *Reader) closeFile() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

// readExact reads exactly n bytes from the current file. On short read
// at EOF, it sets r.err to ErrInvalidRecord (torn write) and returns false.
// On clean EOF (zero bytes read), it returns false with r.err == nil.
func (r *Reader) readExact(n int) ([]byte, bool) {
	if n == 0 {
		return nil, true
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r.f, buf)
	if err == io.EOF {
		// Clean EOF — no bytes at all.
		return nil, false
	}
	if err == io.ErrUnexpectedEOF {
		// Partial read — torn write.
		r.err = ErrInvalidRecord
		return nil, false
	}
	if err != nil {
		r.err = err
		return nil, false
	}
	return buf, true
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// recover scans all segments, validating records. On the first corrupt or
// truncated record, it truncates the segment file at the start of that record
// and deletes all subsequent segments. Returns the records that survived.
func recover(dir string) error {
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return nil
	}

	for i, idx := range segs {
		truncated, err := recoverSegment(dir, idx)
		if err != nil {
			return err
		}
		if truncated {
			// Delete all segments after this one.
			for _, laterIdx := range segs[i+1:] {
				if err := os.Remove(segmentPath(dir, laterIdx)); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return nil
}

// recoverSegment validates all records in a single segment. If it encounters
// corruption, it truncates the file at the last valid record boundary.
// Returns true if truncation occurred.
func recoverSegment(dir string, index int) (bool, error) {
	path := segmentPath(dir, index)
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}

	// Walk records, tracking the offset of the last valid boundary.
	validEnd := 0
	off := 0
	for off < len(data) {
		_, _, consumed, err := DecodeRecord(data[off:])
		if err != nil {
			// Corruption or truncation at this offset.
			break
		}
		off += consumed
		validEnd = off
	}

	if validEnd == len(data) {
		// Entire segment is valid.
		return false, nil
	}

	// Truncate the file at the last valid boundary.
	if err := os.Truncate(path, int64(validEnd)); err != nil {
		return false, err
	}
	return true, nil
}
