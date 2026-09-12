package wal

import (
	"errors"
	"fmt"
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

// recover validates closed segments without modifying them. It repairs only an
// incomplete record at the end of the final segment. CRC failures are never
// repaired.
func recover(dir string) error {
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}
	if len(segs) == 0 {
		return nil
	}

	for i, idx := range segs {
		truncated, err := recoverSegment(dir, idx, i == len(segs)-1)
		if err != nil {
			return err
		}
		if truncated {
			return syncDir(dir)
		}
	}
	return nil
}

// recoverSegment validates all records in a segment. It repairs corruption at
// the last valid record boundary only when repairTail is true.
func recoverSegment(dir string, index int, repairTail bool) (bool, error) {
	path := segmentPath(dir, index)
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}

	// Walk records, tracking the offset of the last valid boundary.
	validEnd := 0
	off := 0
	var recordErr error
	for off < len(data) {
		_, _, consumed, err := DecodeRecord(data[off:])
		if err != nil {
			// Corruption or truncation at this offset.
			recordErr = err
			break
		}
		off += consumed
		validEnd = off
	}

	if validEnd == len(data) {
		// Entire segment is valid.
		return false, nil
	}
	if errors.Is(recordErr, ErrCorruptRecord) {
		return false, fmt.Errorf("wal: corrupt segment %08d at offset %d: %w", index, validEnd, recordErr)
	}
	if hasValidRecordAfter(data, validEnd) {
		return false, fmt.Errorf("wal: invalid segment %08d at offset %d before a later valid record: %w", index, validEnd, recordErr)
	}
	if !repairTail {
		return false, fmt.Errorf("wal: corrupt closed segment %08d at offset %d: %w", index, validEnd, recordErr)
	}

	// Truncate the file at the last valid boundary.
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return false, err
	}
	if err := f.Truncate(int64(validEnd)); err != nil {
		f.Close()
		return false, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	return true, nil
}

func hasValidRecordAfter(data []byte, off int) bool {
	for start := off + 1; start+recordHeaderSize+recordTrailerSize <= len(data); start++ {
		typ := RecordType(data[start])
		if typ < RecordSeries || typ > RecordCheckpointActivate {
			continue
		}
		if _, _, _, err := DecodeRecord(data[start:]); err == nil {
			return true
		}
	}
	return false
}
