// Package wal implements a segmented write-ahead log for crash-safe
// persistence of time-series data.
//
// WAL design informed by Prometheus tsdb/wal. See /NOTICE.md.
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// RecordType identifies the kind of record stored in the WAL.
type RecordType byte

const (
	RecordSeries  RecordType = 1
	RecordSamples RecordType = 2
)

const (
	recordHeaderSize  = 5 // type(1) + len(4)
	recordTrailerSize = 4 // crc32(4)
)

var (
	ErrInvalidRecord = errors.New("wal: invalid record")
	ErrCorruptRecord = errors.New("wal: corrupt record (CRC mismatch)")
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// RecordSize returns the total on-disk size of a record with the given payload length.
func RecordSize(payloadLen int) int {
	return recordHeaderSize + payloadLen + recordTrailerSize
}

// EncodeRecord appends a framed record (type + len + payload + crc32c) to dst
// and returns the extended slice.
func EncodeRecord(dst []byte, typ RecordType, payload []byte) []byte {
	n := RecordSize(len(payload))
	dst = grow(dst, n)
	off := len(dst) - n

	dst[off] = byte(typ)
	binary.BigEndian.PutUint32(dst[off+1:], uint32(len(payload)))
	copy(dst[off+recordHeaderSize:], payload)

	checksum := crc32.Checksum(dst[off:off+recordHeaderSize+len(payload)], castagnoliTable)
	binary.BigEndian.PutUint32(dst[off+recordHeaderSize+len(payload):], checksum)

	return dst
}

// DecodeRecord parses a framed record from b. It returns the record type,
// the payload slice (a sub-slice of b), the total number of bytes consumed,
// and any error. On success, consumed == RecordSize(len(payload)).
func DecodeRecord(b []byte) (typ RecordType, payload []byte, consumed int, err error) {
	if len(b) < recordHeaderSize {
		return 0, nil, 0, ErrInvalidRecord
	}

	typ = RecordType(b[0])
	payloadLen := int(binary.BigEndian.Uint32(b[1:]))
	total := RecordSize(payloadLen)

	if len(b) < total {
		return 0, nil, 0, ErrInvalidRecord
	}

	want := crc32.Checksum(b[:recordHeaderSize+payloadLen], castagnoliTable)
	got := binary.BigEndian.Uint32(b[recordHeaderSize+payloadLen:])
	if want != got {
		return 0, nil, 0, ErrCorruptRecord
	}

	return typ, b[recordHeaderSize : recordHeaderSize+payloadLen], total, nil
}

// grow appends n zero bytes to dst and returns the extended slice.
func grow(dst []byte, n int) []byte {
	if cap(dst)-len(dst) >= n {
		return dst[:len(dst)+n]
	}
	buf := make([]byte, len(dst)+n, 2*(len(dst)+n))
	copy(buf, dst)
	return buf
}
