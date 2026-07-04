package wal

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecord(t *testing.T) {
	type decodeResult struct {
		typ      RecordType
		payload  []byte
		consumed int
		err      error
	}

	tests := []struct {
		name   string
		data   []byte // raw bytes to decode
		want   decodeResult
		encode bool // if true, data was produced by EncodeRecord (round-trip test)
	}{
		// --- Round-trip cases ---
		{
			name:   "series_payload",
			data:   EncodeRecord(nil, RecordSeries, []byte{0xDE, 0xAD}),
			want:   decodeResult{RecordSeries, []byte{0xDE, 0xAD}, RecordSize(2), nil},
			encode: true,
		},
		{
			name:   "samples_payload",
			data:   EncodeRecord(nil, RecordSamples, []byte{1, 2, 3, 4, 5}),
			want:   decodeResult{RecordSamples, []byte{1, 2, 3, 4, 5}, RecordSize(5), nil},
			encode: true,
		},
		{
			name:   "empty_payload",
			data:   EncodeRecord(nil, RecordSeries, nil),
			want:   decodeResult{RecordSeries, []byte{}, RecordSize(0), nil},
			encode: true,
		},
		{
			name: "large_payload",
			data: EncodeRecord(nil, RecordSamples, make([]byte, 8192)),
			want: decodeResult{RecordSamples, make([]byte, 8192), RecordSize(8192), nil},
			encode: true,
		},
		{
			name:   "unknown_record_type",
			data:   EncodeRecord(nil, RecordType(255), []byte{0xFF}),
			want:   decodeResult{RecordType(255), []byte{0xFF}, RecordSize(1), nil},
			encode: true,
		},

		// --- Error cases ---
		{
			name: "nil_input",
			data: nil,
			want: decodeResult{0, nil, 0, ErrInvalidRecord},
		},
		{
			name: "empty_input",
			data: []byte{},
			want: decodeResult{0, nil, 0, ErrInvalidRecord},
		},
		{
			name: "truncated_at_type",
			data: []byte{byte(RecordSeries)},
			want: decodeResult{0, nil, 0, ErrInvalidRecord},
		},
		{
			name: "truncated_at_len",
			data: []byte{byte(RecordSeries), 0, 0},
			want: decodeResult{0, nil, 0, ErrInvalidRecord},
		},
		{
			name: "truncated_at_payload",
			data: func() []byte {
				// Header says 10 bytes of payload, but only 4 present.
				d := make([]byte, recordHeaderSize+4)
				d[0] = byte(RecordSeries)
				binary.BigEndian.PutUint32(d[1:], 10)
				return d
			}(),
			want: decodeResult{0, nil, 0, ErrInvalidRecord},
		},
		{
			name: "truncated_at_crc",
			data: func() []byte {
				// Full header + full payload, but missing CRC.
				d := make([]byte, recordHeaderSize+2) // 2-byte payload, no CRC
				d[0] = byte(RecordSeries)
				binary.BigEndian.PutUint32(d[1:], 2)
				return d
			}(),
			want: decodeResult{0, nil, 0, ErrInvalidRecord},
		},
		{
			name: "corrupted_crc",
			data: func() []byte {
				d := EncodeRecord(nil, RecordSeries, []byte{0xAB, 0xCD})
				d[len(d)-1] ^= 0xFF // flip last CRC byte
				return d
			}(),
			want: decodeResult{0, nil, 0, ErrCorruptRecord},
		},
		{
			name: "corrupted_payload",
			data: func() []byte {
				d := EncodeRecord(nil, RecordSeries, []byte{0xAB, 0xCD})
				d[recordHeaderSize] ^= 0xFF // flip first payload byte
				return d
			}(),
			want: decodeResult{0, nil, 0, ErrCorruptRecord},
		},
		{
			name: "corrupted_type_byte",
			data: func() []byte {
				d := EncodeRecord(nil, RecordSeries, []byte{0xAB})
				d[0] ^= 0xFF // flip type byte
				return d
			}(),
			want: decodeResult{0, nil, 0, ErrCorruptRecord},
		},
		{
			name: "corrupted_len_field",
			data: func() []byte {
				d := EncodeRecord(nil, RecordSeries, []byte{0xAB})
				d[1] ^= 0x01 // flip len byte — now claims different length
				return d
			}(),
			want: decodeResult{0, nil, 0, ErrInvalidRecord},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			typ, payload, consumed, err := DecodeRecord(tc.data)
			assert.Equal(t, tc.want.typ, typ, "type")
			assert.Equal(t, tc.want.payload, payload, "payload")
			assert.Equal(t, tc.want.consumed, consumed, "consumed")
			assert.Equal(t, tc.want.err, err, "error")
		})
	}
}

func TestEncodeRecordAppendsToExisting(t *testing.T) {
	prefix := []byte("existing")
	result := EncodeRecord(prefix, RecordSeries, []byte{0x01})
	assert.Equal(t, []byte("existing"), result[:8])

	_, payload, _, err := DecodeRecord(result[8:])
	assert.Equal(t, []byte{0x01}, payload)
	assert.Equal(t, nil, err)
}

func TestRecordSize(t *testing.T) {
	tests := []struct {
		payloadLen int
		want       int
	}{
		{0, 9},
		{1, 10},
		{100, 109},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.want, RecordSize(tc.payloadLen))
	}
}

func TestEncodeCRCCoversHeaderAndPayload(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	rec := EncodeRecord(nil, RecordSamples, payload)

	// Manually compute expected CRC over type+len+payload.
	headerAndPayload := rec[:recordHeaderSize+len(payload)]
	want := crc32.Checksum(headerAndPayload, castagnoliTable)
	got := binary.BigEndian.Uint32(rec[recordHeaderSize+len(payload):])
	assert.Equal(t, want, got)
}
