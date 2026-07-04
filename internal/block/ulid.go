package block

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strings"
	"time"
)

// ULID encoding alphabet (Crockford's Base32).
const ulidEncoding = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newULID generates a ULID with the current time and random entropy.
// Zero external dependencies — the encoding is simple enough to inline.
func newULID() string {
	var b [16]byte

	// Timestamp: upper 48 bits = milliseconds since epoch.
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	// Randomness: lower 80 bits.
	rand.Read(b[6:])

	return encodeULID(b)
}

// parseULID decodes a ULID string back to its 16-byte binary form.
func parseULID(s string) ([16]byte, error) {
	var b [16]byte
	if len(s) != 26 {
		return b, errInvalidULID
	}

	// Decode 26 base32 chars -> 130 bits -> 16 bytes + 2 spare bits.
	var val [26]byte
	for i := 0; i < 26; i++ {
		idx := strings.IndexByte(ulidEncoding, s[i])
		if idx < 0 {
			// Try lowercase.
			idx = strings.IndexByte(ulidEncoding, s[i]&^0x20)
		}
		if idx < 0 {
			return b, errInvalidULID
		}
		val[i] = byte(idx)
	}

	// Pack 5-bit groups into bytes.
	b[0] = val[0]<<5 | val[1]
	b[1] = val[2]<<3 | val[3]>>2
	b[2] = val[3]<<6 | val[4]<<1 | val[5]>>4
	b[3] = val[5]<<4 | val[6]>>1
	b[4] = val[6]<<7 | val[7]<<2 | val[8]>>3
	b[5] = val[8]<<5 | val[9]
	b[6] = val[10]<<3 | val[11]>>2
	b[7] = val[11]<<6 | val[12]<<1 | val[13]>>4
	b[8] = val[13]<<4 | val[14]>>1
	b[9] = val[14]<<7 | val[15]<<2 | val[16]>>3
	b[10] = val[16]<<5 | val[17]
	b[11] = val[18]<<3 | val[19]>>2
	b[12] = val[19]<<6 | val[20]<<1 | val[21]>>4
	b[13] = val[21]<<4 | val[22]>>1
	b[14] = val[22]<<7 | val[23]<<2 | val[24]>>3
	b[15] = val[24]<<5 | val[25]

	return b, nil
}

// ulidTime extracts the millisecond timestamp from a ULID string.
func ulidTime(s string) (int64, error) {
	b, err := parseULID(s)
	if err != nil {
		return 0, err
	}
	// Timestamp is in the upper 6 bytes.
	ms := int64(binary.BigEndian.Uint64(append([]byte{0, 0}, b[:6]...)))
	return ms, nil
}

func encodeULID(b [16]byte) string {
	// 16 bytes = 128 bits, encoded in 26 base32 chars (130 bits, 2 spare).
	var dst [26]byte
	dst[0] = ulidEncoding[(b[0]&0xE0)>>5]
	dst[1] = ulidEncoding[b[0]&0x1F]
	dst[2] = ulidEncoding[(b[1]&0xF8)>>3]
	dst[3] = ulidEncoding[(b[1]&0x07)<<2|(b[2]&0xC0)>>6]
	dst[4] = ulidEncoding[(b[2]&0x3E)>>1]
	dst[5] = ulidEncoding[(b[2]&0x01)<<4|(b[3]&0xF0)>>4]
	dst[6] = ulidEncoding[(b[3]&0x0F)<<1|(b[4]&0x80)>>7]
	dst[7] = ulidEncoding[(b[4]&0x7C)>>2]
	dst[8] = ulidEncoding[(b[4]&0x03)<<3|(b[5]&0xE0)>>5]
	dst[9] = ulidEncoding[b[5]&0x1F]
	dst[10] = ulidEncoding[(b[6]&0xF8)>>3]
	dst[11] = ulidEncoding[(b[6]&0x07)<<2|(b[7]&0xC0)>>6]
	dst[12] = ulidEncoding[(b[7]&0x3E)>>1]
	dst[13] = ulidEncoding[(b[7]&0x01)<<4|(b[8]&0xF0)>>4]
	dst[14] = ulidEncoding[(b[8]&0x0F)<<1|(b[9]&0x80)>>7]
	dst[15] = ulidEncoding[(b[9]&0x7C)>>2]
	dst[16] = ulidEncoding[(b[9]&0x03)<<3|(b[10]&0xE0)>>5]
	dst[17] = ulidEncoding[b[10]&0x1F]
	dst[18] = ulidEncoding[(b[11]&0xF8)>>3]
	dst[19] = ulidEncoding[(b[11]&0x07)<<2|(b[12]&0xC0)>>6]
	dst[20] = ulidEncoding[(b[12]&0x3E)>>1]
	dst[21] = ulidEncoding[(b[12]&0x01)<<4|(b[13]&0xF0)>>4]
	dst[22] = ulidEncoding[(b[13]&0x0F)<<1|(b[14]&0x80)>>7]
	dst[23] = ulidEncoding[(b[14]&0x7C)>>2]
	dst[24] = ulidEncoding[(b[14]&0x03)<<3|(b[15]&0xE0)>>5]
	dst[25] = ulidEncoding[b[15]&0x1F]
	return string(dst[:])
}

var errInvalidULID = errors.New("block: invalid ULID")
