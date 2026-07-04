// Chunk encoding follows Prometheus tsdb/chunkenc. See /NOTICE.md.
package chunkenc

import (
	"errors"
)

var ErrShortStream = errors.New("chunkenc: unexpected end of stream")

type bstream struct {
	stream []byte
	count  uint8
}

func (b *bstream) bytes() []byte {
	return b.stream
}

func (b *bstream) writeBit(bit bool) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}
	i := len(b.stream) - 1
	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}
	b.count--
}

func (b *bstream) writeBits(u uint64, nbits int) {
	// Byte-aligned fast path from the top of the value down.
	u <<= 64 - uint(nbits)
	for nbits >= 8 {
		b.writeByte(byte(u >> 56))
		u <<= 8
		nbits -= 8
	}
	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}
	i := len(b.stream) - 1

	// Split across the partial last byte and a fresh one.
	b.stream[i] |= byt >> (8 - b.count)
	b.stream = append(b.stream, 0)
	i++
	b.stream[i] = byt << b.count
	// count unchanged: same number of free bits, now in the new last byte
}

type bstreamReader struct {
	stream []byte
	off    int    // byte offset of next unread byte
	buf    uint64 // bit buffer, MSB-first
	valid  uint8  //valid bits remaining in buf
}

func newBReader(b []byte) bstreamReader {
	return bstreamReader{stream: b}
}

// loadNextBuffer refills buf with up to 8 bytes. Returns false at EOF.
func (r *bstreamReader) loadNextBuffer(nbits uint8) bool {
	if r.off >= len(r.stream) {
		return false
	}

	// Fast path: 8 full bytes available.
	if r.off+8 <= len(r.stream) {
		r.buf = uint64(r.stream[r.off])<<56 |
			uint64(r.stream[r.off+1])<<48 |
			uint64(r.stream[r.off+2])<<40 |
			uint64(r.stream[r.off+3])<<32 |
			uint64(r.stream[r.off+4])<<24 |
			uint64(r.stream[r.off+5])<<16 |
			uint64(r.stream[r.off+6])<<8 |
			uint64(r.stream[r.off+7])
		r.off += 8
		r.valid = 64
		return true
	}

	// Tail: load what's left, left-aligned.
	n := len(r.stream) - r.off
	r.buf = 0
	for i := 0; i < n; i++ {
		r.buf |= uint64(r.stream[r.off+i]) << (56 - 8*uint(i))
	}
	r.off += n
	r.valid = uint8(n * 8)
	return true
}

func (r *bstreamReader) readBit() (bool, error) {
	if r.valid == 0 {
		if !r.loadNextBuffer(1) {
			return false, ErrShortStream
		}
	}
	bit := r.buf&(1<<63) != 0
	r.buf <<= 1
	r.valid--
	return bit, nil
}

func (r *bstreamReader) readBits(nbits int) (uint64, error) {
	if nbits == 0 {
		return 0, nil
	}
	var v uint64
	remaining := uint8(nbits)

	for remaining > 0 {
		if r.valid == 0 {
			if !r.loadNextBuffer(remaining) {
				return 0, ErrShortStream
			}
		}
		take := remaining
		if take > r.valid {
			take = r.valid
		}
		v = (v << take) | (r.buf >> (64 - take))
		r.buf <<= take
		r.valid -= take
		remaining -= take
	}
	return v, nil
}

func (r *bstreamReader) readByte() (byte, error) {
	v, err := r.readBits(8)
	return byte(v), err
}
