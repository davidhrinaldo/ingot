// Chunk encoding follows Prometheus tsdb/chunkenc. See /NOTICE.md.
package chunkenc

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// ChunkAppender appends samples to a chunk.
type ChunkAppender interface {
	Append(t int64, v float64)
}

// ChunkIterator iterates over samples in a chunk.
type ChunkIterator interface {
	Next() bool
	At() (int64, float64)
	Err() error
}

type XORChunk struct {
	b bstream
}

func NewXORChunk() *XORChunk {
	return &XORChunk{b: bstream{stream: make([]byte, 2), count: 0}}
}

// XORChunkFromBytes creates a read-only XORChunk from raw bytes.
// The data must include the 2-byte sample count header (as returned by Bytes).
// The returned chunk supports Iterator, NumSamples, and Bytes but not Appender.
func XORChunkFromBytes(data []byte) *XORChunk {
	cp := make([]byte, len(data))
	copy(cp, data)
	return &XORChunk{b: bstream{stream: cp}}
}

func (c *XORChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.b.bytes()))
}

func (c *XORChunk) Appender() (ChunkAppender, error) {
	if c.NumSamples() > 0 {
		return nil, errors.New("chunkenc: appender on non-empty chunk")
	}
	return &xorAppender{b: &c.b, leading: 0xff}, nil
}

func (c *XORChunk) Bytes() []byte {
	return c.b.bytes()
}

type xorAppender struct {
	b        *bstream
	t        int64   // last timestamp
	v        float64 // last value
	tDelta   uint64  // last delta
	leading  uint8   // current XOR window
	trailing uint8
}

func (a *xorAppender) Append(t int64, v float64) {
	num := binary.BigEndian.Uint16(a.b.stream)

	switch num {
	case 0:
		// First sample: raw 64-bit timestamp and value.
		a.b.writeBits(uint64(t), 64)
		a.b.writeBits(math.Float64bits(v), 64)

	case 1:
		// Second sample: fixed 14-bit first delta, then XOR value.
		delta := uint64(t - a.t)
		a.b.writeBits(delta, 14)
		a.writeVDelta(v)
		a.tDelta = delta

	default:
		delta := uint64(t - a.t)
		dod := int64(delta) - int64(a.tDelta)

		// Prefix-code ladder. Buckets match the Prometheus variant
		// (14/17/20/64) rather than the paper's (7/9/12/32) - wider
		// buckets tolerate millisecond timestamps and jittery sources.
		switch {
		case dod == 0:
			a.b.writeBit(false)
		case bitRange(dod, 14):
			a.b.writeBits(0b10, 2)
			a.b.writeBits(uint64(dod)&((1<<14)-1), 14)
		case bitRange(dod, 17):
			a.b.writeBits(0b110, 3)
			a.b.writeBits(uint64(dod)&((1<<17)-1), 17)
		case bitRange(dod, 20):
			a.b.writeBits(0b1110, 4)
			a.b.writeBits(uint64(dod)&((1<<20)-1), 20)
		default:
			a.b.writeBits(0b1111, 4)
			a.b.writeBits(uint64(dod), 64)
		}

		a.writeVDelta(v)
		a.tDelta = delta
	}

	a.t = t
	a.v = v
	binary.BigEndian.PutUint16(a.b.stream, num+1)
}

// bitRange reports whether x fits in an nbits-wide two's-complement field.
func bitRange(x int64, nbits int) bool {
	return -(1<<(nbits-1)) <= x && x < 1<<(nbits-1)
}

func (a *xorAppender) writeVDelta(v float64) {
	xor := math.Float64bits(v) ^ math.Float64bits(a.v)

	if xor == 0 {
		a.b.writeBit(false)
		return
	}
	a.b.writeBit(true)

	leading := uint8(bits.LeadingZeros64(xor))
	trailing := uint8(bits.TrailingZeros64(xor))

	// Leading is stored in 5 bits; clamp so it fits.
	if leading > 31 {
		leading = 31
	}

	if a.leading != 0xff && leading >= a.leading && trailing >= a.trailing {
		// New meaningful bits fit inside the previous window: reuse it.
		a.b.writeBit(false)
		a.b.writeBits(xor>>a.trailing, int(64-a.leading-a.trailing))
		return
	}

	// New window.
	a.leading, a.trailing = leading, trailing
	a.b.writeBit(true)
	a.b.writeBits(uint64(leading), 5)

	// sigbits can be 64 only when leading == trailing == 0, which can't
	// happen here (xor != 0 and both counted on the same word), so the
	// 6-bit field always fits... except leading was clamped, so recompute
	// from the clamped values.
	sigbits := 64 - int(leading) - int(trailing)
	a.b.writeBits(uint64(sigbits), 6)
	a.b.writeBits(xor>>trailing, sigbits)
}

// Iterator decodes the chunk. Snapshot semantics: it reads the byte slice
// as it exists at creation; don't append concurrently.
func (c *XORChunk) Iterator() ChunkIterator {
	return &xorIterator{
		br:    newBReader(c.b.bytes()[2:]),
		total: uint16(c.NumSamples()),
	}
}

type xorIterator struct {
	br    bstreamReader
	total uint16
	read  uint16

	t      int64
	v      float64
	tDelta uint64

	leading  uint8
	trailing uint8

	err error
}

func (it *xorIterator) At() (int64, float64) {
	return it.t, it.v
}

func (it *xorIterator) Err() error {
	return it.err
}

func (it *xorIterator) Next() bool {
	if it.err != nil || it.read >= it.total {
		return false
	}

	switch it.read {
	case 0:
		t, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.t = int64(t)
		it.v = math.Float64frombits(v)

	case 1:
		delta, err := it.br.readBits(14)
		if err != nil {
			it.err = err
			return false
		}
		it.tDelta = delta
		it.t += int64(delta)
		if !it.readVDelta() {
			return false
		}

	default:
		// Walk the prefix tree: count 1-bits unitl a 0 or four 1s.
		var d byte
		for i := 0; i < 4; i++ {
			bit, err := it.br.readBit()
			if err != nil {
				it.err = err
				return false
			}
			if !bit {
				break
			}
			d++
		}

		var dod int64
		switch d {
		case 0:
			//dod == 0
		case 1:
			dod = it.readSigned(14)
		case 2:
			dod = it.readSigned(17)
		case 3:
			dod = it.readSigned(20)
		case 4:
			bits64, err := it.br.readBits(64)
			if err != nil {
				it.err = err
				return false
			}
			dod = int64(bits64)
		}
		if it.err != nil {
			return false
		}

		it.tDelta = uint64(int64(it.tDelta) + dod)
		it.t += int64(it.tDelta)
		if !it.readVDelta() {
			return false
		}
	}

	it.read++
	return true
}

// readSigned reads an nbits two's-complement field and sign-extends it.
func (it *xorIterator) readSigned(nbits int) int64 {
	v, err := it.br.readBits(nbits)
	if err != nil {
		it.err = err
		return 0
	}
	return int64(v<<(64-uint(nbits))) >> (64 - uint(nbits))
}

func (it *xorIterator) readVDelta() bool {
	bit, err := it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}
	if !bit {
		// Value unchanged.
		return true
	}

	bit, err = it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit {
		// New window.
		l, err := it.br.readBits(5)
		if err != nil {
			it.err = err
			return false
		}
		s, err := it.br.readBits(6)
		if err != nil {
			it.err = err
			return false
		}
		// sigbits=64 overflows the 6-bit field to 0; unwrap it.
		if s == 0 {
			s = 64
		}
		it.leading = uint8(l)
		it.trailing = uint8(64 - l - s)
	}

	sigbits := int(64 - it.leading - it.trailing)
	xor, err := it.br.readBits(sigbits)
	if err != nil {
		it.err = err
		return false
	}

	vbits := math.Float64bits(it.v)
	vbits ^= xor << it.trailing
	it.v = math.Float64frombits(vbits)
	return true
}
