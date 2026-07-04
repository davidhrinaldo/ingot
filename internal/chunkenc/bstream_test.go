package chunkenc

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// bstreamBytes writes (value, nbits) pairs and returns the encoded bytes.
func bstreamBytes(pairs [][2]uint64) []byte {
	var b bstream
	for _, p := range pairs {
		b.writeBits(p[0], int(p[1]))
	}
	return b.bytes()
}

func TestBstream(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))

	type readOp struct {
		nbits   int
		wantVal uint64
		wantErr error
	}

	ok := func(val uint64, nbits int) readOp { return readOp{nbits, val, nil} }
	short := func(nbits int) readOp { return readOp{nbits, 0, ErrShortStream} }

	tests := []struct {
		name  string
		data  []byte
		reads []readOp
	}{
		// Error paths: raw bytes, reads that exceed the data.
		{name: "nil_read_1", data: nil, reads: []readOp{short(1)}},
		{name: "nil_read_64", data: nil, reads: []readOp{short(64)}},
		{name: "nil_read_0", data: nil, reads: []readOp{ok(0, 0)}},
		{name: "1_byte_read_16", data: []byte{0xFF}, reads: []readOp{short(16)}},
		{name: "7_bytes_read_64", data: []byte{1, 2, 3, 4, 5, 6, 7}, reads: []readOp{short(64)}},
		{name: "exact_byte_then_past_end", data: []byte{0xAB}, reads: []readOp{ok(0xAB, 8), short(1)}},
		{name: "two_bytes_then_past_end", data: []byte{0xAB, 0xCD}, reads: []readOp{ok(0xAB, 8), ok(0xCD, 8), short(1)}},
		{name: "partial_bits_then_short", data: []byte{0xFF}, reads: []readOp{ok(0b1111, 4), short(8)}},
		{name: "8_bytes_exact_64", data: []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}, reads: []readOp{ok(0xDEADBEEFCAFEBABE, 64), short(1)}},

		// Round-trip: write via bstream, read back.
		{name: "zero_width", data: bstreamBytes([][2]uint64{{0, 0}}), reads: []readOp{ok(0, 0)}},
		{name: "single_bit_true", data: bstreamBytes([][2]uint64{{1, 1}}), reads: []readOp{ok(1, 1)}},
		{name: "single_bit_false", data: bstreamBytes([][2]uint64{{0, 1}}), reads: []readOp{ok(0, 1)}},
		{name: "single_byte", data: bstreamBytes([][2]uint64{{0xAB, 8}}), reads: []readOp{ok(0xAB, 8)}},
		{name: "full_64_bits", data: bstreamBytes([][2]uint64{{0xDEADBEEFCAFEBABE, 64}}), reads: []readOp{ok(0xDEADBEEFCAFEBABE, 64)}},
		{name: "all_ones_8", data: bstreamBytes([][2]uint64{{0xFF, 8}}), reads: []readOp{ok(0xFF, 8)}},
		{name: "all_zeros_8", data: bstreamBytes([][2]uint64{{0, 8}}), reads: []readOp{ok(0, 8)}},
		{name: "all_ones_64", data: bstreamBytes([][2]uint64{{^uint64(0), 64}}), reads: []readOp{ok(^uint64(0), 64)}},
		{name: "all_zeros_64", data: bstreamBytes([][2]uint64{{0, 64}}), reads: []readOp{ok(0, 64)}},
		{
			name: "byte_aligned_sequence",
			data: bstreamBytes([][2]uint64{{0xAA, 8}, {0xBB, 8}, {0xCC, 8}}),
			reads: []readOp{ok(0xAA, 8), ok(0xBB, 8), ok(0xCC, 8)},
		},
		{
			name: "non_aligned_crossing_boundary",
			data: bstreamBytes([][2]uint64{{0b101, 3}, {0b11001, 5}, {0xFF, 8}, {1, 1}}),
			reads: []readOp{ok(0b101, 3), ok(0b11001, 5), ok(0xFF, 8), ok(1, 1)},
		},
		{
			name: "alternating_single_bits",
			data: bstreamBytes([][2]uint64{{1, 1}, {0, 1}, {1, 1}, {0, 1}, {1, 1}, {0, 1}, {1, 1}, {0, 1}}),
			reads: []readOp{
				ok(1, 1), ok(0, 1), ok(1, 1), ok(0, 1),
				ok(1, 1), ok(0, 1), ok(1, 1), ok(0, 1),
			},
		},
		{
			name: "sequential_64_bit_writes",
			data: bstreamBytes([][2]uint64{{0x0123456789ABCDEF, 64}, {0xFEDCBA9876543210, 64}, {0, 64}}),
			reads: []readOp{ok(0x0123456789ABCDEF, 64), ok(0xFEDCBA9876543210, 64), ok(0, 64)},
		},
		{
			name: "every_width_1_through_64",
			data: func() []byte {
				pairs := make([][2]uint64, 64)
				for i := range pairs {
					nbits := i + 1
					val := uint64(1)
					if nbits > 1 {
						val |= uint64(1) << (nbits - 1)
					}
					pairs[i] = [2]uint64{val, uint64(nbits)}
				}
				return bstreamBytes(pairs)
			}(),
			reads: func() []readOp {
				ops := make([]readOp, 64)
				for i := range ops {
					nbits := i + 1
					val := uint64(1)
					if nbits > 1 {
						val |= uint64(1) << (nbits - 1)
					}
					ops[i] = ok(val, nbits)
				}
				return ops
			}(),
		},
		{
			name: "high_bits_ignored",
			data: bstreamBytes([][2]uint64{{0xFFFFFFFFFFFFFFFF, 1}, {0xFFFFFFFFFFFFFFFF, 4}, {0xFFFFFFFFFFFFFFFF, 8}}),
			reads: []readOp{ok(1, 1), ok(0xF, 4), ok(0xFF, 8)},
		},
		{
			name: "mixed_widths",
			data: bstreamBytes([][2]uint64{{0b1, 1}, {0xABCD, 16}, {0, 1}, {0xFF, 8}, {0b110, 3}, {0xDEADBEEF, 32}, {1, 1}, {0, 0}}),
			reads: []readOp{ok(0b1, 1), ok(0xABCD, 16), ok(0, 1), ok(0xFF, 8), ok(0b110, 3), ok(0xDEADBEEF, 32), ok(1, 1), ok(0, 0)},
		},
	}

	// Random round-trip cases.
	for i := 0; i < 100; i++ {
		n := rnd.Intn(200) + 1
		pairs := make([][2]uint64, n)
		reads := make([]readOp, n)
		for j := range pairs {
			nbits := rnd.Intn(64) + 1
			val := rnd.Uint64() & (^uint64(0) >> (64 - uint(nbits)))
			pairs[j] = [2]uint64{val, uint64(nbits)}
			reads[j] = ok(val, nbits)
		}
		tests = append(tests, struct {
			name  string
			data  []byte
			reads []readOp
		}{
			name:  fmt.Sprintf("random_%03d", i),
			data:  bstreamBytes(pairs),
			reads: reads,
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newBReader(tc.data)
			for i, op := range tc.reads {
				got, err := r.readBits(op.nbits)
				assert.Equal(t, op.wantVal, got, "op %d val (nbits=%d)", i, op.nbits)
				assert.Equal(t, op.wantErr, err, "op %d err (nbits=%d)", i, op.nbits)
			}
		})
	}
}

func FuzzBstreamReader(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte, nbits uint8) {
		r := newBReader(data)
		n := int(nbits%64) + 1
		for {
			if _, err := r.readBits(n); err != nil {
				break // must terminate via error, never panic
			}
		}
	})
}
