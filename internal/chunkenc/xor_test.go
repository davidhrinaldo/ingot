package chunkenc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// encodeChunk builds a valid XOR chunk from samples and returns its bytes.
func encodeChunk(samples [][2]float64) []byte {
	c := NewXORChunk()
	a, _ := c.Appender()
	for _, s := range samples {
		a.Append(int64(s[0]), s[1])
	}
	return append([]byte(nil), c.Bytes()...)
}

func TestXORChunk(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))

	type readOp struct {
		wantNext  bool
		wantT     int64
		wantVBits uint64
	}

	type testCase struct {
		name            string
		data            []byte
		reads           []readOp
		wantIterErr     error
		maxBytes        int
		wantAppenderErr error
	}

	nonEmptyErr := errors.New("chunkenc: appender on non-empty chunk")

	// successReads: one successful read per sample, then a terminal Next()=false.
	successReads := func(samples [][2]float64) []readOp {
		ops := make([]readOp, len(samples)+1)
		for i, s := range samples {
			ops[i] = readOp{true, int64(s[0]), math.Float64bits(s[1])}
		}
		term := readOp{wantNext: false}
		if len(samples) > 0 {
			last := samples[len(samples)-1]
			term.wantT = int64(last[0])
			term.wantVBits = math.Float64bits(last[1])
		}
		ops[len(samples)] = term
		return ops
	}

	// --- Build sample sets for named round-trip cases ---

	adversarialVals := []float64{
		math.NaN(), math.Inf(1), math.Inf(-1), 0, math.Copysign(0, -1),
		math.MaxFloat64, math.SmallestNonzeroFloat64, -1e300,
	}
	adversarial := make([][2]float64, len(adversarialVals))
	for i, v := range adversarialVals {
		adversarial[i] = [2]float64{float64(1000 + i*15), v}
	}

	jitteryTimes := []int64{1000, 1015, 1030, 1031, 1500, 1501, 200000, 200015, 9000000000}
	jittery := make([][2]float64, len(jitteryTimes))
	for i, ts := range jitteryTimes {
		jittery[i] = [2]float64{float64(ts), float64(i) * 1.1}
	}

	singleSample := [][2]float64{{1000, 71.3}}
	repeatedValue := [][2]float64{{1000, 71.3}, {1015, 71.3}}
	changedValue := [][2]float64{{1000, 71.3}, {1015, 71.4}}
	traceSeq := [][2]float64{{1000, 71.3}, {1015, 71.3}, {1030, 71.4}, {1045, 71.4}}

	tests := []testCase{
		// --- Round-trip: encode then decode ---
		{
			name:            "single_sample",
			data:            encodeChunk(singleSample),
			reads:           successReads(singleSample),
			wantIterErr:     nil,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			name:            "repeated_value",
			data:            encodeChunk(repeatedValue),
			reads:           successReads(repeatedValue),
			wantIterErr:     nil,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			name:            "changed_value",
			data:            encodeChunk(changedValue),
			reads:           successReads(changedValue),
			wantIterErr:     nil,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			name:            "trace_sequence",
			data:            encodeChunk(traceSeq),
			reads:           successReads(traceSeq),
			wantIterErr:     nil,
			maxBytes:        30,
			wantAppenderErr: nonEmptyErr,
		},
		{
			name:            "adversarial_values",
			data:            encodeChunk(adversarial),
			reads:           successReads(adversarial),
			wantIterErr:     nil,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			name:            "negative_and_jittery_dods",
			data:            encodeChunk(jittery),
			reads:           successReads(jittery),
			wantIterErr:     nil,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},

		// --- Error paths: truncated or empty data ---
		{
			name:            "zero_samples",
			data:            []byte{0, 0},
			reads:           []readOp{{false, 0, 0}},
			wantIterErr:     nil,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nil, // empty chunk, Appender succeeds
		},
		{
			// Header claims 1 sample, no data after header.
			// Case 0: readBits(64) for timestamp fails immediately.
			name:            "no_data_after_header",
			data:            []byte{0, 1},
			reads:           []readOp{{false, 0, 0}},
			wantIterErr:     ErrShortStream,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			// Header claims 1, only 4 bytes of data (partial timestamp).
			// Case 0: readBits(64) for timestamp fails mid-read.
			name: "truncated_first_timestamp",
			data: func() []byte {
				d := make([]byte, 6) // 2 header + 4 data
				binary.BigEndian.PutUint16(d, 1)
				return d
			}(),
			reads:           []readOp{{false, 0, 0}},
			wantIterErr:     ErrShortStream,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			// Header claims 1, full timestamp but only 4 bytes of value.
			// Case 0: readBits(64) for value fails; it.t and it.v not set
			// (assigned only after both reads succeed).
			name: "truncated_first_value",
			data: func() []byte {
				d := make([]byte, 14) // 2 header + 8 timestamp + 4 partial value
				binary.BigEndian.PutUint16(d, 1)
				binary.BigEndian.PutUint64(d[2:], uint64(1000))
				return d
			}(),
			reads:           []readOp{{false, 0, 0}},
			wantIterErr:     ErrShortStream,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			// Header claims 2, data for exactly 1 sample (16 bytes).
			// Case 1: readBits(14) for delta fails (no data remains).
			// At() returns last successful sample.
			name: "truncated_second_delta",
			data: func() []byte {
				d := make([]byte, 18) // 2 header + 16 data = exactly 1 sample
				binary.BigEndian.PutUint16(d, 2)
				binary.BigEndian.PutUint64(d[2:], uint64(1000))
				binary.BigEndian.PutUint64(d[10:], math.Float64bits(71.3))
				return d
			}(),
			reads: []readOp{
				{true, 1000, math.Float64bits(71.3)},
				{false, 1000, math.Float64bits(71.3)},
			},
			wantIterErr:     ErrShortStream,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			// Header claims 2. Data: 1 valid sample + 14-bit delta + value-changed(1) +
			// new-window(1), then truncated before leading-zeros field.
			// Case 1: delta succeeds (it.t updated), readVDelta fails inside
			// new-window branch at readBits(5).
			name: "truncated_second_vdelta",
			data: func() []byte {
				d := make([]byte, 20) // 2 header + 18 data bytes (144 bits)
				binary.BigEndian.PutUint16(d, 2)
				binary.BigEndian.PutUint64(d[2:], uint64(1000))
				binary.BigEndian.PutUint64(d[10:], math.Float64bits(71.3))
				// Bits 128-141: 14-bit delta = 15 (0b00000000001111)
				// Byte 18 (bits 128-135): 0x00
				// Byte 19 (bits 136-143):
				//   136-141 = bottom 6 bits of delta (001111)
				//   142 = value-changed (1)
				//   143 = new-window (1)
				//   = 0b00111111 = 0x3F
				d[19] = 0x3F
				return d
			}(),
			reads: []readOp{
				{true, 1000, math.Float64bits(71.3)},
				{false, 1015, math.Float64bits(71.3)}, // t advanced, v unchanged
			},
			wantIterErr:     ErrShortStream,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
		{
			// Header claims 3. Data: 2 valid samples (same value, so sample 1
			// is 14-bit delta + 1-bit value-unchanged = 15 bits). Total data =
			// 143 bits in 18 bytes (1 padding bit). Sample 2 DoD prefix reads
			// the padding zero (dod=0, t advances), then readVDelta fails.
			name: "truncated_third_vdelta",
			data: func() []byte {
				d := make([]byte, 20) // 2 header + 18 data (144 bits)
				binary.BigEndian.PutUint16(d, 3)
				binary.BigEndian.PutUint64(d[2:], uint64(1000))
				binary.BigEndian.PutUint64(d[10:], math.Float64bits(71.3))
				// Bits 128-141: 14-bit delta = 15
				// Bit 142: value-unchanged (0)
				// Bit 143: padding (0)
				// Byte 18 = 0x00, Byte 19 = 0b00111100 = 0x3C
				d[19] = 0x3C
				return d
			}(),
			reads: []readOp{
				{true, 1000, math.Float64bits(71.3)},
				{true, 1015, math.Float64bits(71.3)},
				{false, 1030, math.Float64bits(71.3)}, // dod=0 decoded from padding, vdelta fails
			},
			wantIterErr:     ErrShortStream,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		},
	}

	// Random walk round-trip cases.
	for i := 0; i < 100; i++ {
		var samples [][2]float64
		ts, v := int64(rnd.Intn(1e6)), rnd.Float64()*100
		for j := 0; j < 120; j++ {
			samples = append(samples, [2]float64{float64(ts), v})
			ts += 15000 + int64(rnd.Intn(100)) - 50
			v += rnd.Float64() - 0.5
		}
		tests = append(tests, testCase{
			name:            fmt.Sprintf("random_walk_%03d", i),
			data:            encodeChunk(samples),
			reads:           successReads(samples),
			wantIterErr:     nil,
			maxBytes:        math.MaxInt,
			wantAppenderErr: nonEmptyErr,
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &XORChunk{b: bstream{stream: tc.data}}

			it := c.Iterator()
			for i, r := range tc.reads {
				next := it.Next()
				gotT, gotV := it.At()
				assert.Equal(t, r.wantNext, next, "read %d Next()", i)
				assert.Equal(t, r.wantT, gotT, "read %d timestamp", i)
				assert.Equal(t, r.wantVBits, math.Float64bits(gotV), "read %d value", i)
			}
			assert.Equal(t, tc.wantIterErr, it.Err())

			assert.LessOrEqual(t, len(tc.data), tc.maxBytes)

			_, appErr := c.Appender()
			assert.Equal(t, tc.wantAppenderErr, appErr)
		})
	}
}

func TestXORChunkFromBytes(t *testing.T) {
	rnd := rand.New(rand.NewSource(99))

	tests := []struct {
		name    string
		samples [][2]float64
	}{
		{"single", [][2]float64{{1000, 71.3}}},
		{"two", [][2]float64{{1000, 71.3}, {1015, 71.4}}},
		{"full_chunk", func() [][2]float64 {
			out := make([][2]float64, 120)
			ts, v := int64(0), 70.0
			for i := range out {
				out[i] = [2]float64{float64(ts), v}
				ts += 15000 + int64(rnd.Intn(100)) - 50
				v += rnd.Float64() - 0.5
			}
			return out
		}()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orig := NewXORChunk()
			a, _ := orig.Appender()
			for _, s := range tc.samples {
				a.Append(int64(s[0]), s[1])
			}

			reconstituted := XORChunkFromBytes(orig.Bytes())

			assert.Equal(t, orig.NumSamples(), reconstituted.NumSamples())
			assert.Equal(t, orig.Bytes(), reconstituted.Bytes())

			// Verify iteration produces identical samples.
			it := reconstituted.Iterator()
			for i, s := range tc.samples {
				assert.True(t, it.Next(), "sample %d", i)
				gotT, gotV := it.At()
				assert.Equal(t, int64(s[0]), gotT, "sample %d t", i)
				assert.Equal(t, math.Float64bits(s[1]), math.Float64bits(gotV), "sample %d v", i)
			}
			assert.False(t, it.Next())
			assert.NoError(t, it.Err())

			// Appender on non-empty reconstituted chunk should fail.
			_, err := reconstituted.Appender()
			assert.Error(t, err)
		})
	}
}

func FuzzXORIterator(f *testing.F) {
	c := NewXORChunk()
	a, _ := c.Appender()
	a.Append(1000, 71.3)
	a.Append(1015, 71.4)
	f.Add(c.Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		chunk := &XORChunk{b: bstream{stream: data}}
		it := chunk.Iterator()
		for it.Next() {
		}
		// Termination without panic is the only assertion.
	})
}

// benchChunk fills a chunk with 120 samples from gen and returns it.
func benchChunk(gen func(i int) float64) *XORChunk {
	c := NewXORChunk()
	a, _ := c.Appender()
	ts := int64(0)
	for i := 0; i < 120; i++ {
		a.Append(ts, gen(i))
		ts += 15000
	}
	return c
}

func BenchmarkAppend(b *testing.B) {
	rnd := rand.New(rand.NewSource(42))
	vals := make([]float64, 120)
	v := 70.0
	for i := range vals {
		vals[i] = v
		v += rnd.Float64() - 0.5
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := NewXORChunk()
		a, _ := c.Appender()
		ts := int64(0)
		for j := 0; j < 120; j++ {
			a.Append(ts, vals[j])
			ts += 15000
		}
	}
	b.ReportMetric(float64(b.N*120)/b.Elapsed().Seconds(), "appends/sec")
}

func TestBytesPerSample(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))

	tests := []struct {
		name string
		max  float64
		gen  func(i int) float64
	}{
		{"constant", 0.5, func(i int) float64 { return 71.3 }},
		{"stepped_sensor", 3.0, func(i int) float64 {
			return 70 + math.Floor(float64(i)/8)*0.1
		}},
		{"integer_counter", 3.0, func(i int) float64 {
			return float64(1000 + i*3)
		}},
		{"full_precision_walk", 10.0, func(i int) float64 {
			return 70 + rnd.NormFloat64()
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := benchChunk(tc.gen)
			bps := float64(len(c.Bytes())) / 120.0
			t.Logf("%s: %.3f bytes/sample (%d bytes total)", tc.name, bps, len(c.Bytes()))
			assert.LessOrEqual(t, bps, tc.max, "%s bytes/sample exceeds ceiling", tc.name)
		})
	}
}
