package wal

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"git.dvdt.dev/david/ingot/labels"
)

func TestSeriesRecord(t *testing.T) {
	type result struct {
		rec SeriesRecord
		err error
	}

	tests := []struct {
		name string
		data []byte
		want result
	}{
		{
			name: "single_label",
			data: EncodeSeriesRecord(nil, SeriesRecord{
				Ref:    42,
				Labels: []labels.Label{{Name: "__name__", Value: "temp"}},
			}),
			want: result{SeriesRecord{
				Ref:    42,
				Labels: []labels.Label{{Name: "__name__", Value: "temp"}},
			}, nil},
		},
		{
			name: "multiple_labels",
			data: EncodeSeriesRecord(nil, SeriesRecord{
				Ref: 1,
				Labels: []labels.Label{
					{Name: "__name__", Value: "cpu_usage"},
					{Name: "host", Value: "web-01"},
					{Name: "region", Value: "us-east"},
				},
			}),
			want: result{SeriesRecord{
				Ref: 1,
				Labels: []labels.Label{
					{Name: "__name__", Value: "cpu_usage"},
					{Name: "host", Value: "web-01"},
					{Name: "region", Value: "us-east"},
				},
			}, nil},
		},
		{
			name: "zero_labels",
			data: EncodeSeriesRecord(nil, SeriesRecord{Ref: 99, Labels: nil}),
			want: result{SeriesRecord{Ref: 99, Labels: []labels.Label{}}, nil},
		},
		{
			name: "unicode_labels",
			data: EncodeSeriesRecord(nil, SeriesRecord{
				Ref:    7,
				Labels: []labels.Label{{Name: "名前", Value: "温度"}},
			}),
			want: result{SeriesRecord{
				Ref:    7,
				Labels: []labels.Label{{Name: "名前", Value: "温度"}},
			}, nil},
		},
		{
			name: "empty_label_strings",
			data: EncodeSeriesRecord(nil, SeriesRecord{
				Ref:    1,
				Labels: []labels.Label{{Name: "", Value: ""}},
			}),
			want: result{SeriesRecord{
				Ref:    1,
				Labels: []labels.Label{{Name: "", Value: ""}},
			}, nil},
		},

		// Error cases.
		{
			name: "nil",
			data: nil,
			want: result{SeriesRecord{}, ErrShortPayload},
		},
		{
			name: "truncated_ref",
			data: make([]byte, 6),
			want: result{SeriesRecord{}, ErrShortPayload},
		},
		{
			name: "truncated_nlabels",
			data: make([]byte, 10),
			want: result{SeriesRecord{}, ErrShortPayload},
		},
		{
			name: "truncated_name_len",
			data: func() []byte {
				d := make([]byte, 13) // ref(8) + nlabels=1(4) + 1 byte (short)
				binary.BigEndian.PutUint32(d[8:], 1)
				return d
			}(),
			want: result{SeriesRecord{}, ErrShortPayload},
		},
		{
			name: "truncated_name_data",
			data: func() []byte {
				d := make([]byte, 16) // ref(8) + nlabels=1(4) + namelen=10(2) + 2 bytes
				binary.BigEndian.PutUint32(d[8:], 1)
				binary.BigEndian.PutUint16(d[12:], 10) // claims 10 bytes, only 2 available
				return d
			}(),
			want: result{SeriesRecord{}, ErrShortPayload},
		},
		{
			name: "truncated_value_len",
			data: func() []byte {
				d := make([]byte, 15) // ref(8) + nlabels=1(4) + namelen=0(2) + 1 byte
				binary.BigEndian.PutUint32(d[8:], 1)
				binary.BigEndian.PutUint16(d[12:], 0) // 0-length name
				return d
			}(),
			want: result{SeriesRecord{}, ErrShortPayload},
		},
		{
			name: "truncated_value_data",
			data: func() []byte {
				d := make([]byte, 18) // ref(8) + nlabels=1(4) + namelen=0(2) + vallen=5(2) + 2 bytes
				binary.BigEndian.PutUint32(d[8:], 1)
				binary.BigEndian.PutUint16(d[12:], 0)
				binary.BigEndian.PutUint16(d[14:], 5) // claims 5 bytes, only 2 available
				return d
			}(),
			want: result{SeriesRecord{}, ErrShortPayload},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec, err := DecodeSeriesRecord(tc.data)
			if !reflect.DeepEqual(rec, tc.want.rec) {
				t.Errorf("record: got %v, want %v", rec, tc.want.rec)
			}
			if err != tc.want.err {
				t.Errorf("error: got %v, want %v", err, tc.want.err)
			}
		})
	}
}

// refSampleBits holds a RefSample with the value stored as raw bits
// so NaN and negative zero compare correctly.
type refSampleBits struct {
	Ref   uint64
	T     int64
	VBits uint64
}

func toBits(s RefSample) refSampleBits {
	return refSampleBits{s.Ref, s.T, math.Float64bits(s.V)}
}

func samplesToBits(ss []RefSample) []refSampleBits {
	out := make([]refSampleBits, len(ss))
	for i, s := range ss {
		out[i] = toBits(s)
	}
	return out
}

func TestSamplesRecord(t *testing.T) {
	type result struct {
		samples []refSampleBits
		err     error
	}

	tests := []struct {
		name string
		data []byte
		want result
	}{
		{
			name: "single_sample",
			data: EncodeSamplesRecord(nil, []RefSample{
				{Ref: 1, T: 1000, V: 71.3},
			}),
			want: result{samplesToBits([]RefSample{{Ref: 1, T: 1000, V: 71.3}}), nil},
		},
		{
			name: "multiple_samples",
			data: EncodeSamplesRecord(nil, []RefSample{
				{Ref: 1, T: 1000, V: 71.3},
				{Ref: 1, T: 1015, V: 71.4},
				{Ref: 2, T: 1000, V: 0},
			}),
			want: result{samplesToBits([]RefSample{
				{Ref: 1, T: 1000, V: 71.3},
				{Ref: 1, T: 1015, V: 71.4},
				{Ref: 2, T: 1000, V: 0},
			}), nil},
		},
		{
			name: "zero_samples",
			data: EncodeSamplesRecord(nil, nil),
			want: result{samplesToBits([]RefSample{}), nil},
		},
		{
			name: "special_float_values",
			data: EncodeSamplesRecord(nil, []RefSample{
				{Ref: 1, T: 0, V: math.NaN()},
				{Ref: 2, T: 0, V: math.Inf(1)},
				{Ref: 3, T: 0, V: math.Inf(-1)},
				{Ref: 4, T: 0, V: math.Copysign(0, -1)},
			}),
			want: result{samplesToBits([]RefSample{
				{Ref: 1, T: 0, V: math.NaN()},
				{Ref: 2, T: 0, V: math.Inf(1)},
				{Ref: 3, T: 0, V: math.Inf(-1)},
				{Ref: 4, T: 0, V: math.Copysign(0, -1)},
			}), nil},
		},
		{
			name: "negative_timestamp",
			data: EncodeSamplesRecord(nil, []RefSample{
				{Ref: 1, T: -5000, V: 1.5},
			}),
			want: result{samplesToBits([]RefSample{{Ref: 1, T: -5000, V: 1.5}}), nil},
		},

		// Error cases.
		{
			name: "nil",
			data: nil,
			want: result{nil, ErrShortPayload},
		},
		{
			name: "truncated_count",
			data: []byte{0, 0},
			want: result{nil, ErrShortPayload},
		},
		{
			name: "truncated_mid_sample",
			data: func() []byte {
				d := make([]byte, 20) // nsamples=1(4) + 16 bytes (need 24)
				binary.BigEndian.PutUint32(d, 1)
				return d
			}(),
			want: result{nil, ErrShortPayload},
		},
		{
			name: "count_exceeds_data",
			data: func() []byte {
				d := make([]byte, 28) // nsamples=2(4) + 24 bytes (only 1 sample)
				binary.BigEndian.PutUint32(d, 2)
				return d
			}(),
			want: result{nil, ErrShortPayload},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			samples, err := DecodeSamplesRecord(tc.data)
			got := samplesToBits(samples)
			if err != tc.want.err {
				t.Errorf("error: got %v, want %v", err, tc.want.err)
			}
			if len(got) != len(tc.want.samples) {
				t.Fatalf("sample count: got %d, want %d", len(got), len(tc.want.samples))
			}
			for i := range tc.want.samples {
				if got[i] != tc.want.samples[i] {
					t.Errorf("sample %d: got %v, want %v", i, got[i], tc.want.samples[i])
				}
			}
		})
	}
}
