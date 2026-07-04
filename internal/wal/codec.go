package wal

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/davidhrinaldo/ingot/labels"
)

var ErrShortPayload = errors.New("wal: payload too short")

// SeriesRecord is a WAL record that registers a new series.
type SeriesRecord struct {
	Ref    uint64
	Labels []labels.Label
}

// RefSample is a single sample keyed by series ref.
type RefSample struct {
	Ref uint64
	T   int64
	V   float64
}

// EncodeSeriesRecord appends the encoded series record to dst.
//
//	ref(8) | nlabels(4) | for each: namelen(2) name valuelen(2) value
func EncodeSeriesRecord(dst []byte, rec SeriesRecord) []byte {
	n := 8 + 4
	for _, l := range rec.Labels {
		n += 2 + len(l.Name) + 2 + len(l.Value)
	}
	dst = grow(dst, n)
	off := len(dst) - n

	binary.BigEndian.PutUint64(dst[off:], rec.Ref)
	off += 8
	binary.BigEndian.PutUint32(dst[off:], uint32(len(rec.Labels)))
	off += 4

	for _, l := range rec.Labels {
		binary.BigEndian.PutUint16(dst[off:], uint16(len(l.Name)))
		off += 2
		off += copy(dst[off:], l.Name)
		binary.BigEndian.PutUint16(dst[off:], uint16(len(l.Value)))
		off += 2
		off += copy(dst[off:], l.Value)
	}

	return dst
}

// DecodeSeriesRecord decodes a series payload. The returned Labels
// hold copies of the strings (not sub-slices of data).
func DecodeSeriesRecord(data []byte) (SeriesRecord, error) {
	if len(data) < 12 {
		return SeriesRecord{}, ErrShortPayload
	}

	ref := binary.BigEndian.Uint64(data)
	nLabels := int(binary.BigEndian.Uint32(data[8:]))
	off := 12

	ls := make([]labels.Label, nLabels)
	for i := range ls {
		if off+2 > len(data) {
			return SeriesRecord{}, ErrShortPayload
		}
		nameLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+nameLen > len(data) {
			return SeriesRecord{}, ErrShortPayload
		}
		name := string(data[off : off+nameLen])
		off += nameLen

		if off+2 > len(data) {
			return SeriesRecord{}, ErrShortPayload
		}
		valueLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+valueLen > len(data) {
			return SeriesRecord{}, ErrShortPayload
		}
		value := string(data[off : off+valueLen])
		off += valueLen

		ls[i] = labels.Label{Name: name, Value: value}
	}

	return SeriesRecord{Ref: ref, Labels: ls}, nil
}

// EncodeSamplesRecord appends the encoded samples record to dst.
//
//	nsamples(4) | for each: ref(8) t(8) v(8)
func EncodeSamplesRecord(dst []byte, samples []RefSample) []byte {
	n := 4 + len(samples)*24
	dst = grow(dst, n)
	off := len(dst) - n

	binary.BigEndian.PutUint32(dst[off:], uint32(len(samples)))
	off += 4

	for _, s := range samples {
		binary.BigEndian.PutUint64(dst[off:], s.Ref)
		off += 8
		binary.BigEndian.PutUint64(dst[off:], uint64(s.T))
		off += 8
		binary.BigEndian.PutUint64(dst[off:], math.Float64bits(s.V))
		off += 8
	}

	return dst
}

// DecodeSamplesRecord decodes a samples payload.
func DecodeSamplesRecord(data []byte) ([]RefSample, error) {
	if len(data) < 4 {
		return nil, ErrShortPayload
	}

	n := int(binary.BigEndian.Uint32(data))
	off := 4

	if len(data) < 4+n*24 {
		return nil, ErrShortPayload
	}

	samples := make([]RefSample, n)
	for i := range samples {
		samples[i].Ref = binary.BigEndian.Uint64(data[off:])
		off += 8
		samples[i].T = int64(binary.BigEndian.Uint64(data[off:]))
		off += 8
		samples[i].V = math.Float64frombits(binary.BigEndian.Uint64(data[off:]))
		off += 8
	}

	return samples, nil
}
