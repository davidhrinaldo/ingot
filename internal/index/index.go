// Package index implements the binary index file format for ingot blocks.
//
// An index file contains four sections:
//
//	[Header 5B] [Symbol Table] [Series] [Postings] [TOC 28B]
//
// The TOC at the end stores offsets to each section. Readers seek to the
// end, read the TOC, and use the offsets to locate each section.
package index

import (
	"errors"

	"git.dvdt.dev/david/ingot/labels"
)

const (
	indexMagic   uint32 = 0x0FACEDB1
	indexVersion byte   = 1
	headerLen           = 5  // 4-byte magic + 1-byte version
	tocLen              = 28 // 3 offsets (24) + CRC (4)
)

var (
	ErrInvalidMagic   = errors.New("index: invalid magic number")
	ErrInvalidVersion = errors.New("index: unsupported version")
	ErrCorruptTOC     = errors.New("index: corrupt TOC (CRC mismatch)")
	ErrCorruptIndex   = errors.New("index: corrupt index data")
	ErrTooShort       = errors.New("index: file too short")
)

// ChunkRef encodes a chunk's location as (segment << 32 | offset).
type ChunkRef uint64

func NewChunkRef(segment, offset uint32) ChunkRef {
	return ChunkRef(uint64(segment)<<32 | uint64(offset))
}

func (r ChunkRef) Segment() uint32 { return uint32(r >> 32) }
func (r ChunkRef) Offset() uint32  { return uint32(r) }

// ChunkMeta describes a chunk's time range and location in a chunk file.
type ChunkMeta struct {
	MinT int64
	MaxT int64
	Ref  ChunkRef
}

// SeriesEntry is the input for writing and the output for reading a series.
type SeriesEntry struct {
	Ref    uint64
	Labels []labels.Label
	Chunks []ChunkMeta
}
