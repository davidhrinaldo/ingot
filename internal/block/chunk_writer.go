package block

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/davidhrinaldo/ingot/internal/index"
)

const (
	chunkMagic          uint32 = 0x0FACE5C4
	chunkVersion        byte   = 1
	chunkHeaderLen             = 5 // 4-byte magic + 1-byte version
	chunkEntryHeaderLen        = 5 // 4-byte length + 1-byte encoding
	chunkEntryCRCLen           = 4
	encodingXOR         byte   = 1
	chunkSegmentMaxSize        = 512 * 1024 * 1024 // 512 MiB
	chunksDirName              = "chunks"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// chunkWriter writes chunk data to segmented chunk files inside a block directory.
type chunkWriter struct {
	dir        string
	segmentIdx int
	segmentOff int
	f          *os.File
}

func newChunkWriter(blockDir string) (*chunkWriter, error) {
	chunksDir := filepath.Join(blockDir, chunksDirName)
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return nil, err
	}
	cw := &chunkWriter{dir: chunksDir, segmentIdx: 1}
	if err := cw.newSegment(); err != nil {
		return nil, err
	}
	return cw, nil
}

func (cw *chunkWriter) newSegment() error {
	if cw.f != nil {
		if err := cw.f.Sync(); err != nil {
			return err
		}
		if err := cw.f.Close(); err != nil {
			return err
		}
	}

	name := segmentName(cw.segmentIdx)
	f, err := os.Create(filepath.Join(cw.dir, name))
	if err != nil {
		return err
	}
	cw.f = f
	cw.segmentOff = 0

	// Write chunk file header.
	var hdr [chunkHeaderLen]byte
	binary.BigEndian.PutUint32(hdr[:4], chunkMagic)
	hdr[4] = chunkVersion
	n, err := f.Write(hdr[:])
	cw.segmentOff += n
	return err
}

// writeChunk writes a chunk entry and returns its ChunkRef.
// Format: dataLen(4) | encoding(1) | data(dataLen) | CRC32C(4)
func (cw *chunkWriter) writeChunk(data []byte) (index.ChunkRef, error) {
	entrySize := chunkEntryHeaderLen + len(data) + chunkEntryCRCLen

	// Rotate if this entry would exceed segment max size.
	if cw.segmentOff+entrySize > chunkSegmentMaxSize {
		cw.segmentIdx++
		if err := cw.newSegment(); err != nil {
			return 0, err
		}
	}

	ref := index.NewChunkRef(uint32(cw.segmentIdx), uint32(cw.segmentOff))

	// Header: length + encoding.
	var hdr [chunkEntryHeaderLen]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(data)))
	hdr[4] = encodingXOR
	if _, err := cw.f.Write(hdr[:]); err != nil {
		return 0, err
	}

	// Chunk data.
	if _, err := cw.f.Write(data); err != nil {
		return 0, err
	}

	// CRC over encoding + data.
	crc := crc32.New(castagnoliTable)
	crc.Write(hdr[4:5]) // encoding byte
	crc.Write(data)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc.Sum32())
	if _, err := cw.f.Write(crcBuf[:]); err != nil {
		return 0, err
	}

	cw.segmentOff += entrySize
	return ref, nil
}

func (cw *chunkWriter) close() error {
	if cw.f == nil {
		return nil
	}
	if err := cw.f.Sync(); err != nil {
		cw.f.Close()
		return err
	}
	return cw.f.Close()
}

func segmentName(idx int) string {
	var buf [6]byte
	s := idx
	for i := 5; i >= 0; i-- {
		buf[i] = '0' + byte(s%10)
		s /= 10
	}
	return string(buf[:])
}
