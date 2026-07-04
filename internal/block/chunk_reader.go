package block

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"git.dvdt.dev/david/ingot/internal/chunkenc"
	"git.dvdt.dev/david/ingot/internal/index"
)

var (
	ErrInvalidChunkMagic   = errors.New("block: invalid chunk file magic")
	ErrInvalidChunkVersion = errors.New("block: unsupported chunk file version")
	ErrCorruptChunk        = errors.New("block: corrupt chunk (CRC mismatch)")
	ErrChunkNotFound       = errors.New("block: chunk ref out of bounds")
)

// chunkReader reads chunk data from mmap'd segment files.
type chunkReader struct {
	segments map[int][]byte // segment index -> mmap'd data
}

func newChunkReader(blockDir string) (*chunkReader, error) {
	chunksDir := filepath.Join(blockDir, chunksDirName)
	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		return nil, err
	}

	cr := &chunkReader{segments: make(map[int][]byte)}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		idx := parseSegmentName(e.Name())
		if idx < 0 {
			continue
		}

		data, err := mmapFile(filepath.Join(chunksDir, e.Name()))
		if err != nil {
			cr.close()
			return nil, fmt.Errorf("block: mmap segment %s: %w", e.Name(), err)
		}

		// Validate header.
		if len(data) < chunkHeaderLen {
			cr.close()
			return nil, ErrInvalidChunkMagic
		}
		magic := binary.BigEndian.Uint32(data[:4])
		if magic != chunkMagic {
			cr.close()
			return nil, ErrInvalidChunkMagic
		}
		if data[4] != chunkVersion {
			cr.close()
			return nil, ErrInvalidChunkVersion
		}

		cr.segments[idx] = data
	}

	return cr, nil
}

// chunkData reads the raw chunk bytes at the given ref, validates CRC.
func (cr *chunkReader) chunkData(ref index.ChunkRef) ([]byte, error) {
	seg := int(ref.Segment())
	off := int(ref.Offset())

	data, ok := cr.segments[seg]
	if !ok {
		return nil, ErrChunkNotFound
	}

	if off+chunkEntryHeaderLen > len(data) {
		return nil, ErrChunkNotFound
	}

	dataLen := int(binary.BigEndian.Uint32(data[off : off+4]))
	encoding := data[off+4]
	off += chunkEntryHeaderLen

	end := off + dataLen + chunkEntryCRCLen
	if end > len(data) {
		return nil, ErrChunkNotFound
	}

	chunkBytes := data[off : off+dataLen]
	off += dataLen

	// Validate CRC.
	wantCRC := binary.BigEndian.Uint32(data[off : off+4])
	crc := crc32.New(castagnoliTable)
	crc.Write([]byte{encoding})
	crc.Write(chunkBytes)
	if crc.Sum32() != wantCRC {
		return nil, ErrCorruptChunk
	}

	return chunkBytes, nil
}

// chunkIterator returns a ChunkIterator for the chunk at the given ref.
func (cr *chunkReader) chunkIterator(ref index.ChunkRef) (chunkenc.ChunkIterator, error) {
	data, err := cr.chunkData(ref)
	if err != nil {
		return nil, err
	}
	return chunkenc.XORChunkFromBytes(data).Iterator(), nil
}

func (cr *chunkReader) close() error {
	for _, data := range cr.segments {
		syscall.Munmap(data)
	}
	cr.segments = nil
	return nil
}

// segmentIndices returns sorted segment indices.
func (cr *chunkReader) segmentIndices() []int {
	idxs := make([]int, 0, len(cr.segments))
	for idx := range cr.segments {
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	return idxs
}

func mmapFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("block: empty file %s", path)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()),
		syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func parseSegmentName(name string) int {
	if len(name) != 6 {
		return -1
	}
	n := 0
	for _, c := range name {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}
