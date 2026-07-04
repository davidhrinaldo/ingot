package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	defaultSegmentMaxSize = 128 * 1024 * 1024 // 128 MiB
	segmentNameLen        = 8                  // "00000001"
)

// segmentFileName returns the zero-padded filename for a segment index.
func segmentFileName(index int) string {
	return fmt.Sprintf("%0*d", segmentNameLen, index)
}

// parseSegmentIndex parses a segment filename back to its index.
// Returns -1 if the name is not a valid segment file.
func parseSegmentIndex(name string) int {
	if len(name) != segmentNameLen {
		return -1
	}
	n, err := strconv.Atoi(name)
	if err != nil {
		return -1
	}
	return n
}

// listSegments returns the sorted indices of all segment files in dir.
func listSegments(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var indices []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		idx := parseSegmentIndex(e.Name())
		if idx >= 0 {
			indices = append(indices, idx)
		}
	}
	sort.Ints(indices)
	return indices, nil
}

// segmentPath returns the full path for a segment index within dir.
func segmentPath(dir string, index int) string {
	return filepath.Join(dir, segmentFileName(index))
}

// createSegment creates a new segment file and returns it open for writing.
func createSegment(dir string, index int) (*os.File, error) {
	return os.OpenFile(segmentPath(dir, index), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
}
