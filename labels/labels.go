// Package labels defines the data model for time-series label pairs.
package labels

import (
	"errors"
	"hash/fnv"
	"sort"
	"unicode/utf8"
)

// Label is a name/value pair identifying a time series.
type Label struct {
	Name  string
	Value string
}

var (
	ErrEmptyName      = errors.New("labels: empty label name")
	ErrInvalidUTF8    = errors.New("labels: label contains invalid UTF-8")
	ErrDuplicateName  = errors.New("labels: duplicate label name")
)

// Sort sorts labels by name in place and returns them.
func Sort(ls []Label) []Label {
	sort.Slice(ls, func(i, j int) bool { return ls[i].Name < ls[j].Name })
	return ls
}

// Hash returns a 64-bit FNV-1a hash of the sorted label set.
// Labels must be sorted by name before calling.
func Hash(ls []Label) uint64 {
	h := fnv.New64a()
	for _, l := range ls {
		h.Write([]byte(l.Name))
		h.Write([]byte{0})
		h.Write([]byte(l.Value))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// Equal reports whether two sorted label sets are identical.
func Equal(a, b []Label) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Validate checks that labels are sorted, non-empty named, valid UTF-8,
// and have no duplicate names.
func Validate(ls []Label) error {
	for i, l := range ls {
		if l.Name == "" {
			return ErrEmptyName
		}
		if !utf8.ValidString(l.Name) || !utf8.ValidString(l.Value) {
			return ErrInvalidUTF8
		}
		if i > 0 && ls[i-1].Name >= l.Name {
			if ls[i-1].Name == l.Name {
				return ErrDuplicateName
			}
			// Not sorted — caller should sort first.
			return ErrDuplicateName
		}
	}
	return nil
}
