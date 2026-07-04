package postings

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntersect(t *testing.T) {
	tests := []struct {
		name  string
		lists [][]uint64
		want  []uint64
	}{
		{name: "empty_input", lists: nil, want: nil},
		{name: "single_list", lists: [][]uint64{{1, 2, 3}}, want: []uint64{1, 2, 3}},
		{name: "full_overlap", lists: [][]uint64{{1, 2, 3}, {1, 2, 3}}, want: []uint64{1, 2, 3}},
		{name: "partial_overlap", lists: [][]uint64{{1, 2, 3, 4}, {2, 3, 5}}, want: []uint64{2, 3}},
		{name: "no_overlap", lists: [][]uint64{{1, 2}, {3, 4}}, want: nil},
		{name: "one_empty", lists: [][]uint64{{1, 2, 3}, {}}, want: nil},
		{name: "three_lists", lists: [][]uint64{{1, 2, 3, 4, 5}, {2, 3, 4}, {3, 4, 5}}, want: []uint64{3, 4}},
		{name: "single_element", lists: [][]uint64{{5}, {5}}, want: []uint64{5}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Intersect(tc.lists...)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestUnion(t *testing.T) {
	tests := []struct {
		name  string
		lists [][]uint64
		want  []uint64
	}{
		{name: "empty_input", lists: nil, want: nil},
		{name: "single_list", lists: [][]uint64{{1, 2, 3}}, want: []uint64{1, 2, 3}},
		{name: "full_overlap", lists: [][]uint64{{1, 2, 3}, {1, 2, 3}}, want: []uint64{1, 2, 3}},
		{name: "no_overlap", lists: [][]uint64{{1, 2}, {3, 4}}, want: []uint64{1, 2, 3, 4}},
		{name: "partial_overlap", lists: [][]uint64{{1, 3, 5}, {2, 3, 4}}, want: []uint64{1, 2, 3, 4, 5}},
		{name: "one_empty", lists: [][]uint64{{1, 2}, {}}, want: []uint64{1, 2}},
		{name: "both_empty", lists: [][]uint64{{}, {}}, want: []uint64{}},
		{name: "three_lists", lists: [][]uint64{{1}, {2}, {3}}, want: []uint64{1, 2, 3}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Union(tc.lists...)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestWithout(t *testing.T) {
	tests := []struct {
		name   string
		full   []uint64
		remove []uint64
		want   []uint64
	}{
		{name: "empty_full", full: nil, remove: []uint64{1, 2}, want: nil},
		{name: "empty_remove", full: []uint64{1, 2, 3}, remove: nil, want: []uint64{1, 2, 3}},
		{name: "both_empty", full: nil, remove: nil, want: nil},
		{name: "remove_subset", full: []uint64{1, 2, 3, 4, 5}, remove: []uint64{2, 4}, want: []uint64{1, 3, 5}},
		{name: "remove_all", full: []uint64{1, 2, 3}, remove: []uint64{1, 2, 3}, want: nil},
		{name: "remove_none", full: []uint64{1, 2, 3}, remove: []uint64{4, 5}, want: []uint64{1, 2, 3}},
		{name: "remove_superset", full: []uint64{2, 3}, remove: []uint64{1, 2, 3, 4}, want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Without(tc.full, tc.remove)
			assert.Equal(t, tc.want, got)
		})
	}
}
