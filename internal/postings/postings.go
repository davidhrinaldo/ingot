// Package postings provides set operations on sorted uint64 slices,
// used for combining postings lists from index lookups.
package postings

// Intersect returns the sorted intersection of all input lists.
// An empty input returns nil.
func Intersect(lists ...[]uint64) []uint64 {
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}
	result := lists[0]
	for _, b := range lists[1:] {
		result = intersectTwo(result, b)
	}
	return result
}

func intersectTwo(a, b []uint64) []uint64 {
	var out []uint64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

// Union returns the sorted union of all input lists.
func Union(lists ...[]uint64) []uint64 {
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}
	result := lists[0]
	for _, b := range lists[1:] {
		result = unionTwo(result, b)
	}
	return result
}

func unionTwo(a, b []uint64) []uint64 {
	out := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// Without returns elements in full that are not in remove.
// Both inputs must be sorted.
func Without(full, remove []uint64) []uint64 {
	var out []uint64
	j := 0
	for _, v := range full {
		for j < len(remove) && remove[j] < v {
			j++
		}
		if j < len(remove) && remove[j] == v {
			continue
		}
		out = append(out, v)
	}
	return out
}
