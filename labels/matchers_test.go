package labels

import (
	"reflect"
	"testing"
)

func TestMatcherMatches(t *testing.T) {
	tests := []struct {
		name    string
		typ     MatchType
		pattern string
		value   string
		want    bool
	}{
		// MatchEqual
		{name: "equal_match", typ: MatchEqual, pattern: "foo", value: "foo", want: true},
		{name: "equal_no_match", typ: MatchEqual, pattern: "foo", value: "bar", want: false},
		{name: "equal_empty", typ: MatchEqual, pattern: "", value: "", want: true},
		{name: "equal_empty_vs_nonempty", typ: MatchEqual, pattern: "", value: "x", want: false},

		// MatchNotEqual
		{name: "not_equal_match", typ: MatchNotEqual, pattern: "foo", value: "bar", want: true},
		{name: "not_equal_no_match", typ: MatchNotEqual, pattern: "foo", value: "foo", want: false},
		{name: "not_equal_empty", typ: MatchNotEqual, pattern: "", value: "x", want: true},

		// MatchRegexp
		{name: "regexp_exact", typ: MatchRegexp, pattern: "foo", value: "foo", want: true},
		{name: "regexp_no_match", typ: MatchRegexp, pattern: "foo", value: "foobar", want: false},
		{name: "regexp_alternation", typ: MatchRegexp, pattern: "foo|bar", value: "bar", want: true},
		{name: "regexp_alternation_no_match", typ: MatchRegexp, pattern: "foo|bar", value: "baz", want: false},
		{name: "regexp_wildcard", typ: MatchRegexp, pattern: "fo.*", value: "foobar", want: true},
		{name: "regexp_prefix_anchored", typ: MatchRegexp, pattern: "fo", value: "foo", want: false},
		{name: "regexp_dot_plus", typ: MatchRegexp, pattern: ".+", value: "anything", want: true},
		{name: "regexp_dot_plus_empty", typ: MatchRegexp, pattern: ".+", value: "", want: false},

		// MatchNotRegexp
		{name: "not_regexp_match", typ: MatchNotRegexp, pattern: "foo", value: "bar", want: true},
		{name: "not_regexp_no_match", typ: MatchNotRegexp, pattern: "foo", value: "foo", want: false},
		{name: "not_regexp_alternation", typ: MatchNotRegexp, pattern: "foo|bar", value: "baz", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewMatcher(tc.typ, "__name__", tc.pattern)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := m.Matches(tc.value); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewMatcherErrors(t *testing.T) {
	tests := []struct {
		name    string
		typ     MatchType
		pattern string
	}{
		{name: "bad_regexp_bracket", typ: MatchRegexp, pattern: "[invalid"},
		{name: "bad_not_regexp_bracket", typ: MatchNotRegexp, pattern: "(unclosed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewMatcher(tc.typ, "__name__", tc.pattern)
			if err == nil {
				t.Errorf("expected error")
			}
		})
	}
}

func TestMustNewMatcherPanics(t *testing.T) {
	tests := []struct {
		name    string
		typ     MatchType
		pattern string
	}{
		{name: "bad_regexp", typ: MatchRegexp, pattern: "[invalid"},
		{name: "bad_not_regexp", typ: MatchNotRegexp, pattern: "(unclosed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			panicked := false
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
					}
				}()
				MustNewMatcher(tc.typ, "__name__", tc.pattern)
			}()
			if !panicked {
				t.Errorf("expected panic")
			}
		})
	}
}

func TestFromStrings(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []Label
	}{
		{
			name: "single_pair",
			args: []string{"__name__", "temp"},
			want: []Label{{Name: "__name__", Value: "temp"}},
		},
		{
			name: "multiple_pairs_sorted",
			args: []string{"room", "office", "__name__", "temp"},
			want: []Label{{Name: "__name__", Value: "temp"}, {Name: "room", Value: "office"}},
		},
		{
			name: "empty",
			args: []string{},
			want: []Label{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FromStrings(tc.args...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFromStringsPanics(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "odd_count_one", args: []string{"__name__"}},
		{name: "odd_count_three", args: []string{"__name__", "temp", "room"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			panicked := false
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
					}
				}()
				FromStrings(tc.args...)
			}()
			if !panicked {
				t.Errorf("expected panic")
			}
		})
	}
}

func TestMatchTypeString(t *testing.T) {
	tests := []struct {
		typ  MatchType
		want string
	}{
		{typ: MatchEqual, want: "="},
		{typ: MatchNotEqual, want: "!="},
		{typ: MatchRegexp, want: "=~"},
		{typ: MatchNotRegexp, want: "!~"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.typ.String(); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
