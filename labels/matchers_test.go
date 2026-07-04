package labels

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			require.NoError(t, err)
			assert.Equal(t, tc.want, m.Matches(tc.value))
		})
	}
}

func TestNewMatcherBadRegexp(t *testing.T) {
	_, err := NewMatcher(MatchRegexp, "__name__", "[invalid")
	assert.Error(t, err)
}

func TestMustNewMatcherPanics(t *testing.T) {
	assert.Panics(t, func() {
		MustNewMatcher(MatchRegexp, "__name__", "[invalid")
	})
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
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFromStringsPanicsOnOdd(t *testing.T) {
	assert.Panics(t, func() {
		FromStrings("__name__")
	})
}

func TestMatchTypeString(t *testing.T) {
	assert.Equal(t, "=", MatchEqual.String())
	assert.Equal(t, "!=", MatchNotEqual.String())
	assert.Equal(t, "=~", MatchRegexp.String())
	assert.Equal(t, "!~", MatchNotRegexp.String())
}
