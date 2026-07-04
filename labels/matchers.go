package labels

import (
	"fmt"
	"regexp"
)

// MatchType identifies the type of a label matcher.
type MatchType int

const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (m MatchType) String() string {
	switch m {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	case MatchRegexp:
		return "=~"
	case MatchNotRegexp:
		return "!~"
	default:
		return "??"
	}
}

// Matcher matches label values against a pattern.
type Matcher struct {
	Type  MatchType
	Name  string
	Value string
	re    *regexp.Regexp // compiled for MatchRegexp/MatchNotRegexp
}

// NewMatcher creates a new Matcher. For regex types, the value is compiled
// as a full-match regular expression (anchored with ^(?:...)$).
func NewMatcher(typ MatchType, name, value string) (*Matcher, error) {
	m := &Matcher{Type: typ, Name: name, Value: value}
	if typ == MatchRegexp || typ == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, fmt.Errorf("labels: bad matcher regex %q: %w", value, err)
		}
		m.re = re
	}
	return m, nil
}

// MustNewMatcher is like NewMatcher but panics on error.
func MustNewMatcher(typ MatchType, name, value string) *Matcher {
	m, err := NewMatcher(typ, name, value)
	if err != nil {
		panic(err)
	}
	return m
}

// Matches reports whether the given label value satisfies this matcher.
func (m *Matcher) Matches(v string) bool {
	switch m.Type {
	case MatchEqual:
		return v == m.Value
	case MatchNotEqual:
		return v != m.Value
	case MatchRegexp:
		return m.re.MatchString(v)
	case MatchNotRegexp:
		return !m.re.MatchString(v)
	default:
		return false
	}
}

func (m *Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

// FromStrings creates a sorted label set from alternating name/value pairs.
// Panics if an odd number of strings is provided.
func FromStrings(ss ...string) []Label {
	if len(ss)%2 != 0 {
		panic("labels.FromStrings: odd number of arguments")
	}
	ls := make([]Label, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		ls[i/2] = Label{Name: ss[i], Value: ss[i+1]}
	}
	return Sort(ls)
}
