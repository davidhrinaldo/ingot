// Package labels defines the data model for time-series label pairs.
package labels

// Label is a name/value pair identifying a time series.
type Label struct {
	Name  string
	Value string
}
