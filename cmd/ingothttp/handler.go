package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"git.dvdt.dev/david/ingot"
	"git.dvdt.dev/david/ingot/labels"
)

type handler struct {
	db *ingot.DB
}

func newHandler(db *ingot.DB) *handler {
	return &handler{db: db}
}

// --- /api/v1/query_range ---

// queryRange handles GET /api/v1/query_range?query=<name>&start=<ms>&end=<ms>
// Returns Prometheus-style JSON matrix results.
func (h *handler) queryRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	query := r.URL.Query().Get("query")
	if query == "" {
		httpError(w, http.StatusBadRequest, "missing query parameter")
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	mint := int64(math.MinInt64)
	maxt := int64(math.MaxInt64)

	if startStr != "" {
		v, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			httpError(w, http.StatusBadRequest, "invalid start: "+err.Error())
			return
		}
		mint = v
	}
	if endStr != "" {
		v, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			httpError(w, http.StatusBadRequest, "invalid end: "+err.Error())
			return
		}
		maxt = v
	}

	matcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", query)
	result, err := h.executeQuery(mint, maxt, []*labels.Matcher{matcher})
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := queryRangeResponse{
		Status: "success",
		Data: queryRangeData{
			ResultType: "matrix",
			Result:     result,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --- /api/v1/read ---

// ReadRequest is a JSON-encoded query for the read endpoint.
type ReadRequest struct {
	Queries []ReadQuery `json:"queries"`
}

// ReadQuery describes a single read query.
type ReadQuery struct {
	StartTimestampMs int64         `json:"startTimestampMs"`
	EndTimestampMs   int64         `json:"endTimestampMs"`
	Matchers         []ReadMatcher `json:"matchers"`
}

// ReadMatcher is a label matcher in a read request.
type ReadMatcher struct {
	Type  string `json:"type"` // "=", "!=", "=~", "!~"
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ReadResponse is the JSON response for the read endpoint.
type ReadResponse struct {
	Results []ReadResult `json:"results"`
}

// ReadResult contains the timeseries for a single query.
type ReadResult struct {
	Timeseries []timeseriesResult `json:"timeseries"`
}

// read handles POST /api/v1/read with a JSON ReadRequest body.
func (h *handler) read(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	resp := ReadResponse{
		Results: make([]ReadResult, len(req.Queries)),
	}

	for i, rq := range req.Queries {
		matchers, err := convertMatchers(rq.Matchers)
		if err != nil {
			httpError(w, http.StatusBadRequest, err.Error())
			return
		}

		result, err := h.executeQuery(rq.StartTimestampMs, rq.EndTimestampMs, matchers)
		if err != nil {
			httpError(w, http.StatusInternalServerError, err.Error())
			return
		}

		ts := make([]timeseriesResult, len(result))
		for j, sr := range result {
			ts[j] = timeseriesResult{
				Labels:  sr.Metric,
				Samples: sr.Values,
			}
		}
		resp.Results[i] = ReadResult{Timeseries: ts}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --- /api/v1/status ---

type statusResponse struct {
	HeadSeries      int `json:"headSeries"`
	HeadChunks      int `json:"headChunks"`
	Blocks          int `json:"blocks"`
	Compactions     int `json:"compactions"`
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats := h.db.Stats()
	resp := statusResponse{
		HeadSeries:  stats.HeadSeries,
		HeadChunks:  stats.HeadChunks,
		Blocks:      stats.Blocks,
		Compactions: stats.Compactions,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --- shared helpers ---

type queryRangeResponse struct {
	Status string         `json:"status"`
	Data   queryRangeData `json:"data"`
}

type queryRangeData struct {
	ResultType string         `json:"resultType"`
	Result     []seriesResult `json:"result"`
}

type seriesResult struct {
	Metric map[string]string `json:"metric"`
	Values [][2]interface{}  `json:"values"` // [timestamp_ms, "value"]
}

type timeseriesResult struct {
	Labels  map[string]string `json:"labels"`
	Samples [][2]interface{}  `json:"samples"`
}

func (h *handler) executeQuery(mint, maxt int64, matchers []*labels.Matcher) ([]seriesResult, error) {
	q, err := h.db.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	ss := q.Select(matchers...)
	var results []seriesResult

	for ss.Next() {
		s := ss.At()
		ls := s.Labels()
		metric := make(map[string]string, len(ls))
		for _, l := range ls {
			metric[l.Name] = l.Value
		}

		var values [][2]interface{}
		it := s.Iterator()
		for it.Next() {
			t, v := it.At()
			values = append(values, [2]interface{}{t, fmt.Sprintf("%g", v)})
		}
		if it.Err() != nil {
			return nil, it.Err()
		}

		if len(values) > 0 {
			results = append(results, seriesResult{
				Metric: metric,
				Values: values,
			})
		}
	}
	if ss.Err() != nil {
		return nil, ss.Err()
	}

	return results, nil
}

func convertMatchers(rms []ReadMatcher) ([]*labels.Matcher, error) {
	matchers := make([]*labels.Matcher, len(rms))
	for i, rm := range rms {
		var matchType labels.MatchType
		switch rm.Type {
		case "=":
			matchType = labels.MatchEqual
		case "!=":
			matchType = labels.MatchNotEqual
		case "=~":
			matchType = labels.MatchRegexp
		case "!~":
			matchType = labels.MatchNotRegexp
		default:
			return nil, fmt.Errorf("unsupported matcher type: %q", rm.Type)
		}
		m, err := labels.NewMatcher(matchType, rm.Name, rm.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid matcher: %w", err)
		}
		matchers[i] = m
	}
	return matchers, nil
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "error",
		"error":  msg,
	})
}
