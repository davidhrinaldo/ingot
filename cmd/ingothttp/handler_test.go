package main

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"git.dvdt.dev/david/ingot"
	"git.dvdt.dev/david/ingot/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *ingot.DB {
	t.Helper()
	db, err := ingot.Open(t.TempDir(), ingot.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func seedDB(t *testing.T, db *ingot.DB) {
	t.Helper()
	app := db.Appender()
	ref, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), 1000, 71.3)
	require.NoError(t, err)
	_, err = app.Append(ref, nil, 2000, 71.4)
	require.NoError(t, err)
	_, err = app.Append(0, labels.FromStrings("__name__", "humidity", "room", "office"), 1000, 55.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
}

func TestQueryRange(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		query      string
		start      string
		end        string
		wantStatus int
		wantCount  int // number of result series; 0 for error responses
	}{
		{
			name:       "match_temp",
			method:     http.MethodGet,
			query:      "temp",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "match_humidity",
			method:     http.MethodGet,
			query:      "humidity",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "no_match",
			method:     http.MethodGet,
			query:      "pressure",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "missing_query",
			method:     http.MethodGet,
			query:      "",
			wantStatus: http.StatusBadRequest,
			wantCount:  0,
		},
		{
			name:       "with_time_range",
			method:     http.MethodGet,
			query:      "temp",
			start:      "1500",
			end:        "3000",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "time_range_miss",
			method:     http.MethodGet,
			query:      "temp",
			start:      "5000",
			end:        "6000",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "method_not_allowed",
			method:     http.MethodPost,
			query:      "temp",
			wantStatus: http.StatusMethodNotAllowed,
			wantCount:  0,
		},
	}

	db := openTestDB(t)
	seedDB(t, db)
	h := newHandler(db)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			url := "/api/v1/query_range?query=" + tc.query
			if tc.start != "" {
				url += "&start=" + tc.start
			}
			if tc.end != "" {
				url += "&end=" + tc.end
			}

			req := httptest.NewRequest(tc.method, url, nil)
			rec := httptest.NewRecorder()
			h.queryRange(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)

			// Always decode — error responses produce zero-valued struct.
			var resp queryRangeResponse
			json.Unmarshal(rec.Body.Bytes(), &resp)
			assert.Equal(t, tc.wantCount, len(resp.Data.Result))
		})
	}
}

func TestRead(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		request    ReadRequest
		wantStatus int
		wantSeries int // in first result; 0 for error responses
	}{
		{
			name:   "single_query",
			method: http.MethodPost,
			request: ReadRequest{
				Queries: []ReadQuery{
					{
						StartTimestampMs: math.MinInt64,
						EndTimestampMs:   math.MaxInt64,
						Matchers: []ReadMatcher{
							{Type: "=", Name: "__name__", Value: "temp"},
						},
					},
				},
			},
			wantStatus: http.StatusOK,
			wantSeries: 1,
		},
		{
			name:   "regex_matcher",
			method: http.MethodPost,
			request: ReadRequest{
				Queries: []ReadQuery{
					{
						StartTimestampMs: math.MinInt64,
						EndTimestampMs:   math.MaxInt64,
						Matchers: []ReadMatcher{
							{Type: "=~", Name: "__name__", Value: "temp|humidity"},
						},
					},
				},
			},
			wantStatus: http.StatusOK,
			wantSeries: 2,
		},
		{
			name:   "invalid_matcher_type",
			method: http.MethodPost,
			request: ReadRequest{
				Queries: []ReadQuery{
					{
						Matchers: []ReadMatcher{
							{Type: "??", Name: "a", Value: "b"},
						},
					},
				},
			},
			wantStatus: http.StatusBadRequest,
			wantSeries: 0,
		},
		{
			name:       "method_not_allowed",
			method:     http.MethodGet,
			request:    ReadRequest{},
			wantStatus: http.StatusMethodNotAllowed,
			wantSeries: 0,
		},
	}

	db := openTestDB(t)
	seedDB(t, db)
	h := newHandler(db)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(tc.request)
			require.NoError(t, err)

			req := httptest.NewRequest(tc.method, "/api/v1/read", bytes.NewReader(body))
			rec := httptest.NewRecorder()
			h.read(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)

			// Always decode — error responses produce zero-valued struct.
			var resp ReadResponse
			json.Unmarshal(rec.Body.Bytes(), &resp)
			firstResultLen := 0
			if len(resp.Results) > 0 {
				firstResultLen = len(resp.Results[0].Timeseries)
			}
			assert.Equal(t, tc.wantSeries, firstResultLen)
		})
	}
}

func TestStatus(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		seed           bool
		wantStatus     int
		wantHeadSeries int
		wantBlocks     int
	}{
		{
			name:           "seeded_db",
			method:         http.MethodGet,
			seed:           true,
			wantStatus:     http.StatusOK,
			wantHeadSeries: 2,
			wantBlocks:     0,
		},
		{
			name:           "empty_db",
			method:         http.MethodGet,
			seed:           false,
			wantStatus:     http.StatusOK,
			wantHeadSeries: 0,
			wantBlocks:     0,
		},
		{
			name:           "method_not_allowed",
			method:         http.MethodPost,
			seed:           false,
			wantStatus:     http.StatusMethodNotAllowed,
			wantHeadSeries: 0,
			wantBlocks:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			if tc.seed {
				seedDB(t, db)
			}
			h := newHandler(db)

			req := httptest.NewRequest(tc.method, "/api/v1/status", nil)
			rec := httptest.NewRecorder()
			h.status(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)

			// Always decode — error responses produce zero-valued struct.
			var resp statusResponse
			json.Unmarshal(rec.Body.Bytes(), &resp)
			assert.Equal(t, tc.wantHeadSeries, resp.HeadSeries)
			assert.Equal(t, tc.wantBlocks, resp.Blocks)
		})
	}
}
