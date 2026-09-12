package ingot_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/davidhrinaldo/ingot"
	"github.com/davidhrinaldo/ingot/internal/block"
	"github.com/davidhrinaldo/ingot/internal/chunkenc"
	"github.com/davidhrinaldo/ingot/labels"
)

type querySample struct {
	t int64
	v float64
}

func TestQueryMergesBlocksBySeriesTimestamp(t *testing.T) {
	dir := t.TempDir()
	sharedLabels := labels.FromStrings("__name__", "shared")

	later := make([]querySample, 121)
	for i := range later {
		later[i] = querySample{t: int64(i + 240), v: float64(i + 240)}
	}
	writeQueryBlock(t, dir,
		querySeries(1, sharedLabels, later),
		querySeries(2, labels.FromStrings("__name__", "first-anchor"), []querySample{{t: -2, v: -2}}),
	)

	middle := make([]querySample, 120)
	for i := range middle {
		middle[i] = querySample{t: int64(i + 120), v: float64(i + 120)}
	}
	writeQueryBlock(t, dir,
		querySeries(1, sharedLabels, middle),
		querySeries(2, labels.FromStrings("__name__", "second-anchor"), []querySample{{t: -1, v: -1}}),
	)

	earlier := make([]querySample, 120)
	for i := range earlier {
		earlier[i] = querySample{t: int64(i), v: float64(i)}
	}
	writeQueryBlock(t, dir, querySeries(1, sharedLabels, earlier))

	db := openQueryDB(t, dir)
	got := querySamples(t, db, "shared")

	want := append(append(earlier, middle...), later...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("samples: got %d/%d samples, want %d", len(got), len(want), len(want))
	}
}

func TestQueryDuplicateTimestampPrecedence(t *testing.T) {
	t.Run("block_beats_head", func(t *testing.T) {
		dir := t.TempDir()
		sharedLabels := labels.FromStrings("__name__", "shared")
		writeQueryBlock(t, dir, querySeries(1, sharedLabels, []querySample{{t: 100, v: 1}}))

		db := openQueryDB(t, dir)
		app := db.Appender()
		if _, err := app.Append(0, sharedLabels, 100, 2); err != nil {
			t.Fatalf("append head sample: %v", err)
		}
		if _, err := app.Append(0, sharedLabels, 200, 3); err != nil {
			t.Fatalf("append head sample: %v", err)
		}
		if err := app.Commit(); err != nil {
			t.Fatalf("commit head sample: %v", err)
		}

		want := []querySample{{t: 100, v: 1}, {t: 200, v: 3}}
		if got := querySamples(t, db, "shared"); !reflect.DeepEqual(got, want) {
			t.Fatalf("samples: got %v, want %v", got, want)
		}
	})

	t.Run("earlier_block_min_time_wins", func(t *testing.T) {
		dir := t.TempDir()
		sharedLabels := labels.FromStrings("__name__", "shared")
		writeQueryBlock(t, dir,
			querySeries(1, sharedLabels, []querySample{{t: 100, v: 1}}),
			querySeries(2, labels.FromStrings("__name__", "early"), []querySample{{t: 0, v: 0}}),
		)
		writeQueryBlock(t, dir,
			querySeries(1, sharedLabels, []querySample{{t: 50, v: 50}, {t: 100, v: 2}}),
		)

		db := openQueryDB(t, dir)
		want := []querySample{{t: 50, v: 50}, {t: 100, v: 1}}
		if got := querySamples(t, db, "shared"); !reflect.DeepEqual(got, want) {
			t.Fatalf("samples: got %v, want %v", got, want)
		}
	})

	t.Run("ULID_breaks_min_time_ties", func(t *testing.T) {
		dir := t.TempDir()
		sharedLabels := labels.FromStrings("__name__", "shared")
		first := writeQueryBlock(t, dir,
			querySeries(1, sharedLabels, []querySample{{t: 100, v: 1}}),
			querySeries(2, labels.FromStrings("__name__", "anchor"), []querySample{{t: 0, v: 0}}),
		)
		second := writeQueryBlock(t, dir,
			querySeries(1, sharedLabels, []querySample{{t: 100, v: 2}}),
			querySeries(2, labels.FromStrings("__name__", "anchor"), []querySample{{t: 0, v: 0}}),
		)

		wantValue := 1.0
		if second < first {
			wantValue = 2
		}
		db := openQueryDB(t, dir)
		want := []querySample{{t: 100, v: wantValue}}
		if got := querySamples(t, db, "shared"); !reflect.DeepEqual(got, want) {
			t.Fatalf("samples: got %v, want %v", got, want)
		}
	})
}

func querySeries(ref uint64, ls []labels.Label, samples []querySample) block.SeriesFlush {
	chunk := chunkenc.NewXORChunk()
	app, err := chunk.Appender()
	if err != nil {
		panic(err)
	}
	for _, sample := range samples {
		app.Append(sample.t, sample.v)
	}
	return block.SeriesFlush{
		Ref:    ref,
		Labels: ls,
		Chunks: []block.ChunkData{{
			MinT: samples[0].t,
			MaxT: samples[len(samples)-1].t,
			Data: append([]byte(nil), chunk.Bytes()...),
		}},
	}
}

func writeQueryBlock(t *testing.T, dir string, series ...block.SeriesFlush) string {
	t.Helper()
	ulid, err := block.Flush(dir, series)
	if err != nil {
		t.Fatalf("flush block: %v", err)
	}
	return ulid
}

func openQueryDB(t *testing.T, dir string) *ingot.DB {
	t.Helper()
	db, err := ingot.Open(dir, ingot.Options{})
	if err != nil {
		t.Fatalf("open DB: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close DB: %v", err)
		}
	})
	return db
}

func querySamples(t *testing.T, db *ingot.DB, name string) []querySample {
	t.Helper()
	q, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("create querier: %v", err)
	}
	defer q.Close()

	set := q.Select(labels.MustNewMatcher(labels.MatchEqual, "__name__", name))
	if !set.Next() {
		t.Fatalf("missing series: %v", set.Err())
	}
	it := set.At().Iterator()
	var samples []querySample
	for it.Next() {
		timestamp, value := it.At()
		samples = append(samples, querySample{t: timestamp, v: value})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterate samples: %v", err)
	}
	if set.Next() {
		t.Fatal("query returned more than one series")
	}
	if err := set.Err(); err != nil {
		t.Fatalf("iterate series: %v", err)
	}
	return samples
}
