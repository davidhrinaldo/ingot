# ingot

An embedded time-series database for Go. SQLite for metrics.

```go
db, _ := ingot.Open("./data", ingot.Options{
    Retention:     30 * 24 * time.Hour,
    BlockDuration: 2 * time.Hour,
})

// Write
app := db.Appender()
ref, _ := app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), ts, 71.3)
app.Append(ref, nil, ts+15000, 71.4) // ref fast-path skips label hashing
app.Commit()

// Read
q, _ := db.Querier(mint, maxt)
ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "room", "office"))
for ss.Next() {
    it := ss.At().Iterator()
    for it.Next() {
        t, v := it.At()
        _ = t; _ = v
    }
}
q.Close()

db.Close()
```

## Status

**Alpha.** The API is frozen (M4) and the system survives a 48h soak test under sustained load (M5), but this hasn't seen production use yet.

| Milestone | State |
|---|---|
| M1 — Gorilla chunk encoding, fuzzed, benchmarked | Done |
| M2 — Head + WAL, kill -9 safe | Done |
| M3 — Immutable blocks, mmap reads | Done |
| M4 — Query path, API freeze, shippable alpha | Done |
| M5 — Compaction + retention, 48h soak | Done |
| M6 — ingotctl, HTTP layer, self-instrumentation | Done |

See [DESIGN.md](DESIGN.md) for architecture, on-disk format, and the non-goals table. See [ROADMAP.md](ROADMAP.md) for what's next.

## Why

I needed a library to store time-series data locally and the options out there didn't quite work for my case. Prometheus was too heavy for what I needed but I wanted that level of compression. tstorage was close but it didn't have the compression or label indexing.

## Install

```sh
go get github.com/davidhrinaldo/ingot
```

## Features

- **Gorilla XOR compression** — ~1 byte/sample on regular metric data (see benchmarks below)
- **Crash-safe** — WAL with CRC32C records. Committed data is persisted
- **Query by label matchers** — equality, negation, regex, negative regex; merged across head and blocks
- **Levelled compaction** — 2h → 8h → 32h blocks, background merging, retention-based expiry
- **Self-instrumentation** — the DB records its own metrics (series/chunk counts, compactions, WAL fsync duration) through the normal write path, queryable like any other series
- **Zero external dependencies**

## Tools

### ingotctl

CLI for block inspection and diagnostics:

```sh
ingotctl blocks ./data                    # list blocks with ULID, time range, stats
ingotctl inspect ./data/01HXYZ.../        # series labels, chunk metadata, postings stats
ingotctl chunks ./data/01HXYZ.../ 42      # decode and print raw samples for series ref 42
ingotctl fsck ./data                      # CRC + index integrity check on all blocks
```

### ingothttp

Minimal HTTP query server for Grafana integration:

```sh
ingothttp -data ./data -addr :9001
```

Endpoints:
- `GET /api/v1/query_range?query=<name>&start=<ms>&end=<ms>` — Prometheus-style JSON matrix response
- `POST /api/v1/read` — JSON read request with label matchers
- `GET /api/v1/status` — DB stats snapshot

This is a demo/bridge, not a full PromQL engine. The query parameter matches `__name__` by equality.

## Compression

Chunk encoding is Gorilla (Pelkonen et al., [VLDB 2015](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf)): delta-of-delta timestamps, XOR floats. Measured on 120-sample chunks at a regular 15s interval:

| Workload | bytes/sample |
|---|---|
| Constant value | 0.42 |
| Stepped sensor (repeats, occasional 0.1 steps) | ~1.0 |
| Integer counter | ~2 |
| Full-precision random walk (adversarial) | ~7.5 |

Regenerate: `go test -v -run TestBytesPerSample ./internal/chunkenc/`

Two things worth knowing about XOR compression that the headline numbers hide:

- Decimal quantization doesn't help. 0.1-precision readings have mantissas as dirty as full-precision ones — 0.1 is non-terminating in binary. Compression comes from exact repeats (1 bit) and integer values (clean trailing zeros), not from "roundness" in base 10.
- The famous 1.37 bytes/sample figure assumes production metric traffic, where roughly half of consecutive values repeat exactly. Your data may not look like that; the adversarial row is what you pay when it doesn't.

## Layout

```
ingot/                  public API: Open, Appender, Querier
├── internal/
│   ├── chunkenc/       Gorilla encoder/decoder, bitstream
│   ├── wal/            segmented write-ahead log
│   ├── head/           in-memory series, active chunks
│   ├── index/          symbols, postings, matchers
│   ├── block/          immutable block read/write, validation
│   ├── compact/        levelled merge + retention
│   └── postings/       sorted posting list operations
├── cmd/
│   ├── ingotctl/       block inspection, fsck
│   └── ingothttp/      HTTP query server
└── labels/             label types
```

## Development

```sh
go test -race -short ./...                                           # all tests (skip soak)
go test -race ./...                                                  # all tests including soak (~5 min)
go test -fuzz=FuzzXORIterator -fuzztime=60s ./internal/chunkenc/     # fuzz the decoder
go test -bench=. ./internal/chunkenc/                                # benchmarks
go build ./cmd/ingotctl/                                             # build CLI tool
go build ./cmd/ingothttp/                                            # build HTTP server
```

The decoder is total: arbitrary bytes produce values or `ErrShortStream` and never panics. Fuzzing gates every change to `chunkenc`.

## Inspiration

Most of the design is lifted from Prometheus TSDB — chunk encoding, index format, block/compaction model, label data model. The WAL is simpler (no page-level framing). The difference is that Prometheus TSDB is a storage engine inside a server; ingot is a library. See [NOTICE.md](NOTICE.md).

The chunk encoding comes from the Gorilla paper (Pelkonen et al., VLDB 2015) via Prometheus, which adapted the bit-width buckets for millisecond timestamps. ingot uses the Prometheus variant.

[tstorage](https://github.com/nakabonne/tstorage) is the closest existing embedded TSDB for Go. It doesn't do Gorilla compression or label-based indexing, which is most of why ingot exists.

## Non-goals

Replication, query languages, non-float64 values, deletes, multi-process access, out-of-order ingestion, Windows (sorry not my thing). Check DESIGN.md for reasoning.

## License

Apache 2.0
