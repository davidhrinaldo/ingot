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

**Alpha.** The system survives a 48h soak test under sustained load, but it has not seen production use yet. Releases may still change behavior and add API before v1.

| Milestone | State |
|---|---|
| M1 — Gorilla chunk encoding, fuzzed, benchmarked | Done |
| M2 — Head + WAL, kill -9 safe | Done |
| M3 — Immutable blocks, mmap reads | Done |
| M4 — Query path, API freeze, shippable alpha | Done |
| M5 — Compaction + retention, 48h soak | Done |
| M6 — ingotctl, HTTP layer, self-instrumentation | Done |

See [ARCHITECTURE.md](ARCHITECTURE.md) for the current runtime and data-flow diagram. See [DESIGN.md](DESIGN.md) for design decisions, the on-disk format, and the non-goals table. See [ROADMAP.md](ROADMAP.md) for what's next.

See [CHANGELOG.md](CHANGELOG.md) for release notes and upgrade guidance.

## Why

I needed a library to store time-series data locally and the options out there didn't quite work for my case. Prometheus was too heavy for what I needed but I wanted that level of compression. tstorage was close but it didn't have the compression or label indexing.

## Install

```sh
go get github.com/davidhrinaldo/ingot
```

## Features

- **Gorilla XOR compression** — ~1 byte/sample on regular metric data (see benchmarks below)
- **Crash-safe** — WAL with CRC32C records. By default, a successful commit is durable on disk
- **Query by label matchers** — equality, negation, regex, negative regex; merged across head and blocks
- **Levelled compaction** — 2h → 8h → 32h blocks, background merging, retention-based expiry
- **Self-instrumentation** — the DB records its own metrics (series/chunk counts, compactions, WAL fsync duration) through the normal write path, queryable like any other series
- **Zero external dependencies**

## Commit durability

The zero-value `Options{}` uses `SyncOnCommit`. `Appender.Commit` writes the batch to the WAL, calls `fsync`, and applies it to the in-memory head only after the sync succeeds. A successful commit therefore survives a process or machine crash, subject to the storage device honoring `fsync`. This differs from `v0.1.1`, which synced periodically by default, so applications upgrading from `v0.1.1` should expect higher commit latency unless they select `SyncPeriodic`.

Applications that accept a bounded loss window can opt into background sync:

```go
db, err := ingot.Open("./data", ingot.Options{
    SyncPolicy:   ingot.SyncPeriodic,
    SyncInterval: time.Second,
})
```

With `SyncPeriodic`, `Commit` does not wait for `fsync`; a crash can lose commits since the last successful background sync. Any WAL write, rotation, `fsync`, or directory-sync failure poisons the open database. Later commits fail, and `Close` returns the original error. This also prevents a background sync error from being ignored.

Recovery removes an incomplete record only from the physical end of the final WAL segment. A CRC mismatch in any segment fails startup without changing the WAL. Ingot does not persist a durable-offset watermark, so it cannot distinguish a crash-torn final append from external truncation in the middle of a previously durable final record. External changes to WAL files are unsupported and can cause that final record to be discarded as an incomplete tail.

A complete record with an unknown type also fails startup without changing the WAL. Current readers accept the released `v0.1.1` record framing. Do not open a data directory with `v0.1.1` after this version has written checkpoint records: `v0.1.1` can silently ignore those records and recover incomplete head state. Restore a pre-upgrade backup instead.

`Retention: 0` disables retention, and `BlockDuration: 0` uses the two-hour default. Nonzero values for either option must be at least one millisecond; negative and nonzero sub-millisecond values are rejected by `Open` before it creates the data directory.

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

The decoder is total: arbitrary bytes produce values or a decoding error such as `ErrShortStream` or `ErrInvalidXORWindow`, and never panic. Fuzz targets are available under `internal/chunkenc`.

## Inspiration

Most of the design is lifted from Prometheus TSDB — chunk encoding, index format, block/compaction model, label data model. The WAL is simpler (no page-level framing). The difference is that Prometheus TSDB is a storage engine inside a server; ingot is a library. See [NOTICE.md](NOTICE.md).

The chunk encoding comes from the Gorilla paper (Pelkonen et al., VLDB 2015) via Prometheus, which adapted the bit-width buckets for millisecond timestamps. ingot uses the Prometheus variant.

[tstorage](https://github.com/nakabonne/tstorage) is the closest existing embedded TSDB for Go. It doesn't do Gorilla compression or label-based indexing, which is most of why ingot exists.

## Non-goals

Replication, query languages, non-float64 values, deletes, shared multi-process access, out-of-order ingestion, Windows (sorry not my thing). `Open` takes an exclusive advisory lock on the data directory and rejects a second owner. Check DESIGN.md for reasoning.

## License

Apache 2.0
