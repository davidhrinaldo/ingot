# ingot

An embedded time-series database for Go. SQLite for metrics: a library you import, not a server you deploy.

```go
db, _ := ingot.Open("./data", ingot.Options{Retention: 30 * 24 * time.Hour})

app := db.Appender()
app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), ts, 71.3)
app.Commit()

q, _ := db.Querier(mint, maxt)
ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "room", "office"))
```

## Status

**Pre-alpha. Not usable yet.** Built bottom-up; the public API above is the design target, not the current state.

| Milestone | State |
|---|---|
| M1 — Gorilla chunk encoding, fuzzed, benchmarked | Done |
| M2 — Head + WAL, kill -9 safe | In progress |
| M3 — Immutable blocks, mmap reads | — |
| M4 — Query path, API freeze, shippable alpha | — |
| M5 — Compaction + retention, 48h soak | — |
| M6 — ingotctl, HTTP layer, Grafana | — |

See [DESIGN.md](DESIGN.md) for architecture, on-disk format, and the non-goals table (no replication, no PromQL, no deletes, no out-of-order writes — each one deliberate).

## Why

Go programs that need local metrics storage have two options: run a Prometheus-shaped server next to your process, or hand-roll encoding on top of a key-value store. The embedded middle ground — common in the SQLite world — doesn't exist for time series in Go. Target users:

- Edge/IoT devices buffering sensor data locally
- Go binaries recording their own operational history
- Homelab sidecars for sensor firehoses (Home Assistant recorder, but purpose-built)
- Sampling agents and CLIs currently writing CSV

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
ingot/                  public API (target: Open, Appender, Querier)
├── internal/
│   ├── chunkenc/       Gorilla encoder/decoder, bitstream      [done]
│   ├── wal/            segmented write-ahead log               [next]
│   ├── head/           in-memory series, active chunks
│   ├── index/          symbols, postings, matchers
│   ├── block/          immutable block read/write
│   └── compact/        merge + retention
├── cmd/ingotctl/       block inspection, fsck
└── labels/             label types
```

## Development

```sh
go test -race ./...
go test -fuzz=FuzzXORIterator -fuzztime=60s ./internal/chunkenc/
go test -bench=. ./internal/chunkenc/
```

The decoder is total: arbitrary bytes produce values or `ErrShortStream`, never a panic. Fuzzing gates every change to `chunkenc`.

## Non-goals

Replication, query languages, non-float64 values, deletes, multi-process access, out-of-order ingestion, Windows. The reasoning for each is in DESIGN.md §3 — they're decisions, not omissions.

## License

TBD