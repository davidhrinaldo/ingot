# Ingot Design Document

## 1. Problem
Go programs that need to record metrics locally don't have great options. Prometheus, VictoriaMetrics, and InfluxDB are servers with their own config, ports, and operational overhead. The embedded alternatives are abandoned (tstorage, last updated ~2021) or amount to hand-rolling encoding on top of bbolt/Badger, which are key-val stores with no notion of time.

Target users:
- Edge/IoT - sensor data on a Pi or gateway; buffer locally, sync upstream opportunistically. Can't afford a server dependency.
- Self-instrumenting binaries - a Go app that records its own operational history and serves it from a /history endpoint. PocketBase-for-metrics.
- Homelab - purpose-built sidecar storage for Home Assistant-style sensor firehoses, where SQLite recorders bloat and slow down.
- Sampling CLIs/agents - network monitors, battery loggers, tick capture. Anything currently writing CSV.

## 2. Goals
- Single-import Go library. `ingot.Open(dir)`
- Compression competitive with Prometheus TSDB (~1.4 bytes/sample on regular metric data, per the Gorilla paper).
- Crash safety: `kill -9` at any point loses at most the samples not yet committed. Never corrupts the store.
- Bounded resources: flat memory under steady ingest, disk bounded by retention policy.
- Query by label matchers over a time range, merged transparently across memory and disk.
- Readable codebase. This is also a reference implementation; clarity beats cleverness where they conflict.

## 3. Non-goals
Explicit and load-bearing. Each of these is a decision, not an omission.

- Replication/clustering: Embedded means one process. Sync-upstream is an application concern.
- PromQL or any query language: v1 ships matchers + range queries. A query language is its own project.
- Value types beyond float64: Gorilla XOR compression assumes floats. Histograms, strings, exemplars: later or never.
- Deletes/tombstones: Retention-based expiry only. Tombstones infect every layer (index, compaction, queries) for a feature metric workloads rarely use.
- Multi-process access: Single writer, single process. No file locking protocol, no shared-memory coordination.
- Out-of-order writes: Samples must arrive in timestamp order per series (small tolerance window TBD in implementation). OOO ingestion doubles head complexity; Prometheus took years to add it.
- Windows support: mmap path is POSIX-first. Documented as unsupported, not broken-by-surprise.
- Backfill/bulk import: Follows from the OOO restriction. Revisit post-v1. It's the most-requested feature this will generate.

## 4. Data Model
- A `series` is a unique set of labels: `{__name__="temp", room="office"}`. Same model as Prometheus - proven, and it makes the project legible to anyone who's used it.
- A `sample` is `(timestamp int64 ms, value float64)`.
- Series are identified internally by a `uint64` series ID (also called a ref), assigned on first append and stable for the life of the store.
- Labels are validated on ingest: non-empty name, UTF-8, sorted canonical order for hashing.

### Public API (frozen at Phase 4)
```go
db, err := ingot.Open("./data", ingot.Options{
    Retention: 30 * 24 * time.Hour,
    BlockDuration: 2 * time.Hour,
})

// Write path — Appender is a lightweight batch, Commit makes it durable.
app := db.Appender()
ref, err := app.Append(0, labels.FromStrings("__name__", "temp", "room", "office"), ts, 71.3)
_, err = app.Append(ref, nil, ts+15000, 71.4) // ref fast-path skips label hashing
err = app.Commit() // or app.Rollback()

// Read path
q, err := db.Querier(mint, maxt)
ss := q.Select(labels.MustNewMatcher(labels.MatchEqual, "room", "office"))
for ss.Next() {
    it := ss.At().Iterator()
    for it.Next() {
        t, v := it.At()
    }
}
q.Close()

db.Close()
```

Everything else lives under `internal/`.

## 5. Architecture

┌─────────────────────────────────────┐
   Append ───────────▶  │  Head (in-memory)                   │
      │                 │  map[ref]*memSeries, striped locks  │
      │                 │  active Gorilla chunk per series    │
      ▼                 └──────────────┬──────────────────────┘
   WAL (append-only,                   │ head cutoff every
   segmented, CRC32)                   │ BlockDuration
                                       ▼
                        ┌─────────────────────────────────────┐
                        │  Blocks (immutable, mmap'd)         │
                        │  chunks/ + index + meta.json        │
                        └──────────────┬──────────────────────┘
                                       │ background
                                       ▼
                          Compactor: merge 2h→8h→32h,
                          drop blocks past retention

   Querier ──▶ merged iterator over head + overlapping blocks

### Package layout

ingot/              public API: Open, Options, Appender, Querier
    internal/
        chunkenc/   Gorilla encoder/decoder, bitstream utils
        wal/        segmented write-ahead log
        head/       in-memory series, active chunks
        index/      symbol table, postings, matchers
        block/      immutable block read/write, meta
        compact/    merge + retention
    cmd/ingotcli/   block inspection, chunk dump, fsck
    labels/         public label types (small, stable)

## 6. Write path
1. `Append` resolves labels->series ref (hash lookup; creates series + WAL series record on miss).
2. Sample buffered in the Appender.
3. `Commit`: a. Encode samples into a WAL record, append, and (per sync policy) fsync. b. Apply samples to head: append to each series' active chunk.
4. When a series' active chunk hits ~120 samples (Gorilla's sweet spot) it's sealed and a new one starts.

### Durability policy
Default fsync on segment rotation plus a periodic (~1s) background sync, with `Options.SyncEvery` for stricter needs. Fsync-per-commit is available but documented as a throughput cliff. The window of loss is stated plainly in the README.md rather than hidden.

### WAL format
- Segments of fixed max size (default 128 MiB), numbered files.
- Records: `type(1) | len(4) | payload | crc32(4)`. Types: `series` (ref + labels), `samples` (batch of ref/ts/val).
- Replay on `Open`: scan segments in order, stop at first CRC failure or truncated record, truncate the tail there. Everything before the corruption point is recovered.
- Truncation: after a head cutoff successfully flushes a block and fsyncs the block dir, WAL segments containing only flushed data are deleted. Ordering is invariant: block fsync -> meta.json write -> WAL truncate. Never reordered.

## 7. Chunk Encoding
Gorilla (Facebook, VLDB 2015), same scheme Prometheus uses:
- Timestamps: delta-of-delta. Regular scrape intervals make the second delta zero, encoded in one bit. Irregular deltas fall through escalating variable-width buckets.
- Values: XOR against the previous value. Identical value = one bit. Similar values share exponent/mantissa prefixes, so the XOR has long leading/trailing zero runs; encode meaningful bits only.
- Target: <= 1.5 bytes/sample on realistic sensor data (the paper's 1.37 is the benchmark to cite, not necessarily to beat).

`chunkenc` is pure functions over byte slices - no I/O, no clocks - which makes it property-testable and fuzzable in isolation. The decoder must be total: any byte input returns data or an error, never a panic. `go test -fuzz` gates every release of this package.

## 8. On-Disk Format

### Block directory
data/
    wal\
        00000001
        00000002
    01HXYZ.../      ULID = block ID
        meta.json   format version, minTime, maxTime, stats, compaction lineage
        index
        chunks/
            000001  segmented chunk files, 512 MiB max

- Every file opens with a magic number and a format version byte. Version 1 readers reject version 2 files loudly instead of misparsing them. Committed now because disk-format migration after users exist is misery.
- Blocks are immutable after the meta.json write. Readers mmap chunk files and the index; the page cace is the caching strategy.

### Index file
Simplified Prometheus index shape:

1. Symbol table - deduplicated label strings, referenced by offset.
2. Series section - per series: labels (as symbol refs) + chunk metadata (minT, maxT, file offset).
3. Postings - for each `label=value` pair, a sorted list of series refs.
4. TOC at the end with section offsets.

Simplifications defended: no postings offset table sparse index (blocks are small enough to binary-search), no label-offset table (iterate symbols). Both can be retrofitted behind the version byte.

## 9. Read Path
1. `Querier(mint, maxt)` snapshots the set of overlapping blocks plus the head.
2. `Select(matchers...)` resolves each matcher to a postings list (equality = direct lookup; regex = scan matching values), intersects/unions them.
3. Per-series iterator merges chunks across head and blocks in time order; exact-duplicate timestamps dedupe to the block value (blocks are the durable record).
4. Correctness oracle: tests compare every query result against a naive `[]sample` in-memory reference implementation fed the same appends. The merge across the head/block boundary is where the bugs live.

Block reaping while a Querier holds references is handled by refcounting block readers; the compactor deletes directories only at refcount zero.

## 10. Compaction & Retention
- Levelled by duration: 2h source blocks -> 8h -> 32h. Merge rewrites chunks (concatenating per-series across sources) and builds a fresh index.
- Runs in a single background goroutine. Live queries proceed against the source blocks; the swap is: write new block -> fsync -> update in-memory block set under a short lock -> refcount-release sources -> delete when drained.
- Retention: any block whose maxTime is older then `now - Retention` is dropped at the next compaction cycle.
- No stop-the-world anywhere. The block-set swap lock is O(pointer swap), held for nanoseconds. This concurrency design is the part of the codebase most worth reading.

## 11. Resource Bounds
- Memory: head holds <= BlockDuration of data. 10k series x 120-sample active chunk + sealed head chunks ~= tens of MB. Label interning keeps series overhead down.
- Disk: retention-bound. Worst-case ~2x steady state traniently during compaction (sources + destination coexist).
- Goroutines: exactly two background: WAL syncer, compactor. No pools, no surprises.

## 12. Testing Strategy

- chunkenc: Property round-trips (rapid), fuzzing the decoder, adversarial cases: NaN, +- Inf, single sample, counter resets, max deltas
- wal: Torn-write harness: truncate segments at every byte offset, assert recovery to last valid record
- head: Race detector on concurrent append/query; `kill -9` simulation via process-level test
- query: Oracle comparison against naive reference implementation, boundary emphasis on head/block seam
- system: Soak: 10k series @15s interval, 48h via fake clock - assert flat RSS, bounded disk, zero errors
- benchmarks: ns/append, bytes/sample, query latency; tracked in-repo, regressions fail CI

## 13. Observability of Ingot Itself

The library exposes its own internals as a series of guages/counters (`ingot_head_series`, `ingot_wal_fsync_duration`, ...) - retrievable via the same query API. A TSDB that can't measure itself would be embarrassing.

## 14. Milestones
Matches the build roadmap:
1. M1 - chunkenc survives fuzzing; bytes/sample benchmark published.
2. M2 - Open -> Append -> kill -9 -> Open -> intact.
3. M3 - restart requires zero WAL replay of flushed data.
4. M4 - query oracle green across head/block boundary. API freeze. Shippable alpha.
5. M5 - 48h soak passes: flat memory, bounded disk, live queries during compaction.
6. M6 - ingotctl + optional HTTP layer (Prometheus remote-read subset) + Grafana demo + HA dogfood bridge.

## 15. Open Questions
- Out-of-order tolerance window in the head: zero, or a small slack (e.g., accept anything newer than the series' sealed-chunk boundary)? Learning small-slack; zero is user-hostile for multi-sensor clock skew.
- ULID vs. sequential block IDs: ULID gives sortable uniqueness for free; sequential is simpler to fsck. Leaning ULID (Prometheus-compatible mental model).
- `labels` package: depend on `prometheus/prometheus/model/lables` or vendor a minimal copy? Leaning minimal copy - zero-dependency is part of the pitch.
- Snappy/zstd over sealed chunk files on top of Gorilla: measure first. Gorilla output is high-entropy; likely not worth it.

