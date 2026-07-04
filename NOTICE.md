NOTICE:
This project was developed with reference to the Prometheus TSDB
(github.com/prometheus/prometheus), licensed under Apache License 2.0,
Copyright The Prometheus Authors.

- internal/chunkenc: Gorilla XOR chunk encoding follows the Prometheus
  tsdb/chunkenc implementation.
- internal/wal: Write-ahead log design is informed by the Prometheus
  tsdb/wal package. Uses a simpler record format without page-level
  framing; shares the same well-established WAL patterns (segmented
  append-only files, CRC32C validation, recovery by truncation).
