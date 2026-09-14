# Changelog

## v0.2.0

### Upgrade notes

- The zero-value `Options{}` now uses `SyncOnCommit`. Each successful commit waits for `fsync`. Applications that prefer the `v0.1.1` throughput and accept a bounded crash-loss window must select `SyncPeriodic` explicitly.
- Use keyed `Options` literals. The new fields make external unkeyed literals fail to compile.
- `Open` now rejects a second process using the same data directory with `ErrDataDirLocked`.
- Startup performs stricter block validation and may reject damaged stores that older versions opened.
- Existing valid version 1 blocks require no migration.
- Do not downgrade a store after v0.2.0 has written WAL checkpoint records. Restore a pre-upgrade backup instead; `v0.1.1` can silently ignore checkpoint records and recover incomplete head state.

### Added

- Configurable `SyncOnCommit` and `SyncPeriodic` WAL durability policies.
- Exclusive advisory locking for the data directory.
- Durable WAL checkpoints for head flushes.
- Durable retention manifests and startup reconciliation for interrupted retention and compaction.
- `DB.RunRetention`, `ErrClosed`, `ErrDataDirLocked`, and label validation errors.
- Semantic block, index, chunk, and metadata validation in block open and `ingotctl fsck`.
- A full 10,000-series soak job that can run nightly or on demand; the shorter suite continues to run under the race detector on every change.

### Fixed

- Commits now reach the in-memory head only after their configured WAL durability step succeeds.
- WAL write, rotation, sync, and directory-sync failures poison the open database instead of allowing later commits.
- Recovery no longer truncates CRC-corrupt records or incomplete records outside the final WAL tail.
- Flush, compaction, retention, and close no longer race or resurrect superseded blocks after restart.
- Sparse series now flush and expire under retention.
- Queriers hold a fixed head and block snapshot and release it on close.
- Queries merge overlapping head and block chunks in timestamp order with deterministic duplicate handling.
- Label matchers now handle absent labels consistently and invalid matchers fail closed.
- Label and index ownership no longer exposes mutable internal slices.
