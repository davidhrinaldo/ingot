# Ingot architecture

Ingot runs inside one Go process and holds an exclusive advisory lock on its data directory. The diagram shows the main runtime paths; the package map below lists the implementation boundaries.

```mermaid
%%{init: {"flowchart": {"curve": "basis", "nodeSpacing": 30, "rankSpacing": 45}}}%%
flowchart TB
    subgraph callers["Callers"]
        direction LR
        app["Go application"]
        http["ingothttp<br/>read-only JSON API"]
        ctl["ingotctl<br/>inspect · validate · fsck"]
    end

    subgraph process["Single Ingot process"]
        direction TB

        api["Public API<br/><b>ingot</b> · Open · DB · Appender · Querier · Stats<br/><b>labels</b> · types · validation · matchers"]

        subgraph runtime["Runtime paths"]
            direction LR
            write["<b>Write</b><br/>Appender.Commit<br/><br/>wal → head → block<br/>persist before memory apply"]
            query["<b>Query</b><br/>Querier.Select<br/><br/>head + block → postings → merge<br/>fixed snapshot · pinned readers"]
            loop["<b>Maintain</b><br/>runs every minute<br/><br/>head → compact → block<br/>self-metrics · flush · retention"]
        end
    end

    subgraph disk["Data directory"]
        direction LR
        lock[("lock<br/>exclusive flock")]
        walfiles[("wal/00000001…")]
        blocks[("ULID block directories<br/>meta.json · index · chunks/000001…")]
        tombstones[(".retention/<br/>deletion manifests")]
    end

    app --> api
    http -->|"queries and stats"| api

    api -->|"append"| write
    api -->|"select"| query
    api -->|"schedule"| loop

    api -->|"acquire on Open"| lock
    write -->|"append and checkpoint"| walfiles
    write -->|"flush"| blocks
    query -->|"read"| blocks
    loop -->|"compact"| blocks
    loop -->|"mark expired"| tombstones
    ctl -.->|"offline read"| blocks

    classDef caller fill:#f8fafc,stroke:#64748b,color:#0f172a
    classDef api fill:#dbeafe,stroke:#2563eb,color:#172554,stroke-width:1.5px
    classDef writePath fill:#ecfdf5,stroke:#059669,color:#022c22,stroke-width:1.5px
    classDef queryPath fill:#eff6ff,stroke:#3b82f6,color:#172554,stroke-width:1.5px
    classDef maintenancePath fill:#fff7ed,stroke:#ea580c,color:#431407,stroke-width:1.5px
    classDef storage fill:#ede9fe,stroke:#7c3aed,color:#2e1065

    class app,http,ctl caller
    class api api
    class write writePath
    class query queryPath
    class loop maintenancePath
    class lock,walfiles,blocks,tombstones storage

    style callers fill:#ffffff,stroke:#cbd5e1,color:#334155,stroke-width:1px
    style process fill:#f8fafc,stroke:#64748b,color:#334155,stroke-width:1.5px
    style runtime fill:#ffffff,stroke:#cbd5e1,color:#334155
    style disk fill:#faf5ff,stroke:#c4b5fd,color:#334155
```

A [rendered PNG](architecture-final-desktop.png) is available for Markdown viewers that do not support Mermaid.

## Flow notes

1. `Appender.Commit` serializes commits. It writes the batch to the WAL, performs the configured durability step, and only then applies the complete batch to the head. Samples within each series must have strictly increasing timestamps.
2. `DB.Querier` takes a fixed head snapshot and pins every overlapping block reader. Label matchers resolve independently against the head and each block. The result iterator merges samples by timestamp. Blocks take precedence over the head at duplicate timestamps.
3. The maintenance goroutine records Ingot's own metrics, flushes old head chunks, runs one compaction cycle, and applies retention. Compaction copies encoded chunks into a new block. It does not decode and recompress them.
4. A flush writes and syncs chunk and index data, writes and syncs a WAL checkpoint, publishes `meta.json`, validates and activates the checkpoint, installs the block reader, evicts flushed head chunks, and truncates older WAL segments.
5. `meta.json` is the block publication gate. The block reader loads the index into memory and memory-maps chunk segments. Queries hold reader references so compaction cannot remove a source block while it is in use.

## Package map

| Package | Responsibility |
|---|---|
| `ingot` | Public API, lifecycle, block-set coordination, query merge, maintenance scheduling |
| `labels` | Public label types, validation, hashing, and matchers |
| `internal/head` | Mutable write window, series refs, appender commits, WAL replay, flush checkpoints |
| `internal/wal` | Segmented write-ahead log, record framing, CRC validation, syncing, checkpoints |
| `internal/chunkenc` | Gorilla delta-of-delta timestamp and XOR float encoding |
| `internal/block` | Immutable block publication, reading, memory mapping, and integrity validation |
| `internal/index` | Block symbol table, series metadata, chunk references, and label postings |
| `internal/postings` | Sorted posting-list union, intersection, and subtraction |
| `internal/compact` | Compaction planning, encoded chunk merging, and retention selection |

## On-disk layout

```text
<data-dir>/
├── lock
├── wal/
│   ├── 00000001
│   └── 00000002
├── .retention/
│   └── <block-ULID>
└── <block-ULID>/
    ├── meta.json
    ├── index
    └── chunks/
        └── 000001
```
