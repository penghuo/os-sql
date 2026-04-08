# AGENTS.md — DQE Development & Benchmarking Guide

## DQE Architecture (Quick Mental Model)

```
SQL query → TransportTrinoSqlAction (coordinator)
          → PlanFragmenter (splits into shard plans)
          → TransportShardExecuteAction (per-shard dispatch)
              ├── FusedScanAggregate     (scalar aggs: SUM, AVG, COUNT)
              ├── FusedGroupByAggregate  (GROUP BY: varchar/numeric keys, flat/hash paths)
              └── Fast paths             (COUNT DISTINCT: HashSet, bitset, ordinal)
          → Coordinator merges shard results → returns to client
```

### Key Files

| File | Lines | Purpose |
|------|-------|---------|
| `TransportShardExecuteAction.java` | ~2200 | Shard-level dispatch: routes queries to fast paths |
| `FusedGroupByAggregate.java` | ~12700 | GROUP BY execution: varchar/numeric keys, flat arrays, collectors |
| `FusedScanAggregate.java` | ~1800 | Scalar aggregation: SUM, AVG, COUNT, flat array path |
| `TransportTrinoSqlAction.java` | ~4200 | Coordinator: plan optimization, shard fan-out, result merge |

### Dispatch Priority (TransportShardExecuteAction.executePlan)

1. Scalar agg → `FusedScanAggregate.canFuse()` → flat array path
2. Bare single-column scan → COUNT(DISTINCT) fast paths
3. 2-key COUNT(DISTINCT) → HashSet paths (numeric/varchar)
4. Expression GROUP BY → ordinal-cached path
5. Generic GROUP BY → `FusedGroupByAggregate.canFuse()` → fused path
6. Fallback → generic pipeline

### Filtered vs Unfiltered Queries

- **MatchAllDocsQuery** (no WHERE): tight `for(doc=0; doc<maxDoc; doc++)` loop, sequential DV access
- **Filtered** (WHERE clause): Collector-based `collect(int doc)` with virtual dispatch overhead
- **Selective filter optimization**: bitset pre-collection for filters matching <50% of docs

## Dev Iteration Loop

1. Code change (edit Java files)
2. Compile: `./gradlew :dqe:compileJava`
3. Reload plugin — see Long-Running Task Rules
4. Correctness gate — MUST be >= 38/43. If regression, STOP and fix.
5. Benchmark target queries — see Long-Running Task Rules

All steps 3-5 MUST follow the async execution pattern in Long-Running Task Rules.

## Long-Running Task Rules

Any command that may take longer than 2 minutes MUST be run asynchronously. This includes: benchmarks, plugin reload, correctness tests, compilation, and full benchmark suites.

### Async Execution Pattern

1. **NEVER run long-running commands synchronously** — always background and poll.
2. **Launch in a subshell** so the parent shell returns immediately:
   ```bash
   nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash run/run_all.sh reload-plugin > /tmp/reload.log 2>&1' &>/dev/null &
   echo "launched"
   ```
   **CRITICAL**: Plain `nohup cmd &` or `(cmd &)` does NOT work — the shell hangs waiting for the background process. You MUST use `nohup bash -c '...' &>/dev/null &`.
3. **Poll for completion** — check output tail for success/failure:
   ```bash
   tail -5 /tmp/reload.log
   ```
4. **Poll interval**: every 10-30s for benchmarks, every 30-60s for builds.
5. **Analyze each poll result** — if ERROR/FAILURE appears in output, stop and diagnose immediately.
6. **Monitoring IS the task** — never launch a long-running command and then do something else.

### Common Long-Running Commands

| Command | Est. Time | Output File | Completion Marker | Error Marker |
|---------|-----------|-------------|-------------------|--------------|
| `./gradlew :dqe:compileJava` | ~5s | `/tmp/compile.log` | `BUILD SUCCESSFUL` | `BUILD FAILED` |
| `run_all.sh reload-plugin` | 2-3 min | `/tmp/reload.log` | `reloaded successfully` | `FAILED` or `Error` |
| `run_all.sh correctness` | ~2 min | `/tmp/correctness.log` | `Summary:` | `Error` |
| `run_opensearch.sh --query N` | ~1 min | `/tmp/bench-qN.log` | `Results written` | `Error` or `failed` |
| `run_opensearch.sh` (full suite) | 5-15 min | `/tmp/bench-full.log` | `Results written` | `Error` or `failed` |

### Multi-Query Benchmark with Monitoring

```bash
# Benchmark multiple queries sequentially, monitoring each
for Q in 31 32 38 41; do
  LOG=/tmp/bench-q${Q}.log
  nohup bash -c "cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash run/run_opensearch.sh --warmup 1 --num-tries 3 --query $Q --output-dir /tmp/q${Q} > $LOG 2>&1" &>/dev/null &
  PID=$!
  while kill -0 $PID 2>/dev/null; do sleep 3; tail -1 $LOG 2>/dev/null; done
  echo "=== Q${Q} ==="
  grep -E "Q[0-9]+ run" $LOG
done
```

### Kill All Benchmarks

```bash
pkill -f "run_opensearch.sh"; pkill -f "run_all.sh"
```

## Query Numbering (CRITICAL)

| Context | Indexing | "Q17" means |
|---------|----------|-------------|
| `--query N` in scripts | 1-based | `--query 18` for Q17 |
| `queries_trino.sql` line | 1-based | line 18 for Q17 |
| JSON `result[N]` | 0-based | `result[17]` for Q17 |

**Mnemonic**: scripts and SQL are 1-based, JSON is 0-based.

## DQE Iceberg Benchmark

### Architecture (Iceberg Path)

```
SQL query → TransportTrinoSqlAction (coordinator)
          → IcebergFragmenter (splits into per-file plans)
          → TransportIcebergSplitExecuteAction (per-Parquet-file)
              └── LocalExecutionPlanner → ParquetPageSource
          → Coordinator merges split results → returns to client
```

Key difference from OpenSearch path: no DSL filters, no DocValues — reads Parquet directly via `ParquetPageSource`. Predicate pushdown is NOT applied (Iceberg uses `optimizeForIceberg()` which skips DSL conversion). AVG is decomposed into SUM+COUNT via `decomposeAvgInPlanTree()`.

### Benchmark Suite

Location: `benchmarks/clickbench/benchmark_dqe_iceberg/`

```
benchmark_dqe_iceberg/
├── golden/                        # 43 expected results (100M dataset, TSV)
│   ├── q01.expected ... q43.expected
├── baseline/
│   └── trino_r5.4xlarge.json      # Trino 442 Hive Parquet (43/43, 363.2s)
├── results/                       # Output dir (gitignored)
│   ├── correctness_report.txt
│   ├── <instance>.json
│   └── comparison.txt
└── omni_harness.sh                # Main harness script
```

### Harness Usage

```bash
cd benchmarks/clickbench

# Full run: correctness → perf (3 runs) → Trino comparison
bash benchmark_dqe_iceberg/omni_harness.sh

# Correctness only
bash benchmark_dqe_iceberg/omni_harness.sh --correctness-only

# Single query
bash benchmark_dqe_iceberg/omni_harness.sh --query 3

# Perf only, 5 runs, 180s timeout
bash benchmark_dqe_iceberg/omni_harness.sh --skip-correctness --tries 5 --timeout 180
```

### Harness Long-Running Commands

| Command | Est. Time | Output File | Completion Marker |
|---------|-----------|-------------|-------------------|
| `omni_harness.sh --correctness-only` | 5-15 min | stdout | `Correctness:` |
| `omni_harness.sh --skip-correctness` | 15-30 min | stdout | `Results written` |
| `omni_harness.sh` (full) | 20-45 min | stdout | `Done.` |

All harness runs MUST follow the async execution pattern:
```bash
nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash benchmark_dqe_iceberg/omni_harness.sh > /tmp/harness.log 2>&1' &>/dev/null &
echo "launched"
# Poll:
tail -5 /tmp/harness.log
```

### Iceberg Table Setup

```bash
# Create optimized Iceberg table (sorted by CounterID, EventDate, IsRefresh)
python3 benchmarks/clickbench/data/create_iceberg_table_optimized.py \
  --data-dir benchmarks/clickbench/data/parquet \
  --warehouse /tmp/iceberg-warehouse
```

Table properties: 128MB target file size, 32MB row groups, 64KB pages, ZSTD, 4MB dict.

### Trino Baseline (Docker)

```bash
# Start Trino 442 with Hive Parquet connector
docker run -d --name trino --network host \
  -v /tmp/trino-docker/etc:/etc/trino \
  -v /local/home/penghuo/oss/os-sql/benchmarks/clickbench/data/parquet:/data/parquet \
  trinodb/trino:442

# Run baseline benchmark
bash benchmarks/clickbench/run/run_trino_baseline.sh 3
```

### Queries

- Iceberg queries: `benchmarks/clickbench/queries/queries_iceberg.sql` (43 queries)
- Uses `iceberg.default.hits` catalog, DATE/TIMESTAMP types (not raw int)
- EventDate is DATE type, EventTime is TIMESTAMP — converted during Iceberg table creation

## Pitfalls

- **NEVER** run `reload-plugin` while a benchmark is running
- OpenSearch benchmark: 100M (`hits`), correctness on 1M (`hits_1m`)
- Iceberg benchmark: always 100M (`iceberg.default.hits`)
- OpenSearch baseline: `results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
- Iceberg baseline: `benchmark_dqe_iceberg/baseline/trino_r5.4xlarge.json`
- OpenSearch endpoint: `http://localhost:9200`, DQE: `POST /_plugins/_trino_sql`

## Current State (2026-04-08)

### OpenSearch DQE
- Correctness: 29/43 on 1M
- Within 2x of CH-Parquet: 19/43 on r5.4xlarge
- Hybrid bitset/collector optimization deployed

### DQE Iceberg
- Correctness: 20/43 on 100M (Q1-Q18, Q20 pass; Q19 type error; Q21+ OOM on high-cardinality GROUP BY)
- Baseline: Trino 442 on Hive Parquet — 43/43, 363.2s total on r5.4xlarge
- Key fixes applied: `optimizeForIceberg()` (skip predicate pushdown), AVG decomposition
- Known issues: OOM on high-cardinality GROUP BY (Q19 extract(minute) type, Q21+ coordinator merge)
- Data: sorted by (CounterID, EventDate, IsRefresh), 125 files, 10.1GB, 32MB row groups
