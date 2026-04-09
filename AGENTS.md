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
3. Restart OpenSearch: `./gradlew :opensearch-sql-plugin:run -x test -x integTest` (32GB heap)
4. Benchmark target queries: `bash benchmark_dqe_iceberg/omni_harness.sh --query N --timeout 300`
5. Full correctness+perf gate: `bash benchmark_dqe_iceberg/omni_harness.sh --timeout 300`

All steps 3-5 MUST follow the async execution pattern in Long-Running Task Rules.

## Long-Running Task Rules

Any command that may take longer than 2 minutes MUST be run asynchronously. This includes: benchmarks, plugin reload, correctness tests, compilation, and full benchmark suites.

### Async Execution Pattern

1. **NEVER run long-running commands synchronously** — always background and poll.
2. **Launch in a subshell** so the parent shell returns immediately:
   ```bash
   nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash benchmark_dqe_iceberg/omni_harness.sh --query 33 --timeout 300 > /tmp/q33.log 2>&1' &>/dev/null &
   echo "launched"
   ```
   **CRITICAL**: Plain `nohup cmd &` or `(cmd &)` does NOT work — the shell hangs waiting for the background process. You MUST use `nohup bash -c '...' &>/dev/null &`.
3. **Poll for completion** — check output tail for success/failure:
   ```bash
   tail -5 /tmp/q33.log
   ```
4. **Poll interval**: every 60-120s for single-query benchmarks, every 30s for builds.
5. **Analyze each poll result** — if ERROR/FAILURE appears in output, stop and diagnose immediately.
6. **Monitoring IS the task** — never launch a long-running command and then do something else.

### Common Long-Running Commands

| Command | Est. Time | Completion Marker | Error Marker |
|---------|-----------|-------------------|--------------|
| `./gradlew :dqe:compileJava` | ~5s | `BUILD SUCCESSFUL` | `BUILD FAILED` |
| `./gradlew :opensearch-sql-plugin:run -x test -x integTest` | ~45s startup | `started` in log | `BUILD FAILED` |
| `omni_harness.sh --query N` | 30s-5min per query | `Done.` | `ERROR` |
| `omni_harness.sh` (full 43 queries) | 20-60 min | `Done.` | `ERROR` |

### Kill All Benchmarks

```bash
pkill -f "omni_harness"; pkill -f "run_opensearch.sh"; pkill -f "run_all.sh"
```

## Query Numbering (CRITICAL)

| Context | Indexing | "Q17" means |
|---------|----------|-------------|
| `--query N` in scripts | 1-based | `--query 17` for Q17 |
| `queries_iceberg.sql` line | 1-based | line 17 for Q17 |
| JSON `result[N]` | 0-based | `result[16]` for Q17 |

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

### Benchmark Suite Layout

```
benchmarks/clickbench/benchmark_dqe_iceberg/
├── golden/                        # 43 expected results (100M dataset, TSV)
│   ├── q01.expected ... q43.expected
├── baseline/
│   └── trino_r5.4xlarge.json      # Trino 442 Hive Parquet (43/43, 363.2s)
├── results/                       # Output dir (gitignored)
│   ├── correctness_report.txt
│   ├── <instance>.json
│   └── comparison.txt
└── omni_harness.sh                # Main harness script — USE THIS FOR EVERYTHING
```

### omni_harness.sh — The One Script

**Always use `omni_harness.sh` for correctness checks, performance benchmarks, and comparisons.** Do NOT write custom curl loops or one-off scripts.

#### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--query N` | Run only query N (1-based) | all 43 |
| `--tries N` | Runs per query for timing | 3 |
| `--timeout N` | Per-query timeout in seconds | 120 |
| `--skip-correctness` | Skip correctness, perf only | false |
| `--correctness-only` | Correctness only, skip perf | false |

#### Modes

| Mode | What It Does | When to Use |
|------|-------------|-------------|
| Default (no flags) | Combined: correctness on run 1 + timing on all N runs, then Trino comparison | **Standard dev loop — use this** |
| `--correctness-only` | Single run, check against golden files | Quick correctness gate |
| `--skip-correctness` | N timing runs only, no correctness check | Re-benchmarking known-correct queries |

**The default combined mode is the most efficient** — it checks correctness on the first run's response and records timing for all runs in a single pass. No redundant query execution.

#### Examples

```bash
cd benchmarks/clickbench

# Single query: correctness + 3 perf runs + Trino comparison
bash benchmark_dqe_iceberg/omni_harness.sh --query 33 --timeout 300

# Full suite: all 43 queries, correctness + perf
bash benchmark_dqe_iceberg/omni_harness.sh --timeout 300

# Quick correctness check on one query
bash benchmark_dqe_iceberg/omni_harness.sh --query 33 --correctness-only --timeout 300

# Perf only, 5 runs, for stable timing
bash benchmark_dqe_iceberg/omni_harness.sh --query 33 --skip-correctness --tries 5 --timeout 300

# Range of queries (run sequentially via loop)
for q in 32 33 34 35 36; do
  bash benchmark_dqe_iceberg/omni_harness.sh --query $q --timeout 300
done
```

#### Async Execution (MANDATORY for agents)

```bash
# Launch in background
nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash benchmark_dqe_iceberg/omni_harness.sh --query 33 --timeout 300 > /tmp/q33.log 2>&1' &>/dev/null &
echo "launched"

# Poll for results
tail -5 /tmp/q33.log

# Expected output when done:
# [HH:MM:SS]   Q33 run 1: 183.787s
# [HH:MM:SS]   Q33: PASS
# [HH:MM:SS]   Q33 run 2: 175.234s
# [HH:MM:SS]   Q33 run 3: 171.890s
# [HH:MM:SS] Correctness: 1 pass, 0 fail, 0 error, 42 skip (out of 43)
# [HH:MM:SS] Results written to results/r5.4xlarge.json
# [HH:MM:SS] Done.
```

#### Outputs

| File | Content |
|------|---------|
| `results/correctness_report.txt` | Per-query PASS/FAIL/ERROR |
| `results/<instance>.json` | Timing results in ClickBench JSON format |
| `results/comparison.txt` | Side-by-side DQE vs Trino table |
| `results/diff_qN.txt` | Diff for failed queries |

### Iceberg Table Setup

```bash
python3 benchmarks/clickbench/data/create_iceberg_table_optimized.py \
  --data-dir benchmarks/clickbench/data/parquet \
  --warehouse /tmp/iceberg-warehouse
```

Table properties: 128MB target file size, 32MB row groups, 64KB pages, ZSTD, 4MB dict.
Data: sorted by (CounterID, EventDate, IsRefresh), 125 files, 10.1GB.

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

## Starting OpenSearch for Benchmarks

**Always use gradle run for 32GB heap:**

```bash
# Kill any existing instance
pkill -f "org.opensearch.bootstrap" 2>/dev/null || true; sleep 3

# Start with 32GB heap
nohup bash -c 'cd /local/home/penghuo/oss/os-sql && ./gradlew :opensearch-sql-plugin:run -x test -x integTest > /tmp/os-gradle-run.log 2>&1' &>/dev/null &

# Wait for cluster green (~45s)
for i in $(seq 1 24); do
  sleep 5
  STATUS=$(curl -s --max-time 3 "http://localhost:9200/_cluster/health" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])" 2>/dev/null)
  if [ "$STATUS" = "green" ]; then echo "Cluster green"; break; fi
done

# Verify heap
curl -s "http://localhost:9200/_nodes/stats/jvm" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for nid,n in d.get('nodes',{}).items():
    jvm=n.get('jvm',{}).get('mem',{})
    print(f'heap: {jvm.get(\"heap_used_in_bytes\",0)/1024/1024/1024:.1f}GB / {jvm.get(\"heap_max_in_bytes\",0)/1024/1024/1024:.1f}GB')
"
```

**Do NOT use `/tmp/os-cluster/node1/bin/opensearch` directly** — that starts with 1GB heap and will OOM on 100M dataset queries.

## Pitfalls

- **NEVER** run benchmarks while another benchmark is running
- **NEVER** use 1GB heap for Iceberg benchmarks — always gradle run (32GB)
- Iceberg benchmark: always 100M (`iceberg.default.hits`)
- Iceberg baseline: `benchmark_dqe_iceberg/baseline/trino_r5.4xlarge.json`
- OpenSearch endpoint: `http://localhost:9200`, DQE: `POST /_plugins/_trino_sql`
- `run_all.sh reload-plugin` requires `OS_INSTALL_DIR=/opt/opensearch` which may not exist — use gradle run instead

## Current State (2026-04-09)

### DQE Iceberg
- Correctness: ~20/43 on 100M
- Parallel bucket aggregation deployed (CompletableFuture)
- Inflated Sort+Limit (500K min) for ORDER BY agg LIMIT N
- Double-dispatch eliminated in multi-pass aggregation fallback
- Baseline: Trino 442 on Hive Parquet — 43/43, 363.2s total on r5.4xlarge
- Known slow: Q33 (~184s vs Trino 26.8s), Q35 (>180s vs Trino 21.1s) — high-cardinality GROUP BY
- Data: sorted by (CounterID, EventDate, IsRefresh), 125 files, 10.1GB, 32MB row groups
