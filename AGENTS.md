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

## Pitfalls

- **NEVER** run `reload-plugin` while a benchmark is running
- Benchmark on 100M (`hits`), correctness on 1M (`hits_1m`)
- Use ClickHouse-Parquet baseline, NOT native MergeTree
- Baseline file: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
- OpenSearch endpoint: `http://localhost:9200`, DQE: `POST /_plugins/_trino_sql`

## Current State (2026-03-26)

- Correctness: 29/43 on 1M
- Within 2x of CH-Parquet: 19/43 on r5.4xlarge (was 16/43 on m5.8xlarge before optimization)
- Hybrid bitset/collector optimization deployed (selective filters use bitset, broad use Collector)
- Bitset path: `Weight.count()` estimates selectivity; <50% of docs → bitset, else → Collector
- Big wins: Q18(0.01x), Q39(1.1x), Q41(0.28x), Q42(1.02x), Q43(0.70x)
- Target: >= 32/43 within 2x
