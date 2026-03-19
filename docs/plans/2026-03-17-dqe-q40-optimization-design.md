# DQE Q9/Q10/Q14/Q40 Optimization Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Get remaining 4 ClickBench queries (Q9, Q10, Q14, Q40) within 2x of ClickHouse.

**Architecture:** Profile-driven optimization targeting three bottlenecks discovered via per-shard instrumentation: (1) inflated shard-side topN causing 16K row writes instead of 1K, (2) single-threaded scan of 50K docs with 5 advanceExact calls per doc, (3) flat map cache pressure from 8 concurrent shards.

**Tech Stack:** Java 11+, Lucene DocValues, ForkJoinPool, open-addressing hash maps.

---

## Current State (perf-lite 2026-03-17)

| Query | OS (ms) | CH HTTP (ms) | Ratio | Target (2x) |
|-------|---------|-------------|-------|-------------|
| Q9    | 48      | 16          | 3.0x  | 32ms        |
| Q10   | 53      | 17          | 3.1x  | 34ms        |
| Q14   | 98      | 19          | 5.2x  | 38ms        |
| Q40   | 583     | 102         | 5.7x  | 204ms       |

## Profiling Data (Q40, per-shard warm JIT)

```
shard=[hits_1m][7] docs=50558 groups=35298 collect=37ms scan=394ms heap=2ms write=258ms total=692ms topN=16160
shard=[hits_1m][0] docs=50614 groups=35535 collect=37ms scan=420ms heap=2ms write=264ms total=726ms
shard=[hits_1m][6] docs=51005 groups=35563 collect=39ms scan=427ms heap=2ms write=284ms total=754ms
```

Key findings:
- **topN=16160** (should be 1010): PlanFragmenter inflates `limit * numShards * 2`
- **write=258ms**: 16K rows × lookupOrd × BytesRefKey — pure waste
- **scan=410ms**: 50K docs × 5 advanceExact calls + flat map hash+probe
- **flatCap=65536**: 65K × 5 × 8 = 2.6MB per shard (8 shards compete for L3)

## Optimization 1: Reduce Shard TopN Inflation (Q40: -240ms)

**Problem:** `PlanFragmenter.buildShardPlanWithInflatedLimit()` line 318 sets
`inflatedLimit = max(1000, originalLimit * numShards * 2)` = 16,160.

For COUNT(*) which is perfectly decomposable (shard count ≤ global count),
the shard's top-K always contains any globally top-K entry. The `numShards * 2`
multiplier is only needed for non-decomposable aggregates.

**Fix:** Use `max(1000, originalLimit)` for COUNT(*)-only aggregation.
For mixed aggregates with SUM/AVG, keep the current inflation.

**Expected impact:** Write drops from 258ms to ~16ms (1K/16K × 258ms).
Q40 total: ~580ms → ~340ms.

**Files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java:318`

## Optimization 2: Intra-Shard Parallel Scan (Q40: -200ms)

**Problem:** 50K docs processed single-threaded at ~8μs/doc.

**Design:** When `segDocCount > 8192`, partition docIDs across N workers:
- N = min(4, available parallelism)
- Each worker: own DocValues iterators + own flat map
- Main thread: merge worker maps by probing worker-0's map

**Thread-local maps reduce cache pressure:** 4 workers × ~9K groups each →
capacity 16K × 5 × 8 = 640KB per worker (fits in L2).

**Expected impact:** Scan drops from 410ms to ~120ms.
Q40 total: ~340ms → ~180ms.

**Files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

## Optimization 3: Columnar Batch DocValues Read (All queries: -15%)

**Problem:** Inner loop interleaves 5 `advanceExact()` calls with hash probing.

**Design:** Pre-read all DocValues columns into flat `long[]` arrays, then run
hash+aggregate as pure computation. Separates I/O from compute.

**Expected impact:** 10-15% improvement on scan phase across all GROUP BY queries.

**Files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

## Predicted Outcomes

| Query | Current | After Opt 1 | After Opt 1+2 | After All | Target |
|-------|---------|-------------|---------------|-----------|--------|
| Q9    | 48ms    | 48ms        | ~30ms         | ~25ms     | 32ms   |
| Q10   | 53ms    | 53ms        | ~35ms         | ~30ms     | 34ms   |
| Q14   | 98ms    | 98ms        | ~45ms         | ~38ms     | 38ms   |
| Q40   | 583ms   | ~340ms      | ~180ms        | ~160ms    | 204ms  |

## Verification Process

After each optimization:
1. `./gradlew :dqe:compileJava` — compile
2. Rebuild plugin, reload OpenSearch
3. `bash run/run_all.sh correctness` — 43/43 must pass
4. `bash run/run_all.sh perf-lite` — compare ratios
5. Commit if improvement confirmed

## Risks

- **Opt 1 correctness:** Reducing inflation could miss globally-top groups when counts split across shards. Mitigated by: COUNT(*) is perfectly decomposable, and we keep `max(1000, ...)` floor.
- **Opt 2 thread safety:** Lucene DocValues iterators are per-call (thread-safe). ForkJoinPool tasks must not share mutable state during scan.
- **Opt 2 overhead:** For small doc counts (<8K), thread creation overhead exceeds parallelism benefit. Threshold guard required.
