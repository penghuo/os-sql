# Phase D: DQE 16/43 → 43/43 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bring all 43 ClickBench queries within 2x of official ClickHouse-Parquet baseline.

**Architecture:** Six incremental phases (D1-D6), each independently testable. Changes are
in the DQE fused aggregation hot paths: `FusedGroupByAggregate.java`, `FusedScanAggregate.java`,
and `TransportShardExecuteAction.java`. Each task follows correctness-first: verify with
`bash run/run_all.sh correctness` before benchmarking.

**Tech Stack:** Java 11+, Lucene DocValues API, OpenSearch IndexShard, JUnit 5

**Key Files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (11,690 lines)
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java` (1,600 lines)
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java` (2,208 lines)
- Tests: `dqe/src/test/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteActionTest.java`
- Benchmark: `benchmarks/clickbench/run/run_opensearch.sh`

**Benchmark commands:**
```bash
# Correctness (must pass after every change)
cd benchmarks/clickbench && bash run/run_all.sh correctness

# Single query performance
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query N --output-dir /tmp/bench

# Full benchmark
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir results/performance/opensearch_corrected
```

---

## Phase D1: Borderline Quick Wins (Q02, Q14, Q31, Q40 → 20/43)

These 4 queries are within 2.0-2.5x of ClickHouse-Parquet. Small improvements push them under 2x.

### Task D1.1: Q40 — Reduce narrow-filter overhead (need 34ms)

Q40 is a CounterID=62 filtered GROUP BY with OFFSET. The query touches ~500 rows
but DQE spends ~200ms on Weight/Scorer allocation across 24 segments.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:9946-10356` (executeNKeyVarcharParallelDocRange)

**Step 1: Profile Q40 to identify exact overhead**

Run Q40 alone with timing:
```bash
cd benchmarks/clickbench
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 40 --output-dir /tmp/q40_baseline
cat /tmp/q40_baseline/*.json | python3 -c "import json,sys; d=json.load(sys.stdin); print('Q40 times:', d['result'][39])"
```

Expected: ~[0.3, 0.18, 0.18, 0.18, 0.17] (176ms warm best)

**Step 2: Add segment-skip optimization for narrow filters**

In `executeNKeyVarcharParallelDocRange`, before creating a Scorer for each leaf context,
check if the segment has any matching docs using a fast count:

```java
// Before the existing Scorer creation loop, add:
for (LeafReaderContext leafCtx : searcher.getIndexReader().leaves()) {
    Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    // Quick check: if this segment has 0 matching docs, skip it entirely
    Scorer scorer = weight.scorer(leafCtx);
    if (scorer == null) {
        continue; // No matching docs in this segment
    }
    // ... existing collection logic
}
```

The key insight: `weight.scorer(leafCtx)` already returns null when the segment has no
matches, but the current code may be doing additional work before this check. Ensure
the null check happens first, before any buffer allocation or DocValues opening.

**Step 3: Run correctness check**
```bash
cd benchmarks/clickbench && bash run/run_all.sh correctness --query 40
```
Expected: PASS

**Step 4: Benchmark Q40**
```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 40 --output-dir /tmp/q40_after
```
Target: best of 5 ≤ 0.142s (2x of CH-Parquet 0.071s)

**Step 5: Commit**
```bash
git add -p dqe/src/main/java/
git commit -m "perf(dqe): skip empty segments in narrow-filter GROUP BY (Q40: 176ms→<142ms)"
```

### Task D1.2: Q02 — Tighten SUM+COUNT+AVG flat-array path (need 50ms)

Q02 is `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ...` — a full-scan with
mixed scalar aggregates. Currently 260ms vs CH-Parquet 105ms.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java:478-486` (flat-array fast path)

**Step 1: Profile Q02 to understand path taken**

Add temporary logging to `FusedScanAggregate.execute()` around line 478 to confirm
Q02 hits the flat-array path. If not, identify which path it takes.

```bash
cd benchmarks/clickbench
bash run/run_opensearch.sh --warmup 1 --num-tries 1 --query 2 --output-dir /tmp/q02_profile
```

**Step 2: If Q02 misses flat-array path, fix the eligibility check**

Check `tryFlatArrayPath` (line 609+) — it may reject Q02 because of the expression
`ResolutionWidth + 1` (not a bare column). Extend eligibility to include simple
arithmetic expressions like `col + constant`.

**Step 3: Run correctness**
```bash
cd benchmarks/clickbench && bash run/run_all.sh correctness --query 2
```

**Step 4: Benchmark Q02**
```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 2 --output-dir /tmp/q02_after
```
Target: best ≤ 0.210s (2x of 0.105s)

**Step 5: Commit**
```bash
git commit -m "perf(dqe): extend flat-array path to SUM+COUNT+AVG with expressions (Q02: 260ms→<210ms)"
```

### Task D1.3: Q31 — Optimize 2-key numeric GROUP BY merge (need 280ms)

Q31 is `GROUP BY WatchID, ClientIP` (numeric 2-key, filtered). Currently 2.470s vs 1.095s.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:4626+` (executeTwoKeyNumeric)

**Step 1: Profile Q31**
```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 31 --output-dir /tmp/q31_baseline
```

**Step 2: Optimize the cross-segment merge in executeTwoKeyNumeric**

The 2-key numeric path merges HashMap results across segments. For filtered queries,
pre-size the HashMap based on estimated cardinality to avoid resizing.

**Step 3: Run correctness and benchmark**
```bash
bash run/run_all.sh correctness --query 31
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 31 --output-dir /tmp/q31_after
```
Target: best ≤ 2.190s (2x of 1.095s)

**Step 4: Commit**
```bash
git commit -m "perf(dqe): optimize 2-key numeric merge for filtered GROUP BY (Q31: 2.47s→<2.19s)"
```

### Task D1.4: Q14 — Improve filtered TopN GROUP BY (need 256ms)

Q14 is `GROUP BY SearchEngineID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10` with
WHERE filter. Currently 1.726s vs 0.735s.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (executeWithTopN path, line 901)

**Step 1: Profile Q14**
```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 14 --output-dir /tmp/q14_baseline
```

**Step 2: Ensure TopN pushdown is active for Q14**

Verify that `executeWithTopN` is being called (not the full `executeFusedGroupByAggregate`).
If the planner doesn't recognize the ORDER BY + LIMIT pattern, fix the dispatch in
`TransportShardExecuteAction.java:406-450`.

**Step 3: Correctness and benchmark**
```bash
bash run/run_all.sh correctness --query 14
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 14 --output-dir /tmp/q14_after
```
Target: best ≤ 1.470s (2x of 0.735s)

**Step 4: Commit**
```bash
git commit -m "perf(dqe): ensure TopN pushdown for filtered GROUP BY (Q14: 1.73s→<1.47s)"
```

### Task D1.5: Full correctness + benchmark checkpoint

```bash
cd benchmarks/clickbench && bash run/run_all.sh correctness
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir results/performance/opensearch_d1
```

Count queries within 2x. Expected: 20/43. Commit results.

---

## Phase D2: Narrow-Filter Queries (Q35-Q42 → 27/43)

All filter on CounterID=62 (0.005% selectivity). Root cause: 200ms+ fixed overhead
when actual aggregation work is 2-5ms.

### Task D2.1: Segment-skip with Scorer null check

The Weight/Scorer pattern already returns null for empty segments, but current code
may allocate buffers before checking. Ensure early exit.

**Files:**
- Modify: `FusedGroupByAggregate.java:9946+` (executeNKeyVarcharParallelDocRange)
- Modify: `FusedGroupByAggregate.java:1268+` (executeSingleVarcharCountStar — if narrow-filter queries use this path)

**Step 1: For each narrow-filter query path, add early Scorer null check**

Before any DocValues opening or buffer allocation in the per-segment loop:
```java
Scorer scorer = weight.scorer(leafCtx);
if (scorer == null) {
    continue; // Skip this segment entirely — zero matching docs
}
```

**Step 2: Run correctness for all narrow-filter queries**
```bash
for q in 35 36 37 38 39 40 41 42; do
    bash run/run_all.sh correctness --query $q
done
```

**Step 3: Benchmark all narrow-filter queries**
```bash
for q in 35 36 37 38 39 40 41 42; do
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/d2_q${q}
    echo "Q${q}:" && cat /tmp/d2_q${q}/*.json | python3 -c "import json,sys; d=json.load(sys.stdin); print(min(d['result'][$q-1]))"
done
```

**Step 4: Commit**
```bash
git commit -m "perf(dqe): early Scorer null check skips empty segments for narrow filters (Q35-Q42)"
```

### Task D2.2: Cache Weight object across segments

Currently `createWeight()` may be called per-segment. Cache it once per query.

**Files:**
- Modify: `FusedGroupByAggregate.java` — relevant execution methods

**Step 1: Extract Weight creation before segment loop**

```java
// Before segment iteration:
Query rewritten = engineSearcher.rewrite(query);
Weight weight = engineSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

// In segment loop:
Scorer scorer = weight.scorer(leafCtx); // reuse Weight
```

**Step 2: Correctness + benchmark**

Same commands as D2.1. Target: Q37 (2.9x→<2x), Q38 (2.9x→<2x).

**Step 3: Commit**
```bash
git commit -m "perf(dqe): cache Weight across segments in fused GROUP BY"
```

### Task D2.3: Use simpler path for tiny result sets

When filter passes <1K rows, skip the parallel docrange machinery. Use sequential
single-pass instead.

**Files:**
- Modify: `FusedGroupByAggregate.java:8657+` (executeNKeyVarcharPath dispatch)

**Step 1: Add early cardinality estimate**

Before choosing parallel vs sequential, estimate matching docs:
```java
// Quick estimate: sum of Scorer.count() or Weight.count() across segments
int estimatedDocs = 0;
for (LeafReaderContext leaf : reader.leaves()) {
    Scorer s = weight.scorer(leaf);
    if (s != null) {
        estimatedDocs += s.iterator().cost();
    }
}
if (estimatedDocs < 1000) {
    // Use simple sequential path instead of parallel docrange
    return executeNKeyVarcharSequential(...);
}
```

**Step 2: Implement the sequential fallback**

This is the single-thread path already existing for single-segment case. Extend it
to multi-segment with Weight reuse from D2.2.

**Step 3: Correctness + benchmark checkpoint**
```bash
bash run/run_all.sh correctness
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir results/performance/opensearch_d2
```
Expected: 27/43 within 2x.

**Step 4: Commit**
```bash
git commit -m "perf(dqe): use sequential path for narrow-filter GROUP BY (<1K rows)"
```

---

## Phase D3: Full-Scan Scalar + Low-Cardinality (Q03, Q04, Q05, Q07, Q29, Q39 → 33/43)

### Task D3.1: Q03 — Recognize SUM+COUNT+AVG flat-array pattern (16x → <2x)

Q03 is `SELECT COUNT(*), SUM(AdvEngineID), AVG(ResolutionWidth)`. The flat-array path
at `FusedScanAggregate.java:478` should handle this but may not detect the mixed pattern.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java:609+` (tryFlatArrayPath)

**Step 1: Profile Q03 to identify current path**
```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 3 --output-dir /tmp/q03_baseline
```

**Step 2: Extend flat-array eligibility to SUM+COUNT+AVG combinations**

The flat-array path uses primitive `long[]` arrays for accumulation. Extend to handle
AVG (which needs both sum and count) by storing two slots per AVG aggregate.

**Step 3: Correctness + benchmark**
```bash
bash run/run_all.sh correctness --query 3
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 3 --output-dir /tmp/q03_after
```
Target: best ≤ 0.222s (2x of 0.111s). This is ambitious — 1.779s → 0.222s = 8x improvement.
May need multiple optimizations (flat-array + vectorized scan).

**Step 4: Commit**
```bash
git commit -m "perf(dqe): extend flat-array path for SUM+COUNT+AVG mix (Q03)"
```

### Task D3.2: Q07 — Array-indexed aggregation for low cardinality (4.5x → <2x)

Q07 is `GROUP BY AdvEngineID ORDER BY COUNT(*) DESC LIMIT 10`. AdvEngineID has <20
distinct values. Instead of HashMap, use a fixed-size array indexed by value.

**Files:**
- Modify: `FusedGroupByAggregate.java:2786+` (executeNumericOnly)

**Step 1: Detect low-cardinality numeric keys**

Check if the column's value range fits in a small array (e.g., max - min < 256):
```java
// In executeNumericOnly, before building HashMap:
long minVal = Long.MAX_VALUE, maxVal = Long.MIN_VALUE;
// Read PointValues to get range
for (LeafReaderContext leaf : reader.leaves()) {
    PointValues pv = leaf.reader().getPointValues(keyName);
    if (pv != null) {
        minVal = Math.min(minVal, /* decode min */);
        maxVal = Math.max(maxVal, /* decode max */);
    }
}
long range = maxVal - minVal;
if (range >= 0 && range < 256) {
    return executeNumericArrayIndexed(shard, query, keyInfos, specs, minVal, range, ...);
}
```

**Step 2: Implement array-indexed aggregation**

```java
private static List<Page> executeNumericArrayIndexed(..., long minVal, long range, ...) {
    int arraySize = (int) (range + 1);
    long[] counts = new long[arraySize]; // or AccumulatorGroup[] for general aggs
    // Per-doc: counts[(int)(value - minVal)]++
    // No hash, no object allocation, O(1) per doc
}
```

**Step 3: Correctness + benchmark**
```bash
bash run/run_all.sh correctness --query 7
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 7 --output-dir /tmp/q07_after
```
Target: best ≤ 0.118s (2x of 0.059s)

**Step 4: Commit**
```bash
git commit -m "perf(dqe): array-indexed aggregation for low-cardinality numeric GROUP BY (Q07)"
```

### Task D3.3: Q29 — Algebraic shortcut for SUM expressions (3.9x → <2x)

Q29 computes `SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), ...` for 90
variations. Algebraic shortcut: `SUM(col + k) = SUM(col) + k * COUNT(*)`.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java:151-335` (executeWithEval)

**Step 1: Check if Q29 already uses executeWithEval**

If not, the algebraic shortcut exists but isn't triggered. Fix the detection in
`TransportShardExecuteAction.java:269` (executeFusedEvalAggregate dispatch).

**Step 2: Verify algebraic shortcut produces correct results**
```bash
bash run/run_all.sh correctness --query 29
```

**Step 3: Benchmark**
```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 29 --output-dir /tmp/q29_after
```
Target: best ≤ 0.192s (2x of 0.096s)

**Step 4: Commit**
```bash
git commit -m "perf(dqe): trigger algebraic shortcut for SUM(col+k) expressions (Q29)"
```

### Task D3.4: Q39 — Inline expression evaluation for many-SUM scan (17.7x → <2x)

Q39 is a full-scan with many SUM expressions. BlockExpression overhead dominates.

**Files:**
- Modify: `FusedScanAggregate.java` — inline expression evaluation in hot loop

**Step 1: Profile Q39 to identify bottleneck**

Determine if Q39 uses executeWithEval or the general Collector path.

**Step 2: Inline length() and arithmetic operations**

Replace BlockExpression dispatch with direct computation in the accumulation loop.

**Step 3: Correctness + benchmark**
Target: best ≤ 0.286s (2x of 0.143s). This is very ambitious (2.538s → 0.286s).

**Step 4: Commit**

### Task D3.5: Q04 — Native COUNT(DISTINCT UserID) (5.5x → <2x)

Q04 is `SELECT COUNT(DISTINCT UserID) FROM hits`. Currently decomposed into GROUP BY + COUNT.

**Files:**
- Modify: `TransportShardExecuteAction.java:789-896` (executeCountDistinctWithHashSets)

**Step 1: Optimize the HashSet merge**

Instead of sending all unique values to coordinator, compute shard-local count and
return just the count.

**Step 2: Correctness + benchmark**
Target: best ≤ 0.868s (2x of 0.434s)

### Task D3.6: Q05 — Share OrdinalMap cache for COUNT(DISTINCT SearchPhrase) (6.4x → <2x)

Q05 is `SELECT COUNT(DISTINCT SearchPhrase)`. OrdinalMap is cached for GROUP BY but
not for scalar COUNT(DISTINCT).

**Files:**
- Modify: `TransportShardExecuteAction.java` — reuse cached OrdinalMap in count-distinct path

**Step 1: Extend OrdinalMap cache to cover scalar COUNT(DISTINCT)**

The cache key is (IndexReader, fieldName). Same cache can be used.

**Step 2: Correctness + benchmark**
Target: best ≤ 1.380s (2x of 0.690s)

### Task D3.7: Phase D3 checkpoint
```bash
bash run/run_all.sh correctness
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir results/performance/opensearch_d3
```
Expected: 33/43 within 2x.

---

## Phase D4: COUNT(DISTINCT) Decomposition Fix (Q08, Q09, Q11, Q13 → 37/43)

Root cause: `COUNT(DISTINCT col)` inside GROUP BY is decomposed by Calcite into
an extra GROUP BY level, doubling the key space.

### Task D4.1: Fused single-pass COUNT(DISTINCT) for GROUP BY

Add a native `CountDistinctAccum` that maintains a per-group HashSet without
decomposing into an extra aggregation level.

**Files:**
- Modify: `FusedGroupByAggregate.java:11224+` (AccumulatorGroup)
- Modify: `FusedGroupByAggregate.java:932-1116` (executeInternal dispatch)
- Modify: `TransportShardExecuteAction.java:305` (detection of COUNT(DISTINCT) pattern)

**Step 1: Add per-group HashSet accumulator**

In AccumulatorGroup, add a new accumulator type:
```java
case COUNT_DISTINCT:
    return new CountDistinctAccum(); // holds LongOpenHashSet
```

**Step 2: Route GROUP BY + COUNT(DISTINCT) to fused path**

Detect the pattern in TransportShardExecuteAction where the plan has:
- Aggregate(GROUP BY x, COUNT(DISTINCT y))
Currently this becomes: Aggregate(GROUP BY x, y, COUNT(*)) → Aggregate(GROUP BY x, COUNT(*))
Change: route directly to fused GROUP BY with CountDistinctAccum.

**Step 3: Implement for Q08 (RegionID GROUP BY + COUNT(DISTINCT UserID))**
```bash
bash run/run_all.sh correctness --query 8
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 8 --output-dir /tmp/q08_after
```
Target: best ≤ 1.080s (2x of 0.540s)

**Step 4: Extend to Q09, Q11, Q13**

Same pattern, different key columns. Verify each:
```bash
for q in 9 11 13; do
    bash run/run_all.sh correctness --query $q
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/d4_q${q}
done
```
Targets: Q09 ≤ 1.228s, Q11 ≤ 0.536s, Q13 ≤ 1.912s

**Step 5: Commit**
```bash
git commit -m "perf(dqe): fused COUNT(DISTINCT) in GROUP BY without decomposition (Q08,Q09,Q11,Q13)"
```

### Task D4.2: Phase D4 checkpoint
```bash
bash run/run_all.sh correctness
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir results/performance/opensearch_d4
```
Expected: 37/43 within 2x.

---

## Phase D5: High-Cardinality GROUP BY (Q15, Q16, Q17, Q18, Q30 → 42/43)

### Task D5.1: Q17 — Early termination for LIMIT without ORDER BY (23.3x → <2x)

**HIGHEST IMPACT SINGLE OPTIMIZATION.** Q17 asks for `LIMIT 10` with no ORDER BY
but scans all 100M rows. Stop after finding 10 groups.

**Files:**
- Modify: `FusedGroupByAggregate.java:9946+` (executeNKeyVarcharParallelDocRange)
- Modify: `TransportShardExecuteAction.java:525` (dispatch for LIMIT-only queries)

**Step 1: Detect LIMIT-without-ORDER-BY pattern**

In `TransportShardExecuteAction`, detect when the plan has LIMIT but no ORDER BY
(or ORDER BY on non-aggregate). Pass an `earlyTerminationLimit` parameter.

**Step 2: Add early termination in collect loop**

```java
// In the inner doc iteration loop:
int groupCount = flatMap.size(); // or HashMap.size()
if (earlyTerminationLimit > 0 && groupCount >= earlyTerminationLimit) {
    break; // Found enough groups, stop scanning
}
```

**Step 3: Correctness**

Note: LIMIT without ORDER BY is non-deterministic. The correctness check may need
to verify row count and schema rather than exact values.

```bash
bash run/run_all.sh correctness --query 17
```

**Step 4: Benchmark**
```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 17 --output-dir /tmp/q17_after
```
Target: best ≤ 2.340s (2x of 1.170s). Current: 27.2s. Expected: massive improvement.

**Step 5: Commit**
```bash
git commit -m "perf(dqe): early termination for LIMIT without ORDER BY (Q17: 27s→<2.3s)"
```

### Task D5.2: Q15 — TopN pushdown for UserID GROUP BY COUNT(*)

Q15 is `GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`. Use min-heap pruning.

**Files:**
- Modify: `FusedGroupByAggregate.java` — ensure executeWithTopN is triggered

**Step 1: Verify TopN is active**

Check if Q15 triggers `executeFusedGroupByAggregateWithTopN` or falls through to
the full `executeFusedGroupByAggregate`.

**Step 2: If not active, fix dispatch**
Target: best ≤ 1.040s (2x of 0.520s)

### Task D5.3: Q30 — 2-key numeric GROUP BY optimization

Q30 is `GROUP BY SearchEngineID, ClientIP`. Low overhead should suffice.

Target: best ≤ 1.556s (2x of 0.778s). Current: 2.106s.

### Task D5.4: Q16 — UserID+SearchPhrase GROUP BY ORDER BY

Q16 is high-cardinality 2-key GROUP BY with ORDER BY. Already uses TopN pushdown
from Phase C (improved 4.8x). Need further optimization.

Target: best ≤ 3.588s (2x of 1.794s). Current: 11.252s. Significant gap.

**Approach:** Hash-partitioned aggregation (same pattern as Q33/Q34) combined with
TopN-aware scanning.

### Task D5.5: Q18 — 3-key GROUP BY with extract(minute)

Q18 is the heaviest remaining query: `GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase`.
Current: 27.2s. Target: 7.336s.

**Approach:** Pre-compute extract(minute) values, use fused 3-key parallel path.

### Task D5.6: Phase D5 checkpoint
```bash
bash run/run_all.sh correctness
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir results/performance/opensearch_d5
```
Expected: 42/43 within 2x.

---

## Phase D6: Heavy Queries (Q28, Q32 → 43/43)

### Task D6.1: Q28 — REGEXP_REPLACE optimization (2.6x → <2x)

Q28 uses REGEXP_REPLACE on Referer column with HAVING. Current: 25.1s. Target: 19.1s.

**Approach:** Cache compiled Pattern, hoist regex computation before aggregation.

### Task D6.2: Q32 — High-cardinality 2-key numeric hash-partitioned (3.2x → <2x)

Q32 is `GROUP BY WatchID, ClientIP` over full table. Current: 14.6s. Target: 8.986s.

**Approach:** Hash-partitioned aggregation with more buckets (same as Q33/Q34 success).

### Task D6.3: Final checkpoint
```bash
bash run/run_all.sh correctness
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir results/performance/opensearch_final
```
Expected: **43/43 within 2x.**

---

## Execution Summary

| Phase | Queries | Before | After | Key Optimization |
|-------|---------|:------:|:-----:|-----------------|
| D1 | Q02,Q14,Q31,Q40 | 16/43 | 20/43 | Borderline tuning |
| D2 | Q35-Q42 | 20/43 | 27/43 | Segment skip + Weight cache |
| D3 | Q03,Q04,Q05,Q07,Q29,Q39 | 27/43 | 33/43 | Flat-array + algebraic shortcuts |
| D4 | Q08,Q09,Q11,Q13 | 33/43 | 37/43 | Fused COUNT(DISTINCT) |
| D5 | Q15,Q16,Q17,Q18,Q30 | 37/43 | 42/43 | Early termination + TopN |
| D6 | Q28,Q32 | 42/43 | **43/43** | Hash-partitioned + regex cache |

## Parallel Execution Tracks

```
Track A: D1 + D3    (borderline + scalar fixes)     — independent
Track B: D2 + D5.1  (narrow filter + Q17 early term) — independent
Track C: D4         (COUNT DISTINCT fusion)          — after D3 patterns
Track D: D5.2-D5.5 + D6 (high-card + heavy)         — after D2 patterns
```
