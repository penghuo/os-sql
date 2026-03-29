# DQE Implementation Details — Explorer Findings

## 1. COUNT(DISTINCT) Fusion (Priority 1) — 6 Queries: Q04, Q05, Q08, Q09, Q11, Q13

### Root Cause
Calcite decomposes `GROUP BY x, COUNT(DISTINCT y)` into two-level aggregation:
1. Inner: `GROUP BY (x, y), COUNT(*)` — produces ~10K-50K rows per shard
2. Outer: `GROUP BY x, COUNT(*)` — reduces at coordinator

This doubles the key space and prevents fused single-pass aggregation.

### Current Implementation (AggDedupNode Detection)

**File:** `TransportShardExecuteAction.java` lines 273-335

The detection logic at line 278 checks:
```java
effectivePlan instanceof AggregationNode aggDedupNode
    && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
    && aggDedupNode.getGroupByKeys().size() == 2
    && FusedGroupByAggregate.canFuse(aggDedupNode, columnTypeMap)
```

Then dispatches to three paths:
- **isSingleCountStar** (line 284): `aggFunctions.size()==1 && "COUNT(*)".equals(...)` → `executeCountDistinctWithHashSets()` (line 304) for 2 numeric keys, or `executeVarcharCountDistinctWithHashSets()` (line 312) for varchar+numeric
- **isMixedDedup** (line 288-291): all aggs match `(sum|count)\(.*\)` → `executeMixedDedupWithHashSets()` (line 330)

### Current executeCountDistinctWithHashSets Method

**File:** `TransportShardExecuteAction.java` lines 936-1063

**Signature:**
```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)
```

**How it works:**
1. Acquires searcher, compiles Lucene query
2. Parallel segment scanning: each segment builds `Map<Long, LongOpenHashSet>` (key0 → set of key1 values)
3. Merges segment results by unioning HashSets per key0
4. Outputs compact page: `(key0, key1_placeholder=0, COUNT(*)=local_distinct_count)` + attached HashSets via `resp.setDistinctSets(finalSets)`
5. Coordinator unions HashSets across shards for final COUNT(DISTINCT)

**Per-segment scan** (lines 1065-1100+): `scanSegmentForCountDistinct()` reads `SortedNumericDocValues` for both keys, builds per-key0 `LongOpenHashSet` of key1 values.

### What Needs to Change for COUNT(DISTINCT) Fusion

The handover doc (Section 9, Category A) says:
> Fix: intercept at `TransportShardExecuteAction` dispatch (line ~280-360, `AggDedupNode` detection), route to fused GROUP BY with per-group `LongOpenHashSet`.

The impl plan (Phase D4, Task D4.1) specifies:
1. **Add `CountDistinctAccum` to FusedGroupByAggregate** (line 11224+) — already exists! See `FusedGroupByAggregate.java` inner class `CountDistinctAccum` which holds `LongOpenHashSet` for numeric or `HashSet<Object>` for varchar.
2. **Route GROUP BY + COUNT(DISTINCT) to fused path** — detect the Calcite-decomposed pattern in TransportShardExecuteAction and instead of running the two-level plan, run a single-level fused GROUP BY with `CountDistinctAccum`.
3. **The `CountDistinctAccum` already works** — it's used in `FusedGroupByAggregate.execute()` when `canFuse()` detects `COUNT(DISTINCT col)` in the aggregate functions. The issue is that Calcite decomposes the plan BEFORE it reaches the fused path.

### Fused Path Should Look Like

Instead of the current two-level plan:
```
AggregationNode(FINAL, GROUP BY x, COUNT(*))
  └─ AggregationNode(PARTIAL, GROUP BY x, y, COUNT(*))  ← this is what shards see
       └─ TableScanNode
```

The fused path should:
1. Detect the two-level pattern at coordinator (PlanOptimizer or PlanFragmenter)
2. Rewrite to single-level: `AggregationNode(GROUP BY x, COUNT(DISTINCT y), SUM(...), ...)` 
3. Send to shards where `FusedGroupByAggregate.canFuse()` already handles `COUNT(DISTINCT y)` via `CountDistinctAccum`

**OR** (simpler approach per handover):
1. At shard level in TransportShardExecuteAction, detect the PARTIAL AggregationNode with 2 keys where key1 is the DISTINCT target
2. Rewrite locally to single-key GROUP BY with CountDistinctAccum
3. Return per-group HashSets to coordinator for cross-shard union

### Affected Queries
| Query | Pattern | Current | Target |
|-------|---------|---------|--------|
| Q04 | `COUNT(DISTINCT UserID)` global | 2.4s | ≤0.87s |
| Q05 | `COUNT(DISTINCT SearchPhrase)` global | 4.0s | ≤1.38s |
| Q08 | `GROUP BY RegionID, COUNT(DISTINCT UserID)` | 1.9s | ≤1.08s |
| Q09 | `GROUP BY RegionID, mixed aggs + COUNT(DISTINCT)` | 4.9s | ≤1.23s |
| Q11 | `GROUP BY MobilePhone, Model, COUNT(DISTINCT)` | 2.6s | ≤0.54s |
| Q13 | `GROUP BY SearchPhrase, COUNT(DISTINCT UserID)` | 6.0s | ≤1.91s |

---

## 2. Parallelize executeSingleKeyNumericFlat (Priority 2)

### Current Implementation

**File:** `FusedGroupByAggregate.java` line 4350

**Signature:**
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int numAggs, boolean[] isCountStar, int[] accType,
    int sortAggIndex, boolean sortAscending, long topN)
```

**How it works:**
- Uses `FlatSingleKeyMap` — open-addressing hash map with single long key + contiguous `long[]` accumulator storage
- Each aggregate maps to fixed slots: COUNT/SUM=1 slot, AVG=2 slots (sum+count)
- Eliminates ALL per-group object allocation (no AccumulatorGroup, no MergeableAccumulator)
- Already has parallel path (lines ~4420-4480): partitions segments across workers using `ForkJoinPool`, each with own `FlatSingleKeyMap`, then merges via `flatMap.mergeFrom(workerMap)`

**Dispatch path:** Called from `executeSingleKeyNumeric()` (line 3242) when `canUseFlatAccumulators` is true (no DISTINCT, no double args, no VARCHAR args, no MIN/MAX).

### What the Handover Says
> Parallelize single-key path (Q15 and similar)

Q15 is `GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` — single numeric key with TopN. The `executeSingleKeyNumericFlat` already has parallelism. The issue may be that Q15 doesn't reach this path (it may go through `executeSingleKeyNumeric` with AccumulatorGroup objects because of TopN).

### Recommended Fix
Ensure Q15 triggers `executeSingleKeyNumericFlat` with TopN support. The flat path already handles `sortAggIndex` and `topN` parameters (lines 4500+). Verify the dispatch chain: `executeInternal` → `executeNumericOnly` → `executeSingleKeyNumeric` → `executeSingleKeyNumericFlat`.

---

## 3. Hash-Partitioned Aggregation (Priority 3) — Q16, Q18, Q32

### Current Implementation

**File:** `FusedGroupByAggregate.java` — `executeTwoKeyNumericFlat()` line ~5242

Already implements hash-partitioned aggregation for 2-key numeric GROUP BY:
- Estimates `numBuckets = ceil(totalDocs / FlatTwoKeyMap.MAX_CAPACITY)` (MAX_CAPACITY = 8M)
- When `numBuckets > 1`: runs multiple passes, each aggregating only groups whose `hash(key0, key1) % numBuckets == bucket`
- Parallel bucket execution via `CompletableFuture` + `PARALLEL_POOL`
- Results merged via `mergePartitionedPages()`

### What Needs to Change
For Q16 (UserID+SearchPhrase, 11.3s→3.6s target) and Q32 (WatchID+ClientIP, 14.6s→9.0s target):
- Increase bucket count or improve hash distribution
- Q18 (3-key) needs `FlatThreeKeyMap` hash-partitioned path (already exists at line ~7800 via `executeThreeKeyFlat`)

### Handover Guidance
> Hash-partitioned aggregation with more buckets. Already proven on Q33/Q34.

The pattern is established. The fix is tuning: more buckets, better load balancing, or TopN-aware scanning within each bucket.

---

## 4. Borderline Fixes (Priority 4) — Q02, Q28, Q30, Q31, Q37

### Q02: SUM+COUNT+AVG borderline (0.2s vs 0.1s target)
**Impl plan Task D1.2:** Extend flat-array path in `FusedScanAggregate.java:478-486` to recognize mixed SUM+COUNT+AVG pattern. Check `tryFlatArrayPath` (line 609+) — may reject Q02 because of `ResolutionWidth + 1` expression.

### Q28: REGEXP_REPLACE GROUP BY HAVING (25.5s vs 9.5s target)
**Impl plan Task D6.1:** Cache compiled `java.util.regex.Pattern` in the expression evaluator. Currently recompiles per-doc or per-batch. Hoist regex computation before aggregation loop. File: expression evaluation in `FusedGroupByAggregate.executeWithExpressionKey()`.

### Q30: GROUP BY SearchEngineID, ClientIP (1.8s vs 0.8s target)
**Impl plan Task D5.3:** Optimize 2-key numeric merge. Already uses `executeTwoKeyNumericFlat`. May need better parallel segment distribution or reduced merge overhead.

### Q31: GROUP BY WatchID, ClientIP (2.2s vs 1.1s target — needs only 3ms!)
**Impl plan Task D1.3:** Optimize 2-key numeric path merge overhead in `executeTwoKeyNumericFlat` (line 4626+). Pre-size HashMap based on estimated cardinality to avoid resizing. The gap is tiny — 2.2s vs 2.19s target.

### Q37: CounterID=62 filtered URL GROUP BY (0.3s vs 0.1s target)
**Impl plan Task D2.1:** Segment-skip for narrow filters. Skip Scorer allocation on segments with 0 matching docs. Ensure early `scorer == null` check before any DocValues opening or buffer allocation.

---

## 5. Code Structure Summary

### FusedGroupByAggregate.java Key Line Numbers
| Method | Line | Purpose |
|--------|------|---------|
| `canFuse()` | ~155 | Eligibility check for fused GROUP BY |
| `executeInternal()` | ~932 | Main dispatch: varchar vs numeric keys |
| `executeSingleVarcharCountStar()` | ~1268 | VARCHAR + COUNT(*) with global ordinals |
| `executeSingleVarcharGeneric()` | ~1937 | VARCHAR + any aggregates |
| `executeNumericOnly()` | ~2786 | Numeric GROUP BY dispatch |
| `executeSingleKeyNumeric()` | ~3100 | Single numeric key with AccumulatorGroup |
| `executeSingleKeyNumericFlat()` | 4350 | Single numeric key with flat long[] storage |
| `executeTwoKeyNumericFlat()` | ~5242 | Two numeric keys with flat storage |
| `executeThreeKeyFlat()` | ~7800 | Three keys with flat storage |
| `executeWithEvalKeys()` | ~6500 | EvalNode-computed expression keys |
| `executeNKeyVarcharPath()` | ~8657 | Multi-key VARCHAR dispatch |
| `executeNKeyVarcharParallelDocRange()` | ~9946 | Parallel multi-key |
| `CountDistinctAccum` | ~11500 | Per-group HashSet accumulator (already exists!) |

### TransportShardExecuteAction.java Key Line Numbers
| Section | Lines | Purpose |
|---------|-------|---------|
| Bare TableScan COUNT(DISTINCT) numeric | 246-255 | Pre-dedup for scalar COUNT(DISTINCT) |
| Bare TableScan COUNT(DISTINCT) varchar | 256-271 | Pre-dedup for varchar COUNT(DISTINCT) |
| AggDedupNode detection | 273-335 | Detects decomposed COUNT(DISTINCT) pattern |
| isSingleCountStar dispatch | 292-313 | Routes to executeCountDistinctWithHashSets |
| isMixedDedup dispatch | 315-332 | Routes to executeMixedDedupWithHashSets |
| Expression GROUP BY dispatch | 336-350 | Routes to executeWithExpressionKey |
| Generic fused GROUP BY | 352-365 | Routes to FusedGroupByAggregate.execute() |
| Sort+Limit GROUP BY | 367-400 | Routes to executeWithTopN |
| executeCountDistinctWithHashSets | 936-1063 | HashSet-per-group for 2 numeric keys |
| scanSegmentForCountDistinct | 1065-1100+ | Per-segment scan helper |
| executeMixedDedupWithHashSets | 1224-1436 | Mixed SUM/COUNT dedup with HashSets |

### Key Data Structures
| Class | Location | Purpose |
|-------|----------|---------|
| `FlatSingleKeyMap` | FusedGroupByAggregate inner class | Open-addressing, single long key, flat long[] accumulators |
| `FlatTwoKeyMap` | FusedGroupByAggregate inner class | Open-addressing, two long keys, flat long[] accumulators, MAX_CAPACITY=8M |
| `FlatThreeKeyMap` | FusedGroupByAggregate inner class | Three long keys variant |
| `SingleKeyHashMap` | FusedGroupByAggregate inner class | Single long key → AccumulatorGroup (for non-flat aggs) |
| `TwoKeyHashMap` | FusedGroupByAggregate inner class | Two long keys → AccumulatorGroup |
| `CountDistinctAccum` | FusedGroupByAggregate inner class | `LongOpenHashSet` for numeric, `HashSet<Object>` for varchar |
| `LongOpenHashSet` | `dqe/.../operator/LongOpenHashSet.java` | Primitive long open-addressing hash set (no boxing) |
| `AccumulatorGroup` | FusedGroupByAggregate inner class | Array of MergeableAccumulator per group |

### Parallelism Infrastructure
- `PARALLEL_POOL`: Static `ForkJoinPool` with `availableProcessors()` threads, asyncMode=true
- `THREADS_PER_SHARD`: `availableProcessors() / numLocalShards` (default 4 shards)
- Segment partitioning: largest-first greedy assignment to workers
- Merge: `flatMap.mergeFrom(workerMap)` adds accumulator slots element-wise
