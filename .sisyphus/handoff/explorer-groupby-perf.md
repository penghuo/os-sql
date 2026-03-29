# DQE GROUP BY Execution Analysis

## 1. FusedGroupByAggregate.java — Hash Aggregation Implementation

### HashMap Types (all custom open-addressing with linear probing)

| Class | Keys | Storage | Max Capacity | Initial Cap |
|-------|------|---------|-------------|-------------|
| `FlatSingleKeyMap` (L11970) | 1 × `long` | `long[] keys` + `boolean[] occupied` + `long[] accData` (contiguous) | 8M | 4096 |
| `FlatTwoKeyMap` (L11692) | 2 × `long` | `long[] keys0/keys1` + `boolean[] occupied` + `long[] accData` | 8M | 8192 |
| `FlatThreeKeyMap` (L11842) | 3 × `long` | `long[] keys0/keys1/keys2` + `boolean[] occupied` + `long[] accData` | 8M | 8192 |
| `SingleKeyHashMap` (L11581) | 1 × `long` | `long[] keys` + `AccumulatorGroup[] groups` (object per group) | 8M | 4096 |
| `TwoKeyHashMap` (L11464) | 2 × `long` | `long[] keys0/keys1` + `AccumulatorGroup[] groups` | 8M | 8192 |
| For varchar N-key | N × mixed | `HashMap<MergedGroupKey, AccumulatorGroup>` (JDK HashMap) | unbounded | default |

### Key Encoding
- **Numeric keys**: Raw `long` values stored directly in primitive arrays. Hash via Murmur3 finalizer (`hash1`/`hash2`/`hash3`).
- **VARCHAR keys**: Per-segment ordinals (`SortedSetDocValues.nextOrd()`) used as keys during segment scan. Cross-segment: ordinals resolved to `BytesRef` strings, stored in `MergedGroupKey` (wraps `Object[]` of mixed `Long`/`BytesRef`).
- **Expressions** (DATE_TRUNC, EXTRACT, arithmetic): Computed inline during key extraction, stored as `long`.

### Memory Management
- **Hard cap**: `MAX_CAPACITY = 8_000_000` groups per flat map. Exceeding throws `RuntimeException`.
- **Load factor**: 0.7 for all flat maps. Resize doubles capacity via `System.arraycopy`.
- **No spill-to-disk**: All aggregation is purely in-memory. No off-heap, no disk spill.
- **Hash-partitioned aggregation** (see §5) is the workaround for exceeding 8M groups.

### Accumulator Types
- `CountStarAccum`: single `long count`
- `SumAccum`: `long sum` or `double sum`
- `AvgAccum`: `long sum + long count` (2 slots in flat path)
- `MinAccum`/`MaxAccum`: `long val` + `boolean hasValue` (or `Object objectVal` for varchar)
- `CountDistinctAccum`: `HashSet<Object>` (not flat-compatible)

**Flat path** stores accumulators as contiguous `long[]` at `slot * slotsPerGroup`, eliminating per-group object allocation entirely. Only works for COUNT/SUM/AVG (not MIN/MAX/COUNT DISTINCT).

---

## 2. Intra-Shard Parallelism

**Yes, parallelism exists** — configured via system properties:

```java
// FusedGroupByAggregate.java:L115-127
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange"); // default ON
private static final int THREADS_PER_SHARD =
    Math.max(1, Runtime.getRuntime().availableProcessors() / Integer.getInteger("dqe.numLocalShards", 4));
private static final ForkJoinPool PARALLEL_POOL = new ForkJoinPool(
    Runtime.getRuntime().availableProcessors(), ..., true); // asyncMode=true for work-stealing
```

### Parallelism strategies:

1. **Segment-level parallelism** (`executeSingleKeyNumericFlat`, L4400+): Segments assigned to workers via largest-first greedy load balancing. Each worker gets its own `FlatSingleKeyMap`, results merged via `mergeFrom()` (element-wise addition of `accData`).

2. **Doc-range parallelism** (`executeNKeyVarcharParallelDocRange`, L10241+): For N-key varchar paths, matching doc IDs collected per segment, then split into chunks across `numWorkers`. Each worker gets its own `Map<MergedGroupKey, AccumulatorGroup>` (JDK `LinkedHashMap`). Results merged via `mergeMergedGroupMaps()`.

3. **Bucket-level parallelism** (L4990+): When estimated groups > 8M, key space partitioned into buckets. Each bucket processed as a separate pass (or in parallel via `CompletableFuture`). Each pass only aggregates docs whose `hash(key) % numBuckets == bucket`.

### Bottleneck for high-cardinality (17M-50M groups):
- **Flat maps cap at 8M** → forces multi-bucket partitioned aggregation → multiple full data passes
- **VARCHAR N-key path uses JDK `HashMap`** with `MergedGroupKey` objects → heavy GC pressure at 17M+ groups
- **No vectorized/SIMD processing** — doc-at-a-time with `advanceExact()` per DocValues field

---

## 3. Coordinator Merge of GROUP BY Results

**File**: `TransportTrinoSqlAction.java` (L168-847) + `ResultMerger.java`

### Merge dispatch (coordinator receives `List<List<Page>>` from shards):

| Condition | Merge Path | Method |
|-----------|-----------|--------|
| FINAL agg + no HAVING + has SORT + has LIMIT | **Fused merge+sort** (top-N heap) | `merger.mergeAggregationAndSort()` |
| FINAL agg + no HAVING + no SORT + has LIMIT | **Capped merge** (stop at N groups) | `merger.mergeAggregationCapped()` |
| FINAL agg + fallback | Separate merge → HAVING → sort | `merger.mergeAggregation()` |
| SINGLE agg (shards sent raw data) | Coordinator re-aggregates | `runCoordinatorAggregation()` |

### Fast merge paths in ResultMerger:
- **Numeric-only keys**: `mergeAggregationFastNumeric` — uses `HashMap<Long, long[]>` or `HashMap<List<Long>, long[]>` for multi-key. Single-key: `mergeSingleKeyNumericCount` with primitive `long[]` arrays.
- **VARCHAR keys**: `mergeAggregationFastVarchar` — `HashMap<String, long[]>`.
- **Mixed keys**: `mergeAggregationFastMixed` — `HashMap<MixedMergeKey, long[]>`.
- **Fallback**: `HashAggregationOperator` (generic operator-based merge).

### Fused merge+sort (top-N):
`mergeAggregationAndSort` dispatches to `*WithSort` variants that maintain a min/max-heap of size `limit` during merge, avoiding full materialization of all groups. This is the **LIMIT pushdown** at coordinator level.

---

## 4. LIMIT Pushdown in GROUP BY + ORDER BY + LIMIT

### Shard-level:
- `executeWithTopN()` (L906) passes `sortAggIndex`, `sortAscending`, `topN` into `executeInternal`.
- **Within shard**: After aggregation completes, `mergePartitionedPages()` (L8216) applies top-N via min-heap selection on the aggregate sort column. This is **post-aggregation top-N** — all groups are still computed, only output is trimmed.
- **Early termination for LIMIT without ORDER BY**: `FlatTwoKeyMap.findOrInsertCapped()` (L11754) rejects new groups once `size >= cap`. `executeNKeyVarcharParallelDocRange` uses `AtomicBoolean stopFlag` to halt workers early.

### Coordinator-level:
- `mergeAggregationAndSort()` fuses merge + top-N heap in a single pass.
- `mergeAggregationCapped()` stops accepting new groups after reaching the limit.

### Gap:
- **No shard-level top-N pushdown for ORDER BY + LIMIT on GROUP BY**: The shard must compute ALL groups before selecting top-N. Each shard cannot know the global top-N without seeing all shards' data. This is fundamental to distributed aggregation.
- **No approximate/streaming top-N** (e.g., lossy count, Space-Saving algorithm).

---

## 5. Hash-Partitioned Aggregation & Spill-to-Disk

### Hash-Partitioned Aggregation: YES (limited)
- **Trigger**: When `totalDocs > MAX_CAPACITY (8M)`, `numBuckets = ceil(totalDocs / 8M)`.
- **Mechanism**: Multiple passes over the data. Each pass filters docs by `hash(key) % numBuckets == currentBucket`. Only groups in the current bucket are aggregated.
- **Parallelism**: Buckets can run in parallel via `CompletableFuture` on `PARALLEL_POOL`.
- **Applies to**: `executeTwoKeyNumericFlat` (L4952+), `executeThreeKeyFlat` (L8936+).
- **Does NOT apply to**: Generic N-key varchar path (`executeNKeyVarcharPath`) — uses unbounded JDK `HashMap`.

### Spill-to-Disk: NO
- **No spill mechanism exists anywhere** in the codebase. All aggregation is purely heap-based.
- The 8M cap + hash-partitioning is the only memory management strategy.

---

## Hot Path Summary for Failing Queries

| Query | Keys | Path | Bottleneck |
|-------|------|------|-----------|
| Q15 (UserID) | 1 varchar | `executeSingleVarcharGeneric` → ordinal-indexed `AccumulatorGroup[]` per segment, `HashMap<MergedGroupKey, AccumulatorGroup>` cross-segment | 17M+ MergedGroupKey objects → GC pressure |
| Q16 (UserID+SearchPhrase) | 2 mixed (long+varchar) | `executeWithVarcharKeys` → `executeNKeyVarcharPath` or `executeMultiSegGlobalOrdFlatTwoKey` | JDK HashMap with MergedGroupKey, no flat path for mixed varchar |
| Q18 (UserID+minute+SearchPhrase) | 3 mixed | `executeNKeyVarcharPath` → `executeThreeKeyFlat` (if all numeric after ordinal resolution) or generic HashMap | 50M groups × MergedGroupKey objects |
| Q32 (WatchID+ClientIP) | 2 numeric | `executeTwoKeyNumericFlat` with hash-partitioned buckets | Multiple data passes (ceil(docs/8M) buckets), but flat path is efficient |

### Key Performance Issues:
1. **Object allocation**: VARCHAR paths use `MergedGroupKey` (Object[] wrapper) + `AccumulatorGroup` + individual `MergeableAccumulator` objects per group. At 17M-50M groups, this creates massive GC pressure.
2. **No flat path for mixed varchar+numeric keys**: Only pure-numeric keys get the zero-allocation `FlatTwoKeyMap`/`FlatThreeKeyMap`. Mixed keys fall back to JDK HashMap.
3. **Multi-pass for >8M groups**: Hash-partitioned aggregation re-scans all docs per bucket. With 50M groups, that's ~7 passes.
4. **Coordinator merge is single-threaded**: All shard results merged sequentially in `ResultMerger`.
5. **No vectorized batch processing**: Doc-at-a-time with individual `advanceExact()` calls per field per doc.
