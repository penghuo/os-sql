# Hot Path Analysis: COUNT(DISTINCT) Methods in TransportShardExecuteAction.java

## Summary Table

| Query | Method | Parallelized? | Data Structures | Inner Loop | WHERE Filter |
|-------|--------|--------------|-----------------|------------|-------------|
| Q04 | executeDistinctValuesScanWithRawSet (line 3023) | **NO** — sequential across segments | Single LongOpenHashSet | nextDoc() iterator per segment | Collector-based for filtered; sequential nextDoc() for MatchAll |
| Q05 | executeDistinctValuesScanVarcharWithRawSet (line 3053) → collectDistinctStringsRaw | **PARTIAL** — parallel ordinal resolution only (>100K ordinals), but scan itself is sequential | HashSet<String>, OrdinalMap | Global ordinals iteration or nextDoc() per segment | Collector-based for filtered |
| Q08 | executeCountDistinctWithHashSets (line 1000) | **YES** — parallel segment scan via ForkJoinPool + CountDownLatch | Map<Long, LongOpenHashSet>, open-addressing hash map per segment | Columnar: flat array `for(doc=0;doc<maxDoc;doc++)`, or nextDoc() fallback | Weight/Scorer for filtered; MatchAll skips filter |
| Q09 | executeMixedDedupWithHashSets (line 1931) | **YES for MatchAll only** — CompletableFuture workers with greedy segment assignment | Open-addressing hash map: long[] keys + LongOpenHashSet[] + long[][] accumulators | `for(doc=0;doc<maxDoc;doc++)` with nextDoc() DV iteration | Sequential fallback for filtered queries |
| Q11 | executeMixedTypeCountDistinctWithHashSets (line 1649) | **YES** — parallel segment scan via ForkJoinPool + CountDownLatch | HashMap<ObjectArrayKey, LongOpenHashSet> | advanceExact() per doc via processDoc lambda | Weight/Scorer for filtered |
| Q13 | executeVarcharCountDistinctWithHashSets (line 2410) | **YES** — CompletableFuture workers with global ordinals | LongOpenHashSet[] indexed by global ordinal | nextDoc() SortedDocValues iteration per segment | Weight/Scorer for filtered |

## Key Findings

### 1. Q04 is COMPLETELY SINGLE-THREADED (5.27x gap)
- `collectDistinctValuesRaw()` at FusedScanAggregate.java:1510
- Comment in code: "parallel with merge was 4.3x SLOWER due to LongOpenHashSet merge cost"
- Uses single shared LongOpenHashSet, iterates all segments sequentially
- **Bottleneck**: For ~18M distinct UserIDs, the single HashSet.add() dominates
- **Opportunity**: Instead of merging huge sets, use per-segment bitsets or bloom filters for cardinality estimation, OR use columnar batch loading (loadNumericColumn) like Q08 does

### 2. Q05 is MOSTLY SINGLE-THREADED (4.79x gap)
- `collectDistinctStringsRaw()` at FusedScanAggregate.java:1587
- Parallel path only kicks in for ordinal→String resolution when globalOrdCount > 100K
- The actual doc scanning is sequential (iterates global ordinals, not docs)
- For MatchAll + no deletes: iterates global ordinals (already deduped) — this is actually efficient
- **Bottleneck**: String allocation and HashSet<String> insertion for high cardinality

### 3. Q08 is PARALLELIZED but has merge overhead (5.00x gap)
- Parallel segment scan with per-segment open-addressing hash maps
- Uses `FusedGroupByAggregate.loadNumericColumn()` for columnar batch loading (fast path)
- Merge phase: unions LongOpenHashSets per group key (smaller→larger optimization)
- **Bottleneck**: LongOpenHashSet merge cost when many groups have large distinct sets

### 4. Q09 is PARALLELIZED for MatchAll only (6.18x gap)
- CompletableFuture workers with greedy segment assignment by doc count
- Each worker builds independent open-addressing hash map with LongOpenHashSet + accumulators
- Sequential fallback for filtered queries
- **Bottleneck**: Per-doc DV iteration (nextDoc() pattern, not columnar), hash map resize storms

### 5. Q11 uses ObjectArrayKey boxing (6.57x gap)
- Parallel segment scan but uses HashMap<ObjectArrayKey, LongOpenHashSet>
- ObjectArrayKey wraps Object[] — causes boxing for every composite key lookup
- advanceExact() per doc per key column — not columnar
- **Bottleneck**: Object allocation pressure from ObjectArrayKey + advanceExact() overhead

### 6. Q13 is PARALLELIZED with global ordinals (8.23x gap, worst)
- Uses global ordinal arrays indexed by ordinal
- Parallel workers with CompletableFuture
- **Bottleneck**: Each worker allocates `new LongOpenHashSet[globalOrdCount]` — for high-cardinality VARCHAR keys this is massive memory allocation. Also per-doc advanceExact() for numeric DV.

## Thread Pool Details
- `PARALLEL_POOL` = ForkJoinPool with `availableProcessors()` threads (16 on this machine)
- `numWorkers` = `availableProcessors() / dqe.numLocalShards` (default 4) = **4 workers per shard**
- Pool is shared across all shard executions

## FusedGroupByAggregate CountDistinctAccum (Alternative Path)
- Located at FusedGroupByAggregate.java:14119
- Uses LongOpenHashSet for numeric types, HashSet<Object> for varchar/double
- Already integrated into the fused GROUP BY path (accType=5)
- canFuse() DOES support COUNT(DISTINCT) — `spec.isDistinct ? 5 : 0`
- However: `canUseFlatVarchar = false` when DISTINCT is present (line 8319)
- The fused path handles COUNT(DISTINCT) within GROUP BY aggregation, but the dedicated methods (Q04-Q13) exist as specialized fast paths

## Optimization Opportunities

### HIGH IMPACT
1. **Q04: Parallelize with per-segment sets + bitwise merge** — Instead of one shared set, each worker builds a segment-local LongOpenHashSet, then merge using addAll (which is O(smaller_set)). The code comment says merge was 4.3x slower, but that was likely before the size-based merge optimization now used in Q08.
2. **Q04/Q05: Use columnar batch loading** — Q08 already uses `FusedGroupByAggregate.loadNumericColumn()` to load flat long[] arrays. Q04 could do the same to avoid per-doc nextDoc() overhead.
3. **Q11: Replace ObjectArrayKey with primitive composite key** — For 2-key case (common), use a packed long or a specialized 2-key hash map to avoid Object[] allocation.

### MEDIUM IMPACT
4. **Q09: Use columnar loading for MatchAll path** — Currently uses nextDoc() DV iteration even in parallel workers. Switch to loadNumericColumn() like Q08.
5. **Q13: Lazy LongOpenHashSet allocation** — Don't allocate `new LongOpenHashSet[globalOrdCount]` per worker. Use a sparse map instead.
6. **All: Reduce LongOpenHashSet initial capacity** — Many groups may have few distinct values. Starting at 16 (Q13) vs default (Q08 segment scan) matters.

### LOWER IMPACT
7. **Q08/Q09 merge phase**: Already uses smaller→larger merge. Could further optimize by using a single pre-allocated merge target.
8. **Filtered query paths**: Q09 falls back to sequential for filtered queries. Could parallelize filtered paths too.
