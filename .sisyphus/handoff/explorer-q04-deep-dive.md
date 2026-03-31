# Q04 Deep Dive: Scalar COUNT(DISTINCT) Optimization

## Key Files
- `FusedScanAggregate.java` — `collectDistinctValuesRaw()` at line 1510
- `TransportShardExecuteAction.java` — `executeDistinctValuesScanWithRawSet()` at line 3023, `executeCountDistinctWithHashSets()` at line 1000
- `FusedGroupByAggregate.java` — `executeSingleKeyNumericFlat()` at line 5143, `CountDistinctAccum` at line 14119, parallelism config at line 117
- `PlanFragmenter.java` — SINGLE step handling at line 95
- `LongOpenHashSet.java` — full implementation (open-addressing, LOAD_FACTOR=0.65)

## Architecture Summary

### Q04 Flow: `SELECT COUNT(DISTINCT UserID) FROM hits`
1. **PlanFragmenter** (line 168): SINGLE step with no GROUP BY → strips AggregationNode, sends bare `TableScanNode(columns=[UserID])` to shards
2. **TransportShardExecuteAction** (line 269): `isBareSingleNumericColumnScan()` matches → calls `executeDistinctValuesScanWithRawSet()`
3. **executeDistinctValuesScanWithRawSet** (line 3023): Calls `FusedScanAggregate.collectDistinctValuesRaw()`, attaches raw `LongOpenHashSet` to response
4. **collectDistinctValuesRaw** (line 1510): **SINGLE-THREADED** sequential scan of all segments. Comment says "parallel with merge was 4.3x SLOWER due to LongOpenHashSet merge cost"
5. **Coordinator merge** (TransportTrinoSqlAction line 2025): `mergeCountDistinctValuesViaRawSets()` — finds largest set, merges others, parallel contains() check

### Q08 Flow: `SELECT RegionID, COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ...`
1. **PlanFragmenter**: SINGLE with GROUP BY + COUNT(DISTINCT) → builds dedup plan: `AggregationNode(PARTIAL, groupBy=[RegionID, UserID], aggs=[COUNT(*)])`
2. **TransportShardExecuteAction** (line 327): Matches 2-key numeric COUNT(*) pattern → calls `executeCountDistinctWithHashSets()`
3. **executeCountDistinctWithHashSets** (line 1000): **PARALLEL** segment scanning via `FusedGroupByAggregate.getParallelPool()`. Each segment builds its own `Map<Long, LongOpenHashSet>`, then merges. Works because per-group sets are SMALL (~few hundred values each).

## Why Q08 Parallelism Works But Q04's Doesn't

**Q08**: ~450 groups (RegionID), each with a small LongOpenHashSet. Merge = iterate 450 entries, union small sets. Cost is trivial.

**Q04**: 1 group (scalar), one GIANT LongOpenHashSet (~17M entries for UserID). If parallelized naively:
- Each of N segments builds a ~4M-entry set
- Merging = iterating all entries of N-1 sets, probing into the target set
- The target set is too large for L3 cache → every probe is a cache miss
- Total merge work: ~17M random probes into a ~200MB hash table

## Key Code: collectDistinctValuesRaw (FusedScanAggregate.java:1510)

```java
public static LongOpenHashSet collectDistinctValuesRaw(
    String columnName, IndexShard shard, Query query) throws Exception {
  LongOpenHashSet distinctSet;
  try (Engine.Searcher engineSearcher = shard.acquireSearcher("dqe-fused-distinct-values-raw")) {
    int totalDocs = engineSearcher.getIndexReader().maxDoc();
    distinctSet = totalDocs > 0
        ? new LongOpenHashSet(Math.min(totalDocs, 32_000_000))
        : new LongOpenHashSet();
    if (query instanceof MatchAllDocsQuery) {
      // Sequential scan — comment: parallel was 4.3x SLOWER
      for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
        LeafReader reader = leafCtx.reader();
        Bits liveDocs = reader.getLiveDocs();
        SortedNumericDocValues dv = reader.getSortedNumericDocValues(columnName);
        if (dv == null) continue;
        if (liveDocs == null) {
          int doc = dv.nextDoc();
          while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            distinctSet.add(dv.nextValue());
            doc = dv.nextDoc();
          }
        } else {
          int maxDoc = reader.maxDoc();
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs.get(doc) && dv.advanceExact(doc)) {
              distinctSet.add(dv.nextValue());
            }
          }
        }
      }
    } else { /* Collector-based path for filtered queries */ }
  }
  return distinctSet;
}
```

## Key Code: executeCountDistinctWithHashSets (TransportShardExecuteAction.java:1000)

```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1) throws Exception {
  // ... setup ...
  Map<Long, LongOpenHashSet> finalSets;
  try (Engine.Searcher engineSearcher = shard.acquireSearcher("dqe-count-distinct-hashset")) {
    List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
    // ... weight setup ...
    if (leaves.size() <= 1) {
      finalSets = scanSegmentForCountDistinct(leaves.get(0), weight, keyName0, keyName1, isMatchAll);
    } else {
      // PARALLEL: dispatch segments to ForkJoinPool
      Map<Long, LongOpenHashSet>[] segResults = new Map[leaves.size()];
      CountDownLatch segLatch = new CountDownLatch(leaves.size() - 1);
      for (int s = 0; s < leaves.size() - 1; s++) {
        final int segIdx = s;
        final LeafReaderContext leafCtx = leaves.get(s);
        FusedGroupByAggregate.getParallelPool().execute(() -> {
          segResults[segIdx] = scanSegmentForCountDistinct(leafCtx, weight, keyName0, keyName1, isMatchAll);
          segLatch.countDown();
        });
      }
      // Last segment on current thread
      segResults[leaves.size() - 1] = scanSegmentForCountDistinct(leaves.get(leaves.size()-1), ...);
      segLatch.await();
      // Merge: union LongOpenHashSets per key0 (small sets, cheap merge)
      finalSets = segResults[0];
      for (int s = 1; s < segResults.length; s++) {
        for (var entry : segResults[s].entrySet()) {
          LongOpenHashSet existing = finalSets.get(entry.getKey());
          if (existing == null) { finalSets.put(entry.getKey(), entry.getValue()); }
          else { /* merge smaller into larger */ }
        }
      }
    }
  }
  // Build compact output page + attach sets
}
```

## Parallelism Config (FusedGroupByAggregate.java:117)

```java
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
private static final int THREADS_PER_SHARD =
    Math.max(1, Runtime.getRuntime().availableProcessors() / Integer.getInteger("dqe.numLocalShards", 4));
private static final ForkJoinPool PARALLEL_POOL =
    new ForkJoinPool(Runtime.getRuntime().availableProcessors(), ..., null, true);
```

## FusedGroupByAggregate: COUNT(DISTINCT) NOT supported in flat path

From line 3546-3548:
```java
case "COUNT":
  if (spec.isDistinct) {
    accType[i] = 5;
    canUseFlatAccumulators = false;  // <-- falls back to object-based SingleKeyHashMap
  }
```

The flat path (`executeSingleKeyNumericFlat`) only supports accType 0 (COUNT), 1 (SUM long), 2 (AVG long). COUNT(DISTINCT) (accType=5) forces the non-flat `SingleKeyHashMap` path with `CountDistinctAccum` objects.

## canFuse Comparison

**FusedScanAggregate.canFuse** (line 79): Returns true for scalar aggs (no GROUP BY) over TableScanNode. Supports COUNT(DISTINCT) — but the current code path strips the AggregationNode before reaching this for SINGLE step.

**FusedGroupByAggregate.canFuse** (line 198): Returns FALSE if `groupByKeys.isEmpty()`. Cannot handle scalar aggregation.

## LongOpenHashSet Merge Cost Analysis

`addAll()` iterates the entire backing array (capacity, not size) of the source set. With LOAD_FACTOR=0.65, a 17M-entry set has capacity ~26M. Merging requires:
- Iterating 26M slots (checking for EMPTY)
- For each non-empty slot (~17M), calling `add()` on target → hash + probe → random memory access
- Target set is also ~26M slots = ~200MB → doesn't fit in L3 cache

## Optimization Approaches

### Approach A: Parallelize collectDistinctValuesRaw with shared set
Instead of per-segment sets + merge, use a single shared `ConcurrentLongOpenHashSet`:
- Each worker thread scans its segment and adds to the shared set
- No merge step needed
- Requires implementing a lock-free or striped concurrent hash set
- Risk: contention on shared set for high-cardinality columns

### Approach B: Parallel scan with bitset-based merge
- Each segment scans into a local `long[]` array (just collecting values, no dedup)
- Single thread inserts all values into one pre-sized LongOpenHashSet
- Parallelizes the I/O-bound DocValues read, keeps the CPU-bound hash insertion sequential
- Simpler but may not help if hash insertion is the bottleneck

### Approach C: Route through FusedGroupByAggregate with synthetic GROUP BY
- Treat `COUNT(DISTINCT col)` as `SELECT COUNT(*) FROM (SELECT col FROM t GROUP BY col)`
- The inner GROUP BY goes through `executeSingleKeyNumericFlat` which IS parallelized
- FlatSingleKeyMap already handles high-cardinality (10M+ threshold for sequential)
- Coordinator just sums the counts
- Requires changes to PlanFragmenter + FusedGroupByAggregate.canFuse

### Approach D: Parallel segment scan with columnar cache + single-set insertion
Like executeSingleKeyNumericFlat's high-cardinality path (line 5230):
```java
// For high-cardinality keys (totalDocs > 10M), scan sequentially into main map.
// Parallel workers each build 4.4M-entry maps that thrash L3 cache and require
// expensive mergeFrom. Sequential scan uses ONE map with better cache locality.
if (totalDocs > 10_000_000) {
  flatMap = new FlatSingleKeyMap(slotsPerGroup, 8_000_000);
  for (int s = 0; s < segKeyArrays.size(); s++) {
    scanDocRangeFlatSingleKeyCountStar(...);
  }
}
```
Apply same pattern: parallel `loadNumericColumn()` per segment, then sequential insert into one LongOpenHashSet.
