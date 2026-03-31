# COUNT(DISTINCT) Dispatch Code — Shard & Coordinator

## File Locations
- **Shard dispatch**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
- **Coordinator merge**: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java`
- **Fused scan**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`

---

## 1. Q04: executeDistinctValuesScanWithRawSet (line 3025)

**Purpose**: Scalar `SELECT COUNT(DISTINCT col)` — no GROUP BY. Collects all distinct values into a single `LongOpenHashSet` per shard.

```java
// TransportShardExecuteAction.java:3025
private ShardExecuteResponse executeDistinctValuesScanWithRawSet(
    DqePlanNode plan, ShardExecuteRequest req) throws Exception {
  TableScanNode scanNode = (TableScanNode) plan;
  String indexName = scanNode.getIndexName();
  String columnName = scanNode.getColumns().get(0);
  CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);
  IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
  IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());
  Query luceneQuery = compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

  // Delegates to FusedScanAggregate — returns LongOpenHashSet of all distinct values
  LongOpenHashSet rawSet =
      FusedScanAggregate.collectDistinctValuesRaw(columnName, shard, luceneQuery);

  // Build minimal 1-row Page with count
  BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
  BigintType.BIGINT.writeLong(builder, rawSet.size());
  List<Type> columnTypes = List.of(BigintType.BIGINT);
  ShardExecuteResponse resp = new ShardExecuteResponse(List.of(new Page(builder.build())), columnTypes);
  resp.setScalarDistinctSet(rawSet);  // <-- attaches raw set for coordinator merge
  return resp;
}
```

**Data flow**: Returns `ShardExecuteResponse` with `scalarDistinctSet` = `LongOpenHashSet` containing all distinct long values from this shard.

---

## 2. collectDistinctValuesRaw (FusedScanAggregate.java:1674)

**Purpose**: Parallel two-phase scan of DocValues into `LongOpenHashSet`.

```java
// FusedScanAggregate.java:1674
public static LongOpenHashSet collectDistinctValuesRaw(
    String columnName, IndexShard shard, Query query) throws Exception {
  LongOpenHashSet distinctSet;
  try (Engine.Searcher engineSearcher = shard.acquireSearcher("dqe-fused-distinct-values-raw")) {
    distinctSet = new LongOpenHashSet(65536);
    if (query instanceof MatchAllDocsQuery) {
      // Phase 1: Parallel columnar load — read DocValues into flat long[] per segment
      // Phase 2: Parallel per-segment hash set insertion with run-length dedup, then merge
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      long[][] segArrays = new long[leaves.size()][];
      int[] segCounts = new int[leaves.size()];
      // ... parallel CompletableFuture loading into segArrays ...
      // ... parallel CompletableFuture building per-segment LongOpenHashSet with run-length dedup ...
      // Merge: find largest set, addAll smaller sets into it
      // Single segment: fused single-pass directly into distinctSet
    } else {
      // Filtered: use Collector with advanceExact per matching doc
      engineSearcher.search(query, new Collector() {
        // LeafCollector.collect(doc) → dv.advanceExact(doc) → filteredSet.add(dv.nextValue())
      });
    }
  }
  return distinctSet;
}
```

**Key optimization**: Two-phase (load arrays, then hash) is faster than fused because Phase 1 has sequential memory access and Phase 2 has sequential array reads.

---

## 3. Q08: executeCountDistinctWithHashSets (line 1001)

**Purpose**: `GROUP BY key0, COUNT(DISTINCT key1)` — builds `Map<Long, LongOpenHashSet>` (group key → set of distinct values).

```java
// TransportShardExecuteAction.java:1001
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1) throws Exception {
  // Setup: get shard, compile lucene query
  // Data structure: Map<Long, LongOpenHashSet> finalSets
  // Uses parallel segment scanning via ForkJoinPool

  // Per-segment scan uses open-addressing hash map:
  //   long[] grpKeys, LongOpenHashSet[] grpSets, boolean[] grpOcc
  // For each doc: hash key0 → probe → if new group, create LongOpenHashSet(1024)
  //               then grpSets[slot].add(key1)

  // MatchAll fast path: loads columns via FusedGroupByAggregate.loadNumericColumn()
  // Filtered path: uses Weight/Scorer with advanceExact()

  // Multi-segment: dispatches N-1 segments to ForkJoinPool, runs last on current thread
  // Merge: union LongOpenHashSets per key0 (merge smaller into larger)

  // Output page: (key0, key1_placeholder=0, COUNT(*)=local_distinct_count)
  List<Type> colTypes = List.of(type0_resolved, type1_resolved, BigintType.BIGINT);
  // ... build BlockBuilders, write key0 + 0 + set.size() per group ...

  ShardExecuteResponse resp = new ShardExecuteResponse(List.of(page), colTypes);
  resp.setDistinctSets(finalSets);  // <-- attaches Map<Long, LongOpenHashSet>
  return resp;
}
```

**Data structures**:
- Open-addressing hash map arrays: `long[] grpKeys`, `LongOpenHashSet[] grpSets`, `boolean[] grpOcc`
- Resize at 70% load factor, doubling capacity
- `scanSegmentForCountDistinct()` helper (line ~1130) handles per-segment logic

---

## 4. Q09: executeMixedDedupWithHashSets (line 1933)

**Purpose**: `GROUP BY key0` with mixed aggregates: `SUM(x)`, `COUNT(*)`, `AVG(y)`, AND `COUNT(DISTINCT key1)`. Combines regular accumulators with per-group HashSets.

```java
// TransportShardExecuteAction.java:1933
private ShardExecuteResponse executeMixedDedupWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1) throws Exception {
  // Parses aggFunctions list: regex "(?i)^(sum|count)\((.+?)\)$"
  // Resolves: aggArgNames[], aggIsCountStar[], aggIsCount[]

  // Data structures per group:
  //   long[] grpKeys          — group key values
  //   LongOpenHashSet[] grpSets — distinct key1 values per group
  //   long[][] grpAccs        — accumulator values per group (one long per agg)
  //   boolean[] grpOcc        — occupancy flags

  // Parallel MatchAll path: partitions segments across workers (largest-first greedy)
  //   Each worker builds its own hash map + sets + accumulators
  //   Workers return Object[] { wKeys, wSets, wAccs, wOcc, wCap, wSize }
  //   Main thread merges: union HashSets + sum accumulators per group

  // Sequential/filtered path: iterates segments, per-doc:
  //   1. Hash key0 → find/create group slot
  //   2. grpSets[slot].add(key1)
  //   3. For each agg: COUNT(*) → acc++, COUNT(col) → acc += 1, SUM(col) → acc += val

  // Output page: (key0, key1_placeholder=0, agg0, agg1, ...)
  // AVG decomposed to SUM + COUNT as two separate accumulators
  ShardExecuteResponse resp = new ShardExecuteResponse(List.of(page), colTypes);
  resp.setDistinctSets(distinctSets);  // <-- Map<Long, LongOpenHashSet>
  return resp;
}
```

---

## 5. Coordinator Merge Logic

### Q04 merge: mergeCountDistinctValuesViaRawSets (TransportTrinoSqlAction.java:2025)

```
Input: ShardExecuteResponse[].getScalarDistinctSet() → LongOpenHashSet per shard
Algorithm:
  1. Find largest set across shards
  2. Merge all non-largest sets into a temporary 'others' set
  3. Parallel count: split others.keys() into chunks, count entries NOT in largest
  4. total = largest.size() + extraCount + zero/sentinel adjustments
Output: Single Page with 1 row: COUNT(DISTINCT) = total
```

### Q08 merge: mergeDedupCountDistinctViaSets (TransportTrinoSqlAction.java:2527)

```
Input: ShardExecuteResponse[].getDistinctSets() → Map<Long, LongOpenHashSet> per shard
Algorithm:
  1. Collect all per-group per-shard sets: Map<Long, List<LongOpenHashSet>>
  2. If topN set and topN*10 < totalGroups: prune via upper-bound min-heap
  3. Per group: countMergedGroupSets() — find largest set, count extras not in largest
Output: Page with (key0, COUNT(DISTINCT)) per group
```

### Q09 merge: mergeMixedDedupViaSets (TransportTrinoSqlAction.java:3074)

```
Input: ShardExecuteResponse[] with both Pages (accumulators) and getDistinctSets()
Algorithm:
  1. For each shard page row: sum accumulators per key0 across shards
  2. For each shard's HashSet per key0: union into mergedSets (addAll)
  3. Build output: per coordinator agg function:
     - SUM/COUNT(*) → direct from merged accumulator
     - AVG → merged_sum / merged_count
     - COUNT(DISTINCT) → mergedSets.get(key0).size()
Output: Page with (key0, agg0, agg1, ...) per group
```

---

## Key Data Structures

| Structure | Type | Used In |
|-----------|------|---------|
| `LongOpenHashSet` | Custom open-addressing hash set for longs | All paths |
| `scalarDistinctSet` | `LongOpenHashSet` on `ShardExecuteResponse` | Q04 |
| `distinctSets` | `Map<Long, LongOpenHashSet>` on `ShardExecuteResponse` | Q08, Q09 |
| Open-addressing group map | `long[] grpKeys + LongOpenHashSet[] grpSets + boolean[] grpOcc` | Q08, Q09 shard-side |
| `long[][] grpAccs` | Per-group accumulator array | Q09 only |

## Key Integration Points for DataFusionBridge

The bridge would need to:
1. **Accept** the same inputs: shard reference, lucene query, column names
2. **Return** compatible structures: `LongOpenHashSet` (Q04) or `Map<Long, LongOpenHashSet>` + `long[][]` accumulators (Q08/Q09)
3. **Attach** results via `resp.setScalarDistinctSet()` or `resp.setDistinctSets()` for coordinator merge
4. The coordinator merge logic is **independent** of shard execution — it only reads the attached sets
