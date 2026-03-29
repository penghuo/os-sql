# COUNT(DISTINCT) Execution Path Analysis

## 1. Dispatch Logic in TransportShardExecuteAction.executePlan (~line 194)

The `executePlan` method first unwraps any top-level `ProjectNode` to expose the underlying `AggregationNode`. The COUNT(DISTINCT) fast path triggers at **line 279** when:

```
effectivePlan instanceof AggregationNode aggDedupNode
  && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
  && aggDedupNode.getGroupByKeys().size() >= 2
  && FusedGroupByAggregate.canFuse(aggDedupNode, columnTypeMap)
```

### Detection Logic (lines 280-340)
Two sub-patterns are detected:

**isSingleCountStar** (pure COUNT(DISTINCT) / "Q9"):
- `aggDedupNode.getAggregateFunctions().size() == 1 && "COUNT(*)".equals(...)`
- The Calcite plan has already decomposed `COUNT(DISTINCT y)` into a two-level plan where the shard sees `GROUP BY (x, y), COUNT(*)` — so the shard plan has 2+ group-by keys and a single COUNT(*).

**isMixedDedup** ("Q10"):
- Not single COUNT(*), but all aggregates match `(sum|count)\(.*\)` — decomposable aggregates.

### Dispatch by key count and type:
| Keys | Types | Method |
|------|-------|--------|
| 2 keys, both numeric | `executeCountDistinctWithHashSets` (line 309) |
| 2 keys, VARCHAR + numeric | `executeVarcharCountDistinctWithHashSets` (line 317) |
| 3+ keys, all numeric | `executeNKeyCountDistinctWithHashSets` (line 335) |
| 2 keys, mixed dedup | `executeMixedDedupWithHashSets` (line 354) |

## 2. executeCountDistinctWithHashSets (line 960)

**Signature:**
```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)
```

**What it does:**
- Instead of GROUP BY (key0, key1) producing ~10K unique pairs per shard, does GROUP BY (key0) with a `LongOpenHashSet` per group to collect key1 values.
- Outputs a compact page (~450 rows) + attached HashSets.

**Parallel segment scanning:**
- Acquires a Lucene searcher, gets leaf reader contexts (segments).
- Single segment → direct `scanSegmentForCountDistinct()` call.
- Multi-segment → dispatches N-1 segments to `FusedGroupByAggregate.getParallelPool()` (ForkJoinPool), runs last segment on current thread.
- Each segment builds its own `Map<Long, LongOpenHashSet>`.
- After all segments complete, merges by unioning HashSets per key0 (smaller set merged into larger).

**Ungrouped vs Grouped:**
- This method is always grouped (2-key). The "ungrouped" scalar COUNT(DISTINCT) takes a different path — it's detected by `isScalarCountDistinctLong` / `isScalarCountDistinctVarchar` in the coordinator and uses `mergeCountDistinctValuesViaRawSets`.

### Variant methods:
- `executeVarcharCountDistinctWithHashSets` (line 2016): VARCHAR key0 + numeric key1. Uses `Map<String, LongOpenHashSet>` keyed by string ordinals.
- `executeNKeyCountDistinctWithHashSets` (line 1248): 3+ keys. Groups by first N-1 keys with HashSet for last key (dedup key). Outputs full dedup tuples for coordinator's generic merge.

## 3. Two-Level Calcite Plan for GROUP BY x, COUNT(DISTINCT y)

Calcite decomposes `SELECT x, COUNT(DISTINCT y) FROM t GROUP BY x` into:

```
Outer: Aggregate(GROUP BY x, COUNT(*))
Inner: Aggregate(GROUP BY x, y)        ← this becomes the shard plan
```

The shard sees `GROUP BY (x, y), COUNT(*)` — a PARTIAL aggregation with 2+ group-by keys and a single COUNT(*). This is exactly what the `AggDedupNode` detection checks for.

The coordinator sees a SINGLE-step `AggregationNode` with `GROUP BY x, COUNT(DISTINCT y)`. The `isShardDedupCountDistinct()` method (line 2155) validates this pattern:
- Shard plan is PARTIAL AggregationNode
- Shard has more group-by keys than coordinator (original keys + distinct columns)
- Shard aggregates exactly COUNT(*)

## 4. Coordinator Merge Logic in TransportTrinoSqlAction

### Dispatch (lines 620-680):
The coordinator checks the plan type and dispatches:

| Condition | Method |
|-----------|--------|
| Scalar COUNT(DISTINCT) long | `mergeCountDistinctValuesViaRawSets` (line 629) |
| Scalar COUNT(DISTINCT) varchar | `mergeCountDistinctVarcharViaRawSets` (line 633) |
| Grouped + VARCHAR HashSet attachments | `mergeDedupCountDistinctViaVarcharSets` (line 660) |
| Grouped + numeric HashSet attachments | `mergeDedupCountDistinctViaSets` (line 674) |
| Grouped fallback (no attachments) | `mergeDedupCountDistinct` (line 680) |

### mergeCountDistinctValuesViaRawSets (line 1989):
```java
private static List<Page> mergeCountDistinctValuesViaRawSets(
    ShardExecuteResponse[] shardResults, List<List<Page>> shardPages)
```
1. Checks if all shards have `getScalarDistinctSet()` — falls back to `mergeCountDistinctValues()` if not.
2. Finds the largest `LongOpenHashSet` across shards.
3. Merges all non-largest sets into a temporary set.
4. **Parallel counting**: splits the temporary set's keys into chunks across `min(availableProcessors, 8)` workers. Each worker counts entries NOT present in the largest set via read-only `contains()`.
5. Handles zero/sentinel values specially.
6. Returns `total = largest.size() + extraCount`.

### mergeCountDistinctVarcharViaRawSets (line 2078):
Same pattern but for `getScalarDistinctStrings()` — string-based HashSet union.

### mergeDedupCountDistinct (line ~2190):
Two-stage merge for grouped COUNT(DISTINCT):
1. **Stage 1**: FINAL merge for dedup keys (removes cross-shard duplicates) — O(n) hash merge.
2. **Stage 2**: GROUP BY original keys with COUNT(*) to get COUNT(DISTINCT).
This avoids expensive per-row CountDistinctAccumulator (HashSet per group).
