# COUNT(DISTINCT) Dispatch Logic in TransportShardExecuteAction

## Two-Level Plan Detection

Calcite decomposes `SELECT x, COUNT(DISTINCT y) FROM t GROUP BY x` into a two-level aggregation:
- **Inner**: `GROUP BY (x, y)` → produces all unique (x,y) pairs (PARTIAL step)
- **Outer**: `GROUP BY x, COUNT(*)` → counts distinct y per x

The shard receives the **inner** plan: an `AggregationNode` with `Step.PARTIAL`, `groupByKeys=[x,y]` (size≥2), and `aggregateFunctions=["COUNT(*)"]`. This is the "dedup plan" pattern.

**Detection** (line ~279 in TransportShardExecuteAction.java):
```java
effectivePlan instanceof AggregationNode aggDedupNode
    && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
    && aggDedupNode.getGroupByKeys().size() >= 2
    && FusedGroupByAggregate.canFuse(aggDedupNode, columnTypeMap)
```

## Dispatch Table (4 fused methods)

| Method | Line | Pattern | Condition |
|--------|------|---------|-----------|
| `executeCountDistinctWithHashSets` | 960 | 2 numeric keys, COUNT(*) only | `numKeys==2`, both non-VARCHAR, `isSingleCountStar` |
| `executeVarcharCountDistinctWithHashSets` | 2016 | VARCHAR key0 + numeric key1, COUNT(*) | key0=VARCHAR, key1=numeric, `isSingleCountStar` |
| `executeNKeyCountDistinctWithHashSets` | 1248 | 3+ numeric keys, COUNT(*) | `numKeys>=3`, all non-VARCHAR, `isSingleCountStar` |
| `executeMixedDedupWithHashSets` | 1537 | 2 numeric keys, mixed SUM/COUNT | `numKeys==2`, both non-VARCHAR, `isMixedDedup` |

## Fused Path (what these methods do)

Instead of materializing all (x,y) pairs (e.g., ~10K rows), the fused path:
1. Does `GROUP BY key0` only
2. Attaches a `LongOpenHashSet` per group to collect `key1` values
3. Outputs compact pages (~450 rows) + attached HashSets
4. Coordinator unions HashSets across shards, then counts

## Unfused Path

If none of the fused conditions match, falls through to the **generic operator pipeline**:
`LucenePageSource → HashAggregationOperator` (line 201 comment). This materializes all (x,y) pairs as full Pages, doubling key space.

## Pre-Dedup Bare Scan Paths (scalar COUNT(DISTINCT))

Before the GROUP BY dedup block, two earlier fast paths handle scalar `COUNT(DISTINCT col)`:
- **Numeric** (line 251): `isBareSingleNumericColumnScan` → `executeDistinctValuesScanWithRawSet` — collects into raw LongOpenHashSet
- **VARCHAR** (line 259): `isBareSingleVarcharColumnScan` → `executeDistinctValuesScanVarcharWithRawSet` — ordinal-based FixedBitSet dedup

## executeSingleKeyNumericFlat (FusedGroupByAggregate.java:4840)

This is the **non-dedup** fused GROUP BY path for single-key numeric aggregation:
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String,Type> columnTypeMap, List<String> groupByKeys, int numAggs,
    boolean[] isCountStar, int[] accType, int sortAggIndex, boolean sortAscending,
    long topN, int bucket, int numBuckets)
```
**Dispatch condition** (line ~3258): `canUseFlatAccumulators` is true (single numeric key, all aggs are COUNT/SUM/MIN/MAX). Uses `FlatSingleKeyMap` with `long[]` arrays per slot — zero per-group object allocation. Supports hash-partitioned multi-bucket execution when groups exceed `MAX_CAPACITY`.

## executeWithTopN (FusedGroupByAggregate.java:908)

```java
public static List<Page> executeWithTopN(
    AggregationNode aggNode, IndexShard shard, Query query,
    Map<String,Type> columnTypeMap, int sortAggIndex, boolean sortAscending, long topN)
```
Called from TransportShardExecuteAction line 2392 for `SortNode → AggregationNode` patterns. Delegates to `executeInternal()` which routes to `executeSingleKeyNumericFlat` when applicable.

## Key Files

- `TransportShardExecuteAction.java` — dispatch hub, lines 244-370 for COUNT(DISTINCT) routing
- `FusedGroupByAggregate.java` — `canFuse()` (line 198), `execute()`/`executeWithTopN()` (lines 908/935), `executeSingleKeyNumericFlat` (line 4840)
