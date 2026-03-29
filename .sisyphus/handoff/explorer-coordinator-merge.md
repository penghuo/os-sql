# Coordinator Merge Path for COUNT(DISTINCT) with GROUP BY

## File
`dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java`

---

## 1. Detection: `isShardDedupCountDistinct` (L2077)

```java
private static boolean isShardDedupCountDistinct(
    DqePlanNode shardPlan, AggregationNode singleAgg, Map<String,Type> columnTypeMap)
```

Returns true when:
- shardPlan is `AggregationNode` with `Step.PARTIAL`
- singleAgg has non-empty GROUP BY keys
- shard has **more** group-by keys than original (original keys + distinct columns)
- shard has exactly 1 agg function: `COUNT(*)`

For Q13: shard does `GROUP BY (SearchPhrase, UserID) COUNT(*)` → 2 dedup keys > 1 original key ✓

---

## 2. Dispatch Logic (L633–690) — Three Fast Paths

The coordinator checks shard responses in priority order:

### Path A: `mergeDedupCountDistinctViaVarcharSets` (L657) — **PREFERRED for Q13**
- **Condition**: All shards have `getVarcharDistinctSets() != null` AND single GROUP BY key
- Shards attach `Map<String, LongOpenHashSet>` (SearchPhrase → set of UserIDs) as side-channel
- **Top-N hint**: `findGlobalLimit(optimizedPlan)` + `findGlobalOffset(optimizedPlan)`
- Output: 2-column Page `(VARCHAR key, BIGINT count)`

### Path B: `mergeDedupCountDistinctViaSets` (L671)
- **Condition**: All shards have `getDistinctSets() != null` (Long-keyed) AND single GROUP BY key
- For numeric GROUP BY keys (e.g., RegionID)

### Path C: `mergeDedupCountDistinct` (L681) — **FALLBACK**
- Page-based two-stage merge (no side-channel sets)
- Used when shard sets are unavailable (e.g., remote/transient shards)

---

## 3. `mergeDedupCountDistinctViaVarcharSets` (L2545) — VARCHAR Key Path

```java
private static List<Page> mergeDedupCountDistinctViaVarcharSets(
    ShardExecuteResponse[] shardResults, AggregationNode singleAgg,
    List<Type> columnTypes, Map<String,Type> columnTypeMap, long topN)
```

### Phase 1: Collect per-group per-shard sets
- Builds `Map<String, LongOpenHashSet[]>` — each group key maps to array of per-shard sets
- Data comes from `shardResults[s].getVarcharDistinctSets()` (side-channel, NOT from Pages)

### Phase 2: Top-K Pruning (when `topN > 0 && topN * 10 < totalGroups`)
- For each group: `upper_bound = sum(shard_sizes)`, `lower_bound = max(shard_sizes)`
- Builds min-heap of top-K lower bounds → threshold
- Prunes groups where `upper_bound < threshold`
- **For Q13 with ORDER BY ... LIMIT 10**: only ~100 candidate groups merged instead of all ~700K

### Phase 3: Merge candidate groups
- **Zero-copy ownership**: takes the largest shard's `LongOpenHashSet` as merge target
- Unions remaining shards' sets into target (iterates raw `long[]` keys array)
- Result: `Map<String, LongOpenHashSet>` with merged distinct UserIDs per SearchPhrase

### Phase 4: Build output
- If `topN < mergedGroupCount`: sorts entries by `count DESC`, takes top-K
- Builds 2-column Page: `(SearchPhrase VARCHAR, COUNT(DISTINCT UserID) BIGINT)`

---

## 4. `mergeDedupCountDistinctVarcharKey` (L4182) — Page-Based VARCHAR Path

```java
private static List<Page> mergeDedupCountDistinctVarcharKey(
    List<List<Page>> shardPages, List<Type> dedupTypes, int numCountDistinctAggs)
```

Called from the fallback `mergeDedupCountDistinct` (L2109) when:
- `numOriginalKeys == 1`, `numDedupKeys == 2`
- First dedup key is VARCHAR, second is numeric

### Stage 1: Global dedup via `SliceLongDedupMap`
- Open-addressing hash set for `(Slice-range, long)` compound keys
- XxHash64 on VARCHAR bytes combined with long value
- Zero-copy: references original block's raw Slice
- Iterates all shard Pages, inserts `(SearchPhrase, UserID)` pairs

### Stage 2: Count per group via `SliceCountMap`
- Calls `dedupMap.countPerGroup(groupCounts::increment)`
- `SliceCountMap`: open-addressing `VARCHAR → long count` map
- Builds output Page: `[SearchPhrase VARCHAR, COUNT(DISTINCT) BIGINT]`

---

## 5. Key Data Structures

| Structure | File | Purpose |
|-----------|------|---------|
| `LongOpenHashSet` | `dqe/.../operator/LongOpenHashSet.java:20` | Open-addressing long hash set for distinct numeric values per group |
| `SliceLongDedupMap` | `dqe/.../operator/SliceLongDedupMap.java:24` | (Slice+long) compound key dedup set, XxHash64, 65% load factor |
| `SliceCountMap` | `dqe/.../operator/SliceCountMap.java:18` | VARCHAR→count map for group counting, 65% load factor |

---

## 6. Data Transfer: Shards → Coordinator

**Two channels per shard:**
1. **Pages** (serialized): `(SearchPhrase, UserID, COUNT(*))` tuples — ~3.3M rows/shard
2. **Side-channel** (transient, same-JVM only): `Map<String, LongOpenHashSet>` via `getVarcharDistinctSets()`

When side-channel is available (Path A), Pages are **not used** for the merge — the `LongOpenHashSet` maps are used directly. This avoids deserializing 13.2M rows.

When side-channel is unavailable (Path C → `mergeDedupCountDistinctVarcharKey`), all 13.2M rows flow through Page deserialization → `SliceLongDedupMap`.

---

## 7. Top-N Optimization Summary

| Path | Top-N Support | Mechanism |
|------|--------------|-----------|
| VarcharSets (Path A) | ✅ Yes | Upper/lower bound pruning + partial sort |
| LongSets (Path B) | ✅ Yes | Min-heap upper bound pruning |
| Page-based VARCHAR (L4182) | ❌ No | Must dedup all rows, then count all groups |
| Page-based fallback | ❌ No | Full two-stage merge |

---

## Q13 Specific Analysis

For `GROUP BY SearchPhrase COUNT(DISTINCT UserID) ORDER BY cnt DESC LIMIT 10`:

- **Preferred path**: `mergeDedupCountDistinctViaVarcharSets` (Path A, L2545)
- **Condition**: shards must be local (same JVM) to have `varcharDistinctSets` attached
- **Top-N hint**: `topN = 10 + offset` → prunes to ~100 candidate groups from ~700K
- **Merge cost**: union of `LongOpenHashSet` for ~100 groups × 4 shards (zero-copy largest set)
- **Fallback** (remote shards): `mergeDedupCountDistinctVarcharKey` (L4182) — processes all 13.2M rows through `SliceLongDedupMap`, no top-N pruning
