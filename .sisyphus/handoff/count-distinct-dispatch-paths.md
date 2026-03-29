# COUNT(DISTINCT) Dispatch Paths in TransportShardExecuteAction.java

## executePlan() Dispatch Chain (line 194)

The `executePlan()` method at line 194 is a massive dispatch chain that tries fast paths in priority order. It first unwraps any top-level `ProjectNode` (lines 201-207), then checks each fast path sequentially. The first match wins.

### Dispatch Order (relevant to COUNT(DISTINCT)):

| Priority | Lines | Pattern | Method |
|----------|-------|---------|--------|
| 1 | 210-215 | Scalar COUNT(*) | `executeScalarCountStar()` |
| 2 | 220-232 | Sorted scan (LIMIT+ORDER BY+TableScan) | `executeSortedScan()` |
| 3 | 235-243 | Scalar agg (no GROUP BY) incl. COUNT(DISTINCT col) | `executeFusedScanAggregate()` via `FusedScanAggregate.canFuse()` |
| 4 | 246-253 | Bare single numeric column scan (PlanFragmenter stripped SINGLE COUNT(DISTINCT)) | `executeDistinctValuesScanWithRawSet()` |
| 5 | 256-261 | Bare single VARCHAR column scan (same, varchar) | `executeDistinctValuesScanVarcharWithRawSet()` |
| 6 | 264-269 | Fused eval-aggregate SUM(col+k) | `executeFusedEvalAggregate()` |
| **7** | **273-357** | **COUNT(DISTINCT) dedup plan: AggregationNode(PARTIAL, ≥2 keys, canFuse)** | **Multiple sub-paths (see below)** |
| 8 | 365-374 | Expression GROUP BY (REGEXP_REPLACE etc.) | `executeFusedExprGroupByAggregate()` |
| 9 | 378-385 | Generic fused GROUP BY | `executeFusedGroupByAggregate()` |
| 10 | 390-590 | Fused GROUP BY + Sort + Limit (with HAVING) | Various sort/limit combos |
| 11 | 595-620 | Limit + AggregationNode (no Sort) | `executeFusedGroupByAggregateWithTopN()` |
| 12 | 623-700 | Sort + Filter + AggregationNode (HAVING, no Limit) | Sort+filter combo |
| LAST | 702-740 | Generic operator pipeline fallback | `LocalExecutionPlanner` |

## COUNT(DISTINCT) Detection Block (lines 273-357)

This is the key block. Entry conditions (line 279-284):
```java
if (scanFactory == null
    && effectivePlan instanceof AggregationNode aggDedupNode
    && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
    && aggDedupNode.getGroupByKeys().size() >= 2
    && FusedGroupByAggregate.canFuse(aggDedupNode, ...))
```

**There is NO explicit `AggDedupNode` class.** The detection is purely structural: it checks for an `AggregationNode` with `Step.PARTIAL`, ≥2 group-by keys, and `canFuse()` compatibility.

### Sub-dispatch within the COUNT(DISTINCT) block:

**A. `isSingleCountStar` (line 285-338):** exactly 1 agg function = `"COUNT(*)"` 
- **2 numeric keys** (line 296-311): → `executeCountDistinctWithHashSets()` at line 960
- **VARCHAR key0 + numeric key1** (line 313-318): → `executeVarcharCountDistinctWithHashSets()` at line 1798
- **3+ all-numeric keys** (line 320-337): → `executeNKeyCountDistinctWithHashSets()` at line 1248

**B. `isMixedDedup` (line 339-356):** multiple SUM/COUNT aggs (no DISTINCT, no AVG), 2 keys
- **2 numeric keys** (line 347-355): → `executeMixedDedupWithHashSets()` at line 1537

## How the 2-Level Calcite Plan is Handled

Calcite decomposes `COUNT(DISTINCT y)` into:
- **Inner**: `GROUP BY (x, y)` → produces unique (x,y) pairs
- **Outer**: `GROUP BY (x), COUNT(*)` → counts distinct y per x

The **PlanFragmenter** splits this into shard vs coordinator plans. The shard receives the **inner** `AggregationNode(PARTIAL, groupBy=[x, y], aggs=["COUNT(*)"])`.

The dispatch at line 273 recognizes this as a "dedup plan" because:
1. It's `PARTIAL` step
2. It has ≥2 group-by keys (x and y)
3. The aggregate is `COUNT(*)`

Instead of materializing all (x,y) pairs, the fused paths GROUP BY the first N-1 keys and collect the last key into a `LongOpenHashSet` per group. The coordinator then unions the sets across shards.

## Key Method Signatures

```java
// Line 960 — 2 numeric keys, COUNT(*) only
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)

// Line 1248 — 3+ numeric keys, COUNT(*) only  
private ShardExecuteResponse executeNKeyCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    List<String> keyNames, Type[] keyTypes)

// Line 1537 — 2 numeric keys, mixed SUM/COUNT aggs
private ShardExecuteResponse executeMixedDedupWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)

// Line 1798 — VARCHAR key0 + numeric key1, COUNT(*) only
private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String varcharKeyName, String numericKeyName, Type numericKeyType)
```

## Gaps / Where Fusion Could Be Intercepted

1. **No VARCHAR+VARCHAR key combo** — falls through to generic pipeline
2. **No 3+ key path with VARCHAR keys** — falls through to generic pipeline
3. **No mixed dedup with >2 keys** — falls through to generic pipeline
4. **The detection relies on `FusedGroupByAggregate.canFuse()`** — this is the gatekeeper for whether the fused path fires at all
5. **Interception point for new fusion**: Lines 273-284 (the entry condition) or add a new block between priorities 6 and 7
