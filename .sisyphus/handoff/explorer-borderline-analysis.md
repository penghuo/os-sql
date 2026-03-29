# Borderline DQE Query Execution Path Analysis

## Query Definitions
| Query | SQL | Ratio | Time |
|-------|-----|-------|------|
| Q03 | `SELECT AVG(UserID) FROM hits` | 2.65x (0.294s vs 0.111s) | Scalar aggregate |
| Q14 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) ... WHERE SearchPhrase <> '' GROUP BY ... ORDER BY c DESC LIMIT 10` | 2.16x (1.589s vs 0.735s) | 2-key varchar GROUP BY |
| Q29 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth+1), ..., SUM(ResolutionWidth+89) FROM hits` | 2.14x (0.205s vs 0.096s) | 90 SUM(col+N) |
| Q36 | `SELECT URL, COUNT(*) ... WHERE CounterID=62 AND ... GROUP BY URL ORDER BY PageViews DESC LIMIT 10` | 2.38x (0.288s vs 0.121s) | Filtered single-varchar GROUP BY |

---

## Q03: AVG(UserID) — Scalar Aggregate

### Dispatch Path
1. `TransportShardExecuteAction.executePlan()` (line ~194)
2. Unwraps ProjectNode → AggregationNode
3. `FusedScanAggregate.canFuse(aggNode)` → **true** (no GROUP BY, child is TableScanNode)
4. `executeFusedScanAggregate()` (line ~930) → `FusedScanAggregate.execute()` (line ~473)

### Execution Path in FusedScanAggregate.execute()
1. Parses `AVG(UserID)` → AggSpec(AVG, false, "UserID", BigintType)
2. Creates `AvgDirectAccumulator`
3. **MatchAllDocsQuery** → tries `tryFlatArrayPath()` (line ~620)
4. `tryFlatArrayPath()` (line ~830): AVG on BigintType → eligible (no DISTINCT, no double, no varchar)
5. Checks no deleted docs → **true**
6. Collects unique columns: `["UserID"]` (1 column)
7. **Parallel path**: `numWorkers = max(1, availableProcessors / dqe.numLocalShards)` = `max(1, 16/4)` = **4 workers**
8. Each worker: column-major `nextDoc()` iteration, accumulates `[sum, count]` per column
9. Merge worker results → compute AVG = sum/count

### Micro-Optimization Opportunities
- **Already well-optimized**: parallel, column-major, zero-alloc inner loop
- **Potential**: On r5.4xlarge (16 vCPU, 4 shards), each shard gets 4 workers. With 4 shards running concurrently, that's 16 threads total — full CPU utilization
- **Minor**: The `tryFlatArrayPath` does column-major iteration (one column at a time per segment). For Q03 with only 1 column, this is optimal. No improvement possible here
- **Bottleneck is likely coordination overhead**: 0.294s for a simple AVG suggests the 0.183s gap vs CH is mostly coordinator + network + plan compilation overhead, not shard execution

---

## Q14: SearchEngineID, SearchPhrase GROUP BY with WHERE filter

### Dispatch Path
1. `TransportShardExecuteAction.executePlan()` (line ~194)
2. Plan is `LimitNode → SortNode → AggregationNode → TableScanNode` (with filter in DSL)
3. `extractAggFromSortedLimit(plan)` extracts the inner AggregationNode (line ~420)
4. `FusedGroupByAggregate.canFuse(innerAgg, colTypeMap)` → **true**
5. Resolves sort column, topN = 10
6. Calls `executeFusedGroupByAggregateWithTopN()` (line ~2702) → `FusedGroupByAggregate.executeWithTopN()` (line ~1248)
7. `executeWithTopN()` → `executeInternal()` (line ~1279) with topN=10

### Execution Path in FusedGroupByAggregate.executeInternal()
1. Classifies keys: SearchEngineID=numeric, SearchPhrase=VARCHAR → `hasVarchar=true`
2. `keyInfos.size()==2` and one is varchar → **NOT** single varchar path
3. No eval keys → falls through to `executeWithVarcharKeys()` (line ~8207)

### Inside executeWithVarcharKeys()
1. **Single-segment check**: 4 shards → likely multi-segment → `singleSegment=false`
2. Falls through to `executeNKeyVarcharPath()` (line ~9283)
3. Uses `HashMap<MergedGroupKey, AccumulatorGroup>` for cross-segment merge
4. Per-segment: Lucene Collector with `advanceExact()` per doc for both keys
5. **Filtered query** (SearchPhrase <> '') → uses Lucene search framework, not direct iteration
6. **No parallelism** in this path — single-threaded sequential scan

### Micro-Optimization Opportunities
- **No parallelism**: `executeNKeyVarcharPath` is fully sequential. Adding segment-level parallelism could help
- **HashMap overhead**: `MergedGroupKey` allocates per-group. For Q14 with filter (SearchPhrase <> ''), cardinality is moderate (~18K groups)
- **advanceExact per doc**: For the filtered path, each matching doc requires `advanceExact()` on both key DocValues + aggregate DocValues. This is inherent to the Lucene model
- **Top-N not pushed down**: topN=10 is passed but `executeWithVarcharKeys` → `executeNKeyVarcharPath` may not use it for early termination during aggregation (only applied post-aggregation)
- **Key opportunity**: If single-segment, the `tryOrdinalIndexedTwoKeyCountStar` path would fire but requires ALL COUNT(*) — Q14 has only COUNT(*) so it should match. But it also requires `allCountStar && numAggs == 1` AND one varchar + one numeric key. Q14 matches this! But only for single-segment. Multi-segment falls through to the slower N-key path

---

## Q29: SUM(ResolutionWidth+N) for N=0..89 — 90 Scalar Aggregates

### Dispatch Path
1. `TransportShardExecuteAction.executePlan()` (line ~194)
2. Unwraps ProjectNode → AggregationNode
3. `FusedScanAggregate.canFuse(aggNode)` → **false** (child is EvalNode, not TableScanNode)
4. `FusedScanAggregate.canFuseWithEval(aggNode)` → **true** (all SUM, all `col+N` patterns)
5. `executeFusedEvalAggregate()` (line ~952) → `FusedScanAggregate.executeWithEval()` (line ~154)

### Execution Path in FusedScanAggregate.executeWithEval()
1. Parses 90 eval expressions: `ResolutionWidth+0` through `ResolutionWidth+89`
2. All map to same physical column `ResolutionWidth` with offsets 0..89
3. `uniquePhysicalColumns = {"ResolutionWidth"}` — **only 1 column to read**
4. Uses algebraic identity: `SUM(col+k) = SUM(col) + k * COUNT(*)`
5. **MatchAllDocsQuery** + no deletes → **parallel path**
6. `numWorkers = max(1, availableProcessors / dqe.numLocalShards)` = **4 workers**
7. Each worker: column-major `nextDoc()` iteration on `ResolutionWidth`, accumulates `[sum, count]`
8. Merge workers → compute each of 90 results as `sum + offset[i] * count`

### Micro-Optimization Opportunities
- **Already very well-optimized**: reads column only once, algebraic shortcut eliminates 89 redundant scans
- **Output construction**: 90 BlockBuilder allocations for the result Page. Minor but could use a single long[] output
- **The 0.205s includes**: plan compilation (parsing 90 SUM expressions), EvalNode pattern matching, shard coordination
- **Potential**: The `nameToPhysical`/`nameToOffset` maps use LinkedHashMap — could use arrays since indices are known. But this is setup cost, not hot loop
- **Main bottleneck**: At 0.205s with algebraic shortcut reading only 1 column, the gap vs CH (0.096s) is likely dominated by JVM overhead, plan compilation for 90 expressions, and coordinator merge

---

## Q36: URL GROUP BY with CounterID=62 filter

### Dispatch Path
1. `TransportShardExecuteAction.executePlan()` (line ~194)
2. Plan is `LimitNode → SortNode → AggregationNode → TableScanNode` (with filter in DSL)
3. `extractAggFromSortedLimit(plan)` extracts inner AggregationNode
4. `FusedGroupByAggregate.canFuse(innerAgg, colTypeMap)` → **true**
5. Resolves sort column (COUNT(*)), topN = 10
6. `executeFusedGroupByAggregateWithTopN()` → `FusedGroupByAggregate.executeWithTopN()` → `executeInternal()`

### Execution Path in FusedGroupByAggregate.executeInternal()
1. Classifies keys: URL=VARCHAR → `hasVarchar=true`
2. `keyInfos.size()==1 && isVarchar && specs.size()==1 && COUNT(*) && *` → **matches**
3. Dispatches to `executeSingleVarcharCountStar()` (line ~1615)

### Inside executeSingleVarcharCountStar()
1. **Multi-segment** (4 shards, likely multiple segments per shard)
2. **Single-segment check fails** → falls to multi-segment path
3. URL has very high cardinality (millions of unique values)
4. **Global ordinals path**: skipped if ordCount > 1M (URL has millions)
5. **Parallel multi-segment**: skipped if ordCount > 500K
6. **Sequential multi-segment fallback**: `HashMap<BytesRefKey, long[]>` for cross-segment merge
7. Per-segment: `long[] ordCounts` if ordCount ≤ 1M, else `HashMap<Long, long[]>`
8. In `finish()`: resolves every non-zero ordinal to `BytesRefKey` (copies UTF-8 bytes) → merges into global HashMap
9. **Completely sequential** — single thread

### Micro-Optimization Opportunities
- **No parallelism**: The biggest issue. URL's high cardinality (millions) disables all parallel paths
- **BytesRefKey allocation**: In `finish()`, every non-zero ordinal triggers a `BytesRefKey` allocation with UTF-8 byte copy. For filtered Q36, matching docs are few (~thousands) but ordinal resolution still happens
- **HashMap overhead**: `HashMap<BytesRefKey, long[]>` has per-entry overhead (Entry object, hash computation, BytesRefKey equals/hashCode on byte arrays)
- **Top-N not exploited during scan**: topN=10 is only applied after full aggregation. For a filtered query with few matching docs, this matters less
- **Key opportunity**: Since Q36 is filtered (CounterID=62 + date range), the matching doc count is small. A per-segment ordinal array + merge could work even for high-cardinality URL if we only resolve ordinals that actually appear in matching docs (which is a small subset)
- **Potential parallel approach**: Even with high cardinality, if each worker uses per-segment ordinal arrays and only the merge phase resolves strings, parallelism could help

---

## Summary of Optimization Opportunities

| Query | Current Path | Bottleneck | Potential Fix | Est. Impact |
|-------|-------------|-----------|---------------|-------------|
| Q03 | FusedScanAggregate.tryFlatArrayPath (parallel) | Coordinator/plan overhead | Reduce plan compilation cost | Low (~10-20%) |
| Q14 | executeNKeyVarcharPath (sequential) | No parallelism, HashMap overhead | Add segment-level parallelism for multi-key varchar | Medium (~30-40%) |
| Q29 | FusedScanAggregate.executeWithEval (parallel) | Plan compilation for 90 exprs | Cache compiled plans, reduce BlockBuilder allocs | Low (~10-20%) |
| Q36 | executeSingleVarcharCountStar sequential fallback | No parallelism, BytesRefKey alloc | Parallel per-segment ordinal scan + lazy string resolution | High (~40-50%) |
