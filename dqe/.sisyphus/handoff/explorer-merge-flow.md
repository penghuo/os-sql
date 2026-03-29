# GROUP BY + ORDER BY + LIMIT: Shard→Coordinator Data Flow

## Key Files
| File | Purpose |
|------|---------|
| `shard/transport/TransportShardExecuteAction.java` | Shard-side plan execution, pattern matching, top-N |
| `shard/transport/ShardExecuteResponse.java` | Response container: `List<Page>` + `List<Type>` + transient HashSet attachments |
| `shard/source/FusedGroupByAggregate.java` | DocValues-based GROUP BY with optional top-N selection |
| `coordinator/transport/TransportTrinoSqlAction.java` | Coordinator: dispatch, merge, format |
| `coordinator/merge/ResultMerger.java` (3297 lines) | All merge strategies: passthrough, aggregation, sorted, capped |

## Data Flow: GROUP BY + ORDER BY + LIMIT

### Phase 1: Shard Execution (`TransportShardExecuteAction.executePlan`)

The shard detects plan patterns via cascading `if` checks (lines 194-744):

1. **Pattern: `LimitNode → [ProjectNode] → SortNode → AggregationNode`** (line 390-593)
   - Extracts `topN = limitNode.getCount() + limitNode.getOffset()`
   - **WITH HAVING**: Aggregates ALL groups first, applies HAVING filter, THEN sort+limit via SortOperator (line 422-482)
   - **WITHOUT HAVING, single sort key on agg column**: Calls `FusedGroupByAggregate.executeWithTopN()` — top-N selection happens INSIDE the hash aggregation on flat accumulator data, avoiding full Page construction for non-top groups (line 487-523)
   - **WITHOUT HAVING, multi-key sort or fallback**: Aggregates all groups via `executeFusedGroupByAggregate()`, then applies SortOperator with topN limit (line 532-593)

2. **Pattern: `LimitNode → AggregationNode` (no Sort)** (line 598-618)
   - Calls `executeFusedGroupByAggregateWithTopN(innerAgg, req, -1, false, topN)` with sortAggIndex=-1
   - Returns only first N groups (arbitrary order)

3. **Generic fallback** (line 725-744)
   - Builds full operator pipeline: `LucenePageSource → HashAggregationOperator → SortOperator → ...`
   - Drains ALL pages — **no shard-side top-N truncation**

### Phase 2: Serialization (`ShardExecuteResponse.writeTo`, line 84)
- `PageSerializer.writePages(out, pages, columnTypes)` — serializes ALL pages from shard
- Transient HashSet attachments (for COUNT DISTINCT) are NOT serialized (local-only optimization)

### Phase 3: Coordinator Merge (`TransportTrinoSqlAction`, lines 314-484)

The coordinator receives `List<List<Page>> shardPages` and selects merge strategy:

**For GROUP BY aggregation (FINAL step):**
1. **Fused merge+sort** (`merger.mergeAggregationAndSort`, line 413): When no HAVING, has SortNode, has LIMIT, and agg step is FINAL. Merges partial aggregates + sorts + limits in one pass. Uses fast numeric/varchar/mixed paths internally.
2. **Capped merge** (`merger.mergeAggregationCapped`, line 434): LIMIT without ORDER BY. Stops after N groups.
3. **Fallback** (line 437-444): `mergeAggregation` → `applyCoordinatorHaving` → `applyCoordinatorSort`

**For non-aggregate (scan) queries:**
1. **Sorted merge** (`merger.mergeSorted`, line 464): ORDER BY + LIMIT on raw scan. Uses bounded max-heap for small limits.
2. **Passthrough** (`merger.mergePassthrough`, line 473): Simple concatenation.

**Final steps** (lines 480-484):
- `applyGlobalOffset(mergedPages, globalOffset)` — skip first N rows
- `applyGlobalLimit(mergedPages, globalLimit)` — trim to LIMIT

## Existing Shard-Side Top-N

**Already exists** in two specific patterns:

| Pattern | Location | Mechanism |
|---------|----------|-----------|
| `LimitNode → SortNode → AggregationNode` (no HAVING, single sort key) | Line 487-523 | `FusedGroupByAggregate.executeWithTopN()` — top-N on flat accumulators |
| `LimitNode → AggregationNode` (no Sort) | Line 598-618 | `executeWithTopN(aggNode, req, -1, false, topN)` — first N groups |
| `LimitNode → SortNode → AggregationNode` (HAVING or multi-sort) | Line 422-593 | Aggregates ALL groups, then SortOperator with topN |
| Generic pipeline fallback | Line 725-744 | **NO top-N** — drains all pages |

## Where Shard-Side Top-N Could Be Inserted

### Gap 1: Generic Pipeline Fallback (line 725-744)
When the plan doesn't match any fused pattern, the generic `LocalExecutionPlanner` builds a full operator pipeline and drains ALL pages. A `TopNOperator` could be inserted after the pipeline to truncate results before shipping.

### Gap 2: HAVING + Sort Path (line 422-482)
Currently aggregates ALL groups, applies HAVING, then sort+limit. The sort+limit is already applied shard-side, but ALL groups must be aggregated first due to HAVING. No optimization possible here without semantic analysis of the HAVING predicate.

### Gap 3: Multi-Sort Key Fallback (line 532-593)
When sort has multiple keys or the primary sort key doesn't map to a single agg column, it falls back to aggregating ALL groups then sorting. The sort+limit IS applied shard-side via SortOperator, but the full aggregation still happens. The `executeFusedGroupByAggregateWithTopN` is only used when the primary sort key maps to a single agg index.

### Gap 4: Coordinator Receives ALL Shard Pages
Even when shards apply top-N, the coordinator's `mergeAggregationAndSort` re-merges partial aggregates from ALL shards. For GROUP BY queries, each shard sends its local top-N groups, but the coordinator must still merge overlapping groups across shards. This is correct behavior — shard top-N is an approximation that reduces data volume but the coordinator must reconcile.

### Key Insight: The Biggest Win
The **generic pipeline fallback** (Gap 1) is the most impactful insertion point. When `FusedGroupByAggregate.canFuse()` returns false (unsupported column types, expressions, etc.), the shard ships ALL rows/groups. Adding a shard-side `TopNOperator` after the pipeline would reduce data shipped without affecting correctness for ORDER BY + LIMIT queries (the coordinator re-sorts anyway).
