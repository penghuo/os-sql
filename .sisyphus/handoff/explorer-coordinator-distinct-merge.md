# Coordinator COUNT(DISTINCT) HashSet Merge Logic

## Overview

`TransportTrinoSqlAction.java` has **two execution paths** (local vs remote) with different HashSet merge strategies. The `transient` fields on `ShardExecuteResponse` carry raw sets only for local execution.

## ShardExecuteResponse Transient Attachments (line 39-65)

| Field | Type | Purpose |
|---|---|---|
| `distinctSets` | `Map<Long, LongOpenHashSet>` | Per-group sets for numeric GROUP BY key |
| `varcharDistinctSets` | `Map<String, LongOpenHashSet>` | Per-group sets for VARCHAR GROUP BY key |
| `scalarDistinctSet` | `LongOpenHashSet` | Scalar COUNT(DISTINCT numericCol) |
| `scalarDistinctStrings` | `Set<String>` | Scalar COUNT(DISTINCT varcharCol) |

All are `transient` — only available on the **local-node fast path** (not serialized over transport).

## SINGLE Step Aggregation Path

When `coordinatorPlan` is `AggregationNode(Step.SINGLE)`, the coordinator must aggregate raw/deduped data itself (no PARTIAL/FINAL split). The dispatch logic in `executeInternal()` checks multiple conditions in order:

### 1. Scalar COUNT(DISTINCT long) — local path (~line 480)
```java
mergedPages = mergeCountDistinctValuesViaRawSets(shardResults, shardPages);
```
Picks the **largest** shard's `LongOpenHashSet`, iterates all other sets' raw `keys()` arrays, and calls `largest.add(v)` for each entry. Returns `Page([largest.size()])`.

### 2. Scalar COUNT(DISTINCT varchar) — local path (~line 485)
```java
mergedPages = mergeCountDistinctVarcharViaRawSets(shardResults, shardPages);
```
Same pattern: picks largest `Set<String>`, calls `largest.addAll(other)` for remaining shards.

### 3. Grouped COUNT(DISTINCT) with HashSets — local path (~line 510-540)
Checks `allHaveSets` / `allHaveVarcharSets` on shard responses:

- **VARCHAR key** (`mergeDedupCountDistinctViaVarcharSets`, ~line 520): Collects `Map<String, LongOpenHashSet[]>` per group across shards. Uses upper/lower-bound pruning when `topN` is set. For each candidate group, takes ownership of the largest shard's set and unions others into it. Output: `Page([groupKey VARCHAR, count BIGINT])`.

- **Numeric key** (`mergeDedupCountDistinctViaSets`, ~line 530): Same pattern with `Map<Long, List<LongOpenHashSet>>`. Picks largest set per group, merges others via raw `keys()` iteration. Supports lazy top-K pruning (only merges ~10% of groups for ORDER BY LIMIT queries).

### 4. Remote/fallback path — no HashSets available
Falls through to `mergeDedupCountDistinct()` which does a two-stage Page-based merge:
1. **Stage 1**: `ResultMerger.mergeAggregation()` with FINAL dedup node (removes cross-shard duplicates by summing partial counts on expanded GROUP BY keys)
2. **Stage 2**: Re-aggregate by original keys, counting rows per group = COUNT(DISTINCT)

## ResultMerger.java — No HashSet Logic

`ResultMerger` has **no HashSet merge code**. It only handles Page-based merges (passthrough, aggregation, sorted). The HashSet union logic lives entirely in `TransportTrinoSqlAction`.

## Key Insight

The HashSet merge is a **local-only optimization**. When shards are on remote nodes, the `transient` fields are null after deserialization, and the coordinator falls back to the two-stage Page-based dedup merge via `ResultMerger.mergeAggregation()`.
