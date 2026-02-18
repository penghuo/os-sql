# Phase 2 Design Document: Full Shuffle + Joins

## Status: APPROVED (extracted from v2 design doc)

## 1. Scope

Phase 2 extends the distributed engine from scatter-gather (Phase 1) to full shuffle execution, enabling distributed joins, window functions, multi-stage aggregations, and spill-to-disk. After Phase 2, ALL existing PPL Calcite integration tests must pass through the distributed engine (no DSL fallback for any supported pattern).

**In scope:**
- Hash joins (hash-partitioned on join key)
- Broadcast joins (small-table broadcast)
- Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG/MIN/MAX OVER)
- HashExchange (hash-partition Pages by key columns)
- BroadcastExchange (full copy to all nodes)
- Spill-to-disk for aggregation, sort, join
- LuceneAggScan (Lucene-native partial aggregation)
- LuceneSortScan (sorted top-K with early termination)
- Full regression suite + performance benchmarks

**Prerequisites:** Phase 1 (IC-3) passes.

## 2. Architecture Extensions

### Additional Layer 2 Operators

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2 Extensions (Phase 2)                                    │
│   Exchange:                                                     │
│     HashExchange, BroadcastExchange                             │
│   Join operators (ported from Trino):                           │
│     LookupJoinOperator, HashBuilderOperator                     │
│   Window operator (ported from Trino):                          │
│     WindowOperator                                              │
│   Lucene-native (advanced):                                     │
│     LuceneAggScan, LuceneSortScan                               │
│   Spill-to-disk:                                                │
│     SpillableHashAggregationBuilder, FileSingleStreamSpiller    │
└─────────────────────────────────────────────────────────────────┘
```

### Multi-Stage Execution Flow

```
Hash Join:
  Build side: LuceneScan -> HashExchange(join_key) -> HashBuilderOperator (build hash table)
  Probe side: LuceneScan -> HashExchange(join_key) -> LookupJoinOperator (probe hash table)
  Result: joined Pages -> GatherExchange -> coordinator

Distributed Aggregation (high cardinality):
  LuceneScan -> partial HashAgg -> HashExchange(group_key) -> final HashAgg -> GatherExchange

Window Functions:
  LuceneScan -> HashExchange(partition_key) -> sort(order_key) -> WindowOperator -> GatherExchange

Broadcast Join (small table):
  Small side: LuceneScan -> GatherExchange -> BroadcastExchange (to all nodes)
  Large side: LuceneScan (local) -> LookupJoinOperator (probe broadcasted hash table)
```

## 3. New Exchange Operators

### 3.1 HashExchange

Hash-partitions Pages by key columns. Sends each partition to the assigned target node via TransportService. Receives Pages from all sources for its partition.

```java
class HashExchange implements SourceOperator {
    // Receives Pages from upstream, hash-partitions by key columns
    // Sends partitions to target nodes via ShardQueryAction
    // Collects Pages assigned to this node's partition
}
```

### 3.2 BroadcastExchange

Sends full copy of Pages to all participating nodes. Used for small-table joins.

```java
class BroadcastExchange implements SourceOperator {
    // Receives all Pages from one side (small table)
    // Sends complete copy to every participating node
    // Memory-bounded buffer
}
```

### 3.3 AddExchanges Extensions

Extend Phase 1's AddExchanges for:
- Hash-partition for joins: partition both sides on join key
- Broadcast for small-table joins: broadcast if build side < threshold
- Hash-partition for distributed aggregation: partition on group key for final agg

Join strategy decision:
```
if (buildSideEstimatedSize < broadcastThreshold) {
    // BroadcastExchange on build side
} else {
    // HashExchange on both sides, partitioned by join key
}
```

## 4. Join Operators (Ported from Trino)

### 4.1 LookupJoinOperator

Port from `io.trino.operator.join.LookupJoinOperator`. Supports INNER, LEFT, RIGHT, FULL, SEMI, ANTI join types. Uses build/probe lifecycle: probe side streams Pages through, probes against build-side hash table.

### 4.2 HashBuilderOperator

Port from `io.trino.operator.join.HashBuilderOperator` + `JoinHash` + `PagesHash`. Builds hash table from build-side Pages. Memory-tracked via OperatorContext.

## 5. Window Operator (Ported from Trino)

Port from `io.trino.operator.WindowOperator` + `RegularWindowPartition`. Supports:
- ROW_NUMBER, RANK, DENSE_RANK
- LAG, LEAD
- SUM, AVG, MIN, MAX OVER
- ROWS frame, RANGE frame
- Single and multiple partitions

## 6. Lucene-Native Advanced Operators

### 6.1 LuceneAggScan

Lucene-native aggregation using `Collector`/`LeafCollector` with DocValues accumulation. Faster than scan + agg for simple aggregates (COUNT, SUM, MIN, MAX).

### 6.2 LuceneSortScan

Uses `IndexSearcher.search(Query, int, Sort)` with early termination for top-K. Faster than full scan + sort when K is small relative to total docs.

## 7. Spill-to-Disk

### 7.1 SpillableHashAggregationBuilder

Extends Phase 1's in-memory `HashAggregationOperator` with spill support. When memory exceeds revocable threshold, flushes hash table to local temp files and rebuilds.

### 7.2 FileSingleStreamSpiller

Simplified port from Trino's `FileSingleStreamSpiller`. Writes Pages to local temp files, reads back during merge. Configurable spill path.

### 7.3 Integration with MemoryPool

Revocable memory tracking:
- `reserve()` for non-revocable (hash table structure)
- `reserveRevocable()` for spillable buffers
- Memory pressure triggers `startMemoryRevoke()` on operators
- Operators flush to disk, call `finishMemoryRevoke()`
- `freeRevocable()` releases memory back to pool

## 8. Module Structure Additions

```
distributed-engine/
  src/main/java/org/opensearch/sql/distributed/
    exchange/      + HashExchange, BroadcastExchange
    operator/join/ LookupJoinOperator, HashBuilderOperator, JoinHash, PagesHash
    operator/      + WindowOperator, RegularWindowPartition
    lucene/        + LuceneAggScan, LuceneSortScan
    spill/         SpillableHashAggregationBuilder, FileSingleStreamSpiller
```

## 9. Phase 2 Exit Criteria

1. **ALL existing PPL Calcite integration tests pass** with distributed engine enabled
2. Join queries execute through distributed engine (no DSL fallback)
3. Window function queries execute through distributed engine
4. Spill-to-disk works under memory pressure
5. Multi-stage explain shows correct stages/exchanges
6. No memory leaks after query completion
7. Full regression suite passes (Calcite ITs, yamlRestTests, doctests)
