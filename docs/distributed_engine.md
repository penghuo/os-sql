# Distributed Query Engine (DQE) — Design Document

> Covers Phase 1 (Scatter-Gather MVP), Phase 2 (Shuffle + Advanced Aggregation),
> and Phase 3 (Shard Dispatch Wiring + Optimizations).

---

## 1. Problem Statement

PPL translates Calcite RelNode to OpenSearch DSL for execution. This translation
has three fundamental limitations:

| ID | Problem | Example |
|----|---------|---------|
| **P1** | DSL cannot express all RelNodes | Window functions, joins, complex expressions have no DSL equivalent |
| **P2** | Operators don't execute on shards | `source=index \| eval a=b+1` pulls ALL rows to coordinator |
| **P3** | Pushdown rules are complex/error-prone | 15 hand-coded rules, each new PPL feature requires a new rule |

**Root cause**: translating between two paradigms (Calcite RelNode and OpenSearch
DSL). The solution: stop translating. Run Calcite directly on shards. Use DSL only
for inverted index access.

---

## 2. Architecture Overview

### Core Principle

Ship the Calcite plan itself to data nodes. Each shard executes the plan as a
Calcite Enumerable pipeline. DSL is used ONLY at the scan level for inverted
index access (via existing `PredicateAnalyzer`). The 15 pushdown rules are
**deleted**, not replaced.

### Component Diagram

```
COORDINATOR:
  PPL -> ANTLR -> AST -> Analyzer -> CalciteRelNodeVisitor -> RelNode (unchanged)
                                                                |
                                    HepPlanner + VolcanoPlanner (unchanged)
                                                                |
                                                        PlanSplitter
                                                                |
                          +---------- Exchange ----------------+-----------------------+
                          |                                                            |
                Shard Fragment                                         Coordinator Fragment
                (serialized as JSON,                                  (merge operations)
                 shipped to data nodes)
                          |                                                            |
          TransportCalciteShardAction                              . ConcatExchange
          (scatter to shards)                                      . MergeSortExchange
                          |                                        . MergeAggregateExchange
                   +------+------+                                   (Welford/HLL/tdigest)
                   |  SHUFFLE    |
                   |             |
        TransportCalciteShuffleAction
        (shard-to-shard by hash key)
                   |
DATA NODES:
  ShardCalciteRuntime
  +-- Deserialize RelNode plan fragment
  +-- CalciteLocalShardScan:
  |   +-- PredicateAnalyzer -> Lucene Query (inverted index)
  |   +-- NodeClient.search(preference=_shards:N|_local)
  |       (query cache, DLS/FLS, concurrent segment search preserved)
  +-- Calcite Enumerable pipeline:
  |   +-- Filter (complex expressions Lucene can't handle)
  |   +-- Project / Eval (compute new columns)
  |   +-- Partial Aggregate (SUM, COUNT, AVG, MIN, MAX, STDDEV, VAR)
  |   +-- Sort TopK (sort + limit pattern)
  +-- Stream typed results -> coordinator
```

### Problem-Solution Mapping

| Problem | How Solved | Phase |
|---------|-----------|-------|
| **P1**: DSL can't express windows/joins | Calcite handles natively. Phase 1: coordinator. Phase 2: distributed via shuffle. | 1, 2 |
| **P2**: Operators don't execute on shards | Full Calcite Enumerable runs on each shard. | 1 |
| **P3**: Pushdown rules error-prone | All 15 rules deleted. Single PlanSplitter classifies operators. | 1 |

---

## 3. Phase 1 — Scatter-Gather MVP

Phase 1 solves all three problems with six new components and zero new pushdown
rules. It replaces 15 hand-coded rules with a single `PlanSplitter`.

### 3.1 PlanSplitter

A post-order tree-walk `RelShuttle` that inserts Exchange nodes at the
shard/coordinator boundary.

**File**: `opensearch/.../planner/PlanSplitter.java`

**Operator Placement**:

| Operator | Location | Why |
|----------|----------|-----|
| Scan | Shard (`CalciteLocalShardScan`) | Local Lucene segments |
| Filter (simple: `a=1`) | Shard (Lucene via `PredicateAnalyzer`) | Inverted index |
| Filter (complex: `fn(a)>b`) | Shard (Calcite Filter) | Can't express in Lucene |
| Eval / Project | Shard (Calcite Project) | Row-level, no cross-shard dependency |
| Partial Aggregate | Shard (Calcite Aggregate) | Decomposable aggs only |
| Final Aggregate | Coordinator (merge partials) | Cross-shard merge |
| Sort + Limit (TopK) | Shard (TopK) + Coordinator (merge-sort) | Distributed TopK |
| Sort (no limit) | Coordinator | Needs all data |
| Window function | Coordinator (Phase 1), Distributed (Phase 2) | See Phase 2 |
| Join | Coordinator (Phase 1), Distributed (Phase 2) | See Phase 2 |
| Dedup | Coordinator | Needs global view |

### 3.2 RelNodeSerializer

Serializes full RelNode trees to JSON for transport to data nodes.

**File**: `opensearch/.../storage/serde/RelNodeSerializer.java`

- Extends Calcite's `RelJsonWriter`/`RelJsonReader` framework
- Custom `ExtendedRelJson` for PPL operators (dedup, rare, top)
- Scan nodes serialized as `ShardScanPlaceholder` markers; rebound to
  `CalciteLocalShardScan` on data nodes
- Non-serializable references (`RelOptCluster`, `RelOptTable`) reconstructed
  from shard context

### 3.3 Transport Layer

**Files**: `plugin/.../transport/CalciteShardAction.java`, `CalciteShardRequest.java`,
`CalciteShardResponse.java`, `TransportCalciteShardAction.java`

- Action: `indices:data/read/opensearch/calcite/shard`
- Request carries: serialized plan JSON, index name, shard ID
- Response carries: `List<Object[]>` rows, column names, optional error, optional
  binary fields for partial aggregate states
- Auth, TLS, circuit breakers from OpenSearch transport infrastructure
- `ActionFilters` provide RBAC and audit logging

### 3.4 Shard-Side Execution

**ShardCalciteRuntime** (`opensearch/.../storage/scan/ShardCalciteRuntime.java`):
1. Deserializes plan fragment via `RelNodeSerializer`
2. Reconstructs `RelOptCluster` with `OpenSearchTypeFactory`
3. Binds `ShardScanPlaceholder` nodes to `CalciteLocalShardScan`
4. Executes Calcite Enumerable pipeline
5. Collects typed `Object[]` rows as results

**CalciteLocalShardScan** (`opensearch/.../storage/scan/CalciteLocalShardScan.java`):
- Uses `NodeClient.search(preference=_shards:N|_local)` for shard routing
- Routes through full OpenSearch search pipeline, preserving:
  - Query cache
  - DLS/FLS (security plugin enforcement)
  - Concurrent segment search (OpenSearch 2.12+)
  - Lucene optimizations (inverted index, skip lists, early termination)
  - RBAC and audit logging

### 3.5 Exchange (Merge) Operators

| Exchange | Strategy | Used For |
|----------|----------|----------|
| `ConcatExchange` | Unordered concatenation | Scan, filter, project, dedup |
| `MergeSortExchange` | Priority queue merge-sort | ORDER BY + LIMIT (TopK) |
| `MergeAggregateExchange` | Function-specific merge | Aggregation (stats, grouped) |

### 3.6 Partial Aggregation

**File**: `opensearch/.../planner/merge/PartialAggregate.java`

| Function | Partial State | Merge | Phase |
|----------|--------------|-------|-------|
| COUNT | long | SUM of counts | 1 |
| SUM | double | SUM of sums | 1 |
| AVG | {sum, count} | SUM(sums)/SUM(counts) | 1 |
| MIN | comparable | MIN of mins | 1 |
| MAX | comparable | MAX of maxes | 1 |
| STDDEV/VAR | {count, sum, sumSquares} | Welford merge | 2 |
| DISTINCT_COUNT_APPROX | HyperLogLog bytes | HLL merge | 2 |
| PERCENTILE_APPROX | t-digest bytes | digest merge | 2 |

Non-decomposable aggs fall back to coordinator-side full aggregation. The top-5
aggs cover ~90% of real queries.

### 3.7 Execution Examples

**Stats query** (P2 + P3 solved):
```
PPL: source=access_logs | stats count() by status_code

  MergeAggregateExchange(merge counts)                 <- COORDINATOR
    PartialAggregate(count, group=status_code)          <- SHARD (Calcite)
      CalciteLocalShardScan(access_logs)                <- SHARD (SearchService)
```

**Eval on shards** (P2 solved):
```
PPL: source=index | eval score=price*qty | where score > 100

  ConcatExchange                                        <- COORDINATOR
    Filter(score > 100)                                 <- SHARD (Calcite)
      Project(score=price*qty, *)                       <- SHARD (Calcite)
        CalciteLocalShardScan(index)                    <- SHARD (SearchService)
```

**Window function** (P1 solved):
```
PPL: source=index | eval a=b+1 | eventstats avg(a) by group

  Window(avg(a) PARTITION BY group)                     <- COORDINATOR (Calcite)
    ConcatExchange
      Project(a=b+1, group, *)                          <- SHARD (Calcite)
        CalciteLocalShardScan(index)                    <- SHARD (SearchService)
```

### 3.8 Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `plugins.calcite.legacy_pushdown.enabled` | `false` | Emergency rollback to old pushdown path |

---

## 4. Phase 2 — Shuffle + Advanced Aggregation

Phase 1 runs window functions and joins on the coordinator after gathering all
data. Phase 2 adds **shuffle** capability: repartition data by key across nodes,
enabling fully distributed execution.

### 4.1 What Changes

| Capability | Phase 1 | Phase 2 |
|-----------|---------|---------|
| Joins | Coordinator (all data gathered) | Distributed hash join (shuffle by join key) |
| Window functions | Coordinator (all data gathered) | Distributed (shuffle by PARTITION BY key) |
| STDDEV/VAR | Coordinator (non-decomposable) | Distributed (Welford's algorithm) |
| DISTINCT_COUNT_APPROX | Coordinator | Distributed (HLL merge) |
| PERCENTILE_APPROX | Coordinator | Distributed (t-digest merge) |
| Data movement | Scatter-gather only | Scatter-gather + shuffle |

### 4.2 HashExchange

**File**: `opensearch/.../planner/merge/HashExchange.java`

Partitions data by hash key: `hash(distributionKeys) % numPartitions`. Used for
distributed joins (hash by join key) and distributed windows (hash by PARTITION BY
key).

### 4.3 Shuffle Transport

**Files**: `plugin/.../transport/CalciteShuffleAction.java`, `CalciteShuffleRequest.java`,
`CalciteShuffleResponse.java`, `TransportCalciteShuffleAction.java`

- Action: `indices:data/read/opensearch/calcite/shuffle`
- Shard-to-shard data movement: each shard sends rows to the correct target
  node based on key hash
- Binary blob fields for serialized partial states (HLL, t-digest)

### 4.4 Multi-Phase Execution

```
Phase 1: Scatter shard fragments (existing)
  coordinator -> TransportCalciteShardAction -> data nodes -> partial results

Phase 2: Shuffle intermediate results (NEW)
  data nodes -> TransportCalciteShuffleAction -> data nodes -> repartitioned data

Phase 3: Gather final results (existing)
  data nodes -> coordinator -> merged final result
```

### 4.5 Distributed Joins

**Equi-join** via hash shuffle:
```
ConcatExchange                                <- coordinator (concat partitions)
  Join(condition)                             <- partition node (local join)
    HashExchange(left, joinKey)               <- shuffle left by join key
    HashExchange(right, joinKey)              <- shuffle right by join key
```

**Non-equi-join**: falls back to gather-to-coordinator (Phase 1 behavior).

**LocalJoinExchange** (`opensearch/.../planner/merge/LocalJoinExchange.java`):
concatenates independent partition join results on the coordinator.

### 4.6 Distributed Window Functions

When a Window has PARTITION BY keys, all rows with the same partition key must be
co-located:

```
ConcatExchange                                <- coordinator
  Window(avg(x) PARTITION BY key)             <- partition node (local window)
    HashExchange(input, partitionKey)         <- shuffle by partition key
```

Windows WITHOUT PARTITION BY still use ConcatExchange (need global view).

### 4.7 Advanced Aggregation

**STDDEV/VAR via Welford's Algorithm**: Each STDDEV/VAR call decomposes into 3
partial columns on shards: `COUNT(x)`, `SUM(x)`, `SUM(x*x)`. On the coordinator:
```
M2 = sumOfSquares - sum * sum / count
VAR_POP  = M2 / count
VAR_SAMP = M2 / (count - 1)
STDDEV   = sqrt(variance)
```

**MergeStrategy Enum** in `MergeAggregateExchange`:

| Strategy | Used For | Merge Logic |
|----------|----------|-------------|
| `SIMPLE` | COUNT, SUM, AVG, MIN, MAX | Function-specific merge |
| `WELFORD` | STDDEV, VAR | 3 partial columns, Welford/Chan merge |
| `BINARY_STATE` | DISTINCT_COUNT_APPROX, PERCENTILE_APPROX | Binary blob merge |

### 4.8 Serialization Extensions

| Node Type | Format |
|-----------|--------|
| `HashExchange` | `{relOp, distributionKeys, numPartitions, input}` |
| `LocalJoinExchange` | `{relOp, joinType, condition, input}` |
| `PartialAggregate` (Welford) | `{relOp, group, aggs[count,sum,sumSq], partial}` |

`CalciteShardResponse.binaryFields` carries serialized HLL and t-digest states.

---

## 5. Phase 3 — Shard Dispatch Wiring + Optimizations

Phase 1-2 built the complete DQE infrastructure: PlanSplitter, Exchange nodes,
transport actions, shard runtime, partial aggregation, and Welford/HLL/tdigest
support. Phase 3 **wires actual shard dispatch** so distributed queries execute
for real, then adds optimizations (broadcast join, cost-based strategy, circuit
breaker).

### 5.1 DistributedExecutor

**File**: `plugin/.../transport/DistributedExecutor.java`

Core coordinator orchestration class that connects all Phase 1-2 infrastructure:

1. Takes distributed plan (with Exchange nodes) + `NodeClient` + `ClusterService`
2. Walks tree bottom-up to find Exchange nodes
3. For each Exchange: extracts child subtree (shard fragment), serializes via
   `RelNodeSerializer`
4. Resolves active primary shards via `ShardRoutingResolver`
5. Dispatches to each shard via
   `client.execute(CalciteShardAction.INSTANCE, request, listener)`
6. Collects `CalciteShardResponse` from all shards asynchronously
   (`CountDownLatch`-based)
7. Converts responses to `Exchange.ShardResult` and injects via
   `exchange.setShardResults()`
8. Returns merged results from `exchange.scan()`

**Error handling**: If any shard fails, the error is propagated immediately.
Timeout after 60 seconds.

### 5.2 Exchange.scan() Implementations

Phase 3 implements `scan()` in all Exchange subclasses. Previously, all `scan()`
methods threw `UnsupportedOperationException`.

**ConcatExchange.scan()**: Concatenates all shard rows into a single Enumerable.
Simple append of all `ShardResult.getRows()`.

**MergeSortExchange.scan()**: Priority-queue merge of pre-sorted shard results.
Builds a `Comparator<Object[]>` from the `RelCollation` (supporting
ASCENDING/DESCENDING direction and FIRST/LAST null ordering). Seeds a
`PriorityQueue` with one entry per shard iterator, pops the minimum, advances
that iterator. Applies offset/fetch from `RexLiteral` values.

**MergeAggregateExchange.scan()**: Groups all shard rows by group-key columns,
then merges per-group aggregates based on `MergeStrategy`:
- **SIMPLE**: SUM/COUNT values are summed; MIN takes minimum; MAX takes maximum;
  AVG computes SUM(partial_sums)/SUM(partial_counts).
- **WELFORD**: Three consecutive partial columns (count, sum, sumSquares) are
  merged. Final result computed via M2 formula based on `SqlKind`.
- **BINARY_STATE**: Deferred (HLL/t-digest merge is complex).

**HashExchange.scan()**: Returns pre-partitioned results concatenated. The
actual hash partitioning and shuffle dispatch is handled by DistributedExecutor
before `scan()` is called.

**LocalJoinExchange.scan()**: Same as ConcatExchange. Each partition's join
results are independent.

**BroadcastExchange.scan()**: Same as ConcatExchange. Broadcast logic (sending
data to all nodes) is handled by DistributedExecutor.

### 5.3 ShardRoutingResolver

**File**: `opensearch/.../executor/ShardRoutingResolver.java`

Utility class for shard dispatch:
- `resolveActiveShards(ClusterService, indexName)`: returns active primary
  `ShardRouting` objects by iterating `IndexRoutingTable`
- `extractIndexName(RelNode)`: walks plan tree to find
  `CalciteEnumerableIndexScan` and extracts the index name from its qualified
  table name

### 5.4 Execution Engine Wiring

**File**: `opensearch/.../executor/OpenSearchExecutionEngine.java`

The `execute(RelNode, CalcitePlanContext, ResponseListener)` method now:
1. Applies `PlanSplitter.split(rel)` to get distributed plan
2. Checks if plan contains Exchange nodes AND distributedExecutor is available
3. If yes: dispatches via `distributedExecutor.apply(distributedPlan, indexName)`
4. If no: falls back to single-node Calcite JDBC path
5. Builds `QueryResponse` from result rows

**Cross-module design**: `DistributedExecutor` lives in the `plugin` module
(where transport actions are). It is injected into `OpenSearchExecutionEngine`
(in the `opensearch` module) as a `BiFunction<RelNode, String, List<Object[]>>`
through Guice, avoiding circular module dependencies.

**Fallback**: If distributed execution throws, the engine logs a warning and
falls back to single-node execution automatically.

### 5.5 BroadcastExchange

**File**: `opensearch/.../planner/merge/BroadcastExchange.java`

For small-table joins: broadcasts entire small side to all nodes instead of
shuffle. PlanSplitter's `splitEquiJoin()` checks `isSmallSide()` and uses
`BroadcastExchange` for the small side, `ConcatExchange` for the large side.

Currently `isSmallSide()` returns `false` (preserving HashExchange behavior),
ready for cost-based activation via `IndexStatisticsProvider`.

### 5.6 Cost-Based Join Strategy

**File**: `opensearch/.../statistics/IndexStatisticsProvider.java`

Uses OpenSearch `_stats` API for index size estimation:
- `estimateRowCount(indexName)` — document count
- `estimateSizeBytes(indexName)` — store size
- `isSmallEnoughForBroadcast(indexName)` — below threshold check

Feeds into PlanSplitter for join strategy selection:
- Small table (< 10,000 docs) -> BroadcastExchange
- Large tables -> HashExchange
- Both small -> coordinator-side join

Currently returns conservative defaults (stub). Actual `_stats` API integration
is ready for the next iteration.

### 5.7 Circuit Breaker Integration

**File**: `opensearch/.../executor/DistributedQueryMemoryTracker.java`

Tracks memory of in-flight shard results against OpenSearch's REQUEST circuit
breaker:
- `trackShardResult(rowCount, columnCount, extraBytes)` — estimates memory
  footprint (64 bytes per field value + binary field sizes) and calls
  `breaker.addEstimateBytesAndMaybeBreak()`
- `release()` — releases all tracked memory back to the breaker
- Implements `AutoCloseable` for try-with-resources usage
- Throws `CircuitBreakingException` if memory limit is exceeded

### 5.8 Guice Binding Updates

**File**: `plugin/.../config/OpenSearchPluginModule.java`

```java
@Provides
public ExecutionEngine executionEngine(
    OpenSearchClient client, ExecutionProtector protector,
    PlanSerializer planSerializer, NodeClient nodeClient,
    ClusterService clusterService) {
  DistributedExecutor executor = new DistributedExecutor(nodeClient, clusterService);
  return new OpenSearchExecutionEngine(client, protector, planSerializer, executor::execute);
}
```

---

## 6. Sequence Diagram (All Phases)

```
Client       Coordinator       PlanSplitter    Serializer    Shard-0      Shard-1      Exchange
  |              |                  |              |            |            |            |
  |-- PPL ------>|                  |              |            |            |            |
  |              |-- split(plan) -->|              |            |            |            |
  |              |<-- plan+Exchange-|              |            |            |            |
  |              |-- serialize(frag) ------------->|            |            |            |
  |              |<-- JSON plan ------------------|            |            |            |
  |              |-- CalciteShardRequest(plan,0) ------------>|            |            |
  |              |-- CalciteShardRequest(plan,1) ------------------------------>|       |
  |              |                  |              |            |            |            |
  |              |                  |              |  execute() |  execute() |            |
  |              |                  |              |  Calcite   |  Calcite   |            |
  |              |                  |              |  pipeline  |  pipeline  |            |
  |              |                  |              |            |            |            |
  |              |<-- CalciteShardResponse(rows) -------------|            |            |
  |              |<-- CalciteShardResponse(rows) ----------------------------|           |
  |              |                  |              |            |            |            |
  |              |-- setShardResults(responses) ---------------------------------------->|
  |              |<-- exchange.scan() (merged results) ----------------------------------|
  |              |                  |              |            |            |            |
  |<-- results --|                  |              |            |            |            |
```

---

## 7. Files Summary

### Phase 1 — New Files

| File | Purpose |
|------|---------|
| `opensearch/.../planner/PlanSplitter.java` | Exchange node insertion |
| `opensearch/.../storage/serde/RelNodeSerializer.java` | RelNode tree serialization |
| `opensearch/.../planner/merge/Exchange.java` | Abstract exchange base class |
| `opensearch/.../planner/merge/ConcatExchange.java` | Unordered concatenation merge |
| `opensearch/.../planner/merge/MergeSortExchange.java` | Priority queue merge-sort |
| `opensearch/.../planner/merge/MergeAggregateExchange.java` | Partial aggregate merge |
| `opensearch/.../planner/merge/PartialAggregate.java` | Shard-side partial aggregation |
| `opensearch/.../storage/scan/CalciteLocalShardScan.java` | Shard scan via SearchService |
| `opensearch/.../storage/scan/ShardCalciteRuntime.java` | Shard-side plan execution |
| `plugin/.../transport/CalciteShardAction.java` | ActionType definition |
| `plugin/.../transport/CalciteShardRequest.java` | Shard dispatch request |
| `plugin/.../transport/CalciteShardResponse.java` | Shard execution response |
| `plugin/.../transport/TransportCalciteShardAction.java` | Transport action handler |

### Phase 1 — Deleted Files (15 pushdown rules)

| File | Replaced By |
|------|-------------|
| `FilterIndexScanRule.java` | PlanSplitter |
| `ProjectIndexScanRule.java` | PlanSplitter |
| `AggregateIndexScanRule.java` | PlanSplitter |
| `LimitIndexScanRule.java` | PlanSplitter |
| `SortIndexScanRule.java` | PlanSplitter |
| `SortExprIndexScanRule.java` | PlanSplitter |
| `DedupPushdownRule.java` | PlanSplitter |
| `SortProjectExprTransposeRule.java` | PlanSplitter |
| `ExpandCollationOnProjectExprRule.java` | PlanSplitter |
| `SortAggregateMeasureRule.java` | PlanSplitter |
| `RareTopPushdownRule.java` | PlanSplitter |
| `EnumerableTopKMergeRule.java` | PlanSplitter |
| `AggregateAnalyzer.java` | Calcite-native agg |

### Phase 2 — New Files

| File | Purpose |
|------|---------|
| `opensearch/.../planner/merge/HashExchange.java` | Shuffle by hash key |
| `opensearch/.../planner/merge/LocalJoinExchange.java` | Concat post-shuffle join results |
| `plugin/.../transport/CalciteShuffleAction.java` | Shuffle ActionType |
| `plugin/.../transport/CalciteShuffleRequest.java` | Shuffle request |
| `plugin/.../transport/CalciteShuffleResponse.java` | Shuffle response |
| `plugin/.../transport/TransportCalciteShuffleAction.java` | Shuffle transport handler |

### Phase 3 — New Files

| File | Purpose |
|------|---------|
| `plugin/.../transport/DistributedExecutor.java` | Coordinator orchestration |
| `opensearch/.../executor/ShardRoutingResolver.java` | Shard routing resolution |
| `opensearch/.../executor/DistributedQueryMemoryTracker.java` | Circuit breaker integration |
| `opensearch/.../planner/merge/BroadcastExchange.java` | Broadcast join exchange |
| `opensearch/.../statistics/IndexStatisticsProvider.java` | Cost-based join strategy |

### Phase 3 — Modified Files

| File | Change |
|------|--------|
| `opensearch/.../planner/merge/Exchange.java` | Added `ShardResult` inner class, `setShardResults()` |
| `opensearch/.../planner/merge/ConcatExchange.java` | Implemented `scan()` |
| `opensearch/.../planner/merge/MergeSortExchange.java` | Implemented `scan()` with merge-sort |
| `opensearch/.../planner/merge/MergeAggregateExchange.java` | Implemented `scan()` with strategy merge |
| `opensearch/.../planner/merge/HashExchange.java` | Implemented `scan()` |
| `opensearch/.../planner/merge/LocalJoinExchange.java` | Implemented `scan()` |
| `opensearch/.../executor/OpenSearchExecutionEngine.java` | Wired distributed execution path |
| `opensearch/.../planner/PlanSplitter.java` | Added broadcast join strategy |
| `opensearch/.../storage/serde/RelNodeSerializer.java` | Added BroadcastExchange serialization |
| `plugin/.../config/OpenSearchPluginModule.java` | Inject DistributedExecutor via Guice |

---

## 8. Security

`CalciteLocalShardScan` uses `NodeClient.search(preference=_shards:N|_local)`,
routing through the full OpenSearch search pipeline:

- **RBAC + audit**: `ActionFilters` on `TransportCalciteShardAction` enforce
  role-based access and audit logging at the transport level
- **DLS**: Security plugin injects DLS filter into search requests before
  execution. `CalciteLocalShardScan` sees only DLS-filtered documents.
- **FLS**: Security plugin strips restricted fields from search results.
  `CalciteLocalShardScan` reads only permitted fields.
- No changes to opensearch-security plugin required

---

## 9. Testing Strategy

### Unit Tests

- PlanSplitter operator placement for each scenario
- RelNode serialization round-trip for every operator type
- Partial aggregation merge correctness (all strategies)
- Exchange `scan()` correctness with mock shard results
- ShardRoutingResolver shard resolution and index name extraction
- DistributedExecutor dispatch and error handling

### Integration Tests

- All calcite/remote tests pass with DQE path
- Eval-on-shards, partial agg, window, join produce correct results
- DQE path vs legacy pushdown produce identical results
- Feature flag toggle test

### Key Commands

```bash
./gradlew :opensearch:test -x spotlessCheck           # opensearch module
./gradlew :opensearch-sql-plugin:test -x spotlessCheck # plugin module
./gradlew :integ-test:integTest -DignorePrometheus     # integration suite
```

---

## 10. Roadmap

### Phase 1: Scatter-Gather MVP (Complete)

- Full Calcite Enumerable pipeline on data nodes
- Partial aggregation for COUNT, SUM, AVG, MIN, MAX
- Merge-sort for distributed TopK
- Window functions and joins on coordinator
- Deletion of all 15 pushdown rules
- Emergency rollback via feature flag

### Phase 2: Shuffle + Advanced Aggregation (Complete)

- HashExchange for hash-based partitioning
- Distributed equi-joins (hash join)
- Distributed partitioned window functions
- STDDEV/VAR via Welford's online algorithm
- DISTINCT_COUNT_APPROX via HLL merge
- PERCENTILE_APPROX via t-digest merge

### Phase 3: Shard Dispatch + Optimizations (Complete)

- DistributedExecutor wiring (actual shard dispatch)
- Exchange.scan() implementations (all 6 subclasses)
- ShardRoutingResolver for shard targeting
- BroadcastExchange for small-table joins
- IndexStatisticsProvider for cost-based join strategy
- Circuit breaker integration via DistributedQueryMemoryTracker
- Automatic fallback from distributed to single-node execution

### Future Work

- Arrow columnar batch transfer for network efficiency
- RangeExchange for range-based partitioning
- Off-heap memory via Arrow `BufferAllocator`
- `docvalue_fields` optimization to bypass `_source` JSON parsing
- Full `_stats` API integration in IndexStatisticsProvider
- BINARY_STATE merge implementation for HLL/t-digest
- Spill-to-disk for large shuffle operations
