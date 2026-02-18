# Design Document: Native Distributed Query Engine for OpenSearch

## Status: APPROVED (v2 — post-challenge revision)

## 1. Problem Statement

OpenSearch's SQL/PPL plugin currently executes queries through a DSL rewrite path: PPL is parsed into an AST, converted to Calcite RelNodes, optimized with pushdown rules, and translated into OpenSearch DSL SearchRequests. This approach cannot express distributed joins, window functions, multi-stage aggregations, or cross-shard shuffle operations. All execution happens on a single coordinating node.

We need a native distributed query engine that runs **inside** OpenSearch nodes (no external processes), uses OpenSearch's own cluster topology, transport layer, and thread pools, and reads data directly from Lucene segments.

## 2. Goals

- PPL queries execute through native distributed operators, not DSL rewrite
- Physical operators read data directly from OpenSearch indices via Lucene APIs
- Scatter-gather mode works for single-index filter/agg/sort queries
- Full shuffle mode works for joins, window functions, multi-stage aggregations
- All existing Calcite integTests, yamlRestTests, doctests pass without regression
- `_explain` API returns meaningful distributed execution plans
- No query latency regression for simple queries
- Feature flag to enable/disable (`plugins.sql.distributed_engine.enabled`)
- No external Trino process required

## 3. Non-Goals

- Replacing Calcite for logical optimization (Calcite stays for join reorder, predicate pushdown, etc.)
- Cross-cluster distributed queries (intra-cluster only)
- Replacing OpenSearch's native search path for non-SQL/PPL queries
- Real-time streaming execution (batch analytical queries only)

## 4. Architecture Overview

### 4-Layer Design (v2)

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4: Calcite Logical Optimization (UNCHANGED)               │
│   CalciteRelNodeVisitor -> RelNode -> Volcano logical rules     │
│   Join reordering, predicate pushdown, constant folding,        │
│   aggregate splitting, projection pruning, subquery decorrel    │
│   All existing UDFs, type system, expression visitors preserved │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3: Physical Planning (Ported from Trino)                  │
│   RelNodeToPlanNodeConverter: Calcite RelNode -> Trino PlanNode │
│   AddExchanges (adapted): inserts ExchangeNodes for distrib.    │
│   PlanFragmenter: splits at ExchangeNode boundaries -> stages   │
│   StageScheduler: assigns fragments to nodes via shard routing  │
│   Decision inputs: ClusterState, shard counts, data locality    │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2: Physical Operators (Ported from Trino + Lucene-native) │
│   Lucene-native (SHARD_LOCAL, OpenSearch-specific):             │
│     LuceneFilterScan, LuceneAggScan, LuceneSortScan,            │
│     LuceneFullScan                                              │
│   Exchange (adapted for TransportService):                      │
│     GatherExchange, HashExchange, BroadcastExchange             │
│   Generic (PORTED from Trino):                                  │
│     HashAggregationOperator, LookupJoinOperator,                │
│     HashBuilderOperator, WindowOperator, OrderByOperator,       │
│     TopNOperator, FilterAndProjectOperator, MergeSortedPages    │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1: Trino Execution Runtime (Ported from Trino)            │
│   Driver/Pipeline cooperative scheduling                        │
│   Context hierarchy: QueryContext -> TaskContext ->             │
│     PipelineContext -> DriverContext -> OperatorContext         │
│   MemoryPool: integrated with OpenSearch circuit breakers       │
│   Page/Block: columnar data format (ported faithfully)          │
│   Operator interface: isBlocked/needsInput/addInput/getOutput   │
└─────────────────────────────────────────────────────────────────┘
```

### Query Execution Flow

```
PPL Query
  -> PPLSyntaxParser -> AST
  -> CalciteRelNodeVisitor -> Calcite LogicalRelNode
  -> Volcano optimizer (logical rules only):
     join reorder, predicate pushdown, aggregate split, constant fold
  -> Optimized LogicalRelNode
  -> [NEW] RelNodeToPlanNodeConverter:
     LogicalFilter    -> FilterNode
     LogicalAggregate -> AggregationNode (partial + exchange + final)
     LogicalJoin      -> JoinNode (broadcast vs hash-partition)
     LogicalTableScan -> LuceneTableScanNode
     LogicalSort      -> SortNode / TopNNode
     LogicalProject   -> ProjectNode
  -> [NEW] AddExchanges (adapted from Trino):
     inserts ExchangeNodes based on distribution requirements
  -> [NEW] PlanFragmenter:
     splits at exchange boundaries -> List<StageFragment>
  -> [NEW] StageScheduler:
     assigns fragments to nodes, assigns shards to leaf stages
  -> [NEW] Execution via Driver/Pipeline/Operator/Page
```

### Decision: Why Calcite for Logical, Trino for Physical

| Concern | Calcite | Trino |
|---------|---------|-------|
| Join reordering | Battle-tested (Flink, Dremio, Beam) | Separate optimizer, not needed |
| Predicate pushdown | Volcano rules, production-proven | Separate rules, not needed |
| Exchange insertion | No production precedent | AddExchanges, 10+ years at petabyte scale |
| Plan fragmentation | Not built-in | PlanFragmenter, production-proven |
| Stage scheduling | Not designed for this | PipelinedQueryScheduler, production-proven |

No production system (Flink, Dremio, Beam, Drill) uses Calcite Volcano conventions for distributed physical planning. They all have separate physical planners downstream of Calcite's logical optimization.

## 5. Layer 4: Calcite Logical Optimization (Unchanged)

Existing infrastructure preserved entirely:
- `CalciteRelNodeVisitor` (3,694 lines): AST -> Calcite RelNode
- `CalciteRexNodeVisitor` (782 lines): expression translation
- Expression system (~22,900 lines): UDFs, type system, function resolution
- Volcano optimizer rules: join reordering, predicate pushdown, constant folding, aggregate splitting, projection pruning, subquery decorrelation
- Pushdown rules in `opensearch/.../planner/rules/` (~2,109 lines, 18 files): retained for DSL fallback path

**Integration point**: The optimized `RelNode` tree is the input to `RelNodeToPlanNodeConverter`.

## 6. Layer 3: Physical Planning (Ported from Trino)

### 6.1 RelNodeToPlanNodeConverter

A visitor over Calcite's optimized LogicalRelNode tree that produces Trino-style PlanNode objects.

```java
class RelNodeToPlanNodeConverter {
    PlanNode convert(RelNode optimizedLogical, PlanningContext ctx);
    // LogicalFilter    -> FilterNode
    // LogicalAggregate -> AggregationNode (with partial/final split)
    // LogicalJoin      -> JoinNode
    // LogicalTableScan -> LuceneTableScanNode
    // LogicalSort      -> SortNode / TopNNode
    // LogicalProject   -> ProjectNode
    // PPL-specific nodes: DedupNode, RareTopNNode, TrendlineNode
}
```

### 6.2 AddExchanges (adapted from Trino)

Adapted from `io.trino.sql.planner.optimizations.AddExchanges` (~1,700 LOC). Walks the PlanNode tree and inserts ExchangeNodes based on distribution requirements:
- Hash-partition for joins (partition both sides on join key)
- Broadcast for small-table joins
- Gather for final result collection
- Single for coordinator-only operations

Decision inputs: shard count from ClusterState, index statistics for table size estimation, configurable broadcast threshold.

### 6.3 PlanFragmenter

Adapted from `io.trino.sql.planner.PlanFragmenter` (~850 LOC). Cuts the plan at ExchangeNode boundaries into StageFragment objects:

```java
class StageFragment {
    int stageId;
    PlanNode root;              // subtree for this stage
    PartitioningScheme output;  // how output is distributed
    List<RemoteSourceNode> inputs; // references to upstream stages
}
```

### 6.4 StageScheduler

Maps StageFragments to OpenSearch nodes using ClusterState:
- Leaf stages: assigned to nodes holding the relevant shards (data locality)
- Intermediate stages: assigned to any available data nodes
- Root stage: runs on the coordinating node

Uses OpenSearch's `RoutingTable` and `IndexShardRoutingTable` for shard-to-node mapping.

## 7. Layer 2: Physical Operators

### 7.1 Lucene-Native Leaf Operators (OpenSearch-specific, custom-built)

| Operator | Lucene API | Description |
|----------|-----------|-------------|
| `LuceneFullScan` | `LeafReaderContext`, DocValues | Reads all documents, converts DocValues -> Block |
| `LuceneFilterScan` | `Weight`, `Scorer`, `DocIdSetIterator` | Pushes predicates to Lucene, produces filtered Pages |
| `LuceneAggScan` | `Collector`, `LeafCollector`, DocValues | Lucene-native partial aggregation per segment |
| `LuceneSortScan` | `IndexSearcher.search(Query, int, Sort)` | Sorted top-K with early termination |

These operators produce `Page` objects from Lucene segment data. DocValues map naturally to Block types: `NumericDocValues` -> `LongBlock`/`DoubleBlock`, `SortedDocValues`/`BinaryDocValues` -> `BytesRefBlock`.

### 7.2 Exchange Operators (custom-built for TransportService)

| Operator | Transport | Description |
|----------|-----------|-------------|
| `GatherExchange` | TransportService RPC | Coordinator collects Pages from all leaf stages |
| `HashExchange` | TransportService RPC | Hash-partitions Pages to target nodes by key |
| `BroadcastExchange` | TransportService RPC | Sends Pages to all participating nodes |

Exchange uses OpenSearch's `TransportService` (existing TLS/SSL, existing transport port), not HTTP. Page serialization via `StreamOutput`/`StreamInput` using `PagesSerde`.

### 7.3 Generic Operators (ported from Trino)

| Operator | Trino Source | Key Features |
|----------|-------------|--------------|
| `HashAggregationOperator` | `io.trino.operator.HashAggregationOperator` | Partial/final modes, memory revocation, spill-to-disk, adaptive partial agg |
| `LookupJoinOperator` | `io.trino.operator.join.LookupJoinOperator` | All join types (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, CROSS), build/probe lifecycle |
| `HashBuilderOperator` | `io.trino.operator.join.HashBuilderOperator` | Builds hash table for join probe side |
| `WindowOperator` | `io.trino.operator.WindowOperator` | ROWS/RANGE/GROUPS frames, partition detection, spill |
| `OrderByOperator` | `io.trino.operator.OrderByOperator` | Full sort with spill |
| `TopNOperator` | `io.trino.operator.TopNOperator` | Heap-based top-K, memory-bounded |
| `FilterAndProjectOperator` | `io.trino.operator.project.FilterAndProjectOperator` | Combined filter+project on Page data |
| `MergeSortedPages` | `io.trino.operator.MergeSortedPages` | K-way merge of pre-sorted Page streams |

Adaptation work: adjust import paths, replace `TypeOperators` with type system bridge, wire `OperatorContext` memory tracking to our MemoryPool.

## 8. Layer 1: Execution Runtime (Ported from Trino)

### 8.1 Page/Block Columnar Data Format

Ported faithfully from Trino's `io.trino.spi.Page` and `io.trino.spi.block`:

```
Page { Block[] blocks; int positionCount; }
Block (sealed): DictionaryBlock | RunLengthEncodedBlock | ValueBlock
ValueBlock types: LongArrayBlock, IntArrayBlock, ByteArrayBlock,
                  ShortArrayBlock, VariableWidthBlock (for VARCHAR/VARBINARY)
BlockBuilder: LongBlockBuilder, VariableWidthBlockBuilder, etc.
```

### 8.2 Operator Interface

```java
interface Operator extends AutoCloseable {
    ListenableFuture<?> isBlocked();  // non-blocking signal
    boolean needsInput();
    void addInput(Page page);
    Page getOutput();
    void finish();
    boolean isFinished();
    OperatorContext getOperatorContext();
}
```

### 8.3 Driver/Pipeline

- **Driver**: Cooperative multitasking loop. Moves Pages between adjacent operators until blocked or finished. One Driver per shard per pipeline.
- **Pipeline**: A chain of OperatorFactories that produces Drivers. One Pipeline per plan fragment per node.
- **DriverFactory**: Creates Driver instances for a Pipeline.

Adapted for OpenSearch's `sql-worker` thread pool instead of Trino's dedicated executor.

### 8.4 Context Hierarchy (simplified)

```
QueryContext (1 per query)
  -> MemoryPool reference, query timeout, feature flags
TaskContext (1 per node-task)
  -> Node-level resource tracking
PipelineContext (1 per pipeline)
  -> Pipeline lifecycle, stats
DriverContext (1 per driver)
  -> Driver lifecycle, thread binding
OperatorContext (1 per operator)
  -> Per-operator memory tracking, stats
```

Stripped of Trino's Session, Metadata SPI, HTTP stats infrastructure. Integrated with OpenSearch's `ThreadContext` for security context propagation.

### 8.5 MemoryPool

Tracks memory per query and per operator. Two categories:
- **User memory**: non-revocable allocations (e.g., hash table entries)
- **Revocable memory**: can be spilled to disk under pressure (e.g., sort buffers)

Integration with OpenSearch: registers a `"sql_distributed_query"` circuit breaker. MemoryPool reserves against this breaker. Configurable budget (default: 25% of heap). Blocks allocations when budget is exceeded via `ListenableFuture`.

## 9. Execution Modes

### 9.1 Scatter-Gather (Phase 1)

For single-index queries without joins:
1. Coordinator converts RelNode -> PlanNode -> single leaf stage + root stage
2. Leaf stage: LuceneScan + partial agg/filter/sort on each shard
3. GatherExchange: coordinator collects Pages from all data nodes
4. Root stage: final aggregation / merge-sort / limit on coordinator

### 9.2 Full Shuffle (Phase 2)

For joins, window functions, multi-stage aggregations:
1. Multiple stages connected by HashExchange/BroadcastExchange
2. Hash-partitioned aggregation: partial agg -> hash exchange -> final agg
3. Hash join: build side -> hash exchange -> hash table; probe side -> hash exchange -> probe
4. Window functions: hash exchange on partition key -> sort -> window evaluation

### 9.3 DSL Fallback

Feature flag `plugins.sql.distributed_engine.enabled` (default: true). When disabled, or for unsupported query patterns, falls back to existing DSL rewrite path. The existing Calcite integration and pushdown rules are completely preserved.

## 10. Security

### 10.1 Index-Level Access Control
Before acquiring `IndexSearcher` for a shard, validate permissions via security plugin's `PrivilegesEvaluator`. Apply DLS as Lucene `Query` filter at `LuceneFilterScan`. Apply FLS by restricting columns in `LuceneFullScan`.

### 10.2 Transport Security
Exchange uses existing TransportService with existing TLS configuration. No new network endpoints.

### 10.3 Resource Limits
MemoryPool + circuit breaker integration prevents OOM. Per-query memory limits. Cooperative scheduling enforces CPU time limits. Configurable max concurrent distributed queries.

### 10.4 Thread Context Propagation
OpenSearch's `ThreadContext` (containing security credentials, trace IDs) propagated across exchange operations and Driver thread boundaries.

## 11. Feature Flag and Rollout

Setting: `plugins.sql.distributed_engine.enabled` (dynamic, default: `true`)

Execution router in `QueryService.executeWithCalcite()`:
1. If disabled -> DSL path (existing)
2. If enabled -> attempt distributed execution
3. If distributed execution fails -> fallback to DSL path with warning log
4. If query pattern not supported by distributed engine -> DSL path

## 12. Module Structure

New Gradle module: `distributed-engine/`

```
distributed-engine/
  src/main/java/org/opensearch/sql/distributed/
    data/          Page, Block, BlockBuilder, PagesSerde
    operator/      Operator interface, ported Trino operators
    operator/join/ Ported join operators
    driver/        Driver, Pipeline, DriverFactory, context hierarchy
    memory/        MemoryPool, circuit breaker integration
    planner/       RelNodeToPlanNodeConverter, PlanNode hierarchy
    planner/plan/  AddExchanges, PlanFragmenter
    scheduler/     StageScheduler, ShardAssigner
    exchange/      GatherExchange, HashExchange, BroadcastExchange, OutputBuffer
    lucene/        LuceneFullScan, LuceneFilterScan, LuceneAggScan, LuceneSortScan
    transport/     TransportActions for shard query execution
```

Dependencies: `core/` (Calcite integration), `opensearch/` (OpenSearch client/storage), Trino source (Apache 2.0, ported classes).
