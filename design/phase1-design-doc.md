# Phase 1 Design Document: Scatter-Gather MVP

## Status: APPROVED (extracted from v2 design doc)

## 1. Scope

Phase 1 delivers a native distributed query engine for **single-index, no-join PPL queries** executing through scatter-gather mode inside OpenSearch nodes. All existing PPL Calcite integration tests (~1,382 test methods across 116 test files) must pass with the distributed engine enabled and DSL fallback disabled.

**In scope:**
- PPL queries with filter, aggregation, sort, dedup, top/rare, trendline, fields projection
- Scatter-gather execution across shards
- Direct Lucene segment reads (no DSL rewrite)
- `_explain` API showing distributed plan
- Feature flag `plugins.sql.distributed_engine.enabled`
- DSL fallback for unsupported patterns (joins, window functions)

**Out of scope (deferred to Phase 2):**
- Hash joins, broadcast joins
- Window functions (ROW_NUMBER, RANK, LAG/LEAD)
- HashExchange, BroadcastExchange
- Spill-to-disk
- Multi-stage shuffle execution

## 2. Architecture

### 4-Layer Design (Phase 1 subset)

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
│   AddExchanges: inserts GatherExchange for scatter-gather       │
│   PlanFragmenter: splits into leaf stage + root stage           │
│   StageScheduler: assigns leaf stage to shard-holding nodes     │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2: Physical Operators                                     │
│   Lucene-native (SHARD_LOCAL):                                  │
│     LuceneFullScan, LuceneFilterScan                            │
│   Exchange:                                                     │
│     GatherExchange (TransportService-based)                     │
│   Generic (PORTED from Trino):                                  │
│     HashAggregationOperator, TopNOperator, OrderByOperator,     │
│     FilterAndProjectOperator, MergeSortedPages                  │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1: Trino Execution Runtime (Ported from Trino)            │
│   Driver/Pipeline cooperative scheduling                        │
│   Context: QueryContext -> TaskContext -> PipelineContext ->     │
│     DriverContext -> OperatorContext                             │
│   MemoryPool: integrated with OpenSearch circuit breakers       │
│   Page/Block: columnar data format (ported faithfully)          │
│   Operator interface: isBlocked/needsInput/addInput/getOutput   │
└─────────────────────────────────────────────────────────────────┘
```

### Query Execution Flow (Phase 1)

```
PPL Query
  -> PPLSyntaxParser -> AST
  -> CalciteRelNodeVisitor -> Calcite LogicalRelNode
  -> Volcano optimizer (logical rules only)
  -> Optimized LogicalRelNode
  -> [NEW] RelNodeToPlanNodeConverter:
     LogicalFilter    -> FilterNode
     LogicalAggregate -> AggregationNode (partial + exchange + final)
     LogicalTableScan -> LuceneTableScanNode
     LogicalSort      -> SortNode / TopNNode
     LogicalProject   -> ProjectNode
     PPL-specific     -> DedupNode, RareTopNNode, TrendlineNode
  -> [NEW] AddExchanges:
     inserts GatherExchange (scatter-gather only in Phase 1)
  -> [NEW] PlanFragmenter:
     splits into leaf stage + root stage
  -> [NEW] StageScheduler:
     assigns leaf stage to shard-holding nodes, root to coordinator
  -> [NEW] Execution via Driver/Pipeline/Operator/Page
```

### Scatter-Gather Mode

For single-index queries without joins:
1. Coordinator converts RelNode -> PlanNode -> leaf stage + root stage
2. Leaf stage: LuceneScan + partial agg/filter/sort on each shard
3. GatherExchange: coordinator collects Pages from all data nodes via TransportService
4. Root stage: final aggregation / merge-sort / limit on coordinator

### DSL Fallback

Feature flag: `plugins.sql.distributed_engine.enabled` (dynamic, default: `true`)

Execution router in `QueryService.executeWithCalcite()`:
1. If disabled -> DSL path (existing)
2. If enabled -> check if query pattern is supported by Phase 1 engine
3. If supported -> distributed execution
4. If unsupported (joins, window functions) -> DSL path
5. If distributed execution fails -> fallback to DSL with warning log

**Critical**: The fallback must be transparent. Users see the same results regardless of execution path.

## 3. Layer 4: Calcite Logical Optimization (Unchanged)

Existing infrastructure preserved entirely:
- `CalciteRelNodeVisitor` (3,694 lines): AST -> Calcite RelNode
- `CalciteRexNodeVisitor` (782 lines): expression translation
- Expression system (~22,900 lines): UDFs, type system, function resolution
- Volcano optimizer rules: join reordering, predicate pushdown, constant folding, aggregate splitting, projection pruning, subquery decorrelation
- Pushdown rules in `opensearch/.../planner/rules/` (~2,109 lines, 18 files): retained for DSL fallback path

**Integration point**: The optimized `RelNode` tree is the input to `RelNodeToPlanNodeConverter`.

## 4. Layer 3: Physical Planning

### 4.1 RelNodeToPlanNodeConverter

A visitor over Calcite's optimized LogicalRelNode tree that produces Trino-style PlanNode objects.

```java
class RelNodeToPlanNodeConverter {
    PlanNode convert(RelNode optimizedLogical, PlanningContext ctx);
    // LogicalFilter    -> FilterNode
    // LogicalAggregate -> AggregationNode (with partial/final split)
    // LogicalTableScan -> LuceneTableScanNode
    // LogicalSort      -> SortNode / TopNNode
    // LogicalProject   -> ProjectNode
    // PPL-specific: DedupNode, RareTopNNode, TrendlineNode
    // Unsupported (LogicalJoin, LogicalWindow) -> throws UnsupportedPatternException -> fallback
}
```

### 4.2 AddExchanges (Phase 1: GatherExchange only)

Adapted from `io.trino.sql.planner.optimizations.AddExchanges` (~1,700 LOC). Phase 1 scope:
- GatherExchange for scatter-gather (single-index filter/agg/sort queries)
- Single distribution for coordinator-only operations

Phase 2 will add hash-partition and broadcast.

### 4.3 PlanFragmenter

Adapted from `io.trino.sql.planner.PlanFragmenter` (~850 LOC). Cuts the plan at ExchangeNode boundaries:
- Phase 1: always produces exactly 2 fragments (leaf + root)

```java
class StageFragment {
    int stageId;
    PlanNode root;              // subtree for this stage
    PartitioningScheme output;  // how output is distributed
    List<RemoteSourceNode> inputs; // references to upstream stages
}
```

### 4.4 StageScheduler

Maps StageFragments to OpenSearch nodes using ClusterState:
- Leaf stage: assigned to nodes holding the relevant shards (data locality)
- Root stage: runs on the coordinating node

Uses OpenSearch's `RoutingTable` and `IndexShardRoutingTable` for shard-to-node mapping.

### 4.5 Execution Mode Router

In `QueryService.executeWithCalcite()`:
```java
if (!distributedEngineEnabled) {
    return executeDSLPath(relNode);
}
try {
    PlanNode plan = converter.convert(relNode, ctx);
    // ... fragment, schedule, execute
} catch (UnsupportedPatternException e) {
    log.debug("Falling back to DSL: {}", e.getMessage());
    return executeDSLPath(relNode);
} catch (Exception e) {
    log.warn("Distributed execution failed, falling back to DSL", e);
    return executeDSLPath(relNode);
}
```

## 5. Layer 2: Physical Operators

### 5.1 Lucene-Native Leaf Operators

| Operator | Lucene API | Description |
|----------|-----------|-------------|
| `LuceneFullScan` | `LeafReaderContext`, DocValues | Reads all documents, converts DocValues -> Block |
| `LuceneFilterScan` | `Weight`, `Scorer`, `DocIdSetIterator` | Pushes predicates to Lucene, produces filtered Pages |

DocValues map to Block types: `NumericDocValues` -> `LongBlock`/`DoubleBlock`, `SortedDocValues`/`BinaryDocValues` -> `BytesRefBlock`.

### 5.2 GatherExchange

| Operator | Transport | Description |
|----------|-----------|-------------|
| `GatherExchange` | TransportService RPC | Coordinator collects Pages from all leaf stages |

Uses OpenSearch's `TransportService` (existing TLS/SSL, existing transport port). Page serialization via `StreamOutput`/`StreamInput` using `PagesSerde`.

### 5.3 Generic Operators (ported from Trino)

| Operator | Trino Source | Phase 1 Scope |
|----------|-------------|---------------|
| `HashAggregationOperator` | `io.trino.operator.HashAggregationOperator` | Partial/final modes, in-memory only (no spill) |
| `TopNOperator` | `io.trino.operator.TopNOperator` | Heap-based top-K, memory-bounded |
| `OrderByOperator` | `io.trino.operator.OrderByOperator` | Full sort, in-memory only (no spill) |
| `FilterAndProjectOperator` | `io.trino.operator.project.FilterAndProjectOperator` | Combined filter+project on Page data |
| `MergeSortedPages` | `io.trino.operator.MergeSortedPages` | K-way merge of pre-sorted Page streams |

## 6. Layer 1: Execution Runtime

### 6.1 Page/Block Columnar Data Format

Ported faithfully from Trino's `io.trino.spi.Page` and `io.trino.spi.block`:

```
Page { Block[] blocks; int positionCount; }
Block (sealed): DictionaryBlock | RunLengthEncodedBlock | ValueBlock
ValueBlock types: LongArrayBlock, IntArrayBlock, ByteArrayBlock,
                  ShortArrayBlock, VariableWidthBlock (for VARCHAR/VARBINARY)
BlockBuilder: LongBlockBuilder, VariableWidthBlockBuilder, etc.
```

### 6.2 Operator Interface

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

### 6.3 Driver/Pipeline

- **Driver**: Cooperative multitasking loop. Moves Pages between adjacent operators until blocked or finished. One Driver per shard per pipeline.
- **Pipeline**: A chain of OperatorFactories that produces Drivers. One Pipeline per plan fragment per node.

Adapted for OpenSearch's `sql-worker` thread pool instead of Trino's dedicated executor.

### 6.4 Context Hierarchy (simplified)

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

### 6.5 MemoryPool

Tracks memory per query and per operator:
- **User memory**: non-revocable allocations (e.g., hash table entries)
- **Revocable memory**: Phase 2 (spill-to-disk)

Integration: registers `"sql_distributed_query"` circuit breaker. Configurable budget (default: 25% of heap). Blocks allocations when budget exceeded via `ListenableFuture`.

## 7. Security

### 7.1 Index-Level Access Control
Before acquiring `IndexSearcher` for a shard, validate permissions via security plugin's `PrivilegesEvaluator`. Apply DLS as Lucene `Query` filter at `LuceneFilterScan`. Apply FLS by restricting columns in `LuceneFullScan`.

### 7.2 Transport Security
Exchange uses existing TransportService with existing TLS configuration. No new network endpoints.

### 7.3 Thread Context Propagation
OpenSearch's `ThreadContext` (containing security credentials, trace IDs) propagated across exchange operations and Driver thread boundaries.

## 8. Module Structure

New Gradle module: `distributed-engine/`

```
distributed-engine/
  src/main/java/org/opensearch/sql/distributed/
    data/          Page, Block, BlockBuilder, PagesSerde
    operator/      Operator interface, FilterAndProject, HashAgg, TopN, OrderBy, MergeSorted
    driver/        Driver, Pipeline, DriverFactory, context hierarchy
    memory/        MemoryPool, circuit breaker integration
    planner/       RelNodeToPlanNodeConverter, PlanNode hierarchy
    planner/plan/  AddExchanges, PlanFragmenter
    scheduler/     StageScheduler, ShardAssigner
    exchange/      GatherExchange, OutputBuffer
    lucene/        LuceneFullScan, LuceneFilterScan
    transport/     ShardQueryAction, ShardQueryRequest/Response
```

## 9. Phase 1 Exit Criteria

1. **ALL existing PPL Calcite integration tests pass** (~1,382 tests across 116 files) with:
   - `plugins.sql.distributed_engine.enabled = true`
   - DSL fallback disabled for supported query patterns
2. Unsupported patterns (joins, window functions) gracefully fall back to DSL path
3. `_explain` API returns distributed plan information
4. No memory leaks after query completion
5. Feature flag toggle works correctly (enable/disable)
6. Security: DLS/FLS enforced with direct Lucene access
