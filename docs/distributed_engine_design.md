# Native Distributed Query Engine — Design Document

## Status: Phase 1 + Phase 2 Implemented

## 1. Overview

The native distributed query engine executes PPL queries directly against Lucene segments via a Trino-inspired operator pipeline, bypassing the OpenSearch DSL query rewrite path. It supports two execution modes: **scatter-gather** for single-index queries and **full shuffle** for joins, window functions, and multi-stage aggregations.

### Goals

- Execute PPL queries (filter, aggregation, sort, dedup, projection, joins, window functions) through a native operator pipeline
- Read data directly from Lucene DocValues instead of going through OpenSearch's search/aggregation framework
- Support distributed hash joins, broadcast joins, and window functions without DSL fallback
- Spill to disk under memory pressure for large aggregations, sorts, and joins
- Preserve full backward compatibility — unsupported patterns fall back transparently to the existing DSL path
- Feature-flagged: `plugins.sql.distributed_engine.enabled` (default: `false`)

## 2. Architecture

### 4-Layer Design

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4: Calcite Logical Optimization (UNCHANGED)               │
│   PPL → AST → CalciteRelNodeVisitor → LogicalRelNode            │
│   Volcano optimizer: predicate pushdown, constant folding,      │
│   aggregate splitting, projection pruning                       │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3: Physical Planning (Ported from Trino)                  │
│   RelNodeToPlanNodeConverter: Calcite RelNode → PlanNode        │
│   AddExchanges: inserts exchanges (Gather, Hash, Broadcast)     │
│   PlanFragmenter: splits plan at exchange boundaries → stages   │
│   StageScheduler: assigns stages to nodes by data locality      │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2: Physical Operators                                     │
│   Lucene-native:                                                │
│     LuceneFullScan, LuceneFilterScan, LuceneAggScan,           │
│     LuceneSortScan                                              │
│   Exchange:                                                     │
│     GatherExchange, HashExchange, BroadcastExchange             │
│   Ported from Trino:                                            │
│     HashAggregation, TopN, OrderBy, FilterAndProject,           │
│     MergeSortedPages, LookupJoinOperator, HashBuilderOperator,  │
│     WindowOperator                                              │
│   Spill-to-disk:                                                │
│     SpillableHashAggregationBuilder, SpillableOrderByOperator,  │
│     FileSingleStreamSpiller                                     │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1: Execution Runtime (Ported from Trino)                  │
│   Page/Block columnar data format                               │
│   Operator interface: isBlocked/needsInput/addInput/getOutput   │
│   Driver/Pipeline cooperative scheduling                        │
│   Context hierarchy, MemoryPool, memory revocation              │
└─────────────────────────────────────────────────────────────────┘
```

### Query Execution Flow

```
PPL Query
  → PPLSyntaxParser → AST
  → CalciteRelNodeVisitor → Calcite LogicalRelNode
  → Volcano optimizer (logical rules)
  → Optimized LogicalRelNode
  → RelNodeToPlanNodeConverter → PlanNode tree
  → AddExchanges → exchange nodes inserted (Gather, Hash, or Broadcast)
  → PlanFragmenter → multi-stage fragments
  → PlanNodeToOperatorConverter → OperatorFactory list per stage
  → Pipeline/Driver → cooperative execution
  → Lucene operators read DocValues → Page/Block output
  → PageToResponseConverter → QueryResponse
```

### Execution Modes

The engine automatically selects the execution mode based on the query pattern:

#### Scatter-Gather (single-index queries without joins/windows)
1. Coordinator converts RelNode → PlanNode → leaf stage + root stage
2. Leaf stage: LuceneScan + partial agg/filter/sort on each shard
3. GatherExchange: coordinator collects Pages from data nodes via TransportService
4. Root stage: final aggregation / merge-sort / limit on coordinator

#### Full Shuffle (joins, windows, multi-stage aggregation)

**Hash Join:**
```
Build side: LuceneScan → HashExchange(join_key) → HashBuilderOperator (build hash table)
Probe side: LuceneScan → HashExchange(join_key) → LookupJoinOperator (probe hash table)
Result: joined Pages → GatherExchange → coordinator
```

**Broadcast Join (small table):**
```
Small side: LuceneScan → GatherExchange → BroadcastExchange (to all nodes)
Large side: LuceneScan (local) → LookupJoinOperator (probe broadcasted hash table)
```

**Distributed Aggregation (high cardinality):**
```
LuceneScan → partial HashAgg → HashExchange(group_key) → final HashAgg → GatherExchange
```

**Window Functions:**
```
LuceneScan → HashExchange(partition_key) → sort(order_key) → WindowOperator → GatherExchange
```

## 3. Module Structure

Gradle module: `distributed-engine/`

```
distributed-engine/
  src/main/java/org/opensearch/sql/distributed/
    data/               Page, Block, BlockBuilder, PagesSerde, PagesSerdeFactory
    operator/           Operator, SourceOperator, SinkOperator, OperatorFactory,
                        FilterAndProjectOperator, HashAggregationOperator,
                        TopNOperator, OrderByOperator, MergeSortedPages,
                        PagesIndex, SortOrder, PageFilter, PageProjection
    operator/aggregation/  GroupByHash, Accumulator, CountAccumulator,
                        SumAccumulator, AvgAccumulator, MinAccumulator,
                        MaxAccumulator, BlockUtils
    operator/join/      LookupJoinOperator, LookupJoinOperatorFactory,
                        HashBuilderOperator, JoinHash, PagesHash,
                        JoinProbe, JoinType
    operator/window/    WindowOperator, WindowFunction, PartitionData,
                        RowNumberFunction, RankFunction, DenseRankFunction,
                        LagFunction, LeadFunction, AggregateWindowFunction
    driver/             Driver, Pipeline, DriverFactory, DriverYieldSignal
    memory/             MemoryPool, MemoryPoolListener
    context/            QueryContext, TaskContext, PipelineContext,
                        DriverContext, OperatorContext
    planner/            PlanNode, PlanNodeId, PlanNodeVisitor,
                        FilterNode, ProjectNode, AggregationNode,
                        SortNode, TopNNode, LimitNode, DedupNode,
                        LuceneTableScanNode, ExchangeNode, RemoteSourceNode,
                        ValuesNode, JoinNode, WindowNode,
                        UnsupportedPatternException
    planner/plan/       AddExchanges, PlanFragmenter, StageFragment,
                        PartitioningScheme
    planner/bridge/     PlanNodeToOperatorConverter, RelNodeToPlanNodeConverter,
                        RexPageFilterInterpreter, RexPageProjectionFactory,
                        AccumulatorFactory, SortOrderConverter
    lucene/             LuceneFullScan, LuceneFilterScan, LuceneAggScan,
                        LuceneSortScan, DocValuesToBlockConverter,
                        DocValuesAccumulator, LuceneSortFieldConverter,
                        ColumnMapping
    exchange/           GatherExchange, HashExchange, HashExchangeSourceOperator,
                        HashPartitioner, BroadcastExchange, OutputBuffer
    spill/              FileSingleStreamSpiller, SpillerFactory,
                        SpillableHashAggregationBuilder,
                        SpillableOrderByOperator, MemoryRevocableOperator,
                        SpillMetrics
    transport/          ShardQueryAction, TransportShardQueryAction,
                        ShardQueryRequest, ShardQueryResponse
    scheduler/          StageScheduler, StageExecution, NodeAssignment,
                        ShardAssigner
    explain/            DistributedPlanExplainer
    metrics/            DistributedEngineMetrics
    DistributedQueryExecutorImpl   (entry point)
    PageToResponseConverter        (output conversion)
```

## 4. Layer 1: Execution Runtime

### Page/Block Columnar Data

Ported from Trino's `io.trino.spi.Page` and `io.trino.spi.block`:

```
Page { Block[] blocks; int positionCount; }
Block (sealed): ValueBlock | DictionaryBlock | RunLengthEncodedBlock
ValueBlock types: LongArrayBlock, IntArrayBlock, DoubleArrayBlock,
                  ByteArrayBlock, ShortArrayBlock, BooleanArrayBlock,
                  VariableWidthBlock (VARCHAR/VARBINARY)
BlockBuilder: type-specific builders for incremental construction
```

### Operator Interface

```java
interface Operator extends AutoCloseable {
    ListenableFuture<?> isBlocked();
    boolean needsInput();
    void addInput(Page page);
    Page getOutput();
    void finish();
    boolean isFinished();
    OperatorContext getOperatorContext();
}
```

Subtypes: `SourceOperator` (no input), `SinkOperator` (no output).

### Driver/Pipeline

- **Driver**: Cooperative multitasking loop. Moves Pages between adjacent operators until blocked or finished.
- **Pipeline**: Chain of OperatorFactories that produces Drivers.
- Adapted for OpenSearch's `sql-worker` thread pool.

### Context Hierarchy

```
QueryContext (1 per query)  → MemoryPool reference, timeout
  TaskContext (1 per node)  → resource tracking
    PipelineContext          → pipeline lifecycle
      DriverContext          → thread binding
        OperatorContext      → per-operator memory tracking
```

### MemoryPool

- Tracks memory per query via `reserve()`/`free()` for non-revocable memory
- `reserveRevocable()`/`freeRevocable()` for spillable buffers
- Circuit breaker integration with OpenSearch's `CircuitBreakerService`
- Memory pressure triggers cooperative revocation (see section 10)

## 5. Layer 2: Physical Operators

### Lucene-Native Operators

| Operator | API | Description |
|----------|-----|-------------|
| `LuceneFullScan` | DocValues, LeafReaderContext | Reads all docs, converts DocValues → Block |
| `LuceneFilterScan` | Weight, Scorer, DocIdSetIterator | Pushes predicates to Lucene, filtered Pages |
| `LuceneAggScan` | Collector/LeafCollector, DocValues | Aggregation directly on DocValues — faster than scan + agg for simple aggregates (COUNT, SUM, MIN, MAX) with optional GROUP BY |
| `LuceneSortScan` | IndexSearcher.search(Query, int, Sort) | Sorted top-K with early termination — faster than full scan + sort when K << total docs |

DocValues type mapping:
- `NumericDocValues` / `SortedNumericDocValues` → `LongArrayBlock` / `DoubleArrayBlock`
- `SortedDocValues` / `SortedSetDocValues` → `VariableWidthBlock`
- `BinaryDocValues` → `VariableWidthBlock`

### Ported Trino Operators

| Operator | Description |
|----------|-------------|
| `HashAggregationOperator` | Partial/final modes, in-memory, COUNT/SUM/AVG/MIN/MAX. GroupByHash for group keys |
| `TopNOperator` | Heap-based top-K |
| `OrderByOperator` | Full in-memory sort via PagesIndex |
| `FilterAndProjectOperator` | Combined filter+project on Page data |
| `MergeSortedPages` | K-way merge of pre-sorted streams |

### Join Operators (Ported from Trino)

| Operator | Description |
|----------|-------------|
| `HashBuilderOperator` | Builds JoinHash from build-side Pages. Memory-tracked via OperatorContext |
| `LookupJoinOperator` | Probe-side join operator. Supports INNER, LEFT, RIGHT, FULL, SEMI, ANTI join types |
| `JoinHash` / `PagesHash` | Hash table for join lookups. Multi-column key, null-safe, murmur3 hashing |
| `JoinProbe` | Probes hash table for matching build rows |

Join lifecycle:
1. `HashBuilderOperator` consumes all build-side Pages, constructs JoinHash
2. `LookupJoinOperator` receives probe-side Pages, probes against JoinHash
3. Output combines probe + build columns based on join type
4. For RIGHT/FULL: unmatched build rows emitted at finish

### Window Operator (Ported from Trino)

| Component | Description |
|-----------|-------------|
| `WindowOperator` | Accumulates pages, partitions by key, computes window functions |
| `PartitionData` | Flattened partition data from multiple pages |
| `RowNumberFunction` | ROW_NUMBER() — sequential numbering within partition |
| `RankFunction` | RANK() — rank with ties |
| `DenseRankFunction` | DENSE_RANK() — dense rank with no gaps |
| `LagFunction` / `LeadFunction` | LAG/LEAD with configurable offset |
| `AggregateWindowFunction` | SUM/AVG/MIN/MAX/COUNT OVER with ROWS frame |

### Exchange Operators

| Operator | Transport | Description |
|----------|-----------|-------------|
| `GatherExchange` | TransportService | Coordinator collects Pages from all leaf stages |
| `HashExchange` | TransportService | Hash-partitions Pages by key columns via murmur3, routes to target nodes |
| `HashExchangeSourceOperator` | In-memory | Receiving side of hash exchange, reads from partitioned buffer |
| `HashPartitioner` | — | Computes partition assignment for each row |
| `BroadcastExchange` | TransportService | Sends full copy of Pages to all participating nodes. Memory-bounded |
| `OutputBuffer` | In-memory | Bounded Page buffer with backpressure (default 8 MB) |

## 6. Layer 3: Physical Planning

### PlanNode Hierarchy

```
PlanNode (abstract)
  ├── FilterNode
  ├── ProjectNode
  ├── AggregationNode (PARTIAL / FINAL / SINGLE)
  ├── SortNode
  ├── TopNNode
  ├── LimitNode
  ├── LuceneTableScanNode
  ├── DedupNode
  ├── ExchangeNode (GATHER / HASH / BROADCAST)
  ├── RemoteSourceNode
  ├── JoinNode (INNER / LEFT / RIGHT / FULL / SEMI / ANTI)
  ├── WindowNode
  └── ValuesNode
```

### RelNodeToPlanNodeConverter

Visitor over Calcite's optimized RelNode tree:
- `LogicalFilter` → `FilterNode`
- `LogicalAggregate` → `AggregationNode` (with partial/final split)
- `LogicalTableScan` → `LuceneTableScanNode`
- `LogicalSort` → `SortNode` / `TopNNode`
- `LogicalProject` → `ProjectNode`
- `LogicalJoin` → `JoinNode` (equi-join key extraction)
- `LogicalWindow` → `WindowNode`
- PPL-specific: `DedupNode`, `RareTopNNode`, `TrendlineNode`
- Unsupported patterns → `UnsupportedPatternException` → DSL fallback

### PlanNode-to-Operator Bridge

The bridge layer converts planning structures to executable operators:

| Bridge Component | Converts |
|-----------------|----------|
| `PlanNodeToOperatorConverter` | PlanNode tree → OperatorFactory list |
| `RexPageFilterInterpreter` | Calcite RexNode → PageFilter (boolean mask) |
| `RexPageProjectionFactory` | Calcite RexNode list → PageProjection list |
| `AccumulatorFactory` | AggregateCall → Accumulator (COUNT, SUM, etc.) |
| `SortOrderConverter` | RelCollation → SortOrder + channel indices |

The converter handles join nodes via `JoinBridgeFactory` which:
1. Converts the build (right) subtree into operator factories
2. At operator creation time, runs the build pipeline to completion
3. Extracts the `JoinHash` and wires it into `LookupJoinOperator`

Window nodes produce `WindowOperatorFactory` directly from the planner node.

### AddExchanges

Inserts exchange nodes based on query pattern:

| Pattern | Exchange Strategy |
|---------|-------------------|
| Single-index filter/agg/sort | `GatherExchange` (scatter-gather) |
| Join (small build side) | `BroadcastExchange` on build side |
| Join (large build side) | `HashExchange` on both sides, partitioned by join key |
| Window function | `HashExchange` on partition-by columns; `GatherExchange` if no partition-by |
| High-cardinality aggregation (>2 group columns) | PARTIAL → `HashExchange`(group_keys) → FINAL → `GatherExchange` |
| Aggregation (low cardinality) | PARTIAL on leaf → `GatherExchange` → FINAL on coordinator |

### PlanFragmenter & StageScheduler

- **PlanFragmenter**: Cuts plan tree at ExchangeNode boundaries to create StageFragments. Supports multi-level exchanges for shuffle plans (not just leaf + root).
- **StageScheduler**: Maps stages to cluster nodes using ClusterState and RoutingTable for data locality.

### PartitioningScheme

Describes how data is distributed across nodes in a stage:

| Partitioning | Description |
|-------------|-------------|
| `SOURCE_DISTRIBUTED` | Data stays on shard-holding nodes (leaf stages) |
| `COORDINATOR_ONLY` | All data gathered to coordinator (GatherExchange output) |
| `HASH_DISTRIBUTED` | Data hash-partitioned by key columns across nodes |
| `BROADCAST` | Data replicated to all participating nodes |
| `SINGLE` | Single node execution |

## 7. Execution Mode Router

In `QueryService.executeWithCalcite()`:

```java
if (isDistributedEngineEnabled() && distributedQueryExecutor != null) {
    try {
        distributedQueryExecutor.execute(calcitePlan, context, listener);
        return;
    } catch (UnsupportedOperationException e) {
        // Expected fallback: unsupported pattern
    } catch (Exception e) {
        if (isStrictModeEnabled()) {
            throw new StrictModeViolationException(...);
        }
        // Fallback to DSL with warning
    }
}
executionEngine.execute(calcitePlan, context, listener); // DSL path
```

The execution mode (scatter-gather vs. full shuffle) is selected automatically by `AddExchanges` based on the query plan structure. There is no manual toggle between modes.

### Feature Flags

| Setting | Default | Description |
|---------|---------|-------------|
| `plugins.sql.distributed_engine.enabled` | `false` | Enable distributed execution |
| `plugins.sql.distributed_engine.strict_mode` | `false` | Fail instead of silent fallback for supported patterns |

## 8. Plugin Wiring

The distributed engine is wired into the plugin via Guice dependency injection:

1. `plugin/build.gradle` depends on `distributed-engine` module
2. `TransportPPLQueryAction` creates `NodeLocalSourceProvider` (bridges to real Lucene IndexSearcher via `IndicesService`)
3. `DistributedQueryExecutorImpl` is instantiated with the source provider and set on `QueryService`

```java
// In TransportPPLQueryAction
NodeLocalSourceProvider sourceProvider =
    new NodeLocalSourceProvider(indicesService, clusterService);
queryService.setDistributedQueryExecutor(
    new DistributedQueryExecutorImpl(sourceProvider));
```

## 9. Explain API

The `_explain` API returns both plans when the distributed engine is enabled:

```json
{
  "calcite": {
    "logical": "LogicalFilter → CalciteLogicalIndexScan",
    "physical": "CalciteEnumerableIndexScan (DSL pushdown)"
  },
  "distributed": {
    "plan": "FilterNode → LuceneTableScanNode",
    "stages": [
      "Stage 0 (leaf): FilterNode → LuceneTableScanNode",
      "Stage 1 (root): RemoteSourceNode [GATHER]"
    ],
    "engine": "distributed"
  }
}
```

For multi-stage shuffle plans (joins, windows):

```json
{
  "distributed": {
    "plan": "LookupJoinOperator → HashExchange(join_key)",
    "stages": [
      "Stage 0 (leaf-left): LuceneTableScanNode [orders]",
      "Stage 1 (leaf-right): LuceneTableScanNode [departments]",
      "Stage 2 (shuffle): HashExchange(dept_id) → LookupJoinOperator",
      "Stage 3 (root): RemoteSourceNode [GATHER]"
    ],
    "engine": "distributed"
  }
}
```

The `distributed` section only appears when the engine is enabled and the pattern is supported.

## 10. Spill-to-Disk

### Overview

When queries exceed available memory, operators spill intermediate data to local temp files and read it back during final processing. Spill is cooperative — the `MemoryPool` signals pressure, and operators decide when and how to spill.

### Components

| Component | Description |
|-----------|-------------|
| `FileSingleStreamSpiller` | Serializes Pages to temp files via `PagesSerde`, reads back during merge. Auto-cleanup on `close()` |
| `SpillerFactory` | Creates spiller instances with configurable spill directory (default: `java.io.tmpdir`) |
| `SpillableHashAggregationBuilder` | Wraps `HashAggregationOperator` — on memory pressure, flushes hash table to spill file, rebuilds empty, continues. On finish, k-way merge of all spill files |
| `SpillableOrderByOperator` | External sort — sorts runs in memory, spills to disk, k-way merge via priority queue |
| `MemoryRevocableOperator` | Interface for cooperative memory revocation protocol |
| `SpillMetrics` | Tracks spill counts and bytes |

### Memory Revocation Protocol

```
1. Operator reserves revocable memory via MemoryPool.reserveRevocable()
2. MemoryPool detects pressure (revocable memory > threshold)
3. MemoryPool notifies MemoryPoolListener
4. Operator.startMemoryRevoke() called → begin spilling to disk
5. Operator.finishMemoryRevoke() called → memory freed
6. MemoryPool.freeRevocable() releases memory back to pool
```

## 11. Verification

### Strict Mode

With `strict_mode=true`, any supported pattern that fails distributed execution throws `StrictModeViolationException` instead of silently falling back. Only explicitly unsupported patterns (subqueries, etc.) are allowed to fall back.

### Execution Counters

`DistributedEngineMetrics` tracks:
- `queries_total` — all queries attempted
- `queries_distributed` — executed through distributed engine
- `fallback_expected` — fell back due to unsupported pattern
- `fallback_error` — fell back due to unexpected error

## 12. Testing

### Unit Tests

| Test Suite | Tests | What It Validates |
|-----------|-------|-------------------|
| Layer 1: Runtime | 15 classes | Page/Block, Serde, Operator, Context, MemoryPool, Driver, Pipeline |
| Layer 2: Trino operators | 37 tests | FilterAndProject, HashAgg, TopN, OrderBy, MergeSorted |
| Layer 2: Lucene operators | 33 tests | LuceneFullScan, LuceneFilterScan, DocValues mapping |
| Layer 2: Join operators | 39 tests | LookupJoinOperator (all 6 types), HashBuilderOperator, JoinHash |
| Layer 2: Window operator | 16 tests | ROW_NUMBER, RANK, DENSE_RANK, LAG/LEAD, SUM OVER |
| Layer 2: Exchange | 26 tests | HashExchange partitioning/skew/null keys, BroadcastExchange |
| Layer 2: Lucene advanced | 16 tests | LuceneAggScan, LuceneSortScan |
| Layer 2: Spill | 8 tests | FileSingleStreamSpiller lifecycle, cleanup |
| Layer 3: Planning | 31 tests | Converter, AddExchanges, Fragmenter |
| Bridge | 38 tests | PlanNodeToOperator, RexFilter, RexProjection, AccumulatorFactory, SortOrder |
| Exchange + transport | 28 tests | OutputBuffer, Transport, ShardQuery, Scheduler, Metrics |
| IC-1: Local execution | 22 tests | Single-node operator pipeline end-to-end |
| IC-2: Scatter-gather (strict) | 68 tests | Full distributed execution across 5 shards, strict mode ON |

### Integration Tests

| Test Suite | Tests | What It Validates |
|-----------|-------|-------------------|
| Scatter-gather ITs | 10 classes | Filter, aggregation, sort/limit, explain, concurrent, memory, DSL comparison, feature flag |
| Shuffle ITs | 6 classes | ShuffleJoinIT, ShuffleWindowIT, ShuffleMultiStageAggIT, ShuffleExplainIT, ShuffleMemoryPressureIT, ShuffleConcurrentIT |

## 13. Supported PPL Patterns

| Pattern | Example | Distributed? |
|---------|---------|-------------|
| Filter | `where age > 30` | Yes (scatter-gather) |
| Projection | `fields name, age` | Yes (scatter-gather) |
| Aggregation | `stats count() by dept` | Yes (scatter-gather, partial/final) |
| Sort | `sort - salary` | Yes (scatter-gather) |
| TopN | `sort - salary \| head 10` | Yes (scatter-gather) |
| Dedup | `dedup dept` | Yes (scatter-gather) |
| Combined | `where age > 30 \| stats avg(salary) by dept \| sort - avg_sal` | Yes (scatter-gather) |
| Global agg | `stats count()` | Yes (scatter-gather) |
| Empty result | `where age > 999 \| stats count()` | Yes (returns 0) |
| Inner join | `source=orders \| join departments on dept_id` | Yes (hash or broadcast shuffle) |
| Left/Right join | `source=a \| left join b on key` | Yes (hash shuffle) |
| Full outer join | `source=a \| full join b on key` | Yes (hash shuffle) |
| Semi/Anti join | Semi and anti join patterns | Yes (hash shuffle) |
| Window functions | ROW_NUMBER, RANK, DENSE_RANK | Yes (hash shuffle) |
| LAG/LEAD | `LAG(col, offset)`, `LEAD(col, offset)` | Yes (hash shuffle) |
| Aggregate OVER | `SUM(col) OVER (PARTITION BY ...)` | Yes (hash shuffle) |
| High-cardinality agg | GROUP BY with many group columns | Yes (hash shuffle, PARTIAL → HASH → FINAL) |
| Subqueries | scalar, exists, IN | No → DSL fallback |

## 14. Future Roadmap

- COUNT(DISTINCT) accumulator
- String MIN/MAX comparison
- Complex expression evaluation (LIKE, CASE, UDFs)
- Join spill-to-disk (large build side exceeds memory)
- Adaptive join strategy (runtime size-based broadcast/hash decision)
- Range-based partitioning for skewed data
- DLS/FLS security enforcement with direct Lucene access
- Multi-index scatter-gather without shuffle
