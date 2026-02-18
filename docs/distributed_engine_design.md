# Native Distributed Query Engine — Design Document

## Status: Phase 1 Implemented

## 1. Overview

The native distributed query engine executes PPL queries directly against Lucene segments via a Trino-inspired operator pipeline, bypassing the OpenSearch DSL query rewrite path. It uses scatter-gather execution across shards with cooperative driver-based scheduling.

### Goals

- Execute single-index PPL queries (filter, aggregation, sort, dedup, projection) through a native operator pipeline
- Read data directly from Lucene DocValues instead of going through OpenSearch's search/aggregation framework
- Preserve full backward compatibility — unsupported patterns fall back transparently to the existing DSL path
- Feature-flagged: `plugins.sql.distributed_engine.enabled` (default: `false`)

### Non-Goals (Phase 2+)

- Hash joins, broadcast joins
- Window functions (ROW_NUMBER, RANK, LAG/LEAD)
- Hash/Broadcast exchange (only Gather exchange in Phase 1)
- Spill-to-disk for large aggregations/sorts
- Multi-stage shuffle execution

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
│   AddExchanges: inserts GatherExchange for scatter-gather       │
│   PlanFragmenter: splits into leaf stage + root stage           │
│   StageScheduler: assigns leaf to shard nodes, root to coord    │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2: Physical Operators                                     │
│   Lucene-native: LuceneFullScan, LuceneFilterScan               │
│   Exchange: GatherExchange (TransportService-based)             │
│   Ported from Trino: HashAggregation, TopN, OrderBy,           │
│     FilterAndProject, MergeSortedPages                          │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1: Execution Runtime (Ported from Trino)                  │
│   Page/Block columnar data format                               │
│   Operator interface: isBlocked/needsInput/addInput/getOutput   │
│   Driver/Pipeline cooperative scheduling                        │
│   Context hierarchy, MemoryPool                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Query Execution Flow

```
PPL Query
  → PPLSyntaxParser → AST
  → CalciteRelNodeVisitor → Calcite LogicalRelNode
  → Volcano optimizer (logical rules)
  → Optimized LogicalRelNode
  → [NEW] RelNodeToPlanNodeConverter → PlanNode tree
  → [NEW] AddExchanges → GatherExchange inserted
  → [NEW] PlanFragmenter → leaf stage + root stage
  → [NEW] PlanNodeToOperatorConverter → OperatorFactory list
  → [NEW] Pipeline/Driver → cooperative execution
  → [NEW] LuceneFullScan reads DocValues → Page/Block output
  → [NEW] PageToResponseConverter → QueryResponse
```

### Scatter-Gather Mode

For single-index queries without joins:
1. Coordinator converts RelNode → PlanNode → leaf stage + root stage
2. Leaf stage: LuceneScan + partial agg/filter/sort on each shard
3. GatherExchange: coordinator collects Pages from data nodes via TransportService
4. Root stage: final aggregation / merge-sort / limit on coordinator

## 3. Module Structure

New Gradle module: `distributed-engine/`

```
distributed-engine/
  src/main/java/org/opensearch/sql/distributed/
    data/           Page, Block, BlockBuilder, PagesSerde
    operator/       Operator interface, FilterAndProject, HashAgg,
                    TopN, OrderBy, MergeSorted
    operator/aggregation/  GroupByHash, Accumulators (COUNT, SUM, AVG, MIN, MAX)
    driver/         Driver, Pipeline, DriverFactory, DriverYieldSignal
    memory/         MemoryPool
    context/        QueryContext, TaskContext, PipelineContext,
                    DriverContext, OperatorContext
    planner/        PlanNode hierarchy, RelNodeToPlanNodeConverter,
                    UnsupportedPatternException
    planner/plan/   AddExchanges, PlanFragmenter, StageFragment,
                    PartitioningScheme
    planner/bridge/ PlanNodeToOperatorConverter, RexPageFilterInterpreter,
                    RexPageProjectionFactory, AccumulatorFactory,
                    SortOrderConverter
    lucene/         LuceneFullScan, LuceneFilterScan,
                    DocValuesToBlockConverter, ColumnMapping
    exchange/       GatherExchange, OutputBuffer
    transport/      ShardQueryAction, ShardQueryRequest/Response
    scheduler/      StageScheduler, ShardAssigner, NodeAssignment
    metrics/        DistributedEngineMetrics
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

- Tracks memory per query via reserve()/free()
- Circuit breaker integration with OpenSearch's `CircuitBreakerService`

## 5. Layer 2: Physical Operators

### Lucene-Native Operators

| Operator | API | Description |
|----------|-----|-------------|
| `LuceneFullScan` | DocValues, LeafReaderContext | Reads all docs, converts DocValues → Block |
| `LuceneFilterScan` | Weight, Scorer, DocIdSetIterator | Pushes predicates to Lucene, filtered Pages |

DocValues type mapping:
- `NumericDocValues` / `SortedNumericDocValues` → `LongArrayBlock` / `DoubleArrayBlock`
- `SortedDocValues` / `SortedSetDocValues` → `VariableWidthBlock`
- `BinaryDocValues` → `VariableWidthBlock`

### Ported Trino Operators

| Operator | Source | Phase 1 Scope |
|----------|--------|---------------|
| `HashAggregationOperator` | Trino | Partial/final modes, in-memory, COUNT/SUM/AVG/MIN/MAX |
| `TopNOperator` | Trino | Heap-based top-K |
| `OrderByOperator` | Trino | Full in-memory sort |
| `FilterAndProjectOperator` | Trino | Combined filter+project on Page data |
| `MergeSortedPages` | Trino | K-way merge of pre-sorted streams |

### Exchange Operator

| Operator | Transport | Description |
|----------|-----------|-------------|
| `GatherExchange` | TransportService | Coordinator collects Pages from leaf stages |
| `OutputBuffer` | In-memory | Bounded Page buffer with backpressure |

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
  ├── ExchangeNode
  ├── RemoteSourceNode
  └── ValuesNode
```

### RelNodeToPlanNodeConverter

Visitor over Calcite's optimized RelNode tree:
- `LogicalFilter` → `FilterNode`
- `LogicalAggregate` → `AggregationNode` (with partial/final split)
- `LogicalTableScan` → `LuceneTableScanNode`
- `LogicalSort` → `SortNode` / `TopNNode`
- `LogicalProject` → `ProjectNode`
- PPL-specific: `DedupNode`, `RareTopNNode`, `TrendlineNode`
- Unsupported (`LogicalJoin`, `LogicalWindow`) → `UnsupportedPatternException` → DSL fallback

### PlanNode-to-Operator Bridge

The bridge layer converts planning structures to executable operators:

| Bridge Component | Converts |
|-----------------|----------|
| `PlanNodeToOperatorConverter` | PlanNode tree → OperatorFactory list |
| `RexPageFilterInterpreter` | Calcite RexNode → PageFilter (boolean mask) |
| `RexPageProjectionFactory` | Calcite RexNode list → PageProjection list |
| `AccumulatorFactory` | AggregateCall → Accumulator (COUNT, SUM, etc.) |
| `SortOrderConverter` | RelCollation → SortOrder + channel indices |

### AddExchanges & PlanFragmenter

- **AddExchanges**: Inserts `GatherExchange` for scatter-gather (Phase 1 only)
- **PlanFragmenter**: Cuts plan at ExchangeNode → leaf stage + root stage
- **StageScheduler**: Maps leaf to shard-holding nodes, root to coordinator

## 7. Execution Mode Router

In `QueryService.executeWithCalcite()`:

```java
if (isDistributedEngineEnabled() && distributedQueryExecutor != null) {
    try {
        distributedQueryExecutor.execute(calcitePlan, context, listener);
        return;
    } catch (UnsupportedOperationException e) {
        // Expected fallback: joins, windows, etc.
    } catch (Exception e) {
        if (isStrictModeEnabled()) {
            throw new StrictModeViolationException(...);
        }
        // Fallback to DSL with warning
    }
}
executionEngine.execute(calcitePlan, context, listener); // DSL path
```

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

The `distributed` section only appears when the engine is enabled and the pattern is supported.

## 10. Verification

### Strict Mode

With `strict_mode=true`, any supported pattern that fails distributed execution throws `StrictModeViolationException` instead of silently falling back. Only explicitly unsupported patterns (joins, windows) are allowed to fall back.

### Execution Counters

`DistributedEngineMetrics` tracks:
- `queries_total` — all queries attempted
- `queries_distributed` — executed through distributed engine
- `fallback_expected` — fell back due to unsupported pattern
- `fallback_error` — fell back due to unexpected error

## 11. Testing

| Test Suite | Tests | What It Validates |
|-----------|-------|-------------------|
| Layer 1 unit tests | 15 classes | Page/Block, Serde, Operator, Context, MemoryPool, Driver, Pipeline |
| Layer 2 unit tests (Trino) | 37 tests | FilterAndProject, HashAgg, TopN, OrderBy, MergeSorted |
| Layer 2 unit tests (Lucene) | 33 tests | LuceneFullScan, LuceneFilterScan, DocValues mapping |
| Layer 3 unit tests | 31 tests | Converter, AddExchanges, Fragmenter |
| Bridge unit tests | 38 tests | PlanNodeToOperator, RexFilter, RexProjection, AccumulatorFactory, SortOrder |
| Exchange unit tests | 28 tests | OutputBuffer, Transport, ShardQuery, Scheduler, Metrics |
| IC-1: Local execution | 22 tests | Single-node operator pipeline end-to-end |
| IC-2: Scatter-gather (strict) | 68 tests | Full distributed execution across 5 shards, strict mode ON |
| Manual sanity tests | 17 queries | Live cluster with strict mode, all query patterns |

## 12. Supported PPL Patterns

| Pattern | Example | Distributed? |
|---------|---------|-------------|
| Filter | `where age > 30` | Yes |
| Projection | `fields name, age` | Yes |
| Aggregation | `stats count() by dept` | Yes (partial/final) |
| Sort | `sort - salary` | Yes |
| TopN | `sort - salary \| head 10` | Yes |
| Dedup | `dedup dept` | Yes |
| Combined | `where age > 30 \| stats avg(salary) by dept \| sort - avg_sal` | Yes |
| Global agg | `stats count()` | Yes |
| Empty result | `where age > 999 \| stats count()` | Yes (returns 0) |
| Joins | `source=a \| join b` | No → DSL fallback |
| Window functions | ROW_NUMBER, RANK | No → DSL fallback |
| Subqueries | scalar, exists, IN | No → DSL fallback |

## 13. Phase 2 Roadmap

- Hash joins and broadcast joins
- Window functions
- Hash/Broadcast exchange types
- Spill-to-disk for large aggregations and sorts
- Multi-stage shuffle execution
- COUNT(DISTINCT) accumulator
- String MIN/MAX comparison
- Complex expression evaluation (LIKE, CASE, UDFs)
