# Phase 1 Test Plan: Scatter-Gather MVP

## Overview

Testing is organized into 5 stages with 3 integration checkpoints. Each stage validates a specific layer. Integration checkpoints verify cross-layer correctness. The final checkpoint (IC-3) runs ALL existing PPL Calcite integration tests with the distributed engine enabled.

```
Stage 0: Foundation ──> Stage 1: Layer 1 ──> Stage 2: Layer 2 ──> IC-1 (Local)
                                                                     │
                        Stage 3: Layer 3 ──> Stage 4: Exchange ──> IC-2 (Scatter-Gather)
                                                                     │
                                                                   IC-3 (PPL Calcite IT Regression)
                                                                     │
                                                                PHASE 1 COMPLETE
```

---

## Stage 0: Foundation Tests

**Scope**: Page/Block data types, Operator interface contract, serialization.
**Gate**: All Stage 0 tests pass before any other stage begins.

### S0.1 Page/Block Unit Tests
| Test | Description | Assertions |
|------|-------------|------------|
| `LongBlockTest` | Create LongBlock from array, verify get/isNull/positionCount | Values match, null positions correct |
| `DoubleBlockTest` | Create DoubleBlock, verify precision handling | IEEE 754 edge cases (NaN, Inf, -0) |
| `BytesRefBlockTest` | Create variable-width block, verify getString | UTF-8 encoding, empty strings, large values |
| `BooleanBlockTest` | Create boolean block | True/false/null combinations |
| `DictionaryBlockTest` | Create dictionary-encoded block, verify transparent access | Logical values match, memory savings verified |
| `RunLengthEncodedBlockTest` | Create RLE block for repeated values | All positions return same value |
| `BlockBuilderTest` | Build blocks incrementally via append | Builder produces correct Block |
| `PageTest` | Create Page from multiple Blocks | positionCount consistent, getBlock returns correct column |
| `PageRegionTest` | getRegion/getColumns on Page | Subset extraction correct |

### S0.2 Page Serialization Tests
| Test | Description | Assertions |
|------|-------------|------------|
| `PagesSerdeRoundTripTest` | Serialize Page -> bytes -> deserialize | All values identical after round-trip |
| `PagesSerdeNullHandlingTest` | Serialize Pages with null positions | Nulls preserved correctly |
| `PagesSerdeEmptyPageTest` | Serialize empty Page (0 positions) | Deserializes to empty Page |
| `PagesSerdeAllTypesTest` | Page with every Block type | All types survive round-trip |
| `PagesSerdeLargePageTest` | Page with 100K+ positions | Correct and within memory bounds |

### S0.3 Operator Interface Contract Tests
| Test | Description | Assertions |
|------|-------------|------------|
| `OperatorLifecycleTest` | Verify finish()/isFinished() contract | isFinished()=true after finish() + last getOutput() |
| `OperatorBlockingTest` | Verify isBlocked() returns resolved future when not blocked | Future is done when operator can proceed |
| `OperatorNeedsInputTest` | Verify needsInput() contract with addInput()/getOutput() | Cannot addInput when needsInput()=false |

**Run command**: `./gradlew :distributed-engine:test --tests '*Block*' --tests '*Page*' --tests '*Operator*Lifecycle*'`

---

## Stage 1: Layer 1 Component Tests — Execution Runtime

**Scope**: Context hierarchy, Driver, Pipeline, MemoryPool.
**Prerequisite**: Stage 0 passes.

### S1.1 Context Hierarchy Tests
| Test | Description | Assertions |
|------|-------------|------------|
| `QueryContextTest` | Create context, verify memory pool binding | MemoryPool reference correct |
| `TaskContextTest` | Create under QueryContext, verify parent linkage | Parent/child hierarchy intact |
| `PipelineContextTest` | Create under TaskContext | Stats collection works |
| `DriverContextTest` | Create under PipelineContext | Thread binding correct |
| `OperatorContextTest` | Create under DriverContext, allocate memory | Memory tracked in MemoryPool |
| `ContextHierarchyCleanupTest` | Close QueryContext, verify cascade cleanup | All child contexts released |

### S1.2 MemoryPool Tests
| Test | Description | Assertions |
|------|-------------|------------|
| `MemoryPoolReserveTest` | Reserve user memory, check available | Available decreases by reserved amount |
| `MemoryPoolFreeTest` | Reserve then free | Available returns to original |
| `MemoryPoolBlockingTest` | Reserve up to limit, verify next reserve blocks | Returns unresolved ListenableFuture |
| `MemoryPoolRevocableTest` | Reserve revocable memory, trigger revocation | Revocable memory freed, future resolved |
| `MemoryPoolCircuitBreakerTest` | Verify circuit breaker integration | OS circuit breaker trips at configured threshold |
| `MemoryPoolConcurrencyTest` | Multi-threaded reserve/free | No lost updates, totals consistent |

### S1.3 Driver Tests
| Test | Description | Assertions |
|------|-------------|------------|
| `DriverBasicTest` | Create Driver with source + sink, process Pages | Pages flow from source to sink |
| `DriverCooperativeTest` | Driver yields after processing quantum | Does not monopolize thread |
| `DriverBlockedTest` | Driver with blocked operator | Driver yields, resumes when unblocked |
| `DriverFinishTest` | Driver processes all input, finishes | All operators finish() called, isFinished()=true |
| `DriverMemoryRevocationTest` | Trigger memory pressure during processing | Driver pauses, revokes, resumes |

### S1.4 Pipeline Tests
| Test | Description | Assertions |
|------|-------------|------------|
| `PipelineCreationTest` | Create Pipeline from OperatorFactories | Correct number of Drivers created |
| `PipelineLifecycleTest` | Start, process, finish pipeline | All Drivers complete |
| `PipelineParallelDriversTest` | Pipeline with N Drivers (1 per shard) | All Drivers execute concurrently |

**Run command**: `./gradlew :distributed-engine:test --tests '*Context*' --tests '*MemoryPool*' --tests '*Driver*' --tests '*Pipeline*'`

---

## Stage 2: Layer 2 Component Tests — Physical Operators

**Scope**: Each operator tested in isolation with synthetic Pages.
**Prerequisite**: Stage 1 passes.

### S2.1 Ported Trino Operator Tests

| Test Suite | Operator | Key Test Cases |
|-----------|----------|----------------|
| `FilterAndProjectOperatorTest` | FilterAndProjectOperator | Filter true/false/null, project column subset, project with expression, empty page input |
| `HashAggregationOperatorTest` | HashAggregationOperator | Single group key, multi-column key, null keys, high-cardinality groups, COUNT/SUM/AVG/MIN/MAX, partial mode, final mode |
| `TopNOperatorTest` | TopNOperator | Top-5 from 1M rows, multi-column sort, null ordering (FIRST/LAST), ties handling, N > input size |
| `OrderByOperatorTest` | OrderByOperator | Single column sort, multi-column, ASC/DESC, null ordering, large input requiring multiple Pages |
| `MergeSortedPagesTest` | MergeSortedPages | Merge 2 streams, merge 10 streams, empty streams, single-element streams, all-duplicates |

### S2.2 Lucene Leaf Operator Tests

Require an embedded OpenSearch index with known test data.

| Test Suite | Operator | Key Test Cases |
|-----------|----------|----------------|
| `LuceneFullScanTest` | LuceneFullScan | Scan all docs from single segment, multi-segment, verify Page/Block content matches documents |
| `LuceneFilterScanTest` | LuceneFilterScan | Term query, range query, boolean (AND/OR/NOT), verify filtered Pages match expected docs |

### S2.3 Type Mapping Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `DocValuesToBlockTest` | NumericDocValues -> LongBlock/DoubleBlock | Values exact match |
| `SortedDocValuesToBlockTest` | SortedDocValues -> BytesRefBlock | String values correct |
| `MultiValuedFieldTest` | Multi-valued DocValues | Proper handling (explode or array block) |
| `NullFieldHandlingTest` | Missing fields in documents | Null positions set correctly in Block |
| `DateFieldMappingTest` | Date field -> LongBlock (epoch millis) | Date values preserved |
| `IpFieldMappingTest` | IP field -> BytesRefBlock | IP address encoding correct |

**Run command**: `./gradlew :distributed-engine:test --tests '*OperatorTest' --tests '*Lucene*Test' --tests '*DocValues*'`

---

## Integration Checkpoint 1: Local Single-Node Execution

**Gate**: Pipeline executes a complete operator chain on a single node without exchange.

| IC-1 Test | Description | Pass Criteria |
|-----------|-------------|---------------|
| `LocalFilterProjectTest` | PPL: `source=test \| where age > 30 \| fields name, age` | LuceneFilterScan -> FilterAndProject in single Pipeline. Results match DSL path. |
| `LocalAggregationTest` | PPL: `source=test \| stats count() by status` | LuceneFullScan -> HashAggregation (partial+final). Results match DSL path. |
| `LocalSortLimitTest` | PPL: `source=test \| sort age \| head 10` | LuceneFullScan -> OrderBy -> Limit (or TopN). Results match DSL path. |
| `LocalMemoryTrackingTest` | Run query, verify memory pool accounting | All memory allocated is freed after query completes. Circuit breaker not tripped. |
| `LocalDriverLifecycleTest` | Run query, verify all operators open/close | No resource leaks. All operators finish cleanly. |

**Run command**: `./gradlew :distributed-engine:test --tests '*LocalExecution*'`

**Decision point**: If IC-1 fails, do NOT proceed to Stage 3 or Stage 4. Fix Layer 1/2 issues first.

---

## Stage 3: Layer 3 Component Tests — Physical Planning

**Scope**: RelNodeToPlanNodeConverter, AddExchanges, PlanFragmenter, StageScheduler.
**Prerequisite**: IC-1 passes.

### S3.1 RelNodeToPlanNodeConverter Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `ConvertFilterTest` | LogicalFilter -> FilterNode | Predicate preserved |
| `ConvertProjectTest` | LogicalProject -> ProjectNode | Column list preserved |
| `ConvertAggregateTest` | LogicalAggregate -> AggregationNode | Group keys and agg functions preserved, partial/final split inserted |
| `ConvertSortTest` | LogicalSort -> SortNode or TopNNode | Sort keys, direction, limit preserved |
| `ConvertTableScanTest` | LogicalTableScan -> LuceneTableScanNode | Index name, projected columns preserved |
| `ConvertComplexPlanTest` | Multi-level RelNode tree | Complete tree structure preserved |
| `ConvertPPLDedupTest` | PPL dedup RelNode -> DedupNode | PPL-specific semantics preserved |
| `ConvertUnsupportedTest` | LogicalJoin -> throws UnsupportedPatternException | Triggers DSL fallback |

### S3.2 AddExchanges Tests (Phase 1: GatherExchange only)

| Test | Description | Assertions |
|------|-------------|------------|
| `ExchangeForAggregateTest` | AggregationNode -> partial + GatherExchange + final | Exchange inserted at correct position |
| `ExchangeForSortTest` | SortNode -> local sort + GatherExchange + merge | Correct exchange type |
| `NoExchangeSimpleFilterTest` | Simple filter+project -> GatherExchange wrapping leaf | Single-stage plan (scatter-gather) |

### S3.3 PlanFragmenter Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `TwoStageFragmentTest` | Plan with GatherExchange -> 2 fragments | Leaf + root, RemoteSourceNode in root |
| `FragmentPartitioningTest` | Gather exchange -> fragment has single partitioning | PartitioningScheme reflects gather semantics |

### S3.4 StageScheduler Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `LeafStageAssignmentTest` | Leaf fragment -> assigned to shard-holding nodes | Each node gets its local shards |
| `RootStageAssignmentTest` | Root fragment -> assigned to coordinator | Runs on coordinating node |
| `ShardLocalityTest` | Verify data locality preference | Shards assigned to their primary node |

### S3.5 Execution Mode Router Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `RouterFeatureFlagDisabledTest` | Flag off -> DSL path | No distributed execution attempted |
| `RouterSupportedPatternTest` | Flag on + supported query -> distributed | Uses distributed engine |
| `RouterUnsupportedPatternTest` | Flag on + join query -> DSL fallback | Falls back gracefully |
| `RouterDistributedFailureTest` | Flag on + distributed fails -> DSL fallback | Falls back with warning log |

**Run command**: `./gradlew :distributed-engine:test --tests '*Converter*' --tests '*Exchange*' --tests '*Fragment*' --tests '*Scheduler*' --tests '*Router*'`

---

## Stage 4: Exchange Component Tests

**Scope**: TransportService-based exchange operators.
**Prerequisite**: Stage 3 passes.

### S4.1 GatherExchange Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `GatherExchangeBasicTest` | Collect Pages from 3 simulated sources | All Pages received at coordinator |
| `GatherExchangeOrderingTest` | Collect with merge-sort semantics | Output globally sorted |
| `GatherExchangeEmptySourceTest` | One source produces 0 Pages | Other sources' Pages collected correctly |
| `GatherExchangeBackpressureTest` | Coordinator slow to consume | Sources block until buffer space available |
| `GatherExchangeTimeoutTest` | Source node does not respond | Query times out with meaningful error |

### S4.2 Transport Serialization Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `TransportPageSerializationTest` | Serialize Page via StreamOutput, deserialize via StreamInput | Round-trip correct |
| `TransportLargePageTest` | 10K-position Page over transport | No truncation, memory bounded |
| `TransportConcurrentExchangeTest` | Multiple queries exchanging simultaneously | No cross-contamination |

### S4.3 ShardQueryAction Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `ShardQueryActionBasicTest` | Send query fragment to data node, receive Pages | Correct results from shard |
| `ShardQueryActionMultiShardTest` | Query across 5 shards | All shards produce results |
| `ShardQueryActionErrorTest` | Query fragment fails on data node | Error propagated to coordinator |

**Run command**: `./gradlew :distributed-engine:test --tests '*Exchange*' --tests '*Transport*' --tests '*ShardQuery*'`

---

## Integration Checkpoint 2: Scatter-Gather End-to-End

**Gate**: Full PPL query executes through distributed engine across multiple shards.

| IC-2 Test | Description | Pass Criteria |
|-----------|-------------|---------------|
| `ScatterGatherFilterTest` | `source=test \| where status = 200` on 5-shard index | Results match DSL path exactly |
| `ScatterGatherAggTest` | `source=test \| stats count() by city` on 5-shard index | Aggregation correct across shards |
| `ScatterGatherSortLimitTest` | `source=test \| sort - timestamp \| head 100` on 5-shard index | Global sort correct, top-100 exact |
| `ScatterGatherFilterAggTest` | `source=test \| where age > 25 \| stats avg(salary) by dept` | Combined filter+agg correct |
| `ScatterGatherExplainTest` | `_explain` API with distributed plan enabled | Returns stage/operator/shard info |
| `ScatterGatherDSLComparisonTest` | Run 20 representative queries via both paths | Results identical (row-for-row) |
| `ScatterGatherMemoryTest` | Query on large index, verify memory cleanup | MemoryPool returns to 0 after query |
| `ScatterGatherConcurrentTest` | 10 concurrent scatter-gather queries | All complete correctly, no resource leaks |
| `ScatterGatherFeatureFlagOffTest` | Disable feature flag, run same queries | Falls back to DSL path, same results |

**Run command**: `./gradlew :distributed-engine:integTest --tests '*ScatterGather*'`

**Decision point**: If IC-2 fails, do NOT proceed to IC-3. Fix scatter-gather issues first.

---

## Integration Checkpoint 3: PPL Calcite Integration Test Regression (PHASE 1 GATE)

**Gate**: ALL existing PPL Calcite integration tests pass with the distributed engine enabled.

This is the final Phase 1 quality gate. It validates that the distributed engine produces correct results for every query pattern already tested in the codebase.

### Configuration

```
plugins.sql.distributed_engine.enabled = true
```

- Supported patterns (filter, agg, sort, dedup, fields, etc.): executed through distributed engine
- Unsupported patterns (joins, window functions): transparently fall back to DSL path
- ALL tests must produce correct results regardless of execution path

### Test Suites

**Total: 116 test files, ~1,382 test methods**

| Category | Files | Tests (approx) |
|----------|-------|----------------|
| Basic PPL operations | 5 | ~100 |
| Aggregation/Stats | 6 | ~180 |
| Sort/Dedup/Top/Rare | 5 | ~60 |
| String functions | 3 | ~80 |
| DateTime functions | 4 | ~120 |
| Math functions | 2 | ~50 |
| Null handling | 2 | ~80 |
| Conditional/Type cast | 3 | ~40 |
| Subqueries | 4 | ~50 |
| JSON/Array functions | 3 | ~80 |
| Search/Relevance | 5 | ~40 |
| Explain | 1 | ~176 |
| Special commands (bin, streamstats, trendline, flatten, expand) | 8 | ~150 |
| Joins (DSL fallback) | 3 | ~50 |
| IP/Geo/Crypto functions | 4 | ~30 |
| Benchmark (Big5, ClickBench, TPCH) | 4 | ~30 |
| Remote test variants | 109 | (mirrors of above) |

### Execution

```bash
# Run ALL Calcite IT with distributed engine enabled
./gradlew :integ-test:integTest --tests '*Calcite*IT' \
  -Dplugins.sql.distributed_engine.enabled=true \
  -DignorePrometheus

# Verify DSL path still works (sanity check)
./gradlew :integ-test:integTest --tests '*Calcite*IT' \
  -Dplugins.sql.distributed_engine.enabled=false \
  -DignorePrometheus
```

### Validation Steps

| Step | Description | Pass Criteria |
|------|-------------|---------------|
| IC3.1 | Categorize all 116 IT files as distributed-eligible or fallback | Documented list |
| IC3.2 | Run full IT suite with distributed engine ON | 0 failures across ~1,382 tests |
| IC3.3 | Run full IT suite with distributed engine OFF | 0 failures (DSL path unchanged) |
| IC3.4 | Verify fallback transparency for join/window tests | Results identical to DSL-only |
| IC3.5 | Dual-mode comparison for 50 representative queries | Row-for-row match |
| IC3.6 | Distributed execution coverage report | % of tests running through distributed vs fallback |

### Pass Criteria

- **0 failures** across ALL ~1,382 Calcite IT test methods (distributed ON)
- **0 failures** across ALL ~1,382 Calcite IT test methods (distributed OFF)
- Fallback path produces identical results for unsupported patterns
- Coverage report shows >60% of tests executing through distributed engine

**This is the Phase 1 delivery gate. Phase 2 does not begin until IC-3 passes.**
