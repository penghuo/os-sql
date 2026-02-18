# Test Plan: Native Distributed Query Engine

## Overview

Testing is organized into 6 stages with 4 integration checkpoints (IC). Each stage validates a specific layer or component. Integration checkpoints verify cross-layer correctness before proceeding. No stage begins until the prior integration checkpoint passes.

```
Stage 0: Foundation ──> Stage 1: Layer 1 ──> Stage 2: Layer 2 ──> IC-1
    │                                                                │
    │    Stage 3: Layer 3 ──> Stage 4: Exchange ──> IC-2             │
    │                                                 │              │
    │    Stage 5: Phase 2 Operators ──> IC-3          │              │
    │                                    │            │              │
    │    Stage 6: Full Regression ──> IC-4 (FINAL)    │              │
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

Each ported operator gets a test suite that mirrors Trino's own tests, adapted for our Page/Block implementation.

| Test Suite | Operator | Key Test Cases |
|-----------|----------|----------------|
| `FilterAndProjectOperatorTest` | FilterAndProjectOperator | Filter true/false/null, project column subset, project with expression, empty page input |
| `HashAggregationOperatorTest` | HashAggregationOperator | Single group key, multi-column key, null keys, high-cardinality groups, COUNT/SUM/AVG/MIN/MAX, partial mode, final mode, memory limit triggers spill (Phase 2) |
| `TopNOperatorTest` | TopNOperator | Top-5 from 1M rows, multi-column sort, null ordering (FIRST/LAST), ties handling, N > input size |
| `OrderByOperatorTest` | OrderByOperator | Single column sort, multi-column, ASC/DESC, null ordering, large input requiring multiple Pages |
| `MergeSortedPagesTest` | MergeSortedPages | Merge 2 streams, merge 10 streams, empty streams, single-element streams, all-duplicates |
| `LookupJoinOperatorTest` | LookupJoinOperator | INNER join, LEFT join, RIGHT join, FULL join, SEMI join, ANTI join, multi-column join key, null join keys, empty build side, empty probe side (Phase 2) |
| `HashBuilderOperatorTest` | HashBuilderOperator | Build hash table, verify lookup, duplicate keys, memory tracking (Phase 2) |
| `WindowOperatorTest` | WindowOperator | ROW_NUMBER, RANK, DENSE_RANK, LAG/LEAD, SUM OVER, ROWS frame, RANGE frame, single partition, multiple partitions (Phase 2) |

### S2.2 Lucene Leaf Operator Tests

Require an embedded OpenSearch index with known test data.

| Test Suite | Operator | Key Test Cases |
|-----------|----------|----------------|
| `LuceneFullScanTest` | LuceneFullScan | Scan all docs from single segment, multi-segment, verify Page/Block content matches documents |
| `LuceneFilterScanTest` | LuceneFilterScan | Term query, range query, boolean (AND/OR/NOT), verify filtered Pages match expected docs |
| `LuceneAggScanTest` | LuceneAggScan | COUNT, SUM, AVG over DocValues, GROUP BY keyword field, verify partial aggregates correct (Phase 2) |
| `LuceneSortScanTest` | LuceneSortScan | Sort by long field ASC/DESC, sort by keyword, top-K with early termination, verify sort order correct (Phase 2) |

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
| `ConvertJoinTest` | LogicalJoin -> JoinNode | Join type, condition, left/right sources preserved |
| `ConvertSortTest` | LogicalSort -> SortNode or TopNNode | Sort keys, direction, limit preserved |
| `ConvertTableScanTest` | LogicalTableScan -> LuceneTableScanNode | Index name, projected columns preserved |
| `ConvertComplexPlanTest` | Multi-level RelNode tree | Complete tree structure preserved |
| `ConvertPPLDedupTest` | PPL dedup RelNode -> DedupNode | PPL-specific semantics preserved |

### S3.2 AddExchanges Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `ExchangeForAggregateTest` | AggregationNode -> partial + GatherExchange + final | Exchange inserted at correct position |
| `ExchangeForJoinTest` | JoinNode -> HashExchange on both sides | Both sides repartitioned on join key |
| `ExchangeBroadcastJoinTest` | Small table join -> BroadcastExchange on small side | Broadcast chosen when table below threshold |
| `ExchangeForSortTest` | SortNode -> local sort + GatherExchange + merge | Correct exchange type |
| `NoExchangeSimpleFilterTest` | Simple filter+project -> no exchange needed | Single-stage plan (scatter-gather) |

### S3.3 PlanFragmenter Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `SingleStageFragmentTest` | Plan with no exchanges -> 1 fragment | Single fragment contains entire plan |
| `TwoStageFragmentTest` | Plan with GatherExchange -> 2 fragments | Leaf + root, RemoteSourceNode in root |
| `ThreeStageFragmentTest` | Plan with 2 exchanges -> 3 fragments | Correct fragment boundaries at each exchange |
| `FragmentPartitioningTest` | Hash exchange -> fragment has hash partitioning scheme | PartitioningScheme reflects hash keys |

### S3.4 StageScheduler Tests

| Test | Description | Assertions |
|------|-------------|------------|
| `LeafStageAssignmentTest` | Leaf fragment -> assigned to shard-holding nodes | Each node gets its local shards |
| `IntermediateStageAssignmentTest` | Non-leaf fragment -> assigned to available nodes | Work distributed across cluster |
| `RootStageAssignmentTest` | Root fragment -> assigned to coordinator | Runs on coordinating node |
| `ShardLocalityTest` | Verify data locality preference | Shards assigned to their primary node |

**Run command**: `./gradlew :distributed-engine:test --tests '*Converter*' --tests '*Exchange*' --tests '*Fragment*' --tests '*Scheduler*'`

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

**Decision point**: If IC-2 fails, do NOT proceed to Phase 2 tasks. This is the Phase 1 delivery gate.

---

## Stage 5: Phase 2 Operator Tests

**Scope**: HashExchange, BroadcastExchange, join operators, window operators, spill.
**Prerequisite**: IC-2 passes.

### S5.1 HashExchange Tests
| Test | Description |
|------|-------------|
| `HashExchangePartitioningTest` | Pages hash-partitioned to N targets by key column |
| `HashExchangeSkewTest` | Skewed key distribution, verify all partitions receive data |
| `HashExchangeNullKeyTest` | Null keys in partition column |

### S5.2 BroadcastExchange Tests
| Test | Description |
|------|-------------|
| `BroadcastExchangeBasicTest` | All targets receive identical Pages |
| `BroadcastExchangeMemoryTest` | Verify memory bounded for broadcast copies |

### S5.3 Join Integration Tests
| Test | Description |
|------|-------------|
| `DistributedInnerJoinTest` | PPL: `source=orders \| join ON orders.cust_id = customers.id customers` |
| `DistributedLeftJoinTest` | PPL: left join with non-matching keys |
| `DistributedBroadcastJoinTest` | Small lookup table broadcast to all nodes |
| `DistributedJoinNullKeysTest` | Join keys with null values |

### S5.4 Window Function Tests
| Test | Description |
|------|-------------|
| `DistributedRowNumberTest` | PPL: `... \| eval rn = row_number() by dept sort salary` |
| `DistributedRankTest` | RANK with ties |
| `DistributedLagLeadTest` | LAG/LEAD window functions |

### S5.5 Spill-to-Disk Tests
| Test | Description |
|------|-------------|
| `AggregationSpillTest` | High-cardinality GROUP BY exceeds memory -> spill |
| `JoinSpillTest` | Large build side exceeds memory -> spill |
| `SortSpillTest` | Large ORDER BY exceeds memory -> spill |
| `SpillCleanupTest` | Verify spill files deleted after query completes |

**Run command**: `./gradlew :distributed-engine:test --tests '*HashExchange*' --tests '*Broadcast*' --tests '*Join*' --tests '*Window*' --tests '*Spill*'`

---

## Integration Checkpoint 3: Full Shuffle End-to-End

**Gate**: Multi-stage distributed queries execute correctly.

| IC-3 Test | Description | Pass Criteria |
|-----------|-------------|---------------|
| `ShuffleJoinTest` | 2-index hash join via distributed engine | Results match expected join output |
| `ShuffleWindowTest` | Window function across distributed data | Correct row numbering/ranking |
| `ShuffleMultiStageAggTest` | High-cardinality agg with hash exchange | Partial -> exchange -> final correct |
| `ShuffleExplainTest` | `_explain` shows multi-stage plan | All stages, exchanges, assignments visible |
| `ShuffleMemoryPressureTest` | Query that requires spill | Completes correctly with spill |
| `ShuffleConcurrentTest` | 5 concurrent multi-stage queries | All complete, no deadlock |

**Run command**: `./gradlew :distributed-engine:integTest --tests '*Shuffle*'`

**Decision point**: If IC-3 fails, do NOT proceed to final regression. Fix Phase 2 issues first.

---

## Stage 6: Full Regression Suite

**Scope**: All existing tests pass with distributed engine enabled.
**Prerequisite**: IC-3 passes.

### S6.1 Existing Test Suites

| Test Suite | Command | Pass Criteria |
|-----------|---------|---------------|
| Calcite Integration Tests | `./gradlew :integ-test:integTest --tests '*Calcite*IT' -DignorePrometheus` | ALL pass (0 failures) |
| YAML REST Tests | `./gradlew yamlRestTest` | ALL pass (0 failures) |
| Documentation Tests | `./gradlew doctest -DignorePrometheus` | ALL pass (0 failures) |
| Core Unit Tests | `./gradlew :core:test` | ALL pass (0 failures) |
| OpenSearch Module Tests | `./gradlew :opensearch:test` | ALL pass (0 failures) |

### S6.2 Dual-Mode Comparison Tests

Run each existing integration test query through BOTH paths and compare:

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `DualModeCalciteITTest` | Each Calcite IT query via DSL and distributed | Results identical |
| `DualModeYamlTest` | Each YAML test query via DSL and distributed | Results identical |

### S6.3 Feature Flag Tests

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `FeatureFlagDisabledTest` | All tests pass with distributed engine disabled | 100% pass, DSL path used |
| `FeatureFlagEnabledTest` | All tests pass with distributed engine enabled | 100% pass, distributed path used where supported |
| `FeatureFlagFallbackTest` | Unsupported patterns fall back to DSL | Graceful fallback, warning logged |

---

## Integration Checkpoint 4 (FINAL): Production Readiness

**Gate**: Full regression + performance validation.

### S7.1 Performance Tests

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `LatencyRegressionTest` | 50 representative simple queries (filter+agg) | p50 within 10% of DSL path, p99 within 20% |
| `ThroughputTest` | 100 concurrent queries | No timeouts, no OOM |
| `MemoryBenchmarkTest` | Profile memory under load | Peak stays within circuit breaker budget |
| `ScalabilityTest` | Same query on 3-node, 5-node, 10-node clusters | Latency decreases with more nodes for parallelizable queries |
| `PlanningOverheadTest` | Measure time in RelNodeToPlanNodeConverter + AddExchanges + PlanFragmenter | < 10ms for typical queries |

### S7.2 Stability Tests

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| `LongRunningStabilityTest` | 1000 queries in sequence | No memory leaks, no crashes |
| `NodeFailureDuringQueryTest` | Kill a data node mid-query | Query fails gracefully with error message |
| `CircuitBreakerTripTest` | Queries that exceed memory budget | Circuit breaker trips, query fails cleanly, system recovers |

### Final Sign-Off Checklist

- [ ] ALL Calcite integTests pass (distributed enabled)
- [ ] ALL Calcite integTests pass (distributed disabled)
- [ ] ALL yamlRestTests pass
- [ ] ALL doctests pass
- [ ] `_explain` API returns distributed plan info
- [ ] No latency regression for simple queries (p50/p99)
- [ ] Feature flag toggles correctly
- [ ] Memory cleanup verified (no leaks)
- [ ] Security: DLS/FLS enforced with direct Lucene access
- [ ] Thread context propagated across exchanges
