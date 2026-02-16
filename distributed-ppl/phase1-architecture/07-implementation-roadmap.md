# Implementation Roadmap: Phase 1 → Phase 2 Handoff

**Status:** DRAFT
**Purpose:** Bridge architecture design to implementation. Phase 2 engineers start here.

---

## 1. Phase 2 Work Breakdown

Phase 2 has two parallel workstreams:

### Workstream A: Fragment Planner + Shard Split Manager
**Engineer:** planner-engineer

| Step | Task | Output | Tests |
|------|------|--------|-------|
| A1 | Implement `Fragment`, `FragmentType`, `PartitionSpec`, `ExchangeSpec` data structures | Data classes in `core/.../planner/distributed/` | Unit tests for construction + equality |
| A2 | Implement `ShardSplit` (extends `Split`) | `core/.../planner/distributed/ShardSplit.java` | Unit tests for getSplitId() |
| A3 | Implement `ShardSplitManager` | `core/.../planner/distributed/ShardSplitManager.java` | Mock ClusterState tests |
| A4 | Implement `FragmentPlanner` — aggregation pattern | `core/.../planner/distributed/FragmentPlanner.java` | Fragment structure tests for stats queries |
| A5 | Implement `FragmentPlanner` — sort+limit pattern | Extension of A4 | Fragment structure tests for sort queries |
| A6 | Implement `FragmentPlanner` — join pattern | Extension of A4 | Fragment structure tests for join queries |
| A7 | Implement `FragmentPlanner` — passthrough (shard-local) detection | `requiresExchange()` method | Tests that filter/eval/fields queries return empty |
| A8 | Implement `CardinalityEstimator` | `core/.../planner/distributed/CardinalityEstimator.java` | Mock metadata tests |
| A9 | Implement `QueryRouter` | `core/.../planner/distributed/QueryRouter.java` or in PPLService | Routing decision tests |

### Workstream B: Transport Actions + Execution Engine
**Engineer:** transport-engineer

| Step | Task | Output | Tests |
|------|------|--------|-------|
| B1 | Implement `ShardExecutionRequest`/`Response` (Writeable) | `plugin/.../transport/distributed/` | Serialization round-trip tests |
| B2 | Implement `ExchangeDataRequest`/`Response` (Writeable) | Same package | Serialization round-trip tests |
| B3 | Implement `QueryCancelRequest`/`Response` (Writeable) | Same package | Serialization round-trip tests |
| B4 | Implement `TransportShardExecutionAction` | `plugin/.../transport/distributed/` | Mock transport test |
| B5 | Implement `TransportExchangeDataAction` | Same package | Mock transport test |
| B6 | Implement `TransportQueryCancelAction` | Same package | Mock transport test |
| B7 | Register actions in `SQLPlugin.getActions()` | Modified `SQLPlugin.java` | Plugin loads without error |
| B8 | Implement `ExchangeBuffer` | `core/.../planner/physical/distributed/` | Concurrency tests, backpressure tests |
| B9 | Implement `ExchangeService` | `opensearch/.../executor/distributed/` | Buffer lifecycle tests |
| B10 | Implement `FragmentExecutor` (data node side) | `opensearch/.../executor/distributed/` | Mock plan execution test |
| B11 | Implement `DistributedQueryContext` | `opensearch/.../executor/distributed/` | State machine tests, cancellation tests |
| B12 | Implement `DistributedExecutionEngine` | `opensearch/.../executor/distributed/` | End-to-end test with mock transport |

### Integration Point

A and B converge at **step A9 + B12**: the QueryRouter routes to DistributedExecutionEngine,
which uses FragmentPlanner output to dispatch fragments.

**Milestone:** A simple aggregation query (`source=idx | stats count()`) executes through
the distributed path end-to-end on a single-node cluster (all fragments local).

## 2. Phase 2 → Phase 3 Dependency

Phase 3 (operators) depends on these Phase 2 deliverables:

| Phase 3 Needs | Provided By |
|---------------|-------------|
| ExchangeBuffer for exchange operator | B8 |
| ExchangeService for buffer management | B9 |
| FragmentExecutor for operator execution | B10 |
| TransportShardExecutionAction for dispatch | B4 |
| TransportExchangeDataAction for data transfer | B5 |
| Fragment/ExchangeSpec data structures | A1 |

Phase 3 can start as soon as B8 (ExchangeBuffer) and B10 (FragmentExecutor) are done,
even if the full DistributedExecutionEngine isn't complete yet.

## 3. Phase 3 Work Breakdown

### Workstream C: Exchange + Aggregation Operators
**Engineer:** exchange-engineer

| Step | Task | Output |
|------|------|--------|
| C1 | Implement `DistributedExchangeOperator` (consumer side) | `core/.../physical/distributed/` |
| C2 | Implement exchange producer logic in `FragmentExecutor` | Modified FragmentExecutor |
| C3 | Implement `PartialAggregationOperator` | `core/.../physical/distributed/` |
| C4 | Implement `PartialAggState` for COUNT, SUM, AVG, MIN, MAX | Same package |
| C5 | Implement `FinalAggregationOperator` | Same package |
| C6 | End-to-end test: partial agg on mock data through exchange | Integration test |

### Workstream D: Join + Sort Operators
**Engineer:** join-engineer

| Step | Task | Output |
|------|------|--------|
| D1 | Implement `DistributedHashJoinOperator` (INNER join) | `core/.../physical/distributed/` |
| D2 | Add LEFT, RIGHT, FULL join support | Extension of D1 |
| D3 | Implement broadcast join optimization | Extension of D1 |
| D4 | Implement `DistributedSortOperator` (merge sort) | `core/.../physical/distributed/` |
| D5 | Add limit pushdown in sort | Extension of D4 |
| D6 | End-to-end test: hash join on mock data through exchange | Integration test |

## 4. Key Files to Create (Complete List)

### Phase 2 New Files (14 files)

```
core/src/main/java/org/opensearch/sql/planner/distributed/
├── Fragment.java                     (A1)
├── FragmentType.java                 (A1)
├── PartitionSpec.java                (A1)
├── ExchangeSpec.java                 (A1)
├── ExchangeType.java                 (A1)
├── ShardSplit.java                   (A2)
├── ShardSplitManager.java            (A3)
├── FragmentPlanner.java              (A4-A7)
├── CardinalityEstimator.java         (A8)
└── QueryRouter.java                  (A9)

core/src/main/java/org/opensearch/sql/planner/physical/distributed/
└── ExchangeBuffer.java              (B8)

opensearch/src/main/java/org/opensearch/sql/opensearch/executor/distributed/
├── DistributedExecutionEngine.java   (B12)
├── DistributedQueryContext.java      (B11)
├── ExchangeService.java             (B9)
└── FragmentExecutor.java            (B10)

plugin/src/main/java/org/opensearch/sql/plugin/transport/distributed/
├── ShardExecutionAction.java         (B4)
├── TransportShardExecutionAction.java (B4)
├── ShardExecutionRequest.java        (B1)
├── ShardExecutionResponse.java       (B1)
├── ExchangeDataAction.java          (B5)
├── TransportExchangeDataAction.java  (B5)
├── ExchangeDataRequest.java         (B2)
├── ExchangeDataResponse.java        (B2)
├── QueryCancelAction.java           (B6)
├── TransportQueryCancelAction.java   (B6)
├── QueryCancelRequest.java          (B3)
└── QueryCancelResponse.java         (B3)
```

### Phase 2 Modified Files (3 files)

```
plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java          (B7)
core/src/main/java/org/opensearch/sql/planner/physical/PhysicalPlanNodeVisitor.java  (add visit methods)
ppl/src/main/java/org/opensearch/sql/ppl/PPLService.java              (A9 routing)
```

### Phase 3 New Files (6 files)

```
core/src/main/java/org/opensearch/sql/planner/physical/distributed/
├── DistributedExchangeOperator.java    (C1)
├── PartialAggregationOperator.java     (C3)
├── PartialAggState.java                (C4)
├── FinalAggregationOperator.java       (C5)
├── DistributedHashJoinOperator.java    (D1)
└── DistributedSortOperator.java        (D4)
```

## 5. Test Files to Create

### Phase 2 Tests

```
core/src/test/java/org/opensearch/sql/planner/distributed/
├── FragmentTest.java
├── ShardSplitTest.java
├── ShardSplitManagerTest.java
├── FragmentPlannerTest.java
├── FragmentPlannerAggregationTest.java
├── FragmentPlannerSortTest.java
├── FragmentPlannerJoinTest.java
├── CardinalityEstimatorTest.java
└── QueryRouterTest.java

core/src/test/java/org/opensearch/sql/planner/physical/distributed/
└── ExchangeBufferTest.java

opensearch/src/test/java/org/opensearch/sql/opensearch/executor/distributed/
├── DistributedExecutionEngineTest.java
├── DistributedQueryContextTest.java
├── ExchangeServiceTest.java
└── FragmentExecutorTest.java

plugin/src/test/java/org/opensearch/sql/plugin/transport/distributed/
├── ShardExecutionRequestSerializationTest.java
├── ShardExecutionResponseSerializationTest.java
├── ExchangeDataRequestSerializationTest.java
├── ExchangeDataResponseSerializationTest.java
├── TransportShardExecutionActionTest.java
├── TransportExchangeDataActionTest.java
└── TransportQueryCancelActionTest.java
```

### Phase 3 Tests

```
core/src/test/java/org/opensearch/sql/planner/physical/distributed/
├── DistributedExchangeOperatorTest.java
├── PartialAggregationOperatorTest.java
├── PartialAggStateTest.java
├── FinalAggregationOperatorTest.java
├── DistributedHashJoinOperatorTest.java
└── DistributedSortOperatorTest.java
```

## 6. Configuration to Add

In OpenSearch settings (registered via `SQLPlugin`):

```java
// New settings for distributed PPL
Setting.boolSetting("plugins.ppl.distributed.enabled", false, NodeScope, Dynamic);
Setting.boolSetting("plugins.ppl.distributed.auto_route", true, NodeScope, Dynamic);
Setting.longSetting("plugins.ppl.distributed.threshold_docs", 100000, 0, NodeScope, Dynamic);
Setting.timeSetting("plugins.ppl.distributed.query.timeout", TimeValue.timeValueSeconds(120), NodeScope, Dynamic);
Setting.timeSetting("plugins.ppl.distributed.shard.timeout", TimeValue.timeValueSeconds(60), NodeScope, Dynamic);
Setting.timeSetting("plugins.ppl.distributed.exchange.timeout", TimeValue.timeValueSeconds(30), NodeScope, Dynamic);
Setting.byteSizeSetting("plugins.ppl.distributed.join.memory_limit", new ByteSizeValue(100, ByteSizeUnit.MB), NodeScope, Dynamic);
Setting.byteSizeSetting("plugins.ppl.distributed.exchange.buffer_size", new ByteSizeValue(10, ByteSizeUnit.MB), NodeScope, Dynamic);
Setting.intSetting("plugins.ppl.distributed.exchange.batch_size", 10000, 100, NodeScope, Dynamic);
Setting.byteSizeSetting("plugins.ppl.distributed.join.broadcast_threshold", new ByteSizeValue(10, ByteSizeUnit.MB), NodeScope, Dynamic);
```

## 7. Acceptance Criteria for Phase 2 Complete

1. **Simple aggregation E2E:** `source=idx | stats count()` executes through distributed path
   on single-node cluster and returns correct result.
2. **Fragment planner:** Correctly fragments aggregation, sort+limit, and join queries.
3. **Transport actions:** All 3 actions registered, serialize/deserialize correctly.
4. **Exchange buffer:** Correctly handles concurrent producers, backpressure, completion.
5. **Query router:** Routes to distributed engine when enabled + index large enough.
6. **All tests pass:** Unit tests for every new class. `./gradlew :core:test :opensearch:test :plugin:test` green.
7. **Single-node backward compatibility:** `plugins.ppl.distributed.enabled=false` → all existing tests pass unchanged.

## 8. Acceptance Criteria for Phase 3 Complete

1. **Partial + final aggregation:** `stats count(), avg(x) by y` produces correct results through
   distributed path with mock multi-shard data.
2. **Exchange operator:** Consumer correctly receives data from producer through ExchangeBuffer.
3. **Hash join:** `join` with INNER/LEFT/RIGHT produces correct results with mock data.
4. **Merge sort:** `sort x | head 100` correctly merge-sorts across shard results.
5. **Memory limits:** Hash join respects memory limit; exchange buffer implements backpressure.
6. **Cancellation:** All operators respect cancellation token.
7. **All tests pass:** Unit tests + operator integration tests green.
