# Task Breakdown: Native Distributed Query Engine

## Organization

Tasks are organized by layer, then by component within each layer. Integration checkpoints (IC) are gates between phases. Dependencies are explicit — no task starts before its prerequisites are complete.

```
Phase 1: Scatter-Gather MVP
  Layer 1 (Runtime) ──> Layer 2a (Leaf Ops) ──> IC-1 (Local Execution)
                                                    │
  Layer 3 (Planning) ──> Layer 2b (Exchange) ──> IC-2 (Scatter-Gather E2E)

Phase 2: Full Shuffle + Joins
  Layer 2c (Join/Window Ops) ──> Exchange (Hash/Broadcast) ──> IC-3 (Full Shuffle E2E)

Phase 3: Regression + Production Readiness
  Full Regression Suite ──> Performance Tests ──> IC-4 (FINAL)
```

---

## Phase 1: Scatter-Gather MVP

### Layer 1: Execution Runtime

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| L1.1 | Page/Block data types | Port `Page`, `Block` (sealed), `LongArrayBlock`, `DoubleArrayBlock`, `ByteArrayBlock`, `VariableWidthBlock`, `BooleanBlock`, `DictionaryBlock`, `RunLengthEncodedBlock` from Trino | ~10 | None | Team Runtime |
| L1.2 | BlockBuilder variants | Port `LongBlockBuilder`, `DoubleBlockBuilder`, `VariableWidthBlockBuilder`, `BooleanBlockBuilder`, `BlockBuilder` interface | ~5 | L1.1 | Team Runtime |
| L1.3 | PagesSerde | Port `PagesSerde`, `PagesSerdeFactory` — Page serialization for exchange. Adapt to use `StreamOutput`/`StreamInput` instead of Trino's `SliceOutput` | ~2 | L1.1 | Team Runtime |
| L1.4 | Operator interface | Port `Operator`, `SourceOperator`, `SinkOperator` interfaces, `OperatorFactory` | ~4 | L1.1 | Team Runtime |
| L1.5 | Context hierarchy | Write simplified `QueryContext`, `TaskContext`, `PipelineContext`, `DriverContext`, `OperatorContext`. Strip Trino's Session/Metadata SPI. Integrate with OpenSearch `ThreadContext` | ~8 | L1.4 | Team Runtime |
| L1.6 | MemoryPool | Write `MemoryPool` with `reserve()`/`free()`/`reserveRevocable()`. Register `sql_distributed_query` circuit breaker. Wire to OpenSearch's `CircuitBreakerService` | ~5 | L1.5 | Team Runtime |
| L1.7 | Driver/Pipeline | Port `Driver` (cooperative loop), `DriverFactory`, `Pipeline`. Adapt for `sql-worker` thread pool. Port `DriverYieldSignal` for cooperative scheduling | ~4 | L1.5, L1.6 | Team Runtime |
| L1.8 | Layer 1 unit tests | All Stage 0 + Stage 1 tests from test plan | ~15 test classes | L1.1-L1.7 | Team Runtime + Tester |

**Layer 1 total: ~38 classes + ~15 test classes**

### Layer 2a: Leaf Operators (Phase 1)

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| L2.1 | LuceneFullScan | Read DocValues into Blocks per `LeafReaderContext`. Iterate segments, produce Pages. Handle `NumericDocValues` -> `LongBlock`, `SortedDocValues` -> `BytesRefBlock` | ~2 | L1.4, L1.5 | Team Operators |
| L2.2 | LuceneFilterScan | Create `Weight`/`Scorer` from pushed predicate. Use `DocIdSetIterator` to filter documents. Produce filtered Pages via DocValues | ~2 | L2.1 | Team Operators |
| L2.3 | FilterAndProjectOperator (port) | Port from `io.trino.operator.project.FilterAndProjectOperator`. Adapt `TypeOperators` to our type bridge. Wire OperatorContext | ~3 | L1.4, L1.5 | Team Operators |
| L2.4 | HashAggregationOperator (port, basic) | Port from `io.trino.operator.HashAggregationOperator`. Phase 1: `InMemoryHashAggregationBuilder` only (no spill). Port `GroupByHash` (simplified, no bytecode gen). Implement COUNT, SUM, AVG, MIN, MAX accumulators | ~8 | L1.4, L1.5, L1.6 | Team Operators |
| L2.5 | TopNOperator (port) | Port from `io.trino.operator.TopNOperator`. Heap-based top-K | ~2 | L1.4 | Team Operators |
| L2.6 | OrderByOperator (port) | Port from `io.trino.operator.OrderByOperator`. In-memory sort. Phase 1: no spill | ~2 | L1.4 | Team Operators |
| L2.7 | MergeSortedPages (port) | Port from `io.trino.operator.MergeSortedPages`. K-way merge for coordinator | ~2 | L1.1 | Team Operators |
| L2.8 | Layer 2a unit tests | All Stage 2 operator tests from test plan (Phase 1 operators) | ~10 test classes | L2.1-L2.7 | Team Operators + Tester |

**Layer 2a total: ~21 classes + ~10 test classes**

---

### INTEGRATION CHECKPOINT 1: Local Single-Node Execution

**Prerequisites**: L1.1-L1.8, L2.1-L2.8 all pass
**Scope**: Wire Pipeline with operator chain, execute locally on single node without exchange
**Tests**: IC-1 test suite (5 tests, see test plan)
**Owner**: Tester (with support from Team Runtime + Team Operators)
**Gate**: ALL IC-1 tests pass before proceeding to Layer 3 and Exchange tasks

---

### Layer 3: Physical Planning

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| L3.1 | PlanNode hierarchy | Define `PlanNode` (abstract), `FilterNode`, `ProjectNode`, `AggregationNode`, `SortNode`, `TopNNode`, `LuceneTableScanNode`, `ExchangeNode`, `RemoteSourceNode` | ~10 | None | Team Planning |
| L3.2 | RelNodeToPlanNodeConverter | Visitor over Calcite RelNode tree. Convert each LogicalXxx -> corresponding PlanNode. Handle PPL-specific nodes (dedup, rare, trendline) | ~3 | L3.1 | Team Planning |
| L3.3 | AddExchanges (adapted) | Adapt from Trino's `io.trino.sql.planner.optimizations.AddExchanges`. Insert GatherExchange for scatter-gather. Decision logic: single-index no-join -> scatter-gather, else -> fallback. Phase 1: only GatherExchange | ~3 | L3.1, L3.2 | Team Planning |
| L3.4 | PlanFragmenter | Adapt from Trino's `PlanFragmenter`. Cut at ExchangeNode boundaries. Create `StageFragment` with `RemoteSourceNode` references | ~2 | L3.3 | Team Planning |
| L3.5 | StageScheduler | Map leaf fragments to shard-holding nodes via `ClusterState`/`RoutingTable`. Map root fragment to coordinator. Use `IndexShardRoutingTable` for shard-to-node mapping | ~4 | L3.4 | Team Planning |
| L3.6 | Execution mode router | In `QueryService.executeWithCalcite()`: check feature flag, attempt distributed, fallback to DSL on failure or unsupported pattern. Setting: `plugins.sql.distributed_engine.enabled` | ~3 | L3.5 | Team Planning |
| L3.7 | `_explain` API integration | Extend explain response to show distributed plan: stages, operators per stage, shard assignments, exchange types | ~2 | L3.5 | Team Planning |
| L3.8 | Layer 3 unit tests | All Stage 3 tests from test plan | ~8 test classes | L3.1-L3.7 | Team Planning + Tester |

**Layer 3 total: ~27 classes + ~8 test classes**

### Exchange: Transport-Based Data Transfer

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| E.1 | ShardQueryAction | `HandledTransportAction` for executing a plan fragment on a data node. `ShardQueryRequest` (serialized StageFragment + shard list), `ShardQueryResponse` (serialized Pages). Register action in SQLPlugin | ~4 | L1.3, L3.4 | Team Planning |
| E.2 | OutputBuffer | Buffer for Pages produced by leaf stage operators. Bounded by configurable max bytes. Signals backpressure via `isBlocked()` future | ~2 | L1.1, L1.3 | Team Planning |
| E.3 | GatherExchange operator | SourceOperator on coordinator side. Sends `ShardQueryRequest` to each data node. Collects response Pages. Supports streaming (multiple response chunks per shard) | ~3 | E.1, E.2 | Team Planning |
| E.4 | Exchange unit tests | Stage 4 tests from test plan | ~6 test classes | E.1-E.3 | Tester |

**Exchange total: ~9 classes + ~6 test classes**

---

### INTEGRATION CHECKPOINT 2: Scatter-Gather End-to-End

**Prerequisites**: IC-1 passes, L3.1-L3.8, E.1-E.4 all pass
**Scope**: Full PPL query -> parse -> Calcite optimize -> convert -> fragment -> schedule -> execute across shards -> gather -> return results
**Tests**: IC-2 test suite (9 tests, see test plan)
**Owner**: Tester (full team support)
**Gate**: ALL IC-2 tests pass. This is the **Phase 1 delivery gate**.

**Phase 1 totals: ~95 classes + ~39 test classes**

---

## Phase 2: Full Shuffle + Joins

### Layer 2b: Exchange Extensions

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| P2.1 | HashExchange operator | Hash-partitions Pages by key columns. Sends each partition to the assigned target node via TransportService. Receives Pages from all sources for its partition | ~4 | E.1, E.2 | Team Planning |
| P2.2 | BroadcastExchange operator | Sends full copy of Pages to all participating nodes. Memory-bounded buffer | ~2 | E.1, E.2 | Team Planning |
| P2.3 | AddExchanges Phase 2 | Extend AddExchanges for hash-partition (joins, distributed agg) and broadcast (small-table join). Join strategy: broadcast if build side < threshold, else hash-partition both sides | ~1 (extend L3.3) | P2.1, P2.2 | Team Planning |

### Layer 2c: Phase 2 Operators

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| P2.4 | LookupJoinOperator (port) | Port `LookupJoinOperator` + `LookupJoinOperatorFactory` from Trino. Phase 2: no spill. Support INNER, LEFT, RIGHT, FULL, SEMI, ANTI join types | ~8 | L1.4, P2.1 | Team Operators |
| P2.5 | HashBuilderOperator (port) | Port `HashBuilderOperator` + `JoinHash` + `PagesHash`. Builds hash table from build side for join probe | ~6 | L1.4, L1.6 | Team Operators |
| P2.6 | WindowOperator (port) | Port `WindowOperator` + `RegularWindowPartition`. Support ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG/MIN/MAX OVER | ~5 | L1.4, P2.1 | Team Operators |
| P2.7 | LuceneAggScan | Lucene-native aggregation: `Collector`/`LeafCollector` with DocValues accumulation. Faster than scan+agg for simple aggregates | ~2 | L2.1 | Team Operators |
| P2.8 | LuceneSortScan | `IndexSearcher.search(Query, int, Sort)` with early termination top-K. Faster than full scan + sort | ~2 | L2.1 | Team Operators |
| P2.9 | Spill-to-disk support | Add `SpillableHashAggregationBuilder`, spill for OrderByOperator. Local temp files, configurable spill path. Port `FileSingleStreamSpiller` (simplified) | ~6 | L1.6, L2.4, L2.6 | Team Runtime |
| P2.10 | Phase 2 unit tests | All Stage 5 tests from test plan | ~12 test classes | P2.1-P2.9 | Tester |

**Phase 2 total: ~36 classes + ~12 test classes**

---

### INTEGRATION CHECKPOINT 3: Full Shuffle End-to-End

**Prerequisites**: IC-2 passes, P2.1-P2.10 all pass
**Scope**: Multi-stage distributed queries (joins, window functions, distributed aggregation)
**Tests**: IC-3 test suite (6 tests, see test plan)
**Owner**: Tester (full team support)
**Gate**: ALL IC-3 tests pass before proceeding to regression suite.

---

## Phase 3: Regression + Production Readiness

### Full Regression

| ID | Task | Description | Dependencies | Owner |
|----|------|-------------|-------------|-------|
| R.1 | Calcite integTest regression | Run ALL `*Calcite*IT` tests with distributed engine enabled AND disabled. Fix any failures | IC-3 | Tester + all teams |
| R.2 | yamlRestTest regression | Run ALL YAML REST tests. Fix any failures | IC-3 | Tester |
| R.3 | doctest regression | Run ALL doctests. Fix any failures | IC-3 | Tester |
| R.4 | Feature flag tests | Verify enable/disable/fallback behavior for all query patterns | R.1 | Tester |
| R.5 | Dual-mode comparison | Run every IT query through both paths, compare results row-for-row | R.1 | Tester |
| R.6 | Security validation | Verify DLS/FLS enforced with direct Lucene access. Test index-level permissions | IC-2 | Team Operators |

### Performance + Stability

| ID | Task | Description | Dependencies | Owner |
|----|------|-------------|-------------|-------|
| P.1 | Performance benchmark suite | JMH micro-benchmarks for Page/Block ops, operator throughput. E2E latency benchmark for 50 representative queries | IC-3 | Tester |
| P.2 | Latency regression validation | Compare p50/p99 of distributed vs DSL path for simple queries | P.1 | Tester |
| P.3 | Memory benchmark | Profile peak memory under load, verify circuit breaker behavior | IC-3 | Team Runtime |
| P.4 | Scalability test | Run same queries on 3/5/10-node clusters, measure speedup | IC-3 | Tester |
| P.5 | Stability test | 1000 sequential queries, check for leaks. Node failure during query | IC-3 | Tester |

---

### INTEGRATION CHECKPOINT 4 (FINAL): Production Readiness

**Prerequisites**: R.1-R.6, P.1-P.5 all pass
**Sign-off checklist** (from test plan):
- [ ] ALL Calcite integTests pass (distributed enabled)
- [ ] ALL Calcite integTests pass (distributed disabled)
- [ ] ALL yamlRestTests pass
- [ ] ALL doctests pass
- [ ] `_explain` API returns distributed plan info
- [ ] No latency regression (p50/p99 within threshold)
- [ ] Feature flag works correctly
- [ ] Memory cleanup verified
- [ ] Security: DLS/FLS enforced
- [ ] Thread context propagated

---

## Summary

| Phase | Layers | New Classes | Test Classes | Integration Checkpoint |
|-------|--------|-------------|-------------|----------------------|
| Phase 1 | L1 (Runtime) + L2a (Leaf Ops) + L3 (Planning) + Exchange | ~95 | ~39 | IC-1 (Local), IC-2 (Scatter-Gather) |
| Phase 2 | L2b (Exchange ext) + L2c (Join/Window) + Spill | ~36 | ~12 | IC-3 (Full Shuffle) |
| Phase 3 | Regression + Performance + Stability | ~0 (fixes only) | ~20 | IC-4 (Final) |
| **Total** | | **~131** | **~71** | **4 checkpoints** |

## Dependency Graph

```
L1.1 (Page/Block) ─────────────────────────────────────────────────┐
  │                                                                 │
  ├─> L1.2 (BlockBuilder)                                          │
  │                                                                 │
  ├─> L1.3 (PagesSerde) ──────────────────────────> E.1 (ShardQuery)
  │                                                   │
  ├─> L1.4 (Operator) ──> L2.1 (LuceneFullScan)     │
  │     │                    │                         │
  │     │                    ├─> L2.2 (LuceneFilter)  │
  │     │                    │                         │
  │     ├─> L2.3 (Filter+Project port)               │
  │     ├─> L2.4 (HashAgg port)                      │
  │     ├─> L2.5 (TopN port)                         │
  │     └─> L2.6 (OrderBy port)                      │
  │                                                    │
  ├─> L1.5 (Context) ──> L1.6 (MemoryPool)           │
  │     │                    │                         │
  │     └─> L1.7 (Driver/Pipeline)                    │
  │                                                    │
  └─> L2.7 (MergeSorted)                              │
                                                       │
L3.1 (PlanNode) ──> L3.2 (Converter) ──> L3.3 (AddExchanges)
                                            │
                                            ├─> L3.4 (Fragmenter)
                                            │     │
                                            │     └─> L3.5 (Scheduler)
                                            │           │
                                            │           └─> L3.6 (Router)
                                            │                 │
                                            │                 └─> L3.7 (_explain)
                                            │
                                            └─> E.2 (OutputBuffer) ──> E.3 (GatherExchange)

════════════════════════════════════════════════
IC-1: L1.* + L2.1-L2.7 (Local execution)
IC-2: IC-1 + L3.* + E.* (Scatter-gather E2E)
IC-3: IC-2 + P2.* (Full shuffle E2E)
IC-4: IC-3 + R.* + P.* (Production readiness)
════════════════════════════════════════════════
```
