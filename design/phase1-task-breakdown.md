# Phase 1 Task Breakdown: Scatter-Gather MVP

## Organization

Tasks organized by layer, then by component. Three integration checkpoints gate Phase 1 completion. Dependencies are explicit — no task starts before its prerequisites are complete.

```
Layer 1 (Runtime) ──────> Layer 2a (Leaf Ops) ──────> IC-1 (Local Execution)
                                                          │
Layer 3 (Planning) ──────> Exchange ──────────────────> IC-2 (Scatter-Gather E2E)
                                                          │
                                                        IC-3 (PPL Calcite IT Regression)
                                                          │
                                                    PHASE 1 COMPLETE
```

---

## Layer 1: Execution Runtime

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| L1.1 | Page/Block data types | Port `Page`, `Block` (sealed), `LongArrayBlock`, `DoubleArrayBlock`, `ByteArrayBlock`, `VariableWidthBlock`, `BooleanBlock`, `DictionaryBlock`, `RunLengthEncodedBlock` from Trino | ~10 | None | runtime |
| L1.2 | BlockBuilder variants | Port `LongBlockBuilder`, `DoubleBlockBuilder`, `VariableWidthBlockBuilder`, `BooleanBlockBuilder`, `BlockBuilder` interface | ~5 | L1.1 | runtime |
| L1.3 | PagesSerde | Port `PagesSerde`, `PagesSerdeFactory` — Page serialization for exchange. Adapt to use `StreamOutput`/`StreamInput` instead of Trino's `SliceOutput` | ~2 | L1.1 | runtime |
| L1.4 | Operator interface | Port `Operator`, `SourceOperator`, `SinkOperator` interfaces, `OperatorFactory` | ~4 | L1.1 | runtime |
| L1.5 | Context hierarchy | Write simplified `QueryContext`, `TaskContext`, `PipelineContext`, `DriverContext`, `OperatorContext`. Strip Trino's Session/Metadata SPI. Integrate with OpenSearch `ThreadContext` | ~8 | L1.4 | runtime |
| L1.6 | MemoryPool | Write `MemoryPool` with `reserve()`/`free()`/`reserveRevocable()`. Register `sql_distributed_query` circuit breaker. Wire to OpenSearch's `CircuitBreakerService` | ~5 | L1.5 | runtime |
| L1.7 | Driver/Pipeline | Port `Driver` (cooperative loop), `DriverFactory`, `Pipeline`. Adapt for `sql-worker` thread pool. Port `DriverYieldSignal` for cooperative scheduling | ~4 | L1.5, L1.6 | runtime |
| L1.8 | Layer 1 unit tests | All Stage 0 + Stage 1 tests from test plan | ~15 test classes | L1.1-L1.7 | runtime |

**Layer 1 total: ~38 classes + ~15 test classes**

---

## Layer 2a: Leaf Operators

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| L2.1 | LuceneFullScan | Read DocValues into Blocks per `LeafReaderContext`. Iterate segments, produce Pages. Handle `NumericDocValues` -> `LongBlock`, `SortedDocValues` -> `BytesRefBlock` | ~2 | L1.4, L1.5 | lucene-ops |
| L2.2 | LuceneFilterScan | Create `Weight`/`Scorer` from pushed predicate. Use `DocIdSetIterator` to filter documents. Produce filtered Pages via DocValues | ~2 | L2.1 | lucene-ops |
| L2.3 | FilterAndProjectOperator (port) | Port from `io.trino.operator.project.FilterAndProjectOperator`. Adapt `TypeOperators` to our type bridge. Wire OperatorContext | ~3 | L1.4, L1.5 | trino-ops |
| L2.4 | HashAggregationOperator (port, basic) | Port from `io.trino.operator.HashAggregationOperator`. Phase 1: `InMemoryHashAggregationBuilder` only (no spill). Port `GroupByHash` (simplified, no bytecode gen). Implement COUNT, SUM, AVG, MIN, MAX accumulators | ~8 | L1.4, L1.5, L1.6 | trino-ops |
| L2.5 | TopNOperator (port) | Port from `io.trino.operator.TopNOperator`. Heap-based top-K | ~2 | L1.4 | trino-ops |
| L2.6 | OrderByOperator (port) | Port from `io.trino.operator.OrderByOperator`. In-memory sort. Phase 1: no spill | ~2 | L1.4 | trino-ops |
| L2.7 | MergeSortedPages (port) | Port from `io.trino.operator.MergeSortedPages`. K-way merge for coordinator | ~2 | L1.1 | trino-ops |
| L2.8 | Layer 2a unit tests | All Stage 2 operator tests from test plan (Phase 1 operators) | ~10 test classes | L2.1-L2.7 | lucene-ops (Lucene tests) + trino-ops (Trino op tests) |

**Layer 2a total: ~21 classes + ~10 test classes**

---

### INTEGRATION CHECKPOINT 1: Local Single-Node Execution

**Prerequisites**: L1.1-L1.8, L2.1-L2.8 all pass
**Scope**: Wire Pipeline with operator chain, execute locally on single node without exchange
**Tests**: IC-1 test suite (5 tests, see test plan)
**Owner**: planner (free since turn 12, with support from runtime + lucene-ops + trino-ops)
**Gate**: ALL IC-1 tests pass before proceeding to Layer 3 and Exchange tasks

---

## Layer 3: Physical Planning

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| L3.1 | PlanNode hierarchy | Define `PlanNode` (abstract), `FilterNode`, `ProjectNode`, `AggregationNode`, `SortNode`, `TopNNode`, `LuceneTableScanNode`, `ExchangeNode`, `RemoteSourceNode` | ~10 | None | planner |
| L3.2 | RelNodeToPlanNodeConverter | Visitor over Calcite RelNode tree. Convert each LogicalXxx -> corresponding PlanNode. Handle PPL-specific nodes (dedup, rare, trendline). Throw `UnsupportedPatternException` for joins/windows -> triggers DSL fallback | ~3 | L3.1 | planner |
| L3.3 | AddExchanges (Phase 1) | Adapt from Trino's `AddExchanges`. Phase 1: insert GatherExchange for scatter-gather only. Decision logic: single-index no-join -> scatter-gather, else -> fallback to DSL | ~3 | L3.1, L3.2 | planner |
| L3.4 | PlanFragmenter | Adapt from Trino's `PlanFragmenter`. Cut at ExchangeNode boundaries. Phase 1: always 2 fragments (leaf + root). Create `StageFragment` with `RemoteSourceNode` references | ~2 | L3.3 | planner |
| L3.5 | StageScheduler | Map leaf fragments to shard-holding nodes via `ClusterState`/`RoutingTable`. Map root fragment to coordinator. Use `IndexShardRoutingTable` for shard-to-node mapping | ~4 | L3.4 | exchange |
| L3.6 | Execution mode router | In `QueryService.executeWithCalcite()`: check feature flag, attempt distributed, fallback to DSL on failure or unsupported pattern. Setting: `plugins.sql.distributed_engine.enabled` | ~3 | L3.5 | exchange |
| L3.7 | `_explain` API integration | Extend explain response to show distributed plan: stages, operators per stage, shard assignments, exchange types | ~2 | L3.5 | exchange |
| L3.8 | Layer 3 unit tests | All Stage 3 tests from test plan | ~8 test classes | L3.1-L3.7 | planner (L3.1-L3.4 tests) + exchange (L3.5-L3.7 tests) |

**Layer 3 total: ~27 classes + ~8 test classes**

---

## Exchange: Transport-Based Data Transfer

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| E.1 | ShardQueryAction | `HandledTransportAction` for executing a plan fragment on a data node. `ShardQueryRequest` (serialized StageFragment + shard list), `ShardQueryResponse` (serialized Pages). Register action in SQLPlugin | ~4 | L1.3, L3.4 | exchange |
| E.2 | OutputBuffer | Buffer for Pages produced by leaf stage operators. Bounded by configurable max bytes. Signals backpressure via `isBlocked()` future | ~2 | L1.1, L1.3 | exchange |
| E.3 | GatherExchange operator | SourceOperator on coordinator side. Sends `ShardQueryRequest` to each data node. Collects response Pages. Supports streaming (multiple response chunks per shard) | ~3 | E.1, E.2 | exchange |
| E.4 | Exchange unit tests | Stage 4 tests from test plan | ~6 test classes | E.1-E.3 | exchange |

**Exchange total: ~9 classes + ~6 test classes**

---

### INTEGRATION CHECKPOINT 2: Scatter-Gather End-to-End

**Prerequisites**: IC-1 passes, L3.1-L3.8, E.1-E.4 all pass
**Scope**: Full PPL query -> parse -> Calcite optimize -> convert -> fragment -> schedule -> execute across shards -> gather -> return results
**Tests**: IC-2 test suite (9 tests, see test plan)
**Owner**: runtime (free since turn 20, full team support)
**Gate**: ALL IC-2 tests pass before proceeding to IC-3

---

### INTEGRATION CHECKPOINT 3: PPL Calcite Integration Test Regression (Phase 1 Gate)

**Prerequisites**: IC-2 passes
**Scope**: ALL existing PPL Calcite integration tests must pass with the distributed engine enabled. This is the critical quality gate that validates the distributed engine produces correct results for every query pattern already tested in the codebase.

**Test configuration:**
- `plugins.sql.distributed_engine.enabled = true`
- DSL fallback enabled for unsupported patterns (joins, window functions)
- For supported patterns (filter, agg, sort, dedup, fields, etc.): distributed engine must execute the query, NOT the DSL path

**Test suites (116 files, ~1,382 test methods):**

| Category | Test Files | Key Coverage |
|----------|-----------|--------------|
| Basic operations | `CalcitePPLBasicIT`, `CalciteFieldsCommandIT`, `CalciteWhereCommandIT` | source, fields, where, head, tail |
| Aggregation | `CalcitePPLAggregationIT`, `CalciteStatsCommandIT`, `CalciteTimechartCommandIT` | stats, count, avg, sum, min, max, group by |
| Sort/Dedup/Top | `CalcitePPLSortIT`, `CalciteDedupCommandIT`, `CalciteTopCommandIT`, `CalciteRareCommandIT` | sort, dedup, top, rare |
| String functions | `CalcitePPLBuiltinStringFunctionIT` | upper, lower, concat, substr, trim, length |
| DateTime functions | `CalcitePPLDateTimeFunctionIT`, `CalcitePPLDateTimeBuiltinFunctionIT` | now, date_format, to_date, convert_tz |
| Math functions | `CalcitePPLBuiltinMathFunctionIT` | abs, round, ceil, floor, sqrt, trig |
| Null handling | `CalcitePPLBuiltinFunctionsNullIT` | null propagation, coalesce, ifnull |
| Subqueries | `CalciteScalarSubqueryIT`, `CalciteExistsSubqueryIT`, `CalciteInSubqueryIT` | scalar, exists, IN subqueries |
| JSON/Array | `CalciteArrayFunctionIT`, `CalciteJsonFunctionIT` | array ops, JSON path extraction |
| Search/Relevance | `CalciteMatchIT`, `CalciteMatchBoolPrefixIT`, `CalciteQueryStringIT` | match, query_string, simple_query_string |
| Explain | `CalciteExplainIT` (176 tests) | explain plan output validation |
| Special commands | `CalciteBinCommandIT`, `CalciteStreamstatsCommandIT`, `CalciteTrendlineCommandIT`, `CalciteFlattenCommandIT`, `CalciteExpandCommandIT` | bin, streamstats, trendline, flatten, expand |
| Joins (FALLBACK) | `CalcitePPLJoinIT`, `CalcitePPLJoinPushIT` | Must pass via DSL fallback path |
| Benchmark | `CalcitePPLBig5IT`, `CalcitePPLClickBenchIT`, TPCH tests | correctness benchmarks |

**Execution approach:**
1. Run ALL 116 Calcite IT files with distributed engine enabled
2. Supported patterns execute through distributed engine
3. Unsupported patterns (joins, windows) transparently fall back to DSL
4. ALL ~1,382 test methods must produce correct results (0 failures)

### Verification Infrastructure (Proving Distributed Engine Was Used)

Three mechanisms ensure the distributed engine is genuinely executing queries — not silently falling back to DSL:

**V.1 Engine Tag in Response** — Add `engine` field to `QueryResponse` and `ExplainResponse`:
```json
{"engine": "distributed", "datarows": [...]}
```
Values: `"distributed"`, `"calcite_local"`, `"legacy_dsl"`. Integration test harness asserts on this field.

**V.2 Execution Counters** — Add atomic counters to `QueryService`:
```
distributed_engine.queries.total
distributed_engine.queries.distributed
distributed_engine.queries.fallback_expected   (join/window -> DSL)
distributed_engine.queries.fallback_error      (distributed failed -> DSL)
```
After IC-3: `fallback_error == 0`. Coverage = `distributed / total`.

**V.3 Strict Mode** — Setting: `plugins.sql.distributed_engine.strict_mode`
When enabled: if a query pattern is classified as "supported" but tries to fall back to DSL for any reason, the query **fails with an error** instead of silently falling back. Only explicitly-unsupported patterns (joins, windows) are allowed to fall back.

**IC-3 passes ONLY when:**
1. ALL ~1,382 tests pass with strict mode ON (0 failures)
2. `fallback_error == 0` (no silent failures)
3. Coverage report shows which tests ran distributed vs expected-fallback
4. `_explain` for distributed queries shows stage/operator/shard info

### Verification tasks:

| ID | Task | Description | Classes | Dependencies | Owner |
|----|------|-------------|---------|-------------|-------|
| V.1 | Engine tag in response | Add `engine` field to `QueryResponse`, `ExplainResponse`. Set to `"distributed"` / `"calcite_local"` / `"legacy_dsl"` based on execution path | ~2 (modify existing) | L3.6 | exchange |
| V.2 | Execution counters | Add `DistributedEngineMetrics` with atomic counters. Expose via `_plugins/_sql/stats` endpoint | ~1 | L3.6 | exchange |
| V.3 | Strict mode | Add `plugins.sql.distributed_engine.strict_mode` setting. In strict mode, supported patterns that fall back throw `StrictModeViolationException` instead of silently falling back | ~1 (modify L3.6) | V.1 | exchange |
| V.4 | Test harness integration | Extend `PPLIntegTestCase` with `assertEngineUsed("distributed")` helper. IC-3 tests run with strict mode enabled | ~1 (modify existing) | V.3 | exchange |

**Run commands:**
```bash
# IC-3: Full Calcite IT regression with distributed engine + strict mode
./gradlew :integ-test:integTest --tests '*Calcite*IT' \
  -Dplugins.sql.distributed_engine.enabled=true \
  -Dplugins.sql.distributed_engine.strict_mode=true \
  -DignorePrometheus

# Sanity: DSL path still works (distributed disabled)
./gradlew :integ-test:integTest --tests '*Calcite*IT' \
  -Dplugins.sql.distributed_engine.enabled=false \
  -DignorePrometheus
```

### Additional validation tasks:

| ID | Task | Description | Dependencies | Owner |
|----|------|-------------|-------------|-------|
| IC3.1 | Identify supported vs unsupported patterns | Categorize all 116 IT files by which execution path they use. Track DSL fallback rate | IC-2, V.1 | planner + exchange |
| IC3.2 | Fix distributed engine failures | For each IT that fails with distributed engine, fix the engine or classify as expected fallback | IC3.1 | All agents (by specialty — see IC-3 Failure Routing in team plan) |
| IC3.3 | Verify fallback transparency | Confirm fallback produces identical results for unsupported patterns | IC3.2 | exchange |
| IC3.4 | Dual-mode comparison | Run 50 representative queries through both paths, compare results row-for-row | IC3.2 | exchange |
| IC3.5 | Distributed execution coverage report | Report showing what % of the 1,382 tests run through distributed vs fallback. Require >60% distributed | IC3.2, V.2 | exchange |

**Owner**: All agents fix failures by specialty (full team support for IC3.2 fixes)
**Gate**: ALL ~1,382 Calcite IT test methods pass with strict mode ON (0 failures). This is the **Phase 1 delivery gate**.

---

## Phase 1 Summary

| Component | New Classes | Test Classes | Owner |
|-----------|-------------|-------------|-------|
| Layer 1 (Runtime) | ~38 | ~15 | runtime |
| Layer 2a — Lucene Operators | ~4 | ~8 | lucene-ops |
| Layer 2a — Trino Operators | ~17 | ~6 | trino-ops |
| Layer 3 — Planning (L3.1-L3.4) | ~18 | ~4 | planner |
| Layer 3 — Scheduling/Router (L3.5-L3.7) | ~9 | ~4 | exchange |
| Exchange (E.1-E.4) | ~9 | ~6 | exchange |
| Verification (V.1-V.4) | ~5 | ~1 | exchange |
| **Total** | **~100** | **~44** | |

**Integration Checkpoints:**
1. **IC-1**: Local single-node execution (5 tests) — run by planner
2. **IC-2**: Scatter-gather end-to-end (9 tests) — run by runtime
3. **IC-3**: PPL Calcite IT regression (~1,382 existing tests, strict mode ON) — **Phase 1 delivery gate**, all agents fix by specialty

---

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
                                            │     └─> L3.5 (Scheduler) [exchange]
                                            │           │
                                            │           └─> L3.6 (Router) [exchange]
                                            │                 │
                                            │                 └─> L3.7 (_explain) [exchange]
                                            │
                                            └─> E.2 (OutputBuffer) [exchange]
                                                  │
                                                  └─> E.3 (GatherExchange) [exchange]

Owner key:
  L1.*: runtime    L2.1-L2.2: lucene-ops    L2.3-L2.7: trino-ops
  L3.1-L3.4, L3.8: planner    L3.5-L3.7, E.*, V.*: exchange

═══════════════════════════════════════════════════════════════
IC-1: L1.* + L2.1-L2.7         (Local single-node execution)
IC-2: IC-1 + L3.* + E.*        (Scatter-gather E2E)
IC-3: IC-2 + ALL Calcite ITs   (PPL regression — PHASE 1 GATE)
═══════════════════════════════════════════════════════════════
```
