# DQE Phase 1 — Task Breakdown

> Companion to [dqe_design_v1.md](../design/dqe_design_v1.md).
> Each task has defined input, output, contract, and tests.
> Tasks are ordered by dependency — later tasks consume earlier outputs.
>
> **Reviewed by**: Claude agent expert, Senior SDE, Senior QA

---

## Dependency Graph

```
T1 (Setting) ─────────────────────────────────────────────┐
T2 (Exchange) ──────────────┐                             │
T3 (PartialAggregate) ──────┤                             │
T4 (DSLScan) ───────────────┤                             │
T5 (PlanSplitter) ──────────┤─── T8 (DistributedExecutor) ┤─── T10 (Hook-Up)
T6 (RelNodeSerializer) ─────┤                             │      │
T7 (Transport) ─────────────┘                             │      │
                                                          │    T11 (Integration Tests)
T9 (ShardCalciteRuntime) ──── T8                          │
                                                          │
                                              T1 ─────────┘
```

Tasks T1–T7 can be developed in parallel. T8–T9 integrate them. T10 wires
everything into the existing engine. T11 validates end-to-end.

## Agent Assignment Plan

With 6 dev agents (D1–D6) + 1 QA agent (Q1):

| Sprint | Agent | Task | Notes |
|--------|-------|------|-------|
| 1 | D1 | T1 (Setting) | Small, done fast. Then joins T10. |
| 1 | D2 | T2 (Exchange) | Pure data structures, no codebase deps |
| 1 | D3 | T3 (PartialAggregate) | Pure logic, no codebase deps |
| 1 | D4 | T4 (DSLScan) | Needs OpenSearch scan knowledge |
| 1 | D5 | T6 (RelNodeSerializer) | Needs Calcite serde knowledge |
| 1 | D6 | T7 (Transport) | Needs OpenSearch transport knowledge |
| 1 | Q1 | Review unit tests from T1-T4, T6-T7 | |
| 2 | D2 | T5 (PlanSplitter) | After T2+T3 complete |
| 2 | D4+D5 | T9 (ShardCalciteRuntime) | After T4+T6 complete |
| 2 | D6 | Help T5 or T9 | Freed after T7 |
| 3 | D3 | T8 (DistributedExecutor) | After T5+T9 complete |
| 3 | D1 | T10 (Hook-Up) | After T8 complete |
| 3 | Q1 | T11 (Integration Tests) | Scriptable: generate 107 classes |

**Critical path**: T2 → T5 → T8 → T10 → T11

**Context guidance per agent**: Each task includes a "Context for Agent"
section listing the exact files to read. Agents should NOT explore beyond
these files to avoid context overflow.

---

## T1: DQE Setting

**Input**: None (greenfield)

**Output**: `plugins.query.dqe.enabled` registered as a dynamic cluster
setting, readable from `Settings.Key.DQE_ENABLED`.

**Contract**:
```
Settings.Key.DQE_ENABLED.getKeyValue() → "plugins.query.dqe.enabled"
settings.getSettingValue(Key.DQE_ENABLED) → Boolean (default false)
```

**Context for agent** (read these files):
- `common/.../setting/Settings.java` — see existing Key enum pattern
- `opensearch/.../setting/OpenSearchSettings.java` — see `CALCITE_ENGINE_ENABLED` registration pattern
- `integ-test/.../ppl/PPLIntegTestCase.java` — see `enableCalcite()` pattern

**Files to create/modify**:
- Modify `common/.../setting/Settings.java` — add `DQE_ENABLED` to `Key` enum
- Modify `opensearch/.../setting/OpenSearchSettings.java` — register the
  dynamic boolean setting
- Modify `integ-test/.../ppl/PPLIntegTestCase.java` — add `enableDQE()` / `disableDQE()`

**Tests**:
- Unit: `DQE_ENABLED` key exists and defaults to `false`
- Unit: setting is dynamic (can be changed without restart)
- Integration: `enableDQE()` / `disableDQE()` helpers toggle setting via cluster API
- Integration: setting defaults to `false` on fresh cluster

**Success criteria**: SC-1.1, SC-1.9

---

## T2: Exchange Nodes

**Input**: None (greenfield)

**Output**: Abstract `Exchange` base class and three concrete implementations.

**Contract**:
```java
// Abstract base — all exchanges implement this interface
abstract class Exchange {
    RelNode getShardPlan();
    void setShardResults(List<ShardResult> results);
    Iterator<Object[]> scan();
    RelDataType getRowType();
}

// ShardResult — one shard's output
class ShardResult {
    int shardId;
    List<Object[]> rows;
    RelDataType rowType;
}
```

**ConcatExchange**: `scan()` returns rows from all shards in arbitrary order.

**MergeSortExchange**: Constructor takes `List<RelFieldCollation>` (sort keys)
and `int limit`. `scan()` returns globally sorted rows via priority queue.
Limit is applied after merge.

**MergeAggregateExchange**: Constructor takes `List<MergeFunction>` (one per
aggregate column) and `int groupCount`. `scan()` merges partial aggregate
states by group key.

```java
enum MergeFunction { SUM_COUNTS, SUM_SUMS, SUM_DIV_COUNT, MIN_OF, MAX_OF }
```

**Context for agent** (read these files):
- `docs/design/dqe_design_v1.md` Section 3.3 (Exchange nodes) and 3.4 (Partial agg)
- No existing codebase files needed — Exchange is greenfield

**Files to create**:
- `opensearch/.../dqe/exchange/Exchange.java`
- `opensearch/.../dqe/exchange/ShardResult.java`
- `opensearch/.../dqe/exchange/ConcatExchange.java`
- `opensearch/.../dqe/exchange/MergeSortExchange.java`
- `opensearch/.../dqe/exchange/MergeAggregateExchange.java`

**Tests**:
- Unit: ConcatExchange with 0, 1, N shards — returns all rows
- Unit: MergeSortExchange with pre-sorted shard results — global order correct
- Unit: MergeSortExchange with ASC/DESC, NULLS FIRST/LAST
- Unit: MergeSortExchange with limit — returns at most K rows
- Unit: MergeAggregateExchange — COUNT merge (SUM of counts)
- Unit: MergeAggregateExchange — SUM merge, MIN merge, MAX merge
- Unit: MergeAggregateExchange — AVG merge ({sum,count} → SUM(sums)/SUM(counts))
- Unit: MergeAggregateExchange — NULL handling (all-NULL shard, mixed)
- Unit: MergeAggregateExchange — empty group on one shard, present on another

**Success criteria**: SC-1.6

---

## T3: Partial Aggregation Decomposition

**Input**: A Calcite `Aggregate` RelNode

**Output**: A `PartialAggregateSpec` describing how to split the aggregate
into shard-side partial and coordinator-side final.

**Contract**:
```java
class PartialAggregate {
    /**
     * Attempt to decompose an Aggregate into partial + final.
     * Returns Optional.empty() if the aggregate is not decomposable
     * (coordinator handles it fully).
     */
    static Optional<PartialAggregateSpec> decompose(Aggregate agg);
}

class PartialAggregateSpec {
    Aggregate shardAggregate;    // partial agg to run on shard
    List<MergeFunction> mergeFunctions;  // one per agg call
    int groupCount;              // number of GROUP BY keys
}
```

**Decomposition rules**:
| AggCall | Decomposable? | Shard outputs | MergeFunction |
|---------|--------------|---------------|---------------|
| COUNT   | Yes          | COUNT → 1 col | SUM_COUNTS    |
| SUM     | Yes          | SUM → 1 col   | SUM_SUMS      |
| AVG     | Yes          | SUM, COUNT → 2 cols | SUM_DIV_COUNT |
| MIN     | Yes          | MIN → 1 col   | MIN_OF        |
| MAX     | Yes          | MAX → 1 col   | MAX_OF        |
| Other   | No           | (not split)   | —             |

When an Aggregate contains a mix of decomposable and non-decomposable calls,
`decompose()` returns `empty()` — the entire aggregate runs on coordinator.

**Files to create**:
- `opensearch/.../dqe/agg/PartialAggregate.java`
- `opensearch/.../dqe/agg/PartialAggregateSpec.java`

**Context for agent** (read these files):
- `org.apache.calcite.rel.core.Aggregate` — Calcite Aggregate RelNode
- `org.apache.calcite.rel.core.AggregateCall` — individual agg function call

**Tests**:
- Unit: `COUNT(*)` decomposes → shard COUNT, merge SUM_COUNTS
- Unit: `COUNT(field)` decomposes → shard COUNT(field), merge SUM_COUNTS
- Unit: `COUNT(DISTINCT x)` → returns empty (not decomposable — per-shard
  distinct counts can't be merged because values overlap across shards.
  Falls back to ConcatExchange: shards send raw rows, coordinator computes
  COUNT(DISTINCT) over all data. Correct, but not optimized.)
- Unit: `AVG(x)` decomposes → shard SUM + COUNT (2 columns), merge SUM_DIV_COUNT
- Unit: `SUM(x), MIN(y), MAX(z)` all decompose in same aggregate
- Unit: `PERCENTILE_APPROX(x)` → returns empty (non-decomposable)
- Unit: mixed decomposable + non-decomposable → returns empty
- Unit: NULL handling — SUM of all-NULL returns NULL, not 0
- Unit: empty input — COUNT returns 0, SUM/AVG/MIN/MAX return NULL
- Unit: overflow — SUM uses widened types (long for int input)
- Unit: aggregate with 0 GROUP BY keys (global aggregate)

**Success criteria**: SC-1.5

---

## T4: DSLScan

**Input**: `PushDownContext` (from retained pushdown rules: filter, project,
limit, sort)

**Output**: A Calcite `EnumerableRel` that executes a shard-local search
and returns typed rows.

**Contract**:
```java
// DSLScan follows the same pattern as CalciteEnumerableIndexScan:
//   extends AbstractCalciteIndexScan (which extends TableScan)
//   implements EnumerableRel
class DSLScan extends AbstractCalciteIndexScan implements EnumerableRel {
    DSLScan(RelOptCluster cluster, RelTraitSet traits,
            List<RelHint> hints, RelOptTable table,
            OpenSearchIndex index, RelDataType schema,
            PushDownContext pushDownContext);

    // Calcite calls this to get the code-gen result
    Result implement(EnumerableRelImplementor impl, Prefer pref);
}
```

**Note on base class**: The existing `CalciteEnumerableIndexScan` extends
`AbstractCalciteIndexScan` (which extends `TableScan`) and implements
`EnumerableRel`. DSLScan follows the same pattern — it does NOT extend
`AbstractEnumerableRel`. This ensures it fits into the existing planner
framework and can reuse `PushDownContext` from the abstract base.

DSLScan builds `SearchSourceBuilder` from `PushDownContext`:
- `query()` from pushed-down filters (via `pushDownContext.createRequestBuilder()`)
- `fetchSource()` / `docValueField()` from projected fields
- `size()` from pushed-down limit
- `sort()` from pushed-down sort
- `searchAfter()` for pagination

**Also**: `DSLScanRule` — a Calcite `ConverterRule` that converts
`CalciteLogicalIndexScan` → `DSLScan` (replaces `EnumerableIndexScanRule`
in DQE mode).

**Context for agent** (read these files):
- `opensearch/.../storage/scan/CalciteEnumerableIndexScan.java` — reference implementation
- `opensearch/.../storage/scan/AbstractCalciteIndexScan.java` — base class
- `opensearch/.../storage/scan/context/PushDownContext.java` — input contract
- `opensearch/.../request/OpenSearchRequestBuilder.java` — builds SearchSourceBuilder

**Files to create**:
- `opensearch/.../dqe/DSLScan.java`
- `opensearch/.../dqe/DSLScanRule.java`

**Tests**:
- Unit: DSLScan builds correct SearchSourceBuilder from PushDownContext
  with filter, project, limit, sort
- Unit: DSLScan with empty PushDownContext → match_all query
- Unit: DSLScan with empty index → returns 0 rows (not error)
- Unit: DSLScan with index alias → resolves correctly
- Unit: DSLScanRule matches CalciteLogicalIndexScan and produces DSLScan
- Integration (via T11): queries with DQE enabled produce correct results

**Success criteria**: SC-1.3

---

## T5: PlanSplitter

**Input**: An optimized Calcite `RelNode` plan (after Hep/Volcano planning
with DQE-mode rules applied)

**Output**: A `DistributedPlan` containing the coordinator fragment (with
Exchange nodes) and the shard fragment (below the Exchange).

**Contract**:
```java
class PlanSplitter {
    /**
     * Walk the plan bottom-up. Insert Exchange at shard/coordinator boundary.
     * Returns DistributedPlan with exchange nodes.
     * If no distribution needed (e.g., system index query), returns null.
     */
    static DistributedPlan split(RelNode plan);
}

class DistributedPlan {
    RelNode coordinatorPlan;     // top of plan, contains Exchange nodes
    List<Exchange> exchanges;    // all Exchange nodes (for executor to process)
}
```

**Operator classification** (from design doc Section 3.1):
- SHARD: Scan, Filter, Project, Calc (eval), PartialAggregate, Sort+Limit
- COORDINATOR: FinalAggregate, Sort(no limit), Window, Join, Dedup
- DEFAULT: any unrecognized operator → COORDINATOR

Note: `Calc` (Calcite's fused filter+project) is SHARD-local — it computes
expressions row-by-row with no cross-shard dependency. This is the key win
for `eval` on shards.

**Exchange selection**:
- Aggregate above shard ops → decompose via `PartialAggregate.decompose()`.
  If decomposable: insert `MergeAggregateExchange`. Else: insert
  `ConcatExchange` (all rows to coordinator).
- Sort + Limit above shard ops → insert `MergeSortExchange` (shard does
  local TopK, coordinator merge-sorts).
- Everything else → insert `ConcatExchange`.

**Context for agent** (read these files):
- `docs/design/dqe_design_v1.md` Section 3.1 (operator placement table)
- `docs/design/dqe_design_v1.md` Section 5 (example query plans)
- T2 contract (Exchange interface) and T3 contract (PartialAggregate.decompose)

**Files to create**:
- `opensearch/.../dqe/PlanSplitter.java`
- `opensearch/.../dqe/DistributedPlan.java`

**Dependencies**: T2 (Exchange), T3 (PartialAggregate)

**Tests**:
- Unit: simple filter → ConcatExchange above DSLScan
- Unit: filter + agg (COUNT by b) → MergeAggregateExchange with PartialAgg
- Unit: filter + eval + agg → MergeAggregateExchange with Calc above DSLScan
- Unit: sort + limit → MergeSortExchange
- Unit: sort without limit → ConcatExchange (sort on coordinator)
- Unit: window function → ConcatExchange (window on coordinator)
- Unit: join → ConcatExchange on both inputs
- Unit: unrecognized operator → ConcatExchange (safe default)
- Unit: non-decomposable agg → ConcatExchange (raw rows to coordinator)
- Unit: system index scan → returns null (no distribution)
- Unit: subquery (EXISTS, IN) → ConcatExchange on subquery input
- Unit: nested aggregation (stats inside stats) → correct split
- Unit: Calc (eval) between scan and agg → Calc stays on shard

**Success criteria**: SC-1.2

---

## T6: RelNodeSerializer

**Input**: A Calcite `RelNode` subtree (the shard fragment extracted by
PlanSplitter)

**Output**: JSON string that can be deserialized back to an equivalent
`RelNode` on a different node.

**Contract**:
```java
class RelNodeSerializer {
    /** Serialize a RelNode subtree to JSON. */
    static String serialize(RelNode node);

    /**
     * Deserialize a JSON string back to a RelNode.
     * Requires RelOptCluster and schema for type reconstruction.
     */
    static RelNode deserialize(String json, RelOptCluster cluster,
                               RelOptSchema schema);
}
```

**Supported RelNode types** (Phase 1):
- `DSLScan` (serialized as index name + PushDownContext)
- `LogicalFilter` / `EnumerableFilter`
- `LogicalProject` / `EnumerableProject`
- `LogicalAggregate` / `EnumerableAggregate`
- `LogicalSort` / `EnumerableSort`
- `LogicalCalc` / `EnumerableCalc`

**RexNode support**: `RexLiteral`, `RexInputRef`, `RexCall` (including
built-in functions and PPL UDFs), `RexFieldAccess`.

**Context for agent** (read these files):
- Calcite's `org.apache.calcite.rel.externalize.RelJsonWriter` / `RelJsonReader`
- `opensearch/.../storage/scan/CalciteEnumerableIndexScan.java` — DSLScan serialization reference
- `opensearch/.../storage/scan/context/PushDownContext.java` — must serialize this

**Files to create**:
- `opensearch/.../dqe/serde/RelNodeSerializer.java`

**Tests**:
- Unit: round-trip for each supported RelNode type
- Unit: round-trip for DSLScan with PushDownContext (filter + project + limit)
- Unit: round-trip for Filter with complex RexCall (e.g., `abs(a) > 10`)
- Unit: round-trip for Aggregate with GROUP BY and multiple agg calls
- Unit: round-trip for Sort with ASC/DESC, NULLS FIRST/LAST
- Unit: round-trip preserves RelDataType (column names, types, nullability)
- Unit: round-trip for RexLiteral of each SqlTypeName (VARCHAR, INTEGER,
  DOUBLE, BOOLEAN, DATE, TIMESTAMP)
- Unit: deserialize with UDF function registry (GEOIP, etc.)
- Unit: unsupported RelNode type → clear error message

**Success criteria**: SC-1.4

---

## T7: Transport Layer

**Input**: Serialized plan JSON (from T6), index name, shard ID

**Output**: `CalciteShardResponse` containing typed rows or error.

**Contract**:
```java
// Action type
class CalciteShardAction extends ActionType<CalciteShardResponse> {
    static final String NAME = "indices:data/read/opensearch/calcite/shard";
    static final CalciteShardAction INSTANCE = new CalciteShardAction();
}

// Request
class CalciteShardRequest extends TransportRequest {
    String planJson;        // serialized shard plan
    String indexName;
    int shardId;
    // Security context propagated via ThreadContext (standard OpenSearch)
}

// Response
class CalciteShardResponse extends TransportResponse {
    List<Object[]> rows;           // typed result rows
    List<String> columnNames;
    List<SqlTypeName> columnTypes;
    Exception error;               // null on success
}

// Handler
class TransportCalciteShardAction
    extends HandledTransportAction<CalciteShardRequest, CalciteShardResponse> {
    // Receives request, executes via ShardCalciteRuntime (T9), returns response
}
```

**Context for agent** (read these files):
- `plugin/.../transport/TransportPPLQueryAction.java` — reference for HandledTransportAction pattern
- `plugin/.../SQLPlugin.java` — see `getActions()` for registration pattern
- OpenSearch `TransportRequest` / `TransportResponse` Writeable interface

**Files to create**:
- `plugin/.../transport/CalciteShardAction.java`
- `plugin/.../transport/CalciteShardRequest.java`
- `plugin/.../transport/CalciteShardResponse.java`
- `plugin/.../transport/TransportCalciteShardAction.java`

**Files to modify**:
- `plugin/.../SQLPlugin.java` — register action (Hook-Up Point 3)

**Tests**:
- Unit: CalciteShardRequest serialization/deserialization (Writeable)
- Unit: CalciteShardResponse serialization/deserialization (Writeable)
- Unit: CalciteShardResponse with error — error propagated
- Integration (via T11): transport action invoked, response received

**Success criteria**: part of SC-1.10

---

## T8: DistributedExecutor

**Input**: `DistributedPlan` (from T5), `NodeClient`, `ClusterService`

**Output**: `QueryResponse` (same format as existing engine output)

**Contract**:
```java
class DistributedExecutor {
    DistributedExecutor(NodeClient client, ClusterService clusterService,
                        ShardRoutingResolver resolver,
                        RelNodeSerializer serializer);

    /**
     * Execute a distributed plan:
     * 1. Extract shard plan from each Exchange node
     * 2. Serialize shard plan via RelNodeSerializer
     * 3. Resolve active shards via ShardRoutingResolver
     * 4. Dispatch CalciteShardRequest to each shard via transport
     * 5. Collect CalciteShardResponse from all shards
     * 6. Feed responses into Exchange.setShardResults()
     * 7. Execute coordinator fragment (reads from Exchange.scan())
     * 8. Build QueryResponse
     *
     * Fail-fast: if any shard returns error, throw immediately.
     */
    void execute(DistributedPlan plan, ResponseListener<QueryResponse> listener);
}

class ShardRoutingResolver {
    /**
     * Returns list of active primary shard routings for the given index.
     */
    List<ShardRouting> resolve(String indexName);
}
```

**Context for agent** (read these files):
- T2 contract (Exchange interface: getShardPlan, setShardResults, scan)
- T5 contract (DistributedPlan: coordinatorPlan + exchanges list)
- T6 contract (RelNodeSerializer.serialize/deserialize)
- T7 contract (CalciteShardRequest/Response, TransportCalciteShardAction)
- `opensearch/.../executor/OpenSearchExecutionEngine.java` — existing execute() pattern

**Files to create**:
- `opensearch/.../dqe/DistributedExecutor.java`
- `opensearch/.../dqe/ShardRoutingResolver.java`

**Dependencies**: T2 (Exchange), T5 (PlanSplitter), T6 (RelNodeSerializer),
T7 (Transport), T9 (ShardCalciteRuntime)

**Tests**:
- Unit: ShardRoutingResolver returns correct shards for a given index
- Unit: ShardRoutingResolver — index with 1 shard → 1 routing
- Unit: ShardRoutingResolver — index alias → resolves to concrete index shards
- Unit: DistributedExecutor dispatches to N shards, collects N responses
- Unit: DistributedExecutor — ConcatExchange integration (rows concatenated)
- Unit: DistributedExecutor — MergeAggregateExchange integration (agg merged)
- Unit: DistributedExecutor — MergeSortExchange integration (sorted + limited)
- Unit: DistributedExecutor — all shards return empty → empty result
- **Fail-fast tests (SC-1.11)**:
  - Unit: one shard returns error → entire query fails with error propagated
  - Unit: serialization failure → query fails with clear message
  - Unit: transport error (shard unreachable) → query fails, no silent fallback
  - Unit: verify error message includes shard ID and root cause

**Success criteria**: SC-1.10, SC-1.11

---

## T9: ShardCalciteRuntime

**Input**: `CalciteShardRequest` (plan JSON, index name, shard ID)

**Output**: `CalciteShardResponse` (typed rows or error)

**Contract**:
```java
class ShardCalciteRuntime {
    /**
     * Execute a serialized plan fragment on a local shard:
     * 1. Deserialize plan JSON via RelNodeSerializer
     * 2. Reconstruct RelOptCluster with OpenSearchTypeFactory
     * 3. Bind DSLScan to local shard (index + shard ID)
     * 4. Execute Calcite Enumerable pipeline
     * 5. Collect typed Object[] rows
     * 6. Return as CalciteShardResponse
     *
     * All exceptions caught and returned in response.error.
     */
    CalciteShardResponse execute(CalciteShardRequest request);
}
```

**Files to create**:
- `opensearch/.../dqe/ShardCalciteRuntime.java`

**Dependencies**: T4 (DSLScan), T6 (RelNodeSerializer)

**Tests**:
- Unit: deserialize + execute a simple scan plan → rows returned
- Unit: deserialize + execute filter + project plan → filtered/projected rows
- Unit: deserialize + execute partial aggregate → aggregated rows
- Unit: malformed plan JSON → error in response (not exception)
- Unit: shard row limit exceeded → clear error in response

**Success criteria**: part of SC-1.3, SC-1.10

---

## T10: Hook-Up and Rule Gating

**Input**: All components from T1–T9

**Output**: DQE integrated into the existing query engine, controlled by
`plugins.query.dqe.enabled`.

**Contract**: When `dqe.enabled=true`:
1. `CalciteLogicalIndexScan.register()` adds `DQE_PUSHDOWN_RULES` + `DSLScanRule`
   (not `PUSHDOWN_RULES`, not `EnumerableIndexScanRule`)
2. `OpenSearchExecutionEngine.execute(RelNode)` calls `PlanSplitter.split()`
   then `DistributedExecutor.execute()`
3. `FilterIndexScanRule` does not push script-based filters

When `dqe.enabled=false`: existing behavior is unchanged.

**Files to modify**:
- `opensearch/.../storage/scan/CalciteLogicalIndexScan.java` — rule gating
  in `register()`
- `opensearch/.../executor/OpenSearchExecutionEngine.java` — DQE branch in
  `execute(RelNode, CalcitePlanContext, ResponseListener)`
- `opensearch/.../planner/rules/OpenSearchIndexRules.java` — add
  `DQE_PUSHDOWN_RULES` list

**Dependencies**: T1 (Setting), T4 (DSLScanRule), T5 (PlanSplitter),
T8 (DistributedExecutor)

**Tests**:
- Unit: with DQE enabled, `register()` adds only DQE rules
- Unit: with DQE disabled, `register()` adds all legacy rules (unchanged)
- Unit: with DQE enabled, `execute()` routes through DistributedExecutor
- Unit: with DQE disabled, `execute()` routes through existing JDBC path
- Unit: FilterIndexScanRule with DQE enabled — script filter NOT pushed
- Unit: FilterIndexScanRule with DQE enabled — inverted-index filter pushed
- Integration: toggle setting at runtime, next query uses correct path

**Success criteria**: SC-1.8, SC-1.9

---

## T11: Integration Tests

**Input**: All components wired (T10 complete)

**Output**: 107 `Distributed*IT` test classes passing.

**Contract**: Each `Distributed*IT` extends its `Calcite*IT` counterpart and
calls `enableCalcite()` + `enableDQE()` in `init()`. All inherited test
methods must produce identical results under DQE.

```java
// Pattern for each of the 107 classes
public class Distributed{X}IT extends Calcite{X}IT {
    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        enableDQE();
    }
}
```

**Excluded** (2 classes):
- `CalciteExplainIT` — DQE produces different plan output
- `CalcitePPLExplainIT` — same reason

**Note for agent**: The 107 class files are mechanical — each is 10 lines
following the same pattern. This can be scripted: list all `Calcite*IT.java`
files, exclude the 2 explain tests, generate `Distributed*IT.java` for each.

**Files to create**:
- 107 files in `integ-test/.../calcite/distributed/Distributed*IT.java`
- 1 file: `DistributedDQESpecificIT.java` — DQE-specific tests (see below)

**Files to modify**:
- `integ-test/.../ppl/PPLIntegTestCase.java` — add `enableDQE()` /
  `disableDQE()` helpers (includes `@After` cleanup calling `disableDQE()`)

**Dependencies**: T10 (Hook-Up complete)

**Tests**: The 107 mirror classes inherit all test methods from their parent
`Calcite*IT` classes. Categories covered:

| Category | Example Classes |
|----------|----------------|
| Basic PPL | DistributedPPLBasicIT, DistributedWhereCommandIT |
| Aggregation | DistributedPPLAggregationIT, DistributedStatsCommandIT |
| Eval/Project | DistributedEvalCommandIT, DistributedFieldsCommandIT |
| Sort/Limit | DistributedSortCommandIT, DistributedHeadCommandIT |
| Join | DistributedPPLJoinIT, DistributedPPLLookupIT |
| Window | DistributedPPLEventstatsIT |
| Dedup | DistributedDedupCommandIT, DistributedPPLDedupIT |
| Subquery | DistributedPPLInSubqueryIT, DistributedPPLScalarSubqueryIT |
| Functions | DistributedMathematicalFunctionIT, DistributedDateTimeFunctionIT |
| Data types | DistributedDataTypeIT, DistributedIPFunctionsIT |
| Relevance | DistributedRelevanceFunctionIT, DistributedMatchIT |
| Format | DistributedCsvFormatIT, DistributedVisualizationFormatIT |

**Additional DQE-specific tests** (in `DistributedDQESpecificIT`):
- Partial aggregation correctness: `stats count(), sum(x), avg(x) by key`
  on multi-shard index — verify results match single-node Calcite
- Eval on shard: `eval score=price*qty | where score > 100` — verify only
  filtered rows returned (not all rows)
- TopK across shards: `sort price | head 10` on multi-shard index — verify
  globally correct top-10
- Script filter NOT pushed: `where abs(a)=1` — verify Calcite Filter on
  shard (not Painless script)
- Fail-fast: query with DQE enabled on a query type that forces error →
  error returned, not silent fallback
- Setting toggle: run query with DQE off, toggle on, run same query, toggle
  off, run again — all produce identical results

**Classes that may need overrides** (not just mirror):
- `CalciteSettingsIT` — may test pushdown-specific settings; review if DQE
  changes affect assertions
- `CalciteResourceMonitorIT` — resource monitoring may differ under DQE
- `CalcitePrometheusDataSourceCommandsIT` — Prometheus datasource does not
  go through DQE (no OpenSearch shards); should skip DQE enablement

**Success criteria**: SC-1.7

---

## SC → Test Traceability Matrix

| SC | Description | Task(s) | Test(s) |
|----|-------------|---------|---------|
| SC-1.1 | DQE setting dynamic | T1 | T1: unit + integration |
| SC-1.2 | PlanSplitter classifies operators | T5 | T5: 13 unit tests |
| SC-1.3 | DSLScan shard-local search | T4, T9 | T4: 5 unit + T9: 5 unit |
| SC-1.4 | Serializer round-trips | T6 | T6: 9 unit tests |
| SC-1.5 | Partial agg correctness | T3 | T3: 11 unit tests |
| SC-1.6 | MergeSortExchange | T2 | T2: 9 unit tests |
| SC-1.7 | 107 Distributed*IT pass | T11 | T11: 107 classes + DQE-specific |
| SC-1.8 | Rule gating | T10 | T10: 6 unit tests |
| SC-1.9 | Setting toggle | T1, T10 | T1: integration + T10: integration |
| SC-1.10 | Executor dispatch | T7, T8, T9 | T7: 3 unit + T8: 8 unit + T9: 5 unit |
| SC-1.11 | Fail-fast | T8 | T8: 4 fail-fast unit tests |

---

## Task Summary

| Task | Component | Dependencies | Success Criteria |
|------|-----------|-------------|-----------------|
| T1  | DQE Setting | — | SC-1.1, SC-1.9 |
| T2  | Exchange Nodes | — | SC-1.6 |
| T3  | PartialAggregate | — | SC-1.5 |
| T4  | DSLScan | — | SC-1.3 |
| T5  | PlanSplitter | T2, T3 | SC-1.2 |
| T6  | RelNodeSerializer | — | SC-1.4 |
| T7  | Transport Layer | — | SC-1.10 (part) |
| T8  | DistributedExecutor | T2, T5, T6, T7, T9 | SC-1.10, SC-1.11 |
| T9  | ShardCalciteRuntime | T4, T6 | SC-1.3, SC-1.10 |
| T10 | Hook-Up & Rule Gating | T1, T4, T5, T8 | SC-1.8, SC-1.9 |
| T11 | Integration Tests | T10 | SC-1.7 |

**Parallelism**: T1, T2, T3, T4, T6, T7 can all be developed in parallel.
T5 starts after T2 + T3. T9 starts after T4 + T6. T8 integrates everything.
T10 wires it up. T11 validates.
