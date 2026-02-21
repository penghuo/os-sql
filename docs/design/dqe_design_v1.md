# Distributed Query Engine (DQE) -- High-Level Design

**Status**: FINAL (v1)
**Authors**: dqe-design team
**Date**: 2026-02-20
**Review**: Reviewed by team-lead and senior SDE reviewer

---

## 1. Problem Statement

The current Calcite-based query engine translates RelNode plans into OpenSearch
DSL for execution. This translation approach has three fundamental limitations:

1. **DSL cannot express all RelNodes.** Window functions, joins, and complex
   expressions have no DSL equivalent. These queries either fail or fall back to
   inefficient coordinator-side execution.

2. **Operators do not execute on shards.** A query like
   `source=index | eval a=b+1` pulls every row to the coordinator, computes the
   expression there, and then returns results. There is no mechanism to push
   Calcite operators to the shard level.

3. **Pushdown rules are complex and error-prone.** There are 15 hand-coded Hep
   and Volcano rules that translate between Calcite RelNodes and OpenSearch DSL
   constructs. Each new PPL feature requires writing a new rule, which is
   fragile and hard to test.

**Root cause**: the system translates between two paradigms (Calcite RelNode and
OpenSearch DSL). Each paradigm has its own semantics, optimizer, and execution
model. Bridging them requires an ever-growing set of translation rules.

**Solution**: stop translating. Ship Calcite plan fragments directly to shards.
Use DSL only at the leaf level for inverted-index access (filters, projections,
relevance functions). Everything above the leaf executes as native Calcite
Enumerable operators -- on the shard.

---

## 2. Architecture Overview

```
                              COORDINATOR NODE
  +------------------------------------------------------------------------+
  |  PPL query                                                             |
  |    |                                                                   |
  |    v                                                                   |
  |  ANTLR Parser --> AST --> Analyzer --> CalciteRelNodeVisitor            |
  |                                           |                            |
  |                               HepPlanner + VolcanoPlanner              |
  |                           (only Filter/Project/Relevance pushdown)     |
  |                                           |                            |
  |                                     PlanSplitter  <--- NEW             |
  |                                      /        \                        |
  |                          +-----------+         +-------------+         |
  |                          | Shard     |         | Coordinator |         |
  |                          | Fragment  |         | Fragment    |         |
  |                          +-----------+         +-------------+         |
  |                               |                      |                 |
  |                          Serialize to           Exchange nodes:        |
  |                          JSON via               - ConcatExchange       |
  |                          RelNodeSerializer      - MergeSortExchange    |
  |                               |                 - MergeAggExchange     |
  +-------------------------------|----------------------------------------+
                                  | TransportCalciteShardAction
                                  | (scatter to shards)
                                  v
  +------------------------------------------------------------------------+
  |                            DATA NODE (per shard)                       |
  |                                                                        |
  |  ShardCalciteRuntime                                                   |
  |    |                                                                   |
  |    +-- Deserialize JSON --> RelNode subtree                            |
  |    |                                                                   |
  |    +-- DSLScan (leaf operator)                                         |
  |    |     - Build SearchSourceBuilder from PushDownContext               |
  |    |     - docvalue_fields for typed columns (_source: false)          |
  |    |     - search_after pagination (shard-scoped)                      |
  |    |     - DLS/FLS enforced by security plugin                         |
  |    |     - Returns typed rows                                          |
  |    |                                                                   |
  |    +-- Calcite Enumerable pipeline (above DSLScan)                     |
  |          - Filter (expressions Lucene cannot evaluate)                 |
  |          - Project / Eval (compute new columns)                        |
  |          - Partial Aggregate (COUNT, SUM, AVG, MIN, MAX)               |
  |          - Sort + Limit (local TopK)                                   |
  |                                                                        |
  |    Returns: typed rows --> coordinator                                 |
  +------------------------------------------------------------------------+
```

### Key Design Principles

- **One tree-walk replaces 15 rules.** `PlanSplitter` inserts Exchange nodes
  at the shard/coordinator boundary in a single post-order traversal. No new
  pushdown rules are needed as PPL features are added.

- **Security preserved by construction.** `DSLScan` routes through the standard
  OpenSearch `IndexShard` search path. DLS/FLS, RBAC, and audit logging apply
  automatically with no changes to the security plugin.

- **Same PPL interface.** The PPL-to-AST-to-RelNode input pipeline is entirely
  unchanged. Only the execution backend changes.

- **Opt-in and reversible.** A single dynamic cluster setting toggles between
  DQE and legacy paths. No restart required.

---

## 3. Core Components

### 3.1 PlanSplitter

The PlanSplitter is a post-order RelNode visitor that walks the optimized
Calcite plan and inserts Exchange nodes at the boundary between shard-local
operators and coordinator-side operators.

**Algorithm (single pass, post-order):**

1. Walk the plan bottom-up.
2. Classify each operator as SHARD-LOCAL or COORDINATOR.
3. When a COORDINATOR operator has a SHARD-LOCAL child, insert the appropriate
   Exchange node between them.
4. Choose the Exchange type based on the coordinator operator:
   - Aggregate above scan --> MergeAggregateExchange (with partial agg on shard)
   - Sort + Limit above scan --> MergeSortExchange
   - Everything else --> ConcatExchange

**Operator Placement Table:**

| Operator                        | Location     | Exchange Type           |
|---------------------------------|--------------|-------------------------|
| Scan                            | Shard        | --                      |
| Filter (simple, e.g. `a=1`)    | Shard (Lucene via PredicateAnalyzer) | -- |
| Filter (complex, e.g. `fn(a)>b`) | Shard (Calcite Filter)             | -- |
| Eval / Project                  | Shard        | --                      |
| Partial Aggregate (COUNT, SUM, AVG, MIN, MAX) | Shard | MergeAggregateExchange |
| Sort + Limit (TopK)            | Shard + Coordinator | MergeSortExchange |
| Sort (no limit)                 | Coordinator  | ConcatExchange          |
| Window function                 | Coordinator  | ConcatExchange          |
| Join                            | Coordinator  | ConcatExchange          |
| Dedup                           | Coordinator  | ConcatExchange          |

The PlanSplitter does not need to know about every possible PPL command. Any
operator it does not recognize is placed on the coordinator by default, which is
always correct (though not always optimal). This is the fundamental advantage
over the pushdown-rule approach: the system is correct by default and
optimizable incrementally.

### 3.2 DSLScan -- Unified Leaf Operator

DSLScan is the only operator that interacts with the OpenSearch search
infrastructure. It replaces the current `CalciteEnumerableIndexScan` when
running in DQE mode.

**How it works:**

1. Receives a `PushDownContext` containing: QueryBuilder (from pushed-down
   filters), projected field list, sort specification, and limit.
2. Builds a `SearchSourceBuilder` with:
   - `docvalue_fields` for all DocValues-enabled columns (`_source: false`)
   - `stored_fields` / `_source` only for text-only fields
   - The pushed-down QueryBuilder (term queries, bool queries, relevance queries)
3. Executes shard-local search via `IndexShard` -- no network hop.
4. Paginates via `search_after` scoped to the local shard.
5. Converts `SearchHit.field()` typed values into typed Calcite rows.

**What comes for free from the search layer:**

| Capability               | Mechanism                                         |
|--------------------------|---------------------------------------------------|
| DLS/FLS                  | Security plugin rewrites query / strips fields    |
| Type coercion            | `docvalue_fields` returns typed values             |
| Field formatting         | `fieldformat` applied by search layer              |
| All field types          | keyword, text, integer, date, geo_point, nested    |
| Query cache              | Shard-level query cache on repeated filters        |
| Concurrent segment search | OpenSearch 2.12+ internal optimization            |

### 3.3 Exchange Nodes

Exchange nodes sit at the boundary between shard-local and coordinator-level
execution. They are responsible for: (a) defining what runs on each shard,
(b) receiving shard results, and (c) merging them.

All Exchange nodes share a common interface:

- `getShardPlan()` -- returns the RelNode subtree to serialize and ship to shards.
- `setShardResults(List<ShardResult>)` -- called by the coordinator after
  collecting results from all shards.
- `scan()` -- returns an iterator over the merged result.

**ConcatExchange**: Concatenates shard results in arbitrary order. Used for
simple scans, filters, and eval-only queries. Cheapest exchange -- no merging
logic.

**MergeSortExchange**: Maintains a priority queue across shard result iterators.
Produces globally sorted output. Used when the query has `sort + limit` (TopK
pattern). Each shard independently sorts and limits locally; the coordinator
merge-sorts the pre-sorted shard results.

**MergeAggregateExchange**: Merges partial aggregate states from shards into
final results. Each shard computes partial aggregates (e.g., local COUNT, local
SUM). The coordinator combines them using aggregation-specific merge functions.

### 3.4 Partial Aggregation

When PlanSplitter encounters an Aggregate operator above a scan, it decomposes
it into a shard-side partial aggregate and a coordinator-side final aggregate.

| Function | Shard Partial State      | Coordinator Merge              |
|----------|--------------------------|--------------------------------|
| COUNT    | count (long)             | SUM of counts                  |
| SUM      | sum (double/long)        | SUM of sums                    |
| AVG      | {sum, count} (2 columns) | SUM(sums) / SUM(counts)        |
| MIN      | min (comparable)         | MIN of mins                    |
| MAX      | max (comparable)         | MAX of maxes                   |

Non-decomposable aggregates (e.g., PERCENTILE, DISTINCT_COUNT without
approximation) fall back to coordinator-side full aggregation. The shard sends
raw rows and the coordinator computes the aggregate over all data. This is
correct but not optimal -- it is the same behavior as today.

### 3.5 Transport Layer

Communication between coordinator and data nodes uses OpenSearch's existing
transport infrastructure:

- **Action name**: `indices:data/read/opensearch/calcite/shard`
- **Request** (`CalciteShardRequest`): serialized plan JSON, index name, shard
  ID, security context
- **Response** (`CalciteShardResponse`): `List<Object[]>` typed rows, column
  metadata, optional error

Auth, TLS, node-to-node encryption, and circuit breakers all come from the
standard OpenSearch transport layer. No custom networking code is needed.

**Scatter-gather pattern:**

```
Client           Coordinator                  Shard-0        Shard-1
  |                  |                           |              |
  |-- PPL ---------> |                           |              |
  |                  |-- split(plan)             |              |
  |                  |   (local: PlanSplitter    |              |
  |                  |    inserts Exchange nodes) |              |
  |                  |                           |              |
  |                  |-- ShardReq(plan,0) ------>|              |
  |                  |-- ShardReq(plan,1) --------------------->|
  |                  |                           |              |
  |                  |                    execute(plan)  execute(plan)
  |                  |                           |              |
  |                  |<-- ShardResp(rows) -------|              |
  |                  |<-- ShardResp(rows) ----------------------|
  |                  |                           |              |
  |                  |-- Exchange.merge(responses)|              |
  |                  |   (coordinator-local)      |              |
  |                  |                           |              |
  |<-- results ------|                           |              |
```

---

## 4. Pushdown Rules in DQE Mode

In DQE mode, the Volcano and Hep planners retain only a small subset of the
existing pushdown rules. The rest are unnecessary because shard-side Calcite
operators handle what those rules were translating into DSL.

### Rules Retained

| Rule                             | Reason                                     |
|----------------------------------|--------------------------------------------|
| `FilterIndexScanRule`            | Push Lucene-expressible predicates to inverted index. Only pushes what `PredicateAnalyzer` can convert; complex expressions (e.g., `abs(a)=1`) remain as Calcite Filter above the scan. See note below on partial pushdown. |
| `ProjectIndexScanRule`           | Column pruning at scan level -- avoids reading unused columns from disk |
| `LimitIndexScanRule`             | Push `SearchSourceBuilder.size(N)` to DSLScan -- prevents reading entire shard for `head N` queries (see Design Decision D2) |
| `SortIndexScanRule`              | Push sort to Lucene's `TopFieldCollector` for TopK patterns -- leverages BKD trees and early termination (see Design Decision D2) |
| `RelevanceFunctionPushdownRule`  | Relevance functions (match, match_phrase, etc.) must execute in Lucene |

**Note: `EnumerableIndexScanRule` is not retained.** This rule converts
`CalciteLogicalIndexScan` → `CalciteEnumerableIndexScan`, which is the current
non-DQE physical scan operator. In DQE mode, `DSLScan` replaces
`CalciteEnumerableIndexScan` as the leaf operator. A new rule or direct
construction in PlanSplitter converts the logical scan to `DSLScan` instead.

**Note: FilterIndexScanRule and complex expressions.** The current
`FilterIndexScanRule` calls `PredicateAnalyzer.analyzeExpression()`, which
classifies each predicate as either:

- **Fully analyzable** (e.g., `a=1` → `TermQuery`) — pushed entirely to DSL
- **Partially analyzable** (e.g., `a=1 AND abs(b)>2`) — the Lucene-expressible
  part (`a=1`) is pushed to DSL; the rest (`abs(b)>2`) remains as a Calcite
  `Filter` above the scan
- **Script-based** (e.g., `abs(a)=1`) — converted to a script query in DSL
- **Unanalyzable** — not pushed, remains as Calcite `Filter`

In DQE mode, this rule should **not** push script-based filters to DSL.
Script queries execute inside Lucene and lose the benefit of shard-side
Calcite evaluation. Instead, the DQE-mode `FilterIndexScanRule` should only
push inverted-index predicates (term, range, bool, match) and leave all
complex/script expressions as Calcite Filter operators on the shard.

Example: `source=index | where abs(a)=1`

```
SHARD:
  Filter(abs(a) = 1)            <-- Calcite Filter (NOT pushed as script)
    DSLScan(index, fields=[a])
```

Example: `source=index | where status='active' AND abs(score)>10`

```
SHARD:
  Filter(abs(score) > 10)       <-- Calcite Filter (complex part)
    DSLScan(index, query=TermQuery("status","active"), fields=[status,score])
                                ^-- inverted-index part pushed to DSL
```

### Rules Removed

| Rule                              | Reason                                     |
|-----------------------------------|--------------------------------------------|
| `AggregateIndexScanRule`          | Aggregation handled by Calcite Enumerable on shard via PartialAggregate |
| `SortExprIndexScanRule`           | Sort expressions handled by Calcite on shard |
| `DedupPushdownRule`               | Dedup handled by Calcite on coordinator    |
| `SortProjectExprTransposeRule`    | Optimizer rule not needed when Calcite runs natively |
| `ExpandCollationOnProjectExprRule`| Same as above                               |
| `SortAggregateMeasureRule`        | Same as above                               |
| `RareTopPushdownRule`             | Rare/Top handled by Calcite on coordinator |
| `EnumerableTopKMergeRule`         | TopK handled by MergeSortExchange          |
| `EnumerableNestedAggregateRule`   | Nested agg handled by Calcite on shard     |

This reduces the pushdown rule surface from 15 rules to 5, simplifying the
planner while retaining the performance-critical pushdowns (inverted-index
filter, column pruning, limit, sort, relevance). Script-based filter
pushdown is intentionally dropped in DQE mode — Calcite Filter on the shard
is the preferred execution path for complex expressions.

---

## 5. Example Query Plans

### 5.1 Simple Filter (scan + filter only)

```
PPL:  source=index | where status='active'

COORDINATOR:
  ConcatExchange
    |
SHARD:
  DSLScan(index, query=TermQuery("status","active"))
```

The filter is pushed into the DSLScan's QueryBuilder by
`FilterIndexScanRule`. No Calcite operators run on the shard above the scan.
ConcatExchange simply concatenates rows from all shards.

### 5.2 Filter + Aggregation

```
PPL:  source=index | where a=1 | stats count() by b

COORDINATOR:
  FinalAggregate(SUM(partial_count) GROUP BY b)
    MergeAggregateExchange
      |
SHARD:
  PartialAggregate(COUNT(*) GROUP BY b)
    DSLScan(index, query=TermQuery("a",1), fields=[b])
```

PlanSplitter decomposes the aggregate into partial (shard) and final
(coordinator). Each shard computes local counts per group. The coordinator sums
the partial counts.

### 5.3 Filter + Eval + Aggregation

```
PPL:  source=index | where a=1 | eval bb=abs(b) | stats count() by bb

COORDINATOR:
  FinalAggregate(SUM(partial_count) GROUP BY bb)
    MergeAggregateExchange
      |
SHARD:
  PartialAggregate(COUNT(*) GROUP BY bb)
    Calc(bb = abs(b))
      DSLScan(index, query=TermQuery("a",1), fields=[b])
```

The Calc (eval) operator runs on the shard, computing `abs(b)` before the
partial aggregate. This is a key win over the current system: today, `eval`
cannot be pushed down, so all rows are pulled to the coordinator.

### 5.4 Eval on Shards (no aggregation)

```
PPL:  source=index | eval score=price*qty | where score > 100

COORDINATOR:
  ConcatExchange
    |
SHARD:
  Filter(score > 100)
    Project(score=price*qty, *)
      DSLScan(index)
```

Both the computed column and the complex filter execute on each shard. Only
matching rows cross the network. Today, the entire index would be pulled to the
coordinator.

### 5.5 Window Function

```
PPL:  source=index | eval a=b+1 | eventstats avg(a) by group

COORDINATOR:
  Window(avg(a) PARTITION BY group)
    ConcatExchange
      |
SHARD:
  Project(a=b+1, group, *)
    DSLScan(index)
```

The eval runs on each shard, but the window function requires seeing all
data for each partition, so it runs on the coordinator after a ConcatExchange.
This is functionally the same as today, but the eval computation is distributed.

### 5.6 TopK (distributed sort + limit)

```
PPL:  source=index | sort price | head 10

COORDINATOR:
  MergeSortExchange(sort=price ASC, limit=10)
    |
SHARD:
  Sort(price ASC) + Limit(10)
    DSLScan(index)
```

Each shard computes its local top-10. The coordinator merge-sorts the K*N
candidates (where K=10, N=shard count) and returns the global top-10.

---

## 6. Phased Implementation

#### Phase 1 -- End-to-End Scatter-Gather

**Goal**: A PPL query that sets `plugins.query.dqe.enabled=true` executes
end-to-end via the distributed path and produces correct results.

**Scope:**

- `PlanSplitter` with Exchange node insertion
- `DSLScan` unified leaf operator
- `ConcatExchange`, `MergeSortExchange`, `MergeAggregateExchange`
- `PartialAggregate` decomposition (COUNT, SUM, AVG, MIN, MAX)
- `RelNodeSerializer` for plan fragment serialization
- `ShardCalciteRuntime` for shard-side execution
- Transport action wiring (`TransportCalciteShardAction`, request/response)
- **`DistributedExecutor`** coordinator orchestration
- **`ShardRoutingResolver`** for shard discovery
- **Execution engine wiring** in `OpenSearchExecutionEngine`
- DQE-mode rule gating in `OpenSearchIndexRules`
- `plugins.query.dqe.enabled` setting
- Mirror `Calcite*IT` test classes as `Distributed*IT` (109 test classes
  total, excluding `CalciteExplainIT` and `CalcitePPLExplainIT` which
  validate plan output that differs under DQE — see Section 8)

**Success Criteria:**

- SC-1.1: `plugins.query.dqe.enabled` is a dynamic cluster setting, toggleable
  at runtime.
- SC-1.2: PlanSplitter correctly classifies operators and inserts Exchange
  nodes.
- SC-1.3: DSLScan executes shard-local searches with `docvalue_fields` and
  `search_after`. DLS/FLS enforced.
- SC-1.4: RelNodeSerializer round-trips every supported RelNode type.
- SC-1.5: Partial aggregation produces correct results for COUNT, SUM, AVG,
  MIN, MAX including NULL handling and empty groups.
- SC-1.6: MergeSortExchange merge-sorts with ASC/DESC and NULL ordering.
- SC-1.7: All `Distributed*IT` integration test classes pass (107 of 109
  classes; `CalciteExplainIT` and `CalcitePPLExplainIT` excluded).
- SC-1.8: Only Filter/Project/Limit/Sort/Relevance pushdown rules are active
  in DQE mode. EnumerableIndexScanRule is replaced by DSLScan construction.
  FilterIndexScanRule does not push script-based filters in DQE mode.
- SC-1.9: Toggling `dqe.enabled` switches paths without restart.
- SC-1.10: DistributedExecutor dispatches to correct shards and collects
  results.
- SC-1.11: Fail-fast semantics: if distributed execution fails (shard error,
  serialization failure, transport error), the query fails immediately with a
  clear error message. No silent fallback to single-node path.

#### Phase 2 -- Robustness and Operational Readiness

**Goal**: DQE is safe for production workloads behind the feature flag.

**Scope:**

- Circuit breaker integration (`DistributedQueryMemoryTracker`)
- Error handling: shard failures, partial results, timeout behavior
- Explain support for DQE plans (show distributed plan in `_explain` output)
- Performance benchmarking against the pushdown path
- Observability: metrics for shard dispatch latency, row counts, fallback rate
- Cost-based decisions: `IndexStatisticsProvider` for choosing between
  coordinator-side and shard-side execution for borderline cases
- BroadcastExchange for small-table joins

**Success Criteria:**

- SC-2.1: Circuit breaker prevents OOM for large result sets.
- SC-2.2: Shard failure on one shard does not crash the query; partial results
  or graceful error returned.
- SC-2.3: `_explain` API shows the distributed plan with Exchange nodes.
- SC-2.4: No performance regression vs. pushdown path on standard benchmarks.
- SC-2.5: BroadcastExchange used for small-table joins (< 10K docs).
- SC-2.6: All `Distributed*IT` tests continue to pass.

#### Phase 3 -- Shuffle and Advanced Distributed Execution

**Goal**: Queries that currently require pulling all data to the coordinator
can optionally execute in a distributed fashion.

**Scope:**

- `HashExchange` for repartitioning data by key
- Distributed equi-joins via hash shuffle
- Distributed window functions (PARTITION BY key shuffle)
- Advanced partial aggregation: Welford's algorithm for STDDEV/VAR,
  HyperLogLog merge for DISTINCT_COUNT_APPROX, t-digest merge for
  PERCENTILE_APPROX
- Shuffle transport action (`TransportCalciteShuffleAction`)

**Why this is last**: Shuffle is a performance optimization for a specific class
of queries (joins and window functions with partition keys). Without shuffle,
these queries still work correctly via coordinator fallback. Shuffle adds
significant infrastructure complexity (node-to-node data movement, partition
assignment, failure handling across shuffle stages) and should only be built
after the basic distributed path is battle-tested.

**Success Criteria:**

- SC-3.1: HashExchange correctly partitions rows.
- SC-3.2: Distributed equi-joins produce identical results to coordinator-side.
- SC-3.3: Distributed window functions produce identical results.
- SC-3.4: Welford STDDEV/VAR within 1e-10 of single-node computation.
- SC-3.5: HLL and t-digest merge within expected error bounds.
- SC-3.6: All `Distributed*IT` tests continue to pass.
- SC-3.7: Simple queries do not regress in performance.

---

## 7. Code Organization

All DQE code lives under a dedicated `dqe` package within the `opensearch`
module:

```
opensearch/src/main/java/org/opensearch/sql/opensearch/dqe/
├── PlanSplitter.java                 # Exchange node insertion
├── DSLScan.java                      # Unified shard-local leaf operator
├── DSLScanRule.java                  # Logical scan → DSLScan conversion
├── ShardCalciteRuntime.java          # Shard-side plan execution
├── DistributedExecutor.java          # Coordinator orchestration
├── ShardRoutingResolver.java         # Shard discovery
├── exchange/
│   ├── Exchange.java                 # Abstract exchange base
│   ├── ConcatExchange.java           # Unordered concat
│   ├── MergeSortExchange.java        # Priority queue merge-sort
│   └── MergeAggregateExchange.java   # Partial agg merge
├── agg/
│   └── PartialAggregate.java         # Partial agg decomposition
└── serde/
    └── RelNodeSerializer.java        # RelNode ↔ JSON

plugin/src/main/java/.../transport/
├── CalciteShardAction.java           # ActionType definition
├── CalciteShardRequest.java          # Transport request
├── CalciteShardResponse.java         # Transport response
└── TransportCalciteShardAction.java  # Transport handler
```

This keeps DQE isolated from the existing pushdown-rule codebase. The only
files modified outside this folder are the hook-up points described below.

---

## 8. Setting and Hook-Up

### Setting

| Setting                        | Default | Type             | Description                      |
|--------------------------------|---------|------------------|----------------------------------|
| `plugins.query.dqe.enabled`   | `false` | Dynamic cluster  | Enable DQE execution path        |

The setting is dynamic and per-cluster. Changing it takes effect on the next
query with no restart.

### Hook-Up Point 1: Execution Engine

The primary entry point is `OpenSearchExecutionEngine.execute(RelNode,
CalcitePlanContext, ResponseListener)` (line ~208). This is where the Calcite
plan is executed via JDBC today.

**Change**: Check the DQE setting before execution. If enabled, route through
the distributed path instead.

```
OpenSearchExecutionEngine.execute(RelNode rel, CalcitePlanContext ctx, listener):
    if (settings.get(DQE_ENABLED)):
        // DQE path
        distributedPlan = PlanSplitter.split(rel)
        DistributedExecutor.execute(distributedPlan, listener)
    else:
        // Existing path (unchanged)
        statement = OpenSearchRelRunners.run(ctx, rel)
        result = statement.executeQuery()
        listener.onResponse(buildResultSet(result))
```

### Hook-Up Point 2: Planner Rule Registration

`CalciteLogicalIndexScan.register(RelOptPlanner)` (line ~128) registers
pushdown rules with the Calcite planner. Today it adds all 15 pushdown rules
when `pushdown.enabled=true`.

**Change**: When DQE is enabled, register only the DQE-mode rules (Filter,
Project, Limit, Sort, Relevance) plus the new `DSLScanRule`. Skip the 10
removed rules. Skip `EnumerableIndexScanRule`.

```
CalciteLogicalIndexScan.register(planner):
    // Non-pushdown rules always apply
    planner.addRules(NON_PUSHDOWN_RULES)

    if (settings.get(DQE_ENABLED)):
        // DQE mode: only inverted-index and scan-level rules
        planner.addRules(DQE_PUSHDOWN_RULES)   // Filter, Project, Limit, Sort, Relevance
        planner.addRule(DSLScanRule)             // Logical scan → DSLScan
    else if (settings.get(PUSHDOWN_ENABLED)):
        // Legacy mode: all 15 pushdown rules
        planner.addRules(PUSHDOWN_RULES)
```

### Hook-Up Point 3: Transport Action Registration

`SQLPlugin` (the OpenSearch plugin entry point) registers transport actions.

**Change**: Register `TransportCalciteShardAction` alongside existing actions.

```
SQLPlugin.getActions():
    // Existing actions
    ...
    // DQE action (always registered; only invoked when dqe.enabled=true)
    CalciteShardAction → TransportCalciteShardAction
```

### Summary of Modified Files Outside `dqe/`

| File | Change |
|------|--------|
| `common/.../setting/Settings.java` | Add `DQE_ENABLED` key |
| `opensearch/.../setting/OpenSearchSettings.java` | Register DQE setting |
| `opensearch/.../executor/OpenSearchExecutionEngine.java` | DQE branch in `execute()` |
| `opensearch/.../storage/scan/CalciteLogicalIndexScan.java` | DQE rule set in `register()` |
| `opensearch/.../planner/rules/OpenSearchIndexRules.java` | Add `DQE_PUSHDOWN_RULES` list |
| `plugin/.../SQLPlugin.java` | Register transport action |
| `integ-test/.../ppl/PPLIntegTestCase.java` | Add `enableDQE()` / `disableDQE()` |

---

## 8. Integration Test Strategy

Every existing `Calcite*IT` test class is mirrored as a `Distributed*IT` class.
The mirror class extends the Calcite test class and enables DQE mode in its
`init()` method.

**Pattern:**

```java
public class DistributedFieldFormatCommandIT extends CalciteFieldFormatCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    enableDQE();
  }
}
```

This approach ensures that:

- Every query that works under the pushdown path also works under DQE.
- No separate test logic needs to be maintained -- the existing test assertions
  are reused.
- Regressions are caught immediately when a Calcite test is added or modified.

There are 109 `Calcite*IT` test classes in `integ-test/.../calcite/remote/`.
Of these, 107 are mirrored as `Distributed*IT` classes. Two are excluded:

- `CalciteExplainIT` — DQE produces different plan output (Exchange nodes,
  DSLScan instead of CalciteEnumerableIndexScan). DQE-specific explain tests
  are written separately.
- `CalcitePPLExplainIT` — same reason.

The 107 mirrored classes cover the full PPL command surface: aggregation,
eval, sort, dedup, join, window functions, subqueries, relevance, data
types, and more.

**Test commands:**

```bash
# All DQE integration tests
./gradlew :integ-test:integTest -DignorePrometheus \
  --tests "org.opensearch.sql.calcite.distributed.*"

# Specific DQE test
./gradlew :integ-test:integTest \
  --tests "org.opensearch.sql.calcite.distributed.DistributedPPLBasicIT"
```

---

## 9. Security

DQE preserves all existing security guarantees without requiring changes to the
`opensearch-security` plugin.

**How:**

- `DSLScan` executes searches via the standard `IndexShard` search path. The
  security plugin's `SearchRequestFilter` intercepts these requests just as it
  does for any other search.
- **DLS (Document-Level Security)**: The security plugin injects a DLS filter
  into the query before execution. DSLScan sees only documents the user is
  authorized to access.
- **FLS (Field-Level Security)**: The security plugin strips restricted fields
  from search results. DSLScan reads only permitted fields.
- **RBAC and Audit**: `ActionFilters` on `TransportCalciteShardAction` enforce
  role-based access control and audit logging at the transport level. The
  action name (`indices:data/read/opensearch/calcite/shard`) is subject to the
  same permission checks as any other `indices:data/read/*` action.
- **TLS**: Node-to-node transport encryption applies to shard dispatch requests
  automatically.

There is no new attack surface. The DQE path uses the same search
infrastructure as the existing engine; it simply runs Calcite operators above
that infrastructure on the shard rather than on the coordinator.

---

## 10. Migration and Rollback

### Migration Path

```
Phase     Setting              Behavior
-------   -------------------  ------------------------------------------
Today     dqe.enabled=false    Existing pushdown path (no change)
Phase 1   dqe.enabled=false    DQE code ships, off by default
          dqe.enabled=true     DQE active (opt-in per cluster)
Stable    dqe.enabled=true     DQE becomes the default
          dqe.enabled=false    Pushdown path still available
Future    (setting removed)    Legacy pushdown rules removed
```

### Rollback

1. Set `plugins.query.dqe.enabled=false` via cluster settings API.
2. All subsequent queries use the existing pushdown path.
3. No restart required. No data migration. No index changes.

The setting is a runtime toggle, so operators can switch back instantly if
they observe unexpected behavior.

### Legacy Cleanup (Future)

Once DQE is stable and enabled by default for multiple releases:

- The 11 removed pushdown rules can be deleted.
- The `pushdown.enabled` setting can be simplified.
- The `CalciteEnumerableIndexScan` can be consolidated with `DSLScan`.

This is not urgent and should not block any DQE phase.

---

## 11. Open Questions and Risks

### Open Questions

1. **Serialization format**: The ideas doc proposes JSON for RelNode
   serialization. JSON is human-readable and easy to debug, but it may be
   verbose for large plans. Should we evaluate a binary format (e.g., Protobuf)
   for production, while keeping JSON for explain/debug? Plans are typically
   small (< 10 KB), so this may not matter in practice.

2. **search_after vs. scroll**: DSLScan uses `search_after` for pagination.
   This is stateless and shard-scoped, which is ideal. However, for very large
   result sets that exceed circuit breaker limits, we may need backpressure
   between the Calcite pipeline and the scan. How does Calcite's Enumerable
   iterator model interact with backpressure?

3. **Cross-cluster search**: The current design assumes all shards are on the
   local cluster. How should DQE interact with cross-cluster search (CCS)?
   The transport action would need to route to remote cluster shards. This is
   likely a post-Phase-1 concern.

4. **Explain output**: What should `_explain` show for a DQE plan? The current
   Calcite explain shows the logical/physical plan. For DQE, it should also
   show the split point and which operators run where. The format needs design.

5. **Aggregation decomposition extensibility**: The initial set of decomposable
   aggregates (COUNT, SUM, AVG, MIN, MAX) is small. How do we add new
   decomposable aggregates (e.g., approximate percentiles) without modifying
   PlanSplitter? Consider a registry-based approach.

### Risks

1. **Serialization correctness**: RelNode serialization/deserialization is the
   single most critical new component. If a plan is mis-serialized, the shard
   executes the wrong query silently. Specific sub-risks:
   - `RexNode` expressions (RexCall, RexLiteral, RexFieldAccess) contain type
     information that must survive round-trip.
   - User-defined functions (e.g., GEOIP) require the shard-side deserializer
     to have access to the same function registry.
   - Custom PPL operators (CalciteEnumerableTopK, EnumerableNestedAggregate)
     may not be supported by Calcite's standard RelJsonWriter.
   Mitigation: exhaustive round-trip unit tests for every RelNode and RexNode
   type, plus the 107 integration tests that validate end-to-end correctness.
   The function registry must be passed to the shard-side deserializer.

2. **Performance regression for simple queries**: For queries that the current
   pushdown path handles well (e.g., simple aggregations that translate
   directly to DSL aggs), DQE may be slower because it runs Calcite operators
   on each shard instead of using OpenSearch's optimized native aggregation.
   Mitigation: benchmark early in Phase 1; consider retaining DSL aggregation
   as an optimization for simple cases if the gap is significant.

3. **Memory pressure**: Shard-side Calcite execution adds memory overhead per
   shard (Enumerable pipeline buffers, partial aggregate state). On nodes with
   many shards, this could increase heap pressure. Mitigation: circuit breaker
   integration in Phase 2; memory-bounded iterators in DSLScan.

4. **Calcite version coupling**: Serializing RelNode trees creates a
   compatibility surface with Calcite's internal types. Calcite version
   upgrades could break serialization. Mitigation: version the serialization
   format; test serialization across Calcite versions during upgrades.

5. **Partial results on shard failure**: If one shard fails during distributed
   execution, should the query fail entirely or return partial results?
   OpenSearch search returns partial results by default, but SQL/PPL users
   may expect all-or-nothing semantics. This needs a policy decision.

---

## 12. Design Decisions and Review Notes

See [dqe_design_v1_review.md](dqe_design_v1_review.md) for the full list of
16 design decisions (D1–D16) made during the review process, covering phasing,
pushdown rules, DSLScan field retrieval, partial aggregation correctness,
error handling, thread pools, memory safety, and more.

---

## 13. Summary

DQE replaces the current paradigm-translation approach (15 pushdown rules
converting Calcite RelNodes to OpenSearch DSL) with direct execution of
Calcite plan fragments on shards. The pushdown rule surface is reduced from
15 to 5 (Filter, Project, Limit, Sort, Relevance). Script-based filter
pushdown is dropped in favor of Calcite Filter evaluation on the shard.

**Key components**: PlanSplitter (one tree-walk), DSLScan (unified leaf),
Exchange nodes (3 types), DistributedExecutor (coordinator orchestration).

**Phasing**:
- Phase 1: End-to-end scatter-gather with 107 integration tests passing
- Phase 2: Robustness, circuit breakers, explain, benchmarking
- Phase 3: Shuffle for distributed joins and windows

**Setting**: `plugins.query.dqe.enabled` (dynamic, opt-in, instant rollback)

**Security**: Preserved by construction via standard search pipeline.

**Key risks**: (1) Serialization correctness — mitigated by exhaustive
round-trip tests and 107 integration tests. (2) `docvalue_fields` is new
to the codebase — Phase 1 can start with `_source`-only and add
`docvalue_fields` incrementally. (3) Performance regression for simple
aggregations — benchmark early in Phase 1.
