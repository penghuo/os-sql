# Native Trino DQE Integration for OpenSearch

**Date**: 2026-03-04
**Status**: Proposed

## Problem Statement

The current Calcite-based query engine translates RelNode plans into OpenSearch DSL for execution. This translation approach has three fundamental limitations:

1. **DSL cannot express all RelNodes.** Window functions, joins, and complex expressions have no DSL equivalent. These queries either fail or fall back to inefficient coordinator-side execution.

2. **Operators do not execute on shards.** A query like `source=index | eval a=b+1` pulls every row to the coordinator, computes the expression there, and then returns results. There is no mechanism to push Calcite operators to the shard level.

3. **Pushdown rules are complex and error-prone.** There are 15 hand-coded Hep and Volcano rules that translate between Calcite RelNodes and OpenSearch DSL constructs. Each new PPL feature requires writing a new rule, which is fragile and hard to test.

**Root cause**: The system translates between two paradigms (Calcite RelNode and OpenSearch DSL). Each paradigm has its own semantics, optimizer, and execution model. Bridging them requires an ever-growing set of translation rules.

**Solution**: Stop translating. Ship plan fragments directly to shards. Use DSL only at the leaf level for inverted-index access (filters, projections, relevance functions). Everything above the leaf executes as native physical operators — on the shard.

## Architecture Overview

A new `/_plugins/_trino_sql` REST endpoint using a distributed query execution (DQE) model:

1. Parse SQL with Trino's ANTLR4 parser (forked library).
2. Analyze and resolve against OpenSearch cluster metadata.
3. Optimize with Trino's rule-based + cost-based optimizer.
4. Fragment the optimized plan and distribute plan fragments to each shard (split).
5. Each shard executes its plan fragment locally using the local search API on that single shard (preserving FLS/DLS security).
6. Intermediate results flow back via OpenSearch's transport layer for merge and final aggregation.

### Design Principles

- The coordinator distributes query plan fragments, not DSL translations.
- Each shard is a first-class execution unit running physical operators against its local data.
- The local search API is always used (never direct Lucene segment access) to preserve Field-Level Security (FLS) and Document-Level Security (DLS).
- This is a completely new execution path that does not build on or reference the existing Calcite-based SQL/PPL implementation.

### Component Diagram

```
                          ┌─────────────────────────────────────────────┐
                          │            Coordinator Node                 │
                          │                                             │
   REST Request ────────► │  ┌──────────┐  ┌───────────┐  ┌─────────┐ │
   /_plugins/_trino_sql   │  │  Trino   │→ │  Trino    │→ │ Trino   │ │
                          │  │  Parser  │  │  Analyzer │  │Optimizer│ │
                          │  └──────────┘  └───────────┘  └────┬────┘ │
                          │                                     │      │
                          │                              ┌──────▼────┐ │
                          │                              │ Fragmenter│ │
                          │                              └──────┬────┘ │
                          │                                     │      │
                          │         ┌───────────────────────────┤      │
                          │         │  TransportAction dispatch │      │
                          └─────────┼───────────────────────────┼──────┘
                                    │                           │
                    ┌───────────────▼──┐              ┌────────▼────────┐
                    │   Shard 0        │              │   Shard 1       │
                    │                  │              │                 │
                    │ ┌──────────────┐ │              │ ┌─────────────┐ │
                    │ │Plan Fragment │ │              │ │Plan Fragment│ │
                    │ │  Executor    │ │              │ │  Executor   │ │
                    │ └──────┬───────┘ │              │ └──────┬──────┘ │
                    │        │         │              │        │        │
                    │ ┌──────▼───────┐ │              │ ┌──────▼──────┐ │
                    │ │ Trino        │ │              │ │ Trino       │ │
                    │ │ Operators    │ │              │ │ Operators   │ │
                    │ │ (filter,proj │ │              │ │ (filter,proj│ │
                    │ │  eval,agg...)│ │              │ │  eval,agg..)│ │
                    │ └──────┬───────┘ │              │ └──────┬──────┘ │
                    │        │         │              │        │        │
                    │ ┌──────▼───────┐ │              │ ┌──────▼──────┐ │
                    │ │ PageSource   │ │              │ │ PageSource  │ │
                    │ │ (SearchAction│ │              │ │ (SearchAction│ │
                    │ │  adapter)    │ │              │ │  adapter)   │ │
                    │ └──────┬───────┘ │              │ └──────┬──────┘ │
                    │        │         │              │        │        │
                    │ ┌──────▼───────┐ │              │ ┌──────▼──────┐ │
                    │ │ Local Search │ │              │ │ Local Search│ │
                    │ │ API (FLS/DLS)│ │              │ │ API(FLS/DLS)│ │
                    │ └──────────────┘ │              │ └─────────────┘ │
                    └──────────┬───────┘              └────────┬────────┘
                               │                               │
                               │    Partial Pages              │
                               └───────────┬───────────────────┘
                                           │
                                    ┌──────▼──────┐
                                    │ Coordinator │
                                    │ Merge +     │
                                    │ Format      │
                                    └──────┬──────┘
                                           │
                                     REST Response
```

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Data access at shard level | Shard-local SearchAction adapter | Preserves FLS/DLS fully. Uses `_only_local` preference + shard routing. Adapter wraps SearchResponse hits into Trino Pages. |
| Embedding strategy | Fork & extract key Trino packages | Full control over dependencies, smaller footprint, ability to evolve independently. |
| Shuffle mechanism | OpenSearch TransportAction-based exchange | Reuses existing transport infrastructure (connection management, security, flow control). Each shuffle stage is a transport round-trip. |
| MVP scope | Single-stage: scan + filter + project + eval + agg + sort + limit | No shuffle needed. Proves the full pipeline without multi-stage complexity. Covers ~80% of common analytical queries. |
| Architecture approach | Full Trino pipeline | Extract parser, analyzer, planner, optimizer, and physical operators. Mature optimizer, full SQL semantics, clear upgrade path. |
| Thread model | Simple thread pool for MVP, cooperative/hybrid later | Thread-per-fragment with `dqe-shard-executor` pool. Design operators with `processNextBatch()` for future upgrade. |
| Module structure | Single `dqe/` module | Forked Trino code and OpenSearch integration in one module, separated by packages. Simpler build graph. |

## Module Structure & Package Layout

```
dqe/
├── build.gradle
└── src/main/java/org/opensearch/sql/dqe/
    ├── trino/                          # Forked Trino components
    │   ├── parser/                     # ANTLR4 grammar, SqlParser, AstBuilder
    │   ├── tree/                       # AST node hierarchy (Statement, Expression, etc.)
    │   ├── analyzer/                   # Semantic analysis, type resolution
    │   ├── planner/                    # PlanNode hierarchy, optimizer rules
    │   │   ├── plan/                   # PlanNode types (TableScan, Filter, Project, etc.)
    │   │   ├── optimizations/          # Optimizer rule implementations
    │   │   └── iterative/             # Iterative optimizer framework
    │   ├── operator/                   # Physical operators (Page-based execution)
    │   ├── block/                      # Block/Page columnar data model
    │   ├── type/                       # Trino type system
    │   └── function/                   # Built-in scalar/aggregate functions
    │
    ├── coordinator/                    # Coordinator-side logic
    │   ├── rest/                       # RestTrinoSqlAction (/_plugins/_trino_sql)
    │   ├── transport/                  # TransportTrinoSqlAction, request/response
    │   ├── metadata/                   # OpenSearchMetadata (cluster state → Trino schema)
    │   ├── fragment/                   # Plan fragmenter, fragment serialization
    │   └── merge/                      # Partial result merger (agg merge, sort merge)
    │
    ├── shard/                          # Shard-side execution
    │   ├── transport/                  # TransportShardExecuteAction
    │   ├── executor/                   # ShardFragmentExecutor (deserialize + run)
    │   └── source/                     # OpenSearchPageSource (SearchAction → Pages)
    │
    └── common/                         # Shared utilities
        ├── types/                      # OpenSearch ↔ Trino type mapping
        └── config/                     # DQE cluster settings
```

## Query Lifecycle (Coordinator)

```
Client                    Coordinator                           Shards
  │                          │                                    │
  │ POST /_plugins/_trino_sql│                                    │
  │ {"query": "SELECT ..."}  │                                    │
  │─────────────────────────►│                                    │
  │                          │                                    │
  │                     1. Parse (SqlParser)                      │
  │                        TrinoAST                               │
  │                          │                                    │
  │                     2. Analyze (Analyzer)                     │
  │                        Resolve tables → OpenSearchMetadata    │
  │                        Resolve columns → index mappings       │
  │                        Resolve types → Trino type system      │
  │                          │                                    │
  │                     3. Plan (LogicalPlanner)                  │
  │                        AST → PlanNode tree                    │
  │                          │                                    │
  │                     4. Optimize (IterativeOptimizer)          │
  │                        Apply rules: predicate pushdown,       │
  │                        projection pruning, aggregation split  │
  │                          │                                    │
  │                     5. Fragment (PlanFragmenter)              │
  │                        Optimized plan → per-shard fragments   │
  │                        Each fragment has a Split (shard ID)   │
  │                          │                                    │
  │                     6. Dispatch                               │
  │                        TransportShardExecuteAction            │
  │                          │────────fragment[shard0]───────────►│
  │                          │────────fragment[shard1]───────────►│
  │                          │────────fragment[shardN]───────────►│
  │                          │                                    │
  │                          │◄───────partial Pages──────────────│
  │                          │◄───────partial Pages──────────────│
  │                          │                                    │
  │                     7. Merge                                  │
  │                        Merge partial aggs, merge-sort,        │
  │                        apply final LIMIT                      │
  │                          │                                    │
  │                     8. Format (JSON/JDBC)                     │
  │◄─────────────────────────│                                    │
  │      Response             │                                    │
```

### Step Details

**Step 2 (Analyze)**: `OpenSearchMetadata` implements Trino's `ConnectorMetadata` interface. It reads cluster state to expose index names as tables, index mappings as columns, and maps OpenSearch field types to Trino types.

**Step 5 (Fragment)**: For single-stage MVP, the fragmenter produces one fragment per shard. Each fragment contains the operator pipeline (serialized PlanNode subtree) and a `Split` identifying the target shard (index name + shard ID + node ID). Shard locations are resolved from cluster state via `IndicesService`.

**Step 6 (Dispatch)**: A custom `TransportShardExecuteAction` sends the serialized fragment to the node hosting each shard. Fan-out is parallel. The transport request includes serialized plan fragment bytes, query ID, and session properties (timeout, memory limit).

**Step 7 (Merge)**: For aggregations, the optimizer splits aggregations into partial (shard-side) and final (coordinator-side) phases. The coordinator receives partial aggregation states from each shard and merges them. For non-aggregate queries, merge is concatenation with a possible merge-sort for ORDER BY.

## Shard-Side Execution

When a shard receives a `TransportShardExecuteAction` request:

```
TransportShardExecuteAction.shardOperation(request, shardId)
  │
  ├── 1. Deserialize plan fragment (PlanNode tree)
  │
  ├── 2. Build operator pipeline (LocalExecutionPlanner)
  │      PlanNode tree → chain of Operator instances
  │
  │      ┌─────────────────────┐
  │      │ PartialAggOperator  │  ← partial hash aggregation
  │      │ (category, count)   │
  │      └─────────┬───────────┘
  │                │ Pages
  │      ┌─────────▼───────────┐
  │      │ FilterOperator      │  ← evaluates predicates not pushed to DSL
  │      └─────────┬───────────┘
  │                │ Pages
  │      ┌─────────▼───────────┐
  │      │ TableScanOperator   │
  │      │ (OpenSearchPageSrc) │  ← leaf: SearchAction adapter
  │      └─────────┬───────────┘
  │                │
  │      ┌─────────▼───────────┐
  │      │ Local Search API    │  ← shard-local search request
  │      │ (single shard,      │     preference=_only_local
  │      │  FLS/DLS enforced)  │     routing=shardId
  │      └─────────────────────┘
  │
  ├── 3. Execute: pull Pages through operator chain
  │
  └── 4. Return serialized Pages in transport response
```

### OpenSearchPageSource (Leaf Adapter)

The adapter between OpenSearch's search API and Trino's Page/Block data model:

```java
class OpenSearchPageSource implements ConnectorPageSource {
    private final NodeClient client;
    private final ShardId shardId;
    private final SearchSourceBuilder dslQuery;  // filter + projection from optimizer
    private final List<ColumnHandle> columns;

    @Override
    public Page getNextPage() {
        // 1. Execute shard-local search (or scroll next batch)
        SearchRequest request = new SearchRequest(index)
            .preference("_only_local")
            .source(dslQuery);
        request.source().size(BATCH_SIZE);  // e.g., 1024 rows per batch

        SearchResponse response = client.search(request).actionGet();

        // 2. Convert SearchHits → Trino Page (columnar)
        SearchHit[] hits = response.getHits().getHits();
        BlockBuilder[] builders = new BlockBuilder[columns.size()];
        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSourceAsMap();
            for (int col = 0; col < columns.size(); col++) {
                appendValue(builders[col], columns.get(col), source);
            }
        }

        // 3. Build Page from completed blocks
        Block[] blocks = new Block[builders.length];
        for (int i = 0; i < builders.length; i++) {
            blocks[i] = builders[i].build();
        }
        return new Page(blocks);
    }
}
```

**Design details**:
- DSL at the leaf: The optimizer pushes filter predicates and column projections into the `SearchSourceBuilder`. OpenSearch's inverted index handles heavy filtering; Trino operators above only process surviving rows.
- Scroll API for large results: For queries scanning many rows, the PageSource uses scroll or PIT+search_after for pagination.
- Batch size tuning: Each `getNextPage()` call fetches `BATCH_SIZE` rows (default 1024). Configurable via cluster setting.

### Plan Fragment Serialization

Each PlanNode implements OpenSearch's `Writeable` interface with explicit `writeTo`/`readFrom` methods. Expressions serialize as compact binary format. Function references serialize by name (shard-side reconstructs from local function registry). Type references serialize as type signatures.

## Type System Mapping

### OpenSearch → Trino

| OpenSearch Field Type | Trino Type | Notes |
|---|---|---|
| `keyword` | `VARCHAR` | Direct mapping |
| `text` | `VARCHAR` | Analyzed text; aggregation uses `.keyword` sub-field |
| `long` | `BIGINT` | |
| `integer` | `INTEGER` | |
| `short` | `SMALLINT` | |
| `byte` | `TINYINT` | |
| `double` | `DOUBLE` | |
| `float` | `REAL` | |
| `boolean` | `BOOLEAN` | |
| `date` | `TIMESTAMP(3)` | OpenSearch dates are epoch millis or formatted strings |
| `ip` | `VARCHAR` | Custom `IPADDRESS` type possible later |
| `geo_point` | `ROW(lat DOUBLE, lon DOUBLE)` | Structured type |
| `nested` | `ARRAY(ROW(...))` | Nested objects → array of rows |
| `object` | `ROW(...)` | Flattened field paths |
| `binary` | `VARBINARY` | |

### Multi-Valued Fields

OpenSearch fields can be multi-valued (arrays). For scalar context, take the first element. For explicit array operations, wrap in `ARRAY` type. This matches how Trino's existing OpenSearch connector handles it.

## Thread Model

### MVP: Simple Thread Pool (Proposal A)

Register a `dqe-shard-executor` thread pool. Each shard fragment gets one thread and runs to completion.

```
DQE Thread Pool (fixed, N_cores/2 threads)
┌──────────────────────────────────┐
│ Thread 1 ──► Fragment (Q1, S0)   │  runs entire operator chain
│              until all pages done │  to completion
│                                  │
│ Thread 2 ──► Fragment (Q2, S1)   │
│              until all pages done │
│                                  │
│ Queue: [fragments waiting...]    │
└──────────────────────────────────┘
```

Operators are designed with a `processNextBatch()` method to allow future migration to cooperative or hybrid scheduling without operator rewrites.

### Future: Cooperative or Hybrid Scheduling

**Option B (Cooperative)**: Port Trino's `TaskExecutor`/`Driver` model. Small thread pool, drivers yield after time quantum (1 second). Non-blocking operator contract. Highest concurrency and fairness.

**Option C (Hybrid)**: OpenSearch thread pool with page budget. After processing N pages, fragment re-enqueues itself. Synchronous operators remain, but fairness improves.

### Comparison

| | Concurrency | Fairness | Complexity | Operator Contract |
|---|---|---|---|---|
| A: Simple Pool (MVP) | Low (pool_size) | None | Low | Synchronous |
| B: Cooperative | High (unlimited) | Excellent | High | Non-blocking |
| C: Hybrid | Medium | Good | Medium | Synchronous + resumable |

## Technology Challenges

### Challenge 1: Trino Dependency Extraction

Trino's internal packages have deep dependency chains. The optimizer depends on the type system, which depends on the SPI, which depends on Airlift.

**Strategy**:
- Fork `io.trino.sql.parser`, `io.trino.sql.tree`, `io.trino.sql.analyzer`, `io.trino.sql.planner`, `io.trino.operator`, `io.trino.spi.block`, `io.trino.spi.type`
- Replace Airlift dependencies with direct implementations or OpenSearch equivalents (Airlift `Slice` → keep as-is; Airlift configuration → OpenSearch `ClusterSettings`)
- Remove Guice injection — wire dependencies explicitly in constructors
- Most labor-intensive part of the project

### Challenge 2: Memory Management

Implement Trino's `MemoryContext` interface backed by OpenSearch's `MemoryCircuitBreaker`. Each query gets a configurable memory budget (default 10% of heap). When an operator exceeds budget, circuit breaker trips and query fails with a clear error.

### Challenge 3: Plan Fragment Serialization

PlanNodes are complex object graphs with type references, function handles, and expression trees. Each PlanNode implements `Writeable` with explicit `writeTo`/`readFrom`. Function references serialize by name. Type references serialize as type signatures. Thorough round-trip serialization tests required for every PlanNode type.

### Challenge 4: Partial Aggregation Merge

Trino already has partial/final aggregation modes. The optimizer splits `Aggregation` nodes: partial on shard, final on coordinator. Each aggregation function has an `intermediateType` (e.g., for `COUNT`, intermediate is `BIGINT`; for `AVG`, intermediate is `ROW(sum DOUBLE, count BIGINT)`). Shards return serialized intermediate states; coordinator runs final aggregation.

### Challenge 5: Security Context Propagation

The coordinator's `TransportAction` preserves `ThreadContext` (authenticated user's security headers). When the shard executes the SearchRequest via `NodeClient`, the security plugin applies FLS/DLS rules. No special handling needed — same path the existing SQL plugin uses. Integration tests must confirm FLS/DLS enforcement with DQE queries.

## REST API

### Execute Endpoint

```
POST /_plugins/_trino_sql
{
  "query": "SELECT category, COUNT(*) FROM logs WHERE status = 200 GROUP BY category ORDER BY COUNT(*) DESC LIMIT 10"
}

Response:
{
  "schema": [
    {"name": "category", "type": "keyword"},
    {"name": "_col1", "type": "long"}
  ],
  "datarows": [
    ["error", 15234],
    ["warn", 8921]
  ],
  "total": 10,
  "size": 10,
  "status": 200
}
```

### Explain Endpoint

```
POST /_plugins/_trino_sql/_explain
{
  "query": "SELECT category, COUNT(*) FROM logs WHERE status = 200 GROUP BY category"
}

Response:
{
  "logicalPlan": "Aggregation[category, count(*)]\n  Filter[status = 200]\n    TableScan[logs]",
  "optimizedPlan": "FinalAggregation[category, count(*)]\n  Exchange[GATHER]\n    PartialAggregation[category, count(*)]\n      TableScan[logs, filter=status:200, columns=[category]]",
  "fragments": [
    {
      "shardId": "[logs][0]",
      "node": "node-1",
      "plan": "PartialAggregation → TableScan[filter=status:200]"
    }
  ]
}
```

### Cluster Settings

| Setting | Default | Description |
|---|---|---|
| `plugins.dqe.enabled` | `false` | Enable/disable DQE endpoint |
| `plugins.dqe.query.timeout` | `30s` | Per-query timeout |
| `plugins.dqe.memory.query_limit` | `10%` | Max heap per query |
| `plugins.dqe.page.batch_size` | `1024` | Rows per Page in PageSource |
| `plugins.dqe.thread_pool.size` | `N_cores/2` | DQE executor pool size |

## Post-MVP Roadmap

### Phase 2: Two-Stage Execution (Shuffle)

Support hash joins and multi-phase aggregations. Adds `TransportExchangeAction` for shard-to-shard data movement, `ExchangeOperator`/`ExchangeSourceOperator`, and hash partitioning.

### Phase 3: Window Functions

Support `ROW_NUMBER()`, `RANK()`, windowed aggregations. Adds `WindowOperator`, sort + partition stage, uses Phase 2 shuffle for partition redistribution.

### Phase 4: Subqueries & CTEs

Support correlated subqueries, `WITH` clauses, `EXISTS`/`IN`. Adds decorrelation optimizer rules, materialized CTE execution.

### Phase 5: Thread Model Upgrade

Upgrade from simple thread pool to cooperative (B) or hybrid (C) scheduling for production-grade concurrency.

## MVP Scope

The first working version supports single-stage queries:
- `SELECT` with column projections
- `WHERE` with filter predicates
- Scalar expressions (`eval`: arithmetic, string functions, type casts)
- `GROUP BY` with aggregation functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
- `ORDER BY` with `LIMIT`

No joins, window functions, subqueries, or multi-stage execution.
