# OpenSearch Distributed Query Engine (DQE) — Design Document

**Status**: Draft
**Date**: 2026-02-21
**Based on**: `docs/research/finding_claude.md` (research), `docs/research/review_codex.md` (review)

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Goals and Non-Goals](#2-goals-and-non-goals)
3. [Semantic Compatibility Contract](#3-semantic-compatibility-contract)
4. [Architecture Overview](#4-architecture-overview)
5. [Module Structure](#5-module-structure)
6. [REST API](#6-rest-api)
7. [SQL Parser Integration](#7-sql-parser-integration)
8. [Metadata Layer](#8-metadata-layer)
9. [Type System and Type Mapping Contract](#9-type-system-and-type-mapping-contract)
10. [Query Planning Pipeline](#10-query-planning-pipeline)
11. [Distributed Execution Model](#11-distributed-execution-model)
12. [Exchange Protocol](#12-exchange-protocol)
13. [Join Strategies](#13-join-strategies)
14. [Function Library](#14-function-library)
15. [Memory Management and Circuit Breaker Integration](#15-memory-management-and-circuit-breaker-integration)
16. [Resource Isolation and Thread Pools](#16-resource-isolation-and-thread-pools)
17. [Consistency Model](#17-consistency-model)
18. [Cancellation and Task Lifecycle](#18-cancellation-and-task-lifecycle)
19. [Failure Modes and Recovery](#19-failure-modes-and-recovery)
20. [Security Integration](#20-security-integration)
21. [Observability](#21-observability)
22. [Configuration Reference](#22-configuration-reference)
23. [Packaging, Shading, and Release Process](#23-packaging-shading-and-release-process)
24. [Phased Implementation Plan](#24-phased-implementation-plan)
25. [Risks and Mitigations](#25-risks-and-mitigations)

---

## 1. Introduction

### 1.1 Problem Statement

OpenSearch lacks a distributed SQL query engine capable of executing analytical queries (aggregations, joins, window functions) efficiently across shards and nodes. The current Trino-OpenSearch connector requires a separate Trino cluster, does not push down aggregations, serializes all data through JSON/HTTP, and drops important types (geo, vector, custom dates).

### 1.2 Proposal

Build a **Distributed Query Engine (DQE)** inside OpenSearch by embedding Trino's SQL parser, analyzer, optimizer, physical operators, and function library within the existing `opensearch-sql` plugin. The DQE is exposed through the existing `/_plugins/_sql` REST endpoint via an engine routing mechanism, and is completely independent of the existing Calcite-based execution path — sharing no planner, optimizer, or execution code.

### 1.3 Key Design Principles

1. **Distributed plan execution to shards** — the coordinator distributes plan fragments to each shard; each shard is a first-class execution unit running physical operators.
2. **Search API always, never direct Lucene** — every shard reads data exclusively through the local search API, preserving Field-Level Security (FLS) and Document-Level Security (DLS).
3. **Completely independent execution path** — no dependency on existing SQL, PPL, or Calcite execution modules. Shares the REST endpoint surface (`/_plugins/_sql`) via engine routing, but uses a new module tree and new transport actions.
4. **Incremental delivery** — scan/filter first, then aggregation, then joins, then optimization. Each phase has explicit semantic contracts and operability gates.
5. **Cluster health first** — DQE must not degrade search or indexing workloads. Safety controls (circuit breakers, thread pool bounds, admission control) are non-negotiable from Phase 1.

---

## 2. Goals and Non-Goals

### 2.1 Goals

- Execute analytical SQL queries (scan, filter, project, aggregate, join, sort, window) distributed across OpenSearch data nodes.
- Achieve 10–1000x improvement over the connector approach for aggregation-heavy queries by eliminating network serialization and enabling per-shard partial aggregation.
- Support the full OpenSearch type system including `geo_point`, `dense_vector`, `date` with custom formats, `nested`, and `flattened`.
- Preserve FLS/DLS security transparently.
- Operate as an in-process plugin with zero additional infrastructure.

### 2.2 Non-Goals

- Replacing or modifying the existing `/_plugins/_sql` or `/_plugins/_ppl` endpoints.
- Write operations (INSERT, UPDATE, DELETE).
- Cross-cluster or cross-catalog federation (single OpenSearch cluster only).
- Full Trino compatibility — advanced constructs (MATCH_RECOGNIZE, polymorphic table functions, time travel) are explicitly out of scope.
- Sub-millisecond point-lookup latency — the DQE targets analytical workloads, not OLTP.

---

## 3. Semantic Compatibility Contract

### 3.1 Pinned Trino Version

The DQE pins to **Trino 479** (the latest stable release at implementation start). The pinned version is recorded in `gradle.properties` as `trinoVersion`. Version upgrades do not follow a fixed cadence — each upgrade is an explicit decision requiring full regression and conformance testing. See §23.3 for the CVE patch and upgrade policy.

### 3.2 Supported SQL Subset by Phase

| Construct | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|---|---|---|---|---|
| SELECT, FROM, WHERE | Yes | Yes | Yes | Yes |
| Column aliases, expressions | Yes | Yes | Yes | Yes |
| AND, OR, NOT, IN, BETWEEN, LIKE | Yes | Yes | Yes | Yes |
| IS NULL, IS NOT NULL | Yes | Yes | Yes | Yes |
| CAST, type coercion | Yes | Yes | Yes | Yes |
| ORDER BY, LIMIT, OFFSET | Yes | Yes | Yes | Yes |
| GROUP BY, HAVING | — | Yes | Yes | Yes |
| COUNT, SUM, AVG, MIN, MAX | — | Yes | Yes | Yes |
| COUNT DISTINCT, APPROX_DISTINCT | — | Yes | Yes | Yes |
| Scalar functions (string, math, date) | — | Yes | Yes | Yes |
| Subqueries (scalar, IN, EXISTS) | — | — | Yes | Yes |
| JOIN (INNER, LEFT, RIGHT, CROSS) | — | — | Yes | Yes |
| Window functions (ROW_NUMBER, RANK, LAG, LEAD) | — | — | — | Yes |
| UNION, INTERSECT, EXCEPT | — | — | — | Yes |
| CTEs (WITH) | — | — | Yes | Yes |
| MATCH_RECOGNIZE | — | — | — | — |
| Polymorphic table functions | — | — | — | — |
| Lambda expressions | — | — | — | Eval |
| Time travel (FOR TIMESTAMP AS OF) | — | — | — | — |

**"—"** = not supported; returns explicit error with message `DQE does not support <construct>. See supported SQL reference.`

### 3.3 Error Contract

- **Unsupported syntax**: The parser accepts it (since we use Trino's full grammar), but the analyzer rejects it with a clear `DqeUnsupportedOperationException` naming the construct.
- **Type errors**: Reported at analysis time with column name, expected type, and actual type.
- **Runtime errors**: Overflow, division by zero, and cast failures follow Trino semantics (e.g., integer overflow throws, `CAST('abc' AS INTEGER)` throws). These are not silently swallowed.

### 3.4 Known Divergences from Standalone Trino

| Area | Trino Behavior | DQE Behavior | Reason |
|---|---|---|---|
| Isolation | Snapshot (connector-provided) | Near-real-time per PIT | OpenSearch does not support snapshot isolation |
| `text` field sorting | N/A (not mapped) | Not sortable unless `fielddata` enabled | OpenSearch text fields require fielddata for sorting |
| Approximate aggs | HyperLogLog via `approx_distinct` | Same algorithm, may differ in precision | Implementation-dependent |
| String collation | JVM default | JVM default | Identical in practice |
| Timestamp precision | Microseconds (TIMESTAMP(6)) | Milliseconds (TIMESTAMP(3)) for `date`, nanoseconds for `date_nanos` | OpenSearch date storage precision |

### 3.5 Minimal Conformance Suite

Before the DQE endpoint is enabled by default, the following must pass:

- **NULL handling**: 50+ test cases covering NULL propagation in arithmetic, comparison, aggregation, and CASE.
- **Type coercion**: Implicit widening (INT + BIGINT), explicit CAST, failure cases.
- **Timezone**: TIMESTAMP WITH TIME ZONE operations, AT TIME ZONE, EXTRACT across DST boundaries.
- **Decimal precision**: Short decimal (p<=18) and long decimal (p>18) arithmetic, rounding.
- **Aggregation correctness**: GROUP BY with NULLs, empty groups, single-row groups, partial/final merge equivalence.
- **Golden results**: 200+ queries compared against standalone Trino producing byte-identical output.

### 3.6 Differential Testing Against Trino Connector

The existing Trino-OpenSearch connector provides a baseline for semantic correctness. The DQE test suite includes a differential testing layer:

**Test data materialization**: A shared dataset (defined in `dqe-integ-test/src/test/resources/datasets/`) is bulk-indexed into OpenSearch at CI test start. The same dataset is accessible to both DQE (via `/_plugins/_sql?engine=dqe`) and a standalone Trino instance with the OpenSearch connector. Datasets include:
- Scalar types: all OpenSearch field types from §9.1 (keyword, text, long, integer, double, boolean, date, date_nanos, geo_point, nested, flattened, dense_vector, etc.).
- Edge cases: NULL-heavy columns, empty strings, multi-valued fields, nested arrays, custom date formats.
- Scale dataset: 1M+ rows for performance regression tests.

**Differential test execution**: For each query in the golden result suite:
1. Execute via DQE (`engine=dqe`) and capture result set + types.
2. Execute via standalone Trino with OpenSearch connector and capture result set + types.
3. Compare row-by-row. Differences are failures.

**Reuse of existing test data**: Where the existing `integ-test/` module already defines datasets and expected results for SQL queries, those datasets are reused by the DQE test suite. New DQE-specific datasets supplement but do not duplicate existing fixtures.

**CI integration**: Differential tests run as a REST API test script (`scripts/dqe-test/run-differential-tests.sh`) against a running OpenSearch cluster (`./gradlew :dqe-plugin:run`) and a standalone Trino instance (started via Docker). Test cases are declarative JSON files (query + expected results) under `scripts/dqe-test/cases/differential/`. Differential tests are required to pass before merging changes to DQE modules.

---

## 4. Architecture Overview

```
                        ┌──────────────────────────────────────────┐
                        │          Client (REST / JDBC)            │
                        └──────────────────┬───────────────────────┘
                                           │ POST /_plugins/_sql?engine=dqe
                        ┌──────────────────▼───────────────────────┐
                        │        Coordinating Node                  │
                        │  ┌─────────┐ ┌──────────┐ ┌───────────┐ │
                        │  │ Parser  │→│ Analyzer │→│ Optimizer │ │
                        │  └─────────┘ └──────────┘ └─────┬─────┘ │
                        │                                  │       │
                        │              ┌───────────────────▼─────┐ │
                        │              │  Distributed Planner    │ │
                        │              │  (Fragment + Schedule)  │ │
                        │              └───────┬─────────────────┘ │
                        │                      │                   │
                        │    ┌─────────────────┼──────────────┐    │
                        │    │ TransportAction: │              │    │
                        │    │ stage/execute    │              │    │
                        └────┼─────────────────┼──────────────┼────┘
                             │                 │              │
               ┌─────────────▼──┐  ┌───────────▼──┐  ┌───────▼──────────┐
               │  Data Node A   │  │  Data Node B │  │  Data Node C     │
               │ ┌────────────┐ │  │ ┌──────────┐ │  │ ┌──────────────┐ │
               │ │ Shard 0    │ │  │ │ Shard 1  │ │  │ │ Shard 2      │ │
               │ │ ┌────────┐ │ │  │ │┌────────┐│ │  │ │ ┌──────────┐ │ │
               │ │ │  Scan  │ │ │  │ ││  Scan  ││ │  │ │ │  Scan    │ │ │
               │ │ │ Filter │ │ │  │ ││ Filter ││ │  │ │ │  Filter  │ │ │
               │ │ │Project │ │ │  │ ││Project ││ │  │ │ │ Project  │ │ │
               │ │ │PartAgg │ │ │  │ ││PartAgg││ │  │ │ │ PartAgg  │ │ │
               │ │ └───┬────┘ │ │  │ │└───┬────┘│ │  │ │ └────┬─────┘ │ │
               │ └─────┼──────┘ │  │ └────┼─────┘ │  │ └──────┼───────┘ │
               └───────┼────────┘  └──────┼───────┘  └────────┼─────────┘
                       │                  │                    │
                       └──────────────────┼────────────────────┘
                                          │ Gather Exchange
                        ┌─────────────────▼────────────────────────┐
                        │        Coordinating Node                  │
                        │  ┌─────────────────────────────────────┐ │
                        │  │ Final Aggregate → Sort → Limit      │ │
                        │  └─────────────────────┬───────────────┘ │
                        │                        │                 │
                        │                   JSON Response          │
                        └──────────────────────────────────────────┘
```

**Key mapping**: Trino Split = OpenSearch Shard Copy (primary or replica).

**Split invariant**: For an index with N primary shards, the scheduler produces exactly N splits. Each split targets exactly one shard copy — either the primary or one replica for that logical shard, never both. Replica selection is for locality and load balancing (prefer local, then least-loaded), not for parallel double-reading. Scanning both primary and replica for the same shard violates this invariant and produces duplicate rows. The scheduler asserts that no two splits share the same logical shard ID.

If the chosen copy becomes unavailable mid-scan (node failure, shard relocation), the query fails for that split — there is no automatic retry on an alternate copy in Phase 1. Phase 3 may introduce per-split retry on a surviving copy if the PIT remains valid on that copy. Each selected shard copy receives a plan fragment and executes it as an operator pipeline using the local search API.

---

## 5. Module Structure

```
dqe/
  dqe-parser/              Trino SQL parser (shaded JAR)
  dqe-types/               Trino type system + OpenSearch type mapping contract
  dqe-metadata/            Catalog/schema/table from cluster state + stats
  dqe-analyzer/            Semantic analysis, scope resolution, type checking
  dqe-planner/             Logical planner, optimizer rules, physical planner, fragmenter
  dqe-execution/           Physical operators, operator context, driver, pipeline
  dqe-functions/           Function library (scalar, aggregate, window)
  dqe-exchange/            Exchange protocol over OpenSearch TransportService
  dqe-memory/              Memory tracking, circuit breaker integration, spill
  dqe-plugin/              REST API, plugin entry point, settings, transport action registration
  dqe-integ-test/          Integration tests, golden result suite, conformance tests
```

**Dependency graph**:

```
dqe-plugin
  → dqe-exchange → dqe-execution → dqe-planner → dqe-analyzer → dqe-metadata → dqe-types
  → dqe-memory (used by dqe-execution, dqe-exchange)
  dqe-parser (standalone, no internal deps)
  dqe-functions (used by dqe-planner, dqe-execution)
```

All Trino dependencies shaded into a single shadow JAR. Package relocation: `io.trino` → `org.opensearch.dqe.shaded.io.trino`, `io.airlift` → `org.opensearch.dqe.shaded.io.airlift`, `com.google.common` → `org.opensearch.dqe.shaded.com.google.common`.

### Dependency Boundary

The DQE lives within the `opensearch-sql` plugin repository and build system, but maintains a strict dependency boundary with existing engine modules.

**Allowed dependencies** (DQE modules may depend on):
- OpenSearch core: `Plugin`, `ActionPlugin`, `TransportService`, `ClusterState`, `SearchRequest`, `SearchResponse`, `CircuitBreaker`, `ThreadPool`, task management, settings framework.
- OpenSearch Security plugin interfaces: permission checks, FLS/DLS enforcement (via the search API).
- `opensearch-sql` plugin framework: `SQLPlugin` entry point for engine routing registration, shared REST handler infrastructure, test harness utilities.
- Shaded Trino artifacts (internal to DQE modules only).

**Disallowed dependencies** (DQE modules must NOT depend on):
- `org.opensearch.sql.sql.*` — existing SQL grammar, parser, or AST.
- `org.opensearch.sql.ppl.*` — PPL grammar, parser, or AST.
- `org.opensearch.sql.calcite.*` — Calcite-based planner, optimizer, or execution.
- `org.opensearch.sql.expression.*` — existing expression evaluation framework.
- `org.opensearch.sql.planner.*` — existing query planner.
- `org.opensearch.sql.executor.*` — existing query executor.
- `org.opensearch.sql.storage.*` — existing storage abstraction (DQE uses OpenSearch search API directly).

**Enforcement**: A Gradle dependency constraint or an ArchUnit architecture test validates that no `dqe-*` module transitively depends on disallowed packages. This check runs in CI and blocks merges on violation.

---

## 6. REST API

The DQE reuses the existing `/_plugins/_sql` endpoint rather than introducing a new top-level endpoint. Engine selection is controlled by a cluster setting and an optional per-request parameter.

### 6.1 Engine Routing

| Setting / Parameter | Values | Default | Description |
|---|---|---|---|
| `plugins.sql.engine` (cluster setting) | `calcite`, `dqe` | `calcite` | Default SQL engine for all queries |
| `engine` (request body field) | `calcite`, `dqe` | Value of cluster setting | Per-request engine override |

When `engine=dqe` is active (via setting or request body), the SQL plugin routes the request to the DQE execution path. When `engine=calcite` (or unset, defaulting to `calcite`), behavior is identical to today's Calcite-based engine. The two engines share no execution state — they are completely independent code paths behind a common REST surface.

**Coexistence rules**:
- Both engines parse SQL independently. A query that succeeds on one engine may fail on the other (different supported SQL subsets).
- The `_explain` endpoint respects the `engine` parameter and returns engine-specific plan output.
- Error responses include an `"engine": "dqe"` field so clients can identify which engine processed the request.
- There is no automatic fallback from DQE to Calcite (or vice versa). If a query is unsupported by the selected engine, it fails with an explicit error.
- The `plugins.dqe.enabled` setting (default: `true`) controls whether the DQE engine is loaded. When `false`, requests with `engine=dqe` are rejected with `DQE_DISABLED`.

### 6.2 Endpoints

| Endpoint | Method | Engine | Description |
|---|---|---|---|
| `/_plugins/_sql` | POST | Both | Execute SQL query synchronously (engine selected per §6.1) |
| `/_plugins/_sql/_explain` | POST | Both | Return query plan without executing |
| `/_plugins/_sql/_async` | POST | DQE only | Submit long-running query, return query ID |
| `/_plugins/_sql/_async/{queryId}` | GET | DQE only | Poll async query status/results |
| `/_plugins/_sql/_async/{queryId}` | DELETE | DQE only | Cancel async query |

### 6.3 Request Format

```json
{
  "query": "SELECT name, COUNT(*) as cnt FROM orders WHERE status='shipped' GROUP BY name ORDER BY cnt DESC LIMIT 10",
  "engine": "dqe",
  "fetch_size": 1000,
  "session_properties": {
    "query_max_memory": "256MB",
    "query_timeout": "5m"
  }
}
```

### 6.4 Response Format

```json
{
  "engine": "dqe",
  "schema": [
    {"name": "name", "type": "VARCHAR"},
    {"name": "cnt", "type": "BIGINT"}
  ],
  "data": [
    ["Alice", 1523],
    ["Bob", 1201]
  ],
  "stats": {
    "state": "FINISHED",
    "query_id": "dqe_abc123",
    "elapsed_ms": 142,
    "rows_processed": 50000,
    "bytes_processed": 4200000,
    "stages": 2,
    "shards_queried": 5
  },
  "next_uri": null
}
```

### 6.5 Registration

DQE execution is registered as an engine backend within the existing SQL plugin (`SQLPlugin.java`). The engine routing layer dispatches to either the Calcite path or the DQE path based on the resolved engine selection. DQE-specific transport actions are registered separately and gated by `plugins.dqe.enabled`. Handlers follow the standard OpenSearch pattern: parse request → wrap in transport request → dispatch via `nodeClient.execute()`.

---

## 7. SQL Parser Integration

**Strategy**: Shade Trino's `trino-parser` JAR (standalone module, minimal dependencies) into the DQE plugin. Relocate to `org.opensearch.dqe.shaded.io.trino.sql.parser` to avoid ANTLR4 conflicts.

```java
public class DqeSqlParser {
    private final SqlParser parser;

    public DqeSqlParser() {
        SqlParser.Options options = SqlParser.Options.builder()
            .setDecimalLiteralTreatment(DOUBLE)
            .build();
        this.parser = new SqlParser(options);
    }

    public Statement parse(String sql) {
        try {
            return parser.createStatement(sql);
        } catch (ParsingException e) {
            throw new DqeParsingException(e.getMessage(), e.getLineNumber(), e.getColumnNumber());
        }
    }
}
```

The full Trino AST hierarchy is reused: `Statement`, `Query`, `QuerySpecification`, `Expression`, `Relation`. No custom grammar extensions. OpenSearch-specific features (full-text search) are exposed as standard function calls (e.g., `os_match(field, 'query text')`) that the analyzer recognizes as OpenSearch UDFs.

---

## 8. Metadata Layer

### 8.1 Catalog–Schema–Table Mapping

| Trino Concept | OpenSearch Concept |
|---|---|
| Catalog | Fixed: `opensearch` (single cluster) |
| Schema | `default` (all indices), or alias-group namespace |
| Table | Index name or index alias |
| Column | Field in index mapping |

### 8.2 Metadata Resolution

```java
public class DqeMetadata {
    // Resolve table from cluster state (no remote calls)
    public DqeTableHandle getTableHandle(String schema, String tableName);

    // Resolve columns from index mapping
    public List<DqeColumnHandle> getColumnHandles(DqeTableHandle table);

    // Shard routing from cluster state
    public List<DqeShardSplit> getSplits(DqeTableHandle table);

    // Table-level statistics (lazy, cached)
    public DqeTableStatistics getStatistics(DqeTableHandle table);
}
```

**Schema snapshot**: At query analysis time, index mappings are read from `ClusterState` (local, no network call) and frozen for the lifetime of the query. Field type conflicts across indices in an index pattern are resolved by widening to the most general compatible type (e.g., `integer` + `long` → `BIGINT`). If types are incompatible (e.g., `keyword` vs `long` for the same field name), the field is excluded and a warning is emitted.

### 8.3 Statistics for Cost-Based Optimization

| Statistic | Source | Cost | Cache TTL |
|---|---|---|---|
| Row count | `_stats` API (`docs.count`) | Cheap (cluster state) | 5 min |
| Index size (bytes) | `_stats` API (`store.size_in_bytes`) | Cheap | 5 min |
| Column cardinality | `cardinality` aggregation (sampled) | Moderate | 5 min |
| Column min/max | `min`/`max` aggregation (sampled) | Moderate | 5 min |
| NULL fraction | `missing` aggregation (sampled) | Moderate | 5 min |

Statistics are gathered **lazily** — only when the optimizer requests them for a specific table or column. Sampled statistics use the `sampler` aggregation with configurable sample size (default: 10,000 docs) to bound cost.

**Stale stats mitigation**: Statistics include a generation number from `_stats`. If the generation advances beyond a threshold (>10% change in doc count), the cache is invalidated proactively. The optimizer uses conservative heuristics (assume larger tables, higher cardinality) when statistics are unavailable rather than waiting for collection.

---

## 9. Type System and Type Mapping Contract

### 9.1 Type Mapping

| OpenSearch Type | DQE (Trino) Type | Sorting | Notes |
|---|---|---|---|
| `keyword` | `VARCHAR` | Yes (lexicographic) | Direct mapping |
| `text` | `VARCHAR` | Only if `fielddata: true` | Analyzed; equality comparisons use keyword sub-field if present |
| `long` | `BIGINT` | Yes | |
| `integer` | `INTEGER` | Yes | |
| `short` | `SMALLINT` | Yes | |
| `byte` | `TINYINT` | Yes | |
| `double` | `DOUBLE` | Yes | |
| `float` / `half_float` | `REAL` | Yes | |
| `scaled_float` | `DECIMAL(p,s)` | Yes | `p` and `s` derived from `scaling_factor` |
| `boolean` | `BOOLEAN` | Yes | |
| `date` (epoch_millis) | `TIMESTAMP(3)` | Yes | Millisecond precision |
| `date` (custom format) | `TIMESTAMP(3)` | Yes | Parsed using format from mapping |
| `date_nanos` | `TIMESTAMP(9)` | Yes | Nanosecond precision |
| `ip` | `VARCHAR` | Yes (lexicographic) | Stored as string; IP-specific functions planned |
| `geo_point` | `ROW(lat DOUBLE, lon DOUBLE)` | No | Accessible via `field.lat`, `field.lon` |
| `geo_shape` | `VARCHAR` (GeoJSON) | No | Queryable via geo functions |
| `nested` | `ARRAY(ROW(...))` | No | Requires UNNEST for access |
| `object` | `ROW(...)` | Per sub-field | Dot notation access |
| `flattened` | `MAP(VARCHAR, VARCHAR)` | No | Key-value access |
| `dense_vector` | `ARRAY(REAL)` | No | k-NN functions planned |
| `binary` | `VARBINARY` | No | Base64 decoded |
| `unsigned_long` | `DECIMAL(20,0)` | Yes | Exceeds BIGINT range |

### 9.2 Multi-Field Handling

For `text` fields with a `keyword` sub-field: equality (`=`), `IN`, and sorting operations transparently use the `.keyword` sub-field for pushdown. Full-text predicates (via `os_match()`) use the `text` field. This behavior is documented and deterministic.

### 9.3 Array Detection

OpenSearch does not distinguish arrays in mappings. Arrays are detected by:
1. Explicit `_meta.dqe.arrays` annotation on the index (preferred).
2. Sampling the first N documents (configurable, default 100) to detect multi-valued fields.
3. If unknown, treat as scalar; user gets a runtime error on multi-value access.

---

## 10. Query Planning Pipeline

```
SQL String
  → [DqeSqlParser]         Parse to Trino AST
    → [DqeAnalyzer]        Resolve tables/columns/types against DqeMetadata
      → [Logical Planner]  Build Trino PlanNode tree
        → [DqeOptimizer]   Rule-based + cost-based optimization
          → [Physical Planner]  Convert to physical operators
            → [Distributed Planner]  Fragment plan at exchange boundaries
              → [Scheduler]  Map fragments to shards/nodes via ClusterState
```

### 10.1 Optimizer Rules (Selectively Included)

| Category | Rules | Phase |
|---|---|---|
| Predicate pushdown | `PushPredicateIntoTableScan`, `PushFilterThroughProject` | 1 |
| Projection pruning | `PruneTableScanColumns`, `PruneFilterColumns` | 1 |
| Limit pushdown | `PushLimitThroughProject`, `PushTopNThroughProject` | 1 |
| Aggregation split | `PushPartialAggregationThroughExchange` | 2 |
| Aggregation pushdown | `PushAggregationIntoTableScan` (when OpenSearch agg is available) | 2 |
| Join reordering | `ReorderJoins` (CBO-driven) | 4 |
| Join strategy selection | `DetermineJoinDistributionType` | 3 |
| Exchange placement | `AddExchanges` (DQE-adapted) | 3 |

**OpenSearch-specific optimization rules** (new):
- `PushFilterToSearchQuery`: Convert predicates into Query DSL within `ShardScanOperator` (term, range, bool, exists).
- `PushAggregationToSearchAgg`: Convert simple aggregations into OpenSearch aggregation framework calls when pattern matches (e.g., single GROUP BY + COUNT/SUM/AVG → terms + stats aggregation). This optimization is opportunistic — if the pattern does not match, the DQE falls back to operator-based execution.
- `DetectColocatedJoin`: Check if both indices share routing key as join key.

### 10.2 Aggregation Execution Strategy

Two aggregation paths:

**Path A — OpenSearch aggregation pushdown** (preferred when pattern matches):
- The optimizer detects that a `GROUP BY` + aggregate pattern maps directly to an OpenSearch aggregation (e.g., `terms` + `stats`).
- The `ShardScanOperator` issues a single search request with both a query filter and aggregation body.
- Results come back as aggregation buckets, not as raw hits. Zero row materialization.
- **Expected improvement: 100–1000x** over connector approach.

**Path B — Operator-based partial aggregation** (fallback for complex expressions):
- The optimizer splits the aggregation into `PARTIAL` (per-shard) and `FINAL` (coordinator).
- The per-shard `PartialHashAggregateOperator` reads hits from `ShardScanOperator`, computes partial aggregates in a hash table.
- The coordinator merges partial aggregates.
- **Expected improvement: 10–50x** over connector approach (avoids network serialization of raw rows, but still materializes rows per-shard).

The `_explain` output explicitly labels which path each aggregation uses.

---

## 11. Distributed Execution Model

### 11.1 Execution Hierarchy

| Concept | Description |
|---|---|
| **Query** | Top-level unit. One query = one REST request. Has a unique `queryId`. |
| **Stage** | A fragment of the plan separated by exchange boundaries. Stages form a DAG. |
| **Task** | A stage executing on a specific node. A stage with 5 shards on 3 nodes = 3 tasks. |
| **Pipeline** | An operator chain within a task. A task may have multiple pipelines (e.g., build + probe for join). |
| **Driver** | A single-threaded execution unit processing one pipeline on one shard. |
| **Operator** | A single processing step (scan, filter, project, aggregate, etc.). |

### 11.2 Shard-Local Execution

Each primary shard receives a plan fragment. The `ShardScanOperator` builds a `SearchRequest` targeting only its shard:

```java
SearchRequest request = new SearchRequest(indexName)
    .preference("_shards:" + shardId + "|_local")
    .source(new SearchSourceBuilder()
        .query(pushedDownQuery)       // Query DSL from optimizer
        .fetchSource(requiredFields)  // Column pruning
        .size(batchSize)              // Default 1000
        .sort("_doc")                 // Efficient iteration
        .searchAfter(lastSortValues)  // Pagination (not scroll)
    );
```

The operator iterates `SearchHits`, converts each hit into a columnar `Page` (batch of `Block` arrays), and feeds downstream operators (filter → project → partial aggregate).

**Data access is always through the search API.** This is non-negotiable because:
- FLS filters out restricted fields at the search layer.
- DLS injects document-level query restrictions at the search layer.
- Bypassing the search API would create a security hole.

### 11.3 Coordinator Execution

After gathering partial results from all shards, the coordinator runs the final stage:
- `FinalHashAggregateOperator`: Merges partial aggregation states.
- `SortOperator` / `TopNOperator`: Final ordering.
- `LimitOperator`: Truncates output.
- `JoinOperator`: Executes joins (in Phases 3+).
- `WindowOperator`: Computes window functions (in Phase 4).

### 11.4 Data Format

The DQE uses Trino's columnar `Page`/`Block` format internally:
- `Page`: A batch of rows represented as an array of `Block` objects (one per column).
- `Block`: Typed columnar array (`LongArrayBlock`, `VariableWidthBlock`, `DictionaryBlock`, etc.).
- Target page size: ~64KB–1MB depending on column count and types.

For transport serialization, `Page` is encoded into a `DqeDataPage` implementing OpenSearch's `Writeable` interface with binary columnar encoding (no JSON intermediate).

---

## 12. Exchange Protocol

### 12.1 Transport Actions

```
internal:dqe/exchange/push     — push a chunk of data pages from producer to consumer
internal:dqe/exchange/request   — consumer requests data from producer (pull-based alternative)
internal:dqe/exchange/close     — producer signals completion
internal:dqe/exchange/abort     — either side signals failure
internal:dqe/stage/execute      — coordinator sends plan fragment to data node
internal:dqe/stage/cancel       — coordinator cancels a running stage
```

### 12.2 Message Framing

Each exchange message (`DqeExchangeChunk`) contains:

| Field | Type | Description |
|---|---|---|
| `queryId` | String | Identifies the query |
| `stageId` | int | Identifies the stage |
| `partitionId` | int | Hash partition (for repartition exchange) |
| `sequenceNumber` | long | Monotonically increasing per channel, for ordering and dedup |
| `pages` | List<DqeDataPage> | One or more serialized Pages |
| `isLast` | boolean | Signals end of data for this partition |
| `uncompressedBytes` | long | For accounting before decompression |

**Chunk size**: Configurable, default 1MB. This bounds the memory for a single in-flight message on the transport layer. Large result sets are chunked into multiple messages.

**Compression**: LZ4 frame compression on the `pages` payload. Typical compression ratio for columnar data: 3–5x.

### 12.3 Flow Control and Backpressure

Each exchange channel has a bounded buffer:

```
Producer → [Buffer: max 32MB] → Consumer
```

- **Producer-side**: If the buffer is full, the producer's driver thread blocks (bounded wait with configurable timeout, default 30s). If the timeout expires, the query is cancelled with `EXCHANGE_BUFFER_TIMEOUT`.
- **Consumer-side**: Pulls pages from the buffer. If the buffer is empty, the consumer blocks until data arrives or a timeout expires.
- **Buffer memory** is tracked under the `dqe` circuit breaker (see §15). If the circuit breaker trips, in-flight exchanges are aborted and the query fails with `CIRCUIT_BREAKER_TRIPPED`.

### 12.4 Exchange Patterns

| Pattern | Use Case | Mechanism |
|---|---|---|
| **Gather** | Shard results → coordinator | All producers push to a single coordinator buffer |
| **Repartition** | Distributed GROUP BY, hash join | Producers hash-partition rows and push to N consumers |
| **Broadcast** | Small-table join | Producers replicate all pages to every consumer |

### 12.5 Failure Handling

- **Node failure mid-exchange**: The query fails. No automatic retry of partial stages (complexity not justified for analytical workloads).
- **Transport timeout**: Default 60s per chunk delivery. Configurable. Timeout triggers query cancellation.
- **Deduplication**: `sequenceNumber` per channel allows the consumer to detect and discard duplicate deliveries from transport retries.

---

## 13. Join Strategies

### 13.1 Strategy Selection (Incremental)

| Strategy | Phase | When Used | Shuffle Required |
|---|---|---|---|
| Coordinator-side join | 3 | Both sides < `join.coordinator_threshold` (default 100MB) | Gather only |
| Broadcast join | 3 | Build side < `join.broadcast_threshold` (default 100MB) | Broadcast small side |
| Colocated join | 4 | Both indices use same `_routing` as join key | None |
| Hash-partitioned join | 4 | Both sides large, no colocation | Full repartition |

### 13.2 Coordinator-Side Join

Both tables are gathered to the coordinator. The coordinator builds a hash table on the smaller side and probes with the larger side. Memory-bounded: if combined size exceeds threshold, the query fails with an explicit error recommending broadcast or hash-partitioned join (once available).

### 13.3 Broadcast Join

The build side (smaller table) is collected at the coordinator, then broadcast to all data nodes holding probe-side shards. Each data node builds a local hash table and probes against its local shards. The probe side never leaves the data node.

### 13.4 Colocated Join

**OpenSearch-specific optimization**: If both indices use `_routing` on the join key, documents with the same key are guaranteed to be on the same node. The join executes entirely node-local with zero shuffle. The optimizer detects this by checking `_routing` configuration in index settings.

### 13.5 Semi-Join Optimization

For `IN (SELECT ...)` and `EXISTS` subqueries: first execute the subquery to collect distinct join keys, then use OpenSearch's `terms` query with the collected keys to filter the outer table at the shard level. This avoids materializing the full outer table.

---

## 14. Function Library

### 14.1 Phase 2 Functions (~100 Core Functions)

**Scalar**:
- **String**: `LENGTH`, `SUBSTRING`, `CONCAT`, `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_EXTRACT`, `SPLIT`, `STARTS_WITH`, `POSITION`
- **Math**: `ABS`, `CEIL`, `FLOOR`, `ROUND`, `TRUNCATE`, `SQRT`, `POWER`, `LOG`, `LOG2`, `LOG10`, `MOD`, `SIGN`, `RAND`
- **Date/Time**: `DATE_TRUNC`, `DATE_ADD`, `DATE_DIFF`, `EXTRACT`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `NOW`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `FROM_UNIXTIME`, `TO_UNIXTIME`, `DATE_FORMAT`, `DATE_PARSE`
- **Conditional**: `COALESCE`, `NULLIF`, `IF`, `CASE` (expression), `TRY`
- **Conversion**: `CAST`, `TRY_CAST`, `TYPEOF`
- **JSON**: `JSON_EXTRACT`, `JSON_EXTRACT_SCALAR`, `JSON_ARRAY_LENGTH`

**Aggregate**:
- `COUNT`, `COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX`
- `COUNT(DISTINCT ...)`, `APPROX_DISTINCT`
- `ARRAY_AGG`, `BOOL_AND`, `BOOL_OR`
- `STDDEV`, `VARIANCE`, `STDDEV_POP`, `VAR_POP`
- `PERCENTILE_CONT`, `PERCENTILE_DISC` (approximate via t-digest)

**OpenSearch-specific UDFs** (new):
- `OS_MATCH(field, query)` — full-text match query
- `OS_MATCH_PHRASE(field, query)` — phrase match
- `OS_SCORE()` — relevance score of matching document
- `OS_HIGHLIGHT(field)` — highlighted snippets
- `OS_GEO_DISTANCE(field, lat, lon)` — distance calculation

### 14.2 Phase 4 Functions

**Window**: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `CUME_DIST`, `PERCENT_RANK`

**Array/Map**: `ELEMENT_AT`, `CARDINALITY`, `CONTAINS`, `ARRAY_JOIN`, `FLATTEN`, `MAP_KEYS`, `MAP_VALUES`, `MAP_ENTRIES`

---

## 15. Memory Management and Circuit Breaker Integration

### 15.1 Architecture

```
OpenSearch Parent Circuit Breaker
  ├── fielddata breaker
  ├── request breaker
  ├── in_flight_requests breaker
  └── dqe breaker (NEW)             ← configurable % of heap (default 20%)
        ├── query Q1 budget
        │     ├── stage S0 (coordinator)
        │     │     ├── pipeline P0: operators' working memory
        │     │     └── exchange buffers (incoming)
        │     └── stage S1 (per-shard)
        │           └── pipeline P0: operators' working memory
        ├── query Q2 budget
        └── ...
```

### 15.2 Tracking Contract

Every DQE memory allocation goes through a `DqeMemoryTracker`:

```java
public class DqeMemoryTracker {
    // Attempt to reserve bytes. Throws CircuitBreakingException if limit exceeded.
    public void reserve(long bytes, String label);

    // Release previously reserved bytes.
    public void release(long bytes, String label);

    // Current usage.
    public long getUsedBytes();
}
```

The `DqeMemoryTracker` wraps OpenSearch's `CircuitBreaker` interface. Every call to `reserve()` also checks the parent breaker. This means:
- If the `dqe` breaker limit is hit, only DQE queries fail.
- If the parent breaker limit is hit (total heap pressure from search + indexing + DQE), the DQE query fails — protecting the cluster.

### 15.3 Per-Query Limits

Each query has a memory budget (default 256MB, configurable per-query via `session_properties.query_max_memory`). Within a query:
- Operators track their allocation via `OperatorContext.reserveMemory()`.
- Exchange buffers track their allocation separately.
- If a query exceeds its budget, the DQE attempts to spill (if the operator supports it). If spill fails or is disabled, the query fails with `EXCEEDED_QUERY_MEMORY_LIMIT`.

### 15.4 Spill-to-Disk

Spill-capable operators: `HashAggregationOperator`, `HashJoinBuildOperator`, `SortOperator`.

When an operator's memory exceeds its revocable threshold:
1. Operator serializes in-memory data to a temporary file in `plugins.dqe.spill.path` (default: `<data_dir>/dqe-spill/`).
2. Memory is released back to the breaker.
3. Operator reads spilled data back in merge passes during finalization.

**Spill limits**: Max `plugins.dqe.spill.max_disk_bytes` (default 10GB) per node. If exceeded, query fails with `EXCEEDED_SPILL_LIMIT`.

### 15.5 Cancellation on Breaker Trip

When the `dqe` or parent circuit breaker trips:
1. The `CircuitBreakingException` is caught by the operator/driver.
2. The driver marks the query as `FAILED`.
3. Cancellation propagates to all stages on all nodes (see §18).
4. Exchange buffers are drained and released.
5. Spill files are deleted.

---

## 16. Resource Isolation and Thread Pools

### 16.1 Thread Pool Configuration

| Pool Name | Type | Size | Queue | Rejection Policy | Purpose |
|---|---|---|---|---|---|
| `dqe_worker` | Fixed | `min(4, cores/2)` | 100 | `AbortPolicy` (query fails) | Execute operator pipelines |
| `dqe_exchange` | Fixed | `min(2, cores/4)` | 200 | `AbortPolicy` (query fails) | Handle exchange send/receive |
| `dqe_coordinator` | Fixed | `min(2, cores/4)` | 50 | `AbortPolicy` (query fails) | Plan, schedule, final merge |

**Rationale**: DQE pools are deliberately smaller than search pools to ensure search/indexing workloads are never starved. Sizing is conservative; operators can adjust upward after observing production behavior.

### 16.2 Admission Control

A node-level semaphore limits concurrent DQE queries (default: 10, configurable via `plugins.dqe.max_concurrent_queries`). When the semaphore is full, new queries are rejected immediately with `TOO_MANY_CONCURRENT_QUERIES` (HTTP 429). There is no queueing at the admission level — clients are expected to retry.

### 16.3 Per-Query Resource Limits

| Limit | Default | Configurable | Enforcement |
|---|---|---|---|
| Wall-clock timeout | 5 min | `plugins.dqe.query_timeout` | Timer cancels query |
| Memory per query | 256 MB | `session_properties.query_max_memory` | Circuit breaker |
| Rows scanned | Unlimited | `session_properties.max_rows_scanned` | Counter in scan operator |
| Result rows | 10,000 | `session_properties.max_result_rows` | Counter in output |

---

## 17. Consistency Model

### 17.1 Single-Table Queries

A **Point-in-Time (PIT)** is created for the target index at query start. All shard-level scans use this PIT, providing a consistent snapshot of the index across all shards. Documents indexed or deleted after PIT creation are not visible to the query.

### 17.2 Multi-Table Queries (Joins)

A PIT is created for **each** table at query start. PITs are created in quick succession but are not atomic — there is a small window during which the tables may reflect different points in time. This is acceptable for analytical workloads and is explicitly documented:

> **DQE provides per-table point-in-time consistency, not cross-table snapshot isolation.** Two tables in a JOIN may reflect slightly different points in time (typically within milliseconds). This matches the behavior of running two independent OpenSearch queries in sequence.

### 17.3 PIT Lifecycle

- PITs are created before the first shard scan operator executes.
- PITs are released in the query cleanup phase (both success and failure paths).
- PIT keep-alive is set to `query_timeout + 1m` to avoid expiration during long queries.
- If a PIT expires mid-query (e.g., node restart), the query fails with `PIT_EXPIRED`.

---

## 18. Cancellation and Task Lifecycle

### 18.1 OpenSearch Task Integration

Every DQE query registers as a `CancellableTask` in OpenSearch's task management framework:

```java
public class DqeQueryTask extends CancellableTask {
    private final String queryId;
    private final String sqlQuery;
    // Cancellation callback chain
}
```

This means:
- DQE queries are visible in `_tasks` API with `action:internal:dqe/*`.
- Users can cancel a query via `POST _tasks/<taskId>/_cancel`.
- The `/_plugins/_sql/_async/{queryId} DELETE` endpoint also triggers cancellation via the task framework.

### 18.2 Cancellation Propagation

```
User cancels (REST DELETE or _tasks/_cancel)
  → Coordinator marks query as CANCELLING
    → Coordinator sends internal:dqe/stage/cancel to all data nodes with active stages
      → Each data node:
        1. Interrupts driver threads (sets interrupt flag)
        2. Operators check interrupt flag in their processing loops
        3. Exchange producers abort (send exchange/abort)
        4. Exchange buffers are drained and memory released
        5. Spill files are deleted
        6. PIT is released
        7. Node sends cancellation ACK to coordinator
    → Coordinator transitions query to CANCELLED after all ACKs (or timeout)
```

### 18.3 Plugin Disable / Node Shutdown

When `plugins.dqe.enabled` is set to `false` or the node is shutting down:
1. New queries are rejected immediately.
2. Running queries are cancelled with `PLUGIN_DISABLED` / `NODE_SHUTTING_DOWN`.
3. The cancellation path (§18.2) is followed.
4. Thread pools are shut down after all tasks complete (or after a grace period of 30s).

---

## 19. Failure Modes and Recovery

| # | Symptom | Root Cause | Signal / Metric | Automated Mitigation | Operator Action |
|---|---|---|---|---|---|
| F1 | Query fails with `CIRCUIT_BREAKER_TRIPPED` | DQE memory usage exceeds breaker limit | `dqe.breaker.tripped` counter | Query cancelled, memory released | Lower `query_max_memory`, reduce concurrency, or increase breaker limit |
| F2 | Query fails with `EXCHANGE_BUFFER_TIMEOUT` | Consumer too slow or dead, producer buffer full for >30s | `dqe.exchange.timeout` counter | Query cancelled, buffers released | Investigate slow consumer node, check GC pressure |
| F3 | Query fails with `EXCEEDED_SPILL_LIMIT` | Spill directory usage exceeds `spill.max_disk_bytes` | `dqe.spill.disk_bytes` gauge | Query cancelled, spill files cleaned | Increase spill limit, reduce concurrent large queries, add disk capacity |
| F4 | Query hangs, then times out | Node restart mid-exchange, downstream waiting for data | `dqe.query.timed_out` counter | Query cancelled after `query_timeout` | Review node stability; DQE does not retry partial stages |
| F5 | Search workload degrades | DQE thread pools saturated, transport congested | `dqe_worker.active` gauge, search latency P99 | Admission control rejects new DQE queries | Reduce `max_concurrent_queries`, add analytics nodes |
| F6 | Broadcast join OOM | Build-side table larger than expected, exceeds memory | `dqe.breaker.tripped` counter with join operator label | Query cancelled | Lower `join.broadcast_threshold`, use hash join instead |
| F7 | Slow spill I/O | Spill directory on slow / shared disk | `dqe.spill.latency_ms` histogram | None (query proceeds slowly) | Move spill path to fast local SSD |
| F8 | PIT expired | Long-running query outlasts PIT keep-alive, or node restart | `PIT_EXPIRED` error | Query fails (no recovery) | Increase `query_timeout`, investigate node restarts |
| F9 | Type mismatch at runtime | Schema changed between plan and execution | `DqeTypeMismatchException` | Query fails with diagnostic | Re-run query (picks up new schema) |
| F10 | Transport saturation | Large shuffle + cluster management traffic | `transport.outbound_bytes` gauge | Exchange backpressure slows producers | Reduce DQE concurrency, add analytics nodes |

---

## 20. Security Integration

### 20.1 Endpoint Protection

When `engine=dqe` is active, requests to `/_plugins/_sql` are protected by the OpenSearch Security plugin. Users must have a cluster permission (e.g., `cluster:admin/opensearch/dqe/query`) in addition to standard SQL permissions to submit DQE queries.

### 20.2 Index-Level Access Control

The `DqeAnalyzer` checks index-level permissions before resolving tables. If a user lacks read access to any index referenced in the query, analysis fails with `ACCESS_DENIED` before any execution occurs.

### 20.3 FLS / DLS Enforcement

**Field-Level Security**: Enforced by the search API. When `ShardScanOperator` issues a `SearchRequest`, the security plugin filters out restricted fields from `_source` and `docvalue_fields`. The DQE operator pipeline never sees restricted fields.

**Document-Level Security**: Enforced by the search API. The security plugin injects an additional query filter that restricts which documents are visible. The DQE scan operator receives only permitted documents.

**No additional DQE logic is required for FLS/DLS.** This is the primary reason the search API is the sole data access path.

### 20.4 Cross-Index Security

For JOIN queries involving multiple indices, the user must have read access to **all** indices. The analyzer validates this upfront. If access is denied on any index, the entire query is rejected.

### 20.5 Audit Logging

Query text, user identity, indices accessed, and execution outcome (success/failure/cancel) are logged to the OpenSearch security audit log.

---

## 21. Observability

### 21.1 Phase 1 Observability (Required Before GA)

**Query-level metrics** (exposed via `_nodes/stats` and `_plugins/_sql/_dqe/stats`):

| Metric | Type | Description |
|---|---|---|
| `dqe.queries.total` | Counter | Total queries submitted |
| `dqe.queries.succeeded` | Counter | Queries completed successfully |
| `dqe.queries.failed` | Counter | Queries failed (by error category) |
| `dqe.queries.cancelled` | Counter | Queries cancelled |
| `dqe.queries.active` | Gauge | Currently executing queries |
| `dqe.queries.wall_time_ms` | Histogram | Wall-clock query latency |
| `dqe.queries.rows_returned` | Histogram | Rows in query result |
| `dqe.queries.rows_scanned` | Counter | Total rows scanned across all shards |
| `dqe.queries.bytes_scanned` | Counter | Total bytes scanned |

**Memory metrics**:

| Metric | Type | Description |
|---|---|---|
| `dqe.memory.used_bytes` | Gauge | Current DQE memory usage |
| `dqe.memory.breaker_limit` | Gauge | DQE breaker limit |
| `dqe.memory.breaker_tripped` | Counter | Times DQE breaker has tripped |

**Thread pool metrics**: Standard OpenSearch thread pool stats for `dqe_worker`, `dqe_exchange`, `dqe_coordinator` (active, queue, rejected, completed).

### 21.2 Phase 2 Observability (Aggregations)

**Per-query plan and execution stats** (returned in `_explain` and optionally in query response with `include_stats=true`):

| Field | Description |
|---|---|
| `planned_stages` | Number of stages in distributed plan |
| `planned_operators` | Operator tree per stage |
| `actual_rows_per_stage` | Rows produced by each stage |
| `actual_time_per_stage_ms` | Wall time per stage |
| `aggregation_path` | `SEARCH_AGG_PUSHDOWN` or `OPERATOR_BASED` |

### 21.3 Phase 3 Observability (Exchanges and Joins)

| Metric | Type | Description |
|---|---|---|
| `dqe.exchange.bytes_sent` | Counter | Total bytes sent via exchange |
| `dqe.exchange.bytes_received` | Counter | Total bytes received |
| `dqe.exchange.buffer_occupancy` | Gauge | Current buffer fill ratio |
| `dqe.exchange.backpressure_time_ms` | Counter | Time producers spent blocked |
| `dqe.exchange.timeout` | Counter | Exchange buffer timeouts |
| `dqe.spill.bytes_written` | Counter | Bytes spilled to disk |
| `dqe.spill.bytes_read` | Counter | Bytes read from spill |
| `dqe.spill.active_files` | Gauge | Currently open spill files |
| `dqe.spill.latency_ms` | Histogram | Spill I/O latency |

### 21.4 Slow Query Log

Queries exceeding a configurable threshold (`plugins.dqe.slow_query_log.threshold`, default 10s) are logged at WARN level with:
- Query text (truncated to 1000 chars)
- Execution time
- Rows scanned / returned
- Stages / shards involved
- Memory peak usage
- Whether spill was used

---

## 22. Configuration Reference

| Setting | Default | Description |
|---|---|---|
| `plugins.sql.engine` | `calcite` | Default SQL engine (`calcite` or `dqe`) |
| `plugins.dqe.enabled` | `true` | Enable/disable DQE engine availability |
| `plugins.dqe.max_concurrent_queries` | `10` | Max concurrent queries per node |
| `plugins.dqe.query_timeout` | `5m` | Query execution timeout |
| `plugins.dqe.memory.breaker_limit` | `20%` | Max heap fraction for DQE circuit breaker |
| `plugins.dqe.memory.query_max_memory` | `256MB` | Default max memory per query |
| `plugins.dqe.exchange.buffer_size` | `32MB` | Buffer size per exchange channel |
| `plugins.dqe.exchange.chunk_size` | `1MB` | Max size of a single exchange message |
| `plugins.dqe.exchange.timeout` | `60s` | Transport timeout per exchange chunk |
| `plugins.dqe.exchange.backpressure_timeout` | `30s` | Max time producer blocks on full buffer |
| `plugins.dqe.spill.enabled` | `true` | Enable spill-to-disk |
| `plugins.dqe.spill.path` | `<data_dir>/dqe-spill` | Spill directory |
| `plugins.dqe.spill.max_disk_bytes` | `10GB` | Max disk usage for spill |
| `plugins.dqe.join.coordinator_threshold` | `100MB` | Max combined table size for coordinator-side join |
| `plugins.dqe.join.broadcast_threshold` | `100MB` | Max build-side size for broadcast join |
| `plugins.dqe.optimizer.join_reordering` | `AUTOMATIC` | Join reordering: `AUTOMATIC`, `ELIMINATE_CROSS_JOINS`, `NONE` |
| `plugins.dqe.statistics.cache_ttl` | `5m` | Statistics cache duration |
| `plugins.dqe.statistics.sample_size` | `10000` | Documents sampled for cardinality estimation |
| `plugins.dqe.scan.batch_size` | `1000` | Documents per search request in scan operator |
| `plugins.dqe.slow_query_log.threshold` | `10s` | Slow query log threshold |
| `plugins.dqe.worker.pool_size` | `min(4, cores/2)` | Worker thread pool size |
| `plugins.dqe.exchange.pool_size` | `min(2, cores/4)` | Exchange thread pool size |
| `plugins.dqe.coordinator.pool_size` | `min(2, cores/4)` | Coordinator thread pool size |

---

## 23. Packaging, Shading, and Release Process

> Addresses review §D: "Shading automation, licensing, upgrade policy."

### 23.1 Shading Strategy

All Trino and transitive dependencies are shaded via Gradle Shadow plugin in `dqe-plugin/build.gradle`:

```groovy
shadowJar {
    relocate 'io.trino', 'org.opensearch.dqe.shaded.io.trino'
    relocate 'io.airlift', 'org.opensearch.dqe.shaded.io.airlift'
    relocate 'com.google.common', 'org.opensearch.dqe.shaded.com.google.common'
    relocate 'com.fasterxml.jackson', 'org.opensearch.dqe.shaded.com.fasterxml.jackson'
    relocate 'org.antlr', 'org.opensearch.dqe.shaded.org.antlr'
    // Additional relocations as needed
}
```

### 23.2 Build Validation

The CI pipeline includes:

1. **No-leak check**: A build step scans the shaded JAR to verify no unshaded `io.trino`, `io.airlift`, or `com.google.common` classes remain.
2. **Linkage smoke test**: Load the shaded JAR into an OpenSearch test node and verify parser, analyzer, and executor can process a basic query without `ClassNotFoundException` or `NoSuchMethodError`.
3. **Classpath conflict check**: A Gradle task (using `shadow-dependency-analyzer` or custom script) reports any duplicate classes between the DQE shaded JAR and OpenSearch core.

### 23.3 Upgrade and CVE Patch Policy

Trino version upgrades are not tied to a fixed cadence. Each upgrade is treated as a deliberate migration, not a routine update.

| Event | Action | Timeline |
|---|---|---|
| New Trino release (biweekly) | Monitor changelog. No action unless it contains a security fix or a bug fix relevant to DQE-used components. | N/A |
| Trino security CVE (DQE-used component) | **Option A**: Cherry-pick the fix into the pinned version, re-shade, run conformance suite. **Option B**: Bump to the Trino release containing the fix, re-shade, run full regression. Choose the lower-risk option. | Within 1 week of CVE disclosure |
| Trino security CVE (non-DQE component) | Evaluate whether the vulnerable code is reachable from DQE. If shaded but unreachable, document and deprioritize. If reachable, treat as above. | Within 2 weeks |
| Planned version bump | Evaluate latest Trino stable. Diff Trino SPI changes against DQE usage. Re-shade. Run full conformance + regression + performance benchmark. | As needed, not on a fixed schedule |
| Breaking Trino SPI change | Evaluate impact scope. If isolated to components not used by DQE, skip. If it affects DQE internals, port the fix before upgrading. | Blocks the planned version bump |

### 23.4 Licensing and NOTICE

The release process includes:
1. Generate third-party dependency report from Gradle (`./gradlew :dqe-plugin:dependencies`).
2. Harvest licenses for all shaded artifacts.
3. Update `NOTICE` file in the DQE module with attributions.
4. Verify all dependencies are compatible with Apache 2.0 (Trino is Apache 2.0).

### 23.5 Release Checklist

- [ ] Update `trinoVersion` in `gradle.properties`.
- [ ] Regenerate shaded JAR (`./gradlew :dqe-plugin:shadowJar`).
- [ ] Run no-leak check.
- [ ] Run linkage smoke tests.
- [ ] Run semantic conformance suite (golden results + NULL/timezone/decimal tests).
- [ ] Run integration test suite.
- [ ] Generate third-party license report and update NOTICE.
- [ ] Update supported SQL subset documentation if new constructs are added.
- [ ] Performance benchmark against previous version (no regression > 10%).

---

## 24. Phased Implementation Plan

### Phase 1: Foundation — Scan, Filter, Project

**Semantic scope**: `SELECT <columns/expressions> FROM <index> WHERE <predicates> ORDER BY <columns> LIMIT <n>`

**Deliverables**:
- `dqe-parser`: Shaded Trino parser with no-leak validation.
- `dqe-types`: Type mapping contract (§9) with full test coverage.
- `dqe-metadata`: Index-to-table, field-to-column resolution from ClusterState.
- `dqe-execution`: `ShardScanOperator`, `FilterOperator`, `ProjectOperator`, `SortOperator`, `LimitOperator`, `TopNOperator`.
- `dqe-exchange`: Gather exchange (shard results → coordinator).
- `dqe-memory`: `DqeMemoryTracker` integrated with OpenSearch circuit breaker.
- `dqe-plugin`: Engine routing integration with `/_plugins/_sql`, transport actions, thread pools, settings.
- Observability: Phase 1 metrics (§21.1).
- Conformance: NULL, type coercion, and timezone test suites.
- Cancellation via `_tasks` API and query timeout.

**Exit criteria** (all must pass before Phase 2 begins):
1. **Unit tests**: `./gradlew :dqe-parser:test :dqe-types:test :dqe-metadata:test :dqe-analyzer:test :dqe-execution:test :dqe-exchange:test :dqe-memory:test :dqe-plugin:test` all green.
2. **Integration test suite**: `scripts/dqe-test/run-phase1-tests.sh` passes against a running cluster (`./gradlew :dqe-plugin:run`). Includes 118 scan/filter/project/sort/limit queries covering all type mappings from §9.1 and all Phase 1 expression constructs (CASE, CAST, TRY_CAST, COALESCE, NULLIF). Test cases are declarative JSON files (query + expected results) under `scripts/dqe-test/cases/phase1/`. Includes 15 error cases verifying rejection of Phase 2+ constructs (DISTINCT, SELECT-without-FROM, named functions, GROUP BY, JOIN, etc.).
3. **NULL conformance**: `scripts/dqe-test/run-null-tests.sh` passes. 52 NULL propagation test cases (§3.5).
4. **Type coercion conformance**: `scripts/dqe-test/run-type-coercion-tests.sh` passes. Implicit widening, explicit CAST/TRY_CAST, failure cases all passing.
5. **Timezone conformance**: `scripts/dqe-test/run-timezone-tests.sh` passes. Timestamp precision, comparison, ordering across DST boundaries. EXTRACT is Phase 2 (function library).
6. **Differential test**: `scripts/dqe-test/run-differential-tests.sh` passes. Phase 1 queries produce byte-identical results against standalone Trino (§3.6).
7. **Performance gate**: `scripts/dqe-test/run-performance-tests.sh` passes. On a 3-node cluster with 100M-doc index (5 primary shards), scan workload at max concurrency (10 queries) must not increase P99 search latency by more than 20% compared to baseline (measured by OpenSearch Rally with a defined mixed workload track).
8. **Memory safety gate**: `scripts/dqe-test/run-memory-tests.sh` passes. 10 concurrent scan queries, each scanning 1M rows, must not trip the DQE circuit breaker with default settings (20% heap, 256MB per query).
9. **Cancellation gate**: `scripts/dqe-test/run-cancellation-tests.sh` passes. Query cancelled via `_tasks` API releases all memory and PITs within 5s.

### Phase 2: Aggregations and Functions

**Semantic scope**: `GROUP BY`, `HAVING`, aggregate functions, scalar functions.

**Deliverables**:
- `dqe-functions`: ~100 core functions (§14.1).
- `dqe-analyzer`: Full type checking, function resolution, scope building.
- Aggregation pushdown to OpenSearch aggregations (Path A, §10.2) for simple patterns.
- Operator-based partial/final aggregation (Path B) for complex expressions.
- `_explain` output labels aggregation path.
- Observability: Phase 2 metrics (§21.2).
- Statistics gathering for CBO (lazy, cached).

**Exit criteria** (all must pass before Phase 3 begins):
1. **Integration test suite**: `scripts/dqe-test/run-phase2-tests.sh` passes. Includes GROUP BY with NULLs, empty groups, single-row groups, all aggregate functions from §14.1, and all scalar functions.
2. **Aggregation conformance**: Partial/final merge equivalence validated — partial aggregation on N shards merged at coordinator produces identical results to single-node aggregation.
3. **Pushdown verification**: `_explain` output confirms OpenSearch aggregation pushdown (Path A) for: single GROUP BY + COUNT, GROUP BY + SUM/AVG/MIN/MAX, GROUP BY + COUNT DISTINCT.
4. **Decimal precision**: Short decimal (p<=18) and long decimal (p>18) arithmetic passing.
5. **Differential test**: Phase 2 queries produce identical results against standalone Trino (§3.6).
6. **Performance gate**: On a 3-node cluster with 100M-doc index, `SELECT status, COUNT(*) FROM orders GROUP BY status` completes in <5s with <500MB peak memory. Path A (pushdown) must be >=10x faster than Path B (operator-based) for simple patterns.
7. **Phase 1 regression**: All Phase 1 exit criteria still pass (no regression).

### Phase 3: Distributed Execution and Joins

**Semantic scope**: `JOIN`, subqueries, CTEs.

**Deliverables**:
- `dqe-exchange`: Full exchange protocol (§12) — repartition, broadcast.
- `dqe-planner`: Distributed plan fragmentation, exchange placement.
- Join operators: `HashJoinBuildOperator`, `HashJoinProbeOperator`.
- Coordinator-side join and broadcast join (§13).
- Spill-to-disk for joins and large sorts.
- Observability: Phase 3 metrics (§21.3).

**Exit criteria** (all must pass before Phase 4 begins):
1. **Integration test suite**: `scripts/dqe-test/run-phase3-tests.sh` passes. Includes INNER/LEFT/RIGHT/CROSS JOIN, scalar subqueries, IN/EXISTS subqueries, CTEs, and join + aggregation combinations.
2. **Exchange correctness**: Repartition exchange with hash partitioning produces correct results for distributed GROUP BY across 5+ shards. Verified by comparing against gather-to-coordinator execution.
3. **Backpressure gate**: Broadcast join with 100MB build side and 1B-row probe side completes without OOM. Exchange buffer occupancy metrics confirm backpressure engaged during execution.
4. **Spill gate**: Hash join where build side exceeds per-query memory limit spills to disk and produces correct results. Spill files are cleaned up after query completion (verified by checking spill directory).
5. **Differential test**: Phase 3 queries produce identical results against standalone Trino (§3.6).
6. **Phase 1–2 regression**: All Phase 1 and Phase 2 exit criteria still pass.

### Phase 4: Optimization and Advanced Features

**Semantic scope**: Window functions, UNION/INTERSECT/EXCEPT, colocated join, hash-partitioned join.

**Deliverables**:
- Cost-based join reordering.
- Colocated join detection and execution.
- Hash-partitioned distributed join.
- Window function operators.
- Dynamic filter propagation (join build → probe-side scan).
- Full function library (~300+ functions).

**Exit criteria** (all must pass before Phase 5 begins):
1. **Integration test suite**: `scripts/dqe-test/run-phase4-tests.sh` passes. Includes all window functions from §14.2, UNION/INTERSECT/EXCEPT, and join strategy selection tests.
2. **TPC-H SF10 benchmark**: All 22 TPC-H queries execute correctly on a 3-node cluster with SF10 data (~60M rows in `lineitem`). Performance must be within 2x of standalone Trino with OpenSearch connector for scan-heavy queries, and faster for aggregation-heavy queries (Q1, Q6, Q13).
3. **Colocated join verification**: For two indices sharing `_routing` on the join key, `_explain` confirms zero-shuffle colocated join and execution produces correct results.
4. **Join reordering verification**: CBO-driven join reordering selects the same or better plan than manual ordering for a 3-table join with skewed cardinalities.
5. **Full conformance suite**: All 200+ golden result queries (§3.5) pass.
6. **Phase 1–3 regression**: All prior phase exit criteria still pass.

### Phase 5: Production Hardening

**Deliverables**:
- Async query support (`/_plugins/_sql/_async` endpoints).
- Per-user/role query quotas.
- Result set pagination (cursor-based).
- JDBC/ODBC driver compatibility (Trino wire protocol adapter or OpenSearch JDBC extension).
- Comprehensive documentation and migration guide.
- Security audit.

**Exit criteria** (GA readiness):
1. **Automated test suite**: All unit tests green and `scripts/dqe-test/run-all.sh` passes (all phases).
2. **Async query lifecycle**: Submit, poll, cancel, and timeout of async queries verified end-to-end.
3. **Security audit**: No findings of severity HIGH or above. All FLS/DLS scenarios tested with DQE engine.
4. **Phase 1–4 regression**: All prior phase exit criteria still pass.
5. **Documentation**: Supported SQL reference, configuration reference, and migration guide reviewed and published.

### Phase Regression Policy

Each phase's exit criteria form a permanent regression gate. On every PR that touches `dqe-*` modules, CI runs: (1) all unit tests via Gradle, and (2) all integration/conformance/differential test scripts via `scripts/dqe-test/run-all.sh` against a running cluster. If a later phase causes a prior phase's gate to fail, the later phase's changes are blocked until the regression is resolved.

---

## 25. Risks and Mitigations

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| R1 | Memory contention between DQE and search causes OOM or search degradation | HIGH | Dedicated circuit breaker (§15), conservative default (20% heap), operability gates per phase |
| R2 | Shaded JAR size bloats plugin (30–50MB) | MEDIUM | Accept as cost. Measure. Strip unused Trino modules. |
| R3 | Trino version drift introduces breaking changes | MEDIUM | Pin quarterly, cherry-pick security fixes, maintain conformance suite as upgrade gate |
| R4 | Exchange protocol throughput insufficient for large joins | HIGH | Start with coordinator-side join (no exchange needed), add exchange incrementally, reserve Netty-based protocol for future |
| R5 | Per-shard operator execution slower than expected (SearchHit → Page conversion overhead) | MEDIUM | Benchmark in Phase 1. Optimize with `docvalue_fields`, batch conversion, SIMD-friendly layouts. Fall back to aggregation pushdown for common patterns. |
| R6 | Partial aggregation correctness bugs (merge of partial states) | HIGH | Golden result suite, fuzz testing, differential testing against standalone Trino |
| R7 | Security bypass if search API is accidentally bypassed | CRITICAL | Code review policy: any class that accesses Lucene directly is blocked at review. Static analysis check. The search API is the only import allowed in scan operators. |
| R8 | Transport layer saturation during large shuffles | HIGH | Dedicated `dqe_exchange` pool, exchange backpressure, admission control |
| R9 | Schema change mid-query causes type errors | LOW | Schema snapshot at plan time (§8.2). Query fails with diagnostic if runtime type differs. |
| R10 | Plugin JAR class conflicts despite shading | MEDIUM | No-leak CI check, linkage smoke test, classpath conflict report (§23.2) |
| R11 | DQE/OpenSearch version incompatibility during rolling upgrades | HIGH | During a rolling upgrade, some nodes may have a newer DQE version while others have an older version (or no DQE). Exchange protocol messages and transport actions must be forward/backward compatible within one major version. The exchange protocol includes a version field; nodes reject messages from incompatible versions with a clear error. Queries in flight during a rolling upgrade may fail — this is acceptable if the error is explicit (`DQE_VERSION_MISMATCH`). |
