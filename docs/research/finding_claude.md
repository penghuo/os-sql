# Native Trino Integration into OpenSearch: Consolidated Research Findings

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Trino Core Architecture](#2-trino-core-architecture)
3. [Current Trino-OpenSearch Connector](#3-current-trino-opensearch-connector)
4. [Native Integration Architecture](#4-native-integration-architecture)
5. [Technology Challenges](#5-technology-challenges)
6. [Phased Implementation Plan](#6-phased-implementation-plan)
7. [Conclusions](#7-conclusions)

---

## 1. Executive Summary

This document consolidates research into natively integrating Trino's distributed SQL query engine components into OpenSearch. "Native integration" means embedding Trino's SQL parser, AST, optimizer, physical operators, function library, scheduler, and shuffle mechanisms directly inside the OpenSearch process as a plugin -- not running Trino as a separate cluster with a connector.

### Why Native Integration

The current Trino-OpenSearch connector has fundamental limitations:

- **No aggregation pushdown**: Every `GROUP BY` query fetches all rows over HTTP and aggregates in Trino. This is the single biggest performance gap.
- **JSON serialization overhead**: Data traverses 6 layers (Lucene segments -> JSON -> HTTP -> Java objects -> decoders -> Trino columnar blocks) even for local reads.
- **Separate cluster overhead**: Requires deploying, configuring, and operating a Trino cluster alongside OpenSearch.
- **Limited type support**: Custom date formats, geo types, vectors, and flattened fields are silently dropped.

Native integration can deliver **10-1000x improvement** for aggregation queries by translating SQL directly to OpenSearch's aggregation framework, and eliminate network serialization for local shard reads.

### Recommended Architecture

A new `/_plugins/_trino_sql` REST endpoint using a **distributed query execution (DQE)** model:

1. Parse SQL with Trino's ANTLR4 parser (shaded library).
2. Analyze and resolve against OpenSearch cluster metadata.
3. Optimize with Trino's rule-based + cost-based optimizer.
4. Fragment the optimized plan and **distribute plan fragments to each shard (split)**.
5. Each shard executes its plan fragment locally using the **local search API on that single shard** (preserving FLS/DLS security).
6. Intermediate results flow back via OpenSearch's transport layer for shuffle, join, and final aggregation.

**Key design principles**:
- The coordinator does NOT translate SQL to Query DSL and execute it. Instead, the coordinator distributes the query plan.
- Each shard is a first-class execution unit that runs physical operators against its local data via the search API.
- The local search API is always used (never direct Lucene segment access) to preserve Field-Level Security (FLS) and Document-Level Security (DLS).
- This is a completely new execution path -- it does NOT build on or reference the existing Calcite-based SQL/PPL implementation.

### Critical Challenges

| Challenge | Severity | Core Issue |
|---|---|---|
| Architecture mismatch | CRITICAL | Trino's MPP model vs OpenSearch's shard-based model |
| Shuffle problem | CRITICAL | No inter-node data streaming mechanism in OpenSearch |
| Memory management | HIGH | Two memory systems competing for one JVM heap |
| Distributed joins | HIGH | OpenSearch has no native distributed join capability |
| Classpath conflicts | HIGH | Overlapping dependencies at different versions |

---

## 2. Trino Core Architecture

### 2.1 Query Lifecycle

```
SQL Text
  -> [SQL Parser] -> AST (Statement tree)
    -> [Analyzer] -> Annotated AST (types, resolved references, scopes)
      -> [Logical Planner] -> Logical Plan (PlanNode tree)
        -> [Optimizer] -> Optimized Logical Plan (200+ rules, CBO)
          -> [Physical Planner / Fragmenter] -> Plan Fragments (stages)
            -> [Scheduler] -> Tasks distributed to workers
              -> [Execution Engine] -> Operators process Pages of columnar data
                -> [Exchange / Shuffle] -> Intermediate data between stages
                  -> Results returned to client
```

### 2.2 SQL Parser

Trino uses an ANTLR4 grammar (`SqlBase.g4`, ~3500 lines) with a two-phase parsing strategy:

1. **SLL mode (fast path)**: Attempts Strong LL prediction, significantly faster.
2. **LL mode (fallback)**: Full LL prediction for ambiguous grammars.

The parser lives in a standalone module (`trino-parser`) with minimal dependencies. Key entry points:

| Method | Purpose |
|---|---|
| `SqlParser.createStatement(sql)` | Parse a complete SQL statement |
| `SqlParser.createExpression(expr)` | Parse a standalone expression |

**AST hierarchy**: `Statement` -> `Query` -> `QuerySpecification` with `Select`, `From` (Relation types: Table, Join, Subquery, Unnest, Lateral, TableFunction), `Where`, `GroupBy`, `Having`, `OrderBy`, `Limit`. Expression types include literals, references, comparisons, logical operators, arithmetic, function calls, CASE, subqueries, window operations, and lambda expressions.

**SQL extensions beyond standard**: UNNEST, LATERAL, lambda expressions (`x -> x + 1`), MATCH_RECOGNIZE (row pattern matching), polymorphic table functions, JSON path functions, time travel (`FOR TIMESTAMP AS OF`).

**Embeddability: HIGH** -- standalone module, no dependency on execution engine or connectors.

### 2.3 Semantic Analysis (Analyzer)

The analyzer transforms raw AST into semantically validated, annotated representation. Two core classes dominate:

- `StatementAnalyzer` (~337KB) -- processes all statement types, resolves tables, validates SQL semantics.
- `ExpressionAnalyzer` (~198KB) -- type inference, function resolution, column binding.

The `Analysis` object stores: expression-to-type mappings, resolved column references, table/column handles from connectors, scope hierarchy, aggregation info, window specifications, and output schema.

**Key processes**:
- **Table resolution**: Catalog.schema.table names resolved via `ConnectorMetadata.getTableHandle()`.
- **Scope building**: Hierarchical scopes for FROM clauses, enabling correlated subqueries and lateral joins.
- **Type inference**: Bottom-up inference with implicit coercion (e.g., INTEGER + BIGINT -> BIGINT).
- **Function resolution**: `FunctionResolver` matches arguments to polymorphic signatures.

**Embeddability: MEDIUM** -- requires ConnectorMetadata (can be implemented for OpenSearch), function registry, and type system.

### 2.4 Query Optimizer

Trino uses a hybrid optimization approach:

**Iterative rule-based optimizer** with 200+ pattern-matching rules organized by category:

| Category | Example Rules | Effect |
|---|---|---|
| Predicate pushdown | `PushPredicateIntoTableScan`, `PushFilterThroughJoin` | Filters closer to data |
| Projection pruning | `PruneTableScanColumns`, `PruneJoinColumns` | Remove unused columns |
| Join optimization | `ReorderJoins` (CBO), `TransformCorrelatedSubqueries` | Reorder and transform joins |
| Aggregation pushdown | `PushPartialAggregationThroughExchange`, `PushAggregationIntoTableScan` | Aggregations closer to data |
| Limit pushdown | `PushLimitThroughJoin`, `PushTopNIntoTableScan` | Limits closer to data |
| Connector pushdown | `PushFilterIntoTableScan`, `PushAggregationIntoTableScan` | Delegate to connectors |

**Cost-based optimization (CBO)**: Statistics model (`PlanNodeStatsEstimate`, `SymbolStatsEstimate`, `PlanCostEstimate`), stats rules per plan node type, cost calculators using weighted CPU/memory/network costs, and join enumeration for multi-way joins.

**Plan-level optimizations**: `PredicatePushDown` (~82KB), `AddExchanges` (~87KB), `AddLocalExchanges` (~55KB).

**PlanNode hierarchy** (40+ node types): TableScanNode, FilterNode, ProjectNode, JoinNode (INNER/LEFT/RIGHT/FULL/CROSS), AggregationNode (SINGLE/PARTIAL/INTERMEDIATE/FINAL), SortNode, TopNNode, LimitNode, WindowNode, ExchangeNode (GATHER/REPARTITION/REPLICATE/LOCAL), UnionNode, and more.

**Embeddability: MEDIUM** -- modular rules can be selectively included. Exchange placement is coupled to distributed execution and needs replacement.

### 2.5 Physical Operators and Execution Engine

Trino uses a **pipelined, pull-based model** with columnar data processing (168 operator files).

**Core data model**:
- `Page`: Batch of rows as array of `Block` objects (one per column), typically ~1MB.
- `Block`: Columnar array -- IntArrayBlock, LongArrayBlock, VariableWidthBlock, DictionaryBlock, RunLengthEncodedBlock, LazyBlock, ArrayBlock, MapBlock, RowBlock.

**Operator interface** (pull model):
```java
public interface Operator {
    boolean needsInput();
    void addInput(Page page);
    Page getOutput();
    void finish();
    boolean isFinished();
}
```

**Key operators**:

| Category | Operators |
|---|---|
| Scan | `TableScanOperator`, `ScanFilterAndProjectOperator`, `ExchangeOperator` |
| Join | `HashBuilderOperator`, `LookupJoinOperator`, `HashSemiJoinOperator` |
| Aggregation | `HashAggregationOperator`, `StreamingAggregationOperator` |
| Sort/TopN | `OrderByOperator`, `TopNOperator`, `LimitOperator`, `MergeOperator` |
| Window | `WindowOperator`, `TopNRankingOperator`, `RowNumberOperator` |

**Execution hierarchy**: Query -> Stage -> Task -> Pipeline -> Driver -> Operator chain. Drivers are single-threaded execution units; multiple drivers run in parallel within a pipeline.

**Embeddability: LOW-MEDIUM** -- operators depend on deep context hierarchy (OperatorContext -> DriverContext -> PipelineContext -> TaskContext -> memory management). Individual operators are relatively self-contained; the Page/Block data model is independent and reusable.

### 2.6 Type System and Functions

**Type system** (defined in `trino-spi`, highly embeddable):

| SQL Type | Java Carrier | Notes |
|---|---|---|
| BOOLEAN | boolean | |
| TINYINT/SMALLINT/INTEGER/BIGINT | long | |
| REAL | long (int bits) | |
| DOUBLE | double | |
| DECIMAL(p,s) | long or Int128 | Short (p<=18) uses long |
| VARCHAR(n) / CHAR(n) | Slice | UTF-8 |
| DATE | long | Days since epoch |
| TIMESTAMP(p) | long or LongTimestamp | Microseconds since epoch |
| ARRAY(T) / MAP(K,V) / ROW(...) | Block | Nested types |

**Functions**: 500+ functions across scalar, aggregate, window, and table categories. Registered via annotations (`@ScalarFunction`, `@AggregationFunction`). Polymorphic resolution with type coercion.

### 2.7 Scheduler and Exchange

**Stage-based execution**: Plans are fragmented at exchange boundaries into stages (source, fixed, single, coordinator-only). Stages contain PlanFragments that run on one or more workers.

**Split model**: Connectors enumerate `Split` objects (data chunks). The scheduler assigns splits to worker tasks using locality-aware, round-robin, or random strategies.

**Exchange types**: GATHER (all to one), REPARTITION (hash-partitioned), REPLICATE (broadcast), LOCAL (intra-node). HTTP-based inter-node transfer with OutputBuffer/DirectExchangeClient. Custom binary serialization for columnar Pages.

**Embeddability: LOW** -- tightly coupled to coordinator-worker model and HTTP exchange. Must be replaced entirely for OpenSearch integration.

### 2.8 Memory Management

Hierarchical tracking: MemoryPool -> QueryContext -> TaskContext -> PipelineContext -> DriverContext -> OperatorContext. Each level tracks user memory and revocable memory (reclaimable by spilling to disk). Spill-capable operators: HashBuilderOperator, HashAggregationOperator, OrderByOperator. Low memory killer strategies select which query to kill under pressure.

### 2.9 Connector SPI

The SPI (`trino-spi`, 119 interface files) defines the engine-connector contract:

- `ConnectorMetadata` (~8000 lines): Schema discovery, table/column access, statistics, and **pushdown protocol** (applyFilter, applyProjection, applyAggregation, applyLimit, applyTopN, applyJoin).
- `ConnectorSplitManager`: Enumerates data partitions for reading.
- `ConnectorPageSourceProvider`: Creates page sources that produce `Page` objects.
- Handle objects (opaque to engine): `ConnectorTableHandle`, `ColumnHandle`, `ConnectorSplit`.
- `DynamicFilter`: Runtime filter propagation from join build side to probe-side table scan.

**Embeddability: HIGH** -- clean interface separation, standard integration point.

### 2.10 Embeddability Summary

| Component | Embeddability | Key Challenge |
|---|---|---|
| SQL Parser | HIGH | Standalone module, minimal dependencies |
| Type System | HIGH | In trino-spi, reusable independently |
| Connector SPI | HIGH | Clean interface, standard integration point |
| Analyzer | MEDIUM | Needs metadata/function resolution |
| Optimizer (rules) | MEDIUM | Modular rules; exchange placement is distributed |
| Optimizer (CBO) | MEDIUM | Needs statistics from connectors |
| Execution Engine | LOW-MEDIUM | Deep context hierarchy, distributed assumptions |
| Scheduler | LOW | Tightly coupled to coordinator-worker model |
| Exchange | LOW | HTTP-based distributed data transfer |

---

## 3. Current Trino-OpenSearch Connector

### 3.1 Architecture

The connector (`trino-elasticsearch` / `trino-opensearch`) uses Trino's standard SPI plugin model:

```
OpenSearchPlugin -> OpenSearchConnectorFactory -> OpenSearchConnector
  -> ElasticsearchMetadata (schema, type mapping, pushdown)
  -> ElasticsearchSplitManager (shard-to-split mapping)
  -> ElasticsearchPageSourceProvider (3 page source variants)
  -> ElasticsearchClient (REST client with scroll API)
  -> ElasticsearchQueryBuilder (predicate -> DSL translation)
```

The connector is **read-only** with `READ_COMMITTED` isolation. OpenSearch indices map to Trino tables under a single schema ("default"). The OpenSearch connector is a thin wrapper reusing all Elasticsearch connector code.

### 3.2 Predicate Pushdown

Pushdown-eligible types: boolean, numeric, timestamp, keyword VARCHAR only.

| Predicate | Translation |
|---|---|
| Equality (`=`) | `TermQuery` |
| Range (`<`, `>`, `<=`, `>=`) | `RangeQuery` |
| IS NULL | `BoolQuery.mustNot(ExistsQuery)` |
| IS NOT NULL | `ExistsQuery` |
| IN list | Multiple `TermQuery` with `should` |
| LIKE on keyword | `RegexpQuery` |

**Not supported**: text fields, IP addresses, complex types, scaled_float, arbitrary expressions, function calls, OR across different columns.

### 3.3 What Cannot Be Pushed Down

| Operation | Status |
|---|---|
| JOINs | Not pushable -- all data flows through Trino |
| Aggregations (COUNT, SUM, AVG, GROUP BY) | **Not pushable** -- full table scan required |
| Window functions | Not pushable |
| Complex expressions (CASE, arithmetic) | Not pushable |
| ORDER BY | Not pushable (except `_doc`) |
| DISTINCT | Not pushable |
| Full-text search scoring | Limited (table name syntax hack) |

**The absence of aggregation pushdown is the single biggest performance gap.** OpenSearch's native aggregation framework (terms, date_histogram, stats, cardinality, percentiles) is entirely unused.

### 3.4 Data Retrieval

Uses the **deprecated Scroll API** (not search_after or PIT). One split per shard. Sort by `_doc` for efficiency. Default scroll size: 1000 docs.

**Serialization path** (6 layers):
```
Lucene segments -> doc values / stored fields
  -> JSON _source reconstruction
    -> HTTP response
      -> Java SearchHit deserialization
        -> Trino Decoder per-field type conversion
          -> Trino Block/Page columnar format
```

Three page source variants: `ScanQueryPageSource` (full scroll), `CountQueryPageSource` (COUNT(*) via _count API), `PassthroughQueryPageSource` (raw DSL).

### 3.5 Type Mapping Gaps

| OpenSearch Type | Trino Mapping | Issue |
|---|---|---|
| `date` (custom format) | **UNSUPPORTED** | Silently dropped |
| `geo_point` / `geo_shape` | **UNSUPPORTED** | Invisible to Trino |
| `dense_vector` / `sparse_vector` | **UNSUPPORTED** | k-NN vectors invisible |
| `date_nanos` | **UNSUPPORTED** | Nanosecond timestamps dropped |
| `flattened` | **UNSUPPORTED** | |
| Arrays | Requires manual `_meta.trino` annotation | Error-prone |

### 3.6 Gap Analysis: Connector vs Native

**Operations that benefit most from native integration**:

| Operation | Connector Cost | Native Benefit | Estimated Improvement |
|---|---|---|---|
| Aggregations (GROUP BY) | Full scan over HTTP | OpenSearch aggregation framework | 10-1000x |
| TopN (ORDER BY + LIMIT) | Full scan + Trino sort | Single search with sort + size | 10-100x |
| Filtered aggregations | Full scan, filter + agg in Trino | Push filter + aggregation together | 10-1000x |
| COUNT DISTINCT | Full scan + hash | `cardinality` aggregation (HLL) | 100-1000x |
| Date histograms | Full scan + GROUP BY | `date_histogram` aggregation | 10-1000x |
| Percentiles | Full scan + sort | `percentiles` aggregation (t-digest) | 100-1000x |
| Geo queries | Unsupported | Native geo_distance, geo_shape | N/A -> supported |
| Full-text search | Limited table name hack | Native match, multi_match | N/A -> full support |

**Where the connector is sufficient**: Simple filtered scans with small result sets, low-volume analytics (<1M docs), cross-data-source federation (Trino's core value), ad-hoc exploration, schema discovery.

---

## 4. Native Integration Architecture

### 4.1 REST API Design

New endpoints completely separate from existing `_sql` and `_ppl`:

| Endpoint | Method | Description |
|---|---|---|
| `/_plugins/_trino_sql` | POST | Execute Trino SQL query |
| `/_plugins/_trino_sql/_explain` | POST | Return query plan without executing |
| `/_plugins/_trino_sql/_async` | POST | Submit long-running query, return query ID |
| `/_plugins/_trino_sql/_async/{queryId}` | GET | Poll async query status/results |
| `/_plugins/_trino_sql/_async/{queryId}` | DELETE | Cancel async query |

**Request format**:
```json
{
  "query": "SELECT customer_id, COUNT(*) FROM orders WHERE status = 'shipped' GROUP BY customer_id ORDER BY COUNT(*) DESC LIMIT 100",
  "catalog": "opensearch",
  "schema": "default",
  "session_properties": {"query_max_memory": "256MB"},
  "fetch_size": 1000
}
```

**Response format**: Columnar JSON with schema metadata, query stats (state, timing, rows/bytes processed), and optional pagination via `next_uri`.

REST handlers follow the same pattern as existing `RestPPLQueryAction` -- parse request, wrap in transport request, dispatch via `nodeClient.execute()`. Registered in `SQLPlugin.java` alongside existing handlers. Gated by `plugins.trino_sql.enabled` cluster setting.

### 4.2 SQL Parser Integration

**Strategy**: Shade Trino's `trino-parser` JAR into the OpenSearch plugin via Gradle Shadow plugin. Relocate to `org.opensearch.sql.trino.shaded.io.trino` to avoid ANTLR4 conflicts with existing PPL/SQL grammars.

```java
public class TrinoSqlParser {
    private final SqlParser parser = new SqlParser();

    public Statement parse(String sql) {
        return parser.createStatement(sql, options);
    }
}
```

Reuse Trino's AST node hierarchy directly (Statement, Expression, Relation types). For OpenSearch-specific syntax (MATCH() functions, nested field access), use UDF-based approach initially (pre-process into standard SQL + UDFs before parsing).

### 4.3 Metadata Layer

**Catalog-schema-table mapping**:

| Trino Concept | OpenSearch Concept |
|---|---|
| Catalog | OpenSearch cluster |
| Schema | Index pattern namespace / alias group |
| Table | Index or index alias |
| Column | Field in mapping |

**Type mapping** (expanded from connector):

| OpenSearch Type | Trino Type |
|---|---|
| `keyword` | `VARCHAR` |
| `text` | `VARCHAR` |
| `long` / `integer` / `short` / `byte` | `BIGINT` / `INTEGER` / `SMALLINT` / `TINYINT` |
| `double` / `float` / `half_float` | `DOUBLE` / `REAL` |
| `boolean` | `BOOLEAN` |
| `date` (all formats) | `TIMESTAMP` |
| `geo_point` | `ROW(lat DOUBLE, lon DOUBLE)` |
| `nested` | `ARRAY(ROW(...))` |
| `object` | `ROW(...)` |
| `flattened` | `MAP(VARCHAR, VARCHAR)` |

**Statistics for CBO**: Index-level stats from `_stats` API (cheap), sampled cardinality via `sampler` aggregation for join keys and filter columns (cached with configurable TTL, default 5 minutes), lazy evaluation (only when optimizer requests).

### 4.4 Query Planning Pipeline

```
SQL String
  -> [TrinoSqlParser] Parse to AST
    -> [TrinoAnalyzer] Resolve tables/columns/types against OpenSearch metadata
      -> [Logical Plan] Trino PlanNode tree
        -> [TrinoOptimizer] Rule-based + cost-based optimization
          |-- PredicatePushdownRule
          |-- ProjectionPruningRule
          |-- AggregationOptimizationRule
          |-- LimitPushdownRule
          |-- JoinReorderingRule (CBO)
          |-- BroadcastJoinRule / ColocatedJoinRule
        -> [Optimized Logical Plan]
          -> [TrinoPhysicalPlanner] Convert to physical operators
            -> [TrinoDistributedPlanner] Fragment plan into per-shard stages
              -> [Distribute] Send plan fragments to each shard (split)
                -> [Per-Shard Execution] Each shard executes operators via local search API
                  -> [Exchange/Shuffle] Intermediate results between nodes via transport
                    -> [Final Aggregation] Coordinator merges results
```

**Key principle**: The coordinator does NOT translate SQL to Query DSL. Instead, the optimizer produces a physical plan that is **fragmented and distributed to each shard**. Each shard executes its plan fragment as a pipeline of physical operators (scan, filter, project, partial aggregate, etc.) using the **local search API on that single shard**. This preserves FLS/DLS security and leverages OpenSearch's existing shard-level search infrastructure.

**Per-shard operator execution**:

Each shard receives a plan fragment containing physical operators. The shard-local scan operator uses the search API targeting only that specific shard (`_shards:N|_local` preference). The operator pipeline runs locally:

```
[ShardScanOperator] -- reads from local shard via search API (respects FLS/DLS)
  -> [FilterOperator] -- evaluates remaining predicates not handled by search API
    -> [ProjectOperator] -- computes expressions, prunes columns
      -> [PartialAggregateOperator] -- computes partial aggregations locally
        -> [Exchange] -- sends partial results to coordinator or next stage
```

The search API on each shard can accelerate the scan via Query DSL (term, range, bool queries) as an optimization within the ShardScanOperator, but the **plan distribution to shards is the primary execution model**, not DSL pushdown at the coordinator.

**Operations executed per-shard**: scan, filter, project, partial aggregation, partial sort/TopN.

**Operations executed at coordinator or via shuffle**: final aggregation merge, joins (broadcast/hash/colocated), window functions, final sort, limit, HAVING with complex predicates.

### 4.5 Execution Engine on OpenSearch Nodes

**Node role mapping**:

| Role | Trino Analog | OpenSearch Node | Responsibility |
|---|---|---|---|
| Coordinator | Trino Coordinator | Coordinating node | Parse, analyze, plan, optimize, schedule |
| Worker | Trino Worker | Data node | Execute operators against local shards |

The node receiving the REST request becomes the coordinator (same pattern as OpenSearch search requests).

**Split-to-shard mapping**: Each primary shard becomes one split. Shard routing from `ClusterState` determines which data node holds each split. Prefer local shards.

**Per-shard data access**:

Each shard executes its plan fragment locally using the **OpenSearch search API on that single shard**. The ShardScanOperator builds a `SearchRequest` targeting the specific shard (`_shards:N|_local` preference) and iterates `SearchResponse.getHits()` to produce columnar Pages.

This approach is **mandatory, not optional** -- direct Lucene segment access (bypassing the search API) is never used because it would bypass Field-Level Security (FLS) and Document-Level Security (DLS). The search API is the only supported data access path.

**Thread pools**: Dedicated `trino-sql-worker` and `trino-sql-exchange` thread pools to avoid starving search workloads.

### 4.6 Shuffle/Exchange via Transport Layer

Replace Trino's HTTP-based exchange with OpenSearch's internal `TransportService`:

**New transport actions**:
```
internal:sql/trino/exchange/send    -- send data pages between nodes
internal:sql/trino/exchange/fetch   -- request data pages
internal:sql/trino/exchange/close   -- signal completion
internal:sql/trino/stage/execute    -- execute a stage on a data node
```

**Data format**: Columnar `TrinoDataPage` implementing `Writeable` (OpenSearch's serialization interface). One block per column, binary encoding.

**Exchange patterns**:
- **Hash exchange** (distributed joins/GROUP BY): Partition rows by `hash(join_key) % N`, send each partition to assigned node.
- **Broadcast exchange** (small table joins): Replicate to all worker nodes.
- **Gather exchange** (final results): All workers send to coordinator.

**Flow control**: Bounded in-memory exchange buffers (default 32MB per channel), integrated with OpenSearch's circuit breakers. Producers pause when buffer is full.

### 4.7 Distributed Join Execution

Three join strategies:

**Broadcast join** (small build side < 100MB):
- Replicate small table to all data nodes.
- Each node builds local hash table and probes against local shards.
- No repartitioning of the large table.

**Hash-partitioned join** (two large tables):
- Partition both sides by join key hash across N buckets.
- Each node processes one bucket.
- Requires shuffle infrastructure.

**Colocated join** (OpenSearch-specific optimization):
- If both indices use the same routing key as the join key, execute locally on each data node.
- Detected by checking `_routing` field configuration.
- Zero shuffle required.

### 4.8 Function Library

500+ functions executed as physical operators in the per-shard or coordinator-level execution pipeline:

**Scalar functions** (evaluated per-row in FilterOperator/ProjectOperator on each shard): comparison operators, arithmetic, string functions (SUBSTRING, CONCAT, REPLACE, TRIM, REGEXP_LIKE), math functions (ABS, CEIL, FLOOR, ROUND, SQRT, LOG), date/time functions (DATE_TRUNC, DATE_ADD, EXTRACT), JSON functions (JSON_EXTRACT, JSON_QUERY), LOWER/UPPER, COALESCE, CAST, conditional (CASE, IF, NULLIF).

**Aggregate functions** (partial execution per-shard, final merge at coordinator): COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT, APPROX_DISTINCT, ARRAY_AGG, BOOL_AND, BOOL_OR.

**Window functions** (executed at coordinator after shuffle/gather): ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE.

**Array/Map functions**: ARRAY_JOIN, ELEMENT_AT, CARDINALITY, CONTAINS, FLATTEN, MAP_KEYS, MAP_VALUES.

### 4.9 Resource Management

- **Circuit breaker**: Register `trino_sql` breaker with configurable limit (default 60% heap). All Trino operator allocations tracked through breaker.
- **Per-query memory limits**: Default 256MB per query, configurable via session property.
- **Query timeout/cancellation**: Following `OpenSearchQueryManager` pattern. Configurable timeout (default 5 minutes). Cancellation propagates to all stages on all nodes.
- **Concurrency control**: Semaphore-based admission control (default 10 concurrent queries per node).
- **Spill-to-disk**: For memory-intensive operations (large sorts, hash joins). Spill to OpenSearch data directory. Configurable max disk usage (default 10GB).

### 4.10 Module Structure

```
trino-engine/
  trino-parser/          Trino SQL parser (shaded)
  trino-types/           Trino type system + OpenSearch type mapping
  trino-metadata/        Catalog/schema/table metadata from cluster state
  trino-analyzer/        Semantic analysis (type inference, scope resolution)
  trino-planner/         Optimizer + physical planner + pushdown rules
  trino-execution/       Physical operators, memory management, spill
  trino-functions/       Function library (500+ functions)
  trino-transport/       Shuffle/exchange over OpenSearch transport layer
  trino-plugin/          REST API, plugin wiring, settings
  trino-integ-test/      Integration tests
```

**Dependency graph**: trino-plugin -> trino-transport -> trino-execution -> trino-planner -> trino-analyzer -> trino-metadata -> trino-types; trino-parser standalone; trino-functions used by planner and execution.

All Trino dependencies shaded into a single shadow JAR to avoid classpath conflicts with OpenSearch.

### 4.11 Configuration Settings

| Setting | Default | Description |
|---|---|---|
| `plugins.trino_sql.enabled` | `true` | Enable/disable the Trino SQL endpoint |
| `plugins.trino_sql.max_concurrent_queries` | `10` | Max concurrent queries per node |
| `plugins.trino_sql.query_timeout` | `5m` | Query execution timeout |
| `plugins.trino_sql.memory.circuit_breaker.limit` | `60%` | Max heap for Trino SQL operations |
| `plugins.trino_sql.memory.query_max_memory` | `256MB` | Max memory per individual query |
| `plugins.trino_sql.exchange.buffer_size` | `32MB` | Buffer size per exchange channel |
| `plugins.trino_sql.spill.enabled` | `true` | Enable spill-to-disk |
| `plugins.trino_sql.spill.path` | `<data_dir>/trino-spill` | Spill directory |
| `plugins.trino_sql.spill.max_disk_bytes` | `10GB` | Max disk usage for spill |
| `plugins.trino_sql.join.broadcast_threshold` | `100MB` | Max table size for broadcast join |
| `plugins.trino_sql.optimizer.join_reordering` | `AUTOMATIC` | Join reordering strategy |
| `plugins.trino_sql.statistics.cache_ttl` | `5m` | Statistics cache duration |

### 4.12 End-to-End Query Flow Example

**Query**: `SELECT name, COUNT(*) FROM orders WHERE status = 'shipped' GROUP BY name ORDER BY COUNT(*) DESC LIMIT 10`

1. **REST Layer**: Receive POST, parse JSON, create transport request.
2. **Parse**: Trino parser produces `Query` AST with `QuerySpecification`.
3. **Analyze**: Resolve `orders` -> `OpenSearchTrinoTableHandle("orders")`, resolve columns, type-check `status = 'shipped'` (VARCHAR = VARCHAR).
4. **Logical Plan**: `Limit(10) -> Sort(count DESC) -> Aggregate(groupBy=[name], aggs=[count(*)]) -> Filter(status='shipped') -> TableScan(orders)`.
5. **Optimize**: Standard Trino optimization: push filter below aggregate, push partial aggregation below exchange, add exchange nodes. Produce optimized plan with partial/final aggregation split.
6. **Physical Plan**:
   - **Per-shard fragment**: `ShardScanOperator(filter=status='shipped', columns=[name]) -> PartialHashAggregateOperator(groupBy=[name], aggs=[count(*)])`
   - **Coordinator fragment**: `GatherExchange -> FinalHashAggregateOperator -> SortOperator(count DESC) -> LimitOperator(10)`
7. **Distribute**: Coordinator looks up shard routing from ClusterState. `orders` has 5 primary shards across 3 data nodes. Coordinator sends the per-shard plan fragment to each data node via `TransportTrinoStageAction`.
8. **Per-shard execution**: Each shard executes its fragment:
   - `ShardScanOperator` issues a search API call targeting only its shard (`_shards:2|_local`), with `status='shipped'` as a query filter and `name` as the only fetched field. FLS/DLS is enforced by the search API.
   - `PartialHashAggregateOperator` builds a local hash table grouping by `name`, counting rows.
   - Partial results (name -> partial_count) are sent back to the coordinator via gather exchange.
9. **Coordinator merge**: `FinalHashAggregateOperator` merges partial counts from all 5 shards. `SortOperator` sorts by count DESC. `LimitOperator` takes top 10.
10. **Return**: JSON response with columns `[name, count]` and 10 result rows.

### 4.13 Security Integration

- All `/_plugins/_trino_sql` endpoints protected by OpenSearch Security plugin.
- Table-level access control: metadata layer checks index-level permissions before resolving tables.
- **Field-level security (FLS) and Document-level security (DLS) are enforced automatically** because every shard reads data exclusively through the local search API. The search API applies FLS field filtering and DLS query injection transparently. This is why direct Lucene access is never used.
- Cross-index join security: user must have read access to ALL indices in the query. The analyzer validates permissions before planning.
- Audit logging: query text and execution metadata logged to security audit log.

---

## 5. Technology Challenges

### 5.1 Challenge 1: Architecture Mismatch (CRITICAL)

**Problem**: Trino assumes a shared-nothing MPP with dedicated coordinator + stateless workers. OpenSearch uses shard-based distributed search where data nodes serve dual roles (storage + search). Trino expects to control the compute cluster; OpenSearch data nodes are already running search, indexing, segment merges, and cluster management.

**Mapping difficulties**:
- Trino Stage -> No direct OpenSearch equivalent (phases are request-scoped, not pipeline-scoped).
- Trino Task -> Closest is shard-level operation, but tasks are compute-centric vs data-centric.
- Trino Pipeline/Driver -> No equivalent. OpenSearch does not chain operators within a shard operation.

**Recommended approach**: Distributed query execution (DQE) model. The coordinator uses Trino's planner for optimization and plan generation, then **distributes plan fragments to each shard (split)**. Each shard executes its fragment as a pipeline of physical operators, reading data via the local search API on that single shard. This is a completely new execution path that does NOT build on or reference the existing Calcite-based implementation.

The key mapping: **Trino Split = OpenSearch Shard**. Each shard is a first-class execution unit. Plan fragments are sent to data nodes and executed against specific shards. Intermediate results flow back via OpenSearch's transport layer for shuffle, join, and final aggregation at the coordinator.

**Scaling strategy**: Introduce dedicated `analytics` node role for workloads requiring distributed joins or large-scale aggregations. These nodes participate in shuffle and join execution without holding shards.

### 5.2 Challenge 2: The Shuffle Problem (CRITICAL)

**Problem**: Distributed query processing requires streaming intermediate data between nodes. Trino uses HTTP-based exchange; OpenSearch has no equivalent. Shuffling is needed for distributed hash joins, repartitioning for aggregation, distributed sort, and union/intersection of intermediate results.

OpenSearch's `TransportService` is designed for request-response patterns, not GB-scale data streaming. Messages are fully materialized in memory before sending.

**Solution options with tradeoffs**:

| Approach | Throughput | Latency | Complexity | Recommended For |
|---|---|---|---|---|
| Transport layer exchange | MBs | Low | Medium | Moderate shuffles |
| Temporary indices | Low | High (seconds) | Low | Not recommended |
| Custom Netty protocol | GBs | Lowest | Very high | Future optimization |
| Single-node execution | N/A | N/A | Lowest | Initial implementation |

**Recommended approach**: Start with the gather-at-coordinator pattern: distribute plan fragments to shards, each shard executes locally via search API, partial results are gathered at the coordinator for final merge. This covers scan + filter + partial aggregate per shard with coordinator-side final aggregation. Add transport-layer exchange with message chunking for distributed joins and repartitioning. Reserve custom protocol for future optimization if throughput proves insufficient.

### 5.3 Challenge 3: Memory Management Conflict (HIGH)

**Problem**: Two memory management systems competing for one JVM heap. Trino has memory pools with per-operator tracking and spill-to-disk. OpenSearch has circuit breakers (parent, field data, request, in-flight). If Trino operators allocate memory that OpenSearch's circuit breakers don't know about, OOM or spurious breaker trips result.

**Recommended approach**: Start with dedicated heap partitioning (e.g., Trino gets 20% of heap via a `trino_sql` circuit breaker, OpenSearch keeps 80%). Evolve toward integrated tracking where Trino's memory allocations register with OpenSearch's parent circuit breaker. Avoid off-heap approach due to complexity of rewriting Trino's memory model.

### 5.4 Challenge 4: Distributed Join Execution (HIGH)

**Problem**: OpenSearch has no native distributed join. Implementing distributed joins requires solving the shuffle problem first and managing memory for hash tables on data nodes that already serve search workloads.

**Recommended approach** (incremental):
1. **Coordinator-side join**: Pull both sides to coordinator, join locally. Works for small-to-medium tables (<1GB combined).
2. **Broadcast join**: For small build side (<100MB), replicate to all nodes. Good for star-schema patterns (large fact table + small dimensions).
3. **Hash-partitioned join**: Full distributed join via shuffle. Deferred until shuffle infrastructure matures.
4. **Colocated join** (OpenSearch-specific): If both indices share routing key as join key, execute locally with zero shuffle.

**Semi-join optimization**: Use OpenSearch's `_terms` lookup query -- first query build side for distinct join keys, then filter probe side with those keys.

### 5.5 Challenge 5: Dependency / Classpath Hell (HIGH)

**Problem**: Overlapping libraries at different versions:

| Library | Conflict Risk |
|---|---|
| Guava | Medium -- Trino uses Airlift's Guava fork |
| Jackson | High -- serialization/deserialization breakage |
| Netty | High -- native library conflicts |
| Guice | High -- binding conflicts |
| SLF4J / Log4j | Medium -- logging framework confusion |

**Recommended approach**: Shade all Trino dependencies via Gradle Shadow plugin (precedent: `async-query-core/build.gradle` already uses `shadowJar`). Relocate packages: `io.airlift` -> `opensearch.shaded.io.airlift`, `com.google.common` -> `opensearch.shaded.com.google.common`, etc. Accept maintenance burden of re-shading on Trino version upgrades.

### 5.6 Challenge 6: Consistent Reads (MEDIUM)

**Problem**: Trino expects snapshot isolation. OpenSearch provides near-real-time search where documents can appear/disappear during long queries.

**Recommended approach**: Use PIT (Point-in-Time) per table, building on existing infrastructure (`OpenSearchRequestBuilder.createPit()`). For multi-table queries, create a PIT for each table at query start. Accept that cross-table snapshot isolation is not guaranteed (acceptable for most analytical workloads). Document the near-real-time semantics.

### 5.7 Challenge 7: Resource Isolation (MEDIUM)

**Problem**: Analytical queries (large scans, hash joins, multi-stage aggregations) can starve search and indexing workloads on shared data nodes.

**Recommended approach**:
- Dedicated `trino-sql-worker` thread pool with configurable size.
- Per-query limits: memory cap, time limit, row limit.
- Concurrency control via semaphore (default 10 concurrent queries).
- For production mixed workloads, evaluate dedicated `analytics` node role.

### 5.8 Challenge 8: Schema Evolution (MEDIUM)

**Problem**: OpenSearch dynamic mappings can change during query execution. Type conflicts across indices in index patterns.

**Recommended approach**: Snapshot schema at plan time by reading index mappings from cluster state. Use the most general type for conflicts across indices in an index pattern. Do not require strict schema mode.

### 5.9 Challenge 9: Shard-Level Operator Execution (MEDIUM)

**Problem**: Trino operators work on columnar Pages/Blocks. OpenSearch stores data in shards backed by Lucene segments. The ShardScanOperator must efficiently bridge the search API response format (SearchHits) into Trino's columnar Page/Block format for downstream operators.

**Recommended approach**: Always use the local search API on each shard to read data. The ShardScanOperator issues search requests targeting a specific shard, iterates SearchHits, and builds columnar Pages. The search API naturally enforces FLS/DLS and handles segment lifecycle. Direct Lucene segment access is never used -- it would bypass security. Optimization focus should be on efficient batching (large scroll/search_after pages), column pruning via `_source` filtering, and using `docvalue_fields` for sorted/numeric access where the search API supports it.

### 5.10 Challenge 10: Testing and Correctness (MEDIUM)

**Problem**: Ensuring embedded engine produces identical results to standalone Trino across NULL handling, type coercion, overflow, timezone, string comparison, and floating point edge cases.

**Recommended approach**: Golden result files first (rapid coverage). Then port relevant subset of Trino's ~10,000 query tests. Add property-based fuzzing as continuous integration tool.

### 5.11 Challenge 11: Upgrade and Maintenance (LOW-MEDIUM)

**Problem**: Trino releases biweekly, OpenSearch quarterly. Maintaining synchronization and avoiding vendor-fork risk.

**Recommended approach**: Pin to a specific Trino version, update quarterly aligned with OpenSearch releases. Minimize dependence on Trino internals (prefer SPI). Cherry-pick critical security fixes between updates.

### 5.12 Challenge Summary and Implementation Order

| # | Challenge | Severity | Solve When |
|---|---|---|---|
| 5 | Classpath conflicts | HIGH | First -- must solve to load any Trino code |
| 1 | Architecture mismatch | CRITICAL | Second -- foundational design decision |
| 3 | Memory management | HIGH | Third -- establish heap partitioning before running operators |
| 9 | Lucene operator bridge | MEDIUM | Fourth -- build scan operator bridge |
| 6 | Consistent reads | MEDIUM | Fifth -- extend PIT for multi-table queries |
| 7 | Resource isolation | MEDIUM | Sixth -- thread pools and query limits |
| 4 | Distributed joins (coordinator) | HIGH | Seventh -- single-node joins |
| 4 | Distributed joins (broadcast) | HIGH | Eighth -- first distributed join |
| 2 | Shuffle infrastructure | CRITICAL | Ninth -- transport-layer exchange |
| 4 | Distributed joins (hash) | HIGH | Tenth -- full distributed joins |
| 10 | Testing | MEDIUM | Continuous from day one |
| 11 | Upgrade strategy | LOW-MEDIUM | After initial stabilization |
| 8 | Schema evolution | MEDIUM | As edge cases surface |

---

## 6. Phased Implementation Plan

### Phase 1: Foundation

**Goal**: Simple `SELECT * FROM index WHERE ...` queries working end-to-end with per-shard execution.

- `trino-parser`: Shaded parser with basic validation.
- `trino-metadata`: Index-to-table mapping, field type mapping from cluster state.
- `trino-plugin`: REST endpoint `/_plugins/_trino_sql`.
- **Per-shard plan distribution**: Coordinator distributes scan plan fragments to each shard via transport actions. Each shard executes ShardScanOperator using local search API (respects FLS/DLS).
- Filter and projection operators in per-shard pipeline.
- Gather exchange: partial results from shards collected at coordinator.

### Phase 2: Aggregations and Functions

**Goal**: Analytical queries with per-shard partial aggregation.

- `trino-functions`: Core function library (~100 most common functions).
- **Per-shard partial aggregation**: PartialHashAggregateOperator runs on each shard, FinalHashAggregateOperator merges at coordinator.
- Per-shard TopN: partial sort + limit per shard, final merge-sort at coordinator.
- `trino-analyzer`: Full type checking and coercion.
- `_explain` endpoint showing distributed plan with per-shard fragments.

### Phase 3: Distributed Execution

**Goal**: Multi-node execution for joins and large aggregations.

- `trino-transport`: Exchange mechanism over OpenSearch transport layer.
- `trino-execution`: Physical operators (hash join, sort, aggregate, window).
- `trino-planner`: Distributed planning with stage fragmentation.
- Coordinator-side joins and broadcast joins.
- Dedicated thread pools.

### Phase 4: Optimization

**Goal**: Production-grade query performance.

- Cost-based optimization with statistics gathering.
- Join reordering.
- Colocated join detection.
- Hash-partitioned distributed joins.
- Spill-to-disk for large operations.
- Dynamic filters (runtime predicate propagation from join build to probe).
- Performance benchmarking and tuning.

### Phase 5: Production Hardening

**Goal**: Enterprise readiness.

- Async query support (`_async` endpoints).
- Security integration (FLS, DLS, audit logging).
- Monitoring and metrics (query stats, memory usage, exchange throughput).
- Circuit breaker tuning and stress testing.
- Documentation and migration guide from connector approach.

---

## 7. Conclusions

### 7.1 Feasibility Assessment

Native integration of Trino into OpenSearch is **technically feasible but architecturally challenging**. The two critical blockers are the architecture mismatch (MPP vs shard-based) and the shuffle problem (no inter-node data streaming). Both are solvable through the distributed query execution (DQE) model: distribute plan fragments to each shard for local execution via the search API, gather partial results, and perform final merge/join operations at the coordinator or via transport-layer exchange.

### 7.2 Expected Benefits

| Capability | Current (Connector) | Native Integration |
|---|---|---|
| Aggregation queries | Full table scan over HTTP | Per-shard partial aggregation, coordinator merge, 10-1000x faster |
| Data access | JSON over remote REST API | Local search API on each shard, no network hop for scan |
| Operational overhead | Separate Trino cluster | In-process, zero additional infra |
| Type support | Limited (drops custom dates, geo, vectors) | Full OpenSearch type coverage |
| Security | Separate config | Unified with OpenSearch Security |
| Join performance | Both sides over network | Colocated/broadcast joins, data locality |

### 7.3 Key Design Decisions

1. **Distributed plan execution to shards** -- the coordinator distributes plan fragments to each shard; each shard is a first-class execution unit. This is NOT DSL pushdown at the coordinator.
2. **Search API always, never direct Lucene** -- the local search API on each shard is the only data access path, preserving FLS/DLS security. Direct Lucene segment access is never used.
3. **Completely independent from existing Calcite implementation** -- the new DQE is a separate execution path; no dependency on existing SQL/PPL modules.
4. **Shading over classpath isolation** -- proven technique, already precedented in the codebase.
5. **Incremental join strategies** -- coordinator-side -> broadcast -> hash-partitioned -> colocated.
6. **PIT per table** for consistency -- extends existing infrastructure, good enough for analytical workloads.

### 7.4 Risks to Monitor

- **Plugin JAR size**: Expected 30-50MB with shaded dependencies. Acceptable but notable.
- **Memory contention**: The biggest operational risk. Requires careful circuit breaker tuning per deployment.
- **Trino version drift**: Quarterly updates with potential breaking API changes. Budget for ongoing maintenance.
- **Operator correctness**: NULL handling, type semantics, and per-shard partial aggregation merge must produce results identical to standalone Trino. Requires comprehensive test suite.
- **Transport layer saturation**: Large shuffles could compete with cluster management traffic. Dedicated channels or rate limiting may be needed.
