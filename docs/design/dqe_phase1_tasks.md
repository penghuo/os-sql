# DQE Phase 1: Implementation Task Breakdown

**Status**: Draft
**Date**: 2026-02-22
**Scope**: Phase 1 — Scan, Filter, Project, Sort, Limit
**Reference**: `docs/design/dqe_design.md`

---

## Build Infrastructure (from task #9)

### BI-1: Set up Gradle multi-project structure for DQE modules
Create the directory layout and `build.gradle` files for all DQE subprojects (`dqe-parser`, `dqe-types`, `dqe-metadata`, `dqe-analyzer`, `dqe-execution`, `dqe-exchange`, `dqe-memory`, `dqe-plugin`, `dqe-integ-test`). Register each subproject in `settings.gradle`. Configure inter-module dependencies matching the dependency graph from design doc Section 5. Set `trinoVersion=479` in `gradle.properties`.

- **Key files**: `settings.gradle`, `gradle.properties`, `dqe/dqe-*/build.gradle`
- **Dependencies**: None (first task)
- **Complexity**: M

### BI-2: Configure Shadow plugin for Trino 479 shading
Configure the Gradle Shadow plugin in `dqe-plugin/build.gradle` to shade all Trino and transitive dependencies. Set up package relocation rules: `io.trino` -> `org.opensearch.dqe.shaded.io.trino`, `io.airlift` -> `org.opensearch.dqe.shaded.io.airlift`, `com.google.common` -> `org.opensearch.dqe.shaded.com.google.common`, `com.fasterxml.jackson` -> `org.opensearch.dqe.shaded.com.fasterxml.jackson`, `org.antlr` -> `org.opensearch.dqe.shaded.org.antlr`. Verify the shaded JAR builds successfully and contains relocated classes. Reference: design doc Section 23.1.

- **Key files**: `dqe/dqe-plugin/build.gradle`
- **Dependencies**: BI-1
- **Complexity**: M

### BI-3: Implement no-leak CI validation
Create a Gradle task that scans the shaded JAR to verify no unshaded `io.trino`, `io.airlift`, or `com.google.common` classes remain after relocation. The task should fail the build if any unshaded classes are found. Add this task as a dependency of the `check` lifecycle task so it runs in CI. Reference: design doc Section 23.2, item 1.

- **Key files**: `dqe/dqe-plugin/build.gradle` (custom task), or `buildSrc/` helper
- **Dependencies**: BI-2
- **Complexity**: S

### BI-4: Implement classpath conflict check
Create a Gradle task that detects duplicate classes between the DQE shaded JAR and OpenSearch core/plugin dependencies. The task produces a report of conflicting class names and fails if any conflicts are found. This prevents runtime `ClassCastException` or method resolution errors. Reference: design doc Section 23.2, item 3.

- **Key files**: `dqe/dqe-plugin/build.gradle` (custom task)
- **Dependencies**: BI-2
- **Complexity**: M

### BI-5: Implement dependency boundary enforcement (ArchUnit/Gradle constraint)
Add a Gradle dependency constraint or ArchUnit architecture test that validates no `dqe-*` module transitively depends on disallowed packages: `org.opensearch.sql.sql.*`, `org.opensearch.sql.ppl.*`, `org.opensearch.sql.calcite.*`, `org.opensearch.sql.expression.*`, `org.opensearch.sql.planner.*`, `org.opensearch.sql.executor.*`, `org.opensearch.sql.storage.*`. This check must run in CI and block merges on violation. Reference: design doc Section 5, "Dependency Boundary".

- **Key files**: `dqe/dqe-integ-test/src/test/java/.../DqeDependencyBoundaryTest.java` or Gradle constraint in root `build.gradle`
- **Dependencies**: BI-1
- **Complexity**: M

### BI-6: Set up test dataset and REST API test infrastructure
Create the test infrastructure under `scripts/dqe-test/`. Define index mappings (`data/mappings/*.json`), bulk data files (`data/bulk/*.ndjson`), and scale data generators (`data/generators/`). Cover all OpenSearch field types from Section 9.1 (keyword, text, long, integer, double, boolean, date, date_nanos, geo_point, nested, flattened, dense_vector, ip, binary, unsigned_long, scaled_float). Include edge cases: NULL-heavy columns, empty strings, multi-valued fields, nested arrays, and custom date formats. Create `setup-data.sh` that bulk-indexes datasets via the REST API. Create the test case JSON format, `validate.py` script for comparing REST responses against expected results, and the test runner scripts (`run-phase1-tests.sh`, `run-null-tests.sh`, `run-type-coercion-tests.sh`, `run-timezone-tests.sh`, `run-all.sh`). Add `./gradlew :dqe-plugin:run` task (modeled on `./gradlew opensearch-sql:run`) to start a local cluster. Reference: Phase 1 Test Plan §2, §4.

- **Key files**: `scripts/dqe-test/`, `scripts/dqe-test/validate.py`, `scripts/dqe-test/setup-data.sh`, `scripts/dqe-test/run-phase1-tests.sh`, `dqe/dqe-plugin/build.gradle` (run task)
- **Dependencies**: BI-1
- **Complexity**: L

### BI-7: Set up differential testing against standalone Trino
Create the differential test infrastructure: Trino Docker setup with OpenSearch connector config (`scripts/dqe-test/trino/opensearch.properties`), `wait-for-trino.sh`, `trino-poll.sh` (for Trino async statement protocol), `diff-results.py` (compare DQE vs Trino results with type mapping and float tolerance), and `run-differential-tests.sh`. Differential test cases are JSON files under `scripts/dqe-test/cases/differential/` with a `differential: true` flag. Reference: design doc Section 3.6 and Phase 1 Test Plan §8.

- **Key files**: `scripts/dqe-test/run-differential-tests.sh`, `scripts/dqe-test/diff-results.py`, `scripts/dqe-test/trino/`
- **Dependencies**: BI-1, BI-6
- **Complexity**: L

### BI-8: Generate third-party license report and NOTICE file
Create a Gradle task that generates the third-party dependency report for all shaded artifacts and harvests their licenses. Generate the `NOTICE` file in the DQE module with attributions. Verify all dependencies are compatible with Apache 2.0. This task should run as part of the release checklist. Reference: design doc Section 23.4.

- **Key files**: `dqe/NOTICE`, `dqe/dqe-plugin/build.gradle` (license task)
- **Dependencies**: BI-2
- **Complexity**: S

---

## dqe-parser Module (from task #2)

### P-1: Create DqeSqlParser wrapper class
Implement `DqeSqlParser` that wraps Trino's `SqlParser` with appropriate options (decimal literal treatment, etc.). The wrapper must accept a SQL string and return a Trino `Statement` AST. Configure parser options as specified in design doc Section 7. The class should live in `org.opensearch.dqe.parser` package and use the shaded Trino parser internally.

- **Key classes**: `DqeSqlParser`
- **Dependencies**: BI-1, BI-2
- **Complexity**: S

### P-2: Create DqeParsingException
Implement `DqeParsingException` that wraps Trino's `ParsingException` with line number and column number information. This exception should extend a common `DqeException` base class (shared across DQE modules) and provide structured error fields (`message`, `lineNumber`, `columnNumber`) for the REST error response. Reference: design doc Section 7.

- **Key classes**: `DqeParsingException`, `DqeException` (base)
- **Dependencies**: BI-1
- **Complexity**: S

### P-3: Create DqeException hierarchy base classes
Define the common exception hierarchy used across all DQE modules: `DqeException` (base), `DqeParsingException`, `DqeUnsupportedOperationException` (for rejecting unsupported SQL constructs at analysis time), `DqeTypeMismatchException`, `DqeAccessDeniedException`. Each exception carries a stable `errorCode` string for the error catalog. Reference: design doc Section 3.3.

- **Key classes**: `DqeException`, `DqeErrorCode` (enum), `DqeUnsupportedOperationException`, `DqeTypeMismatchException`
- **Dependencies**: BI-1
- **Complexity**: S

### P-4: Parser unit tests
Write unit tests for `DqeSqlParser` covering: valid SELECT/FROM/WHERE/ORDER BY/LIMIT queries, invalid syntax (error messages with line/column), expressions (arithmetic, comparisons, boolean logic, CAST, IS NULL, BETWEEN, IN, LIKE), and unsupported constructs that parse successfully but will be rejected later (GROUP BY, JOIN, etc., to confirm the parser accepts full Trino grammar). Minimum 30 test cases.

- **Key files**: `dqe/dqe-parser/src/test/java/.../DqeSqlParserTest.java`
- **Dependencies**: P-1, P-2
- **Complexity**: M

### P-5: Linkage smoke test for shaded parser
Create an integration test that loads the shaded parser JAR, instantiates `DqeSqlParser`, parses a representative SQL query, and verifies no `ClassNotFoundException` or `NoSuchMethodError` occurs. This test validates that all shading relocations are correct and complete. Reference: design doc Section 23.2, item 2.

- **Key files**: `dqe/dqe-parser/src/test/java/.../ParserLinkageTest.java`
- **Dependencies**: P-1, BI-2
- **Complexity**: S

---

## dqe-types Module (from task #3)

### T-1: Define DqeType enum and type representation
Create the `DqeType` class hierarchy that represents DQE's internal type system, wrapping Trino's `Type` interface. Define the enum of supported types aligned with design doc Section 9.1: `VARCHAR`, `BIGINT`, `INTEGER`, `SMALLINT`, `TINYINT`, `DOUBLE`, `REAL`, `DECIMAL`, `BOOLEAN`, `TIMESTAMP(3)`, `TIMESTAMP(9)`, `VARBINARY`, `ROW`, `ARRAY`, `MAP`. Include parameterized types (precision, scale, field descriptors).

- **Key classes**: `DqeType`, `DqeTypes` (static factory methods)
- **Dependencies**: BI-1
- **Complexity**: M

### T-2: Implement OpenSearch-to-DQE type mapping
Create `OpenSearchTypeMappingResolver` that converts OpenSearch field mapping types to DQE types following the mapping table in design doc Section 9.1. Handle all 22 OpenSearch types: keyword, text, long, integer, short, byte, double, float, half_float, scaled_float, boolean, date, date_nanos, ip, geo_point, geo_shape, nested, object, flattened, dense_vector, binary, unsigned_long. Handle `scaled_float` -> `DECIMAL(p,s)` derivation from `scaling_factor`.

- **Key classes**: `OpenSearchTypeMappingResolver`
- **Dependencies**: T-1
- **Complexity**: M

### T-3: Implement multi-field handling for text/keyword
Create logic in the type mapper to detect `text` fields with `keyword` sub-fields. For equality (`=`), `IN`, and sorting operations, the system must transparently select the `.keyword` sub-field for pushdown. For full-text predicates, use the `text` field. Track whether a field has `fielddata` enabled to determine sortability. Reference: design doc Section 9.2.

- **Key classes**: `MultiFieldResolver`, additions to `OpenSearchTypeMappingResolver`
- **Dependencies**: T-2
- **Complexity**: M

### T-4: Implement type coercion and widening rules
Define implicit type coercion rules (e.g., `INTEGER + BIGINT` -> `BIGINT`, `REAL + DOUBLE` -> `DOUBLE`). Implement explicit `CAST` type conversion matrix. Define which conversions are allowed implicitly, which require explicit CAST, and which always fail. When a field has conflicting types across indices in an index pattern, widen to the most general compatible type. Reference: design doc Section 8.2 and Section 3.5.

- **Key classes**: `DqeTypeCoercion`, `TypeWidening`
- **Dependencies**: T-1
- **Complexity**: M

### T-5: Implement date format parsing for custom date fields
Create `DateFormatResolver` that reads the `format` property from OpenSearch date field mappings and constructs the appropriate Java `DateTimeFormatter`. Support common OpenSearch date formats (epoch_millis, epoch_second, strict_date_optional_time, custom patterns). Map all date fields to `TIMESTAMP(3)` and `date_nanos` to `TIMESTAMP(9)`. Reference: design doc Section 9.1.

- **Key classes**: `DateFormatResolver`
- **Dependencies**: T-2
- **Complexity**: M

### T-6: Implement array detection strategy
Create `ArrayDetectionStrategy` with three modes as described in design doc Section 9.3: (1) explicit `_meta.dqe.arrays` annotation, (2) document sampling (configurable, default 100 docs), (3) fallback to scalar treatment. The strategy should return a set of field paths that are multi-valued. Expose configuration for the detection mode and sample size.

- **Key classes**: `ArrayDetectionStrategy`, `ArrayDetector`
- **Dependencies**: T-2
- **Complexity**: M

### T-7: Implement Page/Block data format converters
Create converters that transform OpenSearch `SearchHit` data into Trino's columnar `Page`/`Block` format. Implement converters for each DQE type: `LongArrayBlock` for integer types, `VariableWidthBlock` for string types, `ByteArrayBlock` for boolean, etc. Handle NULL values correctly (null positions array). Target batch size: configurable (default 1000 docs per page). Reference: design doc Section 11.4.

- **Key classes**: `SearchHitToPageConverter`, `BlockBuilder` per type
- **Dependencies**: T-1, T-2
- **Complexity**: L

### T-8: Type mapping unit tests
Write comprehensive unit tests for the type mapping layer. Cover all 22 OpenSearch types, multi-field resolution, type coercion/widening, date format parsing, array detection, and Page/Block conversion. Include edge cases: NULL values, empty strings, extremely large numbers, multi-valued fields, nested objects, custom date formats. Minimum 50 test cases.

- **Key files**: `dqe/dqe-types/src/test/java/.../`
- **Dependencies**: T-1 through T-7
- **Complexity**: M

---

## dqe-metadata Module (from task #4)

### M-1: Define DqeTableHandle and DqeColumnHandle
Create the metadata handle classes that represent resolved tables and columns within the DQE. `DqeTableHandle` holds the index name, index pattern (if applicable), schema snapshot generation, and optional PIT ID. `DqeColumnHandle` holds the field name, field path (for nested), DQE type, sortability flag, and sub-field info. These are the core data carriers passed between analyzer, planner, and execution.

- **Key classes**: `DqeTableHandle`, `DqeColumnHandle`
- **Dependencies**: T-1
- **Complexity**: S

### M-2: Implement DqeMetadata.getTableHandle
Implement table resolution from `ClusterState`. Given a schema and table name, resolve to an OpenSearch index or index alias. Check that the index exists and is open. For index patterns (wildcards), resolve to the set of matching indices. Return a `DqeTableHandle` or throw an appropriate exception if the index does not exist.

- **Key classes**: `DqeMetadata` (partial)
- **Dependencies**: M-1
- **Complexity**: M

### M-3: Implement DqeMetadata.getColumnHandles
Implement column resolution from index mappings in `ClusterState`. Given a `DqeTableHandle`, extract all fields from the index mapping, apply `OpenSearchTypeMappingResolver` to convert each field to a `DqeColumnHandle`, handle nested/object field flattening (dot notation), resolve multi-field information, and detect type conflicts across indices in a pattern. Reference: design doc Section 8.2.

- **Key classes**: `DqeMetadata` (partial)
- **Dependencies**: M-1, T-2, T-3
- **Complexity**: M

### M-4: Implement DqeMetadata.getSplits (shard routing)
Implement shard split resolution from `ClusterState`. Given a `DqeTableHandle`, produce exactly one `DqeShardSplit` per logical shard. Each split represents one shard copy (primary or replica) selected for execution.

**Split contract (invariant)**: For an index with N primary shards, `getSplits()` returns exactly N splits. Each split targets exactly one shard copy — either the primary or one replica, never both. The scheduler selects the copy using adaptive selection: prefer local shards (same node as coordinator), then least-loaded copies. Replica selection is for locality and load balancing, not for parallel double-reading. Violating this invariant (producing >N splits, or scanning both primary and replica for the same shard) causes duplicate rows in query results.

In Phase 1, the implementation selects primaries by default. If a primary is unavailable (relocating, unassigned), the split targets a started replica on another node. If no copy is available for a shard, the query fails with `SHARD_NOT_AVAILABLE`.

`DqeShardSplit` contains: logical shard ID, selected node ID, index name, whether the selected copy is primary or replica. The logical shard ID is the deduplication key — the scheduler asserts that no two splits share the same logical shard ID. Reference: design doc Section 4 and Section 11.

- **Key classes**: `DqeShardSplit`, `DqeMetadata` (partial), `ShardSelector`
- **Dependencies**: M-1
- **Complexity**: M

### M-5: Implement schema snapshot and caching
Ensure that at query analysis time, the index mapping is read from `ClusterState` and frozen for the lifetime of the query. Implement a lightweight cache for the resolved metadata (table handle + column handles) keyed by index name and mapping version. Cache TTL should be short (query lifetime) but avoid redundant re-resolution for the same query.

- **Key classes**: `SchemaSnapshot`, additions to `DqeMetadata`
- **Dependencies**: M-2, M-3
- **Complexity**: S

### M-6: Implement basic table statistics from _stats API
Implement `DqeMetadata.getStatistics` to gather row count and index size from the `_stats` API. These statistics are used by the optimizer for cost estimation. Cache statistics with a configurable TTL (default 5 minutes). Include generation-number-based cache invalidation when doc count changes by >10%. For Phase 1, only row count and index size are needed; column-level statistics are deferred to Phase 2. Reference: design doc Section 8.3.

- **Key classes**: `DqeTableStatistics`, `StatisticsCache`
- **Dependencies**: M-2
- **Complexity**: M

### M-7: Metadata unit tests
Write unit tests for the metadata module covering: table resolution (existing index, missing index, alias, index pattern), column resolution (all type mappings, nested fields, multi-fields, type conflicts), shard split enumeration, schema snapshot freezing, and statistics caching. Use mock `ClusterState` objects. Minimum 30 test cases.

- **Key files**: `dqe/dqe-metadata/src/test/java/.../`
- **Dependencies**: M-1 through M-6
- **Complexity**: M

---

## dqe-analyzer Module (from task #10)

### A-1: Create DqeAnalyzer entry point
Create the `DqeAnalyzer` class that accepts a Trino `Statement` AST (from `DqeSqlParser`) and `DqeMetadata`, performs semantic analysis, and produces an analyzed plan (resolved tables, columns, types). The analyzer walks the AST using a visitor pattern, resolving identifiers against metadata and checking types. For Phase 1, it must handle `QuerySpecification` with SELECT, FROM (single table), WHERE, ORDER BY, and LIMIT. Reference: design doc Section 10.

- **Key classes**: `DqeAnalyzer`, `AnalysisContext`
- **Dependencies**: P-1, M-2, M-3, T-1
- **Complexity**: M

### A-2: Implement scope resolution and identifier binding
Create `Scope` and `ScopeResolver` classes that resolve unqualified and qualified column references against the table's column handles. Handle column aliases defined in SELECT, table aliases in FROM, and ambiguous column detection (error when a column name exists in multiple scopes). This is the core name-resolution logic.

- **Key classes**: `Scope`, `ScopeResolver`, `ResolvedField`
- **Dependencies**: A-1, M-3
- **Complexity**: M

### A-3: Implement expression type checking
Create `ExpressionTypeChecker` that walks expression trees and assigns/verifies types. Handle:
- Arithmetic expressions (type promotion): `+`, `-`, `*`, `/`, `%`, unary `-`
- Comparison expressions (type compatibility): `=`, `!=`, `<`, `>`, `<=`, `>=`
- Boolean logic (AND/OR/NOT require BOOLEAN)
- CAST and TRY_CAST expressions (validate target type, TRY_CAST returns NULL on failure)
- IS NULL / IS NOT NULL
- BETWEEN, IN, LIKE
- CASE expressions: searched CASE (`CASE WHEN cond THEN val ... END`) and simple CASE (`CASE expr WHEN val THEN result ... END`). Validate all THEN/ELSE branches have compatible types.
- COALESCE(expr, expr, ...): validate all arguments have compatible types, result type is common supertype
- NULLIF(expr, expr): validate both arguments have compatible types, result type is first argument's type

Report type errors with column name, expected type, and actual type. Reference: design doc Section 3.3.

**Phase 1 expression scope**: The expression evaluator supports the SQL expression constructs listed above. It does NOT support function calls from the function library (scalar functions, temporal functions like EXTRACT, string functions like CONCAT/LENGTH, etc.). Function library support is Phase 2 (dqe-functions module). Specifically, the following are Phase 2: `IF()`, `TYPEOF()`, `EXTRACT()`, `CONCAT()`, `LENGTH()`, `SUBSTR()`, and all other named functions. SELECT-without-FROM (e.g., `SELECT 1 + 2`) is also Phase 2 — Phase 1 requires a FROM clause with a single table.

- **Key classes**: `ExpressionTypeChecker`, `TypedExpression`
- **Dependencies**: A-1, T-4
- **Complexity**: L

### A-4: Implement unsupported construct rejection
Create the validation pass that detects SQL constructs not supported in Phase 1 and rejects them with `DqeUnsupportedOperationException`. Rejected constructs: GROUP BY, HAVING, aggregate functions (COUNT, SUM, AVG, etc.), JOIN, subqueries, CTEs, UNION/INTERSECT/EXCEPT, window functions, MATCH_RECOGNIZE, table functions, DISTINCT, SELECT-without-FROM, and all named function calls not in the Phase 1 expression construct list (see A-3). Each rejection must name the specific unsupported construct. Reference: design doc Section 3.2 and 3.3.

- **Key classes**: `UnsupportedConstructValidator` (visitor)
- **Dependencies**: A-1, P-3
- **Complexity**: M

### A-5: Implement predicate analysis for pushdown
Analyze WHERE clause predicates to determine which can be pushed down to OpenSearch Query DSL and which must remain as post-filter operators. Classify predicates: term equality (`=`), range (`<`, `>`, `<=`, `>=`, `BETWEEN`), boolean (`AND`, `OR`, `NOT`), existence (`IS NULL`, `IS NOT NULL`), pattern (`LIKE`), set membership (`IN`). Produce a `PushdownPredicate` structure that the execution layer can convert to Query DSL. Reference: design doc Section 10.1, `PushFilterToSearchQuery`.

- **Key classes**: `PredicateAnalyzer`, `PushdownPredicate`, `PushdownClassifier`
- **Dependencies**: A-3
- **Complexity**: L

### A-6: Implement projection analysis and column pruning
Analyze the SELECT clause to determine which columns are needed from the source table. Compute the minimal set of columns required by SELECT expressions, WHERE predicates, and ORDER BY clauses. Produce a `RequiredColumns` structure that the scan operator uses for `fetchSource` field filtering. This implements `PruneTableScanColumns` from design doc Section 10.1.

- **Key classes**: `ProjectionAnalyzer`, `RequiredColumns`
- **Dependencies**: A-2, A-3
- **Complexity**: M

### A-7: Implement ORDER BY and LIMIT analysis + operator selection rule
Analyze ORDER BY clauses: validate that referenced columns/expressions are sortable (check DQE type sortability from Section 9.1), resolve column references and aliases, detect and reject sorting on non-sortable types (e.g., `text` without fielddata, `geo_point`, `nested`). Analyze LIMIT/OFFSET: validate integer literal values. Produce ordering specification for the planner.

**Operator selection rule (Phase 1)**:

| Query Pattern | Shard-local Pipeline | Coordinator Pipeline |
|---|---|---|
| `ORDER BY` without `LIMIT` | Scan → (Filter) → (Project) | Gather → SortOperator (full sort) → output |
| `ORDER BY col LIMIT N` | Scan → (Filter) → (Project) → **TopNOperator(N)** | Gather → **TopNOperator(N)** → output |
| `ORDER BY col LIMIT N OFFSET M` | Scan → (Filter) → (Project) → **TopNOperator(N+M)** | Gather → **TopNOperator(N+M)** → LimitOperator(skip M, take N) → output |
| `LIMIT N` without `ORDER BY` | Scan → (Filter) → (Project) → LimitOperator(N) | Gather → LimitOperator(N) → output |

**Key correctness property of TopNOperator**: The operator maintains a bounded priority queue of size N (or N+M). It must see **all** input rows before producing output — it cannot emit rows early or assume rows arrive in any order. A "take first N rows seen" implementation is incorrect and will produce wrong results when the actual top-N rows are scattered across later batches or shards.

**Shard-local TopN**: When ORDER BY + LIMIT is present, each shard pipeline includes a TopN operator that prunes the shard's output to at most N rows (or N+M) in sorted order. This reduces the data volume flowing through the gather exchange from (shards × full_scan_size) to (shards × N). The coordinator then applies a final TopN merge over all shards' pre-sorted outputs.

- **Key classes**: `OrderByAnalyzer`, `SortSpecification`, `OperatorSelectionRule`
- **Dependencies**: A-2, A-3
- **Complexity**: M

### A-8: Implement index-level permission check
Before resolving tables, check that the current user has read access to the referenced index. Integrate with OpenSearch Security plugin's permission framework. If the user lacks access, fail analysis with `DqeAccessDeniedException` before any execution occurs. Reference: design doc Section 20.2.

- **Key classes**: additions to `DqeAnalyzer`, `SecurityContext`
- **Dependencies**: A-1
- **Complexity**: M

### A-9: Analyzer unit tests
Write comprehensive unit tests for the analyzer module covering: successful analysis of SELECT/FROM/WHERE/ORDER BY/LIMIT queries, identifier resolution (qualified, unqualified, aliases), type checking (all expression types, coercion, errors), unsupported construct rejection (all Phase 2-4 constructs), predicate classification, column pruning, ORDER BY validation, and permission denial. Use mock metadata. Minimum 50 test cases.

- **Key files**: `dqe/dqe-analyzer/src/test/java/.../`
- **Dependencies**: A-1 through A-8
- **Complexity**: L

---

## dqe-execution Module (from task #5)

### E-1: Define Operator interface and OperatorContext
Create the base `Operator` interface that all physical operators implement. Define the pull-based iteration model: `getOutput()` returns a `Page` or null (not ready), `isFinished()` signals completion, `close()` releases resources. Create `OperatorContext` which provides access to the memory tracker, query ID, stage ID, and interrupt flag for cancellation checking. Reference: design doc Section 11.1.

- **Key classes**: `Operator`, `OperatorContext`, `OperatorFactory`
- **Dependencies**: BI-1, T-7
- **Complexity**: M

### E-2: Implement ShardScanOperator
Implement the shard-level scan operator that reads data from OpenSearch via the search API. Build `SearchRequest` targeting a specific shard using `_shards:<id>|_local` preference. Implement batch iteration using `search_after` (not scroll). Apply pushed-down Query DSL predicates and column pruning (`fetchSource`). Convert `SearchHit` batches to `Page` objects. Handle PIT lifecycle (create, use, release). Reference: design doc Section 11.2.

- **Key classes**: `ShardScanOperator`, `SearchRequestBuilder`
- **Dependencies**: E-1, T-7, M-4
- **Complexity**: L

### E-3: Implement predicate-to-QueryDSL converter
Create `PredicateToQueryDslConverter` that converts the `PushdownPredicate` structures from the analyzer into OpenSearch Query DSL. Support conversions: equality -> `term` query, range -> `range` query, AND -> `bool.must`, OR -> `bool.should`, NOT -> `bool.must_not`, IS NULL -> `bool.must_not.exists`, IS NOT NULL -> `exists`, IN -> `terms` query, LIKE -> `wildcard` query (simple patterns) or `regexp` query. Handle multi-field resolution (use `.keyword` sub-field for term/terms queries on text fields).

- **Key classes**: `PredicateToQueryDslConverter`
- **Dependencies**: A-5, T-3
- **Complexity**: L

### E-4: Implement FilterOperator
Implement the post-scan filter operator for predicates that cannot be pushed down to Query DSL (e.g., complex expressions, cross-column comparisons, function calls). The operator evaluates predicate expressions against each row in a `Page` and produces a filtered `Page` with only matching rows. Implement expression evaluation for comparison, arithmetic, boolean, and CAST operations.

- **Key classes**: `FilterOperator`, `ExpressionEvaluator`
- **Dependencies**: E-1, A-3
- **Complexity**: M

### E-5: Implement ProjectOperator
Implement the projection operator that computes output expressions and produces a `Page` with only the requested columns. Handle column references (pass-through), arithmetic expressions, CAST/TRY_CAST expressions, CASE expressions, COALESCE, NULLIF, and NULL handling. Reuse `ExpressionEvaluator` from E-4. String concatenation and named function calls are Phase 2 (dqe-functions).

- **Key classes**: `ProjectOperator`
- **Dependencies**: E-1, E-4
- **Complexity**: M

### E-6: Implement SortOperator
Implement the in-memory sort operator for `ORDER BY` without `LIMIT`. The operator has two phases:
1. **Accumulation phase**: Accept all input pages and buffer all rows in memory. The operator is NOT finished and produces no output during this phase.
2. **Output phase**: Once the upstream operator signals `isFinished()`, sort the accumulated rows by the specified columns and directions (ASC/DESC, NULLS FIRST/LAST), then produce sorted pages on output.

The operator MUST accumulate all rows before sorting. Producing output before all input is consumed is incorrect — it can emit rows that would not be in the correct position after seeing later input. Track memory via `OperatorContext.reserveMemory()`. For Phase 1, sorting is in-memory only (no spill); fail with `EXCEEDED_QUERY_MEMORY_LIMIT` if data exceeds budget. Reference: design doc Section 11.3.

Used only when: `ORDER BY` is present AND `LIMIT` is absent (see A-7 operator selection rule).

- **Key classes**: `SortOperator`
- **Dependencies**: E-1
- **Complexity**: M

### E-7: Implement TopNOperator
Implement the TopN operator for `ORDER BY col LIMIT N`. Maintain a bounded min/max heap (priority queue) of size N. For each input row, compare against the current Nth element in the heap — if the new row should replace it, insert the new row and evict the Nth. The operator MUST process all input rows before producing output, because the actual top-N rows may appear in any order across input batches.

**Correctness invariant**: After consuming all input, the priority queue contains exactly the N rows that would appear first in a full sort of the entire input. A "take first N rows seen" implementation is incorrect and will produce wrong results when top-N rows are scattered across later batches.

Support multi-column ordering with ASC/DESC and NULLS FIRST/LAST positioning. When used with OFFSET M, the heap size is N+M (see A-7 operator selection rule).

Used at both shard-local (prune per-shard output to N rows) and coordinator (merge all shards' top-N outputs into the final top-N).

- **Key classes**: `TopNOperator`
- **Dependencies**: E-1
- **Complexity**: M

### E-8: Implement LimitOperator
Implement the simple limit/offset operator. Count output rows and signal `isFinished()` once the limit is reached. Support OFFSET by skipping the first N rows. This operator is used when ordering is already handled upstream (e.g., by OpenSearch sort pushdown).

- **Key classes**: `LimitOperator`
- **Dependencies**: E-1
- **Complexity**: S

### E-9: Implement Driver and Pipeline execution framework
Create the `Driver` class that chains operators into a pipeline and drives execution in a single-threaded loop: call `getOutput()` on the source operator, pass pages through the pipeline, check for cancellation via interrupt flag. Create the `Pipeline` class that assembles an ordered chain of operators. Implement the execution loop with configurable yield behavior (yield after N pages or M milliseconds to avoid starving other work). Reference: design doc Section 11.1.

- **Key classes**: `Driver`, `Pipeline`, `DriverRunner`
- **Dependencies**: E-1 through E-8
- **Complexity**: M

### E-10: Implement PIT (Point-in-Time) lifecycle management
Create `PitManager` that manages PIT creation, usage, and cleanup for queries. Create a PIT for the target index at query start. Pass the PIT ID to `ShardScanOperator` for all search requests. Release the PIT in the query cleanup phase (both success and failure paths). Set PIT keep-alive to `query_timeout + 1m`. Handle `PIT_EXPIRED` errors. Reference: design doc Section 17.

- **Key classes**: `PitManager`, `PitHandle`
- **Dependencies**: E-2
- **Complexity**: M

### E-11: Implement query cancellation integration
Wire `CancellableTask` integration from OpenSearch's task management framework. Register each DQE query as a `DqeQueryTask` with the query ID and SQL text. On cancellation (via `_tasks` API or timeout), set the interrupt flag in all active `OperatorContext` instances. Operators check this flag in their processing loops and throw `DqeQueryCancelledException`. Ensure PIT release and memory cleanup on cancellation. Reference: design doc Section 18.

- **Key classes**: `DqeQueryTask`, `QueryCancellationHandler`
- **Dependencies**: E-9, E-10
- **Complexity**: M

### E-12: Implement query timeout enforcement
Create a timer-based mechanism that cancels a query after the configured timeout (`plugins.dqe.query_timeout`, default 5m). Allow per-query override via `session_properties.query_timeout`. The timeout triggers the same cancellation path as manual cancellation (E-11). Reference: design doc Section 16.3.

- **Key classes**: `QueryTimeoutScheduler`
- **Dependencies**: E-11
- **Complexity**: S

### E-13: Execution module unit tests
Write unit tests for all operators: `ShardScanOperator` (with mock search client), `FilterOperator`, `ProjectOperator`, `SortOperator`, `TopNOperator`, `LimitOperator`. Test the `Driver`/`Pipeline` framework with multi-operator chains. Test PIT lifecycle. Test cancellation propagation. Test timeout enforcement. Include edge cases: empty result sets, single-row results, NULL handling in sort/filter, large batch sizes. Minimum 60 test cases.

- **Key files**: `dqe/dqe-execution/src/test/java/.../`
- **Dependencies**: E-1 through E-12
- **Complexity**: L

---

## dqe-exchange Module (from task #6)

### X-1: Define exchange transport actions
Register the DQE transport actions with OpenSearch's `TransportService`: `internal:dqe/exchange/push`, `internal:dqe/exchange/close`, `internal:dqe/exchange/abort`, `internal:dqe/stage/execute`, `internal:dqe/stage/cancel`. Define the request/response classes implementing `TransportRequest`/`TransportResponse` and `Writeable` for serialization. Reference: design doc Section 12.1.

- **Key classes**: `DqeExchangePushAction`, `DqeStageExecuteAction`, `DqeStageCancelAction`, and corresponding request/response classes
- **Dependencies**: BI-1
- **Complexity**: M

### X-2: Implement DqeDataPage serialization (Writeable)
Implement binary columnar serialization for `Page`/`Block` data over OpenSearch transport. Create `DqeDataPage` implementing `Writeable` that can serialize/deserialize Trino `Page` objects without JSON intermediate. Implement LZ4 frame compression on the serialized payload. Include `uncompressedBytes` field for memory accounting before decompression. Reference: design doc Section 11.4 and Section 12.2.

- **Key classes**: `DqeDataPage`, `PageSerializer`, `PageDeserializer`
- **Dependencies**: T-7, X-1
- **Complexity**: L

### X-3: Implement DqeExchangeChunk message framing
Create the `DqeExchangeChunk` message class containing: `queryId`, `stageId`, `partitionId`, `sequenceNumber`, serialized `DqeDataPage` list, `isLast` flag, and `uncompressedBytes`. Implement `Writeable` serialization. Enforce configurable chunk size (default 1MB). Reference: design doc Section 12.2.

- **Key classes**: `DqeExchangeChunk`
- **Dependencies**: X-2
- **Complexity**: M

### X-4: Implement gather exchange (shard results to coordinator)
Implement the Phase 1 gather exchange pattern where all shard-level task results are pushed to a single coordinator buffer. Create `GatherExchangeSource` (coordinator-side, receives pages from all shards) and `GatherExchangeSink` (shard-side, pushes pages to coordinator). The coordinator collects pages from all shards and feeds them to the final-stage operators. Reference: design doc Section 12.4.

- **Key classes**: `GatherExchangeSource`, `GatherExchangeSink`, `ExchangeBuffer`
- **Dependencies**: X-1, X-3
- **Complexity**: L

### X-5: Implement exchange buffer with backpressure
Create the bounded exchange buffer (default 32MB per channel) that sits between producers and consumers. When the buffer is full, the producer's driver thread blocks with a configurable timeout (default 30s). If the timeout expires, the query is cancelled with `EXCHANGE_BUFFER_TIMEOUT`. When the buffer is empty, the consumer blocks until data arrives. Track buffer memory under the DQE circuit breaker. Reference: design doc Section 12.3.

- **Key classes**: `ExchangeBuffer`, `BackpressureController`
- **Dependencies**: X-4, MEM-1
- **Complexity**: M

### X-6: Implement stage execution dispatch
Create the coordinator logic that dispatches plan fragments to data nodes. Given the set of `DqeShardSplit` objects, group them by node, create `DqeStageExecuteRequest` for each node, and send via `TransportService`. Each data node receives the request, deserializes the plan fragment, creates a `Driver` + `Pipeline`, and starts execution on the `dqe_worker` thread pool. Collect results via gather exchange.

- **Key classes**: `StageScheduler`, `StageExecutionHandler`
- **Dependencies**: X-1, X-4, E-9, M-4
- **Complexity**: L

### X-7: Implement stage cancellation propagation
Create the cancellation path: coordinator sends `internal:dqe/stage/cancel` to all data nodes with active stages. Each data node interrupts driver threads, aborts exchange producers (sends `exchange/abort`), drains exchange buffers and releases memory, releases PITs, and sends cancellation ACK. Coordinator transitions query to CANCELLED after all ACKs or timeout. Reference: design doc Section 18.2.

- **Key classes**: `StageCancelHandler`, additions to `StageScheduler`
- **Dependencies**: X-6, E-11
- **Complexity**: M

### X-8: Implement sequence number deduplication
Add sequence number tracking to exchange consumers. Each channel maintains a `lastReceivedSequenceNumber`. Incoming chunks with a sequence number <= last received are discarded as duplicates. This handles transport-level retries. Reference: design doc Section 12.5.

- **Key classes**: additions to `ExchangeBuffer`
- **Dependencies**: X-4
- **Complexity**: S

### X-9: Exchange module unit tests
Write unit tests for: `DqeDataPage` serialization roundtrip, `DqeExchangeChunk` framing, gather exchange with multiple producers, exchange buffer backpressure behavior, stage dispatch with mock transport, cancellation propagation, sequence number dedup. Include edge cases: empty pages, single-row pages, buffer full timeout, node failure simulation. Minimum 30 test cases.

- **Key files**: `dqe/dqe-exchange/src/test/java/.../`
- **Dependencies**: X-1 through X-8
- **Complexity**: M

---

## dqe-memory Module (from task #7)

### MEM-1: Implement DqeMemoryTracker wrapping OpenSearch CircuitBreaker
Create `DqeMemoryTracker` that wraps OpenSearch's `CircuitBreaker` interface. Implement `reserve(long bytes, String label)` which checks both the DQE breaker and the parent breaker; throws `CircuitBreakingException` if either limit is exceeded. Implement `release(long bytes, String label)`. Implement `getUsedBytes()`. Every DQE memory allocation must go through this tracker. Reference: design doc Section 15.2.

- **Key classes**: `DqeMemoryTracker`
- **Dependencies**: BI-1
- **Complexity**: M

### MEM-2: Register DQE circuit breaker with OpenSearch
Register a new `dqe` circuit breaker as a child of the OpenSearch parent circuit breaker. Configure the default limit as 20% of heap (configurable via `plugins.dqe.memory.breaker_limit`). The breaker must be registered during plugin initialization and deregistered during plugin shutdown. Reference: design doc Section 15.1.

- **Key classes**: `DqeCircuitBreakerRegistrar`
- **Dependencies**: MEM-1
- **Complexity**: M

### MEM-3: Implement per-query memory budget
Create `QueryMemoryBudget` that tracks memory usage for a single query. Default budget: 256MB (configurable per-query via `session_properties.query_max_memory`). Each operator's `OperatorContext` allocates from this budget. When the budget is exceeded, the query fails with `EXCEEDED_QUERY_MEMORY_LIMIT`. The per-query budget is a sub-allocation of the DQE breaker. Reference: design doc Section 15.3.

- **Key classes**: `QueryMemoryBudget`, integration with `OperatorContext`
- **Dependencies**: MEM-1, E-1
- **Complexity**: M

### MEM-4: Implement memory cleanup on query failure/cancellation
Ensure that when a query fails or is cancelled, all reserved memory is released back to the breaker. Create a `QueryCleanup` handler that is called on both success and failure paths. It must: release all operator memory, drain exchange buffers and release their memory, release PIT resources, and log the final memory accounting. Reference: design doc Section 15.5.

- **Key classes**: `QueryCleanup`
- **Dependencies**: MEM-1, MEM-3, E-11
- **Complexity**: M

### MEM-5: Implement admission control semaphore
Create a node-level semaphore that limits concurrent DQE queries (default: 10, configurable via `plugins.dqe.max_concurrent_queries`). When the semaphore is full, new queries are rejected immediately with `TOO_MANY_CONCURRENT_QUERIES` (HTTP 429). No queueing. Reference: design doc Section 16.2.

- **Key classes**: `AdmissionController`
- **Dependencies**: BI-1
- **Complexity**: S

### MEM-6: Memory module unit tests
Write unit tests for: `DqeMemoryTracker` reserve/release semantics, breaker trip behavior, per-query budget enforcement, memory cleanup on failure, admission control rejection. Include edge cases: concurrent reservation, breaker trip during reservation, negative release (error), cleanup idempotency. Minimum 25 test cases.

- **Key files**: `dqe/dqe-memory/src/test/java/.../`
- **Dependencies**: MEM-1 through MEM-5
- **Complexity**: M

---

## dqe-plugin Module (from task #8)

### PL-1: Create DqeEnginePlugin entry point
Create the `DqeEnginePlugin` class that serves as the DQE engine's entry point within the existing `opensearch-sql` plugin. Implement registration hooks: register transport actions, thread pools, settings, and the engine routing callback. This class is loaded by `SQLPlugin` during plugin initialization. Reference: design doc Section 6.5.

- **Key classes**: `DqeEnginePlugin`
- **Dependencies**: BI-1
- **Complexity**: M

### PL-2: Implement engine routing integration
Implement the engine routing mechanism in the existing `SQLPlugin` REST handler. Add the `engine` request body field and `plugins.sql.engine` cluster setting. When `engine=dqe` is active, dispatch the request to the DQE execution path. When `engine=calcite`, use the existing Calcite path. Add the `plugins.dqe.enabled` setting (default: `true`) that controls whether DQE is available. Return `DQE_DISABLED` when disabled. Reference: design doc Section 6.1.

- **Key classes**: `EngineRouter`, modifications to existing `SQLPlugin` REST handler
- **Dependencies**: PL-1
- **Complexity**: M

### PL-3: Implement DQE request parsing and validation
Create the request parser that extracts the SQL query, `engine` field, `fetch_size`, and `session_properties` from the REST request body. Validate input: reject empty queries, validate session property types and ranges, enforce maximum query length. Produce a `DqeQueryRequest` object passed to the execution pipeline. Reference: design doc Section 6.3.

- **Key classes**: `DqeQueryRequest`, `DqeRequestParser`
- **Dependencies**: PL-2
- **Complexity**: M

### PL-4: Implement DQE response formatting
Create the response formatter that converts DQE query results into the JSON response format defined in design doc Section 6.4. Include the `engine` field (`"dqe"`), schema (column names and types), data (row arrays), and stats (state, query_id, elapsed_ms, rows_processed, bytes_processed, stages, shards_queried). Handle error responses with structured error fields including `error_code`.

- **Key classes**: `DqeResponseFormatter`, `DqeQueryResponse`
- **Dependencies**: PL-1
- **Complexity**: M

### PL-5: Implement the _explain endpoint for DQE
When `engine=dqe` is used with the `/_plugins/_sql/_explain` endpoint, return the DQE-specific query plan without executing. Show the plan tree including: parsed AST summary, resolved tables/columns, pushed-down predicates (Query DSL), remaining filter predicates, projection columns, sort specification, and limit. This is critical for debugging and verification. Reference: design doc Section 6.2.

- **Key classes**: `DqeExplainHandler`, `PlanPrinter`
- **Dependencies**: PL-2, A-1, E-2
- **Complexity**: M

### PL-6: Register DQE thread pools
Register the three DQE thread pools with OpenSearch: `dqe_worker` (fixed, `min(4, cores/2)`, queue 100), `dqe_exchange` (fixed, `min(2, cores/4)`, queue 200), `dqe_coordinator` (fixed, `min(2, cores/4)`, queue 50). All use `AbortPolicy` rejection (query fails on rejection). Reference: design doc Section 16.1.

- **Key classes**: additions to `DqeEnginePlugin`
- **Dependencies**: PL-1
- **Complexity**: S

### PL-7: Register DQE cluster settings
Register all DQE cluster settings from design doc Section 22 that are relevant to Phase 1: `plugins.sql.engine`, `plugins.dqe.enabled`, `plugins.dqe.max_concurrent_queries`, `plugins.dqe.query_timeout`, `plugins.dqe.memory.breaker_limit`, `plugins.dqe.memory.query_max_memory`, `plugins.dqe.exchange.buffer_size`, `plugins.dqe.exchange.chunk_size`, `plugins.dqe.exchange.timeout`, `plugins.dqe.exchange.backpressure_timeout`, `plugins.dqe.scan.batch_size`, `plugins.dqe.slow_query_log.threshold`, thread pool size settings.

- **Key classes**: `DqeSettings`
- **Dependencies**: PL-1
- **Complexity**: M

### PL-8: Implement Phase 1 observability metrics
Expose Phase 1 metrics via `_nodes/stats` and `_plugins/_sql/_dqe/stats`. Implement all query-level metrics (total, succeeded, failed, cancelled, active, wall_time_ms, rows_returned, rows_scanned, bytes_scanned) and memory metrics (used_bytes, breaker_limit, breaker_tripped). Thread pool metrics come from standard OpenSearch infrastructure. Reference: design doc Section 21.1.

- **Key classes**: `DqeMetrics`, `DqeStatsAction`
- **Dependencies**: PL-1, MEM-1
- **Complexity**: M

### PL-9: Implement slow query logging
Implement the slow query log: queries exceeding `plugins.dqe.slow_query_log.threshold` (default 10s) are logged at WARN level with query text (truncated to 1000 chars), execution time, rows scanned/returned, shards involved, and memory peak usage. Reference: design doc Section 21.4.

- **Key classes**: `SlowQueryLogger`
- **Dependencies**: PL-8
- **Complexity**: S

### PL-10: Implement end-to-end query execution orchestrator
Create the top-level orchestrator that wires together all DQE components for a single query: parse -> analyze -> plan (build operator tree) -> schedule (dispatch to shards) -> execute (drive operators) -> collect results (via gather exchange) -> format response. This is the central coordinator logic that sequences the query lifecycle, manages the PIT, handles errors, and invokes cleanup. Reference: design doc Section 10.

- **Key classes**: `DqeQueryOrchestrator`
- **Dependencies**: P-1, A-1, E-9, X-6, MEM-3, PL-3, PL-4
- **Complexity**: L

### PL-11: Implement audit logging integration
Log query text, user identity, indices accessed, and execution outcome (success/failure/cancel) to the OpenSearch security audit log. Reference: design doc Section 20.5.

- **Key classes**: `DqeAuditLogger`
- **Dependencies**: PL-10
- **Complexity**: S

### PL-12: Plugin module unit and integration tests
Write unit tests for: engine routing, request parsing, response formatting, explain output, settings registration, metrics collection. Write integration tests that start an embedded OpenSearch node with the DQE plugin and execute end-to-end queries (SELECT, WHERE, ORDER BY, LIMIT) against test data. Verify correct results, explain output, metrics, slow query logging, and cancellation. Minimum 40 test cases.

- **Key files**: `dqe/dqe-plugin/src/test/java/.../`, `dqe/dqe-integ-test/src/test/java/.../`
- **Dependencies**: PL-1 through PL-11
- **Complexity**: L

---

## Implementation Order (Critical Path)

The following shows the recommended implementation order organized into work streams that can be parallelized across developers.

### Work Stream 1: Build Infrastructure (Foundation)
```
BI-1 → BI-2 → BI-3
              → BI-4
       BI-5 (parallel with BI-2)
       BI-6 → BI-7
       BI-8
```
**Must complete before**: All other work streams can start after BI-1; BI-2 is needed for parser linkage tests.

### Work Stream 2: Parser + Types (Core Data Layer)
```
P-2, P-3 (exceptions, parallel with parser)
P-1 → P-4, P-5
T-1 → T-2 → T-3
           → T-5
           → T-6
     T-4
T-7 (depends on T-1, T-2)
T-8 (depends on T-1 through T-7)
```
**Can start after**: BI-1 (for project structure), BI-2 (for shading, needed by P-5)

### Work Stream 3: Metadata (Cluster State Integration)
```
M-1 → M-2 → M-5
     → M-3
     → M-4
     → M-6
M-7 (depends on M-1 through M-6)
```
**Can start after**: T-1 (for DqeType), T-2 (for type mapping)

### Work Stream 4: Analyzer (Semantic Analysis)
```
A-1 → A-2 → A-3 → A-5
                  → A-6
           → A-7
     A-4
     A-8
A-9 (depends on A-1 through A-8)
```
**Can start after**: P-1 (parser), M-2/M-3 (metadata), T-1 (types)

### Work Stream 5: Execution (Operators and Drivers)
```
E-1 → E-2 → E-10
     → E-4 → E-5
     → E-6
     → E-7
     → E-8
E-3 (depends on A-5, T-3)
E-9 (depends on E-1 through E-8)
E-11 → E-12
E-13 (depends on E-1 through E-12)
```
**Can start after**: T-7 (Page/Block converters), A-5 (predicate analysis for E-3)

### Work Stream 6: Exchange (Distributed Communication)
```
X-1 → X-2 → X-3 → X-4 → X-5
                        → X-8
     X-6 (depends on X-4, E-9, M-4)
     X-7 (depends on X-6, E-11)
X-9 (depends on X-1 through X-8)
```
**Can start after**: T-7 (for DqeDataPage), BI-1 (for transport)

### Work Stream 7: Memory (Safety Layer)
```
MEM-1 → MEM-2
      → MEM-3
      → MEM-4
MEM-5
MEM-6 (depends on MEM-1 through MEM-5)
```
**Can start after**: BI-1. Can proceed largely in parallel with other work streams.

### Work Stream 8: Plugin (Integration Layer)
```
PL-1 → PL-2 → PL-3
             → PL-5
       PL-6
       PL-7
       PL-8 → PL-9
PL-4
PL-10 (depends on P-1, A-1, E-9, X-6, MEM-3, PL-3, PL-4)
PL-11 (depends on PL-10)
PL-12 (depends on PL-1 through PL-11)
```
**Can start after**: BI-1. PL-10 is the final integration point requiring all other modules.

### Parallelization Summary

| Sprint | Developer A | Developer B | Developer C |
|---|---|---|---|
| 1 | BI-1, BI-2, BI-3, BI-4 | P-2, P-3, T-1 | MEM-1, MEM-2, MEM-5 |
| 2 | BI-5, BI-6, BI-8 | P-1, T-2, T-3, T-4, T-5 | MEM-3, MEM-4, X-1 |
| 3 | BI-7 | T-6, T-7, T-8, P-4, P-5 | X-2, X-3, MEM-6 |
| 4 | M-1, M-2, M-3, M-4 | A-1, A-2, A-4 | X-4, X-5, X-8 |
| 5 | M-5, M-6, M-7 | A-3, A-5, A-6, A-7, A-8 | PL-1, PL-2, PL-6, PL-7 |
| 6 | E-1, E-2, E-3 | A-9, E-4, E-5 | PL-3, PL-4, PL-5, PL-8 |
| 7 | E-6, E-7, E-8, E-10 | E-9, E-11, E-12 | X-6, X-7, PL-9 |
| 8 | E-13 | X-9, PL-10 | PL-11, PL-12 |

---

## Dependency Graph

```
                    BI-1 (Gradle structure)
                   / | \  \     \      \       \        \
                  /  |  \  \     \      \       \        \
               BI-2 BI-5 P-2  T-1    MEM-1   MEM-5    X-1     PL-1
               /|\        P-3  |  \     |  \            |      / | \
              / | \         \  T-2 T-4  MEM-2 MEM-3    X-2   PL-2 PL-6
           BI-3 BI-4 BI-8   \ /|\ \     \     |       |     /|\  PL-7
                              T-3 T-5 T-6 \   MEM-4   X-3  / | \
                               |       \   T-7  |     |   PL-3 PL-5
                               |        \  / \   \    X-4  |
                              M-1       T-8   \   \  / | \ PL-4
                             / | \ \          E-1 \ X-5 X-8
                            /  |  \ \        /|\ \ \    |
                          M-2 M-3 M-4 M-6  / | \ E-2 X-6
                           |   |       \  E-4 E-6 E-7 /|\
                          M-5  |        \ |   |    | / | \
                               |        E-5  E-8 E-10 X-7
                               |         |        |    |
                              A-1--------+--------E-9  |
                             / | \ \              / \   |
                            /  |  \ \           E-11 PL-8
                          A-2 A-4 A-8 \          |    |
                          |           A-3       E-12 PL-9
                         / \         / \         |
                       A-6 A-7    A-5   \       E-13
                                   |    \        |
                                  E-3    \     PL-10 <-- FINAL INTEGRATION
                                          \    / |
                                          A-9 / PL-11
                                             /    |
                                            /   PL-12
                                          X-9

Legend:
  BI-*  = Build Infrastructure
  P-*   = Parser
  T-*   = Types
  M-*   = Metadata
  A-*   = Analyzer
  E-*   = Execution
  X-*   = Exchange
  MEM-* = Memory
  PL-*  = Plugin
```

### Critical Path (longest dependency chain)

```
BI-1 → BI-2 → T-1 → T-2 → T-7 → E-1 → E-2 → E-9 → X-6 → PL-10 → PL-12
```

This is the longest sequential chain and determines the minimum calendar time for Phase 1 completion. Tasks not on this critical path have scheduling flexibility.

### Module Dependency Summary (simplified)

```
dqe-plugin ──→ dqe-exchange ──→ dqe-execution ──→ dqe-analyzer ──→ dqe-metadata ──→ dqe-types
    │                │                                                                    │
    │                └─────────→ dqe-memory                                              │
    │                                                                                     │
    └─────────────────────────────────────────────────────────── dqe-parser (standalone) ──┘
```

---

## Task Count Summary

| Module | Task Count |
|---|---|
| Build Infrastructure | 8 |
| dqe-parser | 5 |
| dqe-types | 8 |
| dqe-metadata | 7 |
| dqe-analyzer | 9 |
| dqe-execution | 13 |
| dqe-exchange | 9 |
| dqe-memory | 6 |
| dqe-plugin | 12 |
| **Total** | **77** |
