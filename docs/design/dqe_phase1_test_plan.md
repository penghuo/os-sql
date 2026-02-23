# DQE Phase 1 Test Plan: Foundation — Scan, Filter, Project

**Status**: Draft
**Date**: 2026-02-22
**Scope**: Phase 1 — `SELECT`, `FROM`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
**Reference**: `docs/design/dqe_design.md`, `docs/design/dqe_phase1_tasks.md`

---

## 1. Overview

This test plan covers all Phase 1 exit criteria defined in the DQE design document (Section 24). Phase 1 delivers scan, filter, project, sort, and limit operations across all OpenSearch type mappings. The plan is organized into unit tests per module, integration tests, conformance suites (NULL, type coercion, timezone), differential tests, and non-functional tests (performance, memory safety, cancellation, security).

### Exit Criteria Summary

| # | Criterion | Test Section |
|---|---|---|
| 1 | 118 scan/filter/project/sort/limit queries across all type mappings | §4 |
| 2 | 52 NULL propagation test cases | §5 |
| 3 | Type coercion conformance (implicit widening, explicit CAST, failures) | §6 |
| 4 | Timezone conformance (timestamp precision, comparison, ordering across DST) | §7 |
| 5 | Differential test — byte-identical results against standalone Trino | §8 |
| 6 | Performance gate — P99 search latency increase ≤ 20% under mixed workload | §9 |
| 7 | Memory safety gate — 10 concurrent scans, 1M rows each, no breaker trip | §10 |
| 8 | Cancellation gate — memory and PITs released within 5s | §11 |

### Test Execution Commands

**Unit tests** (Gradle, run in CI):

| Command | Scope |
|---|---|
| `./gradlew :dqe-parser:test` | Parser unit tests |
| `./gradlew :dqe-types:test` | Type mapping unit tests |
| `./gradlew :dqe-metadata:test` | Metadata unit tests |
| `./gradlew :dqe-analyzer:test` | Analyzer unit tests |
| `./gradlew :dqe-execution:test` | Execution unit tests |
| `./gradlew :dqe-exchange:test` | Exchange unit tests |
| `./gradlew :dqe-memory:test` | Memory unit tests |
| `./gradlew :dqe-plugin:test` | Plugin unit tests |

**Integration, differential, and performance tests** (REST API scripts against a running cluster):

| Command | Scope |
|---|---|
| `./gradlew :dqe-plugin:run` | Start local OpenSearch + DQE cluster on `localhost:9200` |
| `scripts/dqe-test/setup-data.sh` | Bulk-index all test datasets into the running cluster |
| `scripts/dqe-test/run-phase1-tests.sh` | Run all Phase 1 integration test queries via REST API |
| `scripts/dqe-test/run-null-tests.sh` | Run NULL conformance suite via REST API |
| `scripts/dqe-test/run-type-coercion-tests.sh` | Run type coercion suite via REST API |
| `scripts/dqe-test/run-timezone-tests.sh` | Run timezone conformance suite via REST API |
| `scripts/dqe-test/run-differential-tests.sh` | Run differential tests (DQE vs standalone Trino) via REST API |
| `scripts/dqe-test/run-performance-tests.sh` | Run performance benchmarks via REST API |
| `scripts/dqe-test/run-memory-tests.sh` | Run memory safety tests via REST API |
| `scripts/dqe-test/run-cancellation-tests.sh` | Run cancellation tests via REST API |
| `scripts/dqe-test/run-security-tests.sh` | Run security tests via REST API |
| `scripts/dqe-test/run-all.sh` | Run all test suites sequentially |

### Test Architecture: Why REST API Scripts, Not Gradle Tasks

Integration, differential, and performance tests are executed as **external scripts calling the REST API** against a running OpenSearch cluster — not as Gradle test tasks. This design is deliberate:

1. **Auditability**: Test cases are stored as declarative query+expected-result pairs in JSON files under `scripts/dqe-test/cases/`. Anyone can inspect the test data, re-run individual queries manually via `curl`, and verify results independently. A Gradle test task compiles and runs Java code whose correctness is harder to audit at a glance.
2. **Reproducibility**: The test runner is a simple bash script that `POST`s queries to `/_plugins/_sql` and diffs the response against expected results. The entire test execution is transparent — every HTTP request and response is logged.
3. **Separation of concerns**: Unit tests (Gradle) validate internal module logic with mocks. Integration tests (REST scripts) validate the assembled system end-to-end through the same interface that users interact with.
4. **Cluster lifecycle**: The cluster is started once via `./gradlew :dqe-plugin:run` (identical to the existing `./gradlew opensearch-sql:run` pattern), and tests run against it. This mirrors how a developer or CI system would test the plugin.

### Local Cluster Startup

The `./gradlew :dqe-plugin:run` task starts a local single-node OpenSearch cluster with the DQE plugin installed, following the same pattern as the existing `./gradlew opensearch-sql:run`:

```gradle
// dqe-plugin/build.gradle
testClusters.integTest {
    plugin(project.tasks.bundlePlugin.archiveFile)
    testDistribution = "ARCHIVE"
    if (System.getProperty("debugJVM") != null) {
        jvmArgs '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005'
    }
}

run {
    useCluster testClusters.integTest
}
```

After startup, the cluster is accessible at `http://localhost:9200`. Debug with `./gradlew :dqe-plugin:run -DdebugJVM` (port 5005).

---

## 2. Test Dataset Specification

### 2.1 Primary Test Index: `dqe_test_all_types`

Mapping covering all OpenSearch field types from design doc Section 9.1:

```json
{
  "mappings": {
    "_meta": {
      "dqe.arrays": ["tags", "nested_items", "vector_field"]
    },
    "properties": {
      "id":              { "type": "keyword" },
      "name":            { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "description":     { "type": "text", "fielddata": true },
      "status":          { "type": "keyword" },
      "count_long":      { "type": "long" },
      "count_int":       { "type": "integer" },
      "count_short":     { "type": "short" },
      "count_byte":      { "type": "byte" },
      "price_double":    { "type": "double" },
      "price_float":     { "type": "float" },
      "price_half":      { "type": "half_float" },
      "price_scaled":    { "type": "scaled_float", "scaling_factor": 100 },
      "is_active":       { "type": "boolean" },
      "created_at":      { "type": "date", "format": "epoch_millis" },
      "updated_at":      { "type": "date", "format": "strict_date_optional_time" },
      "custom_date":     { "type": "date", "format": "yyyy/MM/dd HH:mm:ss" },
      "precise_time":    { "type": "date_nanos" },
      "ip_address":      { "type": "ip" },
      "location":        { "type": "geo_point" },
      "boundary":        { "type": "geo_shape" },
      "tags":            { "type": "keyword" },
      "nested_items":    { "type": "nested", "properties": {
        "item_name":     { "type": "keyword" },
        "item_value":    { "type": "integer" }
      }},
      "metadata":        { "type": "object", "properties": {
        "source":        { "type": "keyword" },
        "version":       { "type": "integer" }
      }},
      "attributes":      { "type": "flattened" },
      "vector_field":    { "type": "dense_vector", "dims": 3 },
      "binary_data":     { "type": "binary" },
      "big_number":      { "type": "unsigned_long" }
    }
  }
}
```

**Data characteristics**:
- 10,000 documents for functional tests
- Approximately 10% of rows have NULL values in each nullable field
- `tags` field has 0-5 values per document (multi-valued)
- `custom_date` uses the non-standard `yyyy/MM/dd HH:mm:ss` format
- `location` includes edge cases: poles, antimeridian, equator
- `big_number` includes values exceeding BIGINT range (>2^63)

### 2.2 NULL-Heavy Test Index: `dqe_test_nulls`

```json
{
  "mappings": {
    "properties": {
      "id":       { "type": "keyword" },
      "int_val":  { "type": "integer" },
      "str_val":  { "type": "keyword" },
      "bool_val": { "type": "boolean" },
      "date_val": { "type": "date" },
      "dbl_val":  { "type": "double" }
    }
  }
}
```

**Data**: 1,000 documents. Columns `int_val`, `str_val`, `bool_val`, `date_val`, `dbl_val` are each NULL in approximately 30% of rows, with systematic patterns (all-NULL rows, no-NULL rows, single-NULL rows).

### 2.3 Type Conflict Test Indices: `dqe_test_conflict_a`, `dqe_test_conflict_b`

Two indices with a shared index pattern `dqe_test_conflict_*` but conflicting field types:
- Index A: `value` is `long`, `label` is `keyword`
- Index B: `value` is `integer`, `label` is `text`

Used to test type widening (`integer` + `long` → `BIGINT`) and incompatible type exclusion.

### 2.4 Scale Test Index: `dqe_test_scale`

- 1,000,000 documents (for memory safety tests)
- 100,000,000 documents (for performance tests, created by OpenSearch Rally)
- Simple mapping: `id` (keyword), `category` (keyword, 100 distinct values), `amount` (double), `timestamp` (date), `payload` (keyword, 200-byte values)

### 2.5 Multi-Shard Test Index: `dqe_test_sharded`

- 5 primary shards, 1 replica
- 50,000 documents with globally unique `id` values: `id_00001` to `id_50000`
- Mapping: `id` (keyword), `category` (keyword, 100 distinct values `cat_01`..`cat_100`), `amount` (double)
- **Adversarial data distribution for TopN testing**: `amount` is inversely correlated with `id` order — `id_00001` has `amount = 1.0`, `id_50000` has `amount = 99999.99`. This ensures that `ORDER BY amount DESC LIMIT N` must scan all shards to find the correct top-N; the highest-amount rows are in the last shard by `id` order. A "take first N rows seen" TopN implementation will return the wrong rows.
- Used to verify: (a) no duplicate rows from double-scanning primary+replica, (b) TopN correctness across shards, (c) full sort correctness

### 2.6 Dataset Materialization

All test datasets are defined as declarative files under `scripts/dqe-test/data/`:

```
scripts/dqe-test/data/
  mappings/
    dqe_test_all_types.json        # Index mapping
    dqe_test_nulls.json
    dqe_test_conflict_a.json
    dqe_test_conflict_b.json
    dqe_test_scale.json
    dqe_test_sharded.json
  bulk/
    dqe_test_all_types.ndjson      # Bulk data (NDJSON)
    dqe_test_nulls.ndjson
    dqe_test_conflict_a.ndjson
    dqe_test_conflict_b.ndjson
    dqe_test_sharded.ndjson
  generators/
    generate_scale_data.sh         # Generates dqe_test_scale (1M docs)
    generate_perf_data.sh          # Generates dqe_test_perf_100m (100M docs via Rally)
```

The `scripts/dqe-test/setup-data.sh` script:
1. Checks that the cluster is reachable at `http://localhost:9200` (or `$DQE_TEST_HOST`).
2. Deletes existing `dqe_test_*` indices (idempotent re-run).
3. Creates each index with its mapping from `data/mappings/`.
4. Bulk-indexes data from `data/bulk/` using `POST _bulk`.
5. For scale datasets, runs the generator script.
6. Calls `POST _refresh` to ensure all data is searchable.
7. Prints index doc counts for verification.

For the 100M-doc performance dataset, an OpenSearch Rally track definition (`scripts/dqe-test/data/generators/dqe-phase1-track.json`) generates the data.

---

## 3. Unit Test Plan (Per Module)

### 3.1 dqe-parser (≥30 tests)

| Test Class | Key Test Cases |
|---|---|
| `DqeSqlParserTest` | Valid SELECT/FROM/WHERE/ORDER BY/LIMIT parsing; expressions (arithmetic, comparison, boolean, CAST, IS NULL, BETWEEN, IN, LIKE); column aliases; star expansion; invalid syntax (error with line/column); empty query; very long query |
| `DqeParsingExceptionTest` | Exception wrapping preserves line/column; error message formatting |
| `ParserLinkageTest` | Shaded JAR loads without ClassNotFoundException; parse representative query end-to-end |

### 3.2 dqe-types (≥50 tests)

| Test Class | Key Test Cases |
|---|---|
| `DqeTypeTest` | All type enum values; parameterized types (DECIMAL precision/scale, TIMESTAMP precision) |
| `OpenSearchTypeMappingResolverTest` | All 22 OpenSearch types map correctly; `scaled_float` derives correct DECIMAL(p,s) from `scaling_factor`; unknown type produces warning |
| `MultiFieldResolverTest` | Text+keyword sub-field detection; fielddata-enabled text field; text without keyword sub-field |
| `DqeTypeCoercionTest` | INTEGER+BIGINT→BIGINT; REAL+DOUBLE→DOUBLE; implicit widening matrix; explicit CAST matrix; incompatible type error |
| `DateFormatResolverTest` | epoch_millis; epoch_second; strict_date_optional_time; custom pattern `yyyy/MM/dd`; invalid format |
| `ArrayDetectorTest` | Explicit `_meta.dqe.arrays` annotation; sampling detection; scalar fallback |
| `SearchHitToPageConverterTest` | Convert hits to Pages for each type; NULL handling; batch boundaries; empty result set |

### 3.3 dqe-metadata (≥30 tests)

| Test Class | Key Test Cases |
|---|---|
| `DqeTableHandleTest` | Handle serialization; equality; index vs alias |
| `DqeMetadataGetTableHandleTest` | Existing index; missing index; closed index; alias; index pattern with wildcards |
| `DqeMetadataGetColumnHandlesTest` | All types; nested fields (dot notation); multi-field info; type conflicts across index pattern (widening); incompatible types (exclusion + warning) |
| `DqeMetadataGetSplitsTest` | Shard enumeration; primary + replica selection; adaptive selection (local preference) |
| `SchemaSnapshotTest` | Snapshot frozen at analysis time; concurrent mapping change does not affect in-flight query |
| `DqeTableStatisticsTest` | Row count from _stats; index size; cache hit; cache invalidation on >10% doc count change |

### 3.4 dqe-analyzer (≥50 tests)

| Test Class | Key Test Cases |
|---|---|
| `DqeAnalyzerTest` | Basic SELECT/FROM; SELECT with aliases; SELECT * |
| `ScopeResolverTest` | Unqualified column; qualified (table.column); alias reference; ambiguous column error |
| `ExpressionTypeCheckerTest` | Arithmetic type promotion; comparison type compatibility; boolean logic requires BOOLEAN; CAST validation; IS NULL returns BOOLEAN; BETWEEN; IN; LIKE |
| `UnsupportedConstructValidatorTest` | GROUP BY rejected; HAVING rejected; aggregate functions rejected; JOIN rejected; subqueries rejected; CTEs rejected; UNION rejected; window functions rejected — each with specific error message |
| `PredicateAnalyzerTest` | Equality → term; range → range; AND → bool.must; OR → bool.should; NOT → bool.must_not; IS NULL → must_not.exists; IS NOT NULL → exists; IN → terms; LIKE → wildcard; complex expression → not pushed down |
| `ProjectionAnalyzerTest` | SELECT columns → required columns; WHERE references → included; ORDER BY references → included; minimal set computation |
| `OrderByAnalyzerTest` | Single column; multiple columns; ASC/DESC; NULLS FIRST/LAST; unsortable type rejection (geo_point, nested) |
| `SecurityPermissionTest` | User with access → proceeds; user without access → ACCESS_DENIED; multi-index query with partial access → rejected |

### 3.5 dqe-execution (≥60 tests)

| Test Class | Key Test Cases |
|---|---|
| `ShardScanOperatorTest` | Basic scan with mock search client; column pruning (fetchSource); predicate pushdown (Query DSL in request); search_after pagination; PIT usage; empty index; batch boundary handling |
| `PredicateToQueryDslConverterTest` | All predicate types → correct Query DSL; multi-field resolution (text.keyword for term); nested boolean expressions; unsupported predicate → not converted |
| `FilterOperatorTest` | Pass-through matching rows; filter out non-matching; NULL handling in predicates; empty input; all filtered out |
| `ProjectOperatorTest` | Column pass-through; arithmetic expression; CAST; string concatenation; NULL propagation; alias mapping |
| `SortOperatorTest` | Single column ASC; single column DESC; multi-column; NULLS FIRST; NULLS LAST; already sorted input; single row; empty input |
| `TopNOperatorTest` | Top 10 from 1000 rows; top 1; LIMIT equals input size; LIMIT exceeds input size; multi-column sort |
| `LimitOperatorTest` | Basic limit; limit with offset; limit 0; offset exceeds row count |
| `DriverTest` | Multi-operator pipeline; cancellation mid-execution (interrupt flag); memory tracking through pipeline |
| `PitManagerTest` | PIT creation; PIT release on success; PIT release on failure; PIT keep-alive configuration; PIT expired error |
| `QueryCancellationTest` | Cancel via interrupt flag; operators check flag in processing loop; resources released on cancellation |
| `QueryTimeoutTest` | Timeout fires; query cancelled; resources released |

### 3.6 dqe-exchange (≥30 tests)

| Test Class | Key Test Cases |
|---|---|
| `DqeDataPageSerializationTest` | Roundtrip serialization for each Block type; compressed vs uncompressed; empty page; large page |
| `DqeExchangeChunkTest` | Framing with all fields; chunk size enforcement; isLast flag |
| `GatherExchangeTest` | Single producer; multiple producers; all producers complete; pages arrive in order |
| `ExchangeBufferTest` | Buffer fill and drain; backpressure blocks producer; timeout on full buffer → EXCHANGE_BUFFER_TIMEOUT; memory tracking under circuit breaker |
| `SequenceDeduplicationTest` | Duplicate sequence number discarded; out-of-order delivery handled; gap detection |
| `StageDispatchTest` | Fragment dispatch to multiple nodes; response collection; node failure handling |
| `StageCancellationTest` | Cancel propagates to all nodes; resources released; ACK collection |

### 3.7 dqe-memory (≥25 tests)

| Test Class | Key Test Cases |
|---|---|
| `DqeMemoryTrackerTest` | Reserve/release; getUsedBytes accurate; label tracking |
| `CircuitBreakerIntegrationTest` | DQE breaker trips at limit; parent breaker trips; CircuitBreakingException propagated; breaker reset after release |
| `QueryMemoryBudgetTest` | Default 256MB; per-query override; budget exceeded → EXCEEDED_QUERY_MEMORY_LIMIT; multiple operators share budget |
| `QueryCleanupTest` | Cleanup on success releases all memory; cleanup on failure releases all memory; cleanup idempotent; exchange buffers released; PIT released |
| `AdmissionControllerTest` | Accept up to limit (10); reject at limit → HTTP 429; release on completion; concurrent access safety |

### 3.8 dqe-plugin (≥40 tests)

| Test Class | Key Test Cases |
|---|---|
| `EngineRouterTest` | `engine=dqe` routes to DQE; `engine=calcite` routes to Calcite; default from setting; `plugins.dqe.enabled=false` → DQE_DISABLED |
| `DqeRequestParserTest` | Valid request; missing query; invalid session properties; max query length |
| `DqeResponseFormatterTest` | Schema formatting; data row formatting; stats formatting; engine field present; error response formatting |
| `DqeExplainHandlerTest` | Explain output includes plan tree; pushed-down predicates shown; remaining filters shown |
| `DqeSettingsTest` | All settings registered; default values correct; dynamic update |
| `DqeMetricsTest` | Query counters increment; wall_time_ms histogram populated; rows_scanned counter; memory metrics |
| `SlowQueryLoggerTest` | Query above threshold logged; query below threshold not logged; log format correct |
| `DqeQueryOrchestratorTest` | End-to-end: parse → analyze → plan → execute → format response; error propagation; cancellation |
| `DqeAuditLoggerTest` | Query text logged; user identity logged; indices logged; outcome logged |
| `DqeIntegrationTest` | End-to-end REST request/response; SELECT with WHERE; ORDER BY + LIMIT; explain; error responses; concurrent queries |

---

## 4. Integration Test Plan (100+ Queries)

### Phase 1 Expression Scope

Phase 1 supports the following **SQL expression constructs** in SELECT and WHERE clauses:
- Arithmetic: `+`, `-`, `*`, `/`, `%`, unary `-`
- Comparison: `=`, `!=`/`<>`, `<`, `>`, `<=`, `>=`
- Boolean: `AND`, `OR`, `NOT`
- Predicates: `BETWEEN`, `IN`, `LIKE`, `IS NULL`, `IS NOT NULL`
- Type coercion: `CAST(expr AS type)`, `TRY_CAST(expr AS type)`
- Conditional: `CASE WHEN ... THEN ... ELSE ... END`, `CASE expr WHEN ... THEN ... END`
- NULL handling: `COALESCE(expr, ...)`, `NULLIF(expr, expr)`
- Literals: integer, decimal, string, boolean, NULL

Phase 1 does **NOT** support:
- Named function calls: `IF()`, `TYPEOF()`, `EXTRACT()`, `CONCAT()`, `LENGTH()`, `SUBSTR()`, etc. (Phase 2 — requires dqe-functions module)
- `SELECT` without `FROM` (e.g., `SELECT 1 + 2`) — Phase 2
- `DISTINCT` — Phase 2 (requires dedup operator)
- Aggregate functions, GROUP BY, JOIN, subqueries, CTEs, window functions — Phase 2+

Every test case in this plan uses only Phase 1 constructs. Test cases for Phase 2+ constructs appear in the error cases section (§4.7) to verify they are properly rejected.

### Test Case Format and Runner

Each test case is a JSON file under `scripts/dqe-test/cases/`. Test cases are organized into subdirectories matching the sections below:

```
scripts/dqe-test/cases/
  phase1/
    basic_select/
      Q001_basic_column_selection.json
      Q002_star_expansion.json
      ...
    where_predicates/
      Q016_equality_term_pushdown.json
      ...
    type_specific/
    order_by_limit/
    multi_shard/
    expressions/
    error_cases/
  null_conformance/
    N01_null_plus_one.json
    ...
  type_coercion/
    C01_byte_plus_short.json
    ...
  timezone/
    TZ01_epoch_millis_precision.json
    ...
```

Each test case file has the following structure:

```json
{
  "id": "Q001",
  "name": "basic_column_selection",
  "query": "SELECT id, name FROM dqe_test_all_types LIMIT 10",
  "engine": "dqe",
  "expect": {
    "status": 200,
    "schema": [
      {"name": "id", "type": "VARCHAR"},
      {"name": "name", "type": "VARCHAR"}
    ],
    "row_count": 10,
    "contains_rows": [
      ["id_00001", "test_name_1"]
    ],
    "ordered": false
  }
}
```

For error cases:

```json
{
  "id": "Q096",
  "name": "missing_index",
  "query": "SELECT * FROM nonexistent_index",
  "engine": "dqe",
  "expect": {
    "status": 400,
    "error_contains": "Table not found",
    "error_code": "TABLE_NOT_FOUND"
  }
}
```

**Supported expect fields**:

| Field | Description |
|---|---|
| `status` | Expected HTTP status code |
| `schema` | Expected column names and types (exact match) |
| `row_count` | Expected number of rows (exact match) |
| `min_row_count` | Minimum number of rows |
| `data` | Expected full result set (exact row-by-row match) |
| `contains_rows` | Expected rows that must appear in the result (order-independent) |
| `ordered` | If `true`, `data` must match in order; if `false`, both sides are sorted before comparison |
| `unique_column` | Column name whose values must all be distinct (fails on any duplicate) |
| `error_contains` | Substring that must appear in the error message |
| `error_code` | Expected DQE error code |
| `engine` | Verify response `engine` field matches |

**Test runner** (`scripts/dqe-test/run-phase1-tests.sh`):

```bash
#!/bin/bash
# Usage: ./scripts/dqe-test/run-phase1-tests.sh [host:port] [test_dir]
HOST=${1:-localhost:9200}
TEST_DIR=${2:-scripts/dqe-test/cases/phase1}

PASS=0; FAIL=0; SKIP=0
for case_file in $(find "$TEST_DIR" -name '*.json' | sort); do
  QUERY=$(jq -r '.query' "$case_file")
  ENGINE=$(jq -r '.engine // "dqe"' "$case_file")
  CASE_ID=$(jq -r '.id' "$case_file")

  # Execute query via REST API
  RESPONSE=$(curl -s -w '\n%{http_code}' -X POST "http://$HOST/_plugins/_sql" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$QUERY\", \"engine\": \"$ENGINE\"}")

  HTTP_CODE=$(echo "$RESPONSE" | tail -1)
  BODY=$(echo "$RESPONSE" | head -n -1)

  # Validate response against expected results
  RESULT=$(scripts/dqe-test/validate.py "$case_file" "$HTTP_CODE" "$BODY")
  # ... report pass/fail per case
done
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
```

The `scripts/dqe-test/validate.py` script compares the actual HTTP response against the `expect` block in the test case file. It handles schema matching, row comparison (ordered and unordered), error message matching, and floating-point tolerance. Every request and response is logged to `scripts/dqe-test/results/` for post-mortem analysis.

All integration tests execute queries via `POST /_plugins/_sql` with `"engine": "dqe"` against test datasets from Section 2.

### 4.1 Basic SELECT (15 queries)

| # | Query | Validates |
|---|---|---|
| Q001 | `SELECT id, name FROM dqe_test_all_types LIMIT 10` | Basic column selection |
| Q002 | `SELECT * FROM dqe_test_all_types LIMIT 5` | Star expansion to all columns |
| Q003 | `SELECT id AS identifier, name AS label FROM dqe_test_all_types LIMIT 5` | Column aliases |
| Q004 | `SELECT id, status, count_int FROM dqe_test_all_types LIMIT 5` | Multi-column selection |
| Q005 | `SELECT count_int + 1 AS incremented FROM dqe_test_all_types LIMIT 5` | Arithmetic expression |
| Q006 | `SELECT count_long * price_double FROM dqe_test_all_types LIMIT 5` | Cross-type arithmetic |
| Q007 | `SELECT CAST(count_int AS BIGINT) FROM dqe_test_all_types LIMIT 5` | Explicit CAST |
| Q008 | `SELECT CAST(count_int AS VARCHAR) FROM dqe_test_all_types LIMIT 5` | CAST to string |
| Q009 | `SELECT id, metadata.source, metadata.version FROM dqe_test_all_types LIMIT 5` | Object field dot notation |
| Q010 | `SELECT location.lat, location.lon FROM dqe_test_all_types WHERE location IS NOT NULL LIMIT 5` | geo_point ROW access |
| Q011 | `SELECT id FROM dqe_test_all_types LIMIT 0` | LIMIT 0 returns empty |
| Q012 | `SELECT id FROM dqe_test_all_types LIMIT 5 OFFSET 3` | OFFSET |
| Q013 | `SELECT id, CAST(count_int AS DOUBLE), CAST(price_double AS VARCHAR) FROM dqe_test_all_types LIMIT 5` | Multiple CAST targets in SELECT |
| Q014 | `SELECT id, COALESCE(str_val, 'default') FROM dqe_test_nulls LIMIT 10` | COALESCE |
| Q015 | `SELECT id, NULLIF(int_val, 0) FROM dqe_test_nulls LIMIT 10` | NULLIF |

### 4.2 WHERE Predicates (25 queries)

| # | Query | Validates |
|---|---|---|
| Q016 | `SELECT id FROM dqe_test_all_types WHERE status = 'active'` | Equality (term pushdown) |
| Q017 | `SELECT id FROM dqe_test_all_types WHERE status != 'active'` | Inequality |
| Q018 | `SELECT id FROM dqe_test_all_types WHERE count_int > 100` | Greater than (range pushdown) |
| Q019 | `SELECT id FROM dqe_test_all_types WHERE count_int >= 100` | Greater than or equal |
| Q020 | `SELECT id FROM dqe_test_all_types WHERE count_int < 50` | Less than |
| Q021 | `SELECT id FROM dqe_test_all_types WHERE count_int <= 50` | Less than or equal |
| Q022 | `SELECT id FROM dqe_test_all_types WHERE status = 'active' AND count_int > 10` | AND (bool.must pushdown) |
| Q023 | `SELECT id FROM dqe_test_all_types WHERE status = 'active' OR count_int > 100` | OR (bool.should pushdown) |
| Q024 | `SELECT id FROM dqe_test_all_types WHERE NOT is_active` | NOT (bool.must_not) |
| Q025 | `SELECT id FROM dqe_test_all_types WHERE status IN ('active', 'pending', 'closed')` | IN (terms pushdown) |
| Q026 | `SELECT id FROM dqe_test_all_types WHERE count_int BETWEEN 10 AND 100` | BETWEEN (range pushdown) |
| Q027 | `SELECT id FROM dqe_test_all_types WHERE name LIKE 'test%'` | LIKE prefix (wildcard pushdown) |
| Q028 | `SELECT id FROM dqe_test_all_types WHERE name LIKE '%middle%'` | LIKE contains |
| Q029 | `SELECT id FROM dqe_test_all_types WHERE count_int IS NULL` | IS NULL (must_not.exists) |
| Q030 | `SELECT id FROM dqe_test_all_types WHERE count_int IS NOT NULL` | IS NOT NULL (exists) |
| Q031 | `SELECT id FROM dqe_test_all_types WHERE status = 'active' AND (count_int > 10 OR price_double < 5.0)` | Nested boolean |
| Q032 | `SELECT id FROM dqe_test_all_types WHERE CAST(count_int AS DOUBLE) > 10.5` | CAST in WHERE |
| Q033 | `SELECT id FROM dqe_test_all_types WHERE count_int + count_long > 200` | Cross-column expression (not pushed down) |
| Q034 | `SELECT id FROM dqe_test_all_types WHERE is_active = true` | Boolean equality |
| Q035 | `SELECT id FROM dqe_test_all_types WHERE created_at > TIMESTAMP '2024-01-01 00:00:00'` | Timestamp comparison |
| Q036 | `SELECT id FROM dqe_test_all_types WHERE ip_address = '192.168.1.1'` | IP field equality |
| Q037 | `SELECT id FROM dqe_test_nulls WHERE int_val IS NULL AND str_val IS NOT NULL` | Multiple NULL checks |
| Q038 | `SELECT id FROM dqe_test_all_types WHERE price_scaled > 99.99` | scaled_float/DECIMAL comparison |
| Q039 | `SELECT id FROM dqe_test_all_types WHERE big_number > 9223372036854775807` | unsigned_long exceeding BIGINT |
| Q040 | `SELECT id FROM dqe_test_all_types WHERE 1 = 1` | Always-true predicate |

### 4.3 Type-Specific Queries (20 queries)

| # | Query | Validates |
|---|---|---|
| Q041 | `SELECT count_long FROM dqe_test_all_types WHERE count_long = 42 LIMIT 5` | long → BIGINT |
| Q042 | `SELECT count_int FROM dqe_test_all_types WHERE count_int = 42 LIMIT 5` | integer → INTEGER |
| Q043 | `SELECT count_short FROM dqe_test_all_types WHERE count_short = 42 LIMIT 5` | short → SMALLINT |
| Q044 | `SELECT count_byte FROM dqe_test_all_types WHERE count_byte = 42 LIMIT 5` | byte → TINYINT |
| Q045 | `SELECT price_double FROM dqe_test_all_types WHERE price_double > 0.0 LIMIT 5` | double → DOUBLE |
| Q046 | `SELECT price_float FROM dqe_test_all_types WHERE price_float > 0.0 LIMIT 5` | float → REAL |
| Q047 | `SELECT price_half FROM dqe_test_all_types WHERE price_half > 0.0 LIMIT 5` | half_float → REAL |
| Q048 | `SELECT price_scaled FROM dqe_test_all_types WHERE price_scaled > 0 LIMIT 5` | scaled_float → DECIMAL |
| Q049 | `SELECT is_active FROM dqe_test_all_types WHERE is_active = true LIMIT 5` | boolean → BOOLEAN |
| Q050 | `SELECT created_at FROM dqe_test_all_types WHERE created_at IS NOT NULL LIMIT 5` | date (epoch_millis) → TIMESTAMP(3) |
| Q051 | `SELECT updated_at FROM dqe_test_all_types WHERE updated_at IS NOT NULL LIMIT 5` | date (strict_date_optional_time) → TIMESTAMP(3) |
| Q052 | `SELECT custom_date FROM dqe_test_all_types WHERE custom_date IS NOT NULL LIMIT 5` | date (custom format) → TIMESTAMP(3) |
| Q053 | `SELECT precise_time FROM dqe_test_all_types WHERE precise_time IS NOT NULL LIMIT 5` | date_nanos → TIMESTAMP(9) |
| Q054 | `SELECT ip_address FROM dqe_test_all_types WHERE ip_address IS NOT NULL LIMIT 5` | ip → VARCHAR |
| Q055 | `SELECT location FROM dqe_test_all_types WHERE location IS NOT NULL LIMIT 5` | geo_point → ROW(lat DOUBLE, lon DOUBLE) |
| Q056 | `SELECT boundary FROM dqe_test_all_types WHERE boundary IS NOT NULL LIMIT 5` | geo_shape → VARCHAR (GeoJSON) |
| Q057 | `SELECT attributes FROM dqe_test_all_types WHERE attributes IS NOT NULL LIMIT 5` | flattened → MAP(VARCHAR, VARCHAR) |
| Q058 | `SELECT binary_data FROM dqe_test_all_types WHERE binary_data IS NOT NULL LIMIT 5` | binary → VARBINARY |
| Q059 | `SELECT big_number FROM dqe_test_all_types WHERE big_number IS NOT NULL LIMIT 5` | unsigned_long → DECIMAL(20,0) |
| Q060 | `SELECT id, name FROM dqe_test_all_types WHERE name.keyword = 'exact match' LIMIT 5` | text multi-field: .keyword sub-field |

### 4.4 ORDER BY and LIMIT (20 queries)

**Operator selection rule under test** (from task A-7):
- `ORDER BY` without `LIMIT` → SortOperator (full in-memory sort)
- `ORDER BY` + `LIMIT N` → TopNOperator (bounded priority queue of size N)
- `LIMIT N` without `ORDER BY` → LimitOperator (truncate after N rows)

Queries Q114–Q118 are adversarial: the top-N rows by sort order are NOT present
in the first few batches or the first shard. A "take first N rows seen"
implementation will produce wrong results on these queries. The test dataset
`dqe_test_sharded` has 50,000 docs across 5 shards with `id` values `id_00001`
to `id_50000` and `amount` values that are inversely correlated with `id` order
(highest amounts have the highest ids). This ensures the top-N by `amount DESC`
are in the last shard by `id` order.

| # | Query | Validates |
|---|---|---|
| Q061 | `SELECT id, count_int FROM dqe_test_all_types ORDER BY count_int ASC LIMIT 10` | Single column ascending |
| Q062 | `SELECT id, count_int FROM dqe_test_all_types ORDER BY count_int DESC LIMIT 10` | Single column descending |
| Q063 | `SELECT id, status, count_int FROM dqe_test_all_types ORDER BY status ASC, count_int DESC LIMIT 10` | Multi-column sort |
| Q064 | `SELECT id, count_int FROM dqe_test_nulls ORDER BY int_val ASC NULLS FIRST LIMIT 10` | NULLS FIRST |
| Q065 | `SELECT id, count_int FROM dqe_test_nulls ORDER BY int_val ASC NULLS LAST LIMIT 10` | NULLS LAST |
| Q066 | `SELECT id FROM dqe_test_all_types ORDER BY id LIMIT 5 OFFSET 10` | LIMIT with OFFSET |
| Q067 | `SELECT id FROM dqe_test_all_types ORDER BY created_at DESC LIMIT 10` | Sort by timestamp |
| Q068 | `SELECT id FROM dqe_test_all_types ORDER BY price_double ASC LIMIT 10` | Sort by double |
| Q069 | `SELECT id, status FROM dqe_test_all_types WHERE status = 'active' ORDER BY count_int DESC LIMIT 10` | Filter + sort + limit |
| Q070 | `SELECT id FROM dqe_test_all_types ORDER BY id LIMIT 10000` | Large limit |
| Q071 | `SELECT id FROM dqe_test_all_types ORDER BY id LIMIT 1` | Limit 1 (TopN optimization) |
| Q072 | `SELECT id FROM dqe_test_all_types ORDER BY description ASC LIMIT 5` | Sort on text with fielddata |
| Q073 | `SELECT id, price_scaled FROM dqe_test_all_types ORDER BY price_scaled DESC LIMIT 5` | Sort on scaled_float |
| Q074 | `SELECT id, big_number FROM dqe_test_all_types ORDER BY big_number ASC LIMIT 5` | Sort on unsigned_long |
| Q075 | `SELECT id FROM dqe_test_sharded ORDER BY id ASC` | Full sort across 5 shards (no limit) → SortOperator |
| Q114 | `SELECT id, amount FROM dqe_test_sharded ORDER BY amount DESC LIMIT 5` | **Adversarial TopN**: top-5 by amount are in last shard by id. "First 5 seen" gives wrong result |
| Q115 | `SELECT id, amount FROM dqe_test_sharded ORDER BY amount ASC LIMIT 5` | **Adversarial TopN**: bottom-5 by amount are in first shard by id. Verifies ASC direction |
| Q116 | `SELECT id, amount FROM dqe_test_sharded ORDER BY amount DESC LIMIT 5 OFFSET 10` | **Adversarial TopN+Offset**: top 15 needed, return rows 11-15 |
| Q117 | `SELECT id, amount FROM dqe_test_sharded ORDER BY amount DESC LIMIT 1` | **Adversarial TopN single row**: the single highest-amount row must be correct |
| Q118 | `SELECT id FROM dqe_test_sharded WHERE category = 'cat_50' ORDER BY id DESC LIMIT 10` | **Adversarial TopN + filter**: top-10 after filter, filtered rows scattered across shards |

Test case JSON for Q114 (primary adversarial test):

```json
{
  "id": "Q114",
  "name": "adversarial_topn_desc",
  "query": "SELECT id, amount FROM dqe_test_sharded ORDER BY amount DESC LIMIT 5",
  "engine": "dqe",
  "expect": {
    "status": 200,
    "row_count": 5,
    "ordered": true,
    "data": [
      ["id_50000", 99999.99],
      ["id_49999", 99998.99],
      ["id_49998", 99997.99],
      ["id_49997", 99996.99],
      ["id_49996", 99995.99]
    ]
  }
}
```

This test has an **exact expected result** with specific row values. If the
TopNOperator takes the first 5 rows seen (from the first shard, which has the
lowest amounts), the test fails because the returned rows will have low amounts
instead of the expected high amounts.

### 4.5 Multi-Shard Correctness (13 queries)

**Split invariant under test**: `dqe_test_sharded` has 5 primary shards and 1 replica.
Every document has a globally unique `id`. Tests Q076–Q078 return the full dataset
(limit >= doc count) and the validator asserts: (a) `row_count` equals the known
doc count, and (b) all `id` values are unique (no duplicates from double-scanning
primary + replica). These tests will fail if the split scheduler produces >5 splits
or scans both copies of any shard.

| # | Query | Validates |
|---|---|---|
| Q076 | `SELECT id FROM dqe_test_sharded WHERE id BETWEEN 'id_00001' AND 'id_00100' ORDER BY id ASC` | Range scan across shards, deterministic subset |
| Q077 | `SELECT id FROM dqe_test_sharded ORDER BY id ASC LIMIT 50000` | Full scan across 5 shards, deterministic order |
| Q078 | `SELECT id FROM dqe_test_sharded WHERE id >= 'id_25000' ORDER BY id ASC` | Filter + sort across shards |
| Q079 | `SELECT id FROM dqe_test_sharded ORDER BY id DESC LIMIT 100` | Reverse sort across shards |
| Q080 | `SELECT * FROM dqe_test_sharded WHERE id = 'id_00001'` | Point lookup, single row from one shard |
| Q081 | `SELECT id FROM dqe_test_sharded LIMIT 100` | Limit with arbitrary shard order |
| Q082 | `SELECT id FROM dqe_test_sharded LIMIT 100 OFFSET 49900` | Late offset across shards |
| Q083 | `SELECT * FROM dqe_test_conflict_a LIMIT 5` | Index A type resolution |
| Q084 | `SELECT * FROM dqe_test_conflict_b LIMIT 5` | Index B type resolution |
| Q085 | `SELECT id, value FROM dqe_test_conflict_* ORDER BY id LIMIT 10` | Index pattern with type widening |
| Q111 | `SELECT id FROM dqe_test_sharded ORDER BY id ASC` | **Uniqueness assertion**: row_count = 50000, all ids unique, no duplicates |
| Q112 | `SELECT id, category, amount FROM dqe_test_sharded ORDER BY id ASC` | **Uniqueness + completeness**: row_count = 50000, all ids unique, multi-column |
| Q113 | `SELECT id FROM dqe_test_sharded WHERE category = 'cat_01' ORDER BY id ASC` | **Filtered uniqueness**: all returned ids unique, row_count matches expected |

Test case JSON for Q111 (the primary duplication guard):

```json
{
  "id": "Q111",
  "name": "full_scan_uniqueness_assertion",
  "query": "SELECT id FROM dqe_test_sharded ORDER BY id ASC",
  "engine": "dqe",
  "expect": {
    "status": 200,
    "row_count": 50000,
    "unique_column": "id",
    "ordered": true
  }
}
```

The `unique_column` field in the expect block tells `validate.py` to assert that
all values in the named column are distinct. If any `id` appears twice, the test
fails with: `FAIL: duplicate id values found: [list of duplicates]`.

### 4.6 Expression Evaluation (10 queries)

| # | Query | Validates |
|---|---|---|
| Q086 | `SELECT id, count_int * 2 + 1 AS computed FROM dqe_test_all_types LIMIT 5` | Arithmetic chain |
| Q087 | `SELECT id, -count_int AS negated FROM dqe_test_all_types LIMIT 5` | Unary negation |
| Q088 | `SELECT id, count_int % 3 AS modulo FROM dqe_test_all_types LIMIT 5` | Modulo operator |
| Q089 | `SELECT id, CASE WHEN count_int > 100 THEN 'high' WHEN count_int > 50 THEN 'medium' ELSE 'low' END AS level FROM dqe_test_all_types LIMIT 10` | CASE expression |
| Q090 | `SELECT id, CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END AS status_code FROM dqe_test_all_types LIMIT 10` | Simple CASE |
| Q091 | `SELECT id, CASE WHEN is_active THEN 'yes' ELSE 'no' END AS active_label FROM dqe_test_all_types LIMIT 5` | CASE as conditional (replaces IF) |
| Q092 | `SELECT id, TRY_CAST(status AS INTEGER) FROM dqe_test_all_types LIMIT 5` | TRY_CAST (returns NULL on failure) |
| Q093 | `SELECT id, NULLIF(count_int, 0) AS non_zero, COALESCE(str_val, bool_val, 'fallback') FROM dqe_test_nulls LIMIT 10` | NULLIF + COALESCE combined |
| Q094 | `SELECT id, COALESCE(int_val, -1) AS val FROM dqe_test_nulls LIMIT 10` | COALESCE with NULL |
| Q095 | `SELECT id, count_int FROM dqe_test_all_types WHERE count_int BETWEEN 10 AND 100 AND status IN ('active', 'pending') ORDER BY count_int DESC LIMIT 20` | Combined predicates |

### 4.7 Error Cases (15 queries)

| # | Query | Expected Error | Validates |
|---|---|---|---|
| Q096 | `SELECT * FROM nonexistent_index` | Table not found | Missing index |
| Q097 | `SELECT nonexistent_column FROM dqe_test_all_types` | Column not found | Missing column |
| Q098 | `SELECT id FROM dqe_test_all_types GROUP BY id` | DQE does not support GROUP BY | Unsupported construct |
| Q099 | `SELECT id FROM dqe_test_all_types a JOIN dqe_test_nulls b ON a.id = b.id` | DQE does not support JOIN | Unsupported construct |
| Q100 | `SELECT ROW_NUMBER() OVER () FROM dqe_test_all_types` | DQE does not support window functions | Unsupported construct |
| Q101 | `SELECT CAST('abc' AS INTEGER) FROM dqe_test_all_types LIMIT 1` | CAST failure | Runtime type error |
| Q102 | `THIS IS NOT SQL` | Parsing error | Syntax error with line/column |
| Q103 | `SELECT id FROM dqe_test_all_types ORDER BY location` | Cannot sort on geo_point | Unsortable type |
| Q104 | `SELECT COUNT(*) FROM dqe_test_all_types` | DQE does not support aggregate functions | Unsupported construct |
| Q105 | `SELECT id FROM dqe_test_all_types WHERE count_int = 'not_a_number'` | Type mismatch | Type error in predicate |
| Q106 | `SELECT DISTINCT status FROM dqe_test_all_types` | DQE does not support DISTINCT | Unsupported construct |
| Q107 | `SELECT 1 + 2 AS constant` | DQE requires a FROM clause | Unsupported: no FROM |
| Q108 | `SELECT id, CONCAT(status, '_suffix') FROM dqe_test_all_types LIMIT 5` | DQE does not support function CONCAT | Unsupported: named function |
| Q109 | `SELECT id, LENGTH(status) FROM dqe_test_all_types LIMIT 5` | DQE does not support function LENGTH | Unsupported: named function |
| Q110 | `SELECT id, IF(is_active, 'yes', 'no') FROM dqe_test_all_types LIMIT 5` | DQE does not support function IF | Unsupported: named function |

---

## 5. NULL Conformance Test Suite (52 cases)

All tests run against `dqe_test_nulls` dataset. Each test verifies the exact output including NULL positions.

### 5.1 NULL in Arithmetic (10 cases)

| # | Expression | Expected Result |
|---|---|---|
| N01 | `NULL + 1` | `NULL` |
| N02 | `1 + NULL` | `NULL` |
| N03 | `NULL * 0` | `NULL` |
| N04 | `NULL - NULL` | `NULL` |
| N05 | `NULL / 1` | `NULL` |
| N06 | `NULL % 2` | `NULL` |
| N07 | `int_val + 1` (where int_val is NULL) | `NULL` |
| N08 | `int_val * dbl_val` (where int_val is NULL) | `NULL` |
| N09 | `-NULL` | `NULL` |
| N10 | `CAST(NULL AS INTEGER) + 1` | `NULL` |

### 5.2 NULL in Comparison (10 cases)

| # | Expression | Expected Result |
|---|---|---|
| N11 | `NULL = NULL` | `NULL` (not TRUE) |
| N12 | `NULL != NULL` | `NULL` (not TRUE) |
| N13 | `NULL = 1` | `NULL` |
| N14 | `NULL != 1` | `NULL` |
| N15 | `NULL < 1` | `NULL` |
| N16 | `NULL > 1` | `NULL` |
| N17 | `NULL <= 1` | `NULL` |
| N18 | `NULL >= 1` | `NULL` |
| N19 | `1 = NULL` | `NULL` |
| N20 | `NULL IS NULL` | `TRUE` |

### 5.3 NULL in Logical Operators (10 cases)

| # | Expression | Expected Result |
|---|---|---|
| N21 | `NULL AND TRUE` | `NULL` |
| N22 | `NULL AND FALSE` | `FALSE` |
| N23 | `NULL AND NULL` | `NULL` |
| N24 | `NULL OR TRUE` | `TRUE` |
| N25 | `NULL OR FALSE` | `NULL` |
| N26 | `NULL OR NULL` | `NULL` |
| N27 | `NOT NULL` | `NULL` |
| N28 | `TRUE AND NULL` | `NULL` |
| N29 | `FALSE OR NULL` | `NULL` |
| N30 | `NOT (NULL AND TRUE)` | `NULL` |

### 5.4 NULL in String/Predicate Operations (2 cases)

| # | Expression | Expected Result |
|---|---|---|
| N31 | `CAST(NULL AS VARCHAR)` | `NULL` |
| N32 | `NULL LIKE '%test%'` | `NULL` |

Note: `CONCAT(NULL, ...)` and `LENGTH(NULL)` are deferred to Phase 2 (function library).

### 5.5 NULL in CAST (5 cases)

| # | Expression | Expected Result |
|---|---|---|
| N36 | `CAST(NULL AS INTEGER)` | `NULL` |
| N37 | `CAST(NULL AS VARCHAR)` | `NULL` |
| N38 | `CAST(NULL AS BOOLEAN)` | `NULL` |
| N39 | `CAST(NULL AS DOUBLE)` | `NULL` |
| N40 | `CAST(NULL AS TIMESTAMP)` | `NULL` |

### 5.6 NULL in CASE (5 cases)

| # | Expression | Expected Result |
|---|---|---|
| N41 | `CASE WHEN NULL THEN 'a' ELSE 'b' END` | `'b'` |
| N42 | `CASE WHEN TRUE THEN NULL ELSE 'b' END` | `NULL` |
| N43 | `CASE WHEN FALSE THEN 'a' END` | `NULL` (no ELSE) |
| N44 | `CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END` | `'no match'` (NULL != NULL) |
| N45 | `CASE WHEN int_val IS NULL THEN 'null' ELSE 'not null' END` | Correct per row |

### 5.7 NULL in Set Operations (5 cases)

| # | Expression | Expected Result |
|---|---|---|
| N46 | `NULL IN (1, 2, 3)` | `NULL` |
| N47 | `1 IN (NULL, 2, 3)` | `NULL` (if 1 not in {2,3}) or `TRUE` (if 1 in set) |
| N48 | `1 IN (NULL, 1, 3)` | `TRUE` |
| N49 | `NULL BETWEEN 1 AND 10` | `NULL` |
| N50 | `5 BETWEEN NULL AND 10` | `NULL` |

### 5.8 NULL in ORDER BY and Functions (5 cases)

| # | Query/Expression | Expected Result |
|---|---|---|
| N51 | `ORDER BY int_val ASC NULLS FIRST` | NULLs appear first |
| N52 | `ORDER BY int_val ASC NULLS LAST` | NULLs appear last |
| N53 | `COALESCE(NULL, NULL, 'c')` | `'c'` |
| N54 | `COALESCE(NULL, 'b', 'c')` | `'b'` |
| N55 | `NULLIF(1, 1)` | `NULL` |

---

## 6. Type Coercion Test Suite (30 cases)

### 6.1 Implicit Widening (12 cases)

| # | Expression | Input Types | Expected Result Type |
|---|---|---|---|
| C01 | `count_byte + count_short` | TINYINT + SMALLINT | SMALLINT (or wider) |
| C02 | `count_short + count_int` | SMALLINT + INTEGER | INTEGER |
| C03 | `count_int + count_long` | INTEGER + BIGINT | BIGINT |
| C04 | `price_float + price_double` | REAL + DOUBLE | DOUBLE |
| C05 | `count_int + price_double` | INTEGER + DOUBLE | DOUBLE |
| C06 | `count_int * 1.5` | INTEGER + DECIMAL | DECIMAL |
| C07 | `count_long + 1` | BIGINT + INTEGER literal | BIGINT |
| C08 | `price_float + 0.0` | REAL + DOUBLE literal | DOUBLE |
| C09 | `WHERE count_int = count_long` | INTEGER vs BIGINT comparison | Valid (widened) |
| C10 | `WHERE count_int > price_double` | INTEGER vs DOUBLE comparison | Valid (widened) |
| C11 | `COALESCE(count_int, count_long)` | INTEGER, BIGINT | BIGINT |
| C12 | `CASE WHEN true THEN count_int ELSE count_long END` | INTEGER, BIGINT | BIGINT |

### 6.2 Explicit CAST (10 cases)

| # | Expression | Expected |
|---|---|---|
| C13 | `CAST(count_int AS BIGINT)` | Succeeds, BIGINT |
| C14 | `CAST(count_long AS INTEGER)` | Succeeds if value fits, error if overflow |
| C15 | `CAST(count_int AS VARCHAR)` | Succeeds, string representation |
| C16 | `CAST('123' AS INTEGER)` | Succeeds, integer 123 |
| C17 | `CAST(price_double AS INTEGER)` | Succeeds, truncated |
| C18 | `CAST(count_int AS DOUBLE)` | Succeeds, double |
| C19 | `CAST(is_active AS VARCHAR)` | Succeeds, 'true'/'false' |
| C20 | `CAST('true' AS BOOLEAN)` | Succeeds |
| C21 | `CAST(created_at AS VARCHAR)` | Succeeds, ISO format string |
| C22 | `CAST(price_scaled AS DOUBLE)` | Succeeds |

### 6.3 CAST Failures (8 cases)

| # | Expression | Expected Error |
|---|---|---|
| C23 | `CAST('abc' AS INTEGER)` | Invalid cast |
| C24 | `CAST('abc' AS DOUBLE)` | Invalid cast |
| C25 | `CAST('abc' AS BOOLEAN)` | Invalid cast |
| C26 | `CAST(99999999999999999999 AS INTEGER)` | Overflow |
| C27 | `CAST(count_int AS geo_point)` | Invalid target type |
| C28 | `TRY_CAST('abc' AS INTEGER)` | Returns NULL (no error) |
| C29 | `TRY_CAST('abc' AS DOUBLE)` | Returns NULL (no error) |
| C30 | `CAST('' AS INTEGER)` | Invalid cast |

---

## 7. Timezone Conformance Test Suite (12 cases)

Note: `EXTRACT()` is a temporal function and is deferred to Phase 2 (dqe-functions module).
Phase 1 timezone testing validates timestamp precision, comparison, ordering, CAST, and
multi-format handling — all using Phase 1 expression constructs only.

### 7.1 Timestamp Precision (5 cases)

| # | Query | Validates |
|---|---|---|
| TZ01 | `SELECT created_at FROM dqe_test_all_types WHERE created_at IS NOT NULL LIMIT 1` | date (epoch_millis) returns TIMESTAMP(3) with ms precision |
| TZ02 | `SELECT precise_time FROM dqe_test_all_types WHERE precise_time IS NOT NULL LIMIT 1` | date_nanos returns TIMESTAMP(9) with ns precision |
| TZ03 | `SELECT custom_date FROM dqe_test_all_types WHERE custom_date IS NOT NULL LIMIT 1` | Custom format `yyyy/MM/dd HH:mm:ss` parsed correctly |
| TZ04 | `SELECT updated_at FROM dqe_test_all_types WHERE updated_at = TIMESTAMP '2024-06-15 10:30:00'` | strict_date_optional_time comparison |
| TZ05 | `SELECT created_at FROM dqe_test_all_types WHERE created_at > TIMESTAMP '2024-01-01 00:00:00' AND created_at < TIMESTAMP '2024-12-31 23:59:59'` | Range filter on timestamp |

### 7.2 Timestamp Comparison and Ordering (7 cases)

Tests use a dataset with timestamps spanning a DST transition (e.g., US Eastern: 2024-03-10 01:00 → 2024-03-10 03:00).

| # | Query | Validates |
|---|---|---|
| TZ06 | `SELECT id, created_at FROM dqe_test_all_types ORDER BY created_at ASC LIMIT 20` | Correct ordering through DST transition |
| TZ07 | `SELECT id FROM dqe_test_all_types WHERE created_at BETWEEN TIMESTAMP '2024-03-10 00:00:00' AND TIMESTAMP '2024-03-10 04:00:00'` | Range spanning DST gap |
| TZ08 | `SELECT id FROM dqe_test_all_types WHERE precise_time > TIMESTAMP '2024-01-01 00:00:00.123456789'` | Nanosecond precision comparison |
| TZ09 | `SELECT id, CAST(created_at AS VARCHAR) FROM dqe_test_all_types WHERE created_at IS NOT NULL LIMIT 5` | Timestamp-to-string CAST |
| TZ10 | `SELECT id, CAST('2024-03-10T02:30:00Z' AS TIMESTAMP) AS parsed FROM dqe_test_all_types LIMIT 1` | String-to-timestamp CAST |
| TZ11 | `SELECT id, created_at, custom_date FROM dqe_test_all_types WHERE created_at IS NOT NULL AND custom_date IS NOT NULL ORDER BY created_at LIMIT 5` | Multiple date formats in same query |
| TZ12 | `SELECT id FROM dqe_test_all_types WHERE created_at IS NULL ORDER BY id LIMIT 10` | NULL timestamps in filter + sort |

---

## 8. Differential Test Plan

### 8.1 Approach

Differential tests compare DQE output against standalone Trino with the OpenSearch connector. Both systems query the same OpenSearch data. The test runner is a script that calls both REST APIs and diffs the results.

**Infrastructure**:
- OpenSearch cluster running at `localhost:9200` (started via `./gradlew :dqe-plugin:run`)
- Standalone Trino started via Docker: `docker run -d --name trino-diff -p 8080:8080 trinodb/trino:479`
- Trino OpenSearch connector configured to point at the test OpenSearch cluster (catalog properties mounted via Docker volume)
- Same datasets from Section 2 are accessible to both DQE and Trino

**Trino connector configuration** (`scripts/dqe-test/trino/opensearch.properties`):

```properties
connector.name=opensearch
opensearch.host=host.docker.internal
opensearch.port=9200
opensearch.security=NONE
```

### 8.2 Test Case Format

Differential test cases are stored as JSON files under `scripts/dqe-test/cases/differential/`. Each file extends the standard test case format with a `differential` flag:

```json
{
  "id": "D001",
  "name": "basic_select_differential",
  "query": "SELECT id, status, count_int FROM dqe_test_all_types ORDER BY id LIMIT 10",
  "differential": true,
  "tolerance": {
    "float_epsilon": 1e-10,
    "timestamp_precision_ms": true
  }
}
```

When `differential: true`, the runner does not check against a static `expect` block. Instead it executes the query against both engines and compares their outputs.

### 8.3 Queries Included

All queries from Sections 4.1–4.6 (Q001–Q095) that are expressible in both DQE and standalone Trino. Excluded:
- Error cases (Q096–Q105) — error formats differ between engines
- Queries using DQE-specific syntax or functions
- Queries on types that the Trino OpenSearch connector does not support (dense_vector, flattened)

**Estimated differential query count**: ~80 queries.

### 8.4 Test Runner

`scripts/dqe-test/run-differential-tests.sh`:

```bash
#!/bin/bash
# Usage: ./scripts/dqe-test/run-differential-tests.sh [os_host:port] [trino_host:port]
OS_HOST=${1:-localhost:9200}
TRINO_HOST=${2:-localhost:8080}

for case_file in $(find scripts/dqe-test/cases/differential -name '*.json' | sort); do
  QUERY=$(jq -r '.query' "$case_file")
  CASE_ID=$(jq -r '.id' "$case_file")

  # 1. Execute via DQE
  DQE_RESPONSE=$(curl -s -X POST "http://$OS_HOST/_plugins/_sql" \
    -H 'Content-Type: application/json' \
    -d "{\"query\": \"$QUERY\", \"engine\": \"dqe\"}")

  # 2. Execute via standalone Trino
  TRINO_RESPONSE=$(curl -s -X POST "http://$TRINO_HOST/v1/statement" \
    -H 'X-Trino-User: test' -H 'X-Trino-Catalog: opensearch' \
    -H 'X-Trino-Schema: default' -d "$QUERY")
  # Poll Trino nextUri until results are complete
  TRINO_RESULT=$(scripts/dqe-test/trino-poll.sh "$TRINO_RESPONSE")

  # 3. Normalize and compare
  RESULT=$(scripts/dqe-test/diff-results.py "$case_file" "$DQE_RESPONSE" "$TRINO_RESULT")
  # ... report pass/fail
done
```

The `scripts/dqe-test/diff-results.py` script:
1. Extracts column types from both responses and maps them to a common type system.
2. Extracts row data from both responses.
3. Sorts both result sets by all columns (unless the query has an explicit `ORDER BY`).
4. Compares row-by-row with configurable tolerance: floating-point epsilon (default 1e-10), timestamp precision differences (ms vs us for `date` type).
5. Reports per-row diff on mismatch.

### 8.5 Execution

Requires Docker for standalone Trino. Runs as a separate CI job.

```bash
# Start Trino with OpenSearch connector
docker run -d --name trino-diff -p 8080:8080 \
  -v $(pwd)/scripts/dqe-test/trino:/etc/trino/catalog \
  trinodb/trino:479

# Wait for Trino to start
scripts/dqe-test/wait-for-trino.sh localhost:8080

# Run differential tests
scripts/dqe-test/run-differential-tests.sh localhost:9200 localhost:8080

# Cleanup
docker rm -f trino-diff
```

---

## 9. Performance Test Plan

### 9.1 Environment

| Parameter | Value |
|---|---|
| Cluster | 3 data nodes + 1 coordinating node |
| Hardware per node | 8 vCPU, 32GB RAM, SSD storage |
| JVM heap | 16GB per node |
| Index | `dqe_test_perf_100m` (100M docs, 5 primary shards, 1 replica) |
| Doc size | ~500 bytes average |
| Index size | ~50GB total |

### 9.2 Baseline Measurement

Run an OpenSearch Rally mixed workload track for 10 minutes with NO DQE queries. Record:
- P50, P90, P95, P99 search latency
- Indexing throughput (docs/sec)
- CPU utilization per node

### 9.3 DQE Load Test

Run the same Rally workload track concurrently with DQE queries:
- 10 concurrent DQE scan queries, each: `SELECT id, category, amount FROM dqe_test_perf_100m WHERE category = '<random>' ORDER BY amount DESC LIMIT 1000`
- DQE queries run in a loop with 1s pause between completions
- Duration: 10 minutes

### 9.4 Pass Criteria

| Metric | Threshold |
|---|---|
| P99 search latency increase | ≤ 20% vs baseline |
| P95 search latency increase | ≤ 15% vs baseline |
| Indexing throughput decrease | ≤ 10% vs baseline |
| DQE query completion rate | 100% (no failures) |
| DQE query P99 latency | < 10s |

### 9.5 Test Runner

`scripts/dqe-test/run-performance-tests.sh`:

```bash
#!/bin/bash
# Usage: ./scripts/dqe-test/run-performance-tests.sh [host:port] [duration_minutes]
HOST=${1:-localhost:9200}
DURATION=${2:-10}

echo "=== Phase 1: Baseline (search-only, no DQE) ==="
opensearchpy rally --track=scripts/dqe-test/data/generators/dqe-phase1-track.json \
  --target-hosts=$HOST --test-mode --duration=$DURATION \
  --report-file=scripts/dqe-test/results/perf_baseline.json

echo "=== Phase 2: Mixed (search + 10 concurrent DQE queries) ==="
# Start DQE load in background
scripts/dqe-test/dqe-load-driver.sh $HOST 10 $DURATION &
DQE_PID=$!

# Run same Rally workload concurrently
opensearchpy rally --track=scripts/dqe-test/data/generators/dqe-phase1-track.json \
  --target-hosts=$HOST --test-mode --duration=$DURATION \
  --report-file=scripts/dqe-test/results/perf_mixed.json

wait $DQE_PID

echo "=== Comparing results ==="
scripts/dqe-test/compare-perf.py \
  scripts/dqe-test/results/perf_baseline.json \
  scripts/dqe-test/results/perf_mixed.json
```

The `scripts/dqe-test/dqe-load-driver.sh` script runs 10 concurrent DQE queries in a loop, each via `curl` to `POST /_plugins/_sql` with `"engine": "dqe"`, with 1s pause between completions. All query latencies are logged to `scripts/dqe-test/results/perf_dqe_latencies.csv`.

The `scripts/dqe-test/compare-perf.py` script reads both Rally reports, computes the delta for P50/P90/P95/P99 search latency and indexing throughput, and reports pass/fail against the thresholds from §9.4.

---

## 10. Memory Safety Test Plan

### 10.1 Test Configuration

| Parameter | Value |
|---|---|
| Cluster | Single node, 16GB heap |
| DQE breaker limit | 20% (3.2GB) |
| Per-query memory | 256MB (default) |
| Index | `dqe_test_scale` (1M docs) |

### 10.2 Test Scenario

1. Submit 10 concurrent DQE queries, each scanning the full 1M-doc index:
   ```sql
   SELECT * FROM dqe_test_scale ORDER BY id ASC LIMIT 10000
   ```
2. All 10 queries scan all rows (sort requires full materialization for ORDER BY without pushdown).
3. Wait for all 10 queries to complete.

### 10.3 Pass Criteria

| Criterion | Threshold |
|---|---|
| CircuitBreakingException | None (0 breaker trips) |
| All queries complete successfully | 10/10 |
| Peak DQE memory (dqe.memory.used_bytes) | ≤ 3.2GB |
| Per-query peak memory | ≤ 256MB |
| No OOM errors in logs | True |

### 10.4 Memory Leak Detection

After all queries complete:
1. Check `dqe.memory.used_bytes` via `GET _plugins/_sql/_dqe/stats` — must be 0 (all memory released).
2. Check `_nodes/stats/breaker` — DQE breaker estimated size must be 0.
3. Optionally: trigger heap dump via `kill -3` and compare against pre-test baseline for DQE object retention.

### 10.5 Test Runner

`scripts/dqe-test/run-memory-tests.sh`:

```bash
#!/bin/bash
HOST=${1:-localhost:9200}
CONCURRENCY=10

# Record baseline memory
BASELINE=$(curl -s "http://$HOST/_plugins/_sql/_dqe/stats" | jq '.memory.used_bytes')

# Launch 10 concurrent queries
for i in $(seq 1 $CONCURRENCY); do
  curl -s -X POST "http://$HOST/_plugins/_sql" \
    -H 'Content-Type: application/json' \
    -d '{"query": "SELECT * FROM dqe_test_scale ORDER BY id ASC LIMIT 10000", "engine": "dqe"}' \
    > scripts/dqe-test/results/memory_query_$i.json &
done
wait

# Check all queries succeeded
FAILURES=0
for i in $(seq 1 $CONCURRENCY); do
  STATUS=$(jq -r '.stats.state' scripts/dqe-test/results/memory_query_$i.json)
  if [ "$STATUS" != "FINISHED" ]; then FAILURES=$((FAILURES+1)); fi
done

# Check memory released
sleep 2
AFTER=$(curl -s "http://$HOST/_plugins/_sql/_dqe/stats" | jq '.memory.used_bytes')

echo "Queries failed: $FAILURES (expect 0)"
echo "Memory after: $AFTER bytes (expect 0)"
echo "Baseline was: $BASELINE bytes"
```

---

## 11. Cancellation Test Plan

### 11.1 Cancel via _tasks API

1. Submit a long-running DQE query (scan 1M-doc index with no LIMIT).
2. Wait for query to start execution (poll `dqe.queries.active` > 0).
3. Cancel via `POST _tasks/<taskId>/_cancel`.
4. Verify within 5 seconds:
   - Query status transitions to CANCELLED.
   - `dqe.memory.used_bytes` returns to pre-query level.
   - PIT is released (no lingering PITs in `_search/point_in_time/_all`).
   - No orphaned exchange buffers.

### 11.2 Cancel via REST DELETE (Async)

1. Submit a long-running query via `/_plugins/_sql/_async` with `"engine": "dqe"`.
2. Get the `queryId` from the response.
3. Cancel via `DELETE /_plugins/_sql/_async/<queryId>`.
4. Same verification as §11.1.

### 11.3 Cancel via Query Timeout

1. Submit a DQE query with `"session_properties": {"query_timeout": "2s"}`.
2. Query scans a large index (takes >2s).
3. Verify query fails with timeout error within 2s + 1s tolerance.
4. Same resource cleanup verification as §11.1.

### 11.4 Cancellation During Different Phases

| # | Phase | How to trigger |
|---|---|---|
| C1 | During shard scan (mid-pagination) | Cancel while scanning 1M docs |
| C2 | During sort (accumulation phase) | Cancel while SortOperator accumulates pages |
| C3 | During exchange (gather phase) | Cancel while coordinator gathers results from shards |

### 11.5 Pass Criteria

| Criterion | Threshold |
|---|---|
| Time from cancel request to CANCELLED status | ≤ 5s |
| Memory fully released | `dqe.memory.used_bytes` returns to 0 |
| PITs released | No lingering PITs for cancelled query |
| No thread leaks | `dqe_worker` active count returns to pre-query level |
| No spill file leaks | Spill directory empty after cleanup |

### 11.6 Test Runner

`scripts/dqe-test/run-cancellation-tests.sh`:

```bash
#!/bin/bash
HOST=${1:-localhost:9200}

# Record baseline state
BASELINE_MEM=$(curl -s "http://$HOST/_plugins/_sql/_dqe/stats" | jq '.memory.used_bytes')
BASELINE_PITS=$(curl -s "http://$HOST/_search/point_in_time/_all" | jq '.pits | length')

# --- Test 1: Cancel via _tasks API ---
# Submit long-running query in background (no LIMIT = full scan)
curl -s -X POST "http://$HOST/_plugins/_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM dqe_test_scale ORDER BY id", "engine": "dqe"}' &
QUERY_PID=$!
sleep 2

# Find and cancel the DQE task
TASK_ID=$(curl -s "http://$HOST/_tasks?actions=*dqe*&detailed" | jq -r '.nodes[].tasks | to_entries[0].key')
curl -s -X POST "http://$HOST/_tasks/$TASK_ID/_cancel"

# Wait and verify cleanup
sleep 5
AFTER_MEM=$(curl -s "http://$HOST/_plugins/_sql/_dqe/stats" | jq '.memory.used_bytes')
AFTER_PITS=$(curl -s "http://$HOST/_search/point_in_time/_all" | jq '.pits | length')

echo "Memory released: $BASELINE_MEM -> $AFTER_MEM (expect 0)"
echo "PITs released: $BASELINE_PITS -> $AFTER_PITS (expect equal)"
wait $QUERY_PID 2>/dev/null

# --- Test 2: Cancel via query timeout ---
START=$(date +%s%3N)
RESPONSE=$(curl -s -X POST "http://$HOST/_plugins/_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM dqe_test_scale ORDER BY id", "engine": "dqe", "session_properties": {"query_timeout": "2s"}}')
END=$(date +%s%3N)
ELAPSED=$(( END - START ))

echo "Timeout query elapsed: ${ELAPSED}ms (expect ≤ 3000)"
echo "Error: $(echo $RESPONSE | jq -r '.error.type')"

# Verify cleanup after timeout
sleep 3
TIMEOUT_MEM=$(curl -s "http://$HOST/_plugins/_sql/_dqe/stats" | jq '.memory.used_bytes')
echo "Memory after timeout: $TIMEOUT_MEM (expect 0)"
```

---

## 12. Security Test Plan

### 12.1 Field-Level Security (FLS)

1. Create an index with fields `public_field` and `restricted_field`.
2. Configure FLS role that excludes `restricted_field`.
3. Query via DQE with the restricted user: `SELECT * FROM ...`
4. Verify: `restricted_field` is NOT in the result schema or data.
5. Query: `SELECT restricted_field FROM ...`
6. Verify: error (column not found or access denied, depending on implementation).

### 12.2 Document-Level Security (DLS)

1. Create an index with a `department` field.
2. Configure DLS role that restricts to `department = 'engineering'`.
3. Query via DQE: `SELECT * FROM ...`
4. Verify: only documents where `department = 'engineering'` are returned.
5. Query: `SELECT * FROM ... WHERE department = 'sales'`
6. Verify: empty result (DLS filter takes precedence).

### 12.3 Index-Level Permissions

1. Create two indices: `allowed_index` and `denied_index`.
2. Configure role with read access only to `allowed_index`.
3. Query `allowed_index` → succeeds.
4. Query `denied_index` → `ACCESS_DENIED` error before execution.

### 12.4 DQE Permission Check

1. User WITHOUT `cluster:admin/opensearch/dqe/query` permission.
2. Query via `engine=dqe` → rejected with permission error.
3. User WITH the permission → query succeeds.

### 12.5 Audit Logging

1. Execute a DQE query.
2. Verify the security audit log contains:
   - Query text
   - User identity
   - Indices accessed
   - Execution outcome (success/failure)
   - Timestamp

### 12.6 Test Runner

`scripts/dqe-test/run-security-tests.sh` runs against a cluster with the security plugin enabled. It uses `curl` with `-u user:password` to authenticate as different users and verifies responses. Security test cases are stored in `scripts/dqe-test/cases/security/` using the same JSON format as §4, with an additional `auth` field:

```json
{
  "id": "SEC01",
  "name": "fls_restricted_field_excluded",
  "auth": {"user": "fls_user", "password": "test_password"},
  "query": "SELECT * FROM sec_test_index LIMIT 5",
  "engine": "dqe",
  "expect": {
    "status": 200,
    "schema_excludes": ["restricted_field"]
  }
}
```

---

## 13. CI Pipeline Integration

The full Phase 1 test pipeline runs in CI as follows:

```yaml
# .github/workflows/dqe-phase1-tests.yml (conceptual)
jobs:
  unit-tests:
    steps:
      - run: ./gradlew :dqe-parser:test :dqe-types:test :dqe-metadata:test
                       :dqe-analyzer:test :dqe-execution:test :dqe-exchange:test
                       :dqe-memory:test :dqe-plugin:test

  integration-tests:
    needs: unit-tests
    steps:
      - run: ./gradlew :dqe-plugin:run &
      - run: scripts/dqe-test/wait-for-cluster.sh localhost:9200
      - run: scripts/dqe-test/setup-data.sh
      - run: scripts/dqe-test/run-phase1-tests.sh
      - run: scripts/dqe-test/run-null-tests.sh
      - run: scripts/dqe-test/run-type-coercion-tests.sh
      - run: scripts/dqe-test/run-timezone-tests.sh
      - run: scripts/dqe-test/run-cancellation-tests.sh

  differential-tests:
    needs: unit-tests
    steps:
      - run: ./gradlew :dqe-plugin:run &
      - run: scripts/dqe-test/wait-for-cluster.sh localhost:9200
      - run: scripts/dqe-test/setup-data.sh
      - run: docker run -d --name trino-diff -p 8080:8080 -v ./scripts/dqe-test/trino:/etc/trino/catalog trinodb/trino:479
      - run: scripts/dqe-test/wait-for-trino.sh localhost:8080
      - run: scripts/dqe-test/run-differential-tests.sh

  performance-tests:
    # Runs on dedicated hardware, not per-PR
    steps:
      - run: # Start 3-node cluster (via Rally or dedicated infra)
      - run: scripts/dqe-test/setup-data.sh
      - run: scripts/dqe-test/run-performance-tests.sh
```

**Key principle**: Unit tests (Gradle) validate internal logic. Everything that touches the REST API runs as external scripts against a real cluster — queryable, auditable, and reproducible with `curl`.

---

## 14. Test Count Summary

| Category | Type | Estimated Count |
|---|---|---|
| dqe-parser unit tests | Gradle | 30+ |
| dqe-types unit tests | Gradle | 50+ |
| dqe-metadata unit tests | Gradle | 30+ |
| dqe-analyzer unit tests | Gradle | 50+ |
| dqe-execution unit tests | Gradle | 60+ |
| dqe-exchange unit tests | Gradle | 30+ |
| dqe-memory unit tests | Gradle | 25+ |
| dqe-plugin unit tests | Gradle | 40+ |
| Integration queries (§4) | REST script | 118 |
| NULL conformance (§5) | REST script | 52 |
| Type coercion (§6) | REST script | 30 |
| Timezone conformance (§7) | REST script | 12 |
| Differential tests (§8) | REST script | ~75 |
| Performance tests (§9) | REST script | 1 scenario |
| Memory safety tests (§10) | REST script | 1 scenario |
| Cancellation tests (§11) | REST script | 5 scenarios |
| Security tests (§12) | REST script | 5 scenarios |
| **Total** | | **~600+ test cases** |

### File Structure Summary

```
scripts/dqe-test/
  setup-data.sh                    # Create indices and bulk-index test data
  run-all.sh                       # Run all test suites sequentially
  run-phase1-tests.sh              # Integration test runner
  run-null-tests.sh                # NULL conformance runner
  run-type-coercion-tests.sh       # Type coercion runner
  run-timezone-tests.sh            # Timezone conformance runner
  run-differential-tests.sh        # Differential test runner (DQE vs Trino)
  run-performance-tests.sh         # Performance benchmark runner
  run-memory-tests.sh              # Memory safety runner
  run-cancellation-tests.sh        # Cancellation test runner
  run-security-tests.sh            # Security test runner
  validate.py                      # Compare REST response against expected results
  diff-results.py                  # Compare DQE vs Trino results for differential tests
  compare-perf.py                  # Compare baseline vs mixed performance reports
  dqe-load-driver.sh              # Concurrent DQE query load generator
  wait-for-cluster.sh             # Poll cluster health until green
  wait-for-trino.sh               # Poll Trino readiness
  trino-poll.sh                    # Poll Trino async statement results
  trino/
    opensearch.properties          # Trino OpenSearch connector config
  cases/
    phase1/                        # Integration test cases (JSON)
      basic_select/*.json
      where_predicates/*.json
      type_specific/*.json
      order_by_limit/*.json
      multi_shard/*.json
      expressions/*.json
      error_cases/*.json
    null_conformance/*.json        # NULL conformance test cases
    type_coercion/*.json           # Type coercion test cases
    timezone/*.json                # Timezone conformance test cases
    differential/*.json            # Differential test cases
    security/*.json                # Security test cases
  data/
    mappings/*.json                # Index mappings
    bulk/*.ndjson                  # Bulk data files
    generators/                    # Scale data generators
  results/                         # Test output (gitignored)
```
