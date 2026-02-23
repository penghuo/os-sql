# DQE Phase 1 Test Report

**Date**: 2026-02-23
**Tester**: QA (automated test infrastructure)
**Cluster**: OpenSearch 3.6.0-SNAPSHOT, single node, `opensearch-sql` plugin with DQE
**Reference**: `docs/design/dqe_phase1_test_plan.md`

---

## 1. Executive Summary

| Metric | Value |
|---|---|
| Total test cases | 212 |
| Passed | 25 (11.8%) |
| Failed | 187 (88.2%) |
| Cluster startup | SUCCESS |
| Data loading | SUCCESS (6 of 7 indices; `dqe_test_datatypes` skipped due to `knn_vector` type) |

**Overall verdict**: The DQE engine routes queries and returns data, but two systemic issues cause the vast majority of failures: (1) schema type names are reported in OpenSearch-native format instead of ANSI SQL / Trino format, and (2) column aliases from expressions/AS clauses are not propagated to the response schema. These are presentation-layer issues in the response formatter, not execution correctness bugs. Additionally, the DQE engine does not yet reject unsupported SQL constructs (GROUP BY, JOIN, DISTINCT, aggregate functions, named functions) -- these queries silently fall back to the legacy SQL engine.

---

## 2. Test Infrastructure

### 2.1 Cluster Startup

- **Command**: `./gradlew :opensearch-sql-plugin:run`
- **Result**: SUCCESS -- cluster green in ~20 seconds
- **Version**: OpenSearch 3.6.0-SNAPSHOT
- **Plugin**: `opensearch-sql` loaded (includes DQE)

### 2.2 Data Loading

| Index | Docs | Status | Notes |
|---|---|---|---|
| `dqe_test_employees` | 5 | SUCCESS | Pre-existing test data |
| `dqe_test_all_types` | 100 | SUCCESS | Created without `dense_vector` and `flattened` fields (unsupported types on this cluster) |
| `dqe_test_nulls` | 50 | SUCCESS | Systematic NULL patterns |
| `dqe_test_conflict_a` | 5 | SUCCESS | Type conflict index (long) |
| `dqe_test_conflict_b` | 5 | SUCCESS | Type conflict index (integer) |
| `dqe_test_sharded` | 500 | SUCCESS | 5 shards, adversarial amount distribution |
| `dqe_test_datatypes` | - | SKIPPED | `knn_vector` type not available (no k-NN plugin) |

---

## 3. Results by Suite

### 3.1 Phase 1 Integration Tests (118 cases)

| Category | Pass | Fail | Total | Pass % |
|---|---|---|---|---|
| basic_select (Q001-Q015) | 5 | 10 | 15 | 33.3% |
| where_predicates (Q016-Q040) | 2 | 23 | 25 | 8.0% |
| type_specific (Q041-Q060) | 5 | 15 | 20 | 25.0% |
| order_by_limit (Q061-Q075, Q114-Q118) | 1 | 19 | 20 | 5.0% |
| multi_shard (Q076-Q085, Q111-Q113) | 0 | 13 | 13 | 0.0% |
| expressions (Q086-Q095) | 4 | 6 | 10 | 40.0% |
| error_cases (Q096-Q110) | 0 | 15 | 15 | 0.0% |
| **Subtotal** | **17** | **101** | **118** | **14.4%** |

### 3.2 NULL Conformance (52 cases)

| Pass | Fail | Total | Pass % |
|---|---|---|---|
| 1 | 51 | 52 | 1.9% |

### 3.3 Type Coercion (30 cases)

| Pass | Fail | Total | Pass % |
|---|---|---|---|
| 6 | 24 | 30 | 20.0% |

### 3.4 Timezone (12 cases)

| Pass | Fail | Total | Pass % |
|---|---|---|---|
| 1 | 11 | 12 | 8.3% |

### 3.5 Grand Total

| Pass | Fail | Total | Pass % |
|---|---|---|---|
| 25 | 187 | 212 | 11.8% |

---

## 4. Failure Analysis

### 4.1 Systemic Issue 1: Schema Type Names (affects ~170 tests)

**Observation**: The DQE returns OpenSearch-native type names in the schema instead of ANSI SQL / Trino type names.

| Expected (Trino) | Actual (OpenSearch) |
|---|---|
| `VARCHAR` | `keyword` or `text` |
| `INTEGER` | `integer` |
| `BIGINT` | `long` |
| `SMALLINT` | `short` |
| `TINYINT` | `byte` |
| `DOUBLE` | `double` |
| `REAL` | `float` |
| `BOOLEAN` | `boolean` |
| `TIMESTAMP` | `timestamp` |
| `DECIMAL` | `double` |
| `VARBINARY` | `binary` |
| `STRUCT` | `object` |

**Example**:
- Query: `SELECT id, name FROM dqe_test_all_types LIMIT 10`
- Expected schema: `[{"name":"id","type":"VARCHAR"}, {"name":"name","type":"VARCHAR"}]`
- Actual schema: `[{"name":"id","type":"keyword"}, {"name":"name","type":"text"}]`

**Impact**: This is the single largest cause of test failures. The DQE execution pipeline produces correct data but the response formatter maps types using OpenSearch field type names rather than Trino/ANSI SQL type names.

**Affected tests**: Nearly all tests with schema assertions (Q001-Q095, N01-N55, C01-C22, TZ01-TZ12).

### 4.2 Systemic Issue 2: Column Aliases Not Propagated (affects ~60 tests)

**Observation**: When a query uses `AS alias` or computes an expression, the response schema column name is the raw expression text rather than the alias.

| Query Fragment | Expected Column Name | Actual Column Name |
|---|---|---|
| `count_int + 1 AS incremented` | `incremented` | `count_int + 1` |
| `count_long * price_double` | `_col0` | `count_long * price_double` |
| `NULLIF(int_val, 0)` | `_col1` | `NULLIF(int_val, 0)` |
| `CASE WHEN ... END AS level` | `level` | Full CASE expression text |
| `id AS identifier` | `identifier` | `id` |
| `NULL + 1 AS result` | `result` | `NULL + 1` |

**Impact**: All NULL conformance tests, expression tests, and type coercion tests that use aliases fail on column name matching.

### 4.3 Systemic Issue 3: Unsupported Constructs Not Rejected (affects 15 error cases)

**Observation**: Queries using GROUP BY, JOIN, DISTINCT, aggregate functions, window functions, named functions (CONCAT, LENGTH, IF), and SELECT without FROM do not produce DQE-specific error messages. Instead, they silently fall back to the legacy SQL engine and return success responses.

| Test ID | Query | Expected | Actual |
|---|---|---|---|
| Q098 | `SELECT id FROM dqe_test_all_types GROUP BY id` | Error: "DQE does not support GROUP BY" | Success response (200) |
| Q099 | `SELECT id FROM ... a JOIN ... b ON a.id = b.id` | Error: "DQE does not support JOIN" | Error: "Field name [id] is ambiguous" (legacy engine error) |
| Q100 | `SELECT ROW_NUMBER() OVER () FROM ...` | Error: "window functions" | Success response (200) |
| Q104 | `SELECT COUNT(*) FROM ...` | Error: "aggregate functions" | Success response (200) |
| Q106 | `SELECT DISTINCT status FROM ...` | Error: "DISTINCT" | Success response (200) |
| Q107 | `SELECT 1 + 2 AS constant` | Error: "FROM clause" | Success response (200) |
| Q108 | `CONCAT(status, '_suffix')` | Error: "CONCAT" | Success response (200) |
| Q109 | `LENGTH(status)` | Error: "LENGTH" | Success response (200) |
| Q110 | `IF(is_active, 'yes', 'no')` | Error: "IF" | Success response (200) |

**Root cause**: The `engine: "dqe"` request parameter either does not fully isolate the DQE path, or the DQE analyzer does not yet detect and reject these constructs, falling back to the legacy engine.

### 4.4 Error Message Format Mismatch (affects 6 error cases)

Queries that do produce errors have different message content than expected:

| Test ID | Expected `message_contains` | Actual error reason |
|---|---|---|
| Q096 | `not found` | `Error occurred in OpenSearch engine: no such index [nonexistent_index]` |
| Q097 | `not found` | `Invalid SQL query` |
| Q101 | `CAST` | `Invalid SQL query` |
| Q102 | `Syntax error` | `Invalid SQL query` (reason text: "Query must start with SELECT...") |
| Q105 | `type` | `Invalid SQL query` |
| Q103 | `sort` | `Error occurred in OpenSearch engine: all shards failed` |

### 4.5 SELECT * Column Ordering (affects multi_shard tests)

`SELECT *` returns columns in alphabetical order by field name rather than declaration order. For `dqe_test_sharded` and `dqe_test_conflict_*`, columns come as `[amount, category, id]` or `[label, value, id]` instead of expected `[id, category, amount]` or `[id, value, label]`.

### 4.6 CAST(NULL AS VARCHAR) Returns Empty Result (affects 4 NULL tests)

`CAST(NULL AS VARCHAR)` and `COALESCE(NULL, NULL, 'c')` queries against `dqe_test_nulls` return 0 rows instead of 1 row with a NULL value. This may indicate a WHERE-clause interaction or response serialization issue with all-NULL result rows.

---

## 5. Passing Tests (25 total)

These tests pass because they use CAST expressions that return Trino-normalized type names, or their schema expectations happen to match the actual response:

### Phase 1 Passing (17)

| ID | Name | Category |
|---|---|---|
| Q007 | explicit_cast_bigint | basic_select |
| Q008 | cast_to_string | basic_select |
| Q010 | geo_point_row_access | basic_select |
| Q013 | multiple_cast_targets | basic_select |
| Q014 | coalesce | basic_select |
| Q024 | not_bool_must_not | where_predicates |
| Q039 | unsigned_long_exceeding_bigint | where_predicates |
| Q055 | geo_point_row | type_specific |
| Q056 | geo_shape_varchar | type_specific |
| Q058 | binary_varbinary | type_specific |
| Q059 | unsigned_long_decimal | type_specific |
| Q060 | text_multifield_keyword | type_specific |
| Q074 | sort_unsigned_long | order_by_limit |
| Q087 | unary_negation | expressions |
| Q092 | try_cast_null_on_failure | expressions |
| Q093 | nullif_coalesce_combined | expressions |
| Q094 | coalesce_with_null | expressions |

### NULL Conformance Passing (1)

| ID | Name |
|---|---|
| N09 | negate_null |

### Type Coercion Passing (6)

| ID | Name |
|---|---|
| C11 | coalesce_int_long |
| C12 | case_int_long |
| C13 | cast_int_to_bigint |
| C15 | cast_int_to_varchar |
| C19 | cast_bool_to_varchar |
| C21 | cast_timestamp_to_varchar |

### Timezone Passing (1)

| ID | Name |
|---|---|
| TZ09 | timestamp_to_string_cast |

---

## 6. Recommendations

### 6.1 High Priority (unblocks ~170 tests)

1. **Type name mapping in response formatter**: The DQE response formatter must translate OpenSearch field type names to ANSI SQL / Trino type names before serializing the schema. This is the `DqeType.toSqlTypeName()` mapping: `keyword` -> `VARCHAR`, `integer` -> `INTEGER`, `long` -> `BIGINT`, etc. The `DqeTypes` class already defines these mappings; they need to be wired into the REST response.

### 6.2 Medium Priority (unblocks ~60 tests)

2. **Column alias propagation**: The DQE must propagate `AS alias` names and generate synthetic names for unnamed expressions (e.g., `_col0`, `_col1`) in the response schema instead of echoing the raw SQL expression text.

### 6.3 Medium Priority (unblocks 15 tests)

3. **Unsupported construct rejection**: The DQE analyzer must detect and reject GROUP BY, JOIN, DISTINCT, aggregates, window functions, and named function calls with DQE-specific error messages instead of falling back to the legacy SQL engine.

### 6.4 Low Priority (unblocks 6 tests)

4. **Error message content alignment**: Adjust error response `reason` text to contain expected substrings (`not found`, `Syntax error`, `CAST`, `type`), or update test expectations to match actual error messages.

5. **NULL row serialization**: Investigate why `CAST(NULL AS VARCHAR)` and `COALESCE(NULL, NULL, 'c')` queries return 0 rows instead of 1 row containing NULL.

6. **SELECT * column ordering**: Determine whether column order should follow mapping declaration order or alphabetical order, and align test expectations.

---

## 7. Estimated Impact of Fixes

If the top 2 systemic issues (type name mapping and column alias propagation) are fixed, the estimated pass rate would increase from **25/212 (11.8%)** to approximately **170+/212 (80%+)**. The remaining ~25 failures would be from error case behavior, NULL edge cases, and column ordering issues.

---

## 8. Test Environment Details

```
OpenSearch Version: 3.6.0-SNAPSHOT
Distribution: opensearch
JDK: Amazon Corretto 21.0.8+9-LTS
Heap: 512MB
Cluster: single node (integTest)
Plugin: opensearch-sql (with DQE)
Test Runner: scripts/dqe-test/validate.py
Test Data: 6 indices, 660 total documents
```

---

## 9. Appendix: Full Test Results

Full test output is available at `/tmp/dqe_full_results.txt` on the test runner machine.

### Commands to Reproduce

```bash
# Start cluster
./gradlew :opensearch-sql-plugin:run &

# Wait for cluster
curl -sf http://localhost:9200/_cluster/health?wait_for_status=green

# Load data (manually, skipping dqe_test_datatypes)
scripts/dqe-test/setup-data.sh

# Run test suites
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/phase1/basic_select/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/phase1/where_predicates/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/phase1/type_specific/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/phase1/order_by_limit/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/phase1/multi_shard/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/phase1/expressions/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/phase1/error_cases/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/null_conformance/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/type_coercion/ --verbose
python3 scripts/dqe-test/validate.py --url http://localhost:9200 --cases scripts/dqe-test/cases/timezone/ --verbose
```
