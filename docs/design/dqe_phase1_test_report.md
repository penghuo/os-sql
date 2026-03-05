# DQE Phase 1 Test Report

**Date**: 2026-03-05 (execution wiring run)
**Previous runs**: 2026-02-23 (schema-only), 2026-02-24 (schema-only after fixes)
**Cluster**: OpenSearch 3.6.0-SNAPSHOT, single node, `opensearch-sql` plugin with DQE
**Reference**: `docs/design/dqe_phase1_test_plan.md`

---

## 1. Executive Summary

| Metric | Run 2 (2026-02-24) | Run 3 (2026-03-05) |
|---|---|---|
| Total test cases | 212 (schema-only) | **118 (with data assertions)** |
| Passed | 163 (76.9%) | **73 (61.9%)** |
| Failed | 49 (23.1%) | **45 (38.1%)** |
| Data assertions | **None** — tests only checked column names/types | **All 103 success cases check row counts or full data** |
| Cluster startup | SUCCESS | SUCCESS |
| Data loading | SUCCESS (6/7 indices) | SUCCESS (6/7 indices) |

**Key change in Run 3**: All success test cases now validate actual data returned (row counts, value comparisons), not just schema. The previous runs were passing with `data=[]` (no rows returned) because tests only checked column names and types. Run 3 represents the first time the DQE execution pipeline returns real data from OpenSearch indices.

---

## 2. Test Infrastructure

### 2.1 Data Loading

| Index | Docs | Shards | Status |
|---|---|---|---|
| `dqe_test_all_types` | 86 | 1 | SUCCESS (14 of 100 docs fail: `big_number` field exceeds `long` range) |
| `dqe_test_nulls` | 50 | 1 | SUCCESS |
| `dqe_test_sharded` | 500 | 5 | SUCCESS |
| `dqe_test_conflict_a` | 5 | 1 | SUCCESS |
| `dqe_test_conflict_b` | 5 | 1 | SUCCESS |
| `dqe_test_employees` | 5 | 1 | SUCCESS |
| `dqe_test_datatypes` | — | — | SKIPPED (`knn_vector` type not available) |

### 2.2 Test Assertion Types

| Assertion Type | Count | Description |
|---|---|---|
| Full data comparison | 12 | Exact row-by-row value matching (sharded tests) |
| Exact row count | 40 | `expected_row_count` — no WHERE clause, count = min(LIMIT, total) |
| Minimum row count | 51 | `expected_min_row_count` — WHERE clause, at least 1 row |
| Error assertions | 15 | Error code + message substring matching |
| **Total** | **118** | |

---

## 3. Results by Suite

| Category | Passed | Total | % | Notes |
|---|---|---|---|---|
| basic_select (Q001-Q015) | 10 | 15 | 67% | SELECT * returns 0 rows (5 failures) |
| where_predicates (Q016-Q040) | 14 | 25 | 56% | WHERE on dqe_test_all_types: 11 return 0 rows |
| type_specific (Q041-Q060) | 12 | 20 | 60% | Type mismatches for geo_point, flattened, binary, timestamps |
| order_by_limit (Q061-Q075, Q114-Q118) | 11 | 20 | 55% | Sort on dqe_test_all_types fails (0 rows); adversarial TopN wrong values |
| multi_shard (Q076-Q085, Q111-Q113) | 10 | 13 | 77% | Conflict indices: column name mismatch; multi-col data mismatch |
| expressions (Q086-Q095) | 7 | 10 | 70% | NULLIF type, COALESCE type, combined predicates |
| error_cases (Q096-Q110) | 9 | 15 | 60% | 6 error message format mismatches |
| **Total** | **73** | **118** | **61.9%** | |

---

## 4. Failure Analysis

### 4.1 ZERO_ROWS — Queries returning 0 rows (27 failures)

**Root cause**: Queries on `dqe_test_all_types` that use `SELECT *` or `SELECT col1, col2, ...` (explicit column list with 3+ columns) return 0 rows. Queries with `SELECT id` or `SELECT id, name` (1-2 columns) work.

**Hypothesis**: The `dqe_test_all_types` index has complex types (geo_point, nested, arrays, flattened) that cause the `SearchHitToPageConverter` to fail during page construction. When a Page build fails, the scan operator returns no results.

**Affected tests**: Q002, Q009, Q010, Q012, Q031, Q032, Q035, Q038, Q039, and 18 more.

### 4.2 TYPE_MISMATCH — Schema type differences (11 failures)

**Root cause**: DQE type mapping for some OpenSearch types doesn't match test expectations:
- `NULLIF(int_val, 0)` returns BIGINT, test expects INTEGER → already fixed for most, 1 remaining
- `date` fields return TIMESTAMP(3), some tests still expect TIMESTAMP
- `geo_point` returns ROW, some tests expect specific format
- `flattened` type mapping
- `binary` type mapping

**Affected tests**: Q015, Q048, Q050, Q055, Q057, Q058, Q064, Q065, and 3 more.

### 4.3 WRONG_DATA — Incorrect values returned (4 failures)

**Root cause**: Adversarial TopN tests on `dqe_test_sharded` sort by `amount` (DOUBLE) but get garbage values like `4.65e+18`. The `amount` field is being read incorrectly — likely the `SearchHitToPageConverter` is reading the `amount` double as a long or vice versa.

**Affected tests**: Q114, Q115, Q117, Q112.

### 4.4 NAME_MISMATCH — Column names differ (3 failures)

**Root cause**: `SELECT *` on conflict indices returns columns in alphabetical order. Test expects specific column order. Point lookup test expects `id` first.

**Affected tests**: Q080, Q083, Q084.

---

## 5. What Changed Since Run 2

### 5.1 Execution Wiring (new in Run 3)

The DQE orchestrator now executes queries end-to-end:
1. Parse SQL → Trino AST
2. Analyze → resolve tables, columns, types, predicates
3. Get shard splits from cluster state
4. Build shard scan pipeline (ShardScanOperator → optional Filter → optional Project)
5. Drive pipeline on DQE worker thread pool
6. Apply coordinator pipeline (Sort/TopN/Limit) on raw pages
7. Convert Trino Pages to row data via PageToRowConverter
8. Return data in DQE response

### 5.2 New Components

| Component | Module | Purpose |
|---|---|---|
| PlanFragment | dqe-execution | Serializable plan fragment for data nodes |
| ExchangeSourceOperator | dqe-exchange | Adapts GatherExchangeSource to Operator |
| TransportChunkSender | dqe-exchange | Sends chunks via transport |
| ExchangePushHandler | dqe-exchange | Receives push requests into ExchangeBuffer |
| PageToRowConverter | dqe-types | Converts Pages to row data |
| ShardStageExecutor | dqe-plugin | Data-node pipeline builder |
| CoordinatorPipelineBuilder | dqe-plugin | Coordinator pipeline from PipelineDecision |

### 5.3 Test Improvements

- Added `expected_row_count` / `expected_min_row_count` to validate.py
- Added row count assertions to all 97 success test cases
- Added full data assertions to 6 key sharded tests (Q076, Q077, Q079, Q111, Q112, Q113)
- Fixed recursive directory scanning in validate.py
- Fixed 23 type expectations, 9 column name expectations, 6 error message expectations

### 5.4 Bug Fixes

- PIT search incompatible with shard preference → scan once per index
- DqeMetadata 2-arg methods need ClusterState → override in DqeEnginePlugin
- jol-core excluded from shadow JAR but needed by Trino → removed exclusion
- DQE thread pools not registered → added to SQLPlugin.getExecutorBuilders()
- Local execution blocks transport thread → offloaded to DQE worker pool

---

## 6. Recommendations

### 6.1 High Priority (unblocks ~27 tests)

**Fix SearchHitToPageConverter for complex types**: The converter fails silently on documents with geo_point, nested, array, or flattened fields, causing entire scan batches to return 0 rows. Need to handle these types gracefully (skip unsupported fields or return nulls).

### 6.2 Medium Priority (unblocks ~11 tests)

**Fix remaining type mapping mismatches**: Update test expectations or DQE type mappings for geo_point (ROW vs specific), flattened (MAP vs VARCHAR), binary (VARBINARY), and NULLIF return type inference.

### 6.3 Low Priority (unblocks ~7 tests)

**Fix adversarial TopN amount values**: The `amount` DOUBLE field reads garbage on sorted queries. Investigate whether the sort comparison uses wrong type accessor in RowComparator.

**Fix column order for SELECT * on conflict indices**: Stabilize column ordering for wildcard index patterns.

---

## 7. Projected Impact

Current pass rate: **73/118 (61.9%)**

| Fix | Tests Unblocked | Projected Pass Rate |
|---|---|---|
| Complex type handling in converter | ~27 | 100/118 (84.7%) |
| Remaining type expectations | ~11 | 111/118 (94.1%) |
| Adversarial TopN + column order | ~7 | 118/118 (100%) |

---

## 8. Commands to Reproduce

```bash
# Start cluster
./gradlew :opensearch-sql-plugin:run &

# Wait for cluster
curl -sf http://localhost:9200/_cluster/health?wait_for_status=green

# Load data
scripts/dqe-test/setup-data.sh

# Run Phase 1 tests (recursive)
python3 scripts/dqe-test/validate.py --url http://localhost:9200 \
    --cases scripts/dqe-test/cases/phase1/ --verbose
```
