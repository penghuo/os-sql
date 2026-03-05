# DQE Phase 1 Integration Tests

Standalone REST-based tests for `/_plugins/_trino_sql`. No Java framework — just `curl` and `jq`.

## Structure

```
dqe/integ-test/
├── data/                        # Test datasets
│   └── dqe_test_logs.json       #   Index mapping + 10 documents
├── queries/                     # Numbered queries with expected results
│   ├── Q1_simple_select.json
│   ├── Q2_where_filter.json
│   ├── Q3_group_by_count.json
│   ├── Q4_order_by_limit.json
│   ├── Q5_where_group_by.json
│   ├── Q6_limit_only.json
│   ├── Q7_order_by_asc.json
│   └── Q8_explain.json
├── reports/                     # Test reports (gitignored)
├── run-tests.sh                 # Test runner
└── README.md
```

## Run

```bash
# Build plugin
./gradlew :opensearch-sql-plugin:bundlePlugin

# Start OpenSearch with plugin installed, then:
./dqe/integ-test/run-tests.sh                        # localhost:9200
./dqe/integ-test/run-tests.sh http://cluster:9200     # custom
```

## Query catalog

| ID | SQL | Asserts |
|----|-----|---------|
| Q1 | `SELECT category, status FROM logs` | 10 rows, exact data |
| Q2 | `SELECT ... WHERE status = 200` | 4 rows, all status=200 |
| Q3 | `SELECT category, COUNT(*) ... GROUP BY` | 4 groups, counts match |
| Q4 | `SELECT ... ORDER BY status DESC LIMIT 3` | 3 rows, descending, exact |
| Q5 | `SELECT ... WHERE status=200 GROUP BY` | 2 groups after filter |
| Q6 | `SELECT category ... LIMIT 5` | 5 rows, schema check |
| Q7 | `SELECT ... ORDER BY status ASC LIMIT 4` | 4 rows, ascending, exact |
| Q8 | `EXPLAIN SELECT ... WHERE` | Plan shows dslFilter pushdown |

## Assertions

Each query file specifies expected results. The runner compares:
- `status` — response status code
- `total` — exact row count
- `schema` — column names and types
- `datarows` — exact row data (sorted if `sort_before_compare` is set)
- `contains` — response body contains substrings (for EXPLAIN)

When `sort_before_compare` is set (e.g., `[0, 1]`), both actual and expected rows are sorted by those column indices before comparison. This handles non-deterministic row ordering from distributed execution.

## Adding queries

Create `queries/Q9_your_test.json`:
```json
{
  "id": "Q9",
  "name": "Description",
  "endpoint": "/_plugins/_trino_sql",
  "query": "SELECT ...",
  "expected": {
    "status": 200,
    "schema": [{"name": "col", "type": "long"}],
    "datarows": [[1], [2]],
    "total": 2
  },
  "sort_before_compare": [0]
}
```
