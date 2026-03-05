# DQE Phase 1 Integration Tests

Standalone REST-based tests for the `/_plugins/_trino_sql` endpoint.
No Java test framework dependency — just `curl` and `jq`.

## Prerequisites

- OpenSearch cluster running with the `opensearch-sql` plugin (includes DQE module)
- `curl` and `jq` installed

## Build and start OpenSearch

```bash
# Build the plugin
./gradlew :opensearch-sql-plugin:bundlePlugin

# Start OpenSearch with the plugin (example using Docker or local install)
# The plugin ZIP is at: plugin/build/distributions/opensearch-sql-*.zip
```

## Run tests

```bash
# Against local cluster (default: http://localhost:9200)
./dqe/integ-test/run-tests.sh

# Against a specific cluster
./dqe/integ-test/run-tests.sh http://my-cluster:9200
```

## Test structure

- `queries.json` — Test data (index mapping + documents) and query definitions with assertions
- `run-tests.sh` — Test runner: creates index, loads data, runs queries, asserts results, cleans up

## What's tested

| Test | SQL | Asserts |
|------|-----|---------|
| Simple SELECT | `SELECT category, status FROM dqe_test_logs` | 10 rows, 2 columns |
| WHERE filter | `SELECT ... WHERE status = 200` | 4 rows, all status=200 |
| GROUP BY + COUNT | `SELECT category, COUNT(*) ... GROUP BY category` | Count sum = 10 |
| ORDER BY + LIMIT | `SELECT ... ORDER BY status DESC LIMIT 3` | 3 rows, descending |
| LIMIT only | `SELECT category ... LIMIT 5` | 5 rows, 1 column |
| EXPLAIN | `/_plugins/_trino_sql/_explain` | Plan contains index name |

## Adding tests

Add entries to `queries.json` under the `tests` array. Available assertions:

- `status` — HTTP response `status` field (200)
- `total` — exact row count
- `schema_length` — number of columns
- `schema_names` — exact column name list
- `all_rows_match` — every row has expected value at column index
- `total_count_sum` — sum of values in column 1 (for GROUP BY + COUNT)
- `min_rows` — at least N rows returned
- `descending_column_index` — column values are non-increasing
- `contains` — response body contains substrings (for EXPLAIN)
