# DQE Phase 1 Integration Tests

REST-based tests for `/_plugins/_trino_sql` using Python validation against expected results.

## Structure

```
dqe/integ-test/
├── cases/phase1/                # 103 test cases across 6 categories
│   ├── basic_select/            #   Q001-Q015
│   ├── where_predicates/        #   Q016-Q040
│   ├── type_specific/           #   Q041-Q060
│   ├── order_by_limit/          #   Q061-Q075, Q114-Q118
│   ├── multi_shard/             #   Q076-Q085, Q111-Q113
│   └── expressions/             #   Q086-Q095
├── data/
│   ├── mappings/                # Index mapping definitions (7 indices)
│   └── bulk/                    # Bulk data files (NDJSON)
├── reports/                     # Test reports
├── validate.py                  # Python test runner
├── setup-data.sh                # Load test data into OpenSearch
├── run-phase1-tests.sh          # Run all 103 phase1 tests
├── start-opensearch.sh          # Start OpenSearch via Gradle
├── stop-opensearch.sh           # Stop OpenSearch
└── redeploy.sh                  # Build → restart → reload (TDD loop)
```

## Quick Start

```bash
# Start OpenSearch with the SQL plugin
./dqe/integ-test/start-opensearch.sh --daemon

# Load test data
./dqe/integ-test/setup-data.sh

# Run all phase1 tests
./dqe/integ-test/run-phase1-tests.sh
```

## TDD Workflow

After making a code change to the DQE engine:

```bash
# Rebuild plugin, restart OpenSearch, reload data, then run tests
./dqe/integ-test/redeploy.sh
./dqe/integ-test/run-phase1-tests.sh

# Or skip data reload if only code changed (faster):
./dqe/integ-test/redeploy.sh --skip-data
./dqe/integ-test/run-phase1-tests.sh
```

## Running Individual Tests

```bash
# Single test case
python3 dqe/integ-test/validate.py --case dqe/integ-test/cases/phase1/basic_select/Q001_basic_column_selection.json --verbose

# All tests in a subdirectory
python3 dqe/integ-test/validate.py --cases dqe/integ-test/cases/phase1/where_predicates/ --verbose
```

## Test Case Format

```json
{
  "name": "test_name",
  "query": "SELECT ...",
  "expected": {
    "schema": [{"name": "col", "type": "VARCHAR"}],
    "data": [["value1"], ["value2"]]
  },
  "ignore_order": false,
  "float_tolerance": 0.001
}
```
