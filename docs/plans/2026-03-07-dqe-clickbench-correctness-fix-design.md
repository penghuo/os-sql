# DQE ClickBench Correctness Fix Design

## Goal

Fix all 43 ClickBench queries in the DQE (`/_plugins/_trino_sql` endpoint) to match ClickHouse results on the 1M-row dataset. Current state: 3/43 PASS.

## Architecture

TDD query-by-query iteration using ralph-loop. Work through Q1→Q43 sequentially. Once a query passes, it's **locked** — it must pass in all future runs. Fixes are organized by root cause but verified per-query.

## Current State (2026-03-07)

| Status | Queries | Count |
|--------|---------|-------|
| PASS | Q2, Q20, Q21 | 3 |
| FAIL (wrong result) | Q4, Q5, Q6, Q18, Q26 | 5 |
| SKIP (timeout >60s) | Q1, Q3, Q24, Q25, Q27 | 5 |
| SKIP (HTTP 500 error) | Q7-Q17, Q19, Q22-Q23, Q28-Q43 | 30 |

## Bug Categories

### G: Performance (12 queries timeout)

Even `SELECT COUNT(*) FROM hits_1m` takes 30-120s. Root cause: `OpenSearchPageSource` likely reads all docs through Lucene without pushdown or batching optimization. Target: <5s for COUNT(*) on 1M rows.

**Key files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/OpenSearchPageSource.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/PageBuilder.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/executor/LocalExecutionPlanner.java`

### A: Column Alias in ORDER BY (~18 queries)

`ORDER BY c DESC` fails because sort key resolution looks for alias `c` in internal column list which stores `count(*)`. Error: `Column 'COUNT(*)' not found in columns: [AdvEngineID, count(*)]` (case mismatch) or `Column 'c' not found in columns: [SearchPhrase, count(*)]` (alias not resolved).

**Key files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/planner/LogicalPlanner.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java`

**Affected queries:** Q8-Q17, Q22-Q23, Q31-Q34, Q37-Q38

### B: Timestamp Operations (~4 queries)

MIN/MAX on `timestamp(3)` columns and ORDER BY timestamp not supported. Error: `Unsupported type for aggregation: timestamp(3)`.

**Key files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/operator/HashAggregationOperator.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/operator/SortOperator.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/common/types/TypeMapping.java`

**Affected queries:** Q7, Q25, Q27, Q43

### C: Expression Column Resolution (~6 queries)

Computed columns in SELECT like `length(URL)`, `ClientIP - 1`, `EXTRACT(MINUTE FROM EventTime)` are not tracked through the plan. Error: `Column 'length(URL)' not found in columns: [CounterID, URL]`.

**Key files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/planner/LogicalPlanner.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java`

**Affected queries:** Q19, Q28-Q30, Q35-Q36

### D: ClassCastException (~2 queries)

`LongArrayBlock cannot be cast to ShortArrayBlock`. OpenSearch stores small integers as long, but DQE maps them to SmallintType/TinyintType which expect ShortArrayBlock.

**Key files:**
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/PageBuilder.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/common/types/TypeMapping.java`

**Affected queries:** Q24+

### F: Correctness Bugs (5 queries)

- **Q4:** AVG(UserID) returns `1.948194E+18` instead of `-2657217693603.6587` (precision/type issue)
- **Q5/Q6:** COUNT(DISTINCT) returns 1000000 instead of actual distinct count (not implemented — returns COUNT(*))
- **Q18:** GROUP BY content mismatch (likely related to alias or type issue)
- **Q26:** String escaping — single quotes rendered with backslash escape (`\'` vs `'`)

### E: Unsupported SQL Features (~8 queries)

| Feature | Queries | Notes |
|---------|---------|-------|
| HAVING | Q28 | Post-aggregation filter |
| CASE WHEN | Q40 | Conditional expression |
| REGEXP_REPLACE | Q29 | Scalar function |
| DATE_TRUNC | Q43 | Scalar function |
| DATE literal comparison | Q37-Q42 | `>= DATE '2013-07-01'` |
| IN clause | Q41 | `TraficSourceID IN (-1, 6)` |
| GROUP BY ordinal | Q35 | `GROUP BY 1, URL` |
| EXTRACT | Q19 | `extract(minute FROM EventTime)` |

## Iteration Loop

### Per-Query TDD

```
For Q_N (N = 1 to 43):
  1. Test:    run_all.sh correctness --query N
  2. If PASS → lock Q_N, run regression, move to next
  3. If FAIL/SKIP:
     a. Diagnose: check logs, run explain, identify root cause category
     b. Fix code in DQE module
     c. Build:   ./gradlew :opensearch-sql-plugin:assemble -x test -x integTest
     d. Reload:  run_all.sh reload-plugin
     e. Re-test: run_all.sh correctness --query N
     f. When PASS → run regression on all locked queries
     g. If regression → fix before continuing
     h. Lock Q_N, commit
```

### Regression Check

After each fix, run only locked queries:
```bash
for locked_q in $LOCKED_QUERIES; do
    run_all.sh correctness --query $locked_q
done
```

A locked query failing is a **hard stop** — must be fixed before any new work.

### Cross-Unlock

When fixing one query's root cause (e.g., alias resolution for Q8), test all queries sharing that root cause. Lock any that pass. This means:
- Fixing Q8 alias issue → test Q9-Q17, Q22, Q23, Q31-Q34, Q37, Q38
- Fixing timestamp for Q7 → test Q25, Q27, Q43

### Tracking

Maintain `benchmarks/clickbench/results/correctness/locked_queries.txt`:
```
# Query : Status : Date locked : Root cause fixed
Q02: PASS : 2026-03-07 : baseline
Q20: PASS : 2026-03-07 : baseline
Q21: PASS : 2026-03-07 : baseline
```

## Dev Loop Commands

```bash
# Test single query
./run/run_all.sh correctness --query N

# After code change
./gradlew :opensearch-sql-plugin:assemble -x test -x integTest
./run/run_all.sh reload-plugin
./run/run_all.sh correctness --query N

# Explain query plan (debug)
curl -s -XPOST 'http://localhost:9200/_plugins/_trino_sql/_explain' \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(*) FROM hits_1m"}'

# Check OpenSearch logs for errors
sudo grep "exception\|Error" /opt/opensearch/logs/clickbench.log | tail -20
```

## Success Criteria

- 43/43 queries PASS in correctness tier (1M dataset)
- All queries complete within 60s timeout
- No regression: once locked, always passing
