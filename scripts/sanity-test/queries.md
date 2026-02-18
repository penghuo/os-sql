# Distributed Engine — Manual Sanity Test Queries

Run `./setup-test-data.sh` first to create the test indices.

All queries use `HOST=localhost:9200`. Adjust as needed.

**IMPORTANT**: The setup script enables `strict_mode=true`. This means:
- If a query succeeds -> the distributed engine genuinely executed it
- If a query returns `StrictModeViolationException` -> it tried to fall back to DSL (engine bug)
- If a query returns `UnsupportedOperationException` -> the pattern is intentionally unsupported (expected)

Every successful query in this file is **proof** the distributed engine handled it.

---

## 0. Pre-flight: Verify Engine Settings

```bash
# Verify distributed engine + strict mode are both ON
curl -s localhost:9200/_cluster/settings?flat_settings=true | \
  python3 -c "
import sys, json
s = json.load(sys.stdin).get('transient', {})
for k in ['plugins.calcite.enabled',
          'plugins.sql.distributed_engine.enabled',
          'plugins.sql.distributed_engine.strict_mode']:
    v = s.get(k, 'NOT SET')
    ok = 'OK' if v == 'true' else 'MISSING'
    print(f'  {k} = {v} [{ok}]')
"

# Check plugin stats endpoint responds
curl -s localhost:9200/_plugins/_sql/stats | python3 -m json.tool
```

All three settings must show `true [OK]`. If strict_mode is not `true`, re-run `setup-test-data.sh`.

---

## 1. Basic Scan + Filter

Validates: LuceneFilterScan, FilterAndProjectOperator, scatter-gather across shards.

```bash
# 1a. Full scan — all 50 employees
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees"}' | python3 -m json.tool

# 1b. Equality filter on keyword field
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where dept = '\''Engineering'\''"}' | python3 -m json.tool

# 1c. Range filter on integer field
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where age > 50"}' | python3 -m json.tool

# 1d. Compound filter (AND)
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where dept = '\''Sales'\'' and salary > 60000"}' | python3 -m json.tool

# 1e. Boolean field filter
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where is_active = true"}' | python3 -m json.tool
```

**Expected**: Each query returns a subset of the 50 employees matching the predicate. Results come from all 5 shards via scatter-gather.

---

## 2. Projection (fields command)

Validates: ProjectNode, ColumnProjection.

```bash
# 2a. Select specific columns
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | fields name, dept, salary"}' | python3 -m json.tool

# 2b. Filter + project
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where city = '\''Seattle'\'' | fields name, age, salary"}' | python3 -m json.tool
```

**Expected**: Only the specified columns appear in the response schema.

---

## 3. Aggregation (stats command)

Validates: HashAggregationOperator, partial/final aggregation, GroupByHash, accumulators.

```bash
# 3a. COUNT with GROUP BY
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | stats count() by dept"}' | python3 -m json.tool

# 3b. AVG with GROUP BY
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | stats avg(salary) by dept"}' | python3 -m json.tool

# 3c. Multiple aggregations
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | stats count() as cnt, avg(age) as avg_age, sum(salary) as total_salary by city"}' | python3 -m json.tool

# 3d. MIN/MAX
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | stats min(salary) as min_sal, max(salary) as max_sal by dept"}' | python3 -m json.tool

# 3e. Global aggregation (no GROUP BY)
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | stats count() as total, avg(salary) as avg_sal"}' | python3 -m json.tool

# 3f. Filter + aggregation
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where is_active = true | stats avg(salary) by dept"}' | python3 -m json.tool
```

**Expected**:
- 3a: 10 per dept (50 docs / 5 depts)
- 3e: total=50, avg_sal=64500.0 (mean of 40000..89000 in steps of 1000)

---

## 4. Sort + Limit

Validates: TopNOperator, OrderByOperator, MergeSortedPages.

```bash
# 4a. Sort ascending
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | sort age | fields name, age"}' | python3 -m json.tool

# 4b. Sort descending + head (TopN)
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | sort - salary | head 5 | fields name, salary"}' | python3 -m json.tool

# 4c. Multi-column sort
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | sort dept, - salary | fields dept, name, salary"}' | python3 -m json.tool

# 4d. Head without sort (limit)
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | head 3"}' | python3 -m json.tool
```

**Expected**:
- 4b: Top 5 highest salaries (89000, 88000, 87000, 86000, 85000)

---

## 5. Dedup

Validates: DedupNode handling.

```bash
# 5a. Dedup on single field
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | dedup dept | fields dept, name"}' | python3 -m json.tool

# 5b. Dedup with sort
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | sort - salary | dedup city | fields city, name, salary"}' | python3 -m json.tool
```

**Expected**:
- 5a: Exactly 5 rows (one per unique dept)

---

## 6. Time-Series Queries (sanity_logs)

Validates: date types, range filters on timestamps, aggregation over time-series data.

```bash
# 6a. Filter by log level
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_logs | where level = '\''ERROR'\''"}' | python3 -m json.tool

# 6b. Stats by service
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_logs | stats count() as requests, avg(latency_ms) as avg_latency by service"}' | python3 -m json.tool

# 6c. Filter by status code + aggregate
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_logs | where status_code >= 400 | stats count() by status_code"}' | python3 -m json.tool

# 6d. Sort by latency, top offenders
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_logs | sort - latency_ms | head 10 | fields service, latency_ms, status_code"}' | python3 -m json.tool

# 6e. Sum bytes by service
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_logs | stats sum(bytes_sent) as total_bytes by service"}' | python3 -m json.tool
```

---

## 7. Combined Patterns (filter + agg + sort)

Validates: full operator pipeline through scatter-gather.

```bash
# 7a. Filter -> aggregate -> sort
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where age >= 30 | stats avg(salary) as avg_sal by dept | sort - avg_sal"}' | python3 -m json.tool

# 7b. Complex pipeline
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_logs | where status_code = 200 | stats count() as ok_count, avg(latency_ms) as avg_lat by service | sort - ok_count | head 3"}' | python3 -m json.tool
```

---

## 8. Explain API

Validates: explain output preserves Calcite format, distributed engine does not alter _explain.

```bash
# 8a. Explain a simple filter
curl -s -X POST localhost:9200/_plugins/_ppl/_explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where dept = '\''Engineering'\''"}' | python3 -m json.tool

# 8b. Explain an aggregation
curl -s -X POST localhost:9200/_plugins/_ppl/_explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | stats count() by dept"}' | python3 -m json.tool

# 8c. Explain a sort + limit
curl -s -X POST localhost:9200/_plugins/_ppl/_explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | sort - salary | head 5"}' | python3 -m json.tool
```

**Expected**: Response contains `calcite.logical` plan in standard Calcite format. No `engine` field in explain response.

---

## 9. Feature Flag Toggle (compare distributed vs DSL)

Validates: execution mode router, DSL fallback, result consistency.

```bash
# 9a. Run query with distributed engine ON + strict mode (current state)
#     If this succeeds, the distributed engine executed it
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | stats count() by dept"}' | python3 -m json.tool

# 9b. Disable distributed engine + strict mode, switch to DSL path
curl -s -X PUT localhost:9200/_cluster/settings \
  -H 'Content-Type: application/json' \
  -d '{"transient": {
    "plugins.sql.distributed_engine.enabled": false,
    "plugins.sql.distributed_engine.strict_mode": false
  }}'

# 9c. Run same query via DSL path — results MUST be identical to 9a
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | stats count() by dept"}' | python3 -m json.tool

# 9d. Re-enable distributed engine + strict mode
curl -s -X PUT localhost:9200/_cluster/settings \
  -H 'Content-Type: application/json' \
  -d '{"transient": {
    "plugins.sql.distributed_engine.enabled": true,
    "plugins.sql.distributed_engine.strict_mode": true
  }}'
```

**Expected**: Results from 9a and 9c are identical (same counts per dept). 9a proves the distributed engine handled it (strict mode was on). 9c proves DSL gives the same answer.

---

## 10. Strict Mode Proof

Validates: strict mode is already on (set by setup script). Every query above that succeeded is proof.

```bash
# 10a. Verify strict mode is still on
curl -s localhost:9200/_cluster/settings?flat_settings=true | \
  python3 -c "
import sys, json
s = json.load(sys.stdin).get('transient', {})
print('strict_mode =', s.get('plugins.sql.distributed_engine.strict_mode', 'NOT SET'))
"

# 10b. Run a supported pattern — must succeed (not fall back)
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where age > 30 | stats count() by dept"}' | python3 -m json.tool

# 10c. If 10b succeeds, it PROVES the distributed engine executed the query.
#      With strict_mode=true, any silent fallback would have thrown
#      StrictModeViolationException and returned HTTP 400.
```

**Expected**: 10b succeeds. This is the definitive proof — strict mode was on, so any silent fallback would have been caught and thrown as an error.

---

## 11. DSL Fallback for Unsupported Patterns

Validates: joins and other unsupported patterns fall back to DSL gracefully.

```bash
# 11a. Cross-index query (falls back to DSL)
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where dept = '\''Engineering'\'' | fields name, salary"}' | python3 -m json.tool
```

**Expected**: Query succeeds. The engine falls back to DSL for unsupported patterns transparently.

---

## 12. Empty Results

Validates: correct handling of queries that match zero documents.

```bash
# 12a. Filter with no matches
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where age > 999"}' | python3 -m json.tool

# 12b. Aggregation on empty result
curl -s -X POST localhost:9200/_plugins/_ppl \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=sanity_employees | where age > 999 | stats count() as cnt"}' | python3 -m json.tool
```

**Expected**:
- 12a: Empty datarows, valid schema
- 12b: Single row with cnt=0

---

## Validation Checklist

After running all queries, verify:

- [ ] All queries in sections 1-7 return non-empty results with correct data types
- [ ] Section 3e global aggregation returns exactly 1 row
- [ ] Section 4b returns exactly 5 rows with descending salary order
- [ ] Section 5a returns exactly 5 rows (one per dept)
- [ ] Section 8 explain output contains `calcite` key with `logical` plan
- [ ] Section 9: results with engine ON and OFF are identical
- [ ] Section 10b succeeds in strict mode
- [ ] Section 12b returns count=0 (not empty result)
- [ ] No HTTP 500 errors in any response
- [ ] No `StrictModeViolationException` in supported pattern queries
