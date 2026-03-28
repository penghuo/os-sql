# Plan 4

## Strategy: Fix the Measuring Stick, Then Fix the Code

Plans 1-3 failed because we optimized blind — the correctness script has fundamental flaws that mask true pass/fail status. 7 of 13 "failures" are ordering/tie-breaking artifacts, not real bugs. We must fix measurement first, then target the real failures and nearest-to-1x performance queries.

**Key insight from Plans 1-3**: The comparison script's `normalize()` function blindly `sort`s ALL output lines, destroying ORDER BY semantics. It also truncates (not rounds) floats, causing edge-case mismatches. Fixing this alone should flip 7+ queries from FAIL to PASS, giving us a true baseline to work from.

## Success Criteria
- [ ] Correctness ≥ 38/43 on hits_1m dataset (currently 30/43; expect +7-10 from comparison fix, +1-2 from code fixes)
- [ ] Performance ≥ 32/43 within 1x of ES on hits 100M dataset (currently 28-29/43)
- [ ] No correctness regressions from performance changes
- [ ] HLL code reverted — all COUNT(DISTINCT) returns exact values

## Anti-Criteria
- [ ] No placeholder/stub implementations
- [ ] No TODO comments left behind
- [ ] No removed tests or logging
- [ ] No approximate algorithms (HLL) that break correctness
- [ ] No MAX_CAPACITY changes above 4M (proven to cause cache thrashing)

---

## Task 1: Revert HLL and Restore Exact COUNT(DISTINCT)
- [x] Done

### Goal
Revert all HLL (HyperLogLogPlusPlus) changes from Plan 2 to restore exact COUNT(DISTINCT). Current correctness is 30/43; HLL broke 2-3 queries. Reverting should restore to ~33/43.

### Approaches
1. **Git revert HLL-specific changes** in `TransportShardExecuteAction.java`, `FusedScanAggregate.java`, `ShardExecuteResponse.java`, and `TransportTrinoSqlAction.java` — revert only HLL-related code blocks while keeping the MAX_CAPACITY=4M fix and other improvements. Preferred because it's surgical.
2. **Full git checkout of pre-HLL versions** of affected files, then re-apply MAX_CAPACITY=4M — fallback if surgical revert is too tangled.

### Verification
```bash
# Compile
./gradlew :dqe:compileJava
# Reload and run correctness
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin && bash run/run_all.sh correctness
# Expect: correctness ≥ 33/43 (restored to pre-HLL baseline)
```

### Dependencies
None — this is the first task.

---

## Task 2: Fix check_correctness.sh Comparison Logic
- [x] Done

### Goal
Fix the correctness comparison script to properly handle: (1) ORDER BY tie-breaking, (2) float precision tolerance, (3) queries without ORDER BY. This should flip 7+ false failures to PASS.

### Approaches
1. **Tie-aware normalization** — For queries with ORDER BY + LIMIT, sort only within groups of rows that share the same ORDER BY key value(s). For queries without ORDER BY, continue sorting all rows. Add a per-query metadata array in the script that specifies ORDER BY columns (by position). Use `awk` to group-sort tied rows by remaining columns. For float comparison, use rounding (`printf '%.6f'`) instead of truncation. — Preferred because it's precise and handles all edge cases.
2. **Query-specific expected-output overrides** — For known tie-breaking queries, pre-sort the expected output and actual output by all columns. Simpler but less correct for queries where ORDER BY matters.

### Implementation Details
The 43 queries break down as:
- **No ORDER BY** (sort all rows): Q00-Q08, Q28-Q29 — current `sort` is correct
- **ORDER BY + LIMIT with ties**: Q18, Q25, Q30-Q33, Q39-Q42 — need tie-aware sort
- **ORDER BY without ties** (unique sort keys): Q09-Q27 (most) — current sort works but is technically wrong; tie-aware sort is safe

Specific fixes needed in `normalize()` or a new `compare_query()` function:
1. Accept query number as parameter
2. Look up ORDER BY column positions for that query
3. Sort within tie groups only (rows sharing same ORDER BY key values get sorted by all remaining columns)
4. Replace float truncation regex with rounding: `awk '{for(i=1;i<=NF;i++) if($i ~ /^-?[0-9]+\.[0-9]+$/) $i=sprintf("%.4f",$i); print}'`
5. Use `diff` on the processed output

### Verification
```bash
cd benchmarks/clickbench && bash run/run_all.sh correctness
# Expect: correctness ≥ 37/43 (7 tie-breaking fixes: Q18, Q25, Q31, Q32, Q33, Q39, Q42)
# Remaining failures should be real bugs: Q05 (COUNT DISTINCT), Q06 (MIN/MAX), Q24, Q29, Q40, Q41
```

### Dependencies
Task 1 (need clean baseline without HLL)

---

## Task 3: Investigate and Fix Real Correctness Bugs (Q05, Q06, Q24, Q29, Q40, Q41)
- [x] Done

### Goal
After Tasks 1-2, the remaining failures are real bugs. Investigate root causes and fix what's feasible. Target: fix at least 2-3 of these to reach ≥ 38/43.

### Approaches
1. **Triage by difficulty and impact**:
   - **Q06** (`MIN/MAX EventDate`): Scalar aggregate — should be trivial. Check if EventDate type handling differs between CH and OS. Likely a date format or type casting issue. HIGH priority, likely easy fix.
   - **Q05** (`COUNT(DISTINCT SearchPhrase)`): Off by ~17K on 1M rows. Could be NULL handling (does DQE count NULL as a distinct value?). Check if CH excludes NULLs from COUNT(DISTINCT). HIGH priority.
   - **Q29** (89 SUMs): Float precision in SUM aggregation. Minor count difference (8227493 vs 8227489) suggests a row-counting issue in filtered scan. MEDIUM priority.
   - **Q24** (column order mismatch): Schema/column ordering issue in result set. MEDIUM priority.
   - **Q40/Q41** (OFFSET queries with count differences): Aggregation values differ, not just ordering. Could be filter evaluation bug with CounterID=62 + date range. LOWER priority — complex filtered GROUP BY.
2. **Fallback**: If a bug is deep in the execution engine and would take >1 iteration to fix, skip it and focus on performance tasks instead.

### Verification
```bash
# Test individual queries after each fix
cd benchmarks/clickbench && bash run/run_all.sh correctness --query N
# Then full suite
bash run/run_all.sh correctness
# Expect: ≥ 38/43
```

### Dependencies
Tasks 1 and 2

---

## Task 4: Performance — Target Nearest-to-1x Queries
- [x] Done

### Goal
Improve performance on queries closest to the 1x boundary. These have the highest ROI — small improvements flip them from >1x to ≤1x.

### Target Queries (sorted by ratio, nearest to 1x first)
| Query | Ratio | Type | Approach |
|-------|-------|------|----------|
| Q32 | 1.64x | 2-key GROUP BY (WatchID, ClientIP) + COUNT/SUM/AVG, no filter | Profile: is it hash map overhead? Try composite key encoding |
| Q16 | 1.88x | Multi-key GROUP BY | Profile: check if global ordinals path applies |
| Q13 | 2.04x | GROUP BY + COUNT(DISTINCT) | Profile: coordinator merge bottleneck |
| Q31 | 2.21x | 2-key GROUP BY + filter | Similar to Q32 but filtered — bitset path may help |
| Q18 | 2.22x | 3-key GROUP BY (UserID, minute, SearchPhrase) | Profile: extract(minute) overhead, 3-key hash map |
| Q11 | 2.51x | GROUP BY + COUNT(DISTINCT) | Similar to Q13 |

### Approaches
1. **Profile first, optimize second** — For each query, run with `-Xprof` or add timing instrumentation to identify the actual bottleneck (hash map ops, DocValues reads, coordinator merge, serialization). Only optimize what the profile shows.
2. **Batch DocValues reads** — For multi-key GROUP BY, current code calls `advanceExact(doc)` per column per doc. Batch reads for sequential doc access patterns (when iterating MatchAllDocs) can reduce overhead.
3. **Composite key encoding** — For 2-key GROUP BY with numeric keys (Q32: WatchID+ClientIP), encode both keys into a single long to avoid hash map overhead of composite keys.

### Verification
```bash
# Benchmark target queries
cd benchmarks/clickbench
for Q in 17 19 32 33; do
  bash run/run_opensearch.sh --warmup 1 --num-tries 3 --query $Q
done
# Compare ratios against ES baseline
# Expect: at least 3 of 6 target queries flip to ≤1x
```

### Dependencies
Tasks 1-3 (need stable correctness baseline before performance work)

---

## Task 5: Performance — Sub-10ms Noise and Quick Wins
- [x] Done

### Goal
Address the sub-100ms queries (Q00=4.50x, Q01=2.75x, Q19=3.67x, Q41=2.86x) where absolute times are tiny but ratios are high. Also look for any quick wins in the remaining >1x queries.

### Approaches
1. **Reduce per-query overhead** — These queries complete in 2-5ms on ES. DQE overhead is likely in: query parsing, plan optimization, shard dispatch, result serialization. Profile the coordinator path (`TransportTrinoSqlAction`) to find fixed-cost overhead. Even 5ms reduction would halve the ratio for these queries.
2. **Skip optimization if overhead is architectural** — If the overhead is inherent to the Trino SQL parsing/planning layer, document it and move on. These queries contribute to the count but the absolute time difference is negligible.

### Verification
```bash
cd benchmarks/clickbench
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 1
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 2
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 20
# Use more warmup/tries for sub-10ms queries to reduce noise
```

### Dependencies
Task 4 (performance work should be done in priority order)

---

## Task 6: Full Validation Run
- [x] Done

### Goal
Run complete correctness + performance suite and verify success criteria are met.

### Approaches
1. Run full correctness suite on hits_1m, verify ≥ 38/43
2. Run full performance suite on hits 100M, verify ≥ 32/43 within 1x of ES
3. Document final state in STATUS.md

### Verification
```bash
cd benchmarks/clickbench
bash run/run_all.sh correctness
# Verify: ≥ 38/43
bash run/run_all.sh performance
# Compare against CH-Parquet baseline: benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json
# Count queries where DQE ≤ ES time
```

### Dependencies
Tasks 1-5
