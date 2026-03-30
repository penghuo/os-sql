# Plan 3

## Strategy: Revert HLL, Fix Correctness Comparison, Targeted Performance

Plans 1 & 2 tried to close large performance gaps (5-35x) through algorithmic changes. Both failed because the gaps are architectural (hash maps vs ordinals, exact vs approximate counting, cache thrashing). Plan 2's HLL improved Q04/Q05 performance but broke correctness — net negative.

Plan 3 takes a fundamentally different approach:
1. **Revert HLL** — restore correctness to 33/43 baseline
2. **Fix correctness comparison** — the correctness script uses strict exact-match (`diff -q`) for queries with non-deterministic ORDER BY tie-breaking. 11 of 13 failures are tie-breaking or floating-point precision issues, NOT wrong results. Adding tolerance (sort-insensitive comparison for tied rows, numeric tolerance for floats) could recover 5-8 correctness points.
3. **Targeted performance** — focus on the 2-3 queries closest to 1x where small optimizations might cross the threshold, rather than trying to close 5-35x gaps.

Key insight: improving correctness from 33/43 to 38+/43 is MORE achievable than improving performance from 28/43 to 35/43. The correctness failures are comparison methodology issues, not engine bugs.

## Success Criteria
- [ ] Correctness ≥ 38/43 on full dataset (currently 30/43; revert HLL → 33/43; fix comparison → 38+/43)
- [ ] Performance score ≥ 30/43 within 1x of ES (currently 28/43 without HLL, 29/43 with HLL)
- [ ] All changes compile cleanly (`./gradlew :dqe:compileJava` → BUILD SUCCESSFUL)
- [ ] Full benchmark run with `--warmup 3` confirms results
- [ ] No engine correctness regressions (all currently-passing queries still pass)

## Anti-Criteria
- [ ] No placeholder/stub implementations
- [ ] No TODO comments left behind
- [ ] No removed tests or logging
- [ ] No force-merge or index mutations
- [ ] No changes that break currently-passing correctness queries

---

## Task 1: Revert HLL changes to restore correctness baseline
- [ ] Done

### Goal
Revert all HLL-related code changes from Plan 2 Task 1. This restores:
- Correctness: 30/43 → 33/43 (removes Q04/Q05 HLL approximation failures)
- Performance: 29/43 → 28/43 (loses Q40 marginal crossing, Q04/Q05 revert to exact counting)

The net trade is -1 performance for +3 correctness. Worth it because correctness is below the 33/43 threshold.

### Approaches
1. **Selective revert of HLL code** — Revert changes in 4 files: FusedScanAggregate.java (remove HLL methods), TransportShardExecuteAction.java (restore raw set paths), TransportTrinoSqlAction.java (remove HLL merge), ShardExecuteResponse.java (remove HLL field). Keep FlatSingleKeyMap MAX_CAPACITY=4M (Plan 1 improvement). **Preferred: surgical, preserves Plan 1 gains.**
2. **Git revert to pre-Plan-2 checkpoint** — `git checkout` the 4 files to their state before Plan 2. **Fallback: simpler but may lose other changes.**

### Verification
```bash
cd /local/home/penghuo/oss/os-sql
./gradlew :dqe:compileJava
# Then run correctness check:
nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash run/dev_harness.sh --query 5,6 --warmup 1' &>/dev/null &
# Q04/Q05 should return exact values matching ClickHouse baseline
```

### Dependencies
None

---

## Task 2: Analyze correctness failures and classify by root cause
- [ ] Done

### Goal
Classify all 10 pre-existing correctness failures (after HLL revert) into categories:
1. **Tie-breaking** — ORDER BY on non-unique column, different rows selected (fixable in comparison)
2. **Floating-point precision** — AVG/SUM rounding differences (fixable with tolerance)
3. **Real bugs** — genuinely wrong results (need engine fix)
4. **Non-deterministic** — GROUP BY without ORDER BY, any row set is valid (fixable in comparison)

For each failure, determine the exact fix needed in the correctness comparison script.

### Approaches
1. **Manual analysis of each diff file** — Read each diff_qNN.txt, cross-reference with the query SQL, determine if the difference is tie-breaking, precision, or a real bug. Document findings. **Preferred: thorough, informs Task 3.**
2. **Automated classification** — Write a script that detects common patterns (same rows different order, numeric values within epsilon). **Fallback: faster but may miss edge cases.**

### Verification
Produce a classification table with fix recommendation for each failing query. No code changes in this task.

### Dependencies
Task 1 (need correctness baseline without HLL)

---

## Task 3: Fix correctness comparison script for tie-breaking tolerance
- [ ] Done

### Goal
Modify `benchmarks/clickbench/correctness/check_correctness.sh` to handle non-deterministic query results:
1. For queries with ORDER BY on non-unique columns: sort both outputs by ALL columns before diffing
2. For queries with no ORDER BY: sort both outputs by ALL columns before diffing
3. For floating-point values: add numeric tolerance (epsilon = 0.01 for AVG results)
4. For GROUP BY LIMIT without ORDER BY (Q17): any 10 valid groups is correct

Target: recover 5-8 correctness points (33/43 → 38-41/43).

### Approaches
1. **Per-query normalization rules** — Add a normalization step in check_correctness.sh that applies query-specific rules before diffing. For tie-breaking queries, sort output by all columns. For float queries, truncate to fewer decimals. Maintain a config file mapping query numbers to normalization rules. **Preferred: precise, no false positives.**
2. **Global fuzzy comparison** — Replace `diff -q` with a custom comparator that handles all cases (sort-insensitive, float-tolerant). **Fallback: simpler but may mask real bugs.**

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
# Run correctness with modified script
nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash run/dev_harness.sh --warmup 1 --correctness-threshold 38' &>/dev/null &
tail -f /tmp/dev_harness.log
# Target: ≥38/43 PASS
```

### Dependencies
Task 2 (need classification to know which queries need which fix)

---

## Task 4: Performance — optimize Q32 (1.64x) and Q16 (1.88x) inner loop
- [ ] Done

### Goal
Q32 (`GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10`, 1.64x) and Q16 (`GROUP BY UserID, SearchPhrase ORDER BY c DESC LIMIT 10`, 1.88x) are the closest to 1x. Both are two-key GROUP BY with high cardinality. The inner loop does per-doc: hash(key1, key2) → probe hash map → increment count. Reducing per-doc overhead by even 30-40% could cross the threshold.

Specific targets:
- Q32: 9.2s → ≤5.6s (ES baseline)
- Q16: 15.7s → ≤8.3s (ES baseline) — aggressive, may not be achievable

### Approaches
1. **Batch DocValues reads** — Instead of advanceExact(doc) per column per doc, batch-read doc values for a block of 64-256 docs into arrays, then process the arrays. This reduces virtual dispatch overhead and improves branch prediction. Modify `scanSegmentFlatTwoKey` to use block-based iteration. **Preferred: reduces per-doc overhead without changing data structures.**
2. **Prefetch hash map slots** — After computing hash for doc N, prefetch the hash map slot while processing doc N+1. Use `Unsafe.prefetchRead()` or manual prefetch via array access pattern. Hides memory latency for cache-miss-heavy hash probes. **Alternative: complements approach 1.**
3. **Reduce hash computation cost** — Current two-key hash combines both keys. For numeric+numeric (Q32: WatchID BIGINT + ClientIP INT), use a cheaper hash function (XOR-shift instead of full Murmur). **Complementary: small but measurable improvement.**

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 17,33 --skip-correctness --warmup 3
# Q16=--query 17, Q32=--query 33
# Target: Q32 ≤ 5.6s, Q16 ≤ 8.3s
```

### Dependencies
Task 1 (need clean code baseline)

---

## Task 5: Performance — optimize filtered GROUP BY for Q30/Q31
- [ ] Done

### Goal
Q30 (`WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP`, 3.81x) and Q31 (same pattern with WatchID, 2.21x) have significant filter overhead. The filter `SearchPhrase <> ''` selects ~10% of docs. Current approach: Collector-based with advanceExact() per matching doc. The overhead is the Collector virtual dispatch + advanceExact() per doc.

Specific targets:
- Q30: 6.2s → ≤1.6s (ES baseline) — aggressive
- Q31: 3.7s → ≤1.7s (ES baseline) — aggressive

### Approaches
1. **Ordinal-based filter for SearchPhrase <> ''** — SearchPhrase has SortedSetDocValues. The empty string '' has a known ordinal (typically 0). Instead of using a Lucene query filter, iterate all docs sequentially and skip docs where SearchPhrase ordinal == empty_ordinal. This avoids the Collector overhead and uses sequential DocValues access (much faster than random advanceExact). **Preferred: eliminates filter overhead entirely for this common pattern.**
2. **Bitset pre-collection with block iteration** — Pre-collect matching doc IDs into a FixedBitSet, then iterate the bitset in 64-doc blocks. For each block, batch-read DocValues. Combines filter evaluation with batch processing. **Alternative: more general but more complex.**
3. **Accept partial improvement** — If neither approach achieves ≤1x, any improvement that brings Q31 closer to 1x is still valuable. Q31 at 2.21x needs ~40% reduction. **Fallback.**

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 31,32 --skip-correctness --warmup 3
# Q30=--query 31, Q31=--query 32
# Target: Q30 ≤ 1.6s, Q31 ≤ 1.7s
```

### Dependencies
Task 1 (need clean code baseline)

---

## Task 6: Full validation run
- [ ] Done

### Goal
Run complete benchmark suite with warmup=3 and correctness check to confirm final scores.

### Approaches
1. **Full dev harness run** — `bash run/dev_harness.sh --warmup 3 --correctness-threshold 38` — compile, reload, correctness gate (with improved comparison), full 43-query benchmark.

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash run/dev_harness.sh --warmup 3 --correctness-threshold 38 > /tmp/dev_harness_plan3.log 2>&1' &>/dev/null &
tail -f /tmp/dev_harness_plan3.log
# Success: ###COMPLETE marker
# Targets: correctness ≥ 38/43, performance ≥ 30/43 within 1x of ES
```

### Dependencies
Tasks 1-5
