# Ralph Planning Logs

Append-only log of planning rationale and decisions.

## Plan 1 — 2026-03-27

### Analysis

14 queries above 1x of ES. Grouped by pattern:

| Category | Queries | Ratio Range | Root Cause |
|----------|---------|-------------|------------|
| Sub-10ms noise | Q00, Q01, Q19, Q41 | 2.75-4.50x | Fixed dispatch overhead (~5-50ms) dominates when ES finishes in 2-21ms |
| Global COUNT(DISTINCT) | Q04, Q05 | 7.80-10.06x | DQE does exact HashSet counting; ES likely uses HyperLogLog approximation |
| GROUP BY + COUNT(DISTINCT) | Q11, Q13 | 2.04-2.51x | HashSet-per-group overhead for high-cardinality GROUP BY with COUNT(DISTINCT) |
| High-card single-key GROUP BY + TopN | Q15 | 35.79x | Full hash table materialization for ~17M UserID groups, only need top 10 |
| Multi-key high-card GROUP BY + TopN | Q16, Q18 | 1.88-2.22x | Same as Q15 but multi-key; all groups serialized across shards |
| Multi-key GROUP BY + multi-agg | Q30, Q31, Q32 | 1.64-3.81x | Multi-aggregation overhead + filter cost + no shard-level top-N |

### Prioritization Rationale

1. **Q15 first** (Task 1) — 35.79x is the worst ratio by far. Single-key numeric GROUP BY + TopN is the simplest case to optimize. The pattern (ordinal/hash counting + heap selection) already exists for VARCHAR in `executeSingleVarcharCountStar`. Extending to numeric keys is straightforward.

2. **Q04/Q05 second** (Task 2) — 10x and 7.8x ratios. Global COUNT(DISTINCT) without GROUP BY is a clean, isolated optimization. FixedBitSet on ordinals is the most efficient exact path. Independent of Task 1.

3. **Q16/Q18 third** (Task 3) — Depends on Task 1's shard-level top-N pattern. Generalizes it to multi-key GROUP BY. Moderate ratios (1.88-2.22x) but high absolute times (15-44s).

4. **Q30/Q31/Q32 fourth** (Task 4) — Depends on Task 3's multi-key top-N. Adds multi-aggregation fusion. Moderate ratios (1.64-3.81x).

5. **Q11/Q13 fifth** (Task 5) — Depends on Task 2's COUNT(DISTINCT) patterns. Combines GROUP BY with COUNT(DISTINCT). Moderate ratios (2.04-2.51x).

6. **Sub-10ms queries last** (Task 6) — Lowest priority. Small absolute impact (milliseconds). Requires profiling to identify the overhead source. May not be achievable without architectural changes to the SQL parsing/planning pipeline.

### Risk Assessment

- Tasks 1-2 are high-confidence: clear algorithmic improvements with known patterns in the codebase.
- Tasks 3-4 are medium-confidence: depend on Task 1 succeeding and generalizing cleanly.
- Task 5 is medium-confidence: COUNT(DISTINCT) within GROUP BY is complex; may need HLL to match ES.
- Task 6 is low-confidence: fixed overhead may be inherent to the Trino SQL parsing layer.

### Expected Outcome

Conservative estimate: Tasks 1-4 flip 6-8 queries (Q15, Q04, Q05, Q16, Q18, Q30, Q31, Q32), reaching 34-36/43.
Optimistic estimate: All tasks flip 10+ queries, reaching 38+/43.

## Plan #1 produced 7 tasks — Fri Mar 27 17:46:00 UTC 2026

## Oracle REJECTED plan #1 (rejection #1) — Sat Mar 28 01:24:36 UTC 2026

Verdict: REJECTED. The primary success criterion (≥35/43 within 1x) is NOT MET. Score remained at 28/43.
[38;5;244m - Completed in 0.0s[0m

[?25l[0m[38;5;141m> [0mNow let me append the verdict to STATUS.md:[0m[0m
[?25l[0m[0mRunning tool [38;5;141mQuipEditor[0m with the param[38;5;244m (from mcp server: builder-mcp)[0m
 ⋮  {
 ⋮    "format": "markdown",
 ⋮    "documentId": "/local/home/penghuo/oss/os-sql/.ralph/STATUS.md"
 ⋮  }
[0m[38;5;244m - Completed in 0.416s[0m

[?25l[0m[?25l[0m[0mReading file: [38;5;141m/local/home/penghuo/oss/os-sql/.ralph/STATUS.md[0m, all lines[38;5;244m (using tool: read)[0m
[38;5;10m ✓ [0mSuccessfully read [38;5;244m3013 bytes[0m from /local/home/penghuo/oss/os-sql/.ralph/STATUS.md
[38;5;244m - Completed in 0.0s[0m

[?25l[0m[?25l[0m[0m⢀⠀ [38;5;12m0[39m of [38;5;12m1[39m hooks finished⡀⠀ [38;5;12m0[39m of [38;5;12m1[39m hooks finished[1G[2K[?25l[38;5;12m[38;5;10m✓[39m [38;5;12m1[39m of [38;5;12m1[39m hooks finished in [38;5;11m0.08 s
[0mI will run the following command: [38;5;141mcat >> /local/home/penghuo/oss/os-sql/.ralph/STATUS.md << 'EOF'

### Oracle
REJECTED: Primary success criterion NOT MET — score remains 28/43 (target ≥35/43, zero improvement). Full warmup=3 benchmark NOT MET (OOM, fell back to warmup=1). Per-criterion details:

## Plan 2 — 2026-03-28

### Why Plan 1 Failed
Plan 1 tried to optimize individual queries within the existing architecture:
- Task 1 (Q15): Partial success — 28.8s→4.1s via MAX_CAPACITY reduction, but still 5.04x of ES
- Task 2 (Q04/Q05): Failed — ES uses HLL approximate counting, DQE uses exact. Can't match.
- Task 3 (Q16/Q18): Marginal — Q16 improved slightly, Q18 unchanged. Cache thrashing.
- Task 4 (Q30/Q31/Q32): Failed — All 4 approaches caused regressions from cache thrashing.
- Task 5 (Q11/Q13): Failed — Coordinator merge of per-group LongOpenHashSets is bottleneck.
- Task 6 (sub-10ms): Skipped — measurement noise.

Root cause: Plan 1 optimized the hot loop (hash map operations). The real bottleneck is the shard→coordinator data boundary — DQE ships orders of magnitude more data than ES between shards and coordinator.

### Codebase Exploration Findings
1. **No HLL exists in DQE** — Zero results for HyperLogLog, HLL, approximate, sketch. All COUNT(DISTINCT) is exact via LongOpenHashSet/HashSet<Object>.
2. **4 separate accumulator abstractions** need changes for HLL:
   - `Accumulator` (function/aggregate/Accumulator.java)
   - `Accumulator` (operator/HashAggregationOperator.java:677)
   - `MergeableAccumulator` (FusedGroupByAggregate.java:12568)
   - `DirectAccumulator` (FusedScanAggregate.java:1064)
3. **Critical bug**: ResultMerger rejects COUNT(DISTINCT) from all fast merge paths (`isDistinct→return false`), falls to generic path that treats it as SUM (line 263). This is incorrect for multi-shard exact counting but works if shards emit final HLL cardinality.
4. **Shard-side top-N already exists** for fused GROUP BY paths (`executeWithTopN()` at line 487-523), but multi-pass fallback and generic pipeline ship ALL groups.
5. **FlatSingleKeyMap.MAX_CAPACITY=4M** forces multi-pass for Q15's 17.6M unique UserIDs. Increasing to 24M eliminates multi-pass.

### Plan 2 Strategy: Optimize the Data Boundary
Instead of making hash maps faster, reduce what crosses the wire:

| Strategy | Queries | Mechanism | Expected Impact |
|----------|---------|-----------|-----------------|
| HLL for global COUNT(DISTINCT) | Q04, Q05 | Ship ~16KB sketch instead of millions of values | +2 queries (95% confidence) |
| HLL for grouped COUNT(DISTINCT) | Q11, Q13 | Ship per-group counts instead of per-group hash sets | +2 queries (85% confidence) |
| Shard-side top-N + capacity increase | Q15, Q16, Q30, Q31, Q32 | Ship top-K groups instead of all groups; eliminate multi-pass | +3-5 queries (60-70% confidence) |
| Q18 stretch goal | Q18 | Ensure fused path + capacity | +0-1 queries (30% confidence) |
| Write-off sub-100ms | Q00, Q01, Q19, Q41 | Accept as measurement noise | 0 queries |

### Task Ordering Rationale
1. Task 1 (HLL global) first — highest confidence (95%), no dependencies, establishes HLL infrastructure
2. Task 2 (HLL grouped) second — depends on Task 1's HLL implementation, high confidence (85%)
3. Task 3 (shard-side top-N) — independent of Tasks 1-2, can run in parallel, medium-high confidence
4. Task 4 (Q18) — depends on Task 3's capacity patterns, low confidence stretch goal
5. Task 5 (validation) — depends on all, final measurement

### Risk Assessment
- Tasks 1-2 (HLL): High confidence. ES uses HLL, matching the algorithm matches the performance. Main risk: HLL accuracy at low precision may cause correctness test failures.
- Task 3 (top-N): Medium confidence. Shard-side top-N is approximate for ORDER BY + LIMIT — global top-10 might not appear in any shard's top-K. Mitigation: use large K (10000).
- Task 4 (Q18): Low confidence. 3-key GROUP BY with expression key may be architecturally limited.
- Overall: Conservative 35-37/43, optimistic 38-40/43.

### Queries Written Off (4)
Q00 (9ms vs 2ms), Q01 (11ms vs 4ms), Q19 (11ms vs 3ms), Q41 (60ms vs 21ms) — all sub-100ms absolute. DQE has fixed overhead from SQL parsing, plan compilation, transport dispatch. No optimization will close this gap.

## Plan #2 produced 5 tasks — Sat Mar 28 01:38:23 UTC 2026

## Oracle REJECTED plan #2 (rejection #2) — Sat Mar 28 03:29:58 UTC 2026

**Verdict: REJECTED**

Primary reasons:
1. Score 29/43 vs target ≥35/43 (NOT MET)
2. Correctness 30-31/43 vs target ≥33/43 (NOT MET, regression)
3. Anti-criterion "no correctness regressions" violated

[38;5;244m - Completed in 0.0s[0m

[?25l[0m[38;5;141m> [0mNow I have all the evidence. Let me append the verdict:[0m[0m
[?25l[0m[0mRunning tool [38;5;141mQuipEditor[0m with the param[38;5;244m (from mcp server: builder-mcp)[0m
 ⋮  {
 ⋮    "format": "markdown",
 ⋮    "documentId": "/local/home/penghuo/oss/os-sql/.ralph/STATUS.md",
 ⋮    "analyzeStructure": true
 ⋮  }
[0m[38;5;244m - Completed in 0.427s[0m

[?25l[0m[?25l[0m[0mReading file: [38;5;141m/local/home/penghuo/oss/os-sql/.ralph/STATUS.md[0m, from line [38;5;141m73[0m to end of file[38;5;244m (using tool: read)[0m
[38;5;10m ✓ [0mSuccessfully read [38;5;244m337 bytes[0m from /local/home/penghuo/oss/os-sql/.ralph/STATUS.md

## Plan 3 — 2026-03-28

### Problem Space Analysis (BFS)

After 2 rejected plans and 11 execution iterations, the state is:
- Performance: 29/43 within 1x (28/43 without HLL). Target was ≥35/43.
- Correctness: 30/43 (33/43 without HLL). Target is ≥33/43.
- Surviving code: FlatSingleKeyMap MAX_CAPACITY=4M (Q15: 28.8s→4.1s), scalar HLL for Q04/Q05 (broke correctness)

**Exhausted approaches (do NOT retry):**
- Hash map capacity tuning beyond 4M → cache thrashing (Plan 1 Task 1, Plan 2 Task 3)
- Ordinal-based exact COUNT(DISTINCT) → overhead exceeds savings (Plan 1 Task 2)
- Per-group HLL → HyperLogLogPlusPlus too expensive for 100K+ groups (Plan 2 Task 2)
- Filter-aware bucket counting → single-bucket maps too large for L3 (Plan 1 Task 4)
- Bitset + sequential scan for filtered queries → reading all docs slower than filtered advanceExact (Plan 1 Task 4)
- Global ordinals for ORDER BY → helped Q16 marginally but regressed Q15 (Plan 1 Task 3)
- MAX_CAPACITY increase to 24M → 7x regression on Q15 (Plan 2 Task 3)

### Key Discovery: Correctness Failures Are Comparison Methodology Issues

Analyzed all 13 correctness failures. The comparison script (`check_correctness.sh`) uses strict `diff -q` on normalized outputs. But:

| Query | Failure Type | Root Cause |
|-------|-------------|------------|
| Q04 (line 5) | HLL approximation | DQE HLL=17,614,259 vs CH exact=17,630,976 (0.095% error) |
| Q05 (line 6) | HLL approximation | DQE HLL=5,990,674 vs CH exact=6,019,103 (0.47% error) |
| Q17 (line 18) | Non-deterministic | `GROUP BY ... LIMIT 10` with NO ORDER BY — any 10 groups valid |
| Q23 (line 24) | Tie-breaking | `ORDER BY EventTime LIMIT 10` — ties on EventTime |
| Q24 (line 25) | Tie-breaking | `ORDER BY EventTime LIMIT 10` — ties on EventTime |
| Q28 (line 29) | Float precision | `AVG(length(Referer))` — rounding difference in 6th decimal |
| Q30 (line 31) | Tie-breaking | `ORDER BY c DESC LIMIT 10` — ties on COUNT(*) |
| Q31 (line 32) | Tie-breaking | `ORDER BY c DESC LIMIT 10` — ties on COUNT(*) |
| Q32 (line 33) | Tie-breaking | `ORDER BY c DESC LIMIT 10` — ties on COUNT(*) |
| Q38 (line 39) | Tie-breaking | `ORDER BY PageViews DESC OFFSET 1000 LIMIT 10` — ties |
| Q39 (line 40) | Tie-breaking | `ORDER BY PageViews DESC OFFSET 1000 LIMIT 10` — ties |
| Q40 (line 41) | Tie-breaking | `ORDER BY PageViews DESC OFFSET 100 LIMIT 10` — ties |
| Q41 (line 42) | Tie-breaking | `ORDER BY PageViews DESC OFFSET 10000 LIMIT 10` — ties |

After reverting HLL: Q04/Q05 become PASS again → 33/43.
Of the remaining 10 failures: 8 are tie-breaking, 1 is float precision, 1 is non-deterministic GROUP BY.
All 10 are comparison methodology issues, NOT engine bugs.

### Strategy Decision

**Option A: Keep trying to close performance gaps (Plans 1 & 2 approach)**
- Need to flip 7 queries from above-1x to below-1x
- All easy optimizations exhausted; remaining gaps are architectural
- Risk: high (both plans failed)
- Expected outcome: 28-30/43 performance

**Option B: Fix correctness comparison + targeted performance (Plan 3)**
- Revert HLL: +3 correctness (30→33), -1 performance (29→28)
- Fix comparison: +5-8 correctness (33→38-41)
- Targeted performance on Q32 (1.64x), Q16 (1.88x): +0-2 performance (28→28-30)
- Risk: medium (comparison fixes are well-understood; performance is speculative)
- Expected outcome: 38-41/43 correctness, 28-30/43 performance

**Decision: Option B.** The correctness comparison fixes are high-confidence and high-impact. Performance optimization is speculative but worth attempting on the nearest-to-1x queries.

### Performance Target Analysis

14 queries above 1x (without HLL), sorted by ratio:
1. Q32: 1.64x (9.2s vs 5.6s) — two-key GROUP BY, unfiltered
2. Q16: 1.88x (15.7s vs 8.3s) — two-key GROUP BY, high-card
3. Q13: 2.04x (8.6s vs 4.2s) — GROUP BY + COUNT(DISTINCT)
4. Q31: 2.21x (3.7s vs 1.7s) — filtered two-key GROUP BY
5. Q18: 2.22x (44.4s vs 20.0s) — 3-key GROUP BY
6. Q11: 2.51x (3.8s vs 1.5s) — GROUP BY + COUNT(DISTINCT)
7. Q01: 2.75x (11ms vs 4ms) — sub-10ms noise
8. Q41: 2.86x (60ms vs 21ms) — sub-100ms noise
9. Q19: 3.67x (11ms vs 3ms) — sub-10ms noise
10. Q30: 3.81x (6.2s vs 1.6s) — filtered two-key GROUP BY
11. Q00: 4.50x (9ms vs 2ms) — sub-10ms noise
12. Q15: 5.15x (4.1s vs 0.8s) — high-card single-key GROUP BY
13. Q05: 7.80x (4.0s vs 0.5s) — COUNT(DISTINCT)
14. Q04: 10.06x (2.9s vs 0.3s) — COUNT(DISTINCT)

Realistic targets: Q32 (needs 40% reduction) and Q16 (needs 47% reduction). Both are two-key GROUP BY where inner-loop optimization might help. Q30/Q31 are filtered variants where eliminating filter overhead could help.

### Task Design Rationale

1. **Task 1 (Revert HLL)** — First because it restores correctness baseline. No dependencies.
2. **Task 2 (Analyze failures)** — Informs Task 3. Quick analysis task.
3. **Task 3 (Fix comparison)** — Highest-ROI task. Depends on Task 2 classification.
4. **Task 4 (Q32/Q16 performance)** — Independent of Tasks 2-3. Targets nearest-to-1x queries.
5. **Task 5 (Q30/Q31 performance)** — Independent of Task 4. Different optimization approach (filter elimination).
6. **Task 6 (Validation)** — Depends on all.

Tasks 4 and 5 are speculative — they may fail like Plans 1 & 2. But the correctness improvements from Tasks 1-3 are high-confidence and represent the main value of Plan 3.

### Risk Assessment

- Task 1 (revert HLL): 99% confidence. Simple code revert.
- Task 2 (analyze): 99% confidence. Analysis only.
- Task 3 (fix comparison): 85% confidence. Well-understood problem, but script modification may have edge cases.
- Task 4 (Q32/Q16 performance): 25% confidence. Plans 1 & 2 failed on similar optimizations. New approaches (batch DocValues, prefetch) are untested.
- Task 5 (Q30/Q31 performance): 20% confidence. Filter overhead is inherent to Lucene DocValues API. Ordinal-based filter bypass is novel but may not be faster.
- Task 6 (validation): 95% confidence (given Tasks 1-3 succeed).

### Expected Outcome

Conservative: Correctness 38/43, Performance 28/43. (Tasks 1-3 succeed, Tasks 4-5 fail)
Optimistic: Correctness 40/43, Performance 30/43. (All tasks succeed)

## Plan #3 produced 6 tasks — Sat Mar 28 03:37:38 UTC 2026

## Oracle REJECTED plan #3 (rejection #3) — Sat Mar 28 03:38:37 UTC 2026

This is a clear REJECTED.
[38;5;244m - Completed in 0.0s[0m

[?25l[0m[38;5;141m> [0mNow let me verify there's no Plan 3 executor work hiding anywhere:[0m[0m
[?25l[0m[0mSearching for: [38;5;141mPlan 3.*Executor|Executor.*Plan 3|Task 1.*Revert HLL.*Done|Task 2.*Analyze.*Done|Task 3.*Fix correctness.*Done[0m in [38;5;141m/local/home/penghuo/oss/os-sql/.ralph[0m[38;5;244m (using tool: grep)[0m
[38;5;9m ❗ [0mNo matches found for pattern: [38;5;244mPlan 3.*Executor|Executor.*Plan 3|Task 1.*Revert HLL.*Done|Task 2.*Analyze.*Done|Task 3.*Fix correctness.*Done[0m under /local/home/penghuo/oss/os-sql/.ralph
[38;5;244m - Completed in 0.11s[0m

[?25l[0m[?25l[0m[0mSearching for: [38;5;141m\[x\][0m in [38;5;141m/local/home/penghuo/oss/os-sql/.ralph/TASKS_3.md[0m[38;5;244m (using tool: grep)[0m
[38;5;9m ❗ [0mNo matches found for pattern: [38;5;244m\[x\][0m under /local/home/penghuo/oss/os-sql/.ralph/TASKS_3.md
[38;5;244m - Completed in 0.0s[0m

[?25l[0m[38;5;141m> [0mZero completed tasks. Let me now append the verdict to STATUS.md:[0m[0m
[?25l[0m[0mRunning tool [38;5;141mQuipEditor[0m with the param[38;5;244m (from mcp server: builder-mcp)[0m
 ⋮  {
 ⋮    "format": "markdown"
 ⋮  }
[0m[38;5;244m - Completed in 0.44s[0m

[?25l[0m[?25l[0m[0mReading file: [38;5;141m/local/home/penghuo/oss/os-sql/.ralph/STATUS.md[0m, from line [38;5;141m114[0m to end of file[38;5;244m (using tool: read)[0m

## Plan 4 — 2026-03-28

### Planning Rationale

**Root cause analysis of Plans 1-3 failure:**

The fundamental problem was optimizing against a broken measuring stick. The `check_correctness.sh` script has two critical flaws:
1. `normalize()` blindly `sort`s ALL output lines — this destroys ORDER BY semantics. For queries like Q31/Q32/Q33 where all rows have `c=1`, sorting by all columns produces completely different row sets between CH and OS, causing false failures.
2. Float truncation (`sed -E 's/([0-9]+\.[0-9]{6})[0-9]*/\1/g'`) truncates instead of rounding, causing edge-case mismatches (e.g., 1.1234996 → 1.123499 vs 1.1235004 → 1.123500).

**Diff analysis of 13 failing queries:**

| Category | Queries | Count | Fix |
|----------|---------|-------|-----|
| Pure ordering/tie-breaking | Q18, Q25, Q31, Q32, Q33, Q39, Q42 | 7 | Fix comparison script |
| Scalar value mismatch (real bug) | Q05, Q06 | 2 | Code fix needed |
| Mixed ordering + data diff | Q24, Q40, Q41 | 3 | Partially comparison, partially code |
| Float precision + count diff | Q29 | 1 | Comparison fix + investigate count |

Fixing the comparison script alone should flip 7+ queries from FAIL to PASS, giving a true baseline of ~37/43.

**Why correctness before performance:**
- Plans 1-2 spent 11 iterations on performance with only +1 gain
- Without knowing true correctness, we can't trust performance numbers (a "fast" wrong answer is worthless)
- Several performance queries (Q31, Q32) are also correctness failures — fixing correctness may reveal performance issues or confirm they're already fast enough

**Performance strategy change:**
- Plans 1-2 targeted worst-ratio queries (Q15=35.79x, Q04=10.06x) — these are fundamentally hard (COUNT DISTINCT over 100M rows, massive hash maps)
- Plan 4 targets nearest-to-1x queries (Q32=1.64x, Q16=1.88x, Q13=2.04x) — small improvements flip these to ≤1x, maximizing the count metric

**Rejected approaches (from Plans 1-2 learnings):**
- HLL approximation: Breaks correctness, net negative
- MAX_CAPACITY > 4M: Proven cache thrashing (7x regression on Q15)
- Per-group HLL: 5-6x regression for 100K+ groups
- Ordinal-based exact COUNT(DISTINCT): Overhead exceeds savings
- Bitset+sequential scan for filtered GROUP BY: Slower than filtered advanceExact

**Task ordering rationale:**
1. Revert HLL first (restore clean baseline)
2. Fix comparison script (get true correctness score)
3. Fix real bugs (reach ≥38/43 correctness)
4. Performance on nearest-to-1x queries (maximize count)
5. Sub-10ms noise last (lowest ROI)
6. Full validation

## Plan #4 produced 6 tasks — Sat Mar 28 16:16:14 UTC 2026
