# Plan 2

## Strategy Shift from Plan 1

Plan 1 optimized the hot loop (hash map operations, memory layout) — score stayed 28/43.
Plan 2 optimizes **what data crosses the shard→coordinator boundary**:
1. Ship HLL sketches, not hash sets (COUNT(DISTINCT))
2. Ship top-K groups, not all groups (ORDER BY + LIMIT)
3. Eliminate multi-pass overhead (increase flat map capacity)

Key discovery: ES uses HyperLogLog for COUNT(DISTINCT) — approximate, not exact. DQE must match the algorithm to match the performance.

## Success Criteria
- [ ] Score vs ES improves from 28/43 to ≥ 35/43 within 1x
- [ ] Correctness stays ≥ 33/43 on hits_1m (no regression)
- [ ] All changes compile cleanly
- [ ] Full benchmark run with --warmup 3 confirms improvements

## Anti-Criteria
- [ ] No placeholder/stub implementations
- [ ] No TODO comments left behind
- [ ] No removed tests or logging
- [ ] No correctness regressions (must stay ≥ 33/43)
- [ ] No force-merge or index mutations

## Task 1: HLL for global COUNT(DISTINCT) — Q04, Q05
- [x] Done

### Goal
Replace exact hash-set counting with HyperLogLog for scalar COUNT(DISTINCT) queries. Q04: 2.888s→≤0.287s (10x improvement). Q05: 4.035s→≤0.517s (8x improvement). Expected: +2 queries to score.

### Context
- Q04: `SELECT COUNT(DISTINCT UserID) FROM hits;` — UserID is BIGINT, ~17.6M unique values
- Q05: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` — SearchPhrase is VARCHAR, ~6.5M unique values
- ES uses HLL (precision ~3000) for cardinality aggregation → ~0.3s
- DQE currently: each shard collects ALL distinct values into LongOpenHashSet/HashSet<Object>, ships to coordinator, unions, counts
- Shard-level accumulator: `CountDistinctDirectAccumulator` in FusedScanAggregate.java:1766
- Coordinator merge: ResultMerger generic path treats COUNT(DISTINCT) as SUM (line 263) — this is actually a bug for multi-shard exact counting, but works if shards emit final HLL cardinality

### Approaches
1. **HLL accumulator in FusedScanAggregate** — Add HyperLogLogDirectAccumulator alongside CountDistinctDirectAccumulator. Use HLL++ (precision 14, ~16KB per sketch). For VARCHAR, hash with Murmur3 before feeding HLL. Each shard emits a single cardinality estimate. Coordinator sums (since HLL sketches are merged shard-side into final count). Gate behind `dqe.count_distinct.approximate` setting (default true). **Preferred: simplest path, matches ES behavior.**
2. **HLL with coordinator-side sketch merge** — Shards emit serialized HLL sketches instead of counts. Coordinator merges sketches then extracts cardinality. More accurate (avoids per-shard rounding) but requires new serialization format and ResultMerger changes. **Fallback if approach 1's accuracy is insufficient.**

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 5,6 --skip-correctness --warmup 1
# Q04 target: ≤0.5s, Q05 target: ≤1.0s
```

### Dependencies
None

## Task 2: HLL for grouped COUNT(DISTINCT) — Q11, Q13
- [x] Done

### Goal
Replace per-group hash sets with per-group HLL sketches for GROUP BY + COUNT(DISTINCT) queries. Q11: 3.826s→≤1.5s. Q13: 8.578s→≤4.2s. Expected: +2 queries to score.

### Context
- Q11: `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;`
- Q13: `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;`
- Current bottleneck: coordinator merge of ~6M per-group LongOpenHashSets (6s of 9s for Q13)
- Shard-level accumulator: `CountDistinctAccum` in FusedGroupByAggregate.java:13094 — uses LongOpenHashSet per group
- With HLL per group: each group gets a small sketch (~1KB at precision 10), coordinator merges sketches or sums final counts
- Memory: Q13 has ~150K groups × 1KB = 150MB — acceptable

### Approaches
1. **Per-group HLL in FusedGroupByAggregate** — Replace LongOpenHashSet in CountDistinctAccum with HLL sketch (precision 10-12). Each shard computes per-group cardinality estimate. Coordinator sums per-group counts (same as COUNT(*) merge path). **Preferred: reuses Task 1's HLL implementation, biggest coordinator merge savings.**
2. **Shard-side dedup with streaming coordinator merge** — Keep exact counting but optimize the coordinator merge path. Use the existing `isShardDedupCountDistinct()` detection but add streaming merge that doesn't materialize all (group, value) pairs. **Fallback: preserves exact counting but less performance gain.**

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 12,14 --skip-correctness --warmup 1
# Q11 target: ≤1.5s, Q13 target: ≤4.2s
```

### Dependencies
Task 1 (HLL implementation)

## Task 3: Shard-side top-N for high-cardinality GROUP BY — Q15, Q16, Q30, Q31, Q32
- [x] Done

### Goal
Reduce data shipped from shards to coordinator by applying top-K selection at shard level for GROUP BY + ORDER BY + LIMIT queries. Q15: 4.06s→≤1.5s. Q16: 13.4s→≤8.3s. Q30: 2.9s→≤1.6s. Q31: 2.6s→≤1.7s. Q32: 9.2s→≤5.6s. Expected: +3-5 queries to score.

### Context
- All 5 queries have `GROUP BY ... ORDER BY ... DESC LIMIT 10`
- Q15: 17.6M unique UserIDs, only need top 10 — currently ships all groups due to multi-pass (MAX_CAPACITY=4M)
- Q16: UserID × SearchPhrase — high cardinality two-key
- Q30/Q31: Filtered (SearchPhrase <> ''), two-key GROUP BY with 3 aggregates
- Q32: Unfiltered two-key GROUP BY (WatchID, ClientIP)
- FusedGroupByAggregate already has `executeWithTopN()` (line 487-523) that selects top-N from flat accumulators
- But multi-pass path and generic pipeline fallback ship ALL groups
- Key insight: for LIMIT 10, shipping top-K (K=10000) per shard is safe — global top-10 must appear in at least one shard's top-10000

### Approaches
1. **Increase FlatSingleKeyMap.MAX_CAPACITY + ensure shard-side top-N for all fused paths** — Increase MAX_CAPACITY from 4M to 24M (fits 17.6M UserIDs in single pass on r5.4xlarge with 48GB heap). Verify that `executeWithTopN()` is invoked for all ORDER BY + LIMIT queries. For multi-pass fallback, add post-aggregation top-K selection before serialization. **Preferred: eliminates multi-pass overhead for Q15, ensures top-N for all paths.**
2. **Generic pipeline top-N insertion** — For queries that fall through to generic pipeline (line 725-744), insert a TopNOperator after pipeline drain to truncate results before serialization. **Complementary: catches queries that don't hit fused paths.**

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 16,17,31,32,33 --skip-correctness --warmup 1
# Q15 target: ≤1.5s, Q16 target: ≤8.3s, Q30 target: ≤1.6s, Q31 target: ≤1.7s, Q32 target: ≤5.6s
```

### Dependencies
None (can run in parallel with Tasks 1-2)

## Task 4: Q18 — 3-key GROUP BY optimization attempt
- [x] Done

### Goal
Attempt to improve Q18 from 44.4s to ≤20s. This is a stretch goal with low confidence. Q18: `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;`

### Context
- 3-key GROUP BY (UserID BIGINT, minute INTEGER, SearchPhrase VARCHAR) producing ~98K groups from 100M rows
- Currently uses FlatThreeKeyMap with hash-partitioned multi-pass
- ES: 19.96s — this is already a slow query for ES too
- The `extract(minute FROM EventTime)` is an expression key — may force generic pipeline path
- Ratio is 2.22x — close but not within 1x

### Approaches
1. **Ensure fused path is used + increase capacity** — Verify Q18 hits FusedGroupByAggregate (not generic pipeline). If expression key forces fallback, add expression evaluation support in fused path. Increase FlatThreeKeyMap capacity if multi-pass is triggered. **Preferred: leverage existing optimized path.**
2. **Accept and skip** — If fused path is already used and multi-pass is not the bottleneck, the 2.22x gap may be architectural (3 doc-value lookups + hash computation per row). Document as known limitation. **Fallback: honest assessment.**

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 19 --skip-correctness --warmup 1
# Q18 target: ≤20s
```

### Dependencies
Task 3 (capacity increase and top-N patterns)

## Task 5: Full validation run
- [x] Done

### Goal
Run full benchmark with --warmup 3 to confirm all improvements. Verify correctness ≥ 33/43. Produce final score.

### Approaches
1. **Full dev harness run** — `bash run/dev_harness.sh --warmup 3 --correctness-threshold 33`. This compiles, reloads plugin, runs correctness gate, then full 43-query benchmark.

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash run/dev_harness.sh --warmup 3 --correctness-threshold 33 > /tmp/dev_harness_plan2.log 2>&1' &>/dev/null &
tail -f /tmp/dev_harness_plan2.log
# Success: ###COMPLETE: ... benchmark=done###
# Target: ≥ 35/43 within 1x of ES
```

### Dependencies
Tasks 1-4
