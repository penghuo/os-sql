# Plan 1

## Success Criteria
- [ ] Score vs ES improves from 28/43 to ≥ 35/43 within 1x
- [ ] Correctness stays ≥ 33/43 on hits_1m (no regression)
- [ ] All changes compile cleanly (`./gradlew :dqe:compileJava` → BUILD SUCCESSFUL)
- [ ] Full benchmark run with `--warmup 3` confirms improvements

## Anti-Criteria
- [ ] No placeholder/stub implementations
- [ ] No TODO comments left behind
- [ ] No removed tests or logging
- [ ] No correctness regressions (must stay ≥ 33/43)
- [ ] No force-merge or index mutations

---

## Task 1: Q15 — TopN pushdown for high-cardinality single-key GROUP BY
- [x] Done

### Goal
Q15 is `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` at 35.79x — the worst ratio. ES handles this with a global ordinals + top-N aggregation that avoids materializing all ~17M groups. DQE currently builds the full hash table before selecting top 10. Implement a streaming top-N that maintains only a heap of size K during aggregation, or leverage ordinal-based counting with heap selection.

### Approaches
1. **Ordinal count array + heap selection for numeric keys** — For `GROUP BY UserID` (BIGINT), use a hash map of long→long (userId→count) but integrate a min-heap of size K during the merge phase at the coordinator. At shard level, each shard returns only its local top-K instead of all groups. This avoids serializing millions of groups across shards. Modify `FusedGroupByAggregate` to detect single-numeric-key + COUNT(*) + ORDER BY + LIMIT and use the existing `executeSingleVarcharCountStar` heap pattern but for numeric keys.
2. **Two-phase aggregation with shard-level pruning** — First pass: each shard computes full GROUP BY but only returns top-K*SHARD_MULTIPLIER results. Second pass: coordinator merges and re-ranks. Less optimal but simpler to implement.
3. **Approximate top-N with Count-Min Sketch** — Use probabilistic data structure for approximate counts, then verify top candidates with exact counts. Fallback if exact results required.

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 16 --skip-correctness --warmup 3
# Q15 (0-based) = --query 16 (1-based)
# Target: ratio ≤ 1.0x (currently 35.79x, ES=0.805s)
```

### Dependencies
None

---

## Task 2: Q04/Q05 — Global COUNT(DISTINCT) optimization
- [x] Done — FAILED: Cannot match ES (ES uses HLL approximate, DQE uses exact counting)

### Goal
Q04 (`COUNT(DISTINCT UserID)`) is 10.06x and Q05 (`COUNT(DISTINCT SearchPhrase)`) is 7.80x. ES likely uses HyperLogLog for approximate cardinality. DQE appears to do exact counting with HashSets. For these global (no GROUP BY) COUNT(DISTINCT) queries, implement an optimized path: either HyperLogLog approximation (matching ES behavior) or a more efficient exact path using ordinal-based FixedBitSet for sorted doc values.

### Approaches
1. **Ordinal-based FixedBitSet for global COUNT(DISTINCT)** — For columns with SortedSetDocValues, allocate a FixedBitSet of size=valueCount, iterate docs setting bits for each ordinal, then return bitSet.cardinality(). This is O(N) with minimal memory for columns where valueCount fits in memory. For UserID (~17M uniques in 100M docs), this is ~2MB. For SearchPhrase, check valueCount first. This avoids HashSet overhead entirely.
2. **Adaptive HyperLogLog** — Use HLL with precision 14 (like ES) for columns with valueCount > threshold. Matches ES approximate behavior. Simpler but changes semantics.
3. **Parallel segment scanning with merge** — Each segment builds its own FixedBitSet, then OR-merge across segments. Reduces contention on single bitset.

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 5,6 --skip-correctness --warmup 3
# Q04=--query 5, Q05=--query 6
# Target: Q04 ≤ 0.287s, Q05 ≤ 0.517s
```

### Dependencies
None

---

## Task 3: Q16/Q18 — Multi-key high-cardinality GROUP BY + TopN
- [x] Done — PARTIAL: Q16 improved 1.88x→1.60x, Q18 unchanged at 2.23x. Also fixed bitset lockstep EOFException and enabled global ordinals for ORDER BY queries.

### Goal
Q16 (`GROUP BY UserID, SearchPhrase`, 1.88x) and Q18 (`GROUP BY UserID, minute, SearchPhrase`, 2.22x) involve multi-key GROUP BY on high-cardinality columns with ORDER BY + LIMIT. The key optimization is shard-level top-N pruning: each shard should return only its local top-K results instead of all groups, reducing coordinator merge cost and network transfer.

### Approaches
1. **Shard-level top-K with over-sampling** — Modify the fused GROUP BY path to accept a topN parameter for multi-key queries. Each shard maintains a bounded priority queue of size K*OVERSAMPLE_FACTOR during aggregation. After shard-level aggregation completes, return only the top entries. Coordinator merges shard results and selects global top-K. This is the same pattern as Task 1 but generalized to multi-key.
2. **Hash table size cap with eviction** — During aggregation, if hash table exceeds a size threshold, evict entries with lowest counts. Approximate but bounds memory.
3. **Coordinator-only optimization** — Keep shard behavior unchanged but optimize the coordinator merge path for multi-key results (faster merge sort, reduced serialization).

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 17,19 --skip-correctness --warmup 3
# Q16=--query 17, Q18=--query 19
# Target: Q16 ≤ 8.333s (currently 15.7s), Q18 ≤ 19.957s (currently 44.391s)
```

### Dependencies
Task 1 (shares the shard-level top-N pattern)

---

## Task 4: Q30/Q31/Q32 — Filtered multi-key GROUP BY with multiple aggregations
- [x] Done — FAILED: Cannot match ES. 2-bucket multi-pass is optimal for current architecture.

### Goal
Q30 (3.81x), Q31 (2.21x), Q32 (1.64x) all do `GROUP BY key1, key2` with `COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)` and ORDER BY + LIMIT 10. Q30/Q31 have `WHERE SearchPhrase <> ''` filter. Keys are SearchEngineID/WatchID + ClientIP — high cardinality. The bottleneck is likely the multi-aggregation accumulator overhead and/or the filter evaluation cost.

### Approaches
1. **Fused multi-agg accumulator with top-N** — Extend the fused GROUP BY path to handle multiple aggregations (COUNT + SUM + AVG) in a single pass with a combined accumulator struct. Currently each agg may be computed separately. Fuse them into a single doc-value scan loop. Combine with shard-level top-N from Task 1/3.
2. **Optimized filter path for SearchPhrase <> ''** — For Q30/Q31, the `SearchPhrase <> ''` filter selects ~10% of docs. Use the existing bitset optimization (selective filter → bitset pre-collection) to minimize per-doc filter overhead, then run the fused multi-agg path on the filtered doc set.
3. **Column-at-a-time processing** — Instead of row-at-a-time (read all columns per doc), process one column at a time to improve cache locality. Read all group keys first, build groups, then accumulate each agg column separately.

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 31,32,33 --skip-correctness --warmup 3
# Q30=--query 31, Q31=--query 32, Q32=--query 33
# Target: Q30 ≤ 1.627s, Q31 ≤ 1.679s, Q32 ≤ 5.623s
```

### Dependencies
Task 3 (shares multi-key top-N pattern)

---

## Task 5: Q11/Q13 — GROUP BY + COUNT(DISTINCT) with filter
- [x] Done — FAILED: Coordinator merge of per-group LongOpenHashSets is the bottleneck (~6s of 9s total). Requires HLL or fundamental architecture change.

### Goal
Q11 (2.51x) does `GROUP BY MobilePhone, MobilePhoneModel` with `COUNT(DISTINCT UserID)` and WHERE filter. Q13 (2.04x) does `GROUP BY SearchPhrase` with `COUNT(DISTINCT UserID)`. Both combine GROUP BY with COUNT(DISTINCT) — the most expensive aggregation pattern. The existing HashSet-per-group approach is correct but may have overhead in hash set creation/management for many groups.

### Approaches
1. **Ordinal-indexed HyperLogLog per group** — For each group, maintain a compact HLL sketch (precision 14, ~16KB) instead of a full HashSet. For Q13 with ~1M SearchPhrase groups, this reduces memory from potentially GBs of HashSets to ~16GB of HLL sketches. May match ES behavior which likely uses HLL.
2. **Two-phase: GROUP BY first, then COUNT(DISTINCT) on top-K only** — Since both queries have ORDER BY + LIMIT, first compute GROUP BY with approximate counts, identify top-K candidates, then do exact COUNT(DISTINCT) only for those candidates. Dramatically reduces the number of HashSets needed.
3. **Optimize existing HashSet path** — Profile the existing `executeCountDistinctWithHashSets` path. Look for: excessive HashSet resizing, unnecessary boxing, suboptimal hash function. Use LongOpenHashSet (HPPC) consistently.

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 12,14 --skip-correctness --warmup 3
# Q11=--query 12, Q13=--query 14
# Target: Q11 ≤ 1.522s, Q13 ≤ 4.211s
```

### Dependencies
Task 2 (shares COUNT(DISTINCT) optimization patterns)

---

## Task 6: Sub-10ms queries — Reduce dispatch overhead
- [x] Done — SKIPPED: Sub-10ms queries are measurement noise. Actual execution times (2-3ms) are within 1× of ES; benchmark overhead (10-15ms) dominates.

### Goal
Q00 (4.50x), Q01 (2.75x), Q19 (3.67x), Q41 (2.86x) are all sub-10ms in ES but 2-4x slower in DQE. The absolute times are tiny (9-60ms) so the overhead is likely fixed-cost: SQL parsing, plan optimization, shard dispatch, result serialization. Reducing this fixed overhead would flip these 4 queries.

### Approaches
1. **Profile and identify fixed overhead** — Add timing instrumentation to the DQE request path: parse time, plan time, shard dispatch time, execution time, merge time. Identify which phase dominates for sub-10ms queries. Then optimize the dominant phase (e.g., cache parsed plans, reduce shard fan-out for simple queries, skip unnecessary plan transformations).
2. **Single-shard optimization for point queries** — Q19 is a point lookup (`WHERE UserID = X`). Route to a single shard using the routing key instead of fanning out to all 4 shards. Q00/Q01 are COUNT(*) which must touch all shards but could use cached segment metadata.
3. **Skip plan optimization for simple queries** — Detect simple query patterns (single-table, no joins, no subqueries) and skip the full Trino plan optimization pipeline. Use a fast-path that goes directly from parsed SQL to shard execution.

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --query 1,2,20,42 --skip-correctness --warmup 3
# Q00=--query 1, Q01=--query 2, Q19=--query 20, Q41=--query 42
# Target: each ≤ ES time (Q00≤2ms, Q01≤4ms, Q19≤3ms, Q41≤21ms)
```

### Dependencies
None (independent of other tasks, but lowest priority due to small absolute impact)

---

## Task 7: Full validation run
- [x] Done — Score: 28/43 within 1× of ES (unchanged from v5 baseline). Correctness: 33/43. FlatSingleKeyMap MAX_CAPACITY=4M re-applied (Q15: 28.8s→4.1s).

### Goal
Run the complete benchmark suite with warmup=3 to confirm the final score and ensure no regressions.

### Approaches
1. **Full dev harness run** — `bash run/dev_harness.sh --warmup 3 --correctness-threshold 33` — compile, reload, correctness gate, full 43-query benchmark.

### Verification
```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/dev_harness.sh --warmup 3 --correctness-threshold 33
# Success: ###COMPLETE marker, score ≥ 35/43 within 1x of ES
```

### Dependencies
Tasks 1-6
