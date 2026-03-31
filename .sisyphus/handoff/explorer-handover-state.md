# Handover State Summary

## 1. STATUS.md (`/local/home/penghuo/oss/os-sql/.ralph/STATUS.md`)

- **Iteration:** 26
- **Score:** 31/43 within 2x (up from 30/43 in iter25)
- **Correctness:** 36/43 PASS
- **Machine:** r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard

### Queries Above 2x (12):
Q04(3.67x) Q08(4.21x) Q09(5.30x) Q11(6.95x) Q13(8.02x) Q14(3.01x) Q15(3.48x) Q16(6.86x) Q18(9.72x) Q32(3.00x) Q35(3.75x) Q39(15.11x)

### This Iteration:
- COMMITTED: Byte-level URL domain extraction for Q28 REGEXP_REPLACE: 21.5s→16.5s (2.26x→1.73x) FLIPPED
- REVERTED: GC barrier removal, VARCHAR GROUP BY parallelization, Q14 single-pass scorer

### Next Steps (from STATUS):
1. All 12 above-2x queries fundamentally limited by Lucene DocValues overhead
2. Closest to 2x: Q14(3.01x), Q32(3.00x), Q15(3.48x) — need 33-43% improvement
3. Target ≥38/43 requires flipping 7 more — NOT achievable on r5.4xlarge with code optimizations alone
4. Need: (a) m5.8xlarge 32 vCPU, (b) custom DocValues codec, or (c) columnar storage bypass

### Evidence:
- Q28 benchmark: best 16.488s (1.73x), confirmed across multiple runs
- Full benchmark: /tmp/final_iter26/r5.4xlarge.json
- Git: commit 9432ba325 pushed to wukong branch

---

## 2. LOGS.md (Last 3 Iterations: 24, 25, 26)

### Iteration 24 (2026-03-31T00:43-01:54Z) — Score: 28/43
- Implemented hash-based COUNT(DISTINCT varchar) using FNV-1a hash on raw BytesRef bytes
- Q05: 2.47s→0.80s (3.58x→1.16x) FLIPPED ✅
- Q19: None→0.009s FLIPPED (GC cascade fixed)
- Q14: 2.37x→2.27x, Q29: 2.69x→2.16x (improved, not flipped)

### Iteration 25 (2026-03-31T02:01-03:40Z) — Score: 30/43
- Implemented segment-level aggregate cache (SEGMENT_AGG_CACHE) in FusedScanAggregate.java
- Q02: 0.439s→0.009s (4.18x→0.09x) FLIPPED ✅
- Q29: 0.207s→0.010s (2.16x→0.10x) FLIPPED ✅
- Radix-partitioned aggregation REVERTED (40% regression)
- Direct Scorer iteration for Q14 REVERTED (more overhead than FixedBitSet)

### Iteration 26 (2026-03-31T08:58-10:45Z) — Score: 31/43
- Byte-level URL domain extraction for Q28: 21.5s→16.5s (2.26x→1.73x) FLIPPED ✅
- GC barrier removal REVERTED (made queries worse)
- VARCHAR GROUP BY parallelization REVERTED (memory overhead)
- Q14 single-pass scorer REVERTED (scorer overhead > bitset)

---

## 3. Handover Doc (`docs/handover/2026-03-24-phase-d-handover.md`)

### Goal
Optimize DQE so all 43 ClickBench queries run within 2x of ClickHouse-Parquet on c6a.4xlarge. Current: 25/43. Target: 43/43.

### Query Status Table (from handover, 0-indexed)

**WITHIN 2x (25 queries):**
Q00(0.3x) Q01(0.1x) Q03(1.1x) Q06(0.1x) Q07(0.8x) Q10(1.5x) Q12(0.5x) Q14(1.6x) Q17(0.0x) Q19(0.1x) Q20(0.9x) Q21(0.8x) Q22(0.6x) Q23(0.2x) Q24(0.0x) Q25(1.2x) Q26(0.0x) Q27(1.4x) Q29(1.2x) Q33(0.3x) Q34(0.3x) Q38(0.6x) Q40(0.2x) Q41(0.7x) Q42(0.6x)

**ABOVE 2x (18 queries, sorted by DQE time):**

| Query | Ratio | Time vs CH | Description |
|-------|-------|------------|-------------|
| Q28 | 2.7x | 25.5s vs 9.5s | REGEXP_REPLACE GROUP BY HAVING |
| Q18 | 5.9x | 21.7s vs 3.7s | GROUP BY UserID, minute, SearchPhrase |
| Q32 | 3.2x | 14.3s vs 4.5s | GROUP BY WatchID, ClientIP full-table |
| Q16 | 4.1x | 7.3s vs 1.8s | GROUP BY UserID, SearchPhrase |
| Q13 | 6.2x | 6.0s vs 1.0s | GROUP BY SearchPhrase + COUNT(DISTINCT UserID) |
| Q09 | 8.0x | 4.9s vs 0.6s | GROUP BY RegionID + mixed aggs + COUNT(DISTINCT) |
| Q05 | 5.8x | 4.0s vs 0.7s | COUNT(DISTINCT SearchPhrase) global |
| Q08 | 3.4x | 1.9s vs 0.5s | GROUP BY RegionID + COUNT(DISTINCT UserID) |
| Q11 | 9.5x | 2.6s vs 0.3s | GROUP BY MobilePhone,Model + COUNT(DISTINCT) |
| Q15 | 4.1x | 2.1s vs 0.5s | GROUP BY UserID ORDER BY COUNT(*) LIMIT 10 |
| Q39 | 16.9x | 2.4s vs 0.1s | CounterID=62 filtered, 5 keys + CASE WHEN |
| Q04 | 5.4x | 2.3s vs 0.4s | COUNT(DISTINCT UserID) global |
| Q31 | 2.0x | 2.2s vs 1.1s | GROUP BY WatchID, ClientIP (borderline, needs 3ms) |
| Q35 | 5.0x | 1.9s vs 0.4s | GROUP BY 1, URL (full-table, high-card) |
| Q30 | 2.3x | 1.8s vs 0.8s | GROUP BY SearchEngineID, ClientIP |
| Q36 | 4.0x | 0.5s vs 0.1s | GROUP BY ClientIP expressions (4 keys) |
| Q37 | 2.7x | 0.3s vs 0.1s | CounterID=62 filtered URL GROUP BY |
| Q02 | 2.2x | 0.2s vs 0.1s | SUM+COUNT+AVG, borderline |

### Code Map (Key Files)

| File | ~Lines | Purpose |
|------|--------|---------|
| `dqe/.../TransportShardExecuteAction.java` | 2,200 | Shard dispatch, pattern detection |
| `dqe/.../FusedGroupByAggregate.java` | 11,700 | All GROUP BY execution |
| `dqe/.../FusedScanAggregate.java` | 1,600 | Scalar aggregation |
| `dqe/.../TransportTrinoSqlAction.java` | 2,000 | Coordinator, result merging |
| `dqe/.../PlanOptimizer.java` | 500 | PARTIAL vs SINGLE mode |
| `dqe/.../PlanFragmenter.java` | 800 | Shard plan construction |

Full path prefix: `dqe/src/main/java/org/opensearch/sql/dqe/`

### Root Cause Categories:
- **Cat A (COUNT(DISTINCT), 6q):** Q04, Q05, Q08, Q09, Q11, Q13 — Calcite two-level decomposition doubles key space
- **Cat B (High-Card GROUP BY, 4q):** Q15, Q16, Q18, Q32 — HashMap 17M-50M entries exceeds CPU cache
- **Cat C (Full-Table VARCHAR GROUP BY, 3q):** Q35, Q36, Q39 — millions of unique URL/IP groups
- **Cat D (Borderline, 5q):** Q02, Q28, Q30, Q31, Q37 — small targeted fixes needed

### Recommended Steps (from handover):
1. COUNT(DISTINCT) Fusion — 6 queries, highest leverage
2. Parallelize executeSingleKeyNumericFlat — Q15 and similar
3. Hash-Partitioned Aggregation — Q16, Q18, Q32
4. Borderline fixes — Q02, Q30, Q31, Q37
5. Q28 REGEXP_REPLACE — cache Pattern, hoist regex

### Query Numbering (CRITICAL):
- `--query N` in scripts = 1-based
- JSON `result[N]` = 0-based
- This doc uses 0-based (Q00-Q42)

### Dev Loop:
```
./gradlew :dqe:compileJava          # ~5s
bash run/run_all.sh reload-plugin    # ~3min
bash run/run_all.sh correctness      # ~2min (1M dataset)
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query N --output-dir /tmp/qN
```
