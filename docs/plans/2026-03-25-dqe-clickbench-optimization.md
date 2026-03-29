# DQE ClickBench Optimization Plan

> **For Kiro:** Use `#executing-plans` for sequential execution.

**Goal:** Optimize DQE to reach 43/43 ClickBench queries within 2x of ClickHouse-Parquet baseline on 100M rows.

**Architecture:** Iterative optimize-measure loop. Each change: compile → reload-plugin → correctness gate → benchmark. Attack queries by category, highest leverage first.

**Tech Stack:** OpenSearch 3.6.0-SNAPSHOT DQE (Java 21), ClickBench 43 queries, ClickHouse-Parquet c6a.4xlarge baseline.

---

## Current State (as of iteration 2, 2026-03-25T18:50Z)

**Environment:** ✅ Fully operational
- OpenSearch 3.6.0-SNAPSHOT, 32GB heap, 4-shard `hits` (99,997,497 docs), 8-shard `hits_1m` (1,000,000 docs)
- ClickHouse with both tables loaded for correctness reference
- All benchmark scripts working

**Score:** 24/43 within 2x (from full benchmark at `/tmp/full-iter2/r5.4xlarge.json`)

**Correctness:** 29/43 pass on 1M (14 failures are tie-breaking diffs, not data bugs)

**Pending code change:** Hash-partitioned multi-pass for single-key flat GROUP BY (FusedGroupByAggregate.java, +150 lines). Compiles but untested. Targets Q15 (143x → should drop to ~5-10x).

### 19 Failing Queries

| Q | Ratio | OS(s) | CH(s) | Target(s) | Speedup Needed | Category |
|---|-------|-------|-------|-----------|----------------|----------|
| Q15 | 143.4x | 74.563 | 0.520 | 1.040 | 71.7x | B: High-card GROUP BY |
| Q39 | 27.0x | 3.860 | 0.143 | 0.286 | 13.5x | E: Multi-key CASE+VARCHAR |
| Q11 | 13.1x | 3.514 | 0.268 | 0.536 | 6.6x | A: COUNT(DISTINCT) |
| Q18 | 10.7x | 39.180 | 3.668 | 7.336 | 5.3x | B: High-card GROUP BY |
| Q09 | 8.0x | 4.908 | 0.614 | 1.228 | 4.0x | A: COUNT(DISTINCT) |
| Q13 | 7.8x | 7.476 | 0.956 | 1.912 | 3.9x | A: COUNT(DISTINCT) |
| Q16 | 6.9x | 12.458 | 1.794 | 3.588 | 3.5x | B: High-card GROUP BY |
| Q05 | 5.8x | 4.030 | 0.690 | 1.380 | 2.9x | A: COUNT(DISTINCT) |
| Q04 | 5.7x | 2.455 | 0.434 | 0.868 | 2.8x | A: COUNT(DISTINCT) |
| Q36 | 5.0x | 0.610 | 0.121 | 0.242 | 2.5x | D: Filtered VARCHAR GB |
| Q32 | 2.5x | 11.289 | 4.493 | 8.986 | 1.3x | B: High-card GROUP BY |
| Q30 | 4.5x | 3.486 | 0.778 | 1.556 | 2.2x | D: Filtered GROUP BY |
| Q37 | 4.5x | 0.454 | 0.102 | 0.204 | 2.2x | D: Filtered VARCHAR GB |
| Q02 | 4.1x | 0.426 | 0.105 | 0.210 | 2.0x | C: Scalar agg |
| Q08 | 4.1x | 2.220 | 0.540 | 1.080 | 2.1x | A: COUNT(DISTINCT) |
| Q35 | 4.0x | 1.485 | 0.370 | 0.740 | 2.0x | D: Filtered GROUP BY |
| Q28 | 3.2x | 30.013 | 9.526 | 19.052 | 1.6x | D: REGEXP GROUP BY |
| Q29 | 3.0x | 0.291 | 0.096 | 0.192 | 1.5x | C: Scalar agg |
| Q14 | 2.1x | 1.545 | 0.735 | 1.470 | 1.1x | D: Borderline |

### Root Cause Summary

| Category | Queries | Root Cause | Key Files |
|----------|---------|------------|-----------|
| A: COUNT(DISTINCT) | Q04,Q05,Q08,Q09,Q11,Q13 | Calcite 2-level agg doubles key space; per-group HashSet overhead | TransportShardExecuteAction.java, FusedGroupByAggregate.java |
| B: High-card GROUP BY | Q15,Q16,Q18,Q32 | FlatSingleKeyMap 8M cap → multi-pass; VARCHAR keys use JDK HashMap with MergedGroupKey GC pressure | FusedGroupByAggregate.java |
| C: Scalar agg | Q02,Q29 | Lucene DocValues decoding overhead vs ClickHouse native columnar | FusedScanAggregate.java |
| D: Filtered/borderline | Q14,Q28,Q30,Q35,Q36,Q37 | Various: regex compilation, filter selectivity, VARCHAR GROUP BY | FusedGroupByAggregate.java |
| E: Multi-key CASE+VARCHAR | Q39 | 5-key GROUP BY with CASE expression + VARCHAR Referer/URL | FusedGroupByAggregate.java |

### DQE Code Map (key files)

| File | Lines | Purpose |
|------|-------|---------|
| `dqe/.../shard/transport/TransportShardExecuteAction.java` | ~2200 | Shard dispatch, pattern detection, COUNT(DISTINCT) fast paths |
| `dqe/.../shard/source/FusedGroupByAggregate.java` | ~12700 | All GROUP BY execution: flat maps, parallel scanning, bucket partitioning |
| `dqe/.../shard/source/FusedScanAggregate.java` | ~1800 | Scalar aggregation (no GROUP BY): column-major, algebraic shortcuts |
| `dqe/.../coordinator/transport/TransportTrinoSqlAction.java` | ~4200 | Coordinator: shard dispatch, result merging, COUNT(DISTINCT) merge |
| `dqe/.../coordinator/ResultMerger.java` | ~3300 | GROUP BY result merge: fast numeric/varchar/mixed paths, top-N heap |

### Key Architecture Insights

- **Flat maps** (`FlatSingleKeyMap`, `FlatTwoKeyMap`, `FlatThreeKeyMap`): Open-addressing, primitive `long[]` keys + contiguous `long[] accData`. Zero per-group object allocation. MAX_CAPACITY = 8M. Load factor 0.7.
- **VARCHAR GROUP BY**: Per-segment ordinals → cross-segment `MergedGroupKey` (Object[] wrapper) in JDK `HashMap`. Heavy GC at high cardinality.
- **Intra-shard parallelism**: Segment-level (workers get disjoint segments, own flat maps, merged via `mergeFrom()`). Doc-range for VARCHAR. Bucket-level for >8M groups.
- **Hash-partitioned multi-pass**: Two-key and three-key paths have it. Single-key path now has it (pending change). Pattern: `hash(key) % numBuckets == bucket` filter in hot loop.
- **COUNT(DISTINCT)**: Shard builds `LongOpenHashSet` per group (numeric) or `HashSet<String>` (varchar). Coordinator unions sets. Transient fields for local-node optimization.
- **Scalar agg**: Column-major `tryFlatArrayPath` (parallel segments, no virtual dispatch). Algebraic shortcut for `SUM(col+k) = SUM(col) + k*COUNT(*)`.

---

## Task 1: Test Pending Multi-Pass Change (Q15)

The hash-partitioned multi-pass for single-key flat GROUP BY is already implemented but untested.

**Step 1: Reload plugin**

```bash
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin
```

Expected: ~3 min.

**Step 2: Correctness gate**

```bash
bash run/run_all.sh correctness
```

Expected: >= 29/43 (no regression from baseline).

**Step 3: Benchmark Q15**

```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 16 --output-dir /tmp/q15-multipass
```

Q15 = `--query 16` (1-based). Baseline: 74.563s. Target: ≤1.040s. Expect significant improvement from multi-pass.

**Step 4: If Q15 improved, benchmark all Category B**

```bash
for q in 16 17 19 33; do
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/catB-$q
done
```

Q15=16, Q16=17, Q18=19, Q32=33.

**Step 5: Commit if improvement confirmed**

```bash
cd /local/home/penghuo/oss/os-sql
git add dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java
git commit -m "perf: hash-partitioned multi-pass for single-key flat GROUP BY"
```

---

## Task 2: Category A — COUNT(DISTINCT) Optimization (Q04,Q05,Q08,Q09,Q11,Q13)

6 queries, 4-13x slower. The DQE already has fast COUNT(DISTINCT) paths (LongOpenHashSet, parallel segment scanning). The issue is that some query shapes don't match the fast-path detection patterns.

**Step 1: Profile each query to identify which path it takes**

Run each query individually and check if it hits the fused path or falls back to generic pipeline:

```bash
for q in 5 6 9 10 12 14; do
    echo "=== Q$(($q-1)) (--query $q) ==="
    time curl -sf -XPOST 'http://localhost:9200/_plugins/_trino_sql' \
      -H 'Content-Type: application/json' \
      -d "{\"query\":\"$(sed -n "${q}p" queries/queries_trino.sql)\"}" > /dev/null 2>&1
done
```

**Step 2: For each query not hitting the fast path, identify the detection gap**

Read `TransportShardExecuteAction.java` dispatch logic (lines 246-360) and check why the query doesn't match.

Common gaps:
- Q04/Q05: Scalar COUNT(DISTINCT) — should hit `executeDistinctValuesScanWithRawSet`. If not, check if PlanFragmenter strips the AggNode correctly.
- Q08/Q09: Grouped COUNT(DISTINCT) with mixed aggs (SUM/COUNT alongside COUNT(DISTINCT)) — may not match the 2-key dedup pattern.
- Q11: VARCHAR key + COUNT(DISTINCT) — check `executeVarcharCountDistinctWithHashSets` detection.
- Q13: VARCHAR key + COUNT(DISTINCT) with filter — check if WHERE clause prevents fast path.

**Step 3: Implement detection fixes**

Modify `TransportShardExecuteAction.java` to widen fast-path detection for each missing pattern.

**Step 4: Compile → Reload → Correctness → Benchmark**

```bash
./gradlew :dqe:compileJava
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin
bash run/run_all.sh correctness
for q in 5 6 9 10 12 14; do
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/catA-$q
done
```

**Step 5: Commit**

```bash
git add -A && git commit -m "perf: widen COUNT(DISTINCT) fast-path detection for Category A"
```

---

## Task 3: Category B Remaining — High-Cardinality GROUP BY (Q16,Q18,Q32)

Q15 addressed in Task 1. Q32 only needs 1.3x speedup (borderline). Q16 and Q18 are the hard ones.

- Q16: `GROUP BY UserID, SearchPhrase` — 2 keys, mixed long+varchar, 6.9x
- Q18: `GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase` — 3 keys, mixed, 10.7x
- Q32: `GROUP BY WatchID, ClientIP` — 2 numeric keys, 2.5x (already has multi-pass)

**Step 1: Profile Q16 and Q18 to identify bottleneck**

Check if they use flat two-key/three-key paths or fall back to VARCHAR N-key path.

**Step 2: For Q32 (2 numeric keys, 2.5x), check if multi-pass bucket count is optimal**

Q32 uses `executeTwoKeyNumericFlat` with hash-partitioned multi-pass. The 2.5x gap may be from too many passes or coordinator merge overhead.

**Step 3: For Q16/Q18 (mixed varchar+numeric), explore optimizations**

Options:
- Global ordinal encoding for SearchPhrase → convert to numeric key → use flat path
- Increase FlatTwoKeyMap/FlatThreeKeyMap MAX_CAPACITY (memory tradeoff)
- Parallel doc-range scanning improvements

**Step 4: Compile → Reload → Correctness → Benchmark**

```bash
./gradlew :dqe:compileJava
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin
bash run/run_all.sh correctness
for q in 17 19 33; do
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/catB2-$q
done
```

**Step 5: Commit**

```bash
git add -A && git commit -m "perf: high-cardinality GROUP BY optimizations for Q16/Q18/Q32"
```

---

## Task 4: Category D — Filtered/Borderline Queries (Q14,Q28,Q30,Q35,Q36,Q37)

6 queries, 1.1x-2.5x speedup needed. Targeted micro-optimizations.

**Step 1: Profile each query**

```bash
for q in 15 29 31 36 37 38; do
    echo "=== Q$(($q-1)) (--query $q) ==="
    time curl -sf -XPOST 'http://localhost:9200/_plugins/_trino_sql' \
      -H 'Content-Type: application/json' \
      -d "{\"query\":\"$(sed -n "${q}p" queries/queries_trino.sql)\"}" > /dev/null 2>&1
done
```

Q14=15, Q28=29, Q30=31, Q35=36, Q36=37, Q37=38.

**Step 2: Targeted fixes per query**

- **Q14 (2.1x, SearchEngineID+SearchPhrase GROUP BY):** Nearly there. Check if filter pushdown is working.
- **Q28 (3.2x, REGEXP_REPLACE GROUP BY):** Cache compiled `java.util.regex.Pattern`. Currently recompiles per row.
- **Q30 (4.5x, SearchEngineID+ClientIP GROUP BY with filter):** Check filter selectivity and GROUP BY path.
- **Q35 (4.0x, ClientIP+derived keys GROUP BY):** 4 GROUP BY keys with expressions. Check if expression evaluation is fused.
- **Q36 (5.0x, URL GROUP BY with filter):** VARCHAR GROUP BY on filtered subset. Check ordinal pre-aggregation.
- **Q37 (4.5x, Title GROUP BY with filter):** Same pattern as Q36 but different column.

**Step 3: Compile → Reload → Correctness → Benchmark per fix**

**Step 4: Commit**

```bash
git add -A && git commit -m "perf: targeted fixes for Category D borderline queries"
```

---

## Task 5: Category C — Scalar Aggregation (Q02,Q29)

2 queries, 2.0x and 1.5x speedup needed. Root cause is Lucene DocValues decoding overhead.

- **Q02:** `SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)` — check if `tryFlatArrayPath` is used or falls back to row-major.
- **Q29:** `SUM(ResolutionWidth+0..89)` — algebraic shortcut already applied. 1.5x gap is DocValues throughput.

**Step 1: Check if Q02 hits tryFlatArrayPath**

If AdvEngineID or ResolutionWidth is DoubleType, flat path is ineligible. Check column types.

**Step 2: If flat path ineligible, add DoubleType support to tryFlatArrayPath**

**Step 3: For Q29, explore if raw column access (bypassing DocValues) is feasible**

**Step 4: Compile → Reload → Correctness → Benchmark**

```bash
./gradlew :dqe:compileJava
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin
bash run/run_all.sh correctness
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 3 --output-dir /tmp/q02
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 30 --output-dir /tmp/q29
```

**Step 5: Commit**

```bash
git add -A && git commit -m "perf: scalar aggregation optimizations for Q02/Q29"
```

---

## Task 6: Category E — Q39 Multi-Key CASE+VARCHAR (27x)

Q39: `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN ... THEN Referer ELSE '' END, URL, COUNT(*) FROM hits WHERE ... GROUP BY ...` — 5 GROUP BY keys including CASE expression and VARCHAR URL/Referer.

This is the hardest query. 27x gap with 5 keys including computed VARCHAR expressions.

**Step 1: Profile Q39 execution path**

**Step 2: Identify if CASE expression prevents fused path**

**Step 3: Explore optimizations (expression fusion, ordinal encoding)**

**Step 4: Compile → Reload → Correctness → Benchmark**

```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 40 --output-dir /tmp/q39
```

**Step 5: Commit**

---

## Task 7: Full Benchmark & Final Verification

**Step 1: Run correctness**

```bash
cd benchmarks/clickbench && bash run/run_all.sh correctness
```

Must be >= 29/43 (no regression).

**Step 2: Run full benchmark**

```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full-final
```

**Step 3: Compare against baseline**

```bash
python3 -c "
import json, glob
with open('results/performance/clickhouse_parquet_official/c6a.4xlarge.json') as f:
    ch = json.load(f)
os_file = glob.glob('/tmp/full-final/*.json')[0]
with open(os_file) as f:
    os_data = json.load(f)
within = 0
for i in range(43):
    ch_best = min(ch['result'][i])
    os_times = [x for x in os_data['result'][i] if x is not None]
    os_best = min(os_times) if os_times else 999
    ratio = os_best / ch_best if ch_best > 0 else 999
    if ratio <= 2.0: within += 1
    else: print(f'Q{i:02d}: {ratio:.1f}x ({os_best:.3f}s vs {ch_best:.3f}s)')
print(f'\n{within}/43 within 2x')
"
```

**Step 4: Save results and commit**

```bash
cp /tmp/full-final/*.json benchmarks/clickbench/results/performance/opensearch/
git add -A && git commit -m "bench: final results — N/43 within 2x"
```

Target: 43/43.

---

## Dev Iteration Loop (per fix)

```
1. Code change
2. ./gradlew :dqe:compileJava                              (~5s)
3. cd benchmarks/clickbench && bash run/run_all.sh reload-plugin  (~3 min)
4. bash run/run_all.sh correctness                          (~2 min, must not regress)
5. bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query N --output-dir /tmp/qN
6. git commit if improvement confirmed
```

**CRITICAL: Never run reload-plugin while a benchmark is running.**

## Query Numbering Reference

| Query (0-based) | `--query N` (1-based) | SQL (first 80 chars) |
|------------------|-----------------------|----------------------|
| Q02 | 3 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits` |
| Q04 | 5 | `SELECT COUNT(DISTINCT UserID) FROM hits` |
| Q05 | 6 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` |
| Q08 | 9 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID...` |
| Q09 | 10 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(...), COUNT(DISTINCT UserID)...` |
| Q11 | 12 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID)...` |
| Q13 | 14 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase...` |
| Q14 | 15 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase...` |
| Q15 | 16 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` |
| Q16 | 17 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase...` |
| Q18 | 19 | `SELECT UserID, extract(minute FROM EventTime), SearchPhrase, COUNT(*)...` |
| Q28 | 29 | `SELECT REGEXP_REPLACE(Referer, ...) AS k, AVG(length(Referer)), COUNT(*)...` |
| Q29 | 30 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ... (90 SUMs)` |
| Q30 | 31 | `SELECT SearchEngineID, ClientIP, COUNT(*) FROM hits WHERE SearchPhrase <> ''...` |
| Q32 | 33 | `SELECT WatchID, ClientIP, COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)...` |
| Q35 | 36 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*)...` |
| Q36 | 37 | `SELECT URL, COUNT(*) FROM hits WHERE CounterID = 62 AND ...` |
| Q37 | 38 | `SELECT Title, COUNT(*) FROM hits WHERE CounterID = 62 AND ...` |
| Q39 | 40 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN ... Referer...` |
