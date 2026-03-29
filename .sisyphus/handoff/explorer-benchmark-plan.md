<!-- Metadata -->
<!-- Source: docs/plans/2026-03-25-benchmark-and-optimization.md -->
<!-- Approximate line count: 385 -->

# ClickBench Full Benchmark & DQE Optimization Plan

> **For Kiro:** Use `#executing-plans` for sequential tasks.

**Goal:** Run full 43-query benchmark on 100M, establish current score, then optimize DQE to reach 43/43 within 2x of ClickHouse-Parquet baseline.

**Architecture:** Benchmark first to get fresh numbers, then attack queries by category (A→B→D→C) following the compile→reload→correctness→benchmark loop.

**Tech Stack:** OpenSearch 3.6.0-SNAPSHOT DQE, Java 21, ClickBench 43 queries

---

### Task 1: Run Full 43-Query Benchmark

**Step 1: Run benchmark with warmup**

```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full-baseline
```

Expected: ~25 min. Output in `/tmp/full-baseline/r5.4xlarge.json`.

**Step 2: Compare against ClickHouse-Parquet baseline**

```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
python3 -c "
import json, glob
with open('results/performance/clickhouse_parquet_official/c6a.4xlarge.json') as f:
    ch = json.load(f)
os_file = glob.glob('/tmp/full-baseline/*.json')[0]
with open(os_file) as f:
    os_data = json.load(f)
within = 0
above = []
for i in range(43):
    ch_best = min(ch['result'][i])
    os_times = [x for x in os_data['result'][i] if x is not None]
    if not os_times:
        above.append((i, 999, 0, ch_best))
        continue
    os_best = min(os_times)
    ratio = os_best / ch_best if ch_best > 0 else 999
    if ratio <= 2.0:
        within += 1
    else:
        above.append((i, ratio, os_best, ch_best))
print(f'{within}/43 within 2x\n')
print('ABOVE 2x:')
for i, ratio, os_best, ch_best in sorted(above, key=lambda x: -x[2]):
    print(f'  Q{i:02d}: {ratio:.1f}x ({os_best:.3f}s vs {ch_best:.3f}s)')
"
```

**Step 3: Save baseline results**

```bash
cp /tmp/full-baseline/*.json /local/home/penghuo/oss/os-sql/benchmarks/clickbench/results/performance/opensearch/
```

**Step 4: Commit baseline**

```bash
cd /local/home/penghuo/oss/os-sql
git add benchmarks/clickbench/results/performance/opensearch/
git commit -m "bench: save initial full benchmark baseline"
```

---

### Task 2: Category A — COUNT(DISTINCT) Fusion (Q04, Q05, Q08, Q09, Q11, Q13)

6 queries. Highest leverage. Root cause: Calcite decomposes `COUNT(DISTINCT x)` into two-level aggregation, doubling key space.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java` (~line 280-360, AggDedupNode detection)
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (add fused COUNT(DISTINCT) path)
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java` (scalar COUNT(DISTINCT))

**Step 1: Read the existing COUNT(DISTINCT) code path**

Read `TransportShardExecuteAction.java` lines 280-360 (AggDedupNode detection) and `FusedScanAggregate.java` method `executeCountDistinctWithHashSets` to understand current approach.

**Step 2: Read the handover root cause analysis**

Read `docs/handover/2026-03-24-phase-d-handover.md` Section 9 Category A for the specific fix strategy.

**Step 3: Implement the fix**

Intercept at `TransportShardExecuteAction` dispatch: detect the Calcite two-level agg pattern (AggDedupNode), route to a fused GROUP BY path with per-group `LongOpenHashSet` instead of doubling the key space.

**Step 4: Compile**

```bash
cd /local/home/penghuo/oss/os-sql
./gradlew :dqe:compileJava
```

Expected: BUILD SUCCESSFUL in ~5s.

**Step 5: Reload plugin**

```bash
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin
```

Expected: ~3 min.

**Step 6: Correctness gate**

```bash
bash run/run_all.sh correctness
```

Expected: >= 29/43 (no regression).

**Step 7: Benchmark Category A queries**

```bash
for q in 5 6 9 10 12 14; do
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/catA-q$q
done
```

Note: Q04=`--query 5`, Q05=`--query 6`, Q08=`--query 9`, Q09=`--query 10`, Q11=`--query 12`, Q13=`--query 14` (1-based).

**Step 8: Compare results**

Check each query's ratio against baseline. Target: all 6 within 2x.

**Step 9: Commit**

```bash
git add -A && git commit -m "perf: fused COUNT(DISTINCT) path for Category A queries"
```

---

### Task 3: Category B — High-Cardinality GROUP BY (Q15, Q16, Q18, Q32)

4 queries. Root cause: HashMap with 17M-50M entries misses CPU cache.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (parallelize single-key path, hash-partitioned aggregation)

**Step 1: Read current GROUP BY execution paths**

Read `FusedGroupByAggregate.java` methods `executeSingleKeyNumericFlat` (~line 4269) and `executeNumericOnly` (~line 2786).

**Step 2: Implement parallel/partitioned aggregation**

- Q15: Parallelize single-key numeric path across segments
- Q16/Q18/Q32: Hash-partitioned aggregation to reduce per-thread HashMap size

**Step 3: Compile**

```bash
./gradlew :dqe:compileJava
```

**Step 4: Reload plugin**

```bash
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin
```

**Step 5: Correctness gate**

```bash
bash run/run_all.sh correctness
```

**Step 6: Benchmark Category B queries**

```bash
for q in 16 17 19 33; do
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/catB-q$q
done
```

Note: Q15=`--query 16`, Q16=`--query 17`, Q18=`--query 19`, Q32=`--query 33` (1-based).

**Step 7: Commit**

```bash
git add -A && git commit -m "perf: parallel hash-partitioned GROUP BY for Category B queries"
```

---

### Task 4: Category D — Borderline Fixes (Q02, Q28, Q30, Q31, Q37)

5 queries. Small gaps, targeted micro-optimizations.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java` (Q02: SUM+COUNT+AVG)
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (Q28: regex Pattern caching, Q30/Q31/Q37: targeted fixes)

**Step 1: Read current code for each query's execution path**

Identify the specific bottleneck for each:
- Q02 (2.2x): SUM+COUNT+AVG scalar — small gap
- Q28 (2.7x): REGEXP_REPLACE — cache compiled Pattern
- Q30 (2.3x): GROUP BY SearchEngineID, ClientIP
- Q31 (2.0x): GROUP BY WatchID, ClientIP — needs only 3ms improvement
- Q37 (2.7x): CounterID=62 filtered URL GROUP BY

**Step 2: Implement fixes**

One fix at a time, compile, test.

**Step 3: Compile**

```bash
./gradlew :dqe:compileJava
```

**Step 4: Reload plugin**

```bash
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin
```

**Step 5: Correctness gate**

```bash
bash run/run_all.sh correctness
```

**Step 6: Benchmark Category D queries**

```bash
for q in 3 29 31 32 38; do
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/catD-q$q
done
```

Note: Q02=`--query 3`, Q28=`--query 29`, Q30=`--query 31`, Q31=`--query 32`, Q37=`--query 38` (1-based).

**Step 7: Commit**

```bash
git add -A && git commit -m "perf: borderline fixes for Category D queries"
```

---

### Task 5: Category C — Full-Table VARCHAR GROUP BY (Q35, Q36, Q39)

3 queries. Hardest — fundamentally limited by per-doc hash overhead with millions of unique URL/IP groups.

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (VARCHAR GROUP BY paths)

**Step 1: Read current VARCHAR GROUP BY paths**

Read `executeNKeyVarcharPath` (~line 8657) and `executeSingleVarcharGeneric` (~line 1937).

**Step 2: Implement optimizations**

Explore: global ordinal pre-aggregation, parallel doc-range scanning, reduced hash overhead.

**Step 3: Compile → Reload → Correctness → Benchmark**

Same loop as above. Benchmark queries:
```bash
for q in 36 37 40; do
    bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query $q --output-dir /tmp/catC-q$q
done
```

Note: Q35=`--query 36`, Q36=`--query 37`, Q39=`--query 40` (1-based).

**Step 4: Commit**

```bash
git add -A && git commit -m "perf: VARCHAR GROUP BY optimizations for Category C queries"
```

---

### Task 6: Full Benchmark & Final Verification

**Step 1: Run full benchmark**

```bash
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full-final
```

**Step 2: Compare against baseline**

```bash
python3 -c "
import json, glob
with open('results/performance/clickhouse_parquet_official/c6a.4xlarge.json') as f:
    ch = json.load(f)
os_file = glob.glob('/tmp/full-final/*.json')[0]
with open(os_file) as f:
    os_data = json.load(f)
within = 0
above = []
for i in range(43):
    ch_best = min(ch['result'][i])
    os_times = [x for x in os_data['result'][i] if x is not None]
    if not os_times:
        above.append((i, 999, 0, ch_best))
        continue
    os_best = min(os_times)
    ratio = os_best / ch_best if ch_best > 0 else 999
    if ratio <= 2.0:
        within += 1
    else:
        above.append((i, ratio, os_best, ch_best))
print(f'{within}/43 within 2x')
if above:
    print('\nStill above 2x:')
    for i, ratio, os_best, ch_best in sorted(above, key=lambda x: -x[2]):
        print(f'  Q{i:02d}: {ratio:.1f}x ({os_best:.3f}s vs {ch_best:.3f}s)')
"
```

**Step 3: Save final results and commit**

```bash
cp /tmp/full-final/*.json benchmarks/clickbench/results/performance/opensearch/
git add -A && git commit -m "bench: final benchmark results — N/43 within 2x"
```

Target: 43/43 within 2x.

---

## Execution Order

```
Task 1: Full baseline benchmark (~25 min)
    │
    └─ Task 2: Category A — COUNT(DISTINCT) fusion (6 queries)
           │
           └─ Task 3: Category B — High-cardinality GROUP BY (4 queries)
                  │
                  └─ Task 4: Category D — Borderline fixes (5 queries)
                         │
                         └─ Task 5: Category C — VARCHAR GROUP BY (3 queries)
                                │
                                └─ Task 6: Final benchmark & verification
```

## Query Numbering Reference

| Query (0-based) | `--query N` (1-based) | Category |
|------------------|-----------------------|----------|
| Q02 | 3 | D |
| Q04 | 5 | A |
| Q05 | 6 | A |
| Q08 | 9 | A |
| Q09 | 10 | A |
| Q11 | 12 | A |
| Q13 | 14 | A |
| Q15 | 16 | B |
| Q16 | 17 | B |
| Q18 | 19 | B |
| Q28 | 29 | D |
| Q30 | 31 | D |
| Q31 | 32 | D |
| Q32 | 33 | B |
| Q35 | 36 | C |
| Q36 | 37 | C |
| Q37 | 38 | D |
| Q39 | 40 | C |

## Dev Iteration Loop (per fix)

```
1. Code change
2. ./gradlew :dqe:compileJava          (~5s)
3. bash run/run_all.sh reload-plugin    (~3 min)
4. bash run/run_all.sh correctness      (~2 min, must not regress)
5. bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query N --output-dir /tmp/qN
6. git commit if improvement confirmed
```

**CRITICAL: Never run reload-plugin while a benchmark is running.**
