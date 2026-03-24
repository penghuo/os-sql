# Task: Execute Phase D Handover — DQE Optimization 25/43 → 43/43

Execute the plan in `docs/handover/2026-03-24-phase-d-handover.md` to optimize the DQE (Direct Query Engine) for the ClickBench benchmark. The goal is to bring all 43 queries within 2x of ClickHouse-Parquet performance.

## Current State
- **Score:** 25/43 queries within 2x of ClickHouse-Parquet
- **Branch:** `wukong`
- **Correctness:** 33/43 pass on 1M dataset
- **Hardware:** OpenSearch on m5.8xlarge (32 vCPU, 128GB RAM), 4 shards, ~100M docs

## Success Criteria
1. **Primary:** ≥38/43 queries within 2x of CH-Parquet (stretch: 43/43)
2. **No regressions:** Correctness must stay ≥33/43
3. **No regressions:** Queries already within 2x must stay within 2x
4. **Evidence:** Full benchmark run with comparison output after each optimization

## Priority Order (from handover)

### Step 1: COUNT(DISTINCT) Fusion (Q04, Q05, Q08, Q09, Q11, Q13 — 6 queries)
Intercept the two-level Calcite plan at `TransportShardExecuteAction` dispatch level. Detect pattern: outer Aggregate(GROUP BY x, COUNT(*)) + inner Aggregate(GROUP BY x, y, COUNT(*)). Route to fused GROUP BY with per-group `LongOpenHashSet` accumulator. Key file: `TransportShardExecuteAction.java:280-360`.

### Step 2: Parallelize executeSingleKeyNumericFlat (Q15 + similar)
Q15 scans 100M rows sequentially. Split across parallel workers like `executeWithEval` already does.

### Step 3: Hash-Partitioned Aggregation (Q16, Q18, Q32)
Partition group-key space into buckets, process one bucket at a time, merge. Proven pattern from Q33/Q34.

### Step 4: Borderline Queries (Q02, Q30, Q31, Q37)
Small targeted optimizations. Q31 needs only 3ms improvement.

### Step 5: Q28 REGEXP_REPLACE
Cache compiled Pattern objects. Hoist regex computation before aggregation loop.

### Step 6: Full-Table High-Cardinality VARCHAR (Q35, Q36, Q39)
Hash-partitioned aggregation + parallel segment scanning.

## Key Architecture
Read the full handover doc at `docs/handover/2026-03-24-phase-d-handover.md` for:
- Complete query status table with ratios
- Code map and key source files
- Known issues and pitfalls (query numbering, JIT warmup, plugin reload)
- Build/test/benchmark commands

## Build & Test Commands
```bash
# Compile DQE only (~5s)
cd /home/ec2-user/oss/wukong && ./gradlew :dqe:compileJava

# Full rebuild + restart + reinstall (~3 min)
cd /home/ec2-user/oss/wukong/benchmarks/clickbench && bash run/run_all.sh reload-plugin

# Correctness (1M dataset)
bash run/run_all.sh correctness

# Single query benchmark
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query N --output-dir /tmp/qN_test

# Full benchmark
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full_bench
```

## CRITICAL WARNINGS
- Query numbering: run script is 1-based, JSON results are 0-based
- Always benchmark on full 100M `hits` index, NOT 1M `hits_1m`
- Always use `--warmup 3` for JIT compilation
- Compare against CH-Parquet official baseline, NOT native MergeTree
- Baseline file: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
- Never run benchmarks and `reload-plugin` concurrently

## Approach
Work one step at a time. After each optimization:
1. Compile and verify no build errors
2. Run correctness tests — must not regress below 33/43
3. Benchmark affected queries
4. Run full benchmark to check for regressions
5. Git commit with descriptive message

---

## Instructions

You are executing one iteration of a ralph loop.

1. Read `.ralph/STATUS.md` for current state (if it exists)
2. Read the tail of `.ralph/LOGS.md` for recent history (if it exists)
3. Do the work described above
4. When done with this iteration, update both files:

### STATUS.md (overwrite entirely)
Write current state with this structure:
```
status: WORKING | COMPLETE
iteration: N

## Current State
[What's the situation right now]

## Next Steps
[What needs to happen next — omit if COMPLETE]

## Evidence
[Test results, benchmark numbers, build output — whatever proves progress]
```

Set `status: COMPLETE` only when ALL success criteria from the task are met with evidence.

### LOGS.md (append a section)
Append to the end:
```
## Iteration N — [date/time]

### What I Did
[Actions taken]

### Results
[Outcomes, test results, errors]

### Decisions
[Any architectural or approach decisions made and why]
```

5. Git commit with a descriptive message and push
