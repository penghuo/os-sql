# Execution Prompt — Plan 1

## Context

### Architecture
DQE executes COUNT(DISTINCT) via this flow:
```
SQL → TransportTrinoSqlAction (coordinator)
    → PlanFragmenter strips AggregationNode
    → TransportShardExecuteAction.executePlan() per shard
        → dispatch: isBareSingleNumericColumnScan (L269) or isBareSingleVarcharColumnScan (L277)
        → FusedScanAggregate collects distinct values
        → ShardExecuteResponse carries results (transient fields, in-process only)
    → Coordinator merges: mergeCountDistinctValuesViaRawSets (L655) or mergeCountDistinctVarcharViaRawSets (L659)
```

### Key Files & Line Numbers
| File | Path | Key Lines |
|------|------|-----------|
| ShardExecuteResponse | `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/ShardExecuteResponse.java` | 93 lines total. Transient fields at L39-65. Add new HLL field after L65. |
| TransportShardExecuteAction | `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java` | Dispatch at L269 (numeric) and L277 (varchar). `executeDistinctValuesScanWithRawSet` at L3661. `executeDistinctValuesScanVarcharWithRawSet` at L3691. |
| FusedScanAggregate | `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java` | `collectDistinctValuesRaw` at L1674. `collectDistinctVarcharHashes` at L1905. Add new HLL methods near these. |
| TransportTrinoSqlAction | `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java` | Coordinator dispatch at L655 (numeric) and L659 (varchar). `mergeCountDistinctValuesViaRawSets` at L2026. `mergeCountDistinctVarcharViaRawSets` at L2115. |

### HLL API (from OpenSearch, already on classpath)
```java
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitMixer;
import org.opensearch.common.hash.MurmurHash3;

// Create
HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(14, BigArrays.NON_RECYCLING_INSTANCE, 1);

// Collect numeric
hll.collect(0, BitMixer.mix64(longValue));

// Collect varchar (hash bytes first)
MurmurHash3.Hash128 hash128 = new MurmurHash3.Hash128();
MurmurHash3.hash128(bytes, offset, length, 0, hash128);
hll.collect(0, hash128.h1);

// Merge
merged.merge(0, otherHll, 0);

// Get result
long count = hll.cardinality(0);

// MUST close (implements Releasable)
hll.close();
```

### Critical Details
- All shard execution is in-process (`executeLocal`), so transient fields on ShardExecuteResponse survive — no serialization needed
- BigArrays is NOT used elsewhere in DQE — use `BigArrays.NON_RECYCLING_INSTANCE` (safe for 16KB HLL)
- HLL is NOT thread-safe — one instance per shard, merge sequentially at coordinator
- HLL instances MUST be closed in finally blocks (they allocate via BigArrays)
- For varchar: cache ordinal→hash mapping in `long[]` array to avoid re-hashing same terms
- Existing exact paths (`executeDistinctValuesScanWithRawSet`, `mergeCountDistinctValuesViaRawSets`) stay intact as dead code / fallback

### Existing Patterns to Follow
- Look at `executeDistinctValuesScanWithRawSet` (L3661) for the structure of shard execution methods
- Look at `collectDistinctValuesRaw` (L1674) for the DocValues iteration pattern (MatchAll vs filtered)
- Look at `mergeCountDistinctValuesViaRawSets` (L2026) for the coordinator merge pattern
- Look at `collectDistinctVarcharHashes` (L1905) for the ordinal-based varchar iteration pattern

### Benchmark Commands (CRITICAL: 1-based query indexing)
```bash
# Compile
./gradlew :dqe:compileJava

# Reload plugin (MUST run async — takes 2-3 min)
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
nohup bash -c 'bash run/run_all.sh reload-plugin > /tmp/reload.log 2>&1' &>/dev/null &

# Benchmark Q04 (1-based: --query 5)
cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench
nohup bash -c 'bash run/run_opensearch.sh --warmup 1 --num-tries 3 --query 5 > /tmp/bench-q04.log 2>&1' &>/dev/null &

# Benchmark Q05 (1-based: --query 6)
nohup bash -c 'bash run/run_opensearch.sh --warmup 1 --num-tries 3 --query 6 > /tmp/bench-q05.log 2>&1' &>/dev/null &

# Correctness
nohup bash -c 'bash run/run_all.sh correctness > /tmp/correctness.log 2>&1' &>/dev/null &

# Full benchmark
nohup bash -c 'bash run/run_opensearch.sh --warmup 1 --num-tries 3 > /tmp/bench-full.log 2>&1' &>/dev/null &
```

## Guidance

### Task ordering
Tasks 1-2 are the core numeric HLL path (Q04). Task 3 extends to varchar (Q05). Task 4 is verification only.

### What to watch for
1. **Import paths**: `BitMixer` is in `org.opensearch.common.util.BitMixer` (may be in `libs/common` — check if it's on classpath. If not, use `MurmurHash3.murmur64(longValue)` as alternative).
2. **HLL close()**: Every HLL instance must be closed. At shard level, do NOT close before coordinator reads it. Close all HLL instances (shard + merged) in the coordinator merge method's finally block.
3. **Varchar ordinal cache sizing**: `SortedSetDocValues.getValueCount()` returns the number of unique ordinals. If this is very large (>100M), the `long[]` cache won't fit. Add a guard: if valueCount > some threshold (e.g., 10M), skip cache and hash on-the-fly.
4. **MatchAll vs filtered**: Follow the existing pattern in `collectDistinctValuesRaw` — check `query instanceof MatchAllDocsQuery` for the tight loop, else use Weight/Scorer.

### What NOT to do
- Do NOT modify the existing exact paths — leave them as fallback
- Do NOT add configuration flags — HLL is always-on for scalar COUNT(DISTINCT)
- Do NOT parallelize HLL collection — it's memory-bandwidth bound, not CPU bound
- Do NOT serialize HLL sketches — all execution is in-process

## Task Reference
Execute tasks in .ralph/TASKS_1.md in order. For each task:
- Read the task's Goal, Approaches, and Verification sections
- Implement the task
- Mark `- [ ] Done` → `- [x] Done` in .ralph/TASKS_1.md
- Append execution details to .ralph/LOGS_EXECUTE.md
- Append task status updates to .ralph/STATUS.md (do NOT overwrite — append only)
