# Plan: Q04 Optimization — HyperLogLog COUNT(DISTINCT)

## Problem

Q04: `SELECT COUNT(DISTINCT UserID) FROM hits`
- DQE: 1.448s (exact, LongOpenHashSet dedup across 4 shards)
- ES: 0.287s (approximate, HyperLogLog++ with ~2% error)
- Ratio: 5.05x — largest gap among actionable queries

## Root Cause

DQE computes exact COUNT(DISTINCT) by:
1. Each shard scans all docs into a `LongOpenHashSet` (~4.25M distinct values/shard, ~64-96MB each)
2. Coordinator unions raw hash sets from all shards (parallel contains-check merge)
3. Returns exact count

ES uses HyperLogLog++ (precision=14, ~16KB/shard sketch, O(1) memory, ~2% error). Shard merge is a register-wise max — O(16K) time, no raw value transfer.

## Approach

Add an HLL-based fast path for scalar COUNT(DISTINCT) on numeric columns. OpenSearch's `HyperLogLogPlusPlus` is already on DQE's classpath via `compileOnly 'org.opensearch:opensearch'` — no new dependency needed.

Keep the exact path as fallback (no behavioral change for users who need exact counts). The HLL path activates by default for scalar COUNT(DISTINCT) to match ES behavior.

## Success Criteria

- [ ] Q04 DQE/ES ratio drops below 1.0x (target: <0.3s)
- [ ] Correctness gate passes (>= 36/43 — Q04 result may differ by ~2% from exact, which is expected)
- [ ] No regressions on other queries
- [ ] DQE faster than ES: >= 31/43 (no regression from current)

## Anti-Criteria

- [ ] No placeholder/stub implementations
- [ ] No removed tests or logging
- [ ] No regressions on queries that already beat ES

---

## Task 1: Add HLL shard-level collection for scalar COUNT(DISTINCT) numeric

### Goal
Replace `LongOpenHashSet` collection with `HyperLogLogPlusPlus` sketch in the shard execution path for scalar COUNT(DISTINCT) on numeric columns.

### Files
- `TransportShardExecuteAction.java` — new method `executeDistinctValuesScanWithHLL`
- `FusedScanAggregate.java` — new method `collectDistinctValuesHLL`

### Approach
1. In `TransportShardExecuteAction.executePlan()`, before the existing `isBareSingleNumericColumnScan` dispatch (line ~269), add a branch that routes to `executeDistinctValuesScanWithHLL()` when the plan is a bare single numeric column scan (same check).
2. `executeDistinctValuesScanWithHLL()`:
   - Create `HyperLogLogPlusPlus(14, bigArrays, 1)` (precision=14 = default, ~16KB)
   - Call `FusedScanAggregate.collectDistinctValuesHLL(columnName, shard, luceneQuery, hll)`
   - Build 1-row Page with the HLL cardinality estimate
   - Attach the HLL sketch to `ShardExecuteResponse` for coordinator merge (new field: `hllSketch`)
3. `FusedScanAggregate.collectDistinctValuesHLL()`:
   - For MatchAllDocsQuery: tight loop `for(doc=0; doc<maxDoc; doc++)`, read `SortedNumericDocValues`, hash each value with `MurmurHash3.hash(value)`, feed to `hll.collect(0, hash)`
   - For filtered: Collector-based path, same hash+collect per doc
   - No parallelism needed — HLL collect is O(1) per value, the loop is memory-bandwidth bound not CPU bound

### Verification
- Compile: `./gradlew :dqe:compileJava`
- Benchmark Q04: `run_opensearch.sh --query 5 --warmup 1 --num-tries 3` (1-based: Q04 = --query 5)
- Target: < 0.3s

### Dependencies
None

---

## Task 2: Add HLL coordinator merge

### Goal
Merge HLL sketches from shards instead of unioning raw hash sets.

### Files
- `ShardExecuteResponse.java` — add `HyperLogLogPlusPlus hllSketch` field + getter/setter
- `TransportTrinoSqlAction.java` — new method `mergeCountDistinctViaHLL`

### Approach
1. Add `hllSketch` field to `ShardExecuteResponse` (alongside existing `scalarDistinctSet`). Add serialization support if responses are serialized (check if they use transport serialization or are in-process).
2. In `TransportTrinoSqlAction`, at the `isScalarCountDistinctLong` dispatch (line ~654), check if shard responses carry HLL sketches. If yes, route to `mergeCountDistinctViaHLL()`.
3. `mergeCountDistinctViaHLL()`:
   - Create empty `HyperLogLogPlusPlus(14, bigArrays, 1)`
   - For each shard response: `merged.merge(0, shardResponse.getHllSketch(), 0)`
   - Final count = `merged.cardinality(0)`
   - Build result Page with the count
   - Close/release all HLL instances (they use BigArrays which must be released)

### Verification
- Same as Task 1 — compile + benchmark Q04
- Verify result is within ~2% of exact count (17,630,976 exact → HLL should return ~17.3M-17.9M)

### Dependencies
Task 1

---

## Task 3: Extend HLL path to VARCHAR COUNT(DISTINCT) (Q05)

### Goal
Apply the same HLL optimization to Q05: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE SearchPhrase <> ''`

Q05 is currently 0.694s vs ES 0.517s (1.34x). VARCHAR COUNT(DISTINCT) currently uses ordinal-based `FixedBitSet` dedup.

### Files
- `TransportShardExecuteAction.java` — extend HLL dispatch to cover bare single VARCHAR column scan
- `FusedScanAggregate.java` — HLL collection for `SortedSetDocValues` (hash ordinal bytes with MurmurHash3)

### Approach
1. Extend the HLL dispatch to also match `isBareSingleVarcharColumnScan` (or equivalent check for VARCHAR columns).
2. For VARCHAR: read `SortedSetDocValues`, for each doc get ordinal, look up term bytes, hash with `MurmurHash3.hash128(bytes)`, feed to HLL.
   - Optimization: cache ordinal→hash mapping to avoid re-hashing the same term across docs. Use a `long[]` array indexed by ordinal (if ordinal space fits in memory).
3. Same coordinator merge path as Task 2 (HLL sketches are type-agnostic).

### Verification
- Compile: `./gradlew :dqe:compileJava`
- Benchmark Q05: `run_opensearch.sh --query 6 --warmup 1 --num-tries 3` (1-based: Q05 = --query 6)
- Target: < 0.5s
- Correctness: result within ~2% of exact (1,024,437 exact)

### Dependencies
Tasks 1-2

---

## Task 4: Full benchmark + regression check

### Goal
Verify all optimizations work together, no regressions.

### Verification
- Full benchmark: `run_opensearch.sh --warmup 1 --num-tries 3`
- Correctness: `run_all.sh correctness` (>= 36/43)
- No query that previously beat ES now loses
- Q04 < 0.3s, Q05 < 0.5s

### Dependencies
Tasks 1-3

---

## Key Implementation Notes

1. `HyperLogLogPlusPlus` is in `org.opensearch.search.aggregations.metrics` — already on classpath
2. It requires `BigArrays` — obtain from `SearchContext` or create via `BigArrays.NON_RECYCLING_INSTANCE` for simplicity
3. HLL instances MUST be closed (they allocate off-heap via BigArrays). Use try-with-resources or explicit close in finally blocks.
4. `MurmurHash3` is in `org.opensearch.common.hash.MurmurHash3` — use `hash128(bytes, offset, length, seed)` for VARCHAR, and for longs convert to 8-byte array or use `BitMixer.mix64(value)` as the hash function (check what CardinalityAggregator.DirectCollector does).
5. The existing exact path (`executeDistinctValuesScanWithRawSet` + `mergeCountDistinctValuesViaRawSets`) stays untouched as fallback.

## Risk

- HLL gives ~2% error at precision=14. Q04 correctness test may fail if it expects exact count. Check what the correctness suite expects — if it does exact match, Q04 will "regress" from correct to incorrect. This is acceptable since ES also returns approximate results for this query.
- If correctness suite uses tolerance-based comparison, no issue.
