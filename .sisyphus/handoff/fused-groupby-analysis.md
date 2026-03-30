# FusedGroupByAggregate.java Execution Path Analysis

## File: dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java (14,235 lines)

## Parallelism Configuration (lines 118-135)
- `PARALLELISM_MODE`: System property `dqe.parallelism`, default `"docrange"`
- `THREADS_PER_SHARD`: `availableProcessors / numLocalShards(default=4)`
- Shared `ForkJoinPool` with `asyncMode=true` (work-stealing), sized to `availableProcessors`
- Parallelism disabled when: mode="off", THREADS_PER_SHARD<=1, or single segment

## Dispatch Chain

### Entry: `executeWithTopN()` (line 1248) → `executeInternal()` (line 1279)

`executeInternal` classifies keys into `KeyInfo` records (varchar vs numeric, expression type), then dispatches:

1. **hasVarchar=true** → varchar path:
   - 1 varchar key + COUNT(*) → `executeSingleVarcharCountStar` (line 1615)
   - 1 varchar key + general aggs → `executeSingleVarcharGeneric` (line 2291)
   - Has eval key → `executeWithEvalKeys` (line 7219)
   - Otherwise → `executeWithVarcharKeys` (line 8210)

2. **hasVarchar=false** → `executeNumericOnly` (line 3156):
   - 1 key, no expr → `executeSingleKeyNumeric` (line 3516)
   - 2 keys, no expr → `executeTwoKeyNumeric` (line 6016)
   - N keys with shared-column arith → `executeDerivedSingleKeyNumeric`
   - Fallback → HashMap<SegmentGroupKey, AccumulatorGroup> with Collector

---

## Per-Query Execution Paths

### Q15: `GROUP BY UserID ORDER BY COUNT(*) LIMIT 10` (31.96x regression)
- **Path**: `executeNumericOnly` → `executeSingleKeyNumeric` (line 3516) → `executeSingleKeyNumericFlat` (line 5184)
- **Data structure**: `FlatSingleKeyMap` — open-addressing hash map with `long[] keys` + `long[] accumulators`, MAX_CAPACITY=16M
- **Parallelism**: Two modes:
  - **Doc-range parallel** (COUNT(*) + MatchAll + no bucketing): loads columnar cache per segment, splits doc ranges across THREADS_PER_SHARD workers (line 5237-5293)
  - **Segment-parallel**: largest-first greedy assignment of segments to workers, each with own FlatSingleKeyMap, merged after (line 5296-5354)
- **TopN**: NO heap-based TopN in this path. Full aggregation completes, then results are materialized. TopN push-down only exists in `executeSingleVarcharCountStar` (line 1707).
- **Bucketing**: When totalDocs > MAX_CAPACITY (16M), partitions key space into buckets via hash, runs sequential passes per bucket (line 3589-3660)
- **Bottleneck**: For high-cardinality UserID with full table scan, the entire hash map must be built before any LIMIT filtering. No TopN push-down for numeric keys.

### Q16: `GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) LIMIT 10` (6.80x)
- **Path**: `executeWithVarcharKeys` (line 8210) — SearchPhrase is varchar
- **Single-segment**: tries `tryOrdinalIndexedTwoKeyCountStar` (line 8283), then flat two-key varchar path with `FlatTwoKeyMap` (line 8327)
- **Multi-segment**: falls through to `executeNKeyVarcharPath` (line 9287) → `executeNKeyVarcharParallelDocRange` (line 11297/11720)
- **Parallelism**: Doc-range parallel splits matched docs among workers, each with own `Map<MergedGroupKey, AccumulatorGroup>` (line 11770). Uses `LinkedHashMap` per worker.
- **TopN**: Early termination for LIMIT without ORDER BY (line 11756: `earlyTerminate = sortAggIndex < 0 && topN > 0`). But Q16 HAS ORDER BY, so no early termination — full aggregation required.
- **Bottleneck**: MergedGroupKey allocation per group (wraps BytesRef for varchar), HashMap overhead. No TopN heap push-down for mixed-key paths.

### Q18: `GROUP BY UserID, minute(EventTime), SearchPhrase ORDER BY COUNT(*) LIMIT 10` (9.96x)
- **Path**: `executeWithVarcharKeys` → `executeNKeyVarcharPath` (line 10359) — 3 keys with varchar
- **Tries**: `executeThreeKeyFlat` (line 9880) with `FlatThreeKeyMap` first, but only when all keys are numeric (varchar ordinals are segment-local). Since SearchPhrase is varchar, falls through.
- **Falls to**: Single-segment N-key path with `SegmentGroupKey` map (line 10490+), or multi-segment `executeNKeyVarcharParallelDocRange`
- **Parallelism for 3-key**: `executeThreeKeyFlat` parallelizes only when `!anyVarcharKey` (line 9920). Since SearchPhrase is varchar, NO parallelism in the flat path.
- **TopN**: Same as Q16 — no heap push-down with ORDER BY.
- **Bottleneck**: Per-group object allocation (SegmentGroupKey + AccumulatorGroup), expression evaluation (minute extraction) per doc, no parallelism due to varchar key.

### Q32: `GROUP BY WatchID, ClientIP WHERE SearchPhrase<>'' ` (1.77x)
- **Path**: `executeNumericOnly` → `executeTwoKeyNumeric` (line 6016) → `executeTwoKeyNumericFlat` (line 6379)
- **Data structure**: `FlatTwoKeyMap` — open-addressing with `long[] key1, key2, accumulators`
- **Parallelism**: Segment-parallel with largest-first greedy assignment (line 6470+). Filter query uses Weight/Scorer per segment.
- **TopN**: topN=0 (no LIMIT in Q32), so no push-down needed.
- **Bottleneck**: Filtered query requires Scorer evaluation per segment. Two numeric keys = efficient flat path. Relatively low regression (1.77x) confirms this path is well-optimized.

### Q35: `GROUP BY 1, URL ORDER BY COUNT(*) LIMIT 10` (3.94x)
- **Path**: `executeWithVarcharKeys` (URL is varchar) → 2-key path
- **Single-segment**: tries ordinal-indexed two-key COUNT(*), then flat two-key varchar path
- **Multi-segment**: `executeNKeyVarcharPath` → tries `executeMultiSegGlobalOrdFlatTwoKey` (line 11275), then `executeNKeyVarcharParallelDocRange`
- **Parallelism**: Doc-range parallel in `executeNKeyVarcharParallelDocRange`. But for MatchAll + high-cardinality varchar (>500K ordinals), parallelism is DISABLED in `executeSingleVarcharCountStar` (line 1984-1987). Similar guard may apply here.
- **TopN**: No heap push-down for 2-key mixed path with ORDER BY.
- **Bottleneck**: URL is extremely high-cardinality varchar. MergedGroupKey with BytesRef copies. No TopN push-down means full aggregation of all URL groups before LIMIT 10 selection.

---

## TopN/LIMIT Push-Down Status

| Path | TopN Heap | Early Termination |
|------|-----------|-------------------|
| `executeSingleVarcharCountStar` (line 1707) | ✅ Yes — min/max heap on ordinal counts | N/A |
| `executeNKeyVarcharParallelDocRange` (line 11756) | ❌ No | ✅ Only when no ORDER BY |
| `executeSingleKeyNumericFlat` | ❌ No | ❌ No |
| `executeTwoKeyNumericFlat` | ❌ No | ❌ No |
| `executeThreeKeyFlat` | ❌ No | ❌ No |
| `executeWithVarcharKeys` 2-key flat | ❌ No | ❌ No |

**Key finding**: TopN heap push-down only exists for single-varchar-key COUNT(*). All other paths (numeric keys, mixed keys, multi-agg) do full aggregation then truncate.

## TODO/Optimization Comments
- **No TODO/FIXME/HACK comments found** in the file.
- Line 67: "The key optimization: during aggregation, string GROUP BY keys are represented as long ordinals"
- Line 1976-1978: Explicit note that high-cardinality VARCHAR parallelism is disabled because "BytesRefKey copies per worker, making merge overhead exceed the parallelism benefit"
- Line 9285: Method extraction to keep `executeWithVarcharKeys` below JVM HugeMethodLimit (8000 bytecodes) for C2 JIT

## Optimization Opportunities

1. **TopN push-down for numeric keys (Q15)**: `executeSingleKeyNumericFlat` builds the entire FlatSingleKeyMap before output. A heap-based TopN during aggregation could avoid materializing all groups. This is the biggest win for Q15's 31.96x regression.

2. **TopN push-down for mixed keys (Q16/Q18/Q35)**: The 2-key and N-key varchar paths have no TopN. Adding a bounded-size map or heap during aggregation would help all three queries.

3. **Parallelism for varchar-containing 3-key (Q18)**: `executeThreeKeyFlat` skips parallelism when any key is varchar. A global-ordinal approach (resolving segment-local ordinals to global) could enable parallelism.

4. **High-cardinality varchar parallelism (Q35)**: The 500K ordinal guard (line 1984-1987) disables parallelism for high-cardinality varchar. The doc-range parallel path (`executeNKeyVarcharParallelDocRange`) uses MergedGroupKey which copies BytesRef — an ordinal-based merge strategy could reduce this overhead.
