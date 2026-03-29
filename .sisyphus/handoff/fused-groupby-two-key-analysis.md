# FusedGroupByAggregate: canFuse() & Two-Key GROUP BY Analysis

## 1. canFuse() Conditions (line 197)

```java
public static boolean canFuse(AggregationNode aggNode, Map<String, Type> columnTypeMap)
```

**Requirements for fast path:**
- `groupByKeys` must be non-empty
- Child must be `TableScanNode` or `EvalNode → TableScanNode`
- All group-by keys must be: plain columns (VARCHAR/numeric/timestamp), DATE_TRUNC(timestamp), EXTRACT(timestamp), arithmetic `(col OP constant)`, or EvalNode-computed expressions
- All aggregate functions must match regex: `COUNT|SUM|MIN|MAX|AVG` with optional `DISTINCT`
- Aggregate arguments must be physical columns, `*`, or `length(varchar_col)`

**Q32/Q33 keys (WatchID + ClientIP):** Both are numeric (BIGINT) → passes canFuse, dispatches to numeric-only path.

## 2. Execution Path Dispatch (executeInternal → executeNumericOnly)

```
executeInternal() → hasVarchar=false → executeNumericOnly()
  → keyInfos.size()==2 && !anyTrunc → executeTwoKeyNumeric()
```

## 3. executeTwoKeyNumeric() — Two Paths

### Path A: Flat Accumulator (FlatTwoKeyMap) — line ~2850
**Condition:** `canUseFlatAccumulators == true`
- All aggs must be COUNT(*), SUM(long), or AVG(long)
- No DOUBLE args, no VARCHAR args, no MIN/MAX, no COUNT(DISTINCT)

**Q32 aggs:** `COUNT(*)`, `SUM(IsRefresh)`, `AVG(ResolutionWidth)`
- COUNT(*) → accType=0 ✓
- SUM(IsRefresh) → IsRefresh is likely TINYINT/SMALLINT → accType=1 (SUM long) ✓
- AVG(ResolutionWidth) → ResolutionWidth is likely INTEGER → accType=2 (AVG long) ✓
- **→ Uses FlatTwoKeyMap (zero per-group object allocation)**

**FlatTwoKeyMap layout:** Each group gets `slotsPerGroup` longs:
- COUNT: 1 slot
- SUM long: 1 slot
- AVG long: 2 slots (sum + count)
- Total for Q32: 1+1+2 = 4 longs per group

### Path B: TwoKeyHashMap with AccumulatorGroup — line ~3050
**Condition:** `canUseFlatAccumulators == false` (has DOUBLE/VARCHAR/MIN/MAX/DISTINCT)
- Uses `AccumulatorGroup` objects with `MergeableAccumulator[]`

## 4. Flat Path Details (executeTwoKeyNumericFlat)

**Hash partitioning for large datasets:**
```java
numBuckets = Math.max(1, (int) Math.ceil((double) totalDocs / FlatTwoKeyMap.MAX_CAPACITY));
// MAX_CAPACITY = 8_000_000
```
- 100M rows → `ceil(100M/8M) = 13 buckets`
- Each bucket scans ALL docs but only inserts groups whose `hash2(k0,k1) % numBuckets == bucket`
- **This is the key bottleneck for Q32/Q33: 13 full passes over 100M docs**

**Parallel bucket execution:**
```java
if (parallelBuckets > 1 && !"off".equals(PARALLELISM_MODE)) {
    // Parallel across buckets via PARALLEL_POOL (ForkJoinPool)
}
```

**Per-segment scanning (scanSegmentFlatTwoKey):**
- MatchAll (Q33): iterates all docs with `liveDocs` check
- Filtered (Q32 WHERE SearchPhrase<>''): uses `Weight.scorer()` per segment
- DV deduplication: if multiple aggs reference same column, reads DocValues once

## 5. WHERE Clause Impact (Q32 vs Q33)

**Q33 (no WHERE, 2.63x):** `query = MatchAllDocsQuery`
- Direct doc iteration: `for (doc = 0; doc < maxDoc; doc++)`
- No scorer overhead, no bitset

**Q32 (WHERE SearchPhrase<>'', 2.33x):** `query = filtered query`
- Uses `Weight.scorer(leafCtx)` per segment
- Scorer-based iteration: `docIt.nextDoc()` loop
- Can skip empty segments early (`if (scorer == null) continue`)
- **Faster than Q33 because WHERE filters out ~97% of docs** (most SearchPhrase are empty)

## 6. Key Performance Constraints

| Factor | Impact |
|--------|--------|
| **Multi-bucket scanning** | 13 passes over 100M docs (8M group cap per bucket) |
| **Hash computation** | `hash2(k0,k1)` per doc per bucket pass |
| **advanceExact per DV column** | 2 key DVs + N agg DVs per doc |
| **No ordinal optimization** | Both keys are numeric, no SortedSetDocValues ordinals |
| **DV dedup** | Helps when AVG decomposes to SUM+COUNT on same column |

## 7. Why 2.33x/2.63x (not faster)?

1. **Multi-pass overhead:** With ~100M docs and 8M max capacity, needs ~13 bucket passes. Each pass reads all DocValues columns for all docs, but only inserts matching-bucket groups.
2. **No columnar batch processing:** Per-doc `advanceExact()` on each DV column (2 keys + 3 agg columns = 5 DV reads per doc).
3. **Hash map overhead:** Even with flat arrays, open-addressing probing + hash computation per doc.
4. **No SIMD/vectorization:** Pure scalar Java loops.

## 8. Potential Optimization Opportunities

- **Increase MAX_CAPACITY** to reduce bucket count (memory tradeoff)
- **Sequential DV iteration** (nextDoc lockstep) instead of advanceExact for dense columns
- **Pre-filter by key0** before reading key1 (like the capped two-phase path in varchar)
- **Batch DV reads** to amortize per-doc overhead
