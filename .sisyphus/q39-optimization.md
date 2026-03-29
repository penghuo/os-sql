# Q39 Optimization — Root Cause Analysis & Results

## Query
```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID,
       CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src,
       URL AS Dst, COUNT(*) AS PageViews
FROM hits
WHERE CounterID = 62 AND EventDate >= DATE '2013-07-01' AND EventDate <= DATE '2013-07-31' AND IsRefresh = 0
GROUP BY TraficSourceID, SearchEngineID, AdvEngineID,
         CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END, URL
ORDER BY PageViews DESC OFFSET 1000 LIMIT 10
```

## Performance
- CH-Parquet: 0.143s (c6a.4xlarge)
- OS baseline: 4.5-5.5s (r5.4xlarge, multi-segment)
- OS force-merged: 1.9-2.3s (r5.4xlarge, single-segment)
- Ratio: 31-38x (multi-segment), 13-16x (single-segment)

## Root Cause (Profiling Results)

### Per-shard breakdown (multi-segment, 4 shards × ~180K docs each):
- Setup (expression compilation): 36-76ms (1%)
- Doc collection (Scorer iteration): 6-70ms (1%)
- **DocValues reads: 4.6-5.0s (98%)**
- Hash map operations: 0.3-0.5ms (0.01%)

### Per-doc cost: ~20µs for DocValues reads across 5 keys
- 3 numeric keys (TraficSourceID, SearchEngineID, AdvEngineID): fast
- 1 inline CASE WHEN → Referer ordinal: slow (high-cardinality VARCHAR)
- 1 URL ordinal: slow (18M unique values, high-cardinality VARCHAR)

### Why DocValues are slow
- 24-27 segments per shard → multi-segment path uses BytesRefKey (byte array copy per doc)
- URL has 18M unique ordinals → large SortedSetDocValues structure
- 180K matching docs spread across 25M-doc shard → ~140 doc gap between matches
- Each advanceExact() must decompress DocValues blocks to seek

## Attempted Optimizations

| # | Idea | Result | Why |
|---|------|--------|-----|
| 1 | Columnar batch processing | ❌ Regressed 15% | Extra memory allocation for column arrays |
| 2 | Flat interleaved count array | ❌ Regressed 20% | Stride alignment causes cache line splits |
| 3 | Boxing elimination (direct long[]) | ❌ No change | JIT already optimizes boxing via escape analysis |
| 4 | Force merge to 1 segment | ✅ 2x improvement | Enables ordinal-based keys, flat long map |
|   | | ❌ Breaks other queries | Q7,Q14,Q30,Q31,Q40,Q41 timeout with 25M-doc segments |

## Conclusion
Q39's bottleneck is Lucene DocValues I/O on high-cardinality VARCHAR columns (URL, Referer).
This is a storage-layer limitation, not an execution-layer issue. The hash map and accumulation
are only 2% of the time. No amount of grouping code optimization can close the 16-38x gap.

### What would help
1. **Force merge + fix regressions**: Single-segment gives 2x, but need to fix queries that break
2. **Parallel segment processing**: Process 24 segments in parallel within each shard
3. **Columnar storage format**: Fundamentally different from DocValues (what CH uses)
4. **Reduce shard count**: 4→1 shard would reduce coordinator overhead but increase per-shard work
