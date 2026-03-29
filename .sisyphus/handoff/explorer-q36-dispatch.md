# Q36 Dispatch Path Analysis: Single VARCHAR GROUP BY URL with Filter

## Query
```sql
SELECT URL, COUNT(*) AS PageViews FROM hits
WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31'
  AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> ''
GROUP BY URL ORDER BY PageViews DESC LIMIT 10
```

## Dispatch Path

1. **`executeInternal()`** (line ~1020): Classifies keys. URL is VARCHAR → `hasVarchar = true`.
2. **Single VARCHAR + COUNT(*) check** (line ~1049-1058): `keyInfos.size()==1 && isVarchar && specs.size()==1 && COUNT(*)` → **matches Q36 exactly**.
3. **Dispatches to `executeSingleVarcharCountStar()`** (line 1273).

## Inside `executeSingleVarcharCountStar()` — Decision Tree

### Path A: Single-segment fast path (line ~1290)
- If `leaves.size() == 1` AND ordinal count ≤ 10M → uses `long[] ordCounts` array indexed by ordinal.
- For filtered query: uses Lucene Collector with `dv.advanceExact(doc)` per doc.
- **Not parallel** — single segment, single thread.
- Top-N via min-heap on ordinal counts → only resolves top-10 ordinals to strings.

### Path B: Global ordinals multi-segment (line ~1462)
- **Condition**: For filtered queries (not MatchAll), only if first segment's ordinal count ≤ 1,000,000.
- **URL likely has >1M ordinals** (comment says "URL with 18M ordinals") → **SKIPPED**.
- If it were used: sequential scan of all segments with `segToGlobal.get(dv.nextOrd())`.

### Path C: Parallel multi-segment (line ~1633)
- **Condition**: `PARALLELISM_MODE != "off" && THREADS_PER_SHARD > 1 && leaves.size() > 1`
- **BUT**: Checks first segment's ordinal count. If `> 500,000` → **`canParallelize = false`**.
- **URL has millions of ordinals** → **PARALLELISM DISABLED**.

### Path D: Sequential multi-segment fallback (line ~1840) ← **Q36 LANDS HERE (multi-segment)**
- Uses `HashMap<BytesRefKey, long[]>` for cross-segment merge.
- Per-segment: allocates `long[] ordCounts` if ordCount ≤ 1M, else `HashMap<Long, long[]>`.
- In `finish()`: resolves every non-zero ordinal to `BytesRefKey` (copies UTF-8 bytes) and merges into global HashMap.
- **Completely sequential** — single thread processes all segments one by one.

## Why Q36 is Slow at 4.0x

### Bottleneck 1: No Parallelism
URL has very high cardinality (millions of unique values). All three parallelization gates reject it:
- Global ordinals: ordCount > 1M → skipped (OrdinalMap build cost ~3s for 18M ordinals)
- Parallel multi-segment: ordCount > 500K → disabled (BytesRefKey merge overhead)
- Result: **single-threaded sequential scan** of all segments

### Bottleneck 2: Per-Segment BytesRefKey Allocation
In the `finish()` callback for each segment, every non-zero ordinal triggers:
- `dv.lookupOrd(i)` — Lucene ordinal-to-bytes lookup
- `new BytesRefKey(bytes)` — copies byte array + computes hash
- `globalCounts.get(key)` / `globalCounts.put(key, ...)` — HashMap lookup with byte-array equals

For URL with potentially millions of unique values per segment, this creates massive GC pressure.

### Bottleneck 3: HashMap Overhead for High-Cardinality Merge
The `HashMap<BytesRefKey, long[]>` grows to millions of entries. Each entry involves:
- BytesRefKey object (byte[] copy + hash)
- `long[1]` array per group
- HashMap.Entry overhead

### Bottleneck 4: Filter Selectivity
The WHERE clause filters to a specific month + counter, but the filtered docs still touch many unique URLs. The Lucene Scorer iterates matching docs, but each `dv.advanceExact(doc)` on SortedSetDocValues for URL is a seek operation.

## Summary

| Aspect | Status |
|--------|--------|
| Dispatch path | `executeSingleVarcharCountStar()` |
| Single-segment path | Used if 1 segment; ordinal array + Collector; **not parallel** |
| Global ordinals | **Skipped** — URL ordinals > 1M threshold |
| Parallel multi-segment | **Disabled** — URL ordinals > 500K threshold |
| Actual path (multi-seg) | Sequential Collector → per-segment ordinal array → BytesRefKey merge into global HashMap |
| Top-N optimization | Applied at output (heap select top-10 from global HashMap) |
| Root cause of 4.0x | Single-threaded + millions of BytesRefKey allocations + HashMap overhead for high-cardinality URL |

## Potential Improvements
1. **Parallel doc-range within single segment**: Split matched doc IDs across threads, each with local ordinal array, merge ordinal arrays (no BytesRefKey needed).
2. **Global ordinals with lazy build**: Cache OrdinalMap across queries; amortize the 3s build cost.
3. **Filtered global ordinals path**: For filtered queries, build OrdinalMap only if cached; use global ordinal array (avoids BytesRefKey entirely).
4. **Increase parallel threshold**: The 500K limit is conservative; with ordinal-array-based per-worker aggregation (no BytesRefKey during scan), parallelism could work at higher cardinalities.
