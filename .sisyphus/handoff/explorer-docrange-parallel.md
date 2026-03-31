# Doc-Range Parallel Analysis in executeSingleKeyNumericFlat

## 1. How Doc-Range Parallel Works

Doc-range parallel splits a **single segment's doc ID space** into ranges, each processed by a separate worker thread. It is implemented **only** in `executeSingleKeyNumericFlat` and only for a narrow case:

### Conditions (ALL must be true):
- `PARALLELISM_MODE != "off"` (default: `"docrange"`)
- `THREADS_PER_SHARD > 1` (= `availableProcessors / numLocalShards`, default 4 shards)
- `leaves.size() > 1` ← **CRITICAL: requires >1 segment**
- All aggregates are `COUNT(*)` (`allCountStar == true`)
- Query is `MatchAllDocsQuery`
- `numBuckets <= 1` (single-bucket flat map, no overflow)

### Execution Flow (lines ~5191-5265):
1. **Load columnar cache**: For each segment, calls `loadNumericColumn()` to read the entire key column into a `long[]` array
2. **Check 10M threshold**: If `totalDocs > 10_000_000`, falls back to **sequential** scan
3. **Split into work units**: Divides each segment's doc range into chunks of `docsPerWorker = totalDocs / THREADS_PER_SHARD`
4. **Parallel scan**: Each worker gets a `FlatSingleKeyMap` and calls `scanDocRangeFlatSingleKeyCountStar(keyValues, startDoc, endDoc, ...)`
5. **Merge**: Main thread merges all worker maps via `flatMap.mergeFrom(workerMap)`

### The 10M Threshold (line ~5213):
```java
if (totalDocs > 10_000_000) {
    // Pre-size main map larger to reduce resize probability
    flatMap = new FlatSingleKeyMap(slotsPerGroup, 8_000_000);
    for (int s = 0; s < segKeyArrays.size(); s++) {
        scanDocRangeFlatSingleKeyCountStar(
            segKeyArrays.get(s), 0, segKeyArrays.get(s).length,
            segLiveDocs.get(s), flatMap, accOffset[0], slotsPerGroup);
    }
}
```
**Rationale**: "Parallel workers each build 4.4M-entry maps that thrash L3 cache and require expensive mergeFrom. Sequential scan uses ONE map with better cache locality."

## 2. Segment-Parallel vs Doc-Range Parallel

| Condition | Path Taken |
|-----------|-----------|
| `leaves.size() == 1` (single segment) | **Sequential** — `canParallelize` is false because `leaves.size() > 1` fails |
| `leaves.size() > 1`, all COUNT(*), MatchAll, ≤10M docs | **Doc-range parallel** within segments |
| `leaves.size() > 1`, all COUNT(*), MatchAll, >10M docs | **Sequential** (10M fallback) |
| `leaves.size() > 1`, not all COUNT(*) or not MatchAll | **Segment-parallel** (partition segments across workers) |

## 3. Q15 Analysis: GROUP BY UserID, COUNT(*), MatchAll, ~25M docs/shard

### Dispatch chain:
1. `executeInternal` → `executeNumericOnly` (UserID is numeric, no VARCHAR)
2. `executeNumericOnly` → `executeSingleKeyNumeric` (single key, no expressions)
3. `executeSingleKeyNumeric` → `executeSingleKeyNumericFlat` (COUNT(*) is flat-compatible: accType=0)

### In `executeSingleKeyNumericFlat`:
- `leaves.size() == 1` (force-merged to 1 segment) → `canParallelize = false`
- Falls to **sequential path** (line ~5310+)
- Within sequential: `isMatchAll = true`, `liveDocs == null`, `allCountStar = true`
- Takes the **ultra-fast COUNT(*)-only path**: loads columnar cache via `loadNumericColumn()`, then sequential `findOrInsert` + `accData[slot]++`

### Result: **Fully sequential, single-threaded scan of ~25M docs per shard**

The doc-range parallel path is **unreachable** for single-segment shards because the guard `leaves.size() > 1` prevents it.

## 4. What It Would Take to Add Doc-Range Parallel to Two-Key and Three-Key Flat

### Current state:
- `executeTwoKeyNumericFlat` (line ~6130): Only has **segment-parallel** (partition segments across workers). No doc-range splitting within a segment.
- `executeThreeKeyFlat` (line ~10380): Same — only segment-parallel.
- Both check `leaves.size() > 1` for parallelism, so single-segment = sequential.

### Required changes for doc-range parallel in two-key/three-key:

1. **Add columnar cache loading** for both key columns (and agg columns if not COUNT(*)-only):
   ```java
   long[] key0Values = loadNumericColumn(leafCtx, keyInfos.get(0).name());
   long[] key1Values = loadNumericColumn(leafCtx, keyInfos.get(1).name());
   ```

2. **Create a `scanDocRangeFlatTwoKeyCountStar` helper** analogous to `scanDocRangeFlatSingleKeyCountStar`:
   ```java
   static void scanDocRangeFlatTwoKeyCountStar(
       long[] key0Values, long[] key1Values, int startDoc, int endDoc,
       Bits liveDocs, FlatTwoKeyMap flatMap, int accOffset0, int slotsPerGroup)
   ```

3. **Remove the `leaves.size() > 1` guard** for the doc-range path (or add a separate single-segment doc-range branch)

4. **Apply the same 10M threshold logic** — for >10M docs, sequential may still win due to merge overhead

5. **Handle non-COUNT(*) aggregates**: Need to also load aggregate columns into `long[]` arrays and pass them to the scan helper

### Estimated complexity: Medium
- The pattern is well-established in `executeSingleKeyNumericFlat`
- Main risk: memory pressure from loading 2-3 columns × 25M docs into `long[]` arrays (~200MB per column for 25M longs)
- The merge cost scales with unique groups × slotsPerGroup × numWorkers

## Key Constants
- `PARALLELISM_MODE`: default `"docrange"` (from `-Ddqe.parallelism`)
- `THREADS_PER_SHARD`: `availableProcessors / 4` (default)
- `FlatSingleKeyMap.MAX_CAPACITY`: 32M groups
- `FlatTwoKeyMap.MAX_CAPACITY`: 32M groups
- `FlatThreeKeyMap.MAX_CAPACITY`: 32M groups
- 10M doc threshold: hardcoded in `executeSingleKeyNumericFlat` line ~5213
