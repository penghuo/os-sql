# Q15 Dispatch Path Analysis: executeSingleKeyNumericFlat

## 1. Q15 Dispatch Path — YES, it uses executeSingleKeyNumericFlat

**Q15**: `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`

Dispatch chain (FusedGroupByAggregate.java):
1. `executeInternal()` (line 1280) classifies keys → UserID is numeric (BigintType), no varchar
2. `hasVarchar = false` → skips varchar paths (line 1388)
3. `keyInfos.size() == 1` → enters single-key numeric path (line 3195)
4. Within `executeSingleKeyNumeric()`, `canUseFlatAccumulators = true` (COUNT(*) is accType=0)
5. **First try**: calls `executeSingleKeyNumericFlat()` with bucket=0, numBuckets=1 (line 3595)
6. **On overflow** (17M unique keys > 16M MAX_CAPACITY): catches RuntimeException, falls to `executeSingleKeyNumericFlatMultiBucket()` (line 3613)

## 2. The 10M-Doc Sequential Threshold — FORCES Q15 SEQUENTIAL

**Location**: `executeSingleKeyNumericFlat()`, line ~5217

```java
// For high-cardinality keys (totalDocs > 10M), scan sequentially into main map.
// Parallel workers each build 4.4M-entry maps that thrash L3 cache and require
// expensive mergeFrom. Sequential scan uses ONE map with better cache locality.
if (totalDocs > 10_000_000) {
    flatMap = new FlatSingleKeyMap(slotsPerGroup, 8_000_000);
    for (int s = 0; s < segKeyArrays.size(); s++) {
        scanDocRangeFlatSingleKeyCountStar(
            segKeyArrays.get(s), 0, segKeyArrays.get(s).length,
            segLiveDocs.get(s), flatMap, accOffset[0], slotsPerGroup);
    }
}
```

**Condition**: `totalDocs > 10_000_000` (10M)
**Applies when**: `allCountStar && isMatchAll && numBuckets <= 1` — Q15 matches ALL of these!
**Result**: Q15 with 100M docs is FORCED into single-threaded sequential scan.

**Rationale in code comment**: Parallel workers each build 4.4M-entry maps that thrash L3 cache and require expensive `mergeFrom()`. Sequential scan uses ONE map with better cache locality.

## 3. Q15 Runs SEQUENTIAL on 100M Docs

**Confirmed**: Q15 hits the `totalDocs > 10_000_000` branch and runs single-threaded.

The parallel path (line ~5230) only activates for `totalDocs <= 10_000_000`:
- Creates `THREADS_PER_SHARD` workers via `CompletableFuture.supplyAsync()`
- Each worker builds a local `FlatSingleKeyMap` and scans a doc range
- Main thread merges via `flatMap.mergeFrom(workerMap)`

**But Q15 never reaches this** — 100M >> 10M threshold.

### Parallelism Configuration (line 118-127):
```java
PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");  // default: on
THREADS_PER_SHARD = availableProcessors() / numLocalShards;  // e.g., 16 cores / 4 shards = 4
PARALLEL_POOL = new ForkJoinPool(availableProcessors(), ..., asyncMode=true);
```

## 4. FlatSingleKeyMap Capacity vs 17M Unique UserIDs — OVERFLOW

**Location**: line 13535

```java
private static final int MAX_CAPACITY = 16_000_000;
private static final float LOAD_FACTOR = 0.7f;
```

**Q15 has ~17M unique UserIDs** but MAX_CAPACITY = 16M.

**What happens**:
1. First attempt with `executeSingleKeyNumericFlat(bucket=0, numBuckets=1)` starts scanning
2. When `size > MAX_CAPACITY` during `findOrInsert()`, `resize()` throws:
   ```java
   throw new RuntimeException("GROUP BY exceeded memory limit (" + size + " unique groups, max " + MAX_CAPACITY + ")...");
   ```
3. Caller catches this at line 3600 and falls back to `executeSingleKeyNumericFlatMultiBucket()`
4. Multi-bucket computes: `numBuckets = ceil(100M / 16M) = 7` buckets
5. Each bucket gets its own `FlatSingleKeyMap`, keys are hash-partitioned across buckets

**CRITICAL ISSUE**: The sequential scan in the first attempt processes potentially millions of rows before hitting the overflow exception — this is WASTED WORK. The exception-based fallback is expensive.

**Actually, wait** — re-reading the code more carefully: the 10M threshold check happens BEFORE the scan starts. So the flow is:
1. Load all segment key arrays into memory (columnar cache)
2. Check `totalDocs > 10_000_000` → YES for Q15
3. Create `FlatSingleKeyMap(slotsPerGroup, 8_000_000)` — pre-sized to 8M
4. Sequential scan all 100M docs
5. At ~16M unique keys, `resize()` throws RuntimeException
6. Exception propagates up to the try/catch at line 3596
7. Falls back to multi-bucket with `numBuckets = ceil(100M / 16M) = 7`

**So Q15 actually does DOUBLE WORK**: scans ~16M+ docs sequentially, fails, then re-scans ALL 100M docs in multi-bucket mode.

## 5. Q16 and Q18 Dispatch Paths

### Q16: `GROUP BY UserID, SearchPhrase` (2 keys, one varchar)
- `hasVarchar = true` (SearchPhrase is VarcharType)
- `keyInfos.size() == 2` but not single varchar → falls to N-key varchar path
- Dispatched to `executeNKeyVarcharPath()` (line 9295)
- Uses `HashMap<MergedGroupKey, AccumulatorGroup>` — NOT flat maps
- No FlatTwoKeyMap because FlatTwoKeyMap is only for 2 NUMERIC keys

### Q18: `GROUP BY UserID, minute(EventTime), SearchPhrase` (3 keys, one varchar)
- `hasVarchar = true` (SearchPhrase is VarcharType)
- `keyInfos.size() == 3` → falls to N-key varchar path
- Same `executeNKeyVarcharPath()` with `HashMap<MergedGroupKey, AccumulatorGroup>`
- Full object allocation per group (MergedGroupKey + AccumulatorGroup + Accumulator[])

### FlatTwoKeyMap (for reference):
- `MAX_CAPACITY = 16_000_000` (same as FlatSingleKeyMap)
- Only used by `executeTwoKeyNumericFlat()` for 2 NUMERIC keys (e.g., Q31/Q32)
- Q16/Q18 cannot use it because SearchPhrase is varchar

## 6. Key Performance Bottlenecks for Q15

| Issue | Impact | Location |
|-------|--------|----------|
| 10M threshold forces sequential | No parallelism for 100M docs | line ~5217 |
| MAX_CAPACITY=16M < 17M unique keys | Overflow → exception → multi-bucket fallback | line 13535 |
| Exception-based fallback = wasted work | Scans millions of docs before failing | line 3596-3613 |
| Multi-bucket re-scans ALL data | 7 buckets × 100M docs each pass | line 4711+ |
| Sequential scan in multi-bucket too? | Need to verify if multi-bucket also hits 10M threshold | line 4757 |

## 7. Parallelization Opportunities

1. **Raise MAX_CAPACITY to 32M**: Eliminates the overflow for 17M unique keys, avoids multi-bucket entirely. Memory cost: 32M × 8 bytes (keys) + 32M × 8 bytes (accData) = ~512MB — may be acceptable.

2. **Remove or raise the 10M threshold**: The comment says parallel workers "thrash L3 cache" but this deserves benchmarking. With 17M unique keys, the single map also thrashes L3. Parallel scan with merge might still win.

3. **Pre-estimate cardinality before scanning**: Use HyperLogLog or segment-level metadata to estimate unique keys BEFORE choosing single vs multi-bucket. Avoids the exception-based fallback.

4. **Partition-parallel instead of doc-range parallel**: Instead of each worker building a full map, hash-partition keys so each worker owns a subset. Eliminates merge entirely.

5. **Multi-bucket parallelism**: In `executeSingleKeyNumericFlatMultiBucket`, run buckets in parallel since they're independent.

## 8. CORRECTION: Multi-Bucket Path HAS Parallelism

The `executeSingleKeyNumericFlatMultiBucket()` (line 4757) DOES support parallelism:
- `canParallelize` check has NO 10M threshold — just checks `PARALLELISM_MODE != "off"` and `THREADS_PER_SHARD > 1`
- Uses segment-parallel: each worker gets its own set of `FlatSingleKeyMap[numBuckets]`
- Workers scan their assigned segments, hash-partitioning keys into bucket maps
- Main thread merges per-bucket: `bucketMaps[b].mergeFrom(workerMaps[b])`

**So Q15's actual execution path is**:
1. Try `executeSingleKeyNumericFlat(bucket=0, numBuckets=1)`
2. Hit `totalDocs > 10M` → sequential scan with single FlatSingleKeyMap(8M)
3. At ~16M unique keys → `resize()` throws RuntimeException
4. Catch → compute `numBuckets = ceil(100M/16M) = 7`
5. Call `executeSingleKeyNumericFlatMultiBucket(numBuckets=7)`
6. This path IS parallel (segment-parallel with THREADS_PER_SHARD workers)
7. Each worker builds 7 local FlatSingleKeyMaps, scans assigned segments
8. Merge 7 × numWorkers maps into 7 main bucketMaps

**Net cost**: Wasted sequential scan of ~16M+ docs (step 2-3) + 7 parallel passes over 100M docs + merge overhead of 7 × numWorkers maps.

## 9. Revised Summary of Q15 Flow

```
executeSingleKeyNumericFlat(bucket=0, numBuckets=1)
  ├── allCountStar=true, isMatchAll=true, numBuckets=1 → enters doc-range path
  ├── totalDocs=100M > 10M → SEQUENTIAL scan
  ├── FlatSingleKeyMap(slotsPerGroup=1, initCap=8M)
  ├── Scans sequentially... hits 16M unique keys
  └── resize() THROWS RuntimeException("GROUP BY exceeded memory limit")

CATCH → executeSingleKeyNumericFlatMultiBucket(numBuckets=7)
  ├── canParallelize=true → segment-parallel
  ├── numWorkers = min(THREADS_PER_SHARD, leaves.size())
  ├── Each worker: FlatSingleKeyMap[7], scans assigned segments
  ├── Hash-partition: bucket = hash(key) % 7
  ├── Merge: 7 buckets × numWorkers maps
  └── Output: collect from all 7 bucketMaps, top-N heap select
```
