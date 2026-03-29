# Q15 Execution Trace: `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`

## Setup
- UserID = BigintType (numeric, not VARCHAR)
- Single numeric key, COUNT(*), ORDER BY + LIMIT
- File: `FusedGroupByAggregate.java`

---

## Q1: executeWithTopN() or executeInternal()?

**Both.** `executeWithTopN()` (line ~488) is a thin wrapper that delegates to `executeInternal()`:

```java
public static List<Page> executeWithTopN(..., int sortAggIndex, boolean sortAscending, long topN) {
    return executeInternal(aggNode, shard, query, columnTypeMap, sortAggIndex, sortAscending, topN);
}
```

The caller passes `sortAggIndex=0` (the COUNT(*) column), `sortAscending=false` (DESC), `topN=10`.

`execute()` (line ~510) also calls `executeInternal()` but with `sortAggIndex=-1, topN=0` (no top-N).

**For Q15 with ORDER BY + LIMIT: `executeWithTopN()` → `executeInternal()`.**

---

## Q2: Does executeNumericOnly() hit executeSingleKeyNumeric()?

**Yes.** In `executeInternal()` (line ~519):

1. `hasVarchar` = false (UserID is BigintType) → goes to `executeNumericOnly()` (line ~636)
2. In `executeNumericOnly()` (line ~648):
   - `keyInfos.size() == 1` ✓
   - `!anyTrunc` ✓ (no DATE_TRUNC/EXTRACT/ARITH expressions)
   - → dispatches to `executeSingleKeyNumeric()` (line ~700)

---

## Q3: Does canUseFlatAccumulators return true for COUNT(*)?

**Yes.** In `executeSingleKeyNumeric()` (line ~720):

```java
boolean canUseFlatAccumulators = true;
// For COUNT(*): isCountStar[0] = true → accType[0] = 0
// No code path sets canUseFlatAccumulators = false for COUNT(*)
```

COUNT(*) maps to `accType=0`, which is supported by the flat path. Only these set `canUseFlatAccumulators = false`:
- COUNT(DISTINCT) → accType=5
- SUM(double) → accType=6
- AVG(double) → accType=7
- MIN → accType=3
- MAX → accType=4
- VARCHAR args

**COUNT(*) stays on the flat accumulator path → `executeSingleKeyNumericFlat()`.**

---

## Q4: Estimated numBuckets for 100M docs?

In `executeSingleKeyNumeric()` (line ~3245):

```java
int numBuckets;
try (... estSearcher = shard.acquireSearcher("dqe-fused-groupby-estimate")) {
    long totalDocs = 0;
    for (LeafReaderContext leaf : estSearcher.getIndexReader().leaves()) {
        totalDocs += leaf.reader().maxDoc();
    }
    numBuckets = Math.max(1, (int) Math.ceil((double) totalDocs / FlatSingleKeyMap.MAX_CAPACITY));
}
```

- `totalDocs = 100,000,000`
- `FlatSingleKeyMap.MAX_CAPACITY = 8,000,000`
- `numBuckets = ceil(100M / 8M) = ceil(12.5) = 13`

**numBuckets = 13.**

Note: This uses `totalDocs` (maxDoc count) as a proxy for unique groups, which is a worst-case overestimate. With 17M unique UserIDs, only ceil(17M/8M)=3 buckets would actually be needed. The estimate is conservative.

---

## Q5: With 13 buckets, parallel or sequential?

In `executeSingleKeyNumeric()` (line ~3258):

```java
int parallelBuckets = Math.min(numBuckets, THREADS_PER_SHARD);
if (parallelBuckets > 1 && !"off".equals(PARALLELISM_MODE)) {
    // Parallel path: submit each bucket as a CompletableFuture to PARALLEL_POOL
    for (int b = 0; b < numBuckets; b++) {
        futures[b] = CompletableFuture.supplyAsync(() -> {
            return executeSingleKeyNumericFlat(shard, query, ..., bkt, numBuckets);
        }, PARALLEL_POOL);
    }
    CompletableFuture.allOf(futures).join();
    // Then merge results via mergePartitionedPages()
}
```

Key constants:
- `PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange")` → default "docrange" (not "off")
- `THREADS_PER_SHARD = max(1, availableProcessors / numLocalShards)` → on r5.4xlarge (16 vCPUs), with default 4 shards: `16/4 = 4`
- `parallelBuckets = min(13, 4) = 4` → > 1 ✓

**All 13 buckets run in parallel via PARALLEL_POOL (ForkJoinPool), with up to 4 concurrent workers.**

Each bucket call `executeSingleKeyNumericFlat(..., bucket, numBuckets)` filters docs by hash-partition:
```java
if (numBuckets > 1 && (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets != bucket) {
    return; // skip this doc
}
```

After all buckets complete, results are merged via `mergePartitionedPages()` which does a global top-N heap selection across all bucket Pages.

---

## Q6: Is there a TopN optimization that avoids materializing all groups?

**Yes, but only at the output stage, not during aggregation.**

Within each bucket's `executeSingleKeyNumericFlat()` (line ~3430):

```java
if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
    // Top-N selection using a min-heap on the flat accData array
    int n = (int) Math.min(topN, flatMap.size);
    int[] heap = new int[n];
    // ... heap-based selection of top-10 slots by COUNT value
}
```

This means:
1. **All groups within a bucket ARE fully materialized** in the FlatSingleKeyMap during scan
2. **Only top-N groups are extracted** from each bucket's map for the output Page
3. **Cross-bucket merge** (`mergePartitionedPages()`) does another top-N heap across all bucket Pages

**There is NO early-termination during aggregation for ORDER BY queries.** The `earlyTerminate` / `stopFlag` optimization only applies to LIMIT-without-ORDER-BY (where any N groups are valid). With ORDER BY DESC, all groups must be counted to find the true top-10.

**No streaming/approximate TopN exists** — the full hash map is built, then top-N is extracted post-aggregation.

---

## Summary: Complete Execution Path for Q15

```
executeWithTopN(sortAggIndex=0, sortAscending=false, topN=10)
  └→ executeInternal()
       └→ executeNumericOnly()  [hasVarchar=false]
            └→ executeSingleKeyNumeric()  [1 key, no expressions]
                 └→ canUseFlatAccumulators=true  [COUNT(*) only]
                      └→ numBuckets=13  [ceil(100M/8M)]
                           └→ parallelBuckets=4 > 1, mode="docrange"
                                └→ 13 × executeSingleKeyNumericFlat(bucket=0..12, numBuckets=13)
                                     [submitted to ForkJoinPool, 4 concurrent]
                                     Each bucket:
                                       - scanSegmentFlatSingleKey() per segment
                                       - Hash-partition filter: skip docs where hash(key)%13 != bucket
                                       - FlatSingleKeyMap accumulation (zero object alloc per group)
                                       - Top-10 heap extraction from flat map
                                └→ mergePartitionedPages()
                                     [global top-10 heap across 13 bucket Pages]
```

### Performance bottleneck for Q15 (14.7s):
- **13 full passes over 100M docs** (each bucket scans all docs, skipping ~92% via hash filter)
- Each pass reads UserID DocValues + does hash computation for partition filter
- Effective work: 13 × 100M DocValues reads = 1.3B reads (vs 100M needed)
- **13× read amplification** is the primary cost vs CH-Parquet's single-pass approach
