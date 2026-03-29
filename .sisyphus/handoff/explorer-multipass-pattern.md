# Hash-Partitioned Multi-Pass Aggregation Pattern (Two-Key Flat Path)

Extracted from `FusedGroupByAggregate.java` — the two-key flat numeric path.

---

## 1. Constants (lines 116-127)

```java
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
private static final int THREADS_PER_SHARD =
    Math.max(1, Runtime.getRuntime().availableProcessors()
        / Integer.getInteger("dqe.numLocalShards", 4));
private static final ForkJoinPool PARALLEL_POOL =
    new ForkJoinPool(Runtime.getRuntime().availableProcessors(), ..., null, true);
```

`FlatTwoKeyMap.MAX_CAPACITY = 8_000_000` (line 11467)

---

## 2. Bucket Calculation (lines 4954-4961)

```java
int numBuckets;
try (Engine.Searcher estSearcher = shard.acquireSearcher("dqe-fused-groupby-estimate")) {
  long totalDocs = 0;
  for (LeafReaderContext leaf : estSearcher.getIndexReader().leaves()) {
    totalDocs += leaf.reader().maxDoc();
  }
  numBuckets = Math.max(1, (int) Math.ceil((double) totalDocs / FlatTwoKeyMap.MAX_CAPACITY));
}
```

Key: `numBuckets = ceil(totalDocs / MAX_CAPACITY)`, minimum 1.

---

## 3. Dispatch Logic (lines 4963-5040)

### Single bucket (no partitioning needed):
```java
if (numBuckets <= 1) {
  return executeTwoKeyNumericFlat(shard, query, keyInfos, specs, columnTypeMap,
      groupByKeys, numAggs, isCountStar, accType, sortAggIndex, sortAscending,
      topN, 0, 1);  // bucket=0, numBuckets=1
}
```

### Multi-bucket — parallel path (lines 4984-5010):
```java
int parallelBuckets = Math.min(numBuckets, THREADS_PER_SHARD);
if (parallelBuckets > 1 && !"off".equals(PARALLELISM_MODE)) {
  CompletableFuture<List<Page>>[] futures = new CompletableFuture[numBuckets];
  for (int b = 0; b < numBuckets; b++) {
    final int bkt = b;
    futures[b] = CompletableFuture.supplyAsync(() -> {
      return executeTwoKeyNumericFlat(shard, query, keyInfos, specs, columnTypeMap,
          groupByKeys, numAggs, isCountStar, accType, sortAggIndex, sortAscending,
          topN, bkt, numBuckets);
    }, PARALLEL_POOL);
  }
  CompletableFuture.allOf(futures).join();
  List<Page> allBucketResults = new ArrayList<>();
  for (var future : futures) {
    allBucketResults.addAll(future.join());
  }
  if (allBucketResults.isEmpty()) return List.of();
  if (allBucketResults.size() == 1) return allBucketResults;
  return mergePartitionedPages(allBucketResults, keyInfos, specs, columnTypeMap,
      groupByKeys, numAggs, accType, sortAggIndex, sortAscending, topN);
}
```

### Multi-bucket — sequential fallback (lines 5012-5040):
```java
List<Page> allBucketResults = new ArrayList<>();
for (int bucket = 0; bucket < numBuckets; bucket++) {
  List<Page> bucketPages = executeTwoKeyNumericFlat(shard, query, keyInfos, specs,
      columnTypeMap, groupByKeys, numAggs, isCountStar, accType, sortAggIndex,
      sortAscending, topN, bucket, numBuckets);
  allBucketResults.addAll(bucketPages);
}
if (allBucketResults.isEmpty()) return List.of();
if (allBucketResults.size() == 1) return allBucketResults;
return mergePartitionedPages(allBucketResults, keyInfos, specs, columnTypeMap,
    groupByKeys, numAggs, accType, sortAggIndex, sortAscending, topN);
```

---

## 4. Bucket Hash Filter in Hot Loop (lines 5698-5703)

Inside `collectFlatTwoKeyDoc()` (line 5692):

```java
long key0 = 0, key1 = 0;
if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
if (dv1 != null && dv1.advanceExact(doc)) key1 = dv1.nextValue();
// Hash-partition filter: skip docs not in this bucket
if (numBuckets > 1) {
  int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
  if (docBucket != bucket) return;
}
int slot = flatMap.findOrInsert(key0, key1);
// ... accumulate into flatMap.accData[slot * slotsPerGroup + accOffset[i]]
```

The hash function (`TwoKeyHashMap.hash2`, line 11565):
```java
static int hash2(long k0, long k1) {
  long h = k0 * 0x9E3779B97F4A7C15L + k1;
  h ^= h >>> 33;
  h *= 0xff51afd7ed558ccdL;
  h ^= h >>> 33;
  return (int) h;
}
```

For single-key, the equivalent would be a single-key hash (Murmur3 finalizer on one long).

---

## 5. Method Signatures — Bucket Parameters

`executeTwoKeyNumericFlat` (line 5244) takes `int bucket, int numBuckets` as last two params.
These are passed through to `scanSegmentFlatTwoKey` (line 5542), which passes them to `collectFlatTwoKeyDoc` (line 5692).

---

## 6. mergePartitionedPages (line 8216)

Combines results from all bucket passes:
- Counts total rows across all bucket pages
- If no sort/topN: concatenates all pages into one (with optional topN limit)
- If sort+topN: builds (pageIdx, rowIdx) index arrays, extracts sort values, uses min/max-heap to select top-N across all bucket pages
- This method is **shared** — it works for any key count since it operates on Page objects

---

## Summary: What to Replicate for Single-Key

1. **Add `bucket` and `numBuckets` params** to `executeOneKeyNumericFlat` and `scanSegmentFlatOneKey` / `collectFlatOneKeyDoc`
2. **Bucket calculation**: Same pattern — `ceil(totalDocs / FlatOneKeyMap.MAX_CAPACITY)`
3. **Hash filter in hot loop**: `if (numBuckets > 1) { int docBucket = (hash(key) & 0x7FFFFFFF) % numBuckets; if (docBucket != bucket) return; }`
4. **Dispatch**: Same 3-way: single bucket → direct call; multi-bucket parallel → CompletableFuture array; multi-bucket sequential → loop
5. **Merge**: Reuse `mergePartitionedPages` — it's Page-level, key-count agnostic
