# Q04 Execution Analysis: COUNT(DISTINCT UserID) FROM hits

## Architecture Overview

```
Coordinator (TransportTrinoSqlAction)
  ├── Shard 0 → dqe-shard-executor pool thread
  ├── Shard 1 → dqe-shard-executor pool thread
  ├── Shard 2 → dqe-shard-executor pool thread
  └── Shard 3 → coordinator thread (inline)
  → mergeCountDistinctValuesViaRawSets() → parallel contains() counting
```

## Thread Pool Configuration

- **dqe-shard-executor**: `max(1, availableProcessors / 2)` fixed threads (SQLPlugin.java:355)
- **PARALLEL_POOL** (intra-shard): `availableProcessors` ForkJoinPool threads (FusedGroupByAggregate.java:124-128)
- Shards 0..N-2 dispatched to dqe-shard-executor; shard N-1 runs on coordinator thread

## Critical Path (Single-Segment per Shard — Force-Merged Index)

With 1 segment per shard, the code takes the **single-segment path** which is entirely **single-threaded** within each shard:

### Step 1: Wasted Pre-allocation (FusedScanAggregate.java:1682-1685)
```java
distinctSet = totalDocs > 0
    ? new LongOpenHashSet(Math.min(totalDocs, 32_000_000))
    : new LongOpenHashSet();
```
- totalDocs=25M → capacity = nextPow2(25M/0.65) = **67,108,864** → **512MB** allocation + Arrays.fill(EMPTY)
- This set IS used in single-segment path (line 1820: `distinctSet.add(v)`)
- **Problem**: Pre-sized for 25M docs but only ~4.25M distinct values → **6x oversized**

### Step 2: Phase 1 — DocValues Load (lines 1738-1760, SINGLE-THREADED)
```java
long[] vals = new long[maxDoc];  // 25M × 8 = 200MB allocation
int doc = dv.nextDoc();
while (doc != DocIdSetIterator.NO_MORE_DOCS) {
    vals[count++] = dv.nextValue();
    doc = dv.nextDoc();
}
```
- Allocates 200MB intermediate array
- Sequential DocValues read: ~25M nextDoc()+nextValue() calls
- **Not parallelized** for single segment

### Step 3: Phase 2 — Run-Length Dedup + Hash Insert (lines 1812-1822, SINGLE-THREADED)
```java
long prev = Long.MIN_VALUE;
for (int i = 0; i < count; i++) {
    long v = vals[i];
    if (v != prev) {
        distinctSet.add(v);  // Murmur3 hash + linear probe into 512MB table
        prev = v;
    }
}
```
- 25M iterations, ~4.25M unique values inserted
- Each insert: Murmur3 finalizer (6 multiply/xor ops) + linear probe into 512MB table
- **Run-length dedup**: skips consecutive duplicates (sorted by UserID)
- Dedup ratio: 25M → ~4.25M = **83% of iterations skip the hash insert** (good!)
- **Not parallelized** for single segment — this is the key bottleneck

### Step 4: Coordinator Merge (TransportTrinoSqlAction.java:2026-2100)
```java
// Find largest shard set, merge non-largest into 'others', then parallel contains()
long[] othersKeys = others.keys();
// Split into chunks, parallel count entries not in largest
for (int j = start; j < end; j++) {
    if (othersKeys[j] != emptyMarker && !L.contains(othersKeys[j])) localCount++;
}
total = largest.size() + extraCount.get();
```
- Already parallelized with up to 8 workers
- Uses read-only contains() against largest set (no mutation)

## LongOpenHashSet Internals (LongOpenHashSet.java)

| Property | Value |
|----------|-------|
| Load factor | 0.65 |
| Hash function | Murmur3 finalizer (h ^= h>>>33; h *= 0xff51afd7ed558ccdL; ...) |
| Collision resolution | Linear probing |
| Sentinel | Long.MIN_VALUE (EMPTY marker) |
| Special values | hasZero flag, hasSentinel flag |
| Resize | 2× capacity, rehash all entries |
| addAll | Iterates FULL capacity array (not just occupied slots) |

### addAll Inefficiency
```java
public void addAll(LongOpenHashSet other) {
    long[] otherKeys = other.keys;
    for (int i = 0; i < otherKeys.length; i++) {  // iterates ALL capacity slots
        if (otherKeys[i] != EMPTY) { add(otherKeys[i]); }
    }
}
```
At 0.65 load factor, **35% of iterations are wasted** scanning empty slots.

## Estimated Cost Breakdown (per shard, ~25M rows)

| Step | Operation | Est. Time | Notes |
|------|-----------|-----------|-------|
| 1 | LongOpenHashSet(25M): alloc 512MB + Arrays.fill | ~50ms | Memory bandwidth bound |
| 2 | long[25M] alloc (200MB, JVM zeroed) | ~20ms | |
| 3 | Read 25M DocValues | ~100-150ms | Sequential I/O, likely cached |
| 4 | 25M loop + 4.25M hash inserts into 512MB table | ~100-150ms | Cache-unfriendly random access |
| **Total per shard** | | **~270-370ms** | |

With 4 shards: 3 on pool + 1 on coordinator thread. If pool has ≥3 threads, wall clock ≈ max(shard) + merge ≈ 370ms + merge.
**But**: pool size = `availableProcessors/2`. If only 2 threads, one shard waits → ~740ms + merge.

## Optimization Opportunities (Ranked by Impact)

### OPT-1: CRITICAL — Fuse DV Read + Hash Insert for Single Segment
**Eliminate the 200MB intermediate array entirely.**
```java
// Instead of: read all into long[], then iterate long[] with dedup
// Do: read DocValues directly with run-length dedup into hash set
long prev = Long.MIN_VALUE;
int doc = dv.nextDoc();
while (doc != DocIdSetIterator.NO_MORE_DOCS) {
    long v = dv.nextValue();
    if (v != prev) { distinctSet.add(v); prev = v; }
    doc = dv.nextDoc();
}
```
- Saves: 200MB allocation + 25M array writes + 25M array reads
- With 83% dedup ratio, only 17% of DV reads trigger a hash probe
- The original concern about "interleaved random hash probes with sequential DV reads" is mitigated by the high dedup ratio
- **Est. savings: 70-100ms per shard (20-30%)**

### OPT-2: HIGH — Right-Size the Hash Set
Pre-sized for `min(totalDocs, 32M)` = 25M, but only ~4.25M distinct values.
```java
// Option A: Two-pass — first count distinct via run-length dedup, then allocate
// Option B: Use PointValues min/max to estimate range, or segment-level metadata
// Option C: Start smaller (e.g., totalDocs/4) and let resize handle it
//           Resize from 8M→16M is cheaper than starting at 64M
```
- Current: capacity=67M (512MB). Optimal: capacity=8M (64MB) — **8× less memory**
- Arrays.fill savings alone: ~40ms per shard
- Better cache behavior for hash probes: 64MB fits in L3 vs 512MB thrashing
- **Est. savings: 50-80ms per shard (15-25%)**

### OPT-3: HIGH — Bitset Path for Bounded Ranges
If UserID values fit in a bounded range (e.g., [0, 2^24) = 16M), use a bitset:
```java
PointValues pv = reader.getPointValues(columnName);
long min = NumericUtils.sortableBytesToLong(pv.getMinPackedValue(), 0);
long max = NumericUtils.sortableBytesToLong(pv.getMaxPackedValue(), 0);
long range = max - min + 1;
if (range <= 32_000_000) {  // 4MB bitset
    BitSet bits = new BitSet((int) range);
    // ... set bits, return bits.cardinality()
}
```
- BitSet(17M) = ~2MB vs LongOpenHashSet = 512MB — **256× less memory**
- No hash computation, no collision handling, perfect cache behavior
- `bits.cardinality()` uses Long.bitCount() — extremely fast
- **Est. savings: 100-200ms per shard if applicable (40-60%)**
- **Requires**: checking PointValues min/max at runtime to verify range is bounded

### OPT-4: MEDIUM — Parallel Phase 2 for Single Segment
Currently single-threaded. Split the vals[] array into chunks:
```java
int numWorkers = Math.min(Runtime.getRuntime().availableProcessors(), 4);
// Each worker: run-length dedup on its chunk → per-worker LongOpenHashSet
// Then merge worker sets (largest-first)
```
- **Complication**: run-length dedup across chunk boundaries needs care
- **Complication**: merge cost may offset parallelism gains
- Only useful if OPT-1 (fused path) is not adopted
- **Est. savings: 30-50ms per shard**

### OPT-5: LOW — Count-Only Coordinator Path
The coordinator already does a smart "count entries in others not in largest" without mutating the largest set. This is already well-optimized. Minor improvement: pre-size the `others` set.

## Key Data Points for Decision-Making

- **17M distinct UserIDs** across 100M rows (17% selectivity)
- **4 shards**, 1 segment each (force-merged)
- ~4.25M distinct per shard (if range-partitioned) or up to 17M (if hash-partitioned)
- Run-length dedup eliminates **83%** of hash insertions
- dqe-shard-executor pool: `availableProcessors/2` threads
- PARALLEL_POOL: `availableProcessors` ForkJoinPool threads (unused for single-segment path!)

## File References

| File | Lines | Purpose |
|------|-------|---------|
| `dqe/.../FusedScanAggregate.java` | 1674-1850 | collectDistinctValuesRaw — the hot path |
| `dqe/.../LongOpenHashSet.java` | 1-230 | Custom open-addressing hash set |
| `dqe/.../TransportTrinoSqlAction.java` | 540-620 | Shard dispatch (parallel) |
| `dqe/.../TransportTrinoSqlAction.java` | 2026-2100 | Coordinator merge |
| `plugin/.../SQLPlugin.java` | 332-360 | Thread pool configuration |
| `dqe/.../FusedGroupByAggregate.java` | 124-135 | PARALLEL_POOL config |
