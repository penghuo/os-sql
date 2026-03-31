# Q18 Three-Key Flat Path Analysis

## 1. Dispatch Path for Q18

Q18 has 3 keys: `UserID` (BIGINT), `EXTRACT(MINUTE FROM EventTime)` (INT→BIGINT), `SearchPhrase` (VARCHAR).

**Dispatch chain:**
1. `execute()` → `executeInternal()` → detects `hasVarchar=true` (SearchPhrase is VARCHAR)
2. Since keyInfos.size() != 1, skips single-varchar paths
3. Since no eval keys, skips `executeWithEvalKeys`
4. Enters `executeWithVarcharKeys()`
5. `singleSegment=true` (1 segment per shard), `keyInfos.size()=3` → falls through 2-key paths
6. Enters `executeNKeyVarcharPath()`
7. Checks 3-key flat eligibility:
   - `keyInfos.size() == 3` ✓
   - `singleSegment || !anyVarcharKey` → `singleSegment=true` ✓
   - All aggs are COUNT(*) → `canUseFlatThreeKey=true` ✓
8. Calls `executeThreeKeyFlat()` with `bucket=0, numBuckets=1`

**Key expression handling:** EXTRACT(MINUTE FROM EventTime) is pre-classified as `KeyInfo(sourceCol="EventTime", type=BigintType, isVarchar=false, exprFunc="extract", exprUnit="MINUTE")`. The `arithUnits[1]` is set to `"E:MINUTE"` which routes through `applyArith()` → `applyExtract()`.

## 2. executeThreeKeyFlat Inner Loop

**Location:** `FusedGroupByAggregate.java` — `scanSegmentFlatThreeKey()` method (called by `executeThreeKeyFlat`)

### Per-document operations (MatchAll, liveDocs==null, allCountStar):

```java
for (int doc = 0; doc < maxDoc; doc++) {
    // 1. Read key0 (UserID) — SortedNumericDocValues.advanceExact + nextValue
    long k0 = 0;
    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
    if (dv != null && dv.advanceExact(doc)) {
        k0 = dv.nextValue();
        // no transform for plain BIGINT key
    }

    // 2. Read key1 (EXTRACT(MINUTE FROM EventTime)) — advanceExact + nextValue + applyArith
    long k1 = 0;
    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
    if (dv != null && dv.advanceExact(doc)) {
        k1 = dv.nextValue();
        // arithUnits[1] = "E:MINUTE" → applyExtract(k1, "MINUTE")
        // = (((k1/1000 / 60) % 60) + 60) % 60  (pure arithmetic, no allocation)
        k1 = applyArith(k1, arithUnits[1]);
    }

    // 3. Read key2 (SearchPhrase) — SortedSetDocValues.advanceExact + nextOrd
    long k2 = 0;
    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[2];
    if (dv != null && dv.advanceExact(doc)) k2 = dv.nextOrd();
    // VARCHAR key stored as segment-local ordinal (long) — NO string resolution

    // 4. Hash probe + insert into FlatThreeKeyMap
    int slot = flatMap.findOrInsert(k0, k1, k2);

    // 5. Accumulate COUNT(*)
    flatMap.accData[slot * flatSlotsPerGroup]++;
}
```

### Per-doc cost breakdown:
| Operation | Cost | Notes |
|-----------|------|-------|
| `advanceExact(doc)` × 3 keys | ~3 binary searches in DV | Dominant cost for random access |
| `nextValue()` × 2 numeric | Cheap (read from positioned iterator) | |
| `nextOrd()` × 1 varchar | Cheap (read from positioned iterator) | |
| `applyExtract(MINUTE)` | ~5 arithmetic ops | Pure integer math, no allocation |
| `FlatThreeKeyMap.findOrInsert()` | Hash + linear probe | `hash3(k0,k1,k2)` + compare 3 longs |
| `accData[slot*1]++` | 1 array write | COUNT(*) = 1 slot |

## 3. VARCHAR Ordinal Handling (Single-Segment Case)

**Critical insight:** With 1 segment per shard, SearchPhrase's `SortedSetDocValues` ordinals are **stable and unique** within the segment. The ordinal is used directly as the hash key (a `long`), avoiding:
- No `lookupOrd()` during aggregation (no BytesRef allocation)
- No String creation during aggregation
- No HashMap<String,...> overhead

**Ordinal resolution happens ONLY at output time** (after aggregation completes):
```java
// In executeThreeKeyFlat output building:
outputKeyReaders[ki] = leaves.get(0).reader().getSortedSetDocValues(keyInfos.get(ki).name);
// ...
BytesRef bytes = dv.lookupOrd(keyVals[ki]);  // Only for output rows
```

For Q18 with ORDER BY + LIMIT 10, only 10 ordinals are resolved to strings.

## 4. FlatThreeKeyMap Memory Layout

**Location:** Inner class `FlatThreeKeyMap`

```
keys0[capacity]     — long[] — key0 values (UserID)
keys1[capacity]     — long[] — key1 values (EXTRACT result)
keys2[capacity]     — long[] — key2 values (SearchPhrase ordinal)
accData[capacity * slotsPerGroup] — long[] — accumulator data
```

- `EMPTY_KEY = Long.MIN_VALUE` sentinel for empty slots
- `INITIAL_CAPACITY = 8192`, `LOAD_FACTOR = 0.7`, `MAX_CAPACITY = 32M`
- Open-addressing with linear probing, power-of-2 capacity
- Hash: `hash3(k0, k1, k2)` — Murmur3-inspired mixing of 3 longs

**For Q18 (COUNT(*) only):** `slotsPerGroup = 1`, so `accData` has 1 long per slot.

**Memory for 25M docs with ~6M unique groups:**
- keys0: 6M × 8B = 48MB
- keys1: 6M × 8B = 48MB  
- keys2: 6M × 8B = 48MB
- accData: 6M × 8B = 48MB
- Total: ~192MB per shard (at 0.7 load factor, capacity ~8.6M → ~275MB actual)

## 5. Where Time Is Spent (37s on 100M rows = 25M docs/shard × 4 shards)

**Per shard: 25M docs, sequential scan:**

| Phase | Est. Time | Notes |
|-------|-----------|-------|
| DocValues advanceExact × 3 | ~60-70% | 3 binary searches per doc × 25M docs. DV compressed blocks require decompression. |
| Hash probe (findOrInsert) | ~15-20% | hash3 + linear probe. High cardinality (~6M groups) means longer probe chains. |
| applyExtract(MINUTE) | ~2-3% | Pure arithmetic, negligible |
| accData increment | ~1-2% | Single array write |
| Output (top-N + ordinal resolve) | <1% | Only 10 groups resolved |

**Bottleneck:** DocValues random access (`advanceExact`) dominates. With 25M docs and 3 keys, that's 75M advanceExact calls per shard.

## 6. Parallelization Opportunities & Challenges

### Current state:
```java
// In executeThreeKeyFlat:
boolean canParallelize =
    !"off".equals(PARALLELISM_MODE)
        && THREADS_PER_SHARD > 1
        && leaves.size() > 1
        && !anyVarcharKey;  // ← BLOCKS parallelism for Q18!
```

**Q18 is NOT parallelized** because:
1. `leaves.size() == 1` (single segment) — blocks segment-parallel
2. `anyVarcharKey == true` (SearchPhrase is VARCHAR) — blocks even if multi-segment

### Challenge: VARCHAR ordinals are segment-local

For **segment-parallel** (multi-segment): ordinals from different segments are incompatible. The existing 2-key path solves this with `OrdinalMap` (global ordinals), but `executeThreeKeyFlat` doesn't implement this yet.

For **doc-range parallel** (single segment): ordinals ARE stable within a segment, so splitting doc ranges across workers is safe. Each worker reads the same `SortedSetDocValues` and gets the same ordinals.

### Doc-range parallel design for Q18:

```
Approach: Split [0, maxDoc) into N ranges, each worker gets own FlatThreeKeyMap
- Worker 0: docs [0, 6.25M)     → local FlatThreeKeyMap
- Worker 1: docs [6.25M, 12.5M) → local FlatThreeKeyMap  
- Worker 2: docs [12.5M, 18.75M)→ local FlatThreeKeyMap
- Worker 3: docs [18.75M, 25M)  → local FlatThreeKeyMap
Then: mergeFrom() all worker maps into one
```

**Key challenges:**
1. **DocValues are forward-only iterators** — each worker needs its own DV instance opened at the segment. `reader.getSortedSetDocValues()` returns a new iterator per call, so this works.
2. **advanceExact vs sequential** — workers starting at doc > 0 must use `advanceExact(startDoc)` to position, then can use `advanceExact` for each doc in range. No `nextDoc()` lockstep possible since workers start at arbitrary positions.
3. **Merge cost** — `FlatThreeKeyMap.mergeFrom()` iterates all slots of the source map. With ~6M groups and 4 workers, merge processes ~18M entries. At ~10ns/entry, merge ≈ 180ms (small vs 37s total).
4. **Memory** — 4 worker maps × ~275MB = ~1.1GB. Acceptable for a 25M-doc shard.
5. **VARCHAR ordinals are safe** — single segment means all workers see identical ordinal space. No global ordinal mapping needed.

### Expected speedup:
- 4 workers on 4 cores: ~3.5-4x speedup (limited by merge overhead and memory bandwidth)
- 37s → ~10s estimated

### Implementation location:
The `canParallelize` check in `executeThreeKeyFlat` needs to be relaxed for single-segment case:
```java
// Current (blocks Q18):
boolean canParallelize = ... && leaves.size() > 1 && !anyVarcharKey;

// Proposed (enables doc-range for single-segment with varchar):
boolean canParallelizeSegment = ... && leaves.size() > 1 && !anyVarcharKey;
boolean canParallelizeDocRange = ... && leaves.size() == 1;  // varchar ordinals safe in single segment
```

The doc-range split pattern already exists in `executeSingleKeyNumericFlat` (see `scanDocRangeFlatSingleKeyCountStar`) and can be adapted for the 3-key case.
