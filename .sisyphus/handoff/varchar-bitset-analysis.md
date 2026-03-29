# VARCHAR Bitset Lockstep Analysis

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

---

## 1. Entry Point: `executeWithTopN()` (line 907)

```java
public static List<Page> executeWithTopN(
    AggregationNode aggNode, IndexShard shard, Query query,
    Map<String, Type> columnTypeMap, int sortAggIndex, boolean sortAscending, long topN)
```

Delegates to `executeInternal()` (line 937), which:
- Parses `AggSpec` list from aggregation functions
- Classifies keys via `KeyInfo` (detecting VARCHAR, DATE_TRUNC, EXTRACT, arithmetic, eval)
- Dispatches single VARCHAR key to `executeSingleVarcharGeneric()` (line ~1070)

Dispatch condition (line ~1068):
```java
if (keyInfos.size() == 1 && keyInfos.get(0).isVarchar) {
    return executeSingleVarcharGeneric(shard, query, keyInfos, specs, ...);
}
```

---

## 2. Single VARCHAR Key Path: `executeSingleVarcharGeneric()` (line ~1955)

```java
private static List<Page> executeSingleVarcharGeneric(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int sortAggIndex, boolean sortAscending, long topN)
```

### Single-segment fast path (ordinal array):
- Opens `SortedSetDocValues` for the key column
- If `ordCount <= 10_000_000`: allocates `AccumulatorGroup[ordCount]` indexed by ordinal
- **MatchAllDocsQuery**: iterates docs via `dv.nextDoc()` forward-only, uses `ordValue()` for single-valued fields
- **Filtered queries (else branch, line ~2097)**: uses `engineSearcher.search(query, new Collector{...})`
  - Collector calls `dv.advanceExact(doc)` per doc, gets ordinal, accumulates
  - **NO bitset lockstep optimization exists here** — this is the gap

### Per-doc accumulation:
- Calls `collectVarcharGenericAccumulate()` (line 12635) which uses `advanceExact(doc)` on each agg DocValues
- Supports: COUNT(*), COUNT(DISTINCT), SUM, AVG, MIN, MAX on numeric and VARCHAR args
- Uses `AccumulatorGroup` objects with `MergeableAccumulator[]` — object-based, not flat long[]

### Multi-segment path:
- Builds `OrdinalMap` for global ordinals
- For filtered queries: iterates `scorer.iterator()` docs, maps segment ord → global ord
- Also uses Collector-based iteration, no bitset optimization

### Output:
- Top-N: builds min/max heap over ordGroups[], outputs top-N entries
- No top-N: iterates all non-null ordGroups[], builds Page

---

## 3. Numeric Bitset Lockstep Path: `executeSingleKeyNumericFlat()` (line 4438)

```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int numAggs, boolean[] isCountStar, int[] accType,
    int sortAggIndex, boolean sortAscending, long topN,
    int bucket, int numBuckets)
```

### Flat accumulator layout:
- `FlatSingleKeyMap` with contiguous `long[] accData`
- COUNT/SUM: 1 slot, AVG: 2 slots (sum + count)
- No object allocation per group

### Filtered path with bitset lockstep (line ~4968):

**Selectivity check:**
```java
int estCount = weight.count(leafCtx);
boolean useBitsetLockstep = estCount >= 0 && estCount < maxDoc / 2
    && dv0 != null && !hasApplyLen;
```

**Phase 1 — Collect matching docs into bitset:**
```java
FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
DocIdSetIterator disi = scorer.iterator();
for (int d = disi.nextDoc(); d != NO_MORE_DOCS; d = disi.nextDoc()) {
    matchingDocs.set(d);
}
```

**Phase 2 — Dense agg check:**
```java
boolean allAggDense = true;
for (int i = 0; i < numAggs; i++) {
    if (!isCountStar[i] && numericAggDvs[i] == null) { allAggDense = false; break; }
}
```

**Phase 3 — Lockstep iteration (allAggDense=true):**
- Advances key DV and agg DVs with `nextDoc()` (forward-only, no advanceExact)
- Iterates `matchingDocs.nextSetBit(doc + 1)` — only visits matching docs
- For each matching doc: advances key DV to doc, reads key value, hash-inserts into flatMap
- For each agg: advances agg DV to doc, reads value, switch-accumulates

Key advantage: `nextDoc()` is ~3x faster than `advanceExact()` for dense DocValues because it avoids binary search in the DV index.

**Phase 3 — Fallback (hasApplyLen or !allAggDense):**
- Still uses `matchingDocs.nextSetBit()` iteration but calls `collectFlatSingleKeyDocWithLength()` which uses `advanceExact()`

**Non-bitset fallback (estCount >= maxDoc/2 or estCount unknown):**
```java
DocIdSetIterator docIt = scorer.iterator();
while ((doc = docIt.nextDoc()) != NO_MORE_DOCS) {
    collectFlatSingleKeyDocWithLength(doc, dv0, flatMap, ...);
}
```

---

## 4. Gap Analysis: What the VARCHAR Path Needs

| Aspect | Numeric Flat Path | VARCHAR Generic Path |
|--------|-------------------|---------------------|
| Filtered iteration | Bitset lockstep with `nextSetBit()` | Collector-based `search()` |
| Selectivity check | `weight.count() < maxDoc/2` | None |
| DV iteration | Forward-only `nextDoc()` | `advanceExact(doc)` per doc |
| Accumulator storage | Flat `long[]` | `AccumulatorGroup` objects |
| Key lookup | `FlatSingleKeyMap.findOrInsert(long)` | `ordGroups[ord]` array index |

### To port bitset lockstep to VARCHAR:

1. **In single-segment filtered branch** (line ~2097, currently Collector-based):
   - Add `weight.count(leafCtx)` selectivity check
   - If selective: collect into `FixedBitSet`, iterate with `nextSetBit()`
   - Use `SortedSetDocValues.nextDoc()` lockstep instead of `advanceExact()`
   - Keep `ordGroups[]` accumulation (already efficient for VARCHAR)

2. **In multi-segment filtered branch** (line ~2340):
   - Same pattern: bitset + nextSetBit + nextDoc lockstep on segment DVs
   - Map segment ord → global ord as before

3. **Key difference from numeric**: VARCHAR uses ordinal-indexed array (no hash), so the per-doc cost is already low. The main win is replacing `advanceExact()` with forward-only `nextDoc()` on the key DV and agg DVs.

### Expected impact:
- Q37 (URL GROUP BY, CounterID=62): 4.02x target — highly selective filter, few matching docs
- Q38 (Title GROUP BY, CounterID=62): 2.75x target — same filter pattern
- Both have <1% selectivity, making bitset lockstep ideal
