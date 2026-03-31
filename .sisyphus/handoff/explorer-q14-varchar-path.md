# Q14 VARCHAR Path Analysis: executeWithVarcharKeys Filter Handling

## Method Signature (line 8203)
```java
private static List<Page> executeWithVarcharKeys(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int sortAggIndex, boolean sortAscending, long topN)
```

## Filter Handling Architecture (Two-Key Flat Path, Single Segment)

### Decision Tree (line 8338+)
1. **MatchAllDocsQuery** (line 8338): Direct doc iteration `for (doc=0; doc<maxDoc; doc++)`, no Lucene Collector overhead
2. **Filtered query** (line 8683): Branches on selectivity:
   - **Selective (<50% docs)** → Bitset pre-collection path (line 8695)
   - **Broad (≥50% docs)** → Collector-based path (line 8775)

### Bitset Path (line 8689-8724) — Q14 likely uses this
```
Weight weight = engineSearcher.createWeight(rewrite(query), NO_SCORES, 1.0f);  // L8689
int estCount = weight.count(leafCtx);                                           // L8691
boolean useDirectBitset = (estCount >= 0 && estCount < maxDoc / 2);             // L8692
```
When selective:
- Creates `FixedBitSet matchingDocs` (line 8696)
- Gets `Scorer` from Weight, iterates `scorer.iterator()` to populate bitset (line 8697-8702)
- Then iterates `matchingDocs.nextSetBit(0)` for doc-value reads (line 8723)
- Uses `advanceExact(doc)` per doc for key/agg DV reads (not forward-only)

### Collector Path (line 8775-8845)
When broad filter (≥50%):
- `engineSearcher.search(query, new Collector{...})` — standard Lucene Collector
- LeafCollector.collect(doc) does same key/agg accumulation per doc
- ScoreMode.COMPLETE_NO_SCORES (line 8843)

## VARCHAR Ordinal Resolution

### During Accumulation (hot loop)
- VARCHAR keys stored as **ordinals** (long values from `SortedSetDocValues.nextOrd()`)
- Ordinals used as hash keys in `FlatTwoKeyMap` — no string allocation during scan
- Example at line 8727: `SortedSetDocValues dv; if (dv.advanceExact(doc)) k0 = dv.nextOrd();`

### At Output Time (line 8855+)
- Ordinals resolved to strings via `dv.lookupOrd(kv0)` → `BytesRef` (line 8996)
- Written as `Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length)` — zero-copy from BytesRef
- Only output slots are resolved (top-N heap selects slots first, lines 8866-8975)

## Comparison: executeTwoKeyNumericFlat Filter Handling (line 6810)

### Key Difference: No Bitset Optimization
- `useBitsetLockstep = false` (line 6820) — **explicitly disabled** ("causes EOFException on two-key DocValues")
- Uses **forward-only DV iteration** with `dv.advance(doc)` lockstep (line 6824-6860)
- Scorer.iterator().nextDoc() drives the loop, DV readers advance forward to match

### Forward-Only vs advanceExact
| Path | Filter Strategy | DV Access |
|------|----------------|-----------|
| executeWithVarcharKeys (bitset) | FixedBitSet pre-collect | `advanceExact(doc)` per doc |
| executeTwoKeyNumericFlat | Scorer iterator lockstep | `dv.advance(doc)` forward-only |

The numeric flat path avoids bitset allocation but requires careful DV position tracking.

## executeNKeyVarcharPath (line 10403) — Generic N-Key Fallback

### Filter Paths:
1. **3-key flat path** (line 10427): Delegates to `executeThreeKeyFlat` if compatible agg types
2. **Global ordinal path** (line 10831): Used when `sortAggIndex >= 0 && topN > 0` and VARCHAR keys have ≤1M ordinals. Builds `OrdinalMap` for cross-segment merge. Uses Weight/Scorer per segment (line 10963-10966)
3. **Parallel doc-range** (line 11341): `executeNKeyVarcharParallelDocRange` — collects matched docs into int[], splits among workers
4. **Generic Collector** (line 11365): Standard `engineSearcher.search(query, Collector)` with per-doc BytesRef resolution via `lookupOrd`

## executeNKeyVarcharParallelDocRange (line 11766)

- Creates Weight once (line 11791), gets Scorer per segment (line 11815)
- Collects ALL matching doc IDs into `int[] matchedDocs` array (not bitset)
- Splits doc array among `numWorkers` threads by chunk
- Each worker opens its own DV readers and processes its doc range
- Uses `advanceExact(doc)` for random-access DV reads within each worker
- Supports early termination via `AtomicBoolean stopFlag` for LIMIT without ORDER BY

## Key Takeaways for Q14

1. Q14's `SearchPhrase <> ''` filter likely triggers the **bitset path** (line 8695) if <50% selectivity
2. VARCHAR ordinals avoid string allocation during scan — resolution deferred to output
3. The bitset path uses `advanceExact` (random access) vs the numeric path's forward-only `advance`
4. If selectivity is >50%, falls back to Collector path with higher per-doc overhead
5. The selectivity threshold is `estCount < maxDoc / 2` (line 8692)
