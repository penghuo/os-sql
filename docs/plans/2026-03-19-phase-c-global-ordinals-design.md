# Phase C: Global Ordinals in DQE — Design Document

## Problem

DQE's FusedGroupByAggregate is 200-2000x slower than OpenSearch's native DSL aggregation on the same data:

| Query | ClickHouse | OS Native DSL (warm) | DQE | DQE/DSL |
|-------|-----------|---------------------|------|---------|
| Q17 (2-key VARCHAR GROUP BY) | 1,709ms | 13ms | 28,878ms | 2,221x |
| Q34 (URL GROUP BY) | 2,782ms | 14ms | 20,062ms | 1,433x |
| Q16 (UserID GROUP BY) | 384ms | 13ms | 2,791ms | 215x |
| Q13 (SearchPhrase GROUP BY) | 599ms | 18ms | 4,433ms | 246x |
| Q3 (scalar SUM/COUNT/AVG) | 21ms | 12ms | 649ms | 54x |
| Q5 (COUNT DISTINCT) | 353ms | 16ms | 2,495ms | 156x |

Current status: 5/43 within 2x ClickHouse. Target: 43/43.

## Root Cause (Data-Backed)

OpenSearch's native aggregation uses **Global Ordinals** — a pre-computed cross-segment ordinal mapping that enables single-pass aggregation with ordinal-indexed arrays. DQE uses per-segment ordinals with HashMap-based merging.

### What Native DSL Does (13ms)

1. **OrdinalMap** (Lucene API): Maps segment-local ordinals → global ordinals. Built once per DirectoryReader, cached.
2. **Global ordinal array**: `counts[globalOrd]++` — no HashMap, no hashing, no object allocation. O(1) per doc.
3. **No merge phase**: All segments write to the same global array.
4. **Sequential scan**: `advanceExact(doc) + ordValue()` — 2-4 CPU cycles per doc.

Key source files:
- `GlobalOrdinalsStringTermsAggregator.java` — collection with global ordinals
- `GlobalOrdinalMapping.java` — segment ord → global ord via `mapping.get(segmentOrd)`
- `GlobalOrdinalsBuilder.java` — builds OrdinalMap via `OrdinalMap.build()`

### What DQE Does (28,878ms)

1. **Per-segment ordinals**: Each segment has its own ordinal space. Builds `HashMap<SegmentGroupKey, AccumulatorGroup>` per segment.
2. **Ordinal → string resolution**: In `finish()`, resolves ordinals to `BytesRefKey` / `MergedGroupKey` — millions of object allocations.
3. **HashMap merge**: Cross-segment merge via `HashMap.put()` with hash + equals on resolved string keys.
4. **Per-doc overhead**: ~50ns per doc (HashMap.get, SegmentGroupKey allocation, computeHash) vs ~2ns for array indexing.

## Solution: Adopt Global Ordinals in DQE

Use Lucene's `OrdinalMap` API in DQE's FusedGroupByAggregate to:
1. Build global ordinal space at query start (one-time cost, ~50-200ms)
2. Use global-ordinal-indexed arrays instead of HashMaps
3. Eliminate cross-segment merge for VARCHAR GROUP BY keys
4. Resolve ordinals to strings only at output (once per unique group)

### Architecture

```
Query arrives at shard
  │
  ├── Build OrdinalMap for VARCHAR key fields (cached per DirectoryReader)
  │     └── OrdinalMap.build(null, segmentOrdinals[], COMPACT)
  │
  ├── Allocate accumulator array indexed by global ordinal
  │     └── long[] counts = new long[globalOrdCount]
  │     └── or AccumulatorGroup[] groups = new AccumulatorGroup[globalOrdCount]
  │
  ├── Collector scans all segments
  │     └── For each doc:
  │           segmentOrd = dv.nextOrd()
  │           globalOrd = ordinalMap.getGlobalOrds(segIdx).get(segmentOrd)
  │           counts[globalOrd]++  // or groups[globalOrd].accumulate()
  │
  ├── Top-N selection from accumulator array
  │
  └── Resolve: globalOrd → string value (one lookupOrd per output row)
```

### Multi-Key GROUP BY (Q17: UserID + SearchPhrase)

For composite keys where one is VARCHAR (ordinal) and one is numeric:
- Use global ordinals for the VARCHAR key
- Combine (numericKey, globalOrd) as composite key in a flat hash map
- Still eliminates string hashing — uses long pairs instead

For two VARCHAR keys:
- Build global ordinals for each VARCHAR field
- Composite key = (globalOrd1, globalOrd2) — two longs, no strings

### Memory Budget

- OrdinalMap: ~1-2 MB (PackedInts compressed, 4 shards × 20-27 segments)
- Global ordinal array for SearchPhrase (~600K unique): 600K × 8 bytes = 4.8MB
- Global ordinal array for URL (~18M unique): 18M × 8 bytes = 144MB (tight but feasible on 32GB heap)
- For very high cardinality (>10M ordinals): fall back to hash-based approach

## Implementation Plan (Ralph Loop Tasks)

### Task 1: Spike — OrdinalMap in DQE

**Goal**: Prove OrdinalMap works in DQE context. Measure build cost and memory.

- Build OrdinalMap from SortedSetDocValues across all segments
- Verify global ordinals are correct (compare resolved strings)
- Measure: build time, memory, lookup speed
- Prototype single-VARCHAR COUNT(*) with global ordinals on Q34

**Success**: Q34 drops from 20s to <5.6s (within 2x CH)

### Task 2: Single-VARCHAR Global Ordinals

**Goal**: Replace HashMap-based multi-segment path with global ordinal arrays.

Queries: Q13 (4.4s→<1.2s), Q16 (2.8s→<0.8s), Q34 (20s→<5.6s), Q35 (20s→<5.7s)

- Modify `executeSingleVarcharCountStar` multi-segment path
- Modify `executeSingleVarcharGeneric` multi-segment path
- Build OrdinalMap, allocate global array, scan with global ordinals
- For high-cardinality (>10M global ordinals): use RemapGlobalOrds strategy (hash-based) like native DSL

**Expected**: ~100-500ms per query (matching native DSL cold start)

### Task 3: Multi-Key Global Ordinals

**Goal**: Replace HashMap-based NKeyVarchar path with global ordinals + flat map.

Queries: Q15 (6.6s→<1.2s), Q17 (26.8s→<3.4s), Q18 (30.4s→<2.0s), Q31 (2.0s→<0.7s), Q32 (2.4s→<1.1s), Q36 (2.0s→<0.6s)

- Modify `executeNKeyVarcharPath` multi-segment path
- For each VARCHAR key, build global ordinals
- Composite key = (numericKey..., globalOrd...) — all longs, no strings
- Use open-addressing flat hash map (like existing FlatTwoKeyMap) with long-pair keys
- Resolve ordinals to strings only for output rows

### Task 4: Numeric-Only Optimization

**Goal**: Improve numeric GROUP BY and scalar aggregation.

Queries: Q3 (0.6s→<0.04s), Q4 (1.9s→<0.05s), Q5 (2.5s→<0.7s), Q6 (4.3s→<1.2s), Q9 (3.3s→<0.9s), Q10 (4.9s→<1.0s), Q30 (0.4s→<0.06s)

- Spike: profile numeric paths (executeSingleKeyNumericFlat, FusedScanAggregate)
- For scalar aggs (Q3/Q4): vectorize single-column scan
- For COUNT(DISTINCT) (Q5/Q6): parallelize HashSet building
- For numeric GROUP BY (Q9/Q10): use array-indexed approach when key range is bounded

### Task 5: WHERE-Filtered Queries

**Goal**: Optimize queries with selective WHERE clauses.

Queries: Q8 (0.2s→<0.02s), Q11 (0.4s→<0.3s), Q12 (2.4s→<0.3s), Q14 (5.9s→<1.6s), Q21-Q24, Q28, Q37-Q43

- Spike: measure how many docs actually match WHERE for each query
- Adopt global ordinals for the GROUP BY after WHERE filtering
- Skip segments with no matching docs (Lucene already does this via Weight/Scorer)
- Reduce per-segment overhead (LeafCollector creation, DocValues opening)

### Task 6: Expression GROUP BY

**Goal**: Optimize GROUP BY with expressions.

Queries: Q19 (51s→<6.1s), Q29 (26s→<19.2s), Q40 (2.9s→<0.15s)

- Q19: EXTRACT(minute FROM EventTime) — pre-compute per unique timestamp
- Q29: REGEXP_REPLACE — already uses ordinal caching, profile for bottleneck
- Q40: CASE WHEN expression — already uses EvalNode
- Spike: can global ordinals be combined with expression caching?

### Task 7: Fixed Overhead

**Goal**: Sub-10ms queries.

Queries: Q1 (7ms→<2ms), Q20 (9ms→<6ms)

- Q1: COUNT(*) from index metadata (no scan needed)
- Q20: point lookup — optimize term query path
- Reduce DQE HTTP/parse/plan overhead for trivial queries

## Execution Order

```
Task 1 (Spike: OrdinalMap) ──→ Task 2 (Single-VARCHAR) ──→ Task 3 (Multi-Key)
                                                                │
Task 4 (Numeric) ───────────────────────────────────────────────┤
Task 5 (WHERE-Filtered) ───────────────────────────────────────┤
Task 6 (Expression) ──────────────────────────────────────────┤
Task 7 (Fixed Overhead) ──────────────────────────────────────┘
```

Tasks 1→2→3 are sequential (each builds on prior). Tasks 4-7 can interleave after Task 2.

## Success Criteria

- 43/43 queries within 2x ClickHouse on 100M ClickBench
- 32/43 correctness on 1M (no regression)
- 0 OOM, 0 circuit breaker, 0 crashes
- Each commit includes "x/43 within 2x" progress metric

## Risks

| Risk | Mitigation |
|------|-----------|
| OrdinalMap build cost too high (>1s) | Cache per DirectoryReader (same as native DSL). Lazy build. |
| Global ordinal array too large (URL: 18M × 8B = 144MB) | Fall back to hash-based RemapGlobalOrds for >10M ordinals |
| Multi-key composite requires new flat map | Extend existing FlatTwoKeyMap to use global ordinals |
| Expression keys can't use ordinals | Ordinal caching already exists for expressions; combine with global ordinals |
| Native DSL warmup includes JIT; fair comparison? | Measure DQE after warmup too. Target is vs CH, not vs DSL. |
