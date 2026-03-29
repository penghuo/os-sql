# DQE GROUP BY High-Cardinality Memory Analysis

## 1. TransportShardExecuteAction — Dispatch Logic (executePlan method)

The dispatch logic in `executePlan()` follows a priority chain of ~15 fast paths before falling back to the generic operator pipeline:

1. **Scalar COUNT(*)** → `executeScalarCountStar()` (Lucene `searcher.count()`)
2. **Sorted scan** → `executeSortedScan()` (Lucene TopFieldCollector)
3. **Fused scalar agg** → `FusedScanAggregate.execute()` (no GROUP BY)
4. **Bare single-column scan** → pre-dedup for COUNT(DISTINCT)
5. **Fused eval-aggregate** → SUM(col+constant) algebraic shortcut
6. **COUNT(DISTINCT) dedup with HashSets** → per-group LongOpenHashSet (2-key, N-key, VARCHAR variants)
7. **Mixed dedup with HashSets** → Q10 pattern (SUM/COUNT + COUNT(DISTINCT))
8. **Expression-key GROUP BY** → ordinal-cached expression evaluation
9. **Fused GROUP BY** → `FusedGroupByAggregate.execute()` (ordinal-based)
10. **Fused GROUP BY + Sort + Limit** → `executeFusedGroupByAggregateWithTopN()` with top-N pre-filter
11. **Fused GROUP BY + Limit (no sort)** → early termination after N groups
12. **HAVING filter path** → fused agg + filter + sort
13. **Generic operator pipeline** → `LocalExecutionPlanner` builds ScanOperator → HashAggregationOperator

**For Q18 (3-key GROUP BY with LIMIT):** The dispatch hits path #10 or #11 depending on ORDER BY presence. With `ORDER BY COUNT(*) DESC LIMIT 10`, it uses `executeFusedGroupByAggregateWithTopN()`.

## 2. FusedGroupByAggregate — Memory Allocation Pattern

### Key Hash Map Structures (NO memory limits, NO spill-to-disk):

**FlatSingleKeyMap / FlatTwoKeyMap / FlatThreeKeyMap:**
- Open-addressing hash maps with parallel `long[]` arrays
- Initial capacity: 4096-8192, load factor: 0.65-0.7
- **MAX_CAPACITY: 16,000,000 groups** — hard-coded limit, throws RuntimeException on exceed
- Resize: doubles capacity, copies all data via `System.arraycopy`
- Accumulator data stored in contiguous `long[]` at `slot * slotsPerGroup`

**SingleKeyHashMap / TwoKeyHashMap:**
- Same open-addressing pattern but with `AccumulatorGroup[]` per slot
- MAX_CAPACITY: 16,000,000 groups
- Each AccumulatorGroup = `MergeableAccumulator[]` (object allocation per group)

**HashMap-based paths (N-key, VARCHAR):**
- `HashMap<SegmentGroupKey, AccumulatorGroup>` or `HashMap<MergedGroupKey, AccumulatorGroup>`
- **NO size limit** — grows unbounded until OOM
- Each entry: SegmentGroupKey (long[] + boolean[] + hash) + AccumulatorGroup + MergeableAccumulator[]

### Memory per group (approximate):
- **Flat path** (COUNT/SUM/AVG only): ~24 bytes/group (2 longs for keys + 1 long for count)
- **Object path** (MIN/MAX/DISTINCT): ~200+ bytes/group (key objects + AccumulatorGroup + accumulators)
- **VARCHAR keys**: additional BytesRefKey (byte[] copy) per group

### For Q18 specifically (UserID × minute × SearchPhrase):
- 3 keys → dispatches to `executeNKeyVarcharPath` (SearchPhrase is VARCHAR)
- If single-segment + 3 keys + flat-compatible aggs → `executeThreeKeyFlat()` with `FlatThreeKeyMap`
- If multi-segment → `executeNKeyVarcharParallelDocRange()` with `HashMap<MergedGroupKey, AccumulatorGroup>`
- **No memory limit on the HashMap path** — millions of groups will consume heap until OOM

### Early termination for LIMIT without ORDER BY:
- `findOrInsertCapped()` on FlatTwoKeyMap/FlatThreeKeyMap stops accepting new groups after `cap` groups
- Two-phase capped path: fill groups, then count-only with key-set filter
- **Only works when there's no ORDER BY** — with ORDER BY, all groups must be materialized

### Bucket partitioning for very high cardinality:
- When estimated docs > MAX_CAPACITY (16M), partitions key space into buckets
- Each bucket scans all docs but only aggregates groups whose hash falls in that bucket
- Sequential or parallel bucket execution via ForkJoinPool

## 3. TransportTrinoSqlAction — Coordinator Merge Logic

### ResultMerger merge paths (selected by type checking):

1. **Fast numeric merge** (`canUseFastNumericMerge`): All group keys are long-representable, all aggs are COUNT/SUM/AVG
   - Open-addressing hash map with `long[][]` keys + `long[][]`/`double[][]` agg values
   - Initial capacity: 1024, load factor: 0.7
   - **NO size limit** — doubles on resize indefinitely
   
2. **Fast varchar merge** (`canUseFastVarcharMerge`): Single VARCHAR key + COUNT/SUM
   - `HashMap<Slice, long[]>` or open-addressing with XxHash64
   
3. **Fast mixed merge** (`canUseFastMixedMerge`): Mixed numeric+varchar keys
   - `HashMap<MixedMergeKey, long[]>` with pre-computed hash

4. **Fused merge+sort** (`mergeAggregationAndSort`): Combines merge + top-N selection
   - Builds full hash map first, then uses bounded heap for top-K selection
   - **Critical optimization**: only builds output Page for top-K rows

5. **Capped merge** (`mergeAggregationCapped`): LIMIT without ORDER BY
   - Caps tracked groups to `maxGroups` — new groups beyond cap are ignored
   - Existing groups continue accumulating from all shards

### For Q18 with ORDER BY:
- Coordinator receives partial results from all shards
- Uses `mergeAggregationAndSort()` → builds full hash map of ALL groups across shards, then top-N heap
- **The coordinator hash map has NO size limit** — all shard groups are materialized

## Critical Finding: No Memory Limits or Spill-to-Disk

**Neither shard-level nor coordinator-level aggregation has:**
- Memory budget tracking
- Spill-to-disk mechanism
- Circuit breaker integration
- Backpressure on group count (except the 16M hard cap on flat maps)

**The only protections are:**
1. `MAX_CAPACITY = 16,000,000` on FlatSingleKeyMap/FlatTwoKeyMap/FlatThreeKeyMap (throws RuntimeException)
2. `findOrInsertCapped()` for LIMIT-without-ORDER-BY queries (stops accepting new groups)
3. `System.gc()` hint after large aggregations (>10K groups)

**For Q18 with ORDER BY COUNT(*) DESC LIMIT 10:**
- Each shard must materialize ALL unique (UserID, minute, SearchPhrase) groups
- Coordinator must merge ALL groups from all shards into a single hash map
- Only after full materialization does top-N selection occur
- With millions of groups × ~200 bytes/group = potential multi-GB allocation per shard
