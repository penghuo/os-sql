# COUNT(DISTINCT) Query Dispatch Analysis

## Queries (1-indexed from queries_trino.sql)

| Query | Line | SQL |
|-------|------|-----|
| Q04 | 5 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| Q05 | 6 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| Q08 | 9 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;` |
| Q09 | 10 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;` |
| Q11 | 12 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;` |
| Q13 | 14 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;` |

## Dispatch Paths

### Q04: Global COUNT(DISTINCT UserID) — numeric column
- **Path**: `isBareSingleNumericColumnScan` → `executeDistinctValuesScanWithRawSet`
- **Location**: TransportShardExecuteAction.java:249-252
- **Mechanism**: PlanFragmenter strips the AggregationNode for SINGLE aggregation, leaving a bare `TableScanNode` with one numeric column. Shard collects distinct values into a raw `LongOpenHashSet` and attaches it to the response. Coordinator unions raw sets across shards.
- **Guard**: `isBareSingleNumericColumnScan` at line 1633 — checks plan is `TableScanNode` with exactly 1 column of numeric/timestamp type.

### Q05: Global COUNT(DISTINCT SearchPhrase) — varchar column
- **Path**: `isBareSingleVarcharColumnScan` → `executeDistinctValuesScanVarcharWithRawSet`
- **Location**: TransportShardExecuteAction.java:255-258
- **Mechanism**: Same PlanFragmenter stripping. Shard uses ordinal-based dedup via `FixedBitSet` for fast ordinal collection, then attaches the raw string set to the response.
- **Guard**: `isBareSingleVarcharColumnScan` at line 1655 — checks plan is `TableScanNode` with exactly 1 VARCHAR column.

### Q08: GROUP BY RegionID + COUNT(DISTINCT UserID) — 2 numeric keys, single COUNT(*)
- **Path**: Dedup fast path → `executeCountDistinctWithHashSets`
- **Location**: TransportShardExecuteAction.java:276-306
- **Mechanism**: Detected as `AggregationNode` with `getGroupByKeys().size() == 2` and single `COUNT(*)` aggregate. Both keys (RegionID=numeric, UserID=numeric) are non-VARCHAR. Uses per-group `LongOpenHashSet` accumulators. Parallel segment scanning with `ForkJoinPool` for multi-segment shards.
- **Guard**: Line 280 `aggDedupNode.getGroupByKeys().size() == 2`, line 285-286 single `COUNT(*)`, lines 298-301 both keys non-VARCHAR.
- **Method**: `executeCountDistinctWithHashSets` at line 788. Signature:
  ```java
  private ShardExecuteResponse executeCountDistinctWithHashSets(
      AggregationNode aggNode, ShardExecuteRequest req,
      String keyName0, String keyName1, Type type0, Type type1)
  ```

### Q09: GROUP BY RegionID + mixed aggregates (SUM, COUNT, AVG, COUNT(DISTINCT UserID))
- **Path**: Falls through dedup fast path — **AVG is NOT decomposable as SUM/COUNT**
- **Location**: The `isMixedDedup` check at line 289-291 requires ALL aggregates match `(sum|count)\(.*\)`. Q09 has `AVG(ResolutionWidth)` which does NOT match.
- **Fallthrough**: Goes to generic `executeFusedGroupByAggregate` path at line 357-363, or the sort+limit variant at line 370+.
- **Note**: Q09 has 5 aggregates: `SUM(AdvEngineID)`, `COUNT(*)`, `AVG(ResolutionWidth)`, `COUNT(DISTINCT UserID)`. The AVG blocks the mixed dedup path.

### Q11: GROUP BY MobilePhone, MobilePhoneModel + COUNT(DISTINCT UserID) — 3 GROUP BY keys
- **Path**: Falls through dedup fast path entirely — **3 keys, not 2**
- **Location**: The guard at line 280 `aggDedupNode.getGroupByKeys().size() == 2` rejects Q11 because it has 3 GROUP BY keys (MobilePhone, MobilePhoneModel, UserID after dedup expansion).
- **Fallthrough**: Goes to generic `executeFusedGroupByAggregate` at line 357-363 (if `canFuse` accepts it) or the sort+limit variant.
- **No 3-key variant exists**: There is NO specialized 3-key dedup dispatch anywhere in TransportShardExecuteAction.java. The `size() == 2` check is the only dedup entry point.

### Q13: GROUP BY SearchPhrase + COUNT(DISTINCT UserID) — varchar key0 + numeric key1
- **Path**: Dedup fast path → `executeVarcharCountDistinctWithHashSets`
- **Location**: TransportShardExecuteAction.java:308-314
- **Mechanism**: Detected as single `COUNT(*)` with 2 keys where key0 is VARCHAR (SearchPhrase) and key1 is numeric (UserID). Uses ordinal-indexed `LongOpenHashSet[]` arrays per segment, then merges into cross-segment `HashMap<String, LongOpenHashSet>`.
- **Guard**: Line 309 `t0 instanceof VarcharType` and line 310 `t1 not instanceof VarcharType`.
- **Method**: `executeVarcharCountDistinctWithHashSets` at line 1337. Signature:
  ```java
  private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(
      AggregationNode aggNode, ShardExecuteRequest req,
      String varcharKeyName, String numericKeyName, Type numericKeyType)
  ```

## Method Algorithms

### executeCountDistinctWithHashSets (line 788)
- **Purpose**: 2 numeric keys, single COUNT(*) — Q08 pattern
- **Algorithm**:
  1. Acquires shard searcher, compiles Lucene query
  2. For multi-segment: parallel scan using `ForkJoinPool` — each segment builds `Map<Long, LongOpenHashSet>` (key0 → set of key1 values)
  3. For single segment: direct sequential scan
  4. MatchAll path: sequential DocValues iteration (nextDoc loop)
  5. Filtered path: Weight/Scorer-based iteration with advanceExact
  6. Merges per-segment maps by unioning LongOpenHashSets per key0
  7. Outputs compact page (~450 rows) + attached HashSets for coordinator union

### executeVarcharCountDistinctWithHashSets (line 1337)
- **Purpose**: VARCHAR key0 + numeric key1, single COUNT(*) — Q13/Q14 pattern
- **Algorithm**:
  1. Acquires shard searcher, compiles Lucene query
  2. Per segment: allocates `LongOpenHashSet[]` indexed by ordinal (up to 10M ordinals)
  3. MatchAll path: unwraps singleton SortedDocValues for sequential scan; falls back to SortedSetDocValues.nextOrd() for multi-valued
  4. Filtered path (Q13 has WHERE): **collect-then-sequential-scan** — first collects matching doc IDs into sorted array via Scorer, then iterates DocValues sequentially matching against collected IDs (converts random advanceExact into sequential nextDoc)
  5. Per segment: calls `mergeOrdSetsIntoMap` (line ~1530) to resolve ordinals to String keys and merge into cross-segment `HashMap<String, LongOpenHashSet>`
  6. Outputs page with (varcharKey, placeholder=0, distinctCount) + attached `varcharDistinctSets` for coordinator

### executeMixedDedupWithHashSets (line 1076)
- **Purpose**: 2 numeric keys, mixed SUM/COUNT aggregates — Q10 pattern
- **Algorithm**:
  1. Parses aggregate functions to identify COUNT(*), COUNT(col), SUM(col)
  2. Uses open-addressing hash map: `grpKeys[]`, `grpSets[]` (LongOpenHashSet per group for key1), `grpAccs[][]` (per-group accumulator array), `grpOcc[]`
  3. MatchAll path: sequential DocValues iteration with parallel DV advancement for all aggregate columns
  4. Filtered path: Weight/Scorer-based iteration with advanceExact per DV
  5. Dynamic hash map resizing at 70% load factor
  6. Per doc: hash-probes key0, adds key1 to group's HashSet, accumulates SUM/COUNT per aggregate
  7. Outputs compact page (key0, distinctCount, acc0, acc1, ...) + attached HashSets

## Summary: Q11 3-Key Gap

**Q11 is NOT handled by any specialized dedup path.** The only dedup entry point requires exactly 2 GROUP BY keys (line 280). Q11 has 3 keys (MobilePhone, MobilePhoneModel, UserID) and falls through to the generic `executeFusedGroupByAggregate` path, which materializes all (key0, key1, key2) tuples — no HashSet-based dedup optimization.

This is the primary optimization opportunity for Q11 (9.5x slowdown factor).

Similarly, Q09 falls through because AVG is not decomposable as SUM/COUNT in the mixed dedup check.
