# LongOpenHashSet & COUNT(DISTINCT) Coordinator Merge Analysis

## 1. LongOpenHashSet Class

**Location**: `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java`

### Data Structure
- **Backing array**: `long[] keys` — open-addressing hash table with linear probing
- **Sentinel value**: `EMPTY = Long.MIN_VALUE` — marks unoccupied slots
- **Load factor**: `0.65f` — resize when `size > capacity * 0.65`
- **Capacity**: Always power-of-two (enables `slot & mask` instead of modulo)
- **Hash function**: Murmur3 finalizer for distribution
- **Special value tracking**:
  - `boolean hasZero` — tracks value `0` (since `0` is default for `long[]` but EMPTY is `Long.MIN_VALUE`, this is actually for the literal value 0)
  - `boolean hasSentinel` — tracks `Long.MIN_VALUE` as an actual data value (since it collides with EMPTY marker)
- **Size tracking**: `int size` field incremented on each new insertion — `size()` is O(1)

### Iteration
The class exposes `keys()` which returns the raw `long[] keys` array. **Iteration must scan ALL slots** (including empty ones) and skip slots where `keys[i] == EMPTY`. There is no compact iteration structure (no linked list, no index array of occupied slots).

**Memory layout for 200K distinct values**:
- Capacity = next power-of-two above `200000 / 0.65 ≈ 307,693` → **524,288 slots**
- Array size = 524,288 × 8 bytes = **~4 MB per shard set**
- Only ~200K of 524K slots are filled (~38% occupancy at that point, well under 65% threshold)
- **Iteration waste**: ~62% of slots scanned are empty

### Key Methods
| Method | Purpose |
|--------|---------|
| `add(long)` | Insert with linear probing, handles 0 and sentinel specially |
| `size()` | O(1) count of distinct values |
| `keys()` | Returns raw backing array (caller must skip EMPTY slots) |
| `addAll(LongOpenHashSet)` | Merges another set — iterates ALL slots of other set |
| `hasZeroValue()` / `hasSentinelValue()` | Check special value flags |
| `emptyMarker()` | Returns `Long.MIN_VALUE` |

## 2. Coordinator Merge Code

**Location**: `TransportTrinoSqlAction.java:1957-2010`
**Method**: `mergeCountDistinctValuesViaRawSets(ShardExecuteResponse[], List<List<Page>>)`

### Merge Algorithm
1. **Check all shards have raw sets** (lines 1961-1968) — falls back to Page-based merge if any shard lacks attachment
2. **Find largest shard set** (lines 1972-1981) — picks the set with max `size()` as the base
3. **Merge all other sets into largest** (lines 1983-1999):
   ```java
   // For each non-largest shard:
   if (other.hasZeroValue()) largest.add(0L);
   if (other.hasSentinelValue()) largest.add(EMPTY);
   long[] otherKeys = other.keys();
   for (int j = 0; j < otherKeys.length; j++) {   // ← scans ALL slots
     if (otherKeys[j] != emptyMarker) {
       largest.add(otherKeys[j]);
     }
   }
   ```
4. **Emit result** (lines 2000-2002) — writes `largest.size()` as a single-row Page

### The Inefficiency
The merge loop at line 1993-1997 iterates **every slot** in `otherKeys[]`, including empty ones. For a set with 200K values, the backing array has ~524K slots, meaning ~324K iterations are wasted per shard checking `otherKeys[j] != emptyMarker`.

With 8 shards (7 non-largest merged), that's `7 × 524,288 = ~3.67M` array accesses, of which ~2.27M are wasted empty-slot checks.

**Note**: The class also has an `addAll()` method (line 148) that does the same thing — iterates all slots. The coordinator merge code manually inlines this logic rather than calling `addAll()`, but the inefficiency is identical.

### Potential Optimization
A compact iteration mechanism (e.g., a secondary `int[] filledSlots` array tracking indices of occupied slots, or a `forEach(LongConsumer)` method that only yields non-empty values) would eliminate the empty-slot scanning overhead. This would reduce merge iteration from O(capacity) to O(size) per shard.

## 3. Q5 End-to-End Flow: `SELECT COUNT(DISTINCT UserID) FROM hits`

### Step 1: Coordinator Plan Fragmentation
- PlanFragmenter strips the `AggregationNode` (COUNT DISTINCT) from the plan
- Leaves a bare `TableScanNode` with single numeric column `UserID`
- `isBareSingleNumericColumnScan()` returns true → dispatches to Branch 4

### Step 2: Shard Execution (`TransportShardExecuteAction.java:1867`)
- `executeDistinctValuesScanWithRawSet()` is called per shard
- Calls `FusedScanAggregate.collectDistinctValuesRaw()` (`FusedScanAggregate.java:1504`)
- Iterates all Lucene segments sequentially via `SortedNumericDocValues`
- Each doc's UserID value is added to a `LongOpenHashSet`
- Response carries both:
  - A 1-row Page with the shard-local count (fallback)
  - The raw `LongOpenHashSet` as a `transient` field (`resp.setScalarDistinctSet(rawSet)`)

### Step 3: Serialization
- `scalarDistinctSet` is marked `transient` in `ShardExecuteResponse.java:58`
- **It is NOT serialized over the wire** — only available for local (same-node) shard execution
- For remote shards, the field is `null` and the fallback Page-based merge is used
- The `writeTo()`/`StreamInput` constructor only serialize Pages + column types

### Step 4: Coordinator Merge (`TransportTrinoSqlAction.java:606`)
- Dispatch condition: `isScalarCountDistinctLong(singleCdAgg, columnTypeMap)` matches
- Calls `mergeCountDistinctValuesViaRawSets(shardResults, shardPages)`
- If all shards are local (all have raw sets):
  - Finds largest set, merges others into it by scanning all backing array slots
  - Returns `largest.size()` as the final count
- If any shard is remote (missing raw set):
  - Falls back to `mergeCountDistinctValues(shardPages)` which extracts values from Pages

### Step 5: Result
- Final merged count is written to a 1-row Page: `BigintType.BIGINT.writeLong(builder, largest.size())`
- Returned to the client as the query result

## Summary of Findings

| Aspect | Detail |
|--------|--------|
| Set class | `LongOpenHashSet` — primitive long open-addressing hash set |
| Sentinel | `Long.MIN_VALUE` for empty slots |
| Load factor | 0.65 → capacity ~1.54× the number of elements |
| Merge inefficiency | Iterates ALL backing array slots (O(capacity)) instead of just filled slots (O(size)) |
| Waste ratio | ~62% of iterations hit empty slots for a typical 200K-element set |
| Serialization | `transient` — raw set only available for local shards, not serialized |
| Optimization opportunity | Add compact iteration (e.g., `forEach(LongConsumer)` or `int[] filledIndices`) to skip empty slots |
