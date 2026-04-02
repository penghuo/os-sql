# COUNT(DISTINCT) Execution Patterns in DQE

## 1. TransportShardExecuteAction.java — Dispatch Logic

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

### Detection Methods

```java
// Line 3575
private boolean isBareSingleNumericColumnScan(DqePlanNode plan)
// Returns true for BigintType, IntegerType, SmallintType, TinyintType, TimestampType

// Line 3597
private boolean isBareSingleVarcharColumnScan(DqePlanNode plan)
// Returns true for VarcharType
```

### Dispatch (lines 264-280 in executePlan)

```
L269: if (scanFactory == null && isBareSingleNumericColumnScan(plan))
L270:   → executeDistinctValuesScanWithRawSet(plan, req)  // attaches LongOpenHashSet
L277: if (scanFactory == null && isBareSingleVarcharColumnScan(plan))
L278:   → executeDistinctValuesScanVarcharWithRawSet(plan, req)  // attaches LongOpenHashSet of hashes
```

### Execution Methods (scalar path)

```java
// Line 3661 — numeric scalar COUNT(DISTINCT)
private ShardExecuteResponse executeDistinctValuesScanWithRawSet(DqePlanNode plan, ShardExecuteRequest req)
// Calls FusedScanAggregate.collectDistinctValuesRaw(), attaches raw LongOpenHashSet via resp.setScalarDistinctSet()

// Line 3691 — varchar scalar COUNT(DISTINCT)
private ShardExecuteResponse executeDistinctValuesScanVarcharWithRawSet(DqePlanNode plan, ShardExecuteRequest req)
// Calls FusedScanAggregate.collectDistinctVarcharHashes(), attaches LongOpenHashSet of hashes
```

### GROUP BY + COUNT(DISTINCT) Methods

```java
// Line 1011 — 2-key numeric: GROUP BY key0, COUNT(DISTINCT key1)
private ShardExecuteResponse executeCountDistinctWithHashSets(AggregationNode, req, keyName0, keyName1, type0, type1)

// Line 1398 — N-key numeric (3+): GROUP BY (key0..keyN-2), COUNT(DISTINCT keyN-1)
private ShardExecuteResponse executeNKeyCountDistinctWithHashSets(AggregationNode, req, keys, keyTypes, shardTopN)

// Line 1875 — mixed-type N-key: some VARCHAR GROUP BY keys + numeric dedup key
private ShardExecuteResponse executeMixedTypeCountDistinctWithHashSets(...)

// Line 2755 — VARCHAR key0 + numeric key1 (Q14 pattern)
private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(AggregationNode, req, keyName0, keyName1, type1, shardTopN)
```

## 2. FusedScanAggregate.java — Distinct Value Collection

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`

```java
// Line 1577 — returns Pages (legacy path)
public static List<Page> executeDistinctValues(String columnName, IndexShard shard, Query query)

// Line 1674 — returns raw LongOpenHashSet (current fast path for numeric)
public static LongOpenHashSet collectDistinctValuesRaw(String columnName, IndexShard shard, Query query)
// Pre-sizes HashSet based on totalDocs/4, up to 8M. Two-phase: parallel columnar load → parallel hash insert.

// Line 1905 — returns LongOpenHashSet of FNV-1a hashes (varchar path)
public static LongOpenHashSet collectDistinctVarcharHashes(String columnName, IndexShard shard, Query query)
// Uses ordinal-based iteration for MatchAll, FixedBitSet for filtered queries.
```

## 3. TransportTrinoSqlAction.java — Coordinator Merge

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java`

### Detection Methods

```java
// Line 1864 — checks SINGLE mode, COUNT(DISTINCT col), numeric type
private static boolean isScalarCountDistinctLong(AggregationNode aggNode, Map<String, Type> columnTypeMap)

// Line 1900 — checks SINGLE mode, COUNT(DISTINCT col), VarcharType
private static boolean isScalarCountDistinctVarchar(AggregationNode aggNode, Map<String, Type> columnTypeMap)
```

### Coordinator Dispatch (lines 650-665)

```
L655: isScalarCountDistinctLong  → mergeCountDistinctValuesViaRawSets(successShardResults, shardPages)
L659: isScalarCountDistinctVarchar → mergeCountDistinctVarcharViaRawSets(successShardResults, shardPages)
L664: isShardDedupCountDistinct  → GROUP BY merge path (checks distinctSets/varcharDistinctSets attachments)
```

### Merge Methods

```java
// Line 1990 — Page-based merge (fallback)
private static List<Page> mergeCountDistinctValues(List<List<Page>> shardPages)

// Line 1931 — Page-based varchar merge (fallback)
private static List<Page> mergeCountDistinctVarcharValues(List<List<Page>> shardPages)

// Line 2026 — Raw LongOpenHashSet merge for numeric (primary path)
private static List<Page> mergeCountDistinctValuesViaRawSets(ShardExecuteResponse[] shardResults, List<List<Page>> shardPages)
// Strategy: find largest set, merge others into temp set, parallel contains() checks against largest

// Line 2115 — Raw LongOpenHashSet merge for varchar hashes (primary path)
private static List<Page> mergeCountDistinctVarcharViaRawSets(ShardExecuteResponse[] shardResults, List<List<Page>> shardPages)
// Strategy: union all hash sets into largest, return count
```

## 4. ShardExecuteResponse.java — Response Fields

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/ShardExecuteResponse.java` (93 lines total)

```java
public class ShardExecuteResponse extends ActionResponse {
  private final List<Page> pages;                    // L28
  private final List<Type> columnTypes;              // L31

  // All transient — only for local (same-node) shard execution, NOT serialized
  @Setter transient Map<Long, LongOpenHashSet> distinctSets;           // L39-40 (GROUP BY numeric key → dedup set)
  @Setter transient Map<String, LongOpenHashSet> varcharDistinctSets;  // L48-49 (GROUP BY varchar key → dedup set)
  @Setter transient LongOpenHashSet scalarDistinctSet;                 // L58 (scalar COUNT(DISTINCT numeric))
  @Setter transient Set<String> scalarDistinctStrings;                 // L65 (scalar COUNT(DISTINCT varchar))

  // Serialization: only pages + columnTypes go over the wire (lines 73-90)
  // writeTo/readFrom use PageSerializer — transient fields are NULL for remote shards
}
```

**Key insight:** All distinct-set fields are `transient` — they only work for local (same-node) shards. Remote shards fall back to Page-based merge.

## 5. Existing HLL Usage

**File:** `opensearch/src/main/java/org/opensearch/sql/opensearch/functions/DistinctCountApproxAggFunction.java`

This is a **Calcite-based** UDF (implements `UserDefinedAggFunction`), NOT used by DQE:
- Uses `HyperLogLogPlusPlus` with `DEFAULT_PRECISION` (14)
- Uses `MurmurHash3.hash128()` for hashing
- Supports: byte[], String, Number types
- **Not integrated with DQE's shard-level execution or coordinator merge**

**Also:** `opensearch/.../MetricAggregationBuilder.java:192` uses `CardinalityAggregationBuilder` for OpenSearch DSL-based distinct count (also not DQE).

**No HLL usage exists in the DQE engine (`dqe/` directory).**

## Summary: Architecture for HLL Replacement

Current flow (exact dedup):
```
Shard: collectDistinctValuesRaw() → LongOpenHashSet (millions of entries)
       ↓ attached as transient field on ShardExecuteResponse
Coordinator: mergeCountDistinctValuesViaRawSets() → union all sets → count
```

For HLL replacement, touch points:
1. **FusedScanAggregate** — new `collectDistinctValuesHLL()` returning HLL sketch instead of LongOpenHashSet
2. **ShardExecuteResponse** — new field for HLL sketch bytes (can be serialized, unlike transient sets)
3. **TransportShardExecuteAction** — dispatch to HLL path based on cardinality threshold or config
4. **TransportTrinoSqlAction** — new `mergeCountDistinctValuesViaHLL()` that merges HLL sketches
