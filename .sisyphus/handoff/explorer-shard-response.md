# ShardExecuteResponse & Shard-to-Coordinator Data Flow

## 1. ShardExecuteResponse Class

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/ShardExecuteResponse.java`

### Fields
| Field | Type | Serialized? | Purpose |
|-------|------|-------------|---------|
| `pages` | `List<Page>` | Yes (via PageSerializer) | Trino Pages with result data |
| `columnTypes` | `List<Type>` | Yes (type signature strings) | Column types for deserialization |
| `distinctSets` | `transient Map<Long, LongOpenHashSet>` | **No** (in-process only) | Per-group distinct sets for numeric COUNT(DISTINCT) |
| `varcharDistinctSets` | `transient Map<String, LongOpenHashSet>` | **No** (in-process only) | Per-group distinct sets for VARCHAR COUNT(DISTINCT) |
| `scalarDistinctSet` | `transient LongOpenHashSet` | **No** (in-process only) | Raw distinct set for scalar COUNT(DISTINCT numericCol) |
| `scalarDistinctStrings` | `transient Set<String>` | **No** (in-process only) | Raw distinct strings for scalar COUNT(DISTINCT varcharCol) |

### Constructor & Serialization
```java
// In-process constructor (line ~72)
public ShardExecuteResponse(List<Page> pages, List<Type> columnTypes)

// Deserialization constructor (line ~78)
public ShardExecuteResponse(StreamInput in) throws IOException {
    this.pages = PageSerializer.readPages(in);
    int typeCount = in.readVInt();
    this.columnTypes = new ArrayList<>(typeCount);
    for (int i = 0; i < typeCount; i++) {
        columnTypes.add(PageSerializer.resolveType(in.readString()));
    }
}

// Serialization (line ~88)
public void writeTo(StreamOutput out) throws IOException {
    PageSerializer.writePages(out, pages, columnTypes);
    out.writeVInt(columnTypes.size());
    for (Type type : columnTypes) {
        out.writeString(type.getTypeSignature().toString());
    }
}
```

## 2. In-Process vs Serialized Transport

**ALL shard execution is currently in-process (same JVM).** The coordinator calls `shardAction.executeLocal(shardPlan, shardReq)` directly.

**Evidence** (TransportTrinoSqlAction.java:581,597):
```java
shardResults[fragIdx] = shardAction.executeLocal(shardPlan, shardReq);
```

The `executeLocal` method (TransportShardExecuteAction.java:185) bypasses transport serialization entirely:
```java
public ShardExecuteResponse executeLocal(DqePlanNode plan, ShardExecuteRequest req) throws Exception {
    return executePlan(plan, req);
}
```

The `doExecute` method (transport path) deserializes the plan from bytes, but `executeLocal` passes the plan object directly. The transient fields (`distinctSets`, `varcharDistinctSets`, `scalarDistinctSet`, `scalarDistinctStrings`) are **only available via the in-process path** — they would be lost during serialization since they're marked `transient`.

## 3. VARCHAR COUNT(DISTINCT) Path

### `isBareSingleVarcharColumnScan` (TransportShardExecuteAction.java:~3640)
```java
private boolean isBareSingleVarcharColumnScan(DqePlanNode plan) {
    if (!(plan instanceof TableScanNode scanNode)) return false;
    List<String> columns = scanNode.getColumns();
    if (columns.size() != 1) return false;
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(scanNode.getIndexName());
    Type colType = cachedMeta.columnTypeMap().get(columns.get(0));
    return colType instanceof io.trino.spi.type.VarcharType;
}
```

### Execution: `executeDistinctValuesScanVarcharWithRawSet` (line ~3695)
- Uses `FusedScanAggregate.collectDistinctVarcharHashes()` to collect hash-based distinct values
- Returns a `LongOpenHashSet` of hashes (not actual strings)
- Attaches via `resp.setScalarDistinctSet(hashes)`

### FixedBitSet / Ordinal-based dedup
The ordinal-based dedup with FixedBitSet is in `FusedScanAggregate.executeDistinctValuesVarchar()` (line ~3660 calls it), but the **raw set path** (`executeDistinctValuesScanVarcharWithRawSet`) uses hash-based counting instead.

For the grouped VARCHAR COUNT(DISTINCT) path (`executeVarcharCountDistinctWithHashSets`), ordinal-based dedup is used extensively:
- SortedSetDocValues ordinals as group keys
- FixedBitSet-like tracking via ordinal-indexed arrays (`ordSets[]`)
- Global ordinal maps for multi-segment merge

## 4. BigArrays Usage

**BigArrays is NOT used anywhere in the DQE codebase.** The DQE engine uses its own custom data structures:
- `LongOpenHashSet` — custom open-addressing hash set for long values
- Open-addressing hash maps with raw `long[]` arrays for group-by keys
- Trino's `BlockBuilder` / `Block` / `Page` for columnar data

No OpenSearch `BigArrays` dependency exists in the shard execution context.

## 5. Coordinator Merge of scalarDistinctSet

**File:** TransportTrinoSqlAction.java:2026 — `mergeCountDistinctValuesViaRawSets()`

Strategy:
1. Find the largest `LongOpenHashSet` across all shards
2. Merge all non-largest sets into a temporary set
3. Parallel counting: check entries in `others` not present in `largest`
4. Final count = `largest.size() + extraCount`

Key insight: the merge is **read-only** against the largest set to avoid expensive rehashing.

## Summary

| Aspect | Detail |
|--------|--------|
| Transport mode | In-process only (`executeLocal`), no serialization for local shards |
| Serialized fields | `pages` (via PageSerializer), `columnTypes` (type signature strings) |
| Transient attachments | `distinctSets`, `varcharDistinctSets`, `scalarDistinctSet`, `scalarDistinctStrings` |
| BigArrays | Not used — DQE has its own `LongOpenHashSet` and raw arrays |
| VARCHAR dedup | Ordinal-indexed arrays + global ordinal maps for grouped; hash-based for scalar |
