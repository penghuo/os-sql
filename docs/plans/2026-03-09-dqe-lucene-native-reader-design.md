# DQE Lucene Native Reader Design

**Date**: 2026-03-09
**Status**: Proposed

## Goal

Achieve <5x ClickHouse timing on all 43 ClickBench queries (1M dataset) by replacing the scroll API + `_source` JSON data access path with direct Lucene doc values reading and native Lucene query execution on each shard.

## Problem

The current `OpenSearchPageSource` reads data via:

```
SearchRequest (scroll API) → SearchHits → _source JSON → Map<String, Object> → PageBuilder → Trino Blocks
```

This path has three bottlenecks:
1. **Scroll API overhead**: Network round-trips even for shard-local reads, scroll context management, serialization/deserialization of SearchResponse objects.
2. **`_source` JSON parsing**: Every document's `_source` field is parsed from stored JSON into a `Map<String, Object>`. For 1M rows this means 1M JSON parse operations.
3. **Row-to-columnar conversion**: `PageBuilder` converts row-oriented Maps to columnar Trino Blocks, creating intermediate objects.

## Architecture

Replace `OpenSearchPageSource` with `LucenePageSource` that reads directly from Lucene:

```
IndexShard.acquireSearcher("dqe")
  → IndexSearcher.search(LuceneQuery, DqeCollector)
    → Per segment: LeafReaderContext → DocValues → BlockBuilder → Trino Pages
```

### Dependency Chain

```
IndicesService (Guice-injectable)
  → indexService(Index)         // resolve by index name
    → getShard(shardId)         // get local shard
      → acquireSearcher("dqe") // Engine.Searcher extends IndexSearcher
        → search(Query, Collector)
```

`Engine.Searcher` extends `IndexSearcher`, so standard Lucene `search(Query, Collector)` is available.

### Data Flow

```
SQL WHERE clause              → Lucene Query (TermQuery, RangeQuery, BooleanQuery, etc.)
Lucene Query + IndexSearcher  → DqeCollector called per matching document
DqeCollector per segment      → reads doc values via LeafReaderContext
Doc values arrays             → Trino BlockBuilder (columnar, no intermediate Map)
BlockBuilder                  → Page returned to operator pipeline
```

### Key Design Decision: No DSL

The DQE reads from Lucene directly. No DSL query translation. No DSL aggregation. SQL predicates compile to Lucene Query objects. Aggregation, sort, eval, projection all remain in DQE's Java operator pipeline.

## Components

### 1. LucenePageSource

New leaf operator replacing `OpenSearchPageSource`. Implements `Operator` interface.

```java
public class LucenePageSource implements Operator {
    private final Engine.Searcher searcher;   // from IndexShard.acquireSearcher()
    private final Query query;                 // compiled from SQL WHERE
    private final List<ColumnHandle> columns;  // columns to read
    private final int batchSize;               // rows per Page

    // Internal state
    private DocValuesCollector collector;       // collects matching docs
    private int currentOffset;                 // position in collected docs
}
```

**Lifecycle**:
1. Constructor: acquires `Engine.Searcher`, compiles Lucene `Query`
2. `processNextBatch()`: on first call, runs `searcher.search(query, collector)` to collect all matching doc IDs. Then iterates collected docs in batches, reading doc values and building Pages.
3. `close()`: releases `Engine.Searcher`

**Alternative batching strategy**: Instead of collecting all doc IDs upfront, use `IndexSearcher.search()` with a custom `Collector` that yields Pages incrementally per segment. This avoids buffering all doc IDs in memory.

### 2. DocValuesReader

Per-segment reader that maps Lucene doc value types to Trino BlockBuilders.

| OpenSearch field type | Lucene DocValues type | Read method | Trino Block |
|----------------------|----------------------|-------------|-------------|
| long, integer, short, byte | SortedNumericDocValues | `advanceExact(doc)` → `nextValue()` | LongArrayBlock |
| float | SortedNumericDocValues | `nextValue()` → `Float.intBitsToFloat()` | LongArrayBlock (cast to int) |
| double | SortedNumericDocValues | `nextValue()` → `Double.longBitsToDouble()` | LongArrayBlock |
| keyword | SortedSetDocValues | `advanceExact(doc)` → `nextOrd()` → `lookupOrd()` | VariableWidthBlock |
| date | SortedNumericDocValues | `nextValue()` (epoch millis) | LongArrayBlock (TimestampType) |
| boolean | SortedNumericDocValues | `nextValue()` (0 or 1) | ByteArrayBlock |
| ip | SortedSetDocValues | `lookupOrd()` → InetAddress decode | VariableWidthBlock |
| text (no doc values) | N/A | Fall back to stored fields | VariableWidthBlock |

**Null handling**: `advanceExact(doc)` returns `false` if the document has no value for that field. Write null to the BlockBuilder in that case.

### 3. LuceneQueryCompiler

Compiles SQL WHERE predicates (Trino AST `Expression` nodes) to Lucene `Query` objects.

| SQL predicate | Lucene Query |
|--------------|-------------|
| `col = value` | `TermQuery` (keyword) or `LongPoint.newExactQuery` (numeric) |
| `col > value` | `LongPoint.newRangeQuery` or `DoublePoint.newRangeQuery` |
| `col >= value` | Same with inclusive bound |
| `col BETWEEN a AND b` | `LongPoint.newRangeQuery(col, a, b)` |
| `col IN (a, b, c)` | `TermInSetQuery` (keyword) or `LongPoint.newSetQuery` (numeric) |
| `A AND B` | `BooleanQuery` with `MUST` clauses |
| `A OR B` | `BooleanQuery` with `SHOULD` clauses |
| `NOT A` | `BooleanQuery` with `MUST_NOT` clause |
| `col LIKE 'pattern%'` | `PrefixQuery` or `WildcardQuery` |
| No WHERE clause | `MatchAllDocsQuery` |

**Unsupported predicates**: If a predicate can't be compiled to a Lucene Query (e.g., complex expressions, function calls), fall back to `MatchAllDocsQuery` and let the DQE `FilterOperator` handle it in the operator pipeline. This preserves correctness while allowing incremental Lucene query support.

### 4. TransportShardExecuteAction Changes

Inject `IndicesService` to resolve shard → `Engine.Searcher`:

```java
@Inject
public TransportShardExecuteAction(
    TransportService transportService,
    ActionFilters actionFilters,
    NodeClient client,
    ClusterService clusterService,
    IndicesService indicesService)  // NEW
```

New scan factory method:

```java
private Function<TableScanNode, Operator> buildLuceneScanFactory(
    ShardExecuteRequest req, Map<String, Type> typeMap, int batchSize) {

    return node -> {
        // 1. Resolve shard
        Index index = clusterService.state().metadata()
            .index(node.getIndexName()).getIndex();
        IndexShard shard = indicesService.indexService(index)
            .getShard(req.getShardId());

        // 2. Compile Lucene query from DSL filter or SQL predicate
        Query query = compileQuery(node);

        // 3. Build column handles
        List<ColumnHandle> columns = buildColumnHandles(node, typeMap);

        // 4. Create LucenePageSource
        return new LucenePageSource(shard, query, columns, batchSize);
    };
}
```

### 5. Plugin Wiring

`SQLPlugin.java` must expose `IndicesService` to the DQE module. `IndicesService` is available via `createComponents()` — it's passed as a parameter or accessible from the `Node`.

### 6. COUNT(*) Fast Path

For queries with no columns (pure `COUNT(*)`), `LucenePageSource` uses `IndexSearcher.count(query)` instead of collecting documents. Returns a single Page with the count. This matches the existing fast path in `OpenSearchPageSource`.

## Files to Create/Modify

### New Files
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/LucenePageSource.java` — leaf operator
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/DocValuesReader.java` — per-segment doc values reader
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/LuceneQueryCompiler.java` — SQL predicate → Lucene Query

### Modified Files
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java` — inject IndicesService, new scan factory
- `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java` — register IndicesService dependency
- `dqe/src/main/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizer.java` — pass SQL predicate AST (not DSL string) through plan for Lucene query compilation

### Retained (Fallback)
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/OpenSearchPageSource.java` — kept for fields without doc values

## Verification Protocol

```bash
# 1. Build plugin
./gradlew :opensearch-sql-plugin:assemble -x test -x integTest

# 2. Reload into running cluster
./run/run_all.sh reload-plugin

# 3. Correctness: must remain 43/43 PASS
./run/run_all.sh correctness

# 4. Perf-lite: establish baseline, compare to ClickHouse
./run/run_all.sh perf-lite
# Target: all 43 queries complete within 5x ClickHouse time

# 5. Full performance (optional): 100M dataset
./run/run_all.sh performance
```

## Success Criteria

- 43/43 correctness PASS (no regression)
- All 43 queries on 1M dataset complete within 5x ClickHouse timing
- No DSL aggregation used — all aggregation in DQE operators
- `OpenSearchPageSource` scroll path fully replaced by `LucenePageSource` for indexed fields

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Text fields without doc values | Fall back to stored fields for those columns |
| Multi-valued fields | SortedNumericDocValues/SortedSetDocValues handle multi-value; take first value |
| Nested objects | Not needed for ClickBench (flat schema); defer |
| Memory usage from collecting all doc IDs | Use segment-streaming approach instead of upfront collection |
| Field mapping changes at runtime | Read mapping at query time from IndexShard.mapperService() |
