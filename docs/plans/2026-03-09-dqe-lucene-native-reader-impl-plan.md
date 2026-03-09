# DQE Lucene Native Reader Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace scroll API + `_source` JSON parsing with direct Lucene doc values reading on each shard to achieve <5x ClickHouse timing on ClickBench.

**Architecture:** `LucenePageSource` replaces `OpenSearchPageSource` as the leaf operator. It acquires an `Engine.Searcher` from `IndexShard`, runs `IndexSearcher.search(query, collector)`, and reads doc values directly from `LeafReaderContext` per segment. SQL WHERE predicates compile to Lucene `Query` objects.

**Tech Stack:** Lucene 10.4.0 (bundled with OpenSearch 3.6.0), Trino SPI 442 (Block/Page), OpenSearch `IndicesService`/`IndexShard`/`Engine.Searcher`.

**ClickBench hits_1m field types:** 49 short, 19 integer, 6 long, 4 date, 25 keyword, 2 text (no doc values).

---

### Task 1: DocValuesReader — Numeric Fields

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/DocValuesReader.java`
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/shard/source/DocValuesReaderTest.java`

**Context:** `DocValuesReader` is a per-segment utility that reads Lucene doc values for a batch of doc IDs and writes directly into Trino `BlockBuilder` arrays. Each `LeafReaderContext` provides access to `SortedNumericDocValues` (for numeric/date fields) and `SortedSetDocValues` (for keyword fields) via `leafReader.getSortedNumericDocValues(fieldName)`.

**Lucene API reference (Lucene 10.4.0):**
- `LeafReader.getSortedNumericDocValues(String field)` → `SortedNumericDocValues`
- `SortedNumericDocValues extends DocValuesIterator extends DocIdSetIterator`
- `advanceExact(int docId)` → returns `boolean` (false = no value = null)
- `nextValue()` → returns `long`
- `docValueCount()` → number of values for current doc

**Step 1: Write failing test for reading BIGINT doc values**

Test creates an in-memory Lucene index with a `NumericDocValuesField`, opens a `LeafReaderContext`, and calls `DocValuesReader.readColumn()` to fill a `BlockBuilder`. Uses Lucene's `MemoryIndex` or `RandomIndexWriter` for test data.

```java
// DocValuesReaderTest.java
@Test
@DisplayName("reads SortedNumericDocValues into LongArrayBlock")
void readsBigintDocValues() throws Exception {
    // Create in-memory index with 3 docs, field "age" = [10, 20, 30]
    Directory dir = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    IndexWriter writer = new IndexWriter(dir, config);
    for (long val : new long[]{10, 20, 30}) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("age", val));
        writer.addDocument(doc);
    }
    writer.commit();
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReaderContext leaf = reader.leaves().get(0);

    ColumnHandle col = new ColumnHandle("age", BigintType.BIGINT);
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    int[] docIds = {0, 1, 2};

    DocValuesReader.readColumn(leaf, col, docIds, 3, builder);

    Block block = builder.build();
    assertEquals(3, block.getPositionCount());
    assertEquals(10L, BigintType.BIGINT.getLong(block, 0));
    assertEquals(20L, BigintType.BIGINT.getLong(block, 1));
    assertEquals(30L, BigintType.BIGINT.getLong(block, 2));
    reader.close();
    dir.close();
}
```

**Step 2: Run test to verify it fails**

```bash
./gradlew :dqe:test --tests "*DocValuesReaderTest*" -x spotlessCheck
```
Expected: FAIL — class `DocValuesReader` does not exist.

**Step 3: Implement DocValuesReader for numeric types**

```java
// DocValuesReader.java
public final class DocValuesReader {
    private DocValuesReader() {}

    public static void readColumn(
            LeafReaderContext leaf, ColumnHandle column,
            int[] docIds, int count, BlockBuilder builder) throws IOException {
        Type type = column.type();
        String field = column.name();

        if (type instanceof BigintType || type instanceof IntegerType
                || type instanceof SmallintType || type instanceof TinyintType) {
            readNumeric(leaf, field, docIds, count, builder);
        } else if (type instanceof DoubleType) {
            readDouble(leaf, field, docIds, count, builder);
        } else if (type instanceof TimestampType) {
            readTimestamp(leaf, field, docIds, count, builder);
        } else if (type instanceof BooleanType) {
            readBoolean(leaf, field, docIds, count, builder);
        } else if (type instanceof VarcharType) {
            readKeyword(leaf, field, docIds, count, builder);
        } else {
            // Fallback: write nulls
            for (int i = 0; i < count; i++) {
                builder.appendNull();
            }
        }
    }

    private static void readNumeric(
            LeafReaderContext leaf, String field,
            int[] docIds, int count, BlockBuilder builder) throws IOException {
        SortedNumericDocValues dv = leaf.reader().getSortedNumericDocValues(field);
        for (int i = 0; i < count; i++) {
            if (dv != null && dv.advanceExact(docIds[i])) {
                builder.writeLong(dv.nextValue());
            } else {
                builder.appendNull();
            }
        }
    }

    // readDouble, readTimestamp, readBoolean similar — see Step 5
}
```

**Step 4: Run test to verify it passes**

```bash
./gradlew :dqe:test --tests "*DocValuesReaderTest*" -x spotlessCheck
```
Expected: PASS

**Step 5: Add tests + implementation for DoubleType, TimestampType, BooleanType**

DoubleType: `SortedNumericDocValues` stores doubles as `Double.doubleToRawLongBits()`. Read with `nextValue()` then `Double.longBitsToDouble()`. Write to builder via `type.writeDouble(builder, doubleVal)`.

TimestampType: OpenSearch stores dates as epoch millis in `SortedNumericDocValues`. Convert to Trino microseconds: `epochMillis * 1000L`. Write via `builder.writeLong(epochMicros)`.

BooleanType: `SortedNumericDocValues` with 0=false, 1=true. Write via `BooleanType.BOOLEAN.writeBoolean(builder, val == 1)`.

**Step 6: Commit**

```bash
git add dqe/src/main/java/org/opensearch/sql/dqe/shard/source/DocValuesReader.java \
        dqe/src/test/java/org/opensearch/sql/dqe/shard/source/DocValuesReaderTest.java
git commit -m "feat(dqe): DocValuesReader for numeric types"
```

---

### Task 2: DocValuesReader — Keyword (VARCHAR) Fields

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/DocValuesReader.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/shard/source/DocValuesReaderTest.java`

**Context:** Keyword fields use `SortedSetDocValues` in Lucene. For single-valued keyword fields (common in ClickBench), there's one ordinal per doc. Read via `advanceExact(doc)` → `nextOrd()` → `lookupOrd(ord)` → `BytesRef` → write as Trino VARCHAR.

**Step 1: Write failing test for keyword doc values**

```java
@Test
@DisplayName("reads SortedSetDocValues into VariableWidthBlock (VARCHAR)")
void readsKeywordDocValues() throws Exception {
    Directory dir = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    for (String val : new String[]{"hello", "world", "test"}) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("name", new BytesRef(val)));
        writer.addDocument(doc);
    }
    writer.commit();
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReaderContext leaf = reader.leaves().get(0);

    ColumnHandle col = new ColumnHandle("name", VarcharType.VARCHAR);
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 3);
    int[] docIds = {0, 1, 2};

    DocValuesReader.readColumn(leaf, col, docIds, 3, builder);

    Block block = builder.build();
    assertEquals(3, block.getPositionCount());
    // Verify string values (order may differ due to Lucene doc ID assignment)
    reader.close();
    dir.close();
}
```

**Step 2: Run test, verify failure**

**Step 3: Implement readKeyword()**

```java
private static void readKeyword(
        LeafReaderContext leaf, String field,
        int[] docIds, int count, BlockBuilder builder) throws IOException {
    SortedSetDocValues dv = leaf.reader().getSortedSetDocValues(field);
    for (int i = 0; i < count; i++) {
        if (dv != null && dv.advanceExact(docIds[i])) {
            long ord = dv.nextOrd();
            BytesRef bytes = dv.lookupOrd(ord);
            VarcharType.VARCHAR.writeSlice(builder,
                Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
        } else {
            builder.appendNull();
        }
    }
}
```

**Step 4: Run test, verify pass**

**Step 5: Add test for null handling (doc with no value for field)**

**Step 6: Commit**

```bash
git commit -m "feat(dqe): DocValuesReader for keyword/VARCHAR fields"
```

---

### Task 3: LucenePageSource — Core Implementation

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/LucenePageSource.java`
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/shard/source/LucenePageSourceTest.java`

**Context:** `LucenePageSource` implements `Operator`. It takes an `IndexShard`, a Lucene `Query`, column handles, and batch size. On `processNextBatch()`, it acquires a searcher, collects matching doc IDs per segment, reads doc values via `DocValuesReader`, and returns Pages. Uses a segment-streaming approach: processes one segment at a time, yields batches from the current segment before moving to the next.

**Step 1: Write failing test for basic scan (no filter)**

Use Lucene's `RandomIndexWriter` to create an in-memory index. Create `LucenePageSource` with `MatchAllDocsQuery` and verify it returns all docs as Pages.

Note: Since `LucenePageSource` needs `IndexShard.acquireSearcher()`, the test should accept an `Engine.Searcher` directly (or use a test constructor). `Engine.Searcher` extends `IndexSearcher`, so we can create one from a `DirectoryReader`.

```java
@Test
@DisplayName("scans all documents with MatchAllDocsQuery")
void scansAllDocs() throws Exception {
    // Create in-memory index with 5 docs
    Directory dir = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    for (int i = 0; i < 5; i++) {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("id", i));
        writer.addDocument(doc);
    }
    writer.commit();
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    List<ColumnHandle> columns = List.of(new ColumnHandle("id", BigintType.BIGINT));

    LucenePageSource source = new LucenePageSource(
        reader, new MatchAllDocsQuery(), columns, 3); // batch=3

    // Should return 2 pages: [0,1,2] and [3,4]
    Page p1 = source.processNextBatch();
    assertNotNull(p1);
    assertEquals(3, p1.getPositionCount());

    Page p2 = source.processNextBatch();
    assertNotNull(p2);
    assertEquals(2, p2.getPositionCount());

    assertNull(source.processNextBatch()); // exhausted
    source.close();
    reader.close();
    dir.close();
}
```

**Step 2: Run test, verify failure**

**Step 3: Implement LucenePageSource**

The implementation collects all matching doc IDs per segment upfront (via a simple `Collector`), then iterates them in batches, reading doc values for each batch.

```java
public class LucenePageSource implements Operator {
    private final IndexSearcher searcher;
    private final Closeable searcherCloseable; // for Engine.Searcher.close()
    private final List<ColumnHandle> columns;
    private final int batchSize;

    // Collected doc IDs grouped by segment
    private List<SegmentDocs> segments;
    private int currentSegment;
    private int currentOffset;
    private boolean initialized;

    // Test constructor: accepts IndexReader directly
    public LucenePageSource(IndexReader reader, Query query,
            List<ColumnHandle> columns, int batchSize) { ... }

    // Production constructor: accepts IndexShard
    public LucenePageSource(IndexShard shard, Query query,
            List<ColumnHandle> columns, int batchSize) { ... }

    @Override
    public Page processNextBatch() {
        if (!initialized) {
            collectDocIds();
            initialized = true;
        }
        // Return next batch from current segment, advance to next segment when exhausted
        ...
    }

    private void collectDocIds() {
        // Use IndexSearcher.search() with a collector that records
        // (segmentIndex, docId) for each matching document
    }

    @Override
    public void close() {
        if (searcherCloseable != null) searcherCloseable.close();
    }

    // Inner class: holds doc IDs for one segment + LeafReaderContext
    private static class SegmentDocs {
        final LeafReaderContext leaf;
        final int[] docIds;
        final int count;
    }
}
```

**Step 4: Run test, verify pass**

**Step 5: Add test for COUNT(*) fast path (empty columns)**

```java
@Test
@DisplayName("COUNT(*) fast path: no columns, returns position count only")
void countStarFastPath() throws Exception {
    // ... create index with 100 docs, no columns requested
    LucenePageSource source = new LucenePageSource(
        reader, new MatchAllDocsQuery(), List.of(), 1000);
    Page page = source.processNextBatch();
    assertEquals(100, page.getPositionCount());
    assertEquals(0, page.getChannelCount());
}
```

Implementation: Use `IndexSearcher.count(query)` when `columns.isEmpty()`.

**Step 6: Commit**

```bash
git commit -m "feat(dqe): LucenePageSource with segment-streaming doc values reader"
```

---

### Task 4: LuceneQueryCompiler — SQL Predicates to Lucene Queries

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/LuceneQueryCompiler.java`
- Test: `dqe/src/test/java/org/opensearch/sql/dqe/shard/source/LuceneQueryCompilerTest.java`

**Context:** Compiles Trino `Expression` AST nodes (from SQL WHERE clause) into Lucene `Query` objects. Also parses existing DSL filter JSON strings (from `PlanOptimizer` pushdown) into Lucene queries. This allows incremental migration — existing DSL pushdown still works while new predicate types are added.

**Step 1: Write failing tests for basic predicates**

```java
@Test
void compilesEqualityToTermQuery() {
    // "CounterID = 62" → LongPoint.newExactQuery("CounterID", 62)
    Map<String, String> fieldTypes = Map.of("CounterID", "integer");
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    Query q = compiler.compileFromDsl("{\"term\":{\"CounterID\":62}}");
    assertInstanceOf(... , q); // verify it's a point query
}

@Test
void compilesNullPredicateToMatchAll() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(Map.of());
    Query q = compiler.compile(null);
    assertInstanceOf(MatchAllDocsQuery.class, q);
}
```

**Step 2: Run test, verify failure**

**Step 3: Implement LuceneQueryCompiler**

Start with:
- `compile(null)` → `MatchAllDocsQuery`
- `compileFromDsl(String dslJson)` → parse `{"term":{"field":value}}` → `TermQuery` or `LongPoint.newExactQuery`
- `compileExpression(Expression expr, Map<String, String> fieldTypes)` → handle `ComparisonExpression`, `LogicalExpression`, `InPredicate`, `BetweenPredicate`

**Step 4: Run tests, verify pass**

**Step 5: Add tests for range predicates, IN, AND/OR, BETWEEN**

```java
@Test
void compilesRangeToPointRange() {
    // "EventDate >= DATE '2013-07-01'" → LongPoint.newRangeQuery
}

@Test
void compilesInToTermInSet() {
    // "AdvEngineID IN (2, 3, 4)" → LongPoint.newSetQuery
}

@Test
void compilesAndToBooleanMust() {
    // "A = 1 AND B = 2" → BooleanQuery with MUST clauses
}
```

**Step 6: Commit**

```bash
git commit -m "feat(dqe): LuceneQueryCompiler for SQL predicates"
```

---

### Task 5: TransportShardExecuteAction — IndicesService Injection

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteActionTest.java`

**Context:** The production constructor needs `IndicesService` injected to resolve `IndexShard` from index name + shard ID. The test constructor remains unchanged (already accepts a scan factory). The `buildScanFactory` method is replaced with `buildLuceneScanFactory` that creates `LucenePageSource` instead of `OpenSearchPageSource`.

**Step 1: Add IndicesService to production constructor**

```java
@Inject
public TransportShardExecuteAction(
    TransportService transportService,
    ActionFilters actionFilters,
    NodeClient client,
    ClusterService clusterService,
    IndicesService indicesService) {  // NEW
```

**Step 2: Implement buildLuceneScanFactory**

```java
private Function<TableScanNode, Operator> buildLuceneScanFactory(
    ShardExecuteRequest req, Map<String, Type> typeMap, int batchSize) {
    return node -> {
        // 1. Resolve IndexShard
        IndexMetadata indexMeta = clusterService.state().metadata()
            .index(node.getIndexName());
        IndexShard shard = indicesService.indexService(indexMeta.getIndex())
            .getShard(req.getShardId());

        // 2. Compile Lucene query
        LuceneQueryCompiler queryCompiler = new LuceneQueryCompiler(
            resolveFieldTypes(node.getIndexName()));
        Query query = (node.getDslFilter() != null)
            ? queryCompiler.compileFromDsl(node.getDslFilter())
            : new MatchAllDocsQuery();

        // 3. Build column handles
        List<ColumnHandle> columns = node.getColumns().stream()
            .map(col -> new ColumnHandle(col,
                typeMap.getOrDefault(col, BigintType.BIGINT)))
            .collect(Collectors.toList());

        // 4. Create LucenePageSource
        return new LucenePageSource(shard, query, columns, batchSize);
    };
}
```

**Step 3: Replace buildScanFactory call with buildLuceneScanFactory in doExecute**

Change line in `doExecute()`:
```java
// Before:
effectiveScanFactory = buildScanFactory(req, effectiveColumnTypeMap, batchSize);
// After:
effectiveScanFactory = buildLuceneScanFactory(req, effectiveColumnTypeMap, batchSize);
```

**Step 4: Update existing tests if needed**

The test constructor path is unchanged (uses injected scan factory), so existing tests should pass. Add a new test verifying the production path creates a `LucenePageSource`.

**Step 5: Commit**

```bash
git commit -m "feat(dqe): wire IndicesService into TransportShardExecuteAction"
```

---

### Task 6: SQLPlugin Wiring — Register IndicesService

**Files:**
- Modify: `plugin/src/main/java/org/opensearch/sql/plugin/SQLPlugin.java`

**Context:** `IndicesService` is a core OpenSearch component available via Guice injection. Since `TransportShardExecuteAction` uses `@Inject`, Guice automatically resolves `IndicesService` from the OpenSearch node's injector. No explicit registration is needed — `IndicesService` is already bound by OpenSearch's core module.

**Step 1: Verify Guice auto-resolution**

Check that `IndicesService` is already in the Guice injector by reviewing OpenSearch's `Node.java` or `NodeModule.java`. If `TransportShardExecuteAction` declares `IndicesService` as a constructor parameter with `@Inject`, Guice resolves it automatically.

**Step 2: Build the plugin to verify compilation**

```bash
./gradlew :opensearch-sql-plugin:assemble -x test -x integTest
```

If compilation succeeds but Guice injection fails at runtime, add explicit binding in `SQLPlugin.createComponents()` or `getGuiceModules()`.

**Step 3: Commit**

```bash
git commit -m "feat(dqe): plugin wiring for IndicesService injection"
```

---

### Task 7: Integration — Build, Deploy, Correctness

**Files:** None new — this is verification.

**Step 1: Build plugin**

```bash
./gradlew :opensearch-sql-plugin:assemble -x test -x integTest
```

**Step 2: Reload plugin into running cluster**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
./run/run_all.sh reload-plugin
```

**Step 3: Smoke test a single query**

```bash
./run/run_all.sh correctness --query 1
```

If it fails, check OpenSearch logs:
```bash
sudo grep "exception\|Error" /opt/opensearch/logs/clickbench.log | tail -30
```

**Step 4: Debug and fix issues**

Common issues to watch for:
- `NullPointerException` in `DocValuesReader`: field has no doc values (text fields)
- `ClassCastException` in block types: type mapping mismatch
- Guice injection failure: `IndicesService` not bound

**Step 5: Run full correctness suite**

```bash
./run/run_all.sh correctness
```
Target: 43/43 PASS (no regression from previous correctness work).

**Step 6: Commit any fixes**

```bash
git commit -m "fix(dqe): correctness fixes for LucenePageSource"
```

---

### Task 8: Perf-Lite Baseline — Measure and Analyze

**Files:**
- Results will be in `benchmarks/clickbench/results/perf-lite/`

**Step 1: Run perf-lite**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
./run/run_all.sh perf-lite
```

This runs all 43 queries × 3 tries on 1M dataset for both ClickHouse and OpenSearch, then builds a summary.

**Step 2: Analyze results**

```bash
cat results/perf-lite/summary.json | python3 -m json.tool
```

For each query, compute the ratio: `os_best / ch_best`. Identify:
- Queries already <5x ClickHouse (done)
- Queries 5-10x ClickHouse (investigate)
- Queries >10x or timeout (need optimization)

**Step 3: Categorize bottlenecks**

For slow queries, determine if the bottleneck is:
- **I/O** (large scan, many rows)
- **Aggregation** (high-cardinality GROUP BY)
- **Sort** (large result set sorted)
- **Predicate not pushed to Lucene** (FilterOperator scanning all rows)

This analysis drives the next optimization phase.

**Step 4: Commit results**

```bash
git add benchmarks/clickbench/results/perf-lite/
git commit -m "perf(dqe): perf-lite baseline with LucenePageSource"
```

---

### Task 9: Predicate Pushdown Enhancement (if needed based on Task 8)

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizer.java`
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/planner/plan/TableScanNode.java`
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/LuceneQueryCompiler.java`

**Context:** If Task 8 shows queries are slow because predicates aren't pushed to Lucene (FilterOperator scanning all rows when a Lucene query could filter at the index level), enhance the predicate pushdown.

**Option A: Expand PlanOptimizer** to push range/IN/compound predicates as DSL, then LuceneQueryCompiler parses them.

**Option B: Change TableScanNode** to carry the raw predicate string (not DSL). Add a `filterPredicateString` field. LuceneQueryCompiler compiles Trino Expression → Lucene Query directly.

Choose based on perf-lite analysis. If most slow queries have simple range predicates, Option A is simpler. If they have complex compound predicates, Option B is better.

---

## Build Commands Reference

```bash
# Compile check (fast)
./gradlew :dqe:compileJava

# Unit tests for DQE module
./gradlew :dqe:test -x spotlessCheck

# Specific test class
./gradlew :dqe:test --tests "*DocValuesReaderTest*" -x spotlessCheck

# Assemble plugin (no tests)
./gradlew :opensearch-sql-plugin:assemble -x test -x integTest

# Reload plugin + correctness
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
./run/run_all.sh reload-plugin
./run/run_all.sh correctness
./run/run_all.sh perf-lite
```
