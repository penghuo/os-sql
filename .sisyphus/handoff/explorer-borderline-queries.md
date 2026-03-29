# Borderline Queries & REGEXP_REPLACE / Parallelization Findings

## 1. Query Text (from `benchmarks/clickbench/queries/queries_trino.sql`)

| Query | Line | SQL |
|-------|------|-----|
| Q02 | 3 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| Q15 | 16 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| Q28 | 29 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;` |
| Q30 | 31 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;` |
| Q31 | 32 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;` |
| Q37 | 38 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= DATE '2013-07-01' AND EventDate <= DATE '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;` |

### Query Characteristics
- **Q02**: Simple full-table scan with 3 aggregates (SUM, COUNT, AVG) — no GROUP BY. Borderline at 2.2x.
- **Q15**: GROUP BY UserID (high cardinality ~17M) with ORDER BY + LIMIT. Needs parallelization.
- **Q28**: REGEXP_REPLACE as GROUP BY key — regex Pattern caching is critical.
- **Q30/Q31**: Multi-key GROUP BY with VARCHAR keys (SearchPhrase filter). Borderline.
- **Q37**: GROUP BY URL (high cardinality VARCHAR) with multiple WHERE filters + OFFSET.

## 2. REGEXP_REPLACE / Regex Pattern Handling

### Implementation: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/StringFunctions.java:385-475`

**Pattern IS cached.** Three-tier optimization:

1. **Cross-batch caching** (line 387-390): `cachedPattern` array persists across `processNextBatch()` calls. Pattern string is compared; if same, compiled Pattern is reused.
   ```java
   final String[] cachedPatternStr = {null};
   final java.util.regex.Pattern[] cachedPattern = {null};
   ```

2. **Ultra-fast path** (line 437-460): For anchored patterns (`^...$`) with simple group reference replacement (`\1` → `$1`), uses `matcher.group(N)` directly instead of `replaceAll()`. This is the Q28/Q29 path.

3. **Constant-replacement fast path** (line 462+): For anchored patterns with no `$N` references, pre-resolves the replacement string and uses `matcher.matches()` + constant slice.

4. **Matcher reuse** (line 436): Single `Matcher` object is created and `reset()` per row, avoiding allocation.

**Performance note from comments**: For 125K rows with Q29's URL extraction pattern, caching reduces per-batch cost from ~100ms to ~5ms.

### Other regex Pattern.compile locations (all static/cached):
- `TransportTrinoSqlAction.java:960-963` — `AGG_TYPE_PATTERN` (static final, cached)
- `FusedGroupByAggregate.java:82-111` — 4 static final patterns for parsing expressions
- `LikeBlockExpression.java:27-31` — `compiledPattern` (compiled once in constructor)
- `TransportShardExecuteAction.java:1871-1872` — `SHARD_AGG_TYPE_PATTERN` (static final)
- `TransportShardExecuteAction.java:1114` — **NOT cached** (inline `Pattern.compile` in method body)
- `PlanOptimizer.java:54` — static final
- `LocalExecutionPlanner.java:62` — static final

**One concern**: `TransportShardExecuteAction.java:1114` compiles a pattern inline per call — but this is in a coordinator path, not per-row, so impact is minimal.

## 3. FusedGroupByAggregate — `executeSingleKeyNumericFlat` Parallelization

### Method signature: `FusedGroupByAggregate.java:4350`
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys, int numAggs,
    boolean[] isCountStar, int[] accType, int sortAggIndex, boolean sortAscending, long topN)
```

### Parallelization Status: **YES — parallelized via ForkJoinPool**

**Configuration** (lines 115-133):
```java
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
private static final int THREADS_PER_SHARD = Integer.getInteger("dqe.threads.per.shard", 4);
private static final ForkJoinPool PARALLEL_POOL = new ForkJoinPool(
    Math.max(4, Runtime.getRuntime().availableProcessors()));
```

**Parallel execution** (lines 4393-4425):
- Condition: `canParallelize = !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1`
- Uses largest-first greedy segment assignment across workers
- Each worker gets its own `FlatSingleKeyMap`
- Workers run via `CompletableFuture.supplyAsync(..., PARALLEL_POOL)`
- Results merged after all futures complete

**This method handles Q15-type queries** (single numeric key GROUP BY with COUNT/SUM/AVG).

### Other parallel paths in FusedGroupByAggregate:
- **Multi-key VARCHAR path** (line 1633-1743): Also parallelized with same ForkJoinPool, but has a guard: `canParallelize` is disabled when ordinal counts exceed 500K per segment (to avoid BytesRefKey copy overhead).
- The shared `PARALLEL_POOL` is exposed via `getParallelPool()` (line 131-133) for use by other fast paths.

## JSON Summary

```json
{
  "patterns": [
    {
      "name": "REGEXP_REPLACE Pattern caching",
      "description": "Regex patterns are compiled once and cached across batches via closure-captured array. Three optimization tiers: cache reuse, group extraction, constant replacement.",
      "files": ["dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/StringFunctions.java:385-475"],
      "example": "final java.util.regex.Pattern[] cachedPattern = {null}; // reused across batches"
    },
    {
      "name": "Intra-shard parallelism via ForkJoinPool",
      "description": "FusedGroupByAggregate uses a shared ForkJoinPool with configurable threads-per-shard (default 4). Segments are distributed largest-first across workers.",
      "files": ["dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:115-133", "dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:4393-4425"],
      "example": "CompletableFuture.supplyAsync(() -> { ... }, PARALLEL_POOL);"
    }
  ],
  "files": [
    {
      "path": "benchmarks/clickbench/queries/queries_trino.sql",
      "purpose": "ClickBench query definitions (43 queries, 0-indexed)",
      "relevance": "Source of Q02/Q15/Q28/Q30/Q31/Q37 SQL text"
    },
    {
      "path": "dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/StringFunctions.java",
      "purpose": "Scalar function implementations including REGEXP_REPLACE with Pattern caching",
      "relevance": "Q28 regex performance — Pattern is cached, Matcher is reused"
    },
    {
      "path": "dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java",
      "purpose": "Fused scan+groupby+aggregate operator with intra-shard parallelism",
      "relevance": "Q15 parallelization — executeSingleKeyNumericFlat IS parallelized via ForkJoinPool"
    }
  ],
  "implementations": [
    {
      "name": "executeSingleKeyNumericFlat",
      "location": "dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:4350-4500+",
      "description": "Flat-map GROUP BY for single numeric key with COUNT/SUM/AVG. Parallelized across Lucene segments via ForkJoinPool.",
      "dependencies": ["FlatSingleKeyMap", "ForkJoinPool", "CompletableFuture"]
    },
    {
      "name": "regexpReplace",
      "location": "dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/StringFunctions.java:385",
      "description": "REGEXP_REPLACE with 3-tier optimization: cross-batch Pattern cache, ultra-fast group extraction for anchored patterns, constant-replacement fast path.",
      "dependencies": ["java.util.regex.Pattern", "java.util.regex.Matcher", "io.airlift.slice.Slices"]
    }
  ],
  "recommendations": [
    "Q28 regex Pattern IS cached — no further caching needed. The 3-tier optimization (cache + group extraction + constant replacement) is already comprehensive.",
    "Q15 executeSingleKeyNumericFlat IS parallelized via ForkJoinPool — if still slow, check dqe.threads.per.shard setting (default 4) and segment count.",
    "TransportShardExecuteAction.java:1114 has an uncached inline Pattern.compile — low impact (coordinator path) but could be made static final for consistency.",
    "Q30/Q31 (multi-key with VARCHAR) parallelism has a 500K ordinal guard — if borderline, this guard may be disabling parallelism for those queries."
  ]
}
```
