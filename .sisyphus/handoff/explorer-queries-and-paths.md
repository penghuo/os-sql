# DQE Codebase Exploration: Borderline Queries & REGEXP_REPLACE

## Q1: ClickBench Query Texts (0-indexed from queries_trino.sql)

File: `benchmarks/clickbench/queries/queries_trino.sql` (43 queries, Q00–Q42)

| Query | SQL Summary |
|-------|-------------|
| Q02 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits` |
| Q04 | `SELECT COUNT(DISTINCT UserID) FROM hits` |
| Q05 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` |
| Q08 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10` |
| Q09 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10` |
| Q11 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10` |
| Q13 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10` |
| Q15 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` |
| Q16 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10` |
| Q18 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10` |
| Q28 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(...) HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25` |
| Q30 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10` |
| Q31 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10` |
| Q32 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10` |
| Q35 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10` |
| Q36 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10` |
| Q37 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10` |
| Q39 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (...) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND ... GROUP BY ... ORDER BY PageViews DESC OFFSET 1000 LIMIT 10` |

## Q2: REGEXP_REPLACE Evaluation & Pattern Compilation

**Primary implementation**: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/StringFunctions.java:385`

**Pattern is NOT compiled per-row.** Three-tier optimization:

1. **Cross-batch caching** (L388-390): `cachedPattern[]` array persists the compiled `java.util.regex.Pattern` across `processNextBatch()` calls. Pattern string equality check avoids recompilation.

2. **Per-batch fast path** (L402-414): On first row of each batch, checks if pattern is constant (same for all positions). If so, compiles once and reuses `Matcher` object via `matcher.reset(input)` for all rows.

3. **Ultra-fast anchored path** (L435-450): For anchored patterns (`^...$`) with simple group references (like Q28's `\1`), uses `matcher.matches()` + `matcher.group()` directly instead of `replaceAll()`, avoiding StringBuffer allocation.

**Registration**: `dqe/src/main/java/org/opensearch/sql/dqe/function/BuiltinFunctions.java:133-136`

**Q28 ordinal optimization**: `FusedGroupByAggregate.java:306` — For Q28 with REGEXP_REPLACE on Referer, evaluates expression per-ordinal (~16K) instead of per-doc (~921K), a ~58x reduction. See `executeWithExpressionKey()` at line 391.

## Q3: executeSingleKeyNumericFlat Structure

**Location**: `FusedGroupByAggregate.java:4350-4490+`

**Signature** (L4350):
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys, int numAggs,
    boolean[] isCountStar, int[] accType, int sortAggIndex, boolean sortAscending, long topN)
```

**NOT single-threaded.** Has both parallel and sequential paths:

- **Parallel path** (L4400-4449): Activated when `PARALLELISM_MODE != "off"` AND `THREADS_PER_SHARD > 1` AND `leaves.size() > 1`. Uses `CompletableFuture.supplyAsync()` on `PARALLEL_POOL` (ForkJoinPool). Each worker gets its own `FlatSingleKeyMap`, segments assigned via largest-first greedy balancing. Worker maps merged after `CompletableFuture.allOf().join()`.

- **Sequential path** (L4455-4465): Single `FlatSingleKeyMap`, iterates all segments linearly.

**Key data structure**: `FlatSingleKeyMap` — flat `long[]` array (`accData`) with `slotsPerGroup` slots per group. Eliminates ~75K object allocations for Q16-class queries. Supports COUNT (1 slot), SUM (1 slot), AVG (2 slots: sum+count).

**Top-N**: Uses min-heap selection when `sortAggIndex >= 0 && topN < flatMap.size` (L4480+).

## Q4: Parallel Execution Patterns in Codebase

**Shared infrastructure** (FusedGroupByAggregate.java:122-133):
```java
private static final ForkJoinPool PARALLEL_POOL = new ForkJoinPool(
    Runtime.getRuntime().availableProcessors(), ..., asyncMode=true);
private static final int THREADS_PER_SHARD = availableProcessors / numLocalShards;
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
```

**Parallel patterns found**:

| Method | Location | Strategy |
|--------|----------|----------|
| `executeSingleKeyNumericFlat` | L4350 | Segment-partitioned, per-worker FlatSingleKeyMap, greedy load balancing |
| `executeSingleVarcharCountStar` | L1670 | `CompletableFuture[]` + `PARALLEL_POOL`, per-worker `HashMap<BytesRefKey, long[]>` |
| `executeSingleVarcharGeneric` | L2560 | `CompletableFuture[]` + `PARALLEL_POOL`, per-worker `HashMap<BytesRefKey, AccumulatorGroup>` |
| `executeDerivedSingleKeyNumeric` | L3901 | `CompletableFuture[]` + `PARALLEL_POOL`, per-worker FlatSingleKeyMap |
| `executeNKeyVarcharParallelDocRange` | L10241 | Doc-range parallel: collects matching doc IDs per segment, splits across workers with per-worker `Map<MergedGroupKey, AccumulatorGroup>` |

**Common pattern**: All use `CompletableFuture.supplyAsync(() -> { ... }, PARALLEL_POOL)` with per-worker local maps, followed by `CompletableFuture.allOf(futures).join()` and sequential merge.

**Guard condition**: `!"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1`

**executeNKeyVarcharPath** (L8880): Dispatches to specialized paths including 3-key flat path (`FlatThreeKeyMap`) and the parallel doc-range path. Multi-segment parallelism only when all keys are numeric (varchar ordinals are segment-local).
