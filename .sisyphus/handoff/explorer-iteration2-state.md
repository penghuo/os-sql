# DQE Source Files — Current State for Iteration 2 Optimizations

## Existing Handoff Files (from iteration 1)
- `explorer-fused-groupby-patterns.md` — parallel execution infrastructure
- `explorer-executeSingleKeyNumericFlat.md` — single-key flat map extraction
- `explorer-singlekey-dispatch.md` — single-key dispatch chain
- `explorer-two-key-flat-code.md` — two-key flat parallelization
- `explorer-canFuseWithEval.md` — eval-fusion methods
- `explorer-count-distinct-dispatch.md` — COUNT(DISTINCT) dispatch logic
- `explorer-count-distinct-queries.md` — COUNT(DISTINCT) query analysis

---

## File 1: FusedGroupByAggregate.java

**Path**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`
**Size**: ~5800 lines

### Parallel Execution Infrastructure (lines 116-133)
```java
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
private static final int THREADS_PER_SHARD = Math.max(1, Runtime.getRuntime().availableProcessors() / Integer.getInteger("dqe.numLocalShards", 4));
private static final ForkJoinPool PARALLEL_POOL = new ForkJoinPool(Runtime.getRuntime().availableProcessors(), ..., true);
public static ForkJoinPool getParallelPool() { return PARALLEL_POOL; }
```

### Key Methods for Hash-Partitioned Parallel Aggregation

| Method | Lines | Purpose | Current Parallelism |
|--------|-------|---------|-------------------|
| `executeSingleKeyNumericFlat` | ~2050-2250 | 1-key flat GROUP BY | ForkJoinPool per-segment workers, mergeFrom() |
| `executeTwoKeyNumericFlat` | ~2700-2950 | 2-key flat GROUP BY | ForkJoinPool per-segment workers via `scanSegmentFlatTwoKey` |
| `executeTwoKeyNumeric` | ~2500-2700 | 2-key with AccumulatorGroup | Hash-partition buckets when numGroups > MAX_CAPACITY (8M) |
| `executeThreeKeyFlat` | ~3800-4200 | 3-key flat GROUP BY | Single-segment only; hash-partition via `numBuckets` param |
| `executeNKeyVarcharParallelDocRange` | ~5100-5400 | N-key parallel doc-range | ForkJoinPool, doc-range splitting per segment |
| `executeWithVarcharKeys` | ~3200-3800 | VARCHAR key dispatch | Delegates to flat paths or N-key path |

### Hash-Partitioned Aggregation Pattern (executeTwoKeyNumeric, line ~2700)
```java
int numBuckets = Math.max(1, (int) Math.ceil((double) totalDocs / FlatTwoKeyMap.MAX_CAPACITY));
// Parallel across buckets:
if (parallelBuckets > 1 && !"off".equals(PARALLELISM_MODE)) {
    CompletableFuture<List<Page>>[] futures = ...;
    for (int b = 0; b < numBuckets; b++) {
        futures[b] = CompletableFuture.supplyAsync(() -> executeTwoKeyNumericFlat(..., bkt, numBuckets), PARALLEL_POOL);
    }
    // merge via mergePartitionedPages()
}
```

### 3-Key Dedup / FlatThreeKeyMap (lines ~3700-3800)
```java
private static final class FlatThreeKeyMap {
    static final int MAX_CAPACITY = 8_000_000;
    long[] keys0, keys1, keys2;
    boolean[] occupied;
    long[] accData;
    static int hash3(long k0, long k1, long k2) { ... }
    int findOrInsert(long k0, long k1, long k2) { ... }
}
```
- Already supports hash-partition via `bucket`/`numBuckets` params in `executeThreeKeyFlat`
- Currently single-segment only; multi-segment falls through to N-key path

### Expression Key / REGEXP_REPLACE Handling
- `canFuseWithExpressionKey()` — line ~270: detects single computed expression key over VARCHAR
- `executeWithExpressionKey()` — line ~340: ordinal-cached expression evaluation
  - Pre-computes expression for each unique ordinal (16K ordinals vs 921K docs = 58x reduction)
  - Uses `ExpressionCompiler` + `BlockExpression.evaluate()` in batches of 4096

### Regex Caching in FusedGroupByAggregate
- Regex patterns are compiled via `ExpressionCompiler` → `BlockExpression` (not raw Pattern)
- The ordinal-caching in `executeWithExpressionKey` IS the regex caching: evaluates REGEXP_REPLACE once per ordinal, not once per doc

---

## File 2: FusedScanAggregate.java

**Path**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`
**Size**: ~900 lines

### Key Methods
| Method | Lines | Purpose |
|--------|-------|---------|
| `canFuseWithEval()` | ~85-130 | Detects SUM(col+k) algebraic shortcut pattern |
| `executeWithEval()` | ~140-310 | Algebraic shortcut: SUM(col+k) = SUM(col) + k*COUNT |
| `canFuse()` | ~60-80 | Detects scalar aggregation (no GROUP BY) |
| `execute()` | ~320-550 | Main scalar aggregation execution |
| `tryFlatArrayPath()` | ~600-800 | Column-major flat-array fast path |
| `collectDistinctValuesRaw()` | ~850-900 | Raw LongOpenHashSet for COUNT(DISTINCT) |

### Parallel Execution in FusedScanAggregate
- `executeWithEval()` line ~200: parallel segment workers via `CompletableFuture.supplyAsync`
- `tryFlatArrayPath()` line ~700: parallel segment workers, largest-first assignment
- Both use `FusedGroupByAggregate.getParallelPool()` for the shared ForkJoinPool

---

## File 3: TransportShardExecuteAction.java

**Path**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
**Size**: ~2375 lines

### Key Dispatch Methods
| Method | Lines | Purpose |
|--------|-------|---------|
| `executePlan()` | 194-573 | Main dispatch: tries fused paths in priority order |
| `executeFusedGroupByAggregate()` | 1530-1555 | Dispatches to `FusedGroupByAggregate.execute()` |
| `executeFusedExprGroupByAggregate()` | 1563-1577 | Dispatches to `FusedGroupByAggregate.executeWithExpressionKey()` |
| `executeFusedGroupByAggregateWithTopN()` | 1586-1625 | Dispatches to `FusedGroupByAggregate.executeWithTopN()` |
| `executeCountDistinctWithHashSets()` | 788-914 | 2-key COUNT(DISTINCT) with per-group HashSets |
| `executeMixedDedupWithHashSets()` | 1076-1329 | Mixed dedup (SUM+COUNT) with per-group HashSets |
| `executeVarcharCountDistinctWithHashSets()` | 1337-1503 | VARCHAR COUNT(DISTINCT) with ordinal-based dedup |
| `scanSegmentForCountDistinct()` | 920-1052 | Per-segment COUNT(DISTINCT) scan helper |

### Dispatch Priority in executePlan() (lines 194-573)
1. Scalar COUNT(*) shortcut (line ~215)
2. Sorted scan (line ~225)
3. Fused scalar aggregate (line ~240)
4. Bare single-column scan for COUNT(DISTINCT) (line ~250)
5. Fused eval-aggregate SUM(col+k) (line ~265)
6. COUNT(DISTINCT) dedup with HashSets (line ~275)
7. Fused GROUP BY aggregate (line ~330)
8. Expression key GROUP BY (REGEXP_REPLACE) (line ~340)
9. GROUP BY with TopN (line ~360)
10. Generic operator pipeline fallback

---

## File 4: StringFunctions.java — REGEXP_REPLACE

**Path**: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/StringFunctions.java`
**Lines**: 371-430+

### Current Regex Caching
```java
public static ScalarFunctionImplementation regexpReplace() {
    final String[] cachedPatternStr = {null};
    final Pattern[] cachedPattern = {null};
    return (args, positionCount) -> {
        // Reuse cached compiled pattern if pattern string matches
        if (firstPattern.equals(cachedPatternStr[0])) {
            compiledPattern = cachedPattern[0];
        } else {
            compiledPattern = Pattern.compile(firstPattern);
            cachedPatternStr[0] = firstPattern;
            cachedPattern[0] = compiledPattern;
        }
        // Ultra-fast path: simple group reference ($1) + anchored pattern (^...$)
        // → uses matcher.group() directly instead of replaceAll()
    };
}
```
- Pattern is compiled once per batch and cached across batches
- Matcher is reused across positions within a batch
- Anchored patterns with simple backreferences get special fast path

---

## Summary: Methods Needing Modification for Next Optimizations

### 1. Hash-Partitioned Parallel Aggregation
- **FusedGroupByAggregate.executeSingleKeyNumericFlat** (~line 2050): already has parallel segment workers; could add hash-partition like 2-key path
- **FusedGroupByAggregate.executeThreeKeyFlat** (~line 3800): has hash-partition params but only single-segment; needs multi-segment parallel support
- **FusedGroupByAggregate.executeNKeyVarcharParallelDocRange** (~line 5100): doc-range parallel; could benefit from hash-partition for very high cardinality

### 2. 3-Key Dedup
- **FlatThreeKeyMap** (~line 3700): already exists with findOrInsert, hash3, resize
- **executeThreeKeyFlat** (~line 3800): needs multi-segment parallel path (currently single-segment only)
- **executeNKeyVarcharPath** (~line 4800): dispatches to executeThreeKeyFlat for 3-key; needs to handle multi-segment case

### 3. Regex Caching
- **StringFunctions.regexpReplace()** (~line 385): already caches Pattern across batches
- **FusedGroupByAggregate.executeWithExpressionKey()** (~line 340): ordinal-caching IS the regex optimization (evaluates per-ordinal not per-doc)
- **ExpressionCompiler** compiles expressions once; the BlockExpression is reused
