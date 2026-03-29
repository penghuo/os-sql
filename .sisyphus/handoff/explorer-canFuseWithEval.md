# FusedScanAggregate — Eval-Fusion Methods

Source: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`

---

## 1. `canFuseWithEval` (lines ~101–137)

```java
  public static boolean canFuseWithEval(AggregationNode aggNode) {
    if (!aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    if (!(aggNode.getChild() instanceof EvalNode evalNode)) {
      return false;
    }
    if (!(evalNode.getChild() instanceof TableScanNode)) {
      return false;
    }
    // All aggregate functions must be SUM (non-distinct)
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        return false;
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      if (!"SUM".equals(funcName) || isDistinct) {
        return false;
      }
    }
    // All eval expressions must be plain columns or (column + integer_constant)
    for (String expr : evalNode.getExpressions()) {
      String trimmed = expr.trim();
      if (trimmed.matches("\\w+")) {
        continue;
      }
      Matcher cm = COL_PLUS_CONST.matcher(trimmed);
      if (cm.matches()) {
        continue;
      }
      return false;
    }
    return true;
  }
```

**Key constraint**: Currently only allows `SUM` (non-distinct). The check `if (!"SUM".equals(funcName) || isDistinct)` rejects COUNT(*) and AVG.

---

## 2. `executeWithEval` (lines ~148–303)

```java
  public static List<Page> executeWithEval(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {
    EvalNode evalNode = (EvalNode) aggNode.getChild();
    List<String> evalExprs = evalNode.getExpressions();
    List<String> evalNames = evalNode.getOutputColumnNames();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Build mapping: eval column name -> (physicalColumn, offset)
    Map<String, String> nameToPhysical = new java.util.LinkedHashMap<>();
    Map<String, Long> nameToOffset = new java.util.LinkedHashMap<>();
    Set<String> uniquePhysicalColumns = new java.util.LinkedHashSet<>();

    for (int i = 0; i < evalExprs.size(); i++) {
      String expr = evalExprs.get(i).trim();
      String name = evalNames.get(i);
      Matcher cm = COL_PLUS_CONST.matcher(expr);
      if (cm.matches()) {
        String col = cm.group(1);
        long offset = Long.parseLong(cm.group(2));
        nameToPhysical.put(name, col);
        nameToOffset.put(name, offset);
        uniquePhysicalColumns.add(col);
      } else {
        nameToPhysical.put(name, expr);
        nameToOffset.put(name, 0L);
        uniquePhysicalColumns.add(expr);
      }
    }

    // Compute SUM and COUNT for each unique physical column
    Map<String, long[]> colSumCount = new java.util.LinkedHashMap<>();
    for (String col : uniquePhysicalColumns) {
      colSumCount.put(col, new long[2]); // [sum, count]
    }

    // ... (Lucene DocValues iteration — MatchAll fast paths, parallel/sequential, filtered) ...

    // === ALGEBRAIC IDENTITY APPLICATION (lines ~280–303) ===
    // Derive results: SUM(col + k) = SUM(col) + k * COUNT(col)
    int numAggs = aggFunctions.size();
    BlockBuilder[] builders = new BlockBuilder[numAggs];
    for (int i = 0; i < numAggs; i++) {
      builders[i] = BigintType.BIGINT.createBlockBuilder(null, 1);
    }

    for (int i = 0; i < numAggs; i++) {
      Matcher m = AGG_FUNCTION.matcher(aggFunctions.get(i));
      if (!m.matches()) {
        throw new IllegalArgumentException("Unsupported aggregate: " + aggFunctions.get(i));
      }
      String arg = m.group(3).trim(); // The eval column name
      String physCol = nameToPhysical.get(arg);
      long offset = nameToOffset.getOrDefault(arg, 0L);
      long[] sc = colSumCount.get(physCol);
      long result = sc[0] + offset * sc[1]; // SUM(col) + k * COUNT
      BigintType.BIGINT.writeLong(builders[i], result);
    }

    Block[] blocks = new Block[numAggs];
    for (int i = 0; i < numAggs; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }
```

**Key detail**: The method already computes both `SUM(col)` and `COUNT(col)` for every unique physical column (stored in `colSumCount` as `long[2]` = `[sum, count]`). The algebraic identity is applied at line `long result = sc[0] + offset * sc[1]`.

---

## 3. `resolveEvalAggOutputTypes` (lines ~309–316)

```java
  public static List<Type> resolveEvalAggOutputTypes(AggregationNode aggNode) {
    List<Type> types = new ArrayList<>();
    for (int i = 0; i < aggNode.getAggregateFunctions().size(); i++) {
      types.add(BigintType.BIGINT);
    }
    return types;
  }
```

**Key constraint**: Hardcodes all output types to `BigintType.BIGINT`. To support COUNT(*) and AVG, this must be extended to return `BigintType.BIGINT` for COUNT and `DoubleType.DOUBLE` for AVG.

---

## 4. How the Algebraic Identity `SUM(col+k) = SUM(col) + k*COUNT(*)` Is Applied

The identity is implemented in three stages:

1. **Parsing** (eval expressions): Each eval expression like `(ResolutionWidth + 1)` is decomposed via `COL_PLUS_CONST` regex into `physicalColumn=ResolutionWidth` and `offset=1`. Plain column refs get `offset=0`.

2. **Accumulation** (Lucene DocValues): For each *unique* physical column, a single pass over DocValues computes both `SUM(col)` and `COUNT(col)` into `colSumCount.get(col) = long[]{sum, count}`. This means 90 eval expressions referencing the same column only read that column once.

3. **Derivation** (result assembly): For each aggregate `SUM(evalName)`, the result is:
   ```java
   long result = sc[0] + offset * sc[1];  // SUM(col) + k * COUNT(col)
   ```
   When `offset=0` (plain column), this reduces to just `SUM(col)`.

**Important for extension**: The `count` (`sc[1]`) is already available per physical column. Adding `COUNT(*)` support just needs to emit `sc[1]` directly. Adding `AVG(col+k)` would use `(sc[0] + offset * sc[1]) / (double) sc[1]`.
