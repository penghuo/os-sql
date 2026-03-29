# PlanFragmenter.java — COUNT(DISTINCT) Handling Code

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java`

---

## 1. buildShardPlan (Lines 97–155)

```java
  private DqePlanNode buildShardPlan(
      DqePlanNode plan, Map<String, Type> columnTypeMap, int numShards) {
    AggregationNode aggNode = findAggregationNode(plan);
    if (aggNode == null) {
      return plan;
    }
    if (aggNode.getStep() == AggregationNode.Step.PARTIAL) {
      // Optimization: Limit -> Sort -> Agg(PARTIAL) — inflate limit for top-K pre-filtering
      DqePlanNode shardPlanWithTopN = buildShardPlanWithInflatedLimit(plan, aggNode, numShards);
      if (shardPlanWithTopN != null) {
        return shardPlanWithTopN;
      }
      // Optimization: Limit -> [Project ->] Agg(PARTIAL) without Sort — capped insertion
      DqePlanNode shardPlanWithLimit = buildShardPlanWithLimitOnly(plan, aggNode);
      if (shardPlanWithLimit != null) {
        return shardPlanWithLimit;
      }
      // Strip Sort, Limit, and Project above the aggregation.
      DqePlanNode strippedPlan = stripAboveAggregation(plan);
      // Decompose AVG into SUM + COUNT at shard level for correct weighted merge
      return decomposeAvgInShardPlan(strippedPlan);
    }
    // SINGLE with GROUP BY: try shard-level dedup for COUNT(DISTINCT)-only queries.
    // Each shard: GROUP BY (original_keys + distinct_columns) with COUNT(*)
    if (!aggNode.getGroupByKeys().isEmpty() && !columnTypeMap.isEmpty()) {
      List<String> distinctColumns = extractCountDistinctColumns(aggNode.getAggregateFunctions());
      if (distinctColumns != null) {
        // Pure COUNT(DISTINCT) path
        List<String> dedupKeys = new ArrayList<>(aggNode.getGroupByKeys());
        dedupKeys.addAll(distinctColumns);
        return new AggregationNode(
            aggNode.getChild(), dedupKeys, List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);
      }
      // Mixed aggregate dedup: COUNT(DISTINCT) + SUM/COUNT/AVG
      DqePlanNode mixedDedupPlan = buildMixedDedupShardPlan(aggNode);
      if (mixedDedupPlan != null) {
        return mixedDedupPlan;
      }
    }
    // SINGLE fallback: strip aggregation and above, shards only scan+filter
    return aggNode.getChild();
  }
```

**Key decision flow**:
1. `aggNode == null` → non-agg query, return full plan
2. `PARTIAL` step → try inflated-limit TopN, then limit-only, then strip+decompose AVG
3. `SINGLE` + GROUP BY + columnTypeMap present → try pure COUNT(DISTINCT) dedup, then mixed dedup
4. `SINGLE` fallback → strip agg, shards just scan+filter

---

## 2. extractCountDistinctColumns (Lines 218–237)

```java
  private static List<String> extractCountDistinctColumns(List<String> aggregateFunctions) {
    if (aggregateFunctions.isEmpty()) {
      return null;
    }
    List<String> distinctCols = new ArrayList<>();
    for (String func : aggregateFunctions) {
      Matcher m = AGG_PATTERN.matcher(func);
      if (!m.matches()) {
        return null;
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      if (!"COUNT".equals(funcName) || !isDistinct) {
        return null;  // ANY non-COUNT(DISTINCT) → bail out
      }
      distinctCols.add(m.group(3).trim());
    }
    return distinctCols;
  }
```

**Behavior**: Returns column list ONLY if ALL aggregates are `COUNT(DISTINCT col)`. Any non-COUNT(DISTINCT) aggregate → returns null (falls through to mixed dedup path).

---

## 3. Sort/Limit Handling — stripAboveAggregation (Lines 310–340)

```java
  private DqePlanNode stripAboveAggregation(DqePlanNode node) {
    if (node instanceof AggregationNode) {
      return node;
    }
    if (node instanceof LimitNode) {
      return stripAboveAggregation(((LimitNode) node).getChild());
    }
    if (node instanceof SortNode) {
      return stripAboveAggregation(((SortNode) node).getChild());
    }
    if (node instanceof ProjectNode) {
      return stripAboveAggregation(((ProjectNode) node).getChild());
    }
    if (node instanceof FilterNode) {
      return stripAboveAggregation(((FilterNode) node).getChild());
    }
    if (node instanceof EvalNode) {
      DqePlanNode child = stripAboveAggregation(((EvalNode) node).getChild());
      if (child instanceof AggregationNode) {
        return new EvalNode(
            child, ((EvalNode) node).getExpressions(), ((EvalNode) node).getOutputColumnNames());
      }
      return child;
    }
    return node;
  }
```

**Behavior**: Recursively strips Limit, Sort, Project, Filter (HAVING) above AggregationNode. EvalNode is preserved if directly above AggregationNode (it computes expressions on agg output).

---

## 4. Mixed Dedup Path — buildMixedDedupShardPlan (Lines 163–216)

```java
  private static DqePlanNode buildMixedDedupShardPlan(AggregationNode aggNode) {
    List<String> aggFunctions = aggNode.getAggregateFunctions();
    List<String> distinctCols = new ArrayList<>();
    boolean hasCountDistinct = false;
    boolean hasDecomposable = false;

    for (String func : aggFunctions) {
      Matcher m = AGG_PATTERN.matcher(func);
      if (!m.matches()) {
        return null; // Unsupported aggregate
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();

      if (isDistinct) {
        if (!"COUNT".equals(funcName)) {
          return null; // Only COUNT(DISTINCT) supported
        }
        hasCountDistinct = true;
        if (!distinctCols.contains(arg)) {
          distinctCols.add(arg);
        }
      } else {
        hasDecomposable = true;
      }
    }

    if (!hasCountDistinct || !hasDecomposable) {
      return null;
    }

    // GROUP BY keys = original keys + distinct columns
    List<String> dedupKeys = new ArrayList<>(aggNode.getGroupByKeys());
    dedupKeys.addAll(distinctCols);

    // Build partial aggregates — AVG decomposed, COUNT(DISTINCT) omitted
    List<String> shardAggs = new ArrayList<>();
    for (String func : aggFunctions) {
      Matcher m = AGG_PATTERN.matcher(func);
      if (!m.matches()) continue;
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();

      if (isDistinct) {
        continue;  // COUNT(DISTINCT) handled by dedup GROUP BY
      }
      if ("AVG".equals(funcName)) {
        shardAggs.add("SUM(" + arg + ")");      // Decompose AVG
        shardAggs.add("COUNT(" + arg + ")");
      } else {
        shardAggs.add(func);  // COUNT(*), SUM, MIN, MAX pass through
      }
    }

    return new AggregationNode(
        aggNode.getChild(), dedupKeys, shardAggs, AggregationNode.Step.PARTIAL);
  }
```

**Example transformation** (from Javadoc):
- Input: `GROUP BY RegionID` with `SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID)`
- Shard plan: `GROUP BY (RegionID, UserID)` with `SUM(AdvEngineID), COUNT(*), SUM(ResolutionWidth), COUNT(ResolutionWidth)`

**Key rules**:
- Requires BOTH `COUNT(DISTINCT)` AND decomposable aggregates present
- `COUNT(DISTINCT col)` → col added to GROUP BY keys, no shard aggregate emitted
- `AVG(col)` → decomposed to `SUM(col) + COUNT(col)`
- `SUM/COUNT(*)/MIN/MAX` → passed through as-is
- Non-COUNT DISTINCT (e.g., `SUM(DISTINCT)`) → returns null (not supported)

---

## 5. AGG_PATTERN Regex (Line 36)

```java
  private static final Pattern AGG_PATTERN =
      Pattern.compile(
          "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$", Pattern.CASE_INSENSITIVE);
```

Groups: (1) function name, (2) DISTINCT flag or null, (3) argument column

---

## 6. buildShardPlanWithInflatedLimit — TopN Preservation (Lines 247–296)

```java
  private static DqePlanNode buildShardPlanWithInflatedLimit(
      DqePlanNode plan, AggregationNode aggNode, int numShards) {
    if (!(plan instanceof LimitNode limitNode)) {
      return null;
    }
    DqePlanNode belowLimit = limitNode.getChild();
    if (belowLimit instanceof ProjectNode projectNode) {
      belowLimit = projectNode.getChild();
    }
    if (!(belowLimit instanceof SortNode sortNode)) {
      return null;
    }
    // Primary sort key must be an aggregate column (not group-by key)
    List<String> sortKeys = sortNode.getSortKeys();
    List<String> groupByKeys = aggNode.getGroupByKeys();
    List<String> aggFunctions = aggNode.getAggregateFunctions();
    if (sortKeys.isEmpty()) {
      return null;
    }
    List<String> aggOutput = new ArrayList<>(groupByKeys);
    aggOutput.addAll(aggFunctions);
    String primarySortKey = sortKeys.get(0);
    int primaryIdx = aggOutput.indexOf(primarySortKey);
    if (primaryIdx < 0 || primaryIdx < groupByKeys.size()) {
      return null;
    }
    if (hasFilterAboveAggregation(plan)) {
      return null;
    }

    long originalLimit = limitNode.getCount() + limitNode.getOffset();
    boolean allCountStar =
        aggNode.getAggregateFunctions().stream()
            .allMatch(f -> f.trim().equalsIgnoreCase("COUNT(*)"));
    long inflatedLimit =
        allCountStar
            ? Math.max(1000, originalLimit)
            : Math.max(1000, originalLimit * numShards * 2);

    return new LimitNode(
        new SortNode(
            aggNode, sortNode.getSortKeys(), sortNode.getAscending(), sortNode.getNullsFirst()),
        inflatedLimit);
  }
```

**Inflation formula**: `allCountStar ? max(1000, limit) : max(1000, limit * numShards * 2)`

**Pattern matched**: `Limit -> [Project ->] Sort -> Agg(PARTIAL)` where primary sort key is an aggregate column.
