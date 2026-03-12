/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.sql.dqe.operator.LongOpenHashSet;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

/**
 * Fused scan-group-aggregate operator that uses Lucene SortedSetDocValues ordinals as hash keys
 * during GROUP BY aggregation on string columns. This avoids the expensive lookupOrd() call for
 * every row, deferring string resolution to the final output phase.
 *
 * <p>The key optimization: during aggregation, string GROUP BY keys are represented as long
 * ordinals from SortedSetDocValues. Since ordinals are segment-local, we aggregate per-segment
 * using ordinals, then merge results across segments by resolving ordinals to strings.
 *
 * <p>Supports GROUP BY patterns with:
 *
 * <ul>
 *   <li>Single VARCHAR key (e.g., GROUP BY SearchPhrase)
 *   <li>Multiple keys mixing VARCHAR and numeric types (e.g., GROUP BY SearchEngineID,
 *       SearchPhrase)
 * </ul>
 *
 * <p>Aggregate functions: COUNT(*), SUM, MIN, MAX, AVG, COUNT(DISTINCT).
 */
public final class FusedGroupByAggregate {

  private static final Pattern AGG_FUNCTION =
      Pattern.compile(
          "^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)\\s*$", Pattern.CASE_INSENSITIVE);

  /**
   * Pattern to recognize DATE_TRUNC expressions in group-by keys. Matches expressions like
   * "date_trunc('minute', EventTime)" and extracts the unit and source column name.
   */
  private static final Pattern DATE_TRUNC_PATTERN =
      Pattern.compile(
          "^date_trunc\\('(second|minute|hour|day|month|year)',\\s*(\\w+)\\)$",
          Pattern.CASE_INSENSITIVE);

  /**
   * Pattern to recognize arithmetic expressions in group-by keys. Matches expressions like
   * "(ClientIP - 1)", "(col + 42)", "(col * 2)", "(col / 10)". Extracts the column name, operator,
   * and integer constant. These are handled by reading the source column from DocValues and
   * applying the arithmetic inline during key extraction.
   */
  private static final Pattern ARITH_EXPR_PATTERN =
      Pattern.compile("^\\(?(\\w+)\\s*([+\\-*/])\\s*(\\d+)\\)?$", Pattern.CASE_INSENSITIVE);

  /**
   * Pattern to recognize EXTRACT expressions in group-by keys. Matches expressions like
   * "EXTRACT(MINUTE FROM EventTime)" and extracts the field name and source column. These are
   * handled by reading the source timestamp column from DocValues (epoch millis) and computing the
   * extracted field inline during key extraction. Supports YEAR, MONTH, DAY, HOUR, MINUTE, SECOND,
   * DOW (day of week), DOY (day of year), and WEEK fields.
   */
  private static final Pattern EXTRACT_PATTERN =
      Pattern.compile(
          "^EXTRACT\\((YEAR|QUARTER|MONTH|WEEK|DAY|DAY_OF_MONTH|DAY_OF_WEEK|DOW|DAY_OF_YEAR|DOY|HOUR|MINUTE|SECOND)\\s+FROM\\s+(\\w+)\\)$",
          Pattern.CASE_INSENSITIVE);

  private FusedGroupByAggregate() {}

  /**
   * Compile an expression string and return its output type. Used to determine the type of
   * EvalNode-computed group-by keys (e.g., CASE WHEN expressions).
   */
  private static Type compileAndGetType(String exprStr, Map<String, Type> columnTypeMap) {
    try {
      org.opensearch.sql.dqe.function.FunctionRegistry registry =
          org.opensearch.sql.dqe.function.BuiltinFunctions.createRegistry();
      io.trino.sql.parser.SqlParser sqlParser = new io.trino.sql.parser.SqlParser();
      // Normalize column types: integer-family -> BigintType
      Map<String, Integer> colIndexMap = new HashMap<>();
      Map<String, Type> normalizedMap = new HashMap<>();
      int idx = 0;
      for (Map.Entry<String, Type> entry : columnTypeMap.entrySet()) {
        colIndexMap.put(entry.getKey(), idx++);
        Type t = entry.getValue();
        if (t instanceof VarcharType || t instanceof DoubleType || t instanceof TimestampType) {
          normalizedMap.put(entry.getKey(), t);
        } else {
          normalizedMap.put(entry.getKey(), BigintType.BIGINT);
        }
      }
      org.opensearch.sql.dqe.function.expression.ExpressionCompiler compiler =
          new org.opensearch.sql.dqe.function.expression.ExpressionCompiler(
              registry, colIndexMap, normalizedMap);
      io.trino.sql.tree.Expression ast = sqlParser.createExpression(exprStr);
      org.opensearch.sql.dqe.function.expression.BlockExpression blockExpr = compiler.compile(ast);
      return blockExpr.getType();
    } catch (Exception e) {
      // If compilation fails, default to VARCHAR (most common for CASE WHEN)
      return VarcharType.VARCHAR;
    }
  }

  /**
   * Find the TableScanNode underneath an AggregationNode, looking through an optional EvalNode.
   * Returns null if the child structure is not TableScanNode or EvalNode → TableScanNode.
   */
  private static TableScanNode findChildTableScan(AggregationNode aggNode) {
    DqePlanNode child = aggNode.getChild();
    if (child instanceof TableScanNode tsn) {
      return tsn;
    }
    if (child instanceof EvalNode evalNode && evalNode.getChild() instanceof TableScanNode tsn) {
      return tsn;
    }
    return null;
  }

  /**
   * Check if the shard plan is a GROUP BY aggregation that can use the fused path. Requirements:
   *
   * <ul>
   *   <li>Plan is AggregationNode with non-empty groupByKeys
   *   <li>Child is a TableScanNode or EvalNode → TableScanNode
   *   <li>All group-by keys are either plain columns with VARCHAR/numeric/timestamp types, or
   *       supported expressions like DATE_TRUNC on a timestamp column
   *   <li>All aggregate functions are supported
   * </ul>
   */
  public static boolean canFuse(AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    if (aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    if (findChildTableScan(aggNode) == null) {
      return false;
    }

    // Build set of EvalNode output column names for recognizing computed keys
    Set<String> evalOutputNames = Set.of();
    if (aggNode.getChild() instanceof EvalNode evalNode) {
      evalOutputNames = new HashSet<>(evalNode.getOutputColumnNames());
    }

    // Check all group-by keys are supported types (VARCHAR, numeric, or timestamp)
    // or recognized expression patterns (DATE_TRUNC)
    for (String key : aggNode.getGroupByKeys()) {
      Type type = columnTypeMap.get(key);
      if (type != null) {
        // Plain column reference
        if (!(type instanceof VarcharType) && !isNumericOrTimestamp(type)) {
          return false; // Unsupported group-by key type
        }
      } else {
        // Not a plain column — check if it's a supported expression
        Matcher dtm = DATE_TRUNC_PATTERN.matcher(key);
        if (dtm.matches()) {
          // Verify the source column is a timestamp type
          String sourceCol = dtm.group(2);
          Type sourceType = columnTypeMap.get(sourceCol);
          if (!(sourceType instanceof TimestampType)) {
            return false;
          }
        } else {
          // Check for EXTRACT expression: EXTRACT(FIELD FROM col)
          Matcher em = EXTRACT_PATTERN.matcher(key);
          if (em.matches()) {
            String sourceCol = em.group(2);
            Type sourceType = columnTypeMap.get(sourceCol);
            if (!(sourceType instanceof TimestampType)) {
              return false;
            }
          } else {
            // Check for arithmetic expression: (col OP constant)
            Matcher am = ARITH_EXPR_PATTERN.matcher(key);
            if (am.matches()) {
              String sourceCol = am.group(1);
              Type sourceType = columnTypeMap.get(sourceCol);
              if (sourceType == null || !isNumericOrTimestamp(sourceType)) {
                return false;
              }
            } else if (evalOutputNames.contains(key)) {
              // EvalNode-computed key (e.g., CASE WHEN expression).
              // The expression will be compiled and evaluated per-document
              // using the ExpressionCompiler. Try to compile it now to verify.
              try {
                EvalNode evalNode = (EvalNode) aggNode.getChild();
                int exprIdx = evalNode.getOutputColumnNames().indexOf(key);
                if (exprIdx < 0) {
                  return false;
                }
                String exprStr = evalNode.getExpressions().get(exprIdx);
                // Quick check: the expression should not be a plain column pass-through
                if (columnTypeMap.containsKey(exprStr)) {
                  return false; // Should have been handled as a plain column
                }
              } catch (Exception e) {
                return false;
              }
            } else {
              return false; // Unknown expression, can't fuse
            }
          }
        }
      }
    }

    // Check all aggregate functions are supported
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if the plan is a GROUP BY with a single computed expression key over a VARCHAR column,
   * suitable for ordinal-based expression caching. The pattern is:
   *
   * <pre>
   *   AggregationNode(groupByKeys=[exprName]) -> EvalNode(exprName=expr(sourceCol)) -> TableScanNode
   * </pre>
   *
   * <p>The key insight: if the source column has N unique ordinals in SortedSetDocValues, we
   * evaluate the expression N times (once per ordinal) instead of M times (once per doc, M >> N).
   * For Q29 with REGEXP_REPLACE on Referer: ~16K ordinals vs ~921K docs = ~58x reduction.
   *
   * @param aggNode the aggregation plan node
   * @param columnTypeMap mapping from physical column name to Trino Type
   * @return true if the plan can use ordinal-cached expression evaluation
   */
  public static boolean canFuseWithExpressionKey(
      AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    // Must have exactly one group-by key
    if (aggNode.getGroupByKeys().size() != 1) {
      return false;
    }
    // Child must be EvalNode -> TableScanNode
    if (!(aggNode.getChild() instanceof EvalNode evalNode)) {
      return false;
    }
    if (!(evalNode.getChild() instanceof TableScanNode)) {
      return false;
    }

    String groupByKey = aggNode.getGroupByKeys().get(0);

    // The group-by key must NOT be a physical column (it must be a computed expression)
    if (columnTypeMap.containsKey(groupByKey)) {
      return false;
    }

    // The group-by key must be one of the EvalNode's output column names
    int keyExprIdx = evalNode.getOutputColumnNames().indexOf(groupByKey);
    if (keyExprIdx < 0) {
      return false;
    }

    // The expression at that index must reference a single VARCHAR source column.
    // We check this by looking at the EvalNode expression string and finding column
    // references that are in the scan node's columns and are VARCHAR.
    String exprStr = evalNode.getExpressions().get(keyExprIdx);
    TableScanNode scanNode = (TableScanNode) evalNode.getChild();
    List<String> scanColumns = scanNode.getColumns();

    // Find which scan columns appear in the expression
    String sourceVarcharCol = null;
    for (String col : scanColumns) {
      Type colType = columnTypeMap.get(col);
      if (colType instanceof VarcharType && exprStr.contains(col)) {
        if (sourceVarcharCol != null) {
          return false; // Multiple VARCHAR columns referenced — not supported
        }
        sourceVarcharCol = col;
      }
    }
    if (sourceVarcharCol == null) {
      return false; // No VARCHAR source column found
    }

    // Check all aggregate functions are supported
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        return false;
      }
      // Verify aggregate arguments are either physical columns, *, or EvalNode outputs
      String arg = m.group(3).trim();
      if (!"*".equals(arg)
          && !columnTypeMap.containsKey(arg)
          && !evalNode.getOutputColumnNames().contains(arg)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Execute GROUP BY aggregation with ordinal-based expression caching. Pre-computes the group-by
   * expression for each unique ordinal in SortedSetDocValues, then uses the cached result during
   * the scan. Also pre-computes any EvalNode expressions used as aggregate arguments that depend on
   * the same source column.
   *
   * @param aggNode the aggregation plan node
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @param columnTypeMap mapping from physical column name to Trino Type
   * @return a list containing a single Page with the aggregated results
   */
  public static List<Page> executeWithExpressionKey(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {

    EvalNode evalNode = (EvalNode) aggNode.getChild();
    TableScanNode scanNode = (TableScanNode) evalNode.getChild();
    List<String> groupByKeys = aggNode.getGroupByKeys();
    List<String> aggFunctions = aggNode.getAggregateFunctions();
    String groupByKey = groupByKeys.get(0);

    // Find the group-by key expression index in EvalNode
    int keyExprIdx = evalNode.getOutputColumnNames().indexOf(groupByKey);
    String keyExprStr = evalNode.getExpressions().get(keyExprIdx);

    // Find the source VARCHAR column
    List<String> scanColumns = scanNode.getColumns();
    String sourceVarcharCol = null;
    for (String col : scanColumns) {
      Type colType = columnTypeMap.get(col);
      if (colType instanceof VarcharType && keyExprStr.contains(col)) {
        sourceVarcharCol = col;
        break;
      }
    }

    // Parse aggregate specs
    final int numAggs = aggFunctions.size();
    List<AggSpec> specs = new ArrayList<>();
    for (String func : aggFunctions) {
      Matcher m = AGG_FUNCTION.matcher(func);
      m.matches();
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();
      Type argType = arg.equals("*") ? null : columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      specs.add(new AggSpec(funcName, isDistinct, arg, argType));
    }

    // Identify which aggregate args are EvalNode-computed (vs physical columns)
    // For computed args that depend on sourceVarcharCol, we can cache them per ordinal too
    boolean[] isComputedArg = new boolean[numAggs];
    String[] computedArgExpr = new String[numAggs];
    boolean[] isCountStar = new boolean[numAggs];
    boolean[] isDoubleArg = new boolean[numAggs];
    boolean[] isVarcharArg = new boolean[numAggs];
    int[] accType = new int[numAggs];

    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);

      if (!isCountStar[i]) {
        // Check if the arg is a computed EvalNode column
        int evalIdx = evalNode.getOutputColumnNames().indexOf(spec.arg);
        if (evalIdx >= 0 && !columnTypeMap.containsKey(spec.arg)) {
          isComputedArg[i] = true;
          computedArgExpr[i] = evalNode.getExpressions().get(evalIdx);
          // Computed args like length(Referer) produce BigintType
          isDoubleArg[i] = false;
          isVarcharArg[i] = false;
        } else {
          isDoubleArg[i] = spec.argType instanceof DoubleType;
          isVarcharArg[i] = spec.argType instanceof VarcharType;
        }
      }

      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        switch (spec.funcName) {
          case "COUNT":
            accType[i] = spec.isDistinct ? 5 : 0;
            break;
          case "SUM":
            accType[i] = isDoubleArg[i] ? 6 : 1;
            break;
          case "AVG":
            accType[i] = isDoubleArg[i] ? 7 : 2;
            break;
          case "MIN":
            accType[i] = 3;
            break;
          case "MAX":
            accType[i] = 4;
            break;
        }
      }
    }

    // Build expression evaluators for ordinal caching
    // We parse and compile expressions using the function registry
    org.opensearch.sql.dqe.function.FunctionRegistry registry =
        org.opensearch.sql.dqe.function.BuiltinFunctions.createRegistry();
    io.trino.sql.parser.SqlParser sqlParser = new io.trino.sql.parser.SqlParser();

    // For the key expression, we need a function: String -> String
    // Parse: "regexp_replace(Referer, '^pattern$', '$1')" with Referer at column index 0
    Map<String, Integer> colIndexMap = new HashMap<>();
    colIndexMap.put(sourceVarcharCol, 0);
    org.opensearch.sql.dqe.function.expression.ExpressionCompiler compiler =
        new org.opensearch.sql.dqe.function.expression.ExpressionCompiler(
            registry, colIndexMap, columnTypeMap);

    // Compile group-by key expression
    io.trino.sql.tree.Expression keyAst = sqlParser.createExpression(keyExprStr);
    org.opensearch.sql.dqe.function.expression.BlockExpression keyBlockExpr =
        compiler.compile(keyAst);

    // Compile computed aggregate arg expressions
    org.opensearch.sql.dqe.function.expression.BlockExpression[] computedArgBlockExprs =
        new org.opensearch.sql.dqe.function.expression.BlockExpression[numAggs];
    for (int i = 0; i < numAggs; i++) {
      if (isComputedArg[i]) {
        io.trino.sql.tree.Expression argAst = sqlParser.createExpression(computedArgExpr[i]);
        computedArgBlockExprs[i] = compiler.compile(argAst);
      }
    }

    return executeWithExpressionKeyImpl(
        shard,
        query,
        sourceVarcharCol,
        specs,
        numAggs,
        isCountStar,
        isComputedArg,
        isDoubleArg,
        isVarcharArg,
        accType,
        keyBlockExpr,
        computedArgBlockExprs,
        columnTypeMap);
  }

  /** Internal implementation of ordinal-cached expression GROUP BY using a global groups map. */
  private static List<Page> executeWithExpressionKeyImpl(
      IndexShard shard,
      Query query,
      String srcCol,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      boolean[] isComputedArg,
      boolean[] isDoubleArg,
      boolean[] isVarcharArg,
      int[] accType,
      org.opensearch.sql.dqe.function.expression.BlockExpression keyBlockExpr,
      org.opensearch.sql.dqe.function.expression.BlockExpression[] computedArgBlockExprs,
      Map<String, Type> columnTypeMap)
      throws Exception {

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-expr-groupby")) {

      // Global groups keyed by the computed expression result string.
      // Since Lucene processes segments sequentially in a single collector thread,
      // we can safely accumulate into this global map without synchronization.
      HashMap<String, AccumulatorGroup> globalGroups = new HashMap<>();

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              LeafReader reader = context.reader();
              SortedSetDocValues dv = reader.getSortedSetDocValues(srcCol);
              if (dv == null) {
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) {}
                };
              }

              long ordCount = dv.getValueCount();
              if (ordCount <= 0 || ordCount > 10_000_000) {
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) {}
                };
              }

              int ordCountInt = (int) ordCount;

              // Pre-compute expressions for each ordinal in this segment
              String[] ordToGroupKey = new String[ordCountInt];
              long[][] ordToComputedArg = new long[numAggs][];
              for (int i = 0; i < numAggs; i++) {
                if (isComputedArg[i]) {
                  ordToComputedArg[i] = new long[ordCountInt];
                }
              }

              // Check if any VARCHAR agg arg references the same source column
              // If so, cache the raw string per ordinal to avoid per-doc lookupOrd
              boolean[] isVarcharSameCol = new boolean[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i]
                    && !isComputedArg[i]
                    && isVarcharArg[i]
                    && specs.get(i).arg.equals(srcCol)) {
                  isVarcharSameCol[i] = true;
                }
              }
              boolean needRawStringCache = false;
              for (int i = 0; i < numAggs; i++) {
                if (isVarcharSameCol[i]) {
                  needRawStringCache = true;
                  break;
                }
              }
              String[] ordToRawString = needRawStringCache ? new String[ordCountInt] : null;

              // Process ordinals in batches for expression evaluation
              int batchSize = Math.min(ordCountInt, 4096);
              for (int batchStart = 0; batchStart < ordCountInt; batchStart += batchSize) {
                int batchEnd = Math.min(batchStart + batchSize, ordCountInt);
                int batchLen = batchEnd - batchStart;

                BlockBuilder inputBuilder = VarcharType.VARCHAR.createBlockBuilder(null, batchLen);
                for (int ord = batchStart; ord < batchEnd; ord++) {
                  BytesRef bytes = dv.lookupOrd(ord);
                  VarcharType.VARCHAR.writeSlice(
                      inputBuilder, Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
                  // Cache raw string if needed for VARCHAR agg args
                  if (ordToRawString != null) {
                    ordToRawString[ord] = bytes.utf8ToString();
                  }
                }
                Block inputBlock = inputBuilder.build();
                Page inputPage = new Page(inputBlock);

                Block keyResultBlock = keyBlockExpr.evaluate(inputPage);
                for (int j = 0; j < batchLen; j++) {
                  if (keyResultBlock.isNull(j)) {
                    ordToGroupKey[batchStart + j] = null;
                  } else {
                    ordToGroupKey[batchStart + j] =
                        VarcharType.VARCHAR.getSlice(keyResultBlock, j).toStringUtf8();
                  }
                }

                for (int i = 0; i < numAggs; i++) {
                  if (isComputedArg[i]) {
                    Block argResultBlock = computedArgBlockExprs[i].evaluate(inputPage);
                    for (int j = 0; j < batchLen; j++) {
                      if (!argResultBlock.isNull(j)) {
                        ordToComputedArg[i][batchStart + j] =
                            computedArgBlockExprs[i].getType().getLong(argResultBlock, j);
                      }
                    }
                  }
                }
              }

              // Pre-wire ordinal -> AccumulatorGroup using global groups map
              // Multiple ordinals may map to the same expression result string
              AccumulatorGroup[] ordGroups = new AccumulatorGroup[ordCountInt];
              for (int ord = 0; ord < ordCountInt; ord++) {
                String gk = ordToGroupKey[ord];
                if (gk != null) {
                  AccumulatorGroup existing = globalGroups.get(gk);
                  if (existing == null) {
                    existing = createAccumulatorGroup(specs);
                    globalGroups.put(gk, existing);
                  }
                  ordGroups[ord] = existing;
                }
              }

              // Open doc values for physical aggregate args (skip same-col varchar args)
              SortedNumericDocValues[] colNumDvs = new SortedNumericDocValues[numAggs];
              SortedSetDocValues[] colVarDvs = new SortedSetDocValues[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i] && !isComputedArg[i] && !isVarcharSameCol[i]) {
                  AggSpec spec = specs.get(i);
                  if (isVarcharArg[i]) {
                    colVarDvs[i] = reader.getSortedSetDocValues(spec.arg);
                  } else {
                    colNumDvs[i] = reader.getSortedNumericDocValues(spec.arg);
                  }
                }
              }

              final long[][] finalOrdToComputedArg = ordToComputedArg;
              final String[] finalOrdToRawString = ordToRawString;
              final boolean[] finalIsVarcharSameCol = isVarcharSameCol;

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  if (!dv.advanceExact(doc)) return;
                  int ord = (int) dv.nextOrd();
                  if (ord >= ordGroups.length) return;
                  AccumulatorGroup accGroup = ordGroups[ord];
                  if (accGroup == null) return;

                  for (int i = 0; i < numAggs; i++) {
                    MergeableAccumulator acc = accGroup.accumulators[i];
                    if (isCountStar[i]) {
                      ((CountStarAccum) acc).count++;
                      continue;
                    }
                    // Fast path: VARCHAR agg arg referencing the source column
                    // Use pre-cached string from ordinal (avoids advanceExact + lookupOrd)
                    if (finalIsVarcharSameCol[i]) {
                      String val = finalOrdToRawString[ord];
                      switch (accType[i]) {
                        case 5:
                          ((CountDistinctAccum) acc).objectDistinctValues.add(val);
                          break;
                        case 3:
                          MinAccum ma = (MinAccum) acc;
                          if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                            ma.objectVal = val;
                            ma.hasValue = true;
                          }
                          break;
                        case 4:
                          MaxAccum xa = (MaxAccum) acc;
                          if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                            xa.objectVal = val;
                            xa.hasValue = true;
                          }
                          break;
                      }
                      continue;
                    }
                    if (isComputedArg[i]) {
                      long val = finalOrdToComputedArg[i][ord];
                      switch (accType[i]) {
                        case 0:
                          ((CountStarAccum) acc).count++;
                          break;
                        case 1:
                          SumAccum sa = (SumAccum) acc;
                          sa.hasValue = true;
                          sa.longSum += val;
                          break;
                        case 2:
                          AvgAccum aa = (AvgAccum) acc;
                          aa.count++;
                          aa.longSum += val;
                          break;
                        case 3:
                          MinAccum mna = (MinAccum) acc;
                          mna.hasValue = true;
                          if (val < mna.longVal) mna.longVal = val;
                          break;
                        case 4:
                          MaxAccum mxa = (MaxAccum) acc;
                          mxa.hasValue = true;
                          if (val > mxa.longVal) mxa.longVal = val;
                          break;
                        case 5:
                          CountDistinctAccum cda = (CountDistinctAccum) acc;
                          if (cda.usePrimitiveLong) cda.longDistinctValues.add(val);
                          break;
                      }
                      continue;
                    }
                    if (isVarcharArg[i]) {
                      SortedSetDocValues varcharDv = colVarDvs[i];
                      if (varcharDv != null && varcharDv.advanceExact(doc)) {
                        BytesRef bytes = varcharDv.lookupOrd(varcharDv.nextOrd());
                        String val = bytes.utf8ToString();
                        switch (accType[i]) {
                          case 5:
                            ((CountDistinctAccum) acc).objectDistinctValues.add(val);
                            break;
                          case 3:
                            MinAccum ma = (MinAccum) acc;
                            if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                              ma.objectVal = val;
                              ma.hasValue = true;
                            }
                            break;
                          case 4:
                            MaxAccum xa = (MaxAccum) acc;
                            if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                              xa.objectVal = val;
                              xa.hasValue = true;
                            }
                            break;
                        }
                      }
                      continue;
                    }
                    SortedNumericDocValues aggDv = colNumDvs[i];
                    if (aggDv != null && aggDv.advanceExact(doc)) {
                      long rawVal = aggDv.nextValue();
                      switch (accType[i]) {
                        case 0:
                          ((CountStarAccum) acc).count++;
                          break;
                        case 1:
                          SumAccum sa = (SumAccum) acc;
                          sa.hasValue = true;
                          sa.longSum += rawVal;
                          break;
                        case 2:
                          AvgAccum aa = (AvgAccum) acc;
                          aa.count++;
                          aa.longSum += rawVal;
                          break;
                        case 3:
                          MinAccum mna = (MinAccum) acc;
                          mna.hasValue = true;
                          if (isDoubleArg[i]) {
                            double d = Double.longBitsToDouble(rawVal);
                            if (d < mna.doubleVal) mna.doubleVal = d;
                          } else {
                            if (rawVal < mna.longVal) mna.longVal = rawVal;
                          }
                          break;
                        case 4:
                          MaxAccum mxa = (MaxAccum) acc;
                          mxa.hasValue = true;
                          if (isDoubleArg[i]) {
                            double d = Double.longBitsToDouble(rawVal);
                            if (d > mxa.doubleVal) mxa.doubleVal = d;
                          } else {
                            if (rawVal > mxa.longVal) mxa.longVal = rawVal;
                          }
                          break;
                        case 5:
                          CountDistinctAccum cda = (CountDistinctAccum) acc;
                          if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
                          else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
                          break;
                        case 6:
                          SumAccum sad = (SumAccum) acc;
                          sad.hasValue = true;
                          sad.doubleSum += Double.longBitsToDouble(rawVal);
                          break;
                        case 7:
                          AvgAccum aad = (AvgAccum) acc;
                          aad.count++;
                          aad.doubleSum += Double.longBitsToDouble(rawVal);
                          break;
                      }
                    }
                  }
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });

      // Build output Page
      if (globalGroups.isEmpty()) {
        return List.of();
      }

      int groupCount = globalGroups.size();
      int totalColumns = 1 + numAggs;
      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
      for (int i = 0; i < numAggs; i++) {
        builders[1 + i] =
            resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
      }

      for (Map.Entry<String, AccumulatorGroup> entry : globalGroups.entrySet()) {
        VarcharType.VARCHAR.writeSlice(builders[0], Slices.utf8Slice(entry.getKey()));
        AccumulatorGroup accGroup = entry.getValue();
        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[1 + a]);
        }
      }

      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
      return List.of(new Page(blocks));
    }
  }

  /** Check if a type is a numeric or timestamp type suitable for group-by keys. */
  private static boolean isNumericOrTimestamp(Type type) {
    return type instanceof BigintType
        || type instanceof IntegerType
        || type instanceof SmallintType
        || type instanceof TinyintType
        || type instanceof DoubleType
        || type instanceof TimestampType
        || type instanceof BooleanType;
  }

  /**
   * Execute the fused GROUP BY aggregation with a top-N selection. After aggregation, selects only
   * the top-N groups by the specified aggregate column value, avoiding full ordinal resolution and
   * Page construction for high-cardinality GROUP BY queries with ORDER BY + LIMIT (e.g., Q17/Q18).
   *
   * @param aggNode the aggregation plan node
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @param columnTypeMap type mapping for columns
   * @param sortAggIndex index of the aggregate column to sort by (0-based within agg columns), or
   *     -1 to disable top-N
   * @param sortAscending true for ASC, false for DESC sort direction
   * @param topN number of top groups to return, or 0 to return all
   * @return a list containing a single Page with the top-N aggregated results
   */
  public static List<Page> executeWithTopN(
      AggregationNode aggNode,
      IndexShard shard,
      Query query,
      Map<String, Type> columnTypeMap,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {
    return executeInternal(aggNode, shard, query, columnTypeMap, sortAggIndex, sortAscending, topN);
  }

  /**
   * Execute the fused GROUP BY aggregation directly from Lucene DocValues. For keys that include
   * VARCHAR columns, uses SortedSetDocValues ordinals as hash keys during per-segment aggregation,
   * then resolves ordinals to strings across segments. For numeric-only keys, aggregates directly
   * into a global map without ordinal resolution.
   *
   * @param aggNode the aggregation plan node
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @param columnTypeMap type mapping for columns
   * @return a list containing a single Page with the aggregated results
   */
  public static List<Page> execute(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {
    return executeInternal(aggNode, shard, query, columnTypeMap, -1, false, 0);
  }

  /** Internal implementation shared by execute() and executeWithTopN(). */
  private static List<Page> executeInternal(
      AggregationNode aggNode,
      IndexShard shard,
      Query query,
      Map<String, Type> columnTypeMap,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {
    TableScanNode scanNode = findChildTableScan(aggNode);
    List<String> groupByKeys = aggNode.getGroupByKeys();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Parse aggregate specs
    List<AggSpec> specs = new ArrayList<>();
    for (String func : aggFunctions) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        throw new IllegalArgumentException("Unsupported aggregate: " + func);
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();
      Type argType = arg.equals("*") ? null : columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      specs.add(new AggSpec(funcName, isDistinct, arg, argType));
    }

    // Classify group-by keys, detecting DATE_TRUNC expressions
    List<KeyInfo> keyInfos = new ArrayList<>();
    boolean hasVarchar = false;
    for (String key : groupByKeys) {
      Type type = columnTypeMap.get(key);
      if (type != null) {
        // Plain column reference
        boolean isVarchar = type instanceof VarcharType;
        keyInfos.add(new KeyInfo(key, type, isVarchar, null, null));
        if (isVarchar) {
          hasVarchar = true;
        }
      } else {
        // Check for DATE_TRUNC expression
        Matcher dtm = DATE_TRUNC_PATTERN.matcher(key);
        if (dtm.matches()) {
          String unit = dtm.group(1).toLowerCase(Locale.ROOT);
          String sourceCol = dtm.group(2);
          // Output type is TimestampType, source column is read from DocValues
          keyInfos.add(
              new KeyInfo(sourceCol, TimestampType.TIMESTAMP_MILLIS, false, "date_trunc", unit));
        } else {
          // Check for EXTRACT expression: EXTRACT(FIELD FROM col)
          Matcher em = EXTRACT_PATTERN.matcher(key);
          if (em.matches()) {
            String field = em.group(1).toUpperCase(Locale.ROOT);
            String sourceCol = em.group(2);
            // Output type is BigintType (extract returns an integer)
            keyInfos.add(new KeyInfo(sourceCol, BigintType.BIGINT, false, "extract", field));
          } else {
            // Check for arithmetic expression: (col OP constant)
            Matcher am = ARITH_EXPR_PATTERN.matcher(key);
            if (am.matches()) {
              String sourceCol = am.group(1);
              String op = am.group(2);
              String constant = am.group(3);
              // Use BigintType for arithmetic output to match Trino's expectations
              // (arithmetic on integers produces long in the Trino execution model)
              keyInfos.add(
                  new KeyInfo(sourceCol, BigintType.BIGINT, false, "arith", op + ":" + constant));
            } else if (aggNode.getChild() instanceof EvalNode evalNode
                && evalNode.getOutputColumnNames().contains(key)) {
              // EvalNode-computed key (e.g., CASE WHEN expression).
              // Mark as "eval" expression — will be compiled and evaluated per-document.
              // Determine the output type by compiling the expression.
              int exprIdx = evalNode.getOutputColumnNames().indexOf(key);
              String exprStr = evalNode.getExpressions().get(exprIdx);
              Type evalOutputType = compileAndGetType(exprStr, columnTypeMap);
              boolean isEvalVarchar = evalOutputType instanceof VarcharType;
              // Use the expression key name (not a physical column name) with "eval" exprFunc.
              // The exprUnit carries the expression index in the EvalNode.
              keyInfos.add(
                  new KeyInfo(key, evalOutputType, isEvalVarchar, "eval", String.valueOf(exprIdx)));
              if (isEvalVarchar) {
                hasVarchar = true;
              }
            } else {
              throw new IllegalArgumentException("Unsupported group-by expression: " + key);
            }
          }
        }
      }
    }

    // Dispatch to specialized path based on key types
    if (hasVarchar) {
      // Fast path: single VARCHAR key with COUNT(*) only — uses ordinal array directly,
      // avoiding MergedGroupKey wrapper and AccumulatorGroup object allocation per group.
      if (keyInfos.size() == 1
          && keyInfos.get(0).isVarchar
          && specs.size() == 1
          && "COUNT".equals(specs.get(0).funcName)
          && "*".equals(specs.get(0).arg)) {
        return executeSingleVarcharCountStar(
            shard,
            query,
            keyInfos.get(0).name,
            columnTypeMap,
            groupByKeys,
            sortAggIndex,
            sortAscending,
            topN);
      }
      // Fast path: single VARCHAR key with general aggregates — uses ordinal-indexed
      // AccumulatorGroup array, eliminating HashMap lookups for single-segment case.
      if (keyInfos.size() == 1 && keyInfos.get(0).isVarchar) {
        return executeSingleVarcharGeneric(
            shard,
            query,
            keyInfos,
            specs,
            columnTypeMap,
            groupByKeys,
            sortAggIndex,
            sortAscending,
            topN);
      }
      // Check if any key is an eval expression — use eval-aware path
      boolean hasEvalKey = false;
      for (KeyInfo ki : keyInfos) {
        if ("eval".equals(ki.exprFunc)) {
          hasEvalKey = true;
          break;
        }
      }
      if (hasEvalKey) {
        return executeWithEvalKeys(
            aggNode, shard, query, keyInfos, specs, columnTypeMap, groupByKeys);
      }
      return executeWithVarcharKeys(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN);
    } else {
      return executeNumericOnly(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN);
    }
  }

  /**
   * Apply DATE_TRUNC transformation to an epoch-millis value. Truncates to the specified unit
   * boundary directly in millis, avoiding ZonedDateTime allocation for the common 'minute' case.
   *
   * @param millis epoch milliseconds from DocValues
   * @param unit the truncation unit (second, minute, hour, day, month, year)
   * @return truncated epoch milliseconds
   */
  private static long truncateMillis(long millis, String unit) {
    switch (unit) {
      case "second":
        return millis / 1000 * 1000;
      case "minute":
        return millis / 60_000 * 60_000;
      case "hour":
        return millis / 3_600_000 * 3_600_000;
      case "day":
        return millis / 86_400_000 * 86_400_000;
      case "month":
      case "year":
        // For month/year, fall through to calendar-based truncation
        java.time.Instant instant = java.time.Instant.ofEpochMilli(millis);
        java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneOffset.UTC);
        if ("month".equals(unit)) {
          zdt = zdt.withDayOfMonth(1).toLocalDate().atStartOfDay(java.time.ZoneOffset.UTC);
        } else {
          zdt =
              zdt.withMonth(1)
                  .withDayOfMonth(1)
                  .toLocalDate()
                  .atStartOfDay(java.time.ZoneOffset.UTC);
        }
        return zdt.toInstant().toEpochMilli();
      default:
        return millis;
    }
  }

  /**
   * Apply an arithmetic transformation to a long value from DocValues. The exprUnit encodes the
   * operator and constant as "op:constant" (e.g., "-:1" for "col - 1").
   *
   * @param value raw long value from DocValues
   * @param exprUnit the encoded operator and constant (format: "op:constant")
   * @return transformed value
   */
  private static long applyArith(long value, String exprUnit) {
    // Check for EXTRACT encoding: "E:FIELD_NAME"
    if (exprUnit.charAt(0) == 'E' && exprUnit.charAt(1) == ':') {
      return applyExtract(value, exprUnit.substring(2));
    }
    int colonIdx = exprUnit.indexOf(':');
    char op = exprUnit.charAt(0);
    long constant = Long.parseLong(exprUnit.substring(colonIdx + 1));
    switch (op) {
      case '-':
        return value - constant;
      case '+':
        return value + constant;
      case '*':
        return value * constant;
      case '/':
        return constant != 0 ? value / constant : 0;
      default:
        return value;
    }
  }

  /**
   * Extract a date/time field from a timestamp value stored as epoch milliseconds (OpenSearch
   * DocValues format for date fields). Uses arithmetic on epoch millis to avoid object allocation
   * in the hot aggregation loop. For common fields (MINUTE, HOUR, SECOND), the computation is pure
   * arithmetic. For calendar-aware fields (MONTH, YEAR, DOW, WEEK), falls back to java.time
   * conversion.
   *
   * @param epochMillis the timestamp value in milliseconds since epoch (DocValues format)
   * @param field the field name to extract (e.g., "MINUTE", "HOUR", "YEAR")
   * @return the extracted field value as a long
   */
  private static long applyExtract(long epochMillis, String field) {
    // Fast arithmetic path for common time-of-day fields
    long epochSeconds = epochMillis / 1000;
    switch (field) {
      case "SECOND":
        // Seconds within the minute (0-59)
        return ((epochSeconds % 60) + 60) % 60;
      case "MINUTE":
        // Minutes within the hour (0-59)
        return (((epochSeconds / 60) % 60) + 60) % 60;
      case "HOUR":
        // Hour within the day (0-23)
        return (((epochSeconds / 3600) % 24) + 24) % 24;
      default:
        // Fall back to java.time for calendar-aware fields
        java.time.Instant instant = java.time.Instant.ofEpochMilli(epochMillis);
        java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneOffset.UTC);
        switch (field) {
          case "YEAR":
            return zdt.getYear();
          case "QUARTER":
            return (zdt.getMonthValue() - 1) / 3 + 1;
          case "MONTH":
            return zdt.getMonthValue();
          case "WEEK":
            return zdt.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
          case "DAY":
          case "DAY_OF_MONTH":
            return zdt.getDayOfMonth();
          case "DAY_OF_WEEK":
          case "DOW":
            return zdt.getDayOfWeek().getValue();
          case "DAY_OF_YEAR":
          case "DOY":
            return zdt.getDayOfYear();
          default:
            return 0;
        }
    }
  }

  /**
   * Ultra-fast path for single VARCHAR key with COUNT(*). Uses per-segment ordinal arrays for
   * counting. When the index has a single segment (common for small-to-medium indices), the ordinal
   * array IS the final result — ordinals are resolved to strings only when building the output
   * Page, completely avoiding the intermediate HashMap&lt;String, long&gt; and all String
   * allocations during aggregation. For multi-segment indices, falls back to a byte[]-based HashMap
   * that avoids the expensive UTF-8 to Java char[] round-trip.
   *
   * <p>When sortAggIndex >= 0 and topN > 0, applies top-N selection directly on the ordinal count
   * array, avoiding BlockBuilder construction for all groups. Critical for high-cardinality VARCHAR
   * GROUP BY with small LIMIT (e.g., Q34: ~700K URL groups with LIMIT 10).
   */
  private static List<Page> executeSingleVarcharCountStar(
      IndexShard shard,
      Query query,
      String columnName,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-varchar-count")) {

      // Check if we have a single segment — enables the zero-string-allocation fast path
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      if (leaves.size() == 1) {
        // === Single-segment fast path ===
        // Count per-ordinal in a long array, then build output Page directly from ordinals.
        // No HashMap, no String allocation during aggregation.
        // Use 10M ordinal limit (80MB memory) — covers all practical single-segment indices.
        LeafReaderContext leafCtx = leaves.get(0);
        LeafReader reader = leafCtx.reader();
        SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
        long ordCount = (dv != null) ? dv.getValueCount() : 0;

        if (ordCount > 0 && ordCount <= 10_000_000) {
          long[] ordCounts = new long[(int) ordCount];

          // === MatchAllDocsQuery fast path: iterate docs directly ===
          if (query instanceof MatchAllDocsQuery) {
            Bits liveDocs = reader.getLiveDocs();
            if (liveDocs == null) {
              // Ultra-fast path: use forward-only nextDoc() iteration.
              // For single-valued fields (common for keyword columns like URL),
              // unwrap to SortedDocValues and use ordValue() which avoids the
              // multi-value ordinal state tracking overhead of nextOrd().
              SortedDocValues sdv = DocValues.unwrapSingleton(dv);
              if (sdv != null) {
                while (sdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                  ordCounts[sdv.ordValue()]++;
                }
              } else {
                while (dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                  ordCounts[(int) dv.nextOrd()]++;
                }
              }
            } else {
              int maxDoc = reader.maxDoc();
              for (int doc = 0; doc < maxDoc; doc++) {
                if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                  ordCounts[(int) dv.nextOrd()]++;
                }
              }
            }
          } else {
            // General path: use Lucene's search framework with Collector
            engineSearcher.search(
                query,
                new Collector() {
                  @Override
                  public LeafCollector getLeafCollector(LeafReaderContext context)
                      throws IOException {
                    return new LeafCollector() {
                      @Override
                      public void setScorer(Scorable scorer) {}

                      @Override
                      public void collect(int doc) throws IOException {
                        if (dv.advanceExact(doc)) {
                          ordCounts[(int) dv.nextOrd()]++;
                        }
                      }
                    };
                  }

                  @Override
                  public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                  }
                });
          }

          // Count non-zero entries first to size builders correctly
          int groupCount = 0;
          for (int i = 0; i < ordCounts.length; i++) {
            if (ordCounts[i] > 0) groupCount++;
          }
          if (groupCount == 0) return List.of();

          // === Top-N selection: build only top-N entries instead of all groups ===
          if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
            int n = (int) Math.min(topN, groupCount);
            // Min-heap (for DESC) or max-heap (for ASC) of ordinal indices
            int[] heap = new int[n];
            long[] heapVals = new long[n];
            int heapSize = 0;

            for (int i = 0; i < ordCounts.length; i++) {
              if (ordCounts[i] == 0) continue;
              long cnt = ordCounts[i];
              if (heapSize < n) {
                heap[heapSize] = i;
                heapVals[heapSize] = cnt;
                heapSize++;
                // Sift up
                int k = heapSize - 1;
                while (k > 0) {
                  int parent = (k - 1) >>> 1;
                  boolean swap =
                      sortAscending
                          ? (heapVals[k] > heapVals[parent])
                          : (heapVals[k] < heapVals[parent]);
                  if (swap) {
                    int tmpI = heap[parent];
                    heap[parent] = heap[k];
                    heap[k] = tmpI;
                    long tmpV = heapVals[parent];
                    heapVals[parent] = heapVals[k];
                    heapVals[k] = tmpV;
                    k = parent;
                  } else break;
                }
              } else {
                boolean better = sortAscending ? (cnt < heapVals[0]) : (cnt > heapVals[0]);
                if (better) {
                  heap[0] = i;
                  heapVals[0] = cnt;
                  // Sift down
                  int k = 0;
                  while (true) {
                    int left = 2 * k + 1;
                    if (left >= heapSize) break;
                    int right = left + 1;
                    int target = left;
                    if (right < heapSize) {
                      boolean pickRight =
                          sortAscending
                              ? (heapVals[right] > heapVals[left])
                              : (heapVals[right] < heapVals[left]);
                      if (pickRight) target = right;
                    }
                    boolean swap =
                        sortAscending
                            ? (heapVals[target] > heapVals[k])
                            : (heapVals[target] < heapVals[k]);
                    if (swap) {
                      int tmpI = heap[k];
                      heap[k] = heap[target];
                      heap[target] = tmpI;
                      long tmpV = heapVals[k];
                      heapVals[k] = heapVals[target];
                      heapVals[target] = tmpV;
                      k = target;
                    } else break;
                  }
                }
              }
            }

            BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, heapSize);
            BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, heapSize);
            for (int h = 0; h < heapSize; h++) {
              int ord = heap[h];
              BytesRef bytes = dv.lookupOrd(ord);
              VarcharType.VARCHAR.writeSlice(
                  keyBuilder, Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              BigintType.BIGINT.writeLong(countBuilder, ordCounts[ord]);
            }
            return List.of(new Page(keyBuilder.build(), countBuilder.build()));
          }

          // No top-N: output all groups
          BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
          BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, groupCount);
          for (int i = 0; i < ordCounts.length; i++) {
            if (ordCounts[i] > 0) {
              BytesRef bytes = dv.lookupOrd(i);
              // wrappedBuffer is safe: writeSlice copies bytes into BlockBuilder's buffer
              VarcharType.VARCHAR.writeSlice(
                  keyBuilder, Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              BigintType.BIGINT.writeLong(countBuilder, ordCounts[i]);
            }
          }
          return List.of(new Page(keyBuilder.build(), countBuilder.build()));
        }
        // Fall through to multi-segment path for very large ordinal spaces
      }

      // === Multi-segment path ===
      // Use HashMap<BytesRefKey, long[]> to avoid String allocation during cross-segment merge.
      HashMap<BytesRefKey, long[]> globalCounts = new HashMap<>();

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              SortedSetDocValues dv = context.reader().getSortedSetDocValues(columnName);
              long ordCount = (dv != null) ? dv.getValueCount() : 0;
              long[] ordCounts =
                  (ordCount > 0 && ordCount <= 1_000_000) ? new long[(int) ordCount] : null;
              HashMap<Long, long[]> ordMap = (ordCounts == null) ? new HashMap<>() : null;

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  if (dv != null && dv.advanceExact(doc)) {
                    long ord = dv.nextOrd();
                    if (ordCounts != null) {
                      ordCounts[(int) ord]++;
                    } else {
                      ordMap.merge(
                          ord,
                          new long[] {1},
                          (a, b) -> {
                            a[0]++;
                            return a;
                          });
                    }
                  }
                }

                @Override
                public void finish() throws IOException {
                  if (dv == null) return;
                  if (ordCounts != null) {
                    for (int i = 0; i < ordCounts.length; i++) {
                      if (ordCounts[i] > 0) {
                        BytesRef bytes = dv.lookupOrd(i);
                        BytesRefKey key = new BytesRefKey(bytes);
                        long[] existing = globalCounts.get(key);
                        if (existing == null) {
                          globalCounts.put(key, new long[] {ordCounts[i]});
                        } else {
                          existing[0] += ordCounts[i];
                        }
                      }
                    }
                  } else {
                    for (Map.Entry<Long, long[]> entry : ordMap.entrySet()) {
                      BytesRef bytes = dv.lookupOrd(entry.getKey());
                      BytesRefKey key = new BytesRefKey(bytes);
                      long[] existing = globalCounts.get(key);
                      if (existing == null) {
                        globalCounts.put(key, new long[] {entry.getValue()[0]});
                      } else {
                        existing[0] += entry.getValue()[0];
                      }
                    }
                  }
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });

      if (globalCounts.isEmpty()) {
        return List.of();
      }

      int groupCount = globalCounts.size();
      BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
      BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, groupCount);

      for (Map.Entry<BytesRefKey, long[]> entry : globalCounts.entrySet()) {
        VarcharType.VARCHAR.writeSlice(keyBuilder, Slices.wrappedBuffer(entry.getKey().bytes));
        BigintType.BIGINT.writeLong(countBuilder, entry.getValue()[0]);
      }

      return List.of(new Page(keyBuilder.build(), countBuilder.build()));
    }
  }

  /**
   * Specialized fast path for single VARCHAR key with arbitrary aggregates. Uses ordinal-indexed
   * AccumulatorGroup[] array for single-segment indices, eliminating all HashMap lookups during
   * aggregation. For multi-segment indices, uses BytesRefKey-based HashMap for cross-segment merge.
   *
   * <p>This is the generalization of {@link #executeSingleVarcharCountStar} to support any
   * aggregate function combination (SUM, MIN, MAX, AVG, COUNT(DISTINCT), etc.) while still
   * leveraging the ordinal-based fast path. For single-segment indices with &lt;= 10M ordinals, the
   * per-doc cost is just: advanceExact + nextOrd + array index — no hash computation, no key
   * comparison.
   *
   * <p>Uses pre-computed dispatch flags (accType[], isCountStar[], etc.) for inline switch-based
   * accumulation, eliminating the instanceof chain in accumulateDoc().
   */
  private static List<Page> executeSingleVarcharGeneric(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    final String columnName = keyInfos.get(0).name();

    // Pre-compute aggregate dispatch types
    final int numAggs = specs.size();
    final boolean[] isCountStar = new boolean[numAggs];
    final boolean[] isDoubleArg = new boolean[numAggs];
    final boolean[] isVarcharArg = new boolean[numAggs];
    final int[] accType = new int[numAggs];
    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      isDoubleArg[i] = spec.argType instanceof DoubleType;
      isVarcharArg[i] = spec.argType instanceof VarcharType;
      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        switch (spec.funcName) {
          case "COUNT":
            accType[i] = spec.isDistinct ? 5 : 0;
            break;
          case "SUM":
            accType[i] = isDoubleArg[i] ? 6 : 1;
            break;
          case "AVG":
            accType[i] = isDoubleArg[i] ? 7 : 2;
            break;
          case "MIN":
            accType[i] = 3;
            break;
          case "MAX":
            accType[i] = 4;
            break;
        }
      }
    }

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-varchar-generic")) {

      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      if (leaves.size() == 1) {
        // === Single-segment fast path ===
        // Use ordinal-indexed AccumulatorGroup array — no HashMap, no hash computation.
        LeafReaderContext leafCtx = leaves.get(0);
        LeafReader reader = leafCtx.reader();
        SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
        long ordCountLong = (dv != null) ? dv.getValueCount() : 0;

        if (ordCountLong > 0 && ordCountLong <= 10_000_000) {
          AccumulatorGroup[] ordGroups = new AccumulatorGroup[(int) ordCountLong];

          // Open agg doc values with typed arrays
          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              AggSpec spec = specs.get(i);
              if (isVarcharArg[i]) {
                varcharAggDvs[i] = reader.getSortedSetDocValues(spec.arg);
              } else {
                numericAggDvs[i] = reader.getSortedNumericDocValues(spec.arg);
              }
            }
          }

          // === MatchAllDocsQuery fast path: iterate docs directly ===
          if (query instanceof MatchAllDocsQuery) {
            Bits liveDocs = reader.getLiveDocs();
            if (liveDocs == null && dv != null) {
              // Ultra-fast path: use forward-only nextDoc() iteration.
              // For single-valued fields, unwrap to SortedDocValues and use
              // ordValue() for less overhead than nextOrd().
              SortedDocValues sdv = DocValues.unwrapSingleton(dv);
              if (sdv != null) {
                int doc;
                while ((doc = sdv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  int ord = sdv.ordValue();
                  AccumulatorGroup accGroup = ordGroups[ord];
                  if (accGroup == null) {
                    accGroup = createAccumulatorGroup(specs);
                    ordGroups[ord] = accGroup;
                  }
                  collectVarcharGenericAccumulate(
                      doc,
                      accGroup,
                      numAggs,
                      isCountStar,
                      isVarcharArg,
                      isDoubleArg,
                      accType,
                      numericAggDvs,
                      varcharAggDvs);
                }
              } else {
                int doc;
                while ((doc = dv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  int ord = (int) dv.nextOrd();
                  AccumulatorGroup accGroup = ordGroups[ord];
                  if (accGroup == null) {
                    accGroup = createAccumulatorGroup(specs);
                    ordGroups[ord] = accGroup;
                  }
                  collectVarcharGenericAccumulate(
                      doc,
                      accGroup,
                      numAggs,
                      isCountStar,
                      isVarcharArg,
                      isDoubleArg,
                      accType,
                      numericAggDvs,
                      varcharAggDvs);
                }
              }
            } else {
              int maxDoc = reader.maxDoc();
              for (int doc = 0; doc < maxDoc; doc++) {
                if (liveDocs != null && !liveDocs.get(doc)) continue;
                if (dv == null || !dv.advanceExact(doc)) continue;
                int ord = (int) dv.nextOrd();
                AccumulatorGroup accGroup = ordGroups[ord];
                if (accGroup == null) {
                  accGroup = createAccumulatorGroup(specs);
                  ordGroups[ord] = accGroup;
                }
                collectVarcharGenericAccumulate(
                    doc,
                    accGroup,
                    numAggs,
                    isCountStar,
                    isVarcharArg,
                    isDoubleArg,
                    accType,
                    numericAggDvs,
                    varcharAggDvs);
              }
            }
          } else {
            // General path: use Lucene's search framework with Collector
            engineSearcher.search(
                query,
                new Collector() {
                  @Override
                  public LeafCollector getLeafCollector(LeafReaderContext context)
                      throws IOException {
                    return new LeafCollector() {
                      @Override
                      public void setScorer(Scorable scorer) {}

                      @Override
                      public void collect(int doc) throws IOException {
                        if (dv == null || !dv.advanceExact(doc)) return;
                        int ord = (int) dv.nextOrd();
                        AccumulatorGroup accGroup = ordGroups[ord];
                        if (accGroup == null) {
                          accGroup = createAccumulatorGroup(specs);
                          ordGroups[ord] = accGroup;
                        }
                        collectVarcharGenericAccumulate(
                            doc,
                            accGroup,
                            numAggs,
                            isCountStar,
                            isVarcharArg,
                            isDoubleArg,
                            accType,
                            numericAggDvs,
                            varcharAggDvs);
                      }
                    };
                  }

                  @Override
                  public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                  }
                });
          }

          // Count non-null entries
          int groupCount = 0;
          for (int i = 0; i < ordGroups.length; i++) {
            if (ordGroups[i] != null) groupCount++;
          }
          if (groupCount == 0) return List.of();

          int numGroupKeys = groupByKeys.size();
          int totalColumns = numGroupKeys + numAggs;

          // === Top-N selection: build only top-N entries ===
          if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
            int n = (int) Math.min(topN, groupCount);
            int[] heap = new int[n];
            long[] heapVals = new long[n];
            int heapSize = 0;

            for (int i = 0; i < ordGroups.length; i++) {
              if (ordGroups[i] == null) continue;
              long val = ordGroups[i].accumulators[sortAggIndex].getSortValue();
              if (heapSize < n) {
                heap[heapSize] = i;
                heapVals[heapSize] = val;
                heapSize++;
                int k = heapSize - 1;
                while (k > 0) {
                  int parent = (k - 1) >>> 1;
                  boolean swap =
                      sortAscending
                          ? (heapVals[k] > heapVals[parent])
                          : (heapVals[k] < heapVals[parent]);
                  if (swap) {
                    int tmpI = heap[parent];
                    heap[parent] = heap[k];
                    heap[k] = tmpI;
                    long tmpV = heapVals[parent];
                    heapVals[parent] = heapVals[k];
                    heapVals[k] = tmpV;
                    k = parent;
                  } else break;
                }
              } else {
                boolean better = sortAscending ? (val < heapVals[0]) : (val > heapVals[0]);
                if (better) {
                  heap[0] = i;
                  heapVals[0] = val;
                  int k = 0;
                  while (true) {
                    int left = 2 * k + 1;
                    if (left >= heapSize) break;
                    int right = left + 1;
                    int target = left;
                    if (right < heapSize) {
                      boolean pickRight =
                          sortAscending
                              ? (heapVals[right] > heapVals[left])
                              : (heapVals[right] < heapVals[left]);
                      if (pickRight) target = right;
                    }
                    boolean swap =
                        sortAscending
                            ? (heapVals[target] > heapVals[k])
                            : (heapVals[target] < heapVals[k]);
                    if (swap) {
                      int tmpI = heap[k];
                      heap[k] = heap[target];
                      heap[target] = tmpI;
                      long tmpV = heapVals[k];
                      heapVals[k] = heapVals[target];
                      heapVals[target] = tmpV;
                      k = target;
                    } else break;
                  }
                }
              }
            }

            BlockBuilder[] builders = new BlockBuilder[totalColumns];
            builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, heapSize);
            for (int i = 0; i < numAggs; i++) {
              builders[numGroupKeys + i] =
                  resolveAggOutputType(specs.get(i), columnTypeMap)
                      .createBlockBuilder(null, heapSize);
            }
            for (int h = 0; h < heapSize; h++) {
              int ord = heap[h];
              BytesRef bytes = dv.lookupOrd(ord);
              VarcharType.VARCHAR.writeSlice(
                  builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              AccumulatorGroup accGroup = ordGroups[ord];
              for (int a = 0; a < numAggs; a++) {
                accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
              }
            }
            Block[] blocks = new Block[totalColumns];
            for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
            return List.of(new Page(blocks));
          }

          // No top-N: output all groups
          BlockBuilder[] builders = new BlockBuilder[totalColumns];
          builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
          for (int i = 0; i < numAggs; i++) {
            builders[numGroupKeys + i] =
                resolveAggOutputType(specs.get(i), columnTypeMap)
                    .createBlockBuilder(null, groupCount);
          }

          for (int i = 0; i < ordGroups.length; i++) {
            if (ordGroups[i] != null) {
              BytesRef bytes = dv.lookupOrd(i);
              VarcharType.VARCHAR.writeSlice(
                  builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              AccumulatorGroup accGroup = ordGroups[i];
              for (int a = 0; a < numAggs; a++) {
                accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
              }
            }
          }

          Block[] blocks = new Block[totalColumns];
          for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
          return List.of(new Page(blocks));
        }
        // Fall through to multi-segment path
      }

      // === Multi-segment path ===
      // Use BytesRefKey-based HashMap for cross-segment merge
      HashMap<BytesRefKey, AccumulatorGroup> globalGroups = new HashMap<>();

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              SortedSetDocValues dv = context.reader().getSortedSetDocValues(columnName);
              long ordCount = (dv != null) ? dv.getValueCount() : 0;
              AccumulatorGroup[] ordGroups =
                  (ordCount > 0 && ordCount <= 1_000_000)
                      ? new AccumulatorGroup[(int) ordCount]
                      : null;

              // Open agg doc values with typed arrays
              final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
              final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i]) {
                  AggSpec spec = specs.get(i);
                  if (isVarcharArg[i]) {
                    varcharAggDvs[i] = context.reader().getSortedSetDocValues(spec.arg);
                  } else {
                    numericAggDvs[i] = context.reader().getSortedNumericDocValues(spec.arg);
                  }
                }
              }

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  if (dv == null || !dv.advanceExact(doc)) return;
                  int ord = (int) dv.nextOrd();
                  AccumulatorGroup accGroup;
                  if (ordGroups != null) {
                    accGroup = ordGroups[ord];
                    if (accGroup == null) {
                      accGroup = createAccumulatorGroup(specs);
                      ordGroups[ord] = accGroup;
                    }
                  } else {
                    // Fallback for very large ordinal spaces - should not happen in practice
                    return;
                  }
                  collectVarcharGenericAccumulate(
                      doc,
                      accGroup,
                      numAggs,
                      isCountStar,
                      isVarcharArg,
                      isDoubleArg,
                      accType,
                      numericAggDvs,
                      varcharAggDvs);
                }

                @Override
                public void finish() throws IOException {
                  if (dv == null) return;
                  if (ordGroups != null) {
                    for (int i = 0; i < ordGroups.length; i++) {
                      if (ordGroups[i] != null) {
                        BytesRef bytes = dv.lookupOrd(i);
                        BytesRefKey key = new BytesRefKey(bytes);
                        AccumulatorGroup existing = globalGroups.get(key);
                        if (existing == null) {
                          globalGroups.put(key, ordGroups[i]);
                        } else {
                          existing.merge(ordGroups[i]);
                        }
                      }
                    }
                  }
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });

      if (globalGroups.isEmpty()) {
        return List.of();
      }

      int numGroupKeys = groupByKeys.size();
      int totalColumns = numGroupKeys + numAggs;
      int groupCount = globalGroups.size();
      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
      for (int i = 0; i < numAggs; i++) {
        builders[numGroupKeys + i] =
            resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
      }

      for (Map.Entry<BytesRefKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
        VarcharType.VARCHAR.writeSlice(builders[0], Slices.wrappedBuffer(entry.getKey().bytes));
        AccumulatorGroup accGroup = entry.getValue();
        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
        }
      }

      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
      return List.of(new Page(blocks));
    }
  }

  /**
   * Fast path for numeric-only GROUP BY keys. Since all keys are raw long values (no segment-local
   * ordinals), we aggregate directly into a global map across all segments without
   * per-segment/cross-segment merge overhead. Supports DATE_TRUNC expressions on timestamp columns
   * by applying truncation during key value extraction.
   *
   * <p>Uses a reusable {@link NumericProbeKey} for HashMap lookups to avoid per-doc allocation. An
   * immutable {@link SegmentGroupKey} is only created when inserting a new group (cache miss),
   * eliminating ~4 array allocations per doc for the common case where the group already exists.
   *
   * <p>For exactly two numeric keys, dispatches to {@link #executeTwoKeyNumeric} which uses an
   * open-addressing hash map with parallel arrays, eliminating all per-group object allocation.
   */
  private static List<Page> executeNumericOnly(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    // Pre-compute which keys need DATE_TRUNC, arithmetic, or EXTRACT transformation
    final String[] truncUnits = new String[keyInfos.size()];
    final String[] arithUnits = new String[keyInfos.size()];
    boolean hasExpr = false;
    for (int i = 0; i < keyInfos.size(); i++) {
      KeyInfo ki = keyInfos.get(i);
      if ("date_trunc".equals(ki.exprFunc())) {
        truncUnits[i] = ki.exprUnit();
        hasExpr = true;
      } else if ("arith".equals(ki.exprFunc())) {
        arithUnits[i] = ki.exprUnit();
        hasExpr = true;
      } else if ("extract".equals(ki.exprFunc())) {
        // Encode EXTRACT as arithmetic with "E:" prefix for the field name.
        // applyArith detects this prefix and delegates to applyExtract.
        arithUnits[i] = "E:" + ki.exprUnit();
        hasExpr = true;
      }
    }
    final boolean anyTrunc = hasExpr;

    // Fast path: single numeric key with no expressions — uses open-addressing hash map
    // with single long key array, eliminating SegmentGroupKey, NumericProbeKey, and HashMap.Entry
    // allocation per group. Particularly effective for COUNT(DISTINCT) per group (Q9).
    if (keyInfos.size() == 1 && !anyTrunc) {
      return executeSingleKeyNumeric(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN);
    }

    // Fast path: two numeric keys with no expressions — uses open-addressing hash map
    // with parallel arrays, eliminating SegmentGroupKey, AccumulatorGroup, and HashMap.Entry
    // allocation per group. This is ~2x faster for high-cardinality two-key GROUP BY (Q31/Q32).
    if (keyInfos.size() == 2 && !anyTrunc) {
      return executeTwoKeyNumeric(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN);
    }

    // Fast path: all keys derive from the same single column (e.g., Q36:
    // GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3). Since the derived keys
    // are deterministic functions of the source column, this is really a single-key GROUP BY.
    // We GROUP BY only the source column and compute derived keys at output time.
    if (anyTrunc && keyInfos.size() > 1) {
      String sharedCol = keyInfos.get(0).name();
      boolean allSameCol = true;
      boolean allArithOrPlain = true;
      for (int i = 0; i < keyInfos.size(); i++) {
        KeyInfo ki = keyInfos.get(i);
        if (!sharedCol.equals(ki.name())) {
          allSameCol = false;
          break;
        }
        // Each key must be either plain (no expression) or arith
        if (ki.exprFunc() != null && !"arith".equals(ki.exprFunc())) {
          allArithOrPlain = false;
          break;
        }
      }
      if (allSameCol && allArithOrPlain) {
        return executeDerivedSingleKeyNumeric(
            shard, query, keyInfos, specs, columnTypeMap, groupByKeys, arithUnits);
      }
    }

    // Global map: SegmentGroupKey (long[]) -> AccumulatorGroup
    // For numeric-only keys, long values are globally unique (not ordinals),
    // so we can accumulate directly without per-segment resolution.
    Map<SegmentGroupKey, AccumulatorGroup> globalGroups = new HashMap<>();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric")) {

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              final int numKeys = keyInfos.size();

              // Open doc values for group-by keys (all numeric)
              SortedNumericDocValues[] keyReaders = new SortedNumericDocValues[numKeys];
              for (int i = 0; i < numKeys; i++) {
                keyReaders[i] = context.reader().getSortedNumericDocValues(keyInfos.get(i).name());
              }

              // Open doc values for aggregate arguments
              Object[] aggReaders = new Object[specs.size()];
              for (int i = 0; i < specs.size(); i++) {
                AggSpec spec = specs.get(i);
                if ("*".equals(spec.arg)) {
                  aggReaders[i] = null;
                } else if (spec.argType instanceof VarcharType) {
                  aggReaders[i] = context.reader().getSortedSetDocValues(spec.arg);
                } else {
                  aggReaders[i] = context.reader().getSortedNumericDocValues(spec.arg);
                }
              }

              // Pre-allocate a reusable probe key to avoid per-doc allocation.
              // The probe key is mutated in-place for each doc and used for HashMap.get().
              // An immutable SegmentGroupKey is only created on cache miss (new group).
              final NumericProbeKey probeKey = new NumericProbeKey(numKeys);

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  // Reset and populate the reusable probe key
                  probeKey.reset();
                  for (int k = 0; k < numKeys; k++) {
                    SortedNumericDocValues dv = keyReaders[k];
                    if (dv != null && dv.advanceExact(doc)) {
                      long val = dv.nextValue();
                      if (anyTrunc) {
                        if (truncUnits[k] != null) {
                          val = truncateMillis(val, truncUnits[k]);
                        } else if (arithUnits[k] != null) {
                          val = applyArith(val, arithUnits[k]);
                        }
                      }
                      probeKey.set(k, val);
                    } else {
                      probeKey.setNull(k);
                    }
                  }
                  probeKey.computeHash();

                  // Fast path: probe with reusable key (no allocation for existing groups)
                  AccumulatorGroup accGroup = globalGroups.get(probeKey);
                  if (accGroup == null) {
                    // Cache miss: create an immutable key copy and new accumulator group
                    SegmentGroupKey immutableKey = probeKey.toImmutableKey();
                    accGroup = createAccumulatorGroup(specs);
                    globalGroups.put(immutableKey, accGroup);
                  }
                  accumulateDoc(doc, accGroup, specs, aggReaders);
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });
    }

    if (globalGroups.isEmpty()) {
      return List.of();
    }

    // Build result Page directly from SegmentGroupKey (no ordinal resolution needed)
    int numGroupKeys = groupByKeys.size();
    int numAggs = specs.size();
    int totalColumns = numGroupKeys + numAggs;

    // If top-N is requested and fewer than total groups, select top-N entries
    java.util.Collection<Map.Entry<SegmentGroupKey, AccumulatorGroup>> outputEntries;
    if (sortAggIndex >= 0 && topN > 0 && topN < globalGroups.size()) {
      int n = (int) Math.min(topN, globalGroups.size());
      // Use a min-heap (for DESC) of map entries
      @SuppressWarnings("unchecked")
      Map.Entry<SegmentGroupKey, AccumulatorGroup>[] heap = new Map.Entry[n];
      int heapSize = 0;
      for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
        long val = entry.getValue().accumulators[sortAggIndex].getSortValue();
        if (heapSize < n) {
          heap[heapSize++] = entry;
          int k = heapSize - 1;
          while (k > 0) {
            int parent = (k - 1) >>> 1;
            long pVal = heap[parent].getValue().accumulators[sortAggIndex].getSortValue();
            long kVal = heap[k].getValue().accumulators[sortAggIndex].getSortValue();
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              var tmp = heap[parent];
              heap[parent] = heap[k];
              heap[k] = tmp;
              k = parent;
            } else break;
          }
        } else {
          long rootVal = heap[0].getValue().accumulators[sortAggIndex].getSortValue();
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = entry;
            int k = 0;
            while (true) {
              int left = 2 * k + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = heap[left].getValue().accumulators[sortAggIndex].getSortValue();
                long rv = heap[right].getValue().accumulators[sortAggIndex].getSortValue();
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = heap[k].getValue().accumulators[sortAggIndex].getSortValue();
              long tVal = heap[target].getValue().accumulators[sortAggIndex].getSortValue();
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                var tmp = heap[k];
                heap[k] = heap[target];
                heap[target] = tmp;
                k = target;
              } else break;
            }
          }
        }
      }
      outputEntries = java.util.Arrays.asList(heap).subList(0, heapSize);
    } else {
      outputEntries = globalGroups.entrySet();
    }

    int outputCount = outputEntries.size();
    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      builders[i] = keyInfos.get(i).type.createBlockBuilder(null, outputCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }

    for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry : outputEntries) {
      SegmentGroupKey key = entry.getKey();
      AccumulatorGroup accGroup = entry.getValue();

      // Write group-by keys (all numeric)
      for (int k = 0; k < numGroupKeys; k++) {
        if (key.nulls[k]) {
          builders[k].appendNull();
        } else {
          writeNumericKeyValue(builders[k], keyInfos.get(k), key.values[k]);
        }
      }

      // Write aggregate results
      for (int a = 0; a < numAggs; a++) {
        accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
      }
    }

    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) {
      blocks[i] = builders[i].build();
    }

    return List.of(new Page(blocks));
  }

  /**
   * Specialized fast path for GROUP BY with exactly one numeric key. Uses an open-addressing hash
   * map with a single long key array and AccumulatorGroup array, eliminating SegmentGroupKey,
   * NumericProbeKey, and HashMap.Entry overhead.
   *
   * <p>For single-key GROUP BY with COUNT(DISTINCT) (Q9: GROUP BY RegionID, COUNT(DISTINCT
   * UserID)), this avoids:
   *
   * <ul>
   *   <li>NumericProbeKey allocation + reset + computeHash per document
   *   <li>SegmentGroupKey wrapping (long[] + boolean[] + hash) per new group
   *   <li>HashMap.Entry per group
   * </ul>
   *
   * <p>The hash function uses the Murmur3 finalizer for good distribution of correlated long keys
   * (e.g., RegionID values that are sequential integers).
   */
  private static List<Page> executeSingleKeyNumeric(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    final int numAggs = specs.size();
    final boolean[] isCountStar = new boolean[numAggs];
    final boolean[] isDoubleArg = new boolean[numAggs];
    final int[] accType = new int[numAggs];
    boolean canUseFlatAccumulators = true;

    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      isDoubleArg[i] = spec.argType instanceof DoubleType;
      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        switch (spec.funcName) {
          case "COUNT":
            if (spec.isDistinct) {
              accType[i] = 5;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 0;
            }
            break;
          case "SUM":
            if (isDoubleArg[i]) {
              accType[i] = 6;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 1;
            }
            break;
          case "AVG":
            if (isDoubleArg[i]) {
              accType[i] = 7;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 2;
            }
            break;
          case "MIN":
            accType[i] = 3;
            canUseFlatAccumulators = false;
            break;
          case "MAX":
            accType[i] = 4;
            canUseFlatAccumulators = false;
            break;
        }
        // VARCHAR args can't use flat accumulators
        if (spec.argType instanceof VarcharType) {
          canUseFlatAccumulators = false;
        }
      }
    }

    // === Flat accumulator path ===
    // For aggregates representable as longs (COUNT(*), SUM long, AVG long), use a flat
    // long[] array per hash map slot, eliminating ALL per-group object allocation.
    // This is critical for high-cardinality GROUP BY (Q16: ~25K groups per shard).
    if (canUseFlatAccumulators) {
      return executeSingleKeyNumericFlat(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          numAggs,
          isCountStar,
          accType,
          sortAggIndex,
          sortAscending,
          topN);
    }

    final SingleKeyHashMap singleKeyMap = new SingleKeyHashMap(specs);

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-1key")) {

      if (query instanceof MatchAllDocsQuery) {
        // Fast path: iterate all docs directly without Collector/Scorer overhead.
        // This eliminates the Lucene search framework cost (~2-4ms for 100M docs).
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          LeafReader reader = leafCtx.reader();
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();
          SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());

          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              AggSpec spec = specs.get(i);
              if (spec.argType instanceof VarcharType) {
                varcharAggDvs[i] = reader.getSortedSetDocValues(spec.arg);
              } else {
                numericAggDvs[i] = reader.getSortedNumericDocValues(spec.arg);
              }
            }
          }

          if (liveDocs == null) {
            for (int doc = 0; doc < maxDoc; doc++) {
              collectSingleKeyNumericDoc(
                  doc,
                  dv0,
                  singleKeyMap,
                  numAggs,
                  isCountStar,
                  isDoubleArg,
                  accType,
                  numericAggDvs,
                  varcharAggDvs);
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc)) {
                collectSingleKeyNumericDoc(
                    doc,
                    dv0,
                    singleKeyMap,
                    numAggs,
                    isCountStar,
                    isDoubleArg,
                    accType,
                    numericAggDvs,
                    varcharAggDvs);
              }
            }
          }
        }
      } else {
        // General path: use Lucene's search framework with Collector
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues dv0 =
                    context.reader().getSortedNumericDocValues(keyInfos.get(0).name());

                final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    AggSpec spec = specs.get(i);
                    if (spec.argType instanceof VarcharType) {
                      varcharAggDvs[i] = context.reader().getSortedSetDocValues(spec.arg);
                    } else {
                      numericAggDvs[i] = context.reader().getSortedNumericDocValues(spec.arg);
                    }
                  }
                }

                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    collectSingleKeyNumericDoc(
                        doc,
                        dv0,
                        singleKeyMap,
                        numAggs,
                        isCountStar,
                        isDoubleArg,
                        accType,
                        numericAggDvs,
                        varcharAggDvs);
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });
      }
    }

    if (singleKeyMap.size == 0) return List.of();

    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Determine which slots to output (with optional top-N selection)
    int[] outputSlots;
    int outputCount;
    if (sortAggIndex >= 0 && topN > 0 && topN < singleKeyMap.size) {
      int n = (int) Math.min(topN, singleKeyMap.size);
      int[] heap = new int[n];
      int heapSize = 0;
      for (int slot = 0; slot < singleKeyMap.capacity; slot++) {
        if (!singleKeyMap.occupied[slot]) continue;
        long val = singleKeyMap.groups[slot].accumulators[sortAggIndex].getSortValue();
        if (heapSize < n) {
          heap[heapSize++] = slot;
          int k = heapSize - 1;
          while (k > 0) {
            int parent = (k - 1) >>> 1;
            long pVal = singleKeyMap.groups[heap[parent]].accumulators[sortAggIndex].getSortValue();
            long kVal = singleKeyMap.groups[heap[k]].accumulators[sortAggIndex].getSortValue();
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              int tmp = heap[parent];
              heap[parent] = heap[k];
              heap[k] = tmp;
              k = parent;
            } else break;
          }
        } else {
          long rootVal = singleKeyMap.groups[heap[0]].accumulators[sortAggIndex].getSortValue();
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = slot;
            int k = 0;
            while (true) {
              int left = 2 * k + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = singleKeyMap.groups[heap[left]].accumulators[sortAggIndex].getSortValue();
                long rv =
                    singleKeyMap.groups[heap[right]].accumulators[sortAggIndex].getSortValue();
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = singleKeyMap.groups[heap[k]].accumulators[sortAggIndex].getSortValue();
              long tVal =
                  singleKeyMap.groups[heap[target]].accumulators[sortAggIndex].getSortValue();
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                int tmp = heap[k];
                heap[k] = heap[target];
                heap[target] = tmp;
                k = target;
              } else break;
            }
          }
        }
      }
      outputSlots = heap;
      outputCount = heapSize;
    } else if (topN > 0 && topN < singleKeyMap.size) {
      int n = (int) Math.min(topN, singleKeyMap.size);
      outputSlots = new int[n];
      int idx = 0;
      for (int slot = 0; slot < singleKeyMap.capacity && idx < n; slot++) {
        if (singleKeyMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[singleKeyMap.size];
      int idx = 0;
      for (int slot = 0; slot < singleKeyMap.capacity; slot++) {
        if (singleKeyMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    }

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    builders[0] = keyInfos.get(0).type.createBlockBuilder(null, outputCount);
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }
    for (int o = 0; o < outputCount; o++) {
      int slot = outputSlots[o];
      writeNumericKeyValue(builders[0], keyInfos.get(0), singleKeyMap.keys[slot]);
      AccumulatorGroup accGroup = singleKeyMap.groups[slot];
      for (int a = 0; a < numAggs; a++) {
        accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /** Build the result Page from a SingleKeyHashMap after aggregation. */
  private static List<Page> buildSingleKeyResult(
      SingleKeyHashMap singleKeyMap,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys) {
    if (singleKeyMap.size == 0) return List.of();
    int numAggs = specs.size();
    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;
    int groupCount = singleKeyMap.size;
    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    builders[0] = keyInfos.get(0).type.createBlockBuilder(null, groupCount);
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
    }
    for (int slot = 0; slot < singleKeyMap.capacity; slot++) {
      if (singleKeyMap.occupied[slot]) {
        writeNumericKeyValue(builders[0], keyInfos.get(0), singleKeyMap.keys[slot]);
        AccumulatorGroup accGroup = singleKeyMap.groups[slot];
        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
        }
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /**
   * Process a single document for the single-key numeric GROUP BY path. Extracted as a helper to
   * share between the MatchAllDocsQuery fast path and the Collector-based general path.
   */
  private static void collectSingleKeyNumericDoc(
      int doc,
      SortedNumericDocValues dv0,
      SingleKeyHashMap singleKeyMap,
      int numAggs,
      boolean[] isCountStar,
      boolean[] isDoubleArg,
      int[] accType,
      SortedNumericDocValues[] numericAggDvs,
      SortedSetDocValues[] varcharAggDvs)
      throws IOException {
    long key0 = 0;
    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
    AccumulatorGroup accGroup = singleKeyMap.getOrCreate(key0);

    for (int i = 0; i < numAggs; i++) {
      MergeableAccumulator acc = accGroup.accumulators[i];
      if (isCountStar[i]) {
        ((CountStarAccum) acc).count++;
        continue;
      }
      if (varcharAggDvs[i] != null) {
        SortedSetDocValues varcharDv = varcharAggDvs[i];
        if (varcharDv.advanceExact(doc)) {
          BytesRef bytes = varcharDv.lookupOrd(varcharDv.nextOrd());
          String val = bytes.utf8ToString();
          if (acc instanceof CountDistinctAccum cda) {
            cda.objectDistinctValues.add(val);
          } else if (acc instanceof MinAccum ma) {
            if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
              ma.objectVal = val;
              ma.hasValue = true;
            }
          } else if (acc instanceof MaxAccum xa) {
            if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
              xa.objectVal = val;
              xa.hasValue = true;
            }
          }
        }
        continue;
      }
      SortedNumericDocValues aggDv = numericAggDvs[i];
      if (aggDv != null && aggDv.advanceExact(doc)) {
        long rawVal = aggDv.nextValue();
        switch (accType[i]) {
          case 0:
            ((CountStarAccum) acc).count++;
            break;
          case 1: // SUM long
            SumAccum sa = (SumAccum) acc;
            sa.hasValue = true;
            sa.longSum += rawVal;
            break;
          case 2: // AVG long
            AvgAccum aa = (AvgAccum) acc;
            aa.count++;
            aa.longSum += rawVal;
            break;
          case 3: // MIN
            MinAccum ma = (MinAccum) acc;
            ma.hasValue = true;
            if (isDoubleArg[i]) {
              double d = Double.longBitsToDouble(rawVal);
              if (d < ma.doubleVal) ma.doubleVal = d;
            } else {
              if (rawVal < ma.longVal) ma.longVal = rawVal;
            }
            break;
          case 4: // MAX
            MaxAccum xa = (MaxAccum) acc;
            xa.hasValue = true;
            if (isDoubleArg[i]) {
              double d = Double.longBitsToDouble(rawVal);
              if (d > xa.doubleVal) xa.doubleVal = d;
            } else {
              if (rawVal > xa.longVal) xa.longVal = rawVal;
            }
            break;
          case 5: // COUNT(DISTINCT)
            CountDistinctAccum cda = (CountDistinctAccum) acc;
            if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
            else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
            break;
          case 6: // SUM double
            SumAccum sad = (SumAccum) acc;
            sad.hasValue = true;
            sad.doubleSum += Double.longBitsToDouble(rawVal);
            break;
          case 7: // AVG double
            AvgAccum aad = (AvgAccum) acc;
            aad.count++;
            aad.doubleSum += Double.longBitsToDouble(rawVal);
            break;
        }
      }
    }
  }

  /**
   * Specialized fast path for GROUP BY with multiple numeric keys that all derive from the same
   * single source column via arithmetic expressions (e.g., Q36: GROUP BY ClientIP, ClientIP-1,
   * ClientIP-2, ClientIP-3). Since the derived keys are deterministic functions of the source
   * column, this reduces to a single-key GROUP BY on the source column. Derived keys are computed
   * at output time only, eliminating the multi-key hash map overhead.
   *
   * <p>Uses the flat single-key map (FlatSingleKeyMap) for maximum performance, avoiding all
   * per-group object allocation. For Q36, this reduces the GROUP BY from 4 keys to 1 key, cutting
   * hash computation and comparison cost by ~4x.
   */
  private static List<Page> executeDerivedSingleKeyNumeric(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      String[] arithUnits)
      throws Exception {

    final int numAggs = specs.size();
    final boolean[] isCountStar = new boolean[numAggs];
    final int[] accType = new int[numAggs];
    boolean canUseFlat = true;

    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        boolean isDoubleArg = spec.argType instanceof DoubleType;
        switch (spec.funcName) {
          case "COUNT":
            accType[i] = spec.isDistinct ? 5 : 0;
            if (spec.isDistinct) canUseFlat = false;
            break;
          case "SUM":
            accType[i] = isDoubleArg ? 6 : 1;
            if (isDoubleArg) canUseFlat = false;
            break;
          case "AVG":
            accType[i] = isDoubleArg ? 7 : 2;
            if (isDoubleArg) canUseFlat = false;
            break;
          case "MIN":
            accType[i] = 3;
            canUseFlat = false;
            break;
          case "MAX":
            accType[i] = 4;
            canUseFlat = false;
            break;
        }
        if (spec.argType instanceof VarcharType) canUseFlat = false;
      }
    }

    if (!canUseFlat) {
      // Fall back to single-key with AccumulatorGroup
      final SingleKeyHashMap singleKeyMap = new SingleKeyHashMap(specs);
      final String sourceCol = keyInfos.get(0).name();

      try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
          shard.acquireSearcher("dqe-fused-groupby-derived-1key")) {

        if (query instanceof MatchAllDocsQuery) {
          for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
            LeafReader reader = leafCtx.reader();
            int maxDoc = reader.maxDoc();
            Bits liveDocs = reader.getLiveDocs();
            SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(sourceCol);

            if (liveDocs == null) {
              for (int doc = 0; doc < maxDoc; doc++) {
                long key0 = 0;
                if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
                AccumulatorGroup accGroup = singleKeyMap.getOrCreate(key0);
                for (int i = 0; i < numAggs; i++) {
                  if (isCountStar[i]) ((CountStarAccum) accGroup.accumulators[i]).count++;
                }
              }
            } else {
              for (int doc = 0; doc < maxDoc; doc++) {
                if (liveDocs.get(doc)) {
                  long key0 = 0;
                  if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
                  AccumulatorGroup accGroup = singleKeyMap.getOrCreate(key0);
                  for (int i = 0; i < numAggs; i++) {
                    if (isCountStar[i]) ((CountStarAccum) accGroup.accumulators[i]).count++;
                  }
                }
              }
            }
          }
        } else {
          engineSearcher.search(
              query,
              new Collector() {
                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context)
                    throws IOException {
                  SortedNumericDocValues dv0 =
                      context.reader().getSortedNumericDocValues(sourceCol);
                  return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) throws IOException {
                      long key0 = 0;
                      if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
                      AccumulatorGroup accGroup = singleKeyMap.getOrCreate(key0);
                      for (int i = 0; i < numAggs; i++) {
                        if (isCountStar[i]) ((CountStarAccum) accGroup.accumulators[i]).count++;
                      }
                    }
                  };
                }

                @Override
                public ScoreMode scoreMode() {
                  return ScoreMode.COMPLETE_NO_SCORES;
                }
              });
        }
      }

      if (singleKeyMap.size == 0) return List.of();

      return buildDerivedKeyResult(
          singleKeyMap, keyInfos, specs, columnTypeMap, groupByKeys, arithUnits);
    }

    // === Flat path (COUNT/SUM long/AVG long only) ===
    final int[] accOffset = new int[numAggs];
    int totalSlots = 0;
    for (int i = 0; i < numAggs; i++) {
      accOffset[i] = totalSlots;
      switch (accType[i]) {
        case 0:
        case 1:
          totalSlots += 1;
          break;
        case 2:
          totalSlots += 2;
          break;
        default:
          throw new IllegalStateException("Flat path used with unsupported accType: " + accType[i]);
      }
    }

    final int slotsPerGroup = totalSlots;
    final FlatSingleKeyMap flatMap = new FlatSingleKeyMap(slotsPerGroup);
    final String sourceCol = keyInfos.get(0).name();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-derived-1key-flat")) {

      if (query instanceof MatchAllDocsQuery) {
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          LeafReader reader = leafCtx.reader();
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();
          SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(sourceCol);

          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
            }
          }

          if (liveDocs == null) {
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0 = 0;
              if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
              int slot = flatMap.findOrInsert(key0);
              int base = slot * slotsPerGroup;
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) {
                  flatMap.accData[off]++;
                  continue;
                }
                SortedNumericDocValues aggDv = numericAggDvs[i];
                if (aggDv != null && aggDv.advanceExact(doc)) {
                  long rawVal = aggDv.nextValue();
                  switch (accType[i]) {
                    case 0:
                      flatMap.accData[off]++;
                      break;
                    case 1:
                      flatMap.accData[off] += rawVal;
                      break;
                    case 2:
                      flatMap.accData[off] += rawVal;
                      flatMap.accData[off + 1]++;
                      break;
                  }
                }
              }
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc)) {
                long key0 = 0;
                if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
                int slot = flatMap.findOrInsert(key0);
                int base = slot * slotsPerGroup;
                for (int i = 0; i < numAggs; i++) {
                  int off = base + accOffset[i];
                  if (isCountStar[i]) {
                    flatMap.accData[off]++;
                    continue;
                  }
                  SortedNumericDocValues aggDv = numericAggDvs[i];
                  if (aggDv != null && aggDv.advanceExact(doc)) {
                    long rawVal = aggDv.nextValue();
                    switch (accType[i]) {
                      case 0:
                        flatMap.accData[off]++;
                        break;
                      case 1:
                        flatMap.accData[off] += rawVal;
                        break;
                      case 2:
                        flatMap.accData[off] += rawVal;
                        flatMap.accData[off + 1]++;
                        break;
                    }
                  }
                }
              }
            }
          }
        }
      } else {
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues dv0 = context.reader().getSortedNumericDocValues(sourceCol);

                final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    numericAggDvs[i] = context.reader().getSortedNumericDocValues(specs.get(i).arg);
                  }
                }

                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    long key0 = 0;
                    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
                    int slot = flatMap.findOrInsert(key0);
                    int base = slot * slotsPerGroup;
                    for (int i = 0; i < numAggs; i++) {
                      int off = base + accOffset[i];
                      if (isCountStar[i]) {
                        flatMap.accData[off]++;
                        continue;
                      }
                      SortedNumericDocValues aggDv = numericAggDvs[i];
                      if (aggDv != null && aggDv.advanceExact(doc)) {
                        long rawVal = aggDv.nextValue();
                        switch (accType[i]) {
                          case 0:
                            flatMap.accData[off]++;
                            break;
                          case 1:
                            flatMap.accData[off] += rawVal;
                            break;
                          case 2:
                            flatMap.accData[off] += rawVal;
                            flatMap.accData[off + 1]++;
                            break;
                        }
                      }
                    }
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });
      }
    }

    if (flatMap.size == 0) return List.of();

    // Build output: expand single source key to all derived keys.
    // Plain source keys use their original type; derived (arith) keys use BigintType.
    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;
    int groupCount = flatMap.size;
    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      Type keyType = (arithUnits[i] != null) ? BigintType.BIGINT : keyInfos.get(i).type;
      builders[i] = keyType.createBlockBuilder(null, groupCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
    }
    for (int slot = 0; slot < flatMap.capacity; slot++) {
      if (flatMap.occupied[slot]) {
        long sourceVal = flatMap.keys[slot];
        // Write all group-by keys from the single source value
        for (int k = 0; k < numGroupKeys; k++) {
          long keyVal = sourceVal;
          if (arithUnits[k] != null) {
            keyVal = applyArith(sourceVal, arithUnits[k]);
            BigintType.BIGINT.writeLong(builders[k], keyVal);
          } else {
            writeNumericKeyValue(builders[k], keyInfos.get(k), keyVal);
          }
        }
        // Write aggregate results
        int base = slot * slotsPerGroup;
        for (int a = 0; a < numAggs; a++) {
          int off = base + accOffset[a];
          switch (accType[a]) {
            case 0:
            case 1:
              BigintType.BIGINT.writeLong(builders[numGroupKeys + a], flatMap.accData[off]);
              break;
            case 2:
              long sum = flatMap.accData[off];
              long cnt = flatMap.accData[off + 1];
              if (cnt == 0) {
                builders[numGroupKeys + a].appendNull();
              } else {
                DoubleType.DOUBLE.writeDouble(builders[numGroupKeys + a], (double) sum / cnt);
              }
              break;
          }
        }
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /** Build the result Page from a SingleKeyHashMap for derived-key GROUP BY. */
  private static List<Page> buildDerivedKeyResult(
      SingleKeyHashMap singleKeyMap,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      String[] arithUnits) {
    if (singleKeyMap.size == 0) return List.of();
    int numAggs = specs.size();
    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;
    int groupCount = singleKeyMap.size;
    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      Type keyType = (arithUnits[i] != null) ? BigintType.BIGINT : keyInfos.get(i).type;
      builders[i] = keyType.createBlockBuilder(null, groupCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
    }
    for (int slot = 0; slot < singleKeyMap.capacity; slot++) {
      if (singleKeyMap.occupied[slot]) {
        long sourceVal = singleKeyMap.keys[slot];
        for (int k = 0; k < numGroupKeys; k++) {
          long keyVal = sourceVal;
          if (arithUnits[k] != null) {
            keyVal = applyArith(sourceVal, arithUnits[k]);
            BigintType.BIGINT.writeLong(builders[k], keyVal);
          } else {
            writeNumericKeyValue(builders[k], keyInfos.get(k), keyVal);
          }
        }
        AccumulatorGroup accGroup = singleKeyMap.groups[slot];
        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
        }
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /**
   * Flat accumulator path for single-key numeric GROUP BY. Stores all accumulator state in a
   * contiguous {@code long[]} array, eliminating per-group object allocation. Each aggregate maps
   * to a fixed number of long slots:
   *
   * <ul>
   *   <li>COUNT(*) / COUNT(col): 1 slot (count)
   *   <li>SUM(long col): 1 slot (sum)
   *   <li>AVG(long col): 2 slots (sum, count)
   * </ul>
   *
   * <p>For Q16 (GROUP BY UserID, COUNT(*)) with ~25K groups per shard, this eliminates ~75K object
   * allocations (AccumulatorGroup + MergeableAccumulator[] + CountStarAccum per group).
   */
  private static List<Page> executeSingleKeyNumericFlat(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    // Compute flat layout: offset and slots per aggregate
    final int[] accOffset = new int[numAggs];
    int totalSlots = 0;
    for (int i = 0; i < numAggs; i++) {
      accOffset[i] = totalSlots;
      switch (accType[i]) {
        case 0: // COUNT
        case 1: // SUM long
          totalSlots += 1;
          break;
        case 2: // AVG long (sum + count)
          totalSlots += 2;
          break;
        default:
          throw new IllegalStateException("Flat path used with unsupported accType: " + accType[i]);
      }
    }

    final int slotsPerGroup = totalSlots;
    final FlatSingleKeyMap flatMap = new FlatSingleKeyMap(slotsPerGroup);

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-1key-flat")) {

      if (query instanceof MatchAllDocsQuery) {
        // Fast path: iterate all docs directly without Collector/Scorer overhead.
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          LeafReader reader = leafCtx.reader();
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();
          SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());

          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
            }
          }

          if (liveDocs == null) {
            for (int doc = 0; doc < maxDoc; doc++) {
              collectFlatSingleKeyDoc(
                  doc,
                  dv0,
                  flatMap,
                  slotsPerGroup,
                  numAggs,
                  isCountStar,
                  accType,
                  accOffset,
                  numericAggDvs);
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc)) {
                collectFlatSingleKeyDoc(
                    doc,
                    dv0,
                    flatMap,
                    slotsPerGroup,
                    numAggs,
                    isCountStar,
                    accType,
                    accOffset,
                    numericAggDvs);
              }
            }
          }
        }
      } else {
        // General path: use Lucene's search framework with Collector
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues dv0 =
                    context.reader().getSortedNumericDocValues(keyInfos.get(0).name());

                final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    numericAggDvs[i] = context.reader().getSortedNumericDocValues(specs.get(i).arg);
                  }
                }

                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    collectFlatSingleKeyDoc(
                        doc,
                        dv0,
                        flatMap,
                        slotsPerGroup,
                        numAggs,
                        isCountStar,
                        accType,
                        accOffset,
                        numericAggDvs);
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });
      }
    }

    if (flatMap.size == 0) return List.of();

    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Determine which slots to output (with optional top-N selection)
    int[] outputSlots;
    int outputCount;

    if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
      int sortAccOff = accOffset[sortAggIndex];
      int n = (int) Math.min(topN, flatMap.size);
      int[] heap = new int[n];
      int heapSize = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (!flatMap.occupied[slot]) continue;
        long val = flatMap.accData[slot * slotsPerGroup + sortAccOff];
        if (heapSize < n) {
          heap[heapSize] = slot;
          heapSize++;
          int k = heapSize - 1;
          while (k > 0) {
            int parent = (k - 1) >>> 1;
            long pVal = flatMap.accData[heap[parent] * slotsPerGroup + sortAccOff];
            long kVal = flatMap.accData[heap[k] * slotsPerGroup + sortAccOff];
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              int tmp = heap[parent];
              heap[parent] = heap[k];
              heap[k] = tmp;
              k = parent;
            } else break;
          }
        } else {
          long rootVal = flatMap.accData[heap[0] * slotsPerGroup + sortAccOff];
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = slot;
            int k = 0;
            while (true) {
              int left = 2 * k + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = flatMap.accData[heap[left] * slotsPerGroup + sortAccOff];
                long rv = flatMap.accData[heap[right] * slotsPerGroup + sortAccOff];
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = flatMap.accData[heap[k] * slotsPerGroup + sortAccOff];
              long tVal = flatMap.accData[heap[target] * slotsPerGroup + sortAccOff];
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                int tmp = heap[k];
                heap[k] = heap[target];
                heap[target] = tmp;
                k = target;
              } else break;
            }
          }
        }
      }
      outputSlots = heap;
      outputCount = heapSize;
    } else if (topN > 0 && topN < flatMap.size) {
      int n = (int) Math.min(topN, flatMap.size);
      outputSlots = new int[n];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity && idx < n; slot++) {
        if (flatMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[flatMap.size];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    }

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    builders[0] = keyInfos.get(0).type.createBlockBuilder(null, outputCount);
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }
    for (int o = 0; o < outputCount; o++) {
      int slot = outputSlots[o];
      writeNumericKeyValue(builders[0], keyInfos.get(0), flatMap.keys[slot]);
      int base = slot * slotsPerGroup;
      for (int a = 0; a < numAggs; a++) {
        int off = base + accOffset[a];
        switch (accType[a]) {
          case 0: // COUNT
          case 1: // SUM long
            BigintType.BIGINT.writeLong(builders[numGroupKeys + a], flatMap.accData[off]);
            break;
          case 2: // AVG long
            long sum = flatMap.accData[off];
            long cnt = flatMap.accData[off + 1];
            if (cnt == 0) {
              builders[numGroupKeys + a].appendNull();
            } else {
              DoubleType.DOUBLE.writeDouble(builders[numGroupKeys + a], (double) sum / cnt);
            }
            break;
        }
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /** Process a single document for the flat single-key numeric GROUP BY path. */
  private static void collectFlatSingleKeyDoc(
      int doc,
      SortedNumericDocValues dv0,
      FlatSingleKeyMap flatMap,
      int slotsPerGroup,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      SortedNumericDocValues[] numericAggDvs)
      throws IOException {
    long key0 = 0;
    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
    int slot = flatMap.findOrInsert(key0);
    int base = slot * slotsPerGroup;

    for (int i = 0; i < numAggs; i++) {
      int off = base + accOffset[i];
      if (isCountStar[i]) {
        flatMap.accData[off]++;
        continue;
      }
      SortedNumericDocValues aggDv = numericAggDvs[i];
      if (aggDv != null && aggDv.advanceExact(doc)) {
        long rawVal = aggDv.nextValue();
        switch (accType[i]) {
          case 0: // COUNT (non-star, non-distinct)
            flatMap.accData[off]++;
            break;
          case 1: // SUM long
            flatMap.accData[off] += rawVal;
            break;
          case 2: // AVG long (sum, count)
            flatMap.accData[off] += rawVal;
            flatMap.accData[off + 1]++;
            break;
        }
      }
    }
  }

  /**
   * Specialized fast path for GROUP BY with exactly two numeric keys. Uses an open-addressing hash
   * map with flat long-array accumulators, eliminating ALL per-group object allocation.
   *
   * <p>For aggregate patterns involving only COUNT(*), SUM(long), and AVG(long), accumulator state
   * is stored directly in a flat {@code long[]} per hash map slot. Each aggregate maps to a fixed
   * offset within this array:
   *
   * <ul>
   *   <li>COUNT(*): 1 long (count)
   *   <li>SUM(long): 1 long (sum)
   *   <li>AVG(long): 2 longs (sum, count)
   * </ul>
   *
   * <p>This eliminates AccumulatorGroup + MergeableAccumulator objects (5 allocations per group),
   * the MergeableAccumulator[] array, and all virtual dispatch in the accumulation loop. For Q32
   * with ~100K groups, this saves ~500K object allocations and associated GC pressure.
   *
   * <p>Falls back to the object-based TwoKeyHashMap for patterns with double args, VARCHAR args,
   * MIN, MAX, or COUNT(DISTINCT).
   */
  private static List<Page> executeTwoKeyNumeric(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    // Pre-compute per-aggregate dispatch flags
    final int numAggs = specs.size();
    final boolean[] isCountStar = new boolean[numAggs];
    final boolean[] isDoubleArg = new boolean[numAggs];
    // Accumulator type: 0=CountStar, 1=SumLong, 2=AvgLong, 3=Min, 4=Max, 5=CountDistinct,
    //                    6=SumDouble, 7=AvgDouble
    final int[] accType = new int[numAggs];
    boolean canUseFlatAccumulators = true;

    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      isDoubleArg[i] = spec.argType instanceof DoubleType;
      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        switch (spec.funcName) {
          case "COUNT":
            if (spec.isDistinct) {
              accType[i] = 5;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 0;
            }
            break;
          case "SUM":
            if (isDoubleArg[i]) {
              accType[i] = 6;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 1;
            }
            break;
          case "AVG":
            if (isDoubleArg[i]) {
              accType[i] = 7;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 2;
            }
            break;
          case "MIN":
            accType[i] = 3;
            canUseFlatAccumulators = false;
            break;
          case "MAX":
            accType[i] = 4;
            canUseFlatAccumulators = false;
            break;
        }
        // VARCHAR args can't use flat accumulators
        if (spec.argType instanceof VarcharType) {
          canUseFlatAccumulators = false;
        }
      }
    }

    // === Flat accumulator path for two-key numeric ===
    if (canUseFlatAccumulators) {
      return executeTwoKeyNumericFlat(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          numAggs,
          isCountStar,
          accType,
          sortAggIndex,
          sortAscending,
          topN);
    }

    // Use TwoKeyHashMap with AccumulatorGroup objects and pre-computed dispatch
    final TwoKeyHashMap twoKeyMap = new TwoKeyHashMap(specs);

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-2key")) {

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              SortedNumericDocValues dv0 =
                  context.reader().getSortedNumericDocValues(keyInfos.get(0).name());
              SortedNumericDocValues dv1 =
                  context.reader().getSortedNumericDocValues(keyInfos.get(1).name());

              final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i]) {
                  AggSpec spec = specs.get(i);
                  if (spec.argType instanceof VarcharType) {
                    // Skip varchar agg readers
                  } else {
                    numericAggDvs[i] = context.reader().getSortedNumericDocValues(spec.arg);
                  }
                }
              }

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  long key0 = 0, key1 = 0;
                  if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
                  if (dv1 != null && dv1.advanceExact(doc)) key1 = dv1.nextValue();
                  AccumulatorGroup accGroup = twoKeyMap.getOrCreate(key0, key1);

                  // Inline accumulation with pre-computed dispatch flags
                  for (int i = 0; i < numAggs; i++) {
                    MergeableAccumulator acc = accGroup.accumulators[i];
                    if (isCountStar[i]) {
                      ((CountStarAccum) acc).count++;
                      continue;
                    }
                    SortedNumericDocValues aggDv = numericAggDvs[i];
                    if (aggDv != null && aggDv.advanceExact(doc)) {
                      long rawVal = aggDv.nextValue();
                      switch (accType[i]) {
                        case 0:
                          ((CountStarAccum) acc).count++;
                          break;
                        case 1: // SUM
                          SumAccum sa = (SumAccum) acc;
                          sa.hasValue = true;
                          if (isDoubleArg[i]) sa.doubleSum += Double.longBitsToDouble(rawVal);
                          else sa.longSum += rawVal;
                          break;
                        case 2: // AVG
                          AvgAccum aa = (AvgAccum) acc;
                          aa.count++;
                          if (isDoubleArg[i]) aa.doubleSum += Double.longBitsToDouble(rawVal);
                          else aa.longSum += rawVal;
                          break;
                        case 3: // MIN
                          MinAccum ma = (MinAccum) acc;
                          ma.hasValue = true;
                          if (isDoubleArg[i]) {
                            double d = Double.longBitsToDouble(rawVal);
                            if (d < ma.doubleVal) ma.doubleVal = d;
                          } else {
                            if (rawVal < ma.longVal) ma.longVal = rawVal;
                          }
                          break;
                        case 4: // MAX
                          MaxAccum xa = (MaxAccum) acc;
                          xa.hasValue = true;
                          if (isDoubleArg[i]) {
                            double d = Double.longBitsToDouble(rawVal);
                            if (d > xa.doubleVal) xa.doubleVal = d;
                          } else {
                            if (rawVal > xa.longVal) xa.longVal = rawVal;
                          }
                          break;
                        case 5: // COUNT(DISTINCT)
                          CountDistinctAccum cda = (CountDistinctAccum) acc;
                          if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
                          else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
                          break;
                      }
                    }
                  }
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });
    }

    if (twoKeyMap.size == 0) return List.of();

    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Determine which slots to output (with optional top-N selection)
    int[] outputSlots;
    int outputCount;
    if (sortAggIndex >= 0 && topN > 0 && topN < twoKeyMap.size) {
      // Top-N using MergeableAccumulator.getSortValue() on the sort aggregate
      int n = (int) Math.min(topN, twoKeyMap.size);
      int[] heap = new int[n];
      int heapSize = 0;
      for (int slot = 0; slot < twoKeyMap.capacity; slot++) {
        if (!twoKeyMap.occupied[slot]) continue;
        long val = twoKeyMap.groups[slot].accumulators[sortAggIndex].getSortValue();
        if (heapSize < n) {
          heap[heapSize++] = slot;
          int k = heapSize - 1;
          while (k > 0) {
            int parent = (k - 1) >>> 1;
            long pVal = twoKeyMap.groups[heap[parent]].accumulators[sortAggIndex].getSortValue();
            long kVal = twoKeyMap.groups[heap[k]].accumulators[sortAggIndex].getSortValue();
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              int tmp = heap[parent];
              heap[parent] = heap[k];
              heap[k] = tmp;
              k = parent;
            } else break;
          }
        } else {
          long rootVal = twoKeyMap.groups[heap[0]].accumulators[sortAggIndex].getSortValue();
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = slot;
            int k = 0;
            while (true) {
              int left = 2 * k + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = twoKeyMap.groups[heap[left]].accumulators[sortAggIndex].getSortValue();
                long rv = twoKeyMap.groups[heap[right]].accumulators[sortAggIndex].getSortValue();
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = twoKeyMap.groups[heap[k]].accumulators[sortAggIndex].getSortValue();
              long tVal = twoKeyMap.groups[heap[target]].accumulators[sortAggIndex].getSortValue();
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                int tmp = heap[k];
                heap[k] = heap[target];
                heap[target] = tmp;
                k = target;
              } else break;
            }
          }
        }
      }
      outputSlots = heap;
      outputCount = heapSize;
    } else if (topN > 0 && topN < twoKeyMap.size) {
      int n = (int) Math.min(topN, twoKeyMap.size);
      outputSlots = new int[n];
      int idx = 0;
      for (int slot = 0; slot < twoKeyMap.capacity && idx < n; slot++) {
        if (twoKeyMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[twoKeyMap.size];
      int idx = 0;
      for (int slot = 0; slot < twoKeyMap.capacity; slot++) {
        if (twoKeyMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    }

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      builders[i] = keyInfos.get(i).type.createBlockBuilder(null, outputCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }
    for (int o = 0; o < outputCount; o++) {
      int slot = outputSlots[o];
      writeNumericKeyValue(builders[0], keyInfos.get(0), twoKeyMap.keys0[slot]);
      writeNumericKeyValue(builders[1], keyInfos.get(1), twoKeyMap.keys1[slot]);
      AccumulatorGroup accGroup = twoKeyMap.groups[slot];
      for (int a = 0; a < numAggs; a++) {
        accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /**
   * Flat accumulator path for two-key numeric GROUP BY. Same concept as {@link
   * #executeSingleKeyNumericFlat} but with two keys, using {@link FlatTwoKeyMap}.
   */
  private static List<Page> executeTwoKeyNumericFlat(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    final int[] accOffset = new int[numAggs];
    int totalSlots = 0;
    for (int i = 0; i < numAggs; i++) {
      accOffset[i] = totalSlots;
      switch (accType[i]) {
        case 0: // COUNT
        case 1: // SUM long
          totalSlots += 1;
          break;
        case 2: // AVG long
          totalSlots += 2;
          break;
        default:
          throw new IllegalStateException("Flat path used with unsupported accType: " + accType[i]);
      }
    }

    final int slotsPerGroup = totalSlots;
    final FlatTwoKeyMap flatMap = new FlatTwoKeyMap(slotsPerGroup);

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-2key-flat")) {

      if (query instanceof MatchAllDocsQuery) {
        // Fast path: iterate all docs directly without Collector/Scorer overhead.
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          LeafReader reader = leafCtx.reader();
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();
          SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());
          SortedNumericDocValues dv1 = reader.getSortedNumericDocValues(keyInfos.get(1).name());

          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
            }
          }

          if (liveDocs == null) {
            for (int doc = 0; doc < maxDoc; doc++) {
              collectFlatTwoKeyDoc(
                  doc,
                  dv0,
                  dv1,
                  flatMap,
                  slotsPerGroup,
                  numAggs,
                  isCountStar,
                  accType,
                  accOffset,
                  numericAggDvs);
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc)) {
                collectFlatTwoKeyDoc(
                    doc,
                    dv0,
                    dv1,
                    flatMap,
                    slotsPerGroup,
                    numAggs,
                    isCountStar,
                    accType,
                    accOffset,
                    numericAggDvs);
              }
            }
          }
        }
      } else {
        // General path: use Lucene's search framework with Collector
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues dv0 =
                    context.reader().getSortedNumericDocValues(keyInfos.get(0).name());
                SortedNumericDocValues dv1 =
                    context.reader().getSortedNumericDocValues(keyInfos.get(1).name());

                final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    numericAggDvs[i] = context.reader().getSortedNumericDocValues(specs.get(i).arg);
                  }
                }

                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    collectFlatTwoKeyDoc(
                        doc,
                        dv0,
                        dv1,
                        flatMap,
                        slotsPerGroup,
                        numAggs,
                        isCountStar,
                        accType,
                        accOffset,
                        numericAggDvs);
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });
      }
    }

    if (flatMap.size == 0) return List.of();

    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Determine which slots to output (with optional top-N selection)
    int[] outputSlots;
    int outputCount;

    if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
      // Top-N selection using a min-heap (for DESC) or max-heap (for ASC)
      // on the sort aggregate value from the flat accData array.
      int sortAccOff = accOffset[sortAggIndex];
      int n = (int) Math.min(topN, flatMap.size);
      int[] heap = new int[n];
      int heapSize = 0;

      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (!flatMap.occupied[slot]) continue;
        long val = flatMap.accData[slot * slotsPerGroup + sortAccOff];
        if (heapSize < n) {
          heap[heapSize] = slot;
          heapSize++;
          int k = heapSize - 1;
          while (k > 0) {
            int parent = (k - 1) >>> 1;
            long pVal = flatMap.accData[heap[parent] * slotsPerGroup + sortAccOff];
            long kVal = flatMap.accData[heap[k] * slotsPerGroup + sortAccOff];
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              int tmp = heap[parent];
              heap[parent] = heap[k];
              heap[k] = tmp;
              k = parent;
            } else {
              break;
            }
          }
        } else {
          long rootVal = flatMap.accData[heap[0] * slotsPerGroup + sortAccOff];
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = slot;
            int k = 0;
            while (true) {
              int left = 2 * k + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = flatMap.accData[heap[left] * slotsPerGroup + sortAccOff];
                long rv = flatMap.accData[heap[right] * slotsPerGroup + sortAccOff];
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = flatMap.accData[heap[k] * slotsPerGroup + sortAccOff];
              long tVal = flatMap.accData[heap[target] * slotsPerGroup + sortAccOff];
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                int tmp = heap[k];
                heap[k] = heap[target];
                heap[target] = tmp;
                k = target;
              } else {
                break;
              }
            }
          }
        }
      }
      outputSlots = heap;
      outputCount = heapSize;
    } else if (topN > 0 && topN < flatMap.size) {
      int n = (int) Math.min(topN, flatMap.size);
      outputSlots = new int[n];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity && idx < n; slot++) {
        if (flatMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[flatMap.size];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    }

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      builders[i] = keyInfos.get(i).type.createBlockBuilder(null, outputCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }
    for (int o = 0; o < outputCount; o++) {
      int slot = outputSlots[o];
      writeNumericKeyValue(builders[0], keyInfos.get(0), flatMap.keys0[slot]);
      writeNumericKeyValue(builders[1], keyInfos.get(1), flatMap.keys1[slot]);
      int base = slot * slotsPerGroup;
      for (int a = 0; a < numAggs; a++) {
        int off = base + accOffset[a];
        switch (accType[a]) {
          case 0: // COUNT
          case 1: // SUM long
            BigintType.BIGINT.writeLong(builders[numGroupKeys + a], flatMap.accData[off]);
            break;
          case 2: // AVG long
            long sum = flatMap.accData[off];
            long cnt = flatMap.accData[off + 1];
            if (cnt == 0) {
              builders[numGroupKeys + a].appendNull();
            } else {
              DoubleType.DOUBLE.writeDouble(builders[numGroupKeys + a], (double) sum / cnt);
            }
            break;
        }
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /** Process a single document for the flat two-key numeric GROUP BY path. */
  private static void collectFlatTwoKeyDoc(
      int doc,
      SortedNumericDocValues dv0,
      SortedNumericDocValues dv1,
      FlatTwoKeyMap flatMap,
      int slotsPerGroup,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      SortedNumericDocValues[] numericAggDvs)
      throws IOException {
    long key0 = 0, key1 = 0;
    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
    if (dv1 != null && dv1.advanceExact(doc)) key1 = dv1.nextValue();
    int slot = flatMap.findOrInsert(key0, key1);
    int base = slot * slotsPerGroup;

    for (int i = 0; i < numAggs; i++) {
      int off = base + accOffset[i];
      if (isCountStar[i]) {
        flatMap.accData[off]++;
        continue;
      }
      SortedNumericDocValues aggDv = numericAggDvs[i];
      if (aggDv != null && aggDv.advanceExact(doc)) {
        long rawVal = aggDv.nextValue();
        switch (accType[i]) {
          case 0: // COUNT
            flatMap.accData[off]++;
            break;
          case 1: // SUM long
            flatMap.accData[off] += rawVal;
            break;
          case 2: // AVG long
            flatMap.accData[off] += rawVal;
            flatMap.accData[off + 1]++;
            break;
        }
      }
    }
  }

  /**
   * Path for GROUP BY keys that include at least one EvalNode-computed expression (e.g., CASE
   * WHEN). Uses a two-phase approach: (1) collect all matching doc IDs, (2) evaluate eval
   * expressions in one large batch, then group and aggregate. Non-eval keys and aggregates use
   * DocValues directly.
   *
   * <p>Only columns referenced by eval expressions are materialized into Pages for vectorized
   * evaluation, minimizing allocation overhead. The eval Page is built once per segment, not per
   * micro-batch.
   */
  private static List<Page> executeWithEvalKeys(
      AggregationNode aggNode,
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys)
      throws Exception {

    EvalNode evalNode = (aggNode.getChild() instanceof EvalNode en) ? en : null;
    TableScanNode scanNode = findChildTableScan(aggNode);

    // Identify eval keys and their source columns
    final int numKeys = keyInfos.size();
    final int numAggs = specs.size();
    final boolean[] isEvalKey = new boolean[numKeys];

    // Collect only columns needed for eval expressions (not all scan columns)
    Set<String> evalSourceColumns = new HashSet<>();
    if (evalNode != null) {
      for (int k = 0; k < numKeys; k++) {
        KeyInfo ki = keyInfos.get(k);
        if ("eval".equals(ki.exprFunc)) {
          isEvalKey[k] = true;
          int exprIdx = Integer.parseInt(ki.exprUnit);
          String exprStr = evalNode.getExpressions().get(exprIdx);
          for (String col : columnTypeMap.keySet()) {
            if (exprStr.contains(col)) {
              evalSourceColumns.add(col);
            }
          }
        }
      }
    }

    // Build column index map for expression compilation: only eval source columns
    List<String> evalColumns = new ArrayList<>(evalSourceColumns);
    java.util.Collections.sort(evalColumns); // Deterministic order
    Map<String, Integer> colIndexMap = new HashMap<>();
    for (int i = 0; i < evalColumns.size(); i++) {
      colIndexMap.put(evalColumns.get(i), i);
    }

    // Normalized type map for expression compilation
    Map<String, Type> normalizedTypeMap = new HashMap<>(columnTypeMap.size());
    for (Map.Entry<String, Type> entry : columnTypeMap.entrySet()) {
      Type t = entry.getValue();
      if (t instanceof VarcharType || t instanceof DoubleType || t instanceof TimestampType) {
        normalizedTypeMap.put(entry.getKey(), t);
      } else {
        normalizedTypeMap.put(entry.getKey(), BigintType.BIGINT);
      }
    }

    // Normalized types for the eval columns
    final Type[] evalColTypes = new Type[evalColumns.size()];
    for (int c = 0; c < evalColumns.size(); c++) {
      Type t = normalizedTypeMap.getOrDefault(evalColumns.get(c), BigintType.BIGINT);
      evalColTypes[c] = t;
    }

    // Compile eval expressions
    org.opensearch.sql.dqe.function.FunctionRegistry registry =
        org.opensearch.sql.dqe.function.BuiltinFunctions.createRegistry();
    io.trino.sql.parser.SqlParser sqlParser = new io.trino.sql.parser.SqlParser();
    org.opensearch.sql.dqe.function.expression.ExpressionCompiler compiler =
        new org.opensearch.sql.dqe.function.expression.ExpressionCompiler(
            registry, colIndexMap, normalizedTypeMap);

    final org.opensearch.sql.dqe.function.expression.BlockExpression[] evalExprs =
        new org.opensearch.sql.dqe.function.expression.BlockExpression[numKeys];
    for (int k = 0; k < numKeys; k++) {
      if (isEvalKey[k]) {
        int exprIdx = Integer.parseInt(keyInfos.get(k).exprUnit);
        String exprStr = evalNode.getExpressions().get(exprIdx);
        io.trino.sql.tree.Expression ast = sqlParser.createExpression(exprStr);
        evalExprs[k] = compiler.compile(ast);
      }
    }

    // Pre-compute aggregate dispatch types
    final boolean[] isCountStar = new boolean[numAggs];
    final int[] accType = new int[numAggs];
    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        boolean isDouble = spec.argType instanceof DoubleType;
        switch (spec.funcName) {
          case "COUNT":
            accType[i] = spec.isDistinct ? 5 : 0;
            break;
          case "SUM":
            accType[i] = isDouble ? 6 : 1;
            break;
          case "AVG":
            accType[i] = isDouble ? 7 : 2;
            break;
          case "MIN":
            accType[i] = 3;
            break;
          case "MAX":
            accType[i] = 4;
            break;
        }
      }
    }

    // Pre-compute transformation units for non-eval keys
    final String[] truncUnits = new String[numKeys];
    final String[] arithUnits = new String[numKeys];
    for (int i = 0; i < numKeys; i++) {
      KeyInfo ki = keyInfos.get(i);
      if ("date_trunc".equals(ki.exprFunc)) truncUnits[i] = ki.exprUnit;
      else if ("arith".equals(ki.exprFunc)) arithUnits[i] = ki.exprUnit;
      else if ("extract".equals(ki.exprFunc)) arithUnits[i] = "E:" + ki.exprUnit;
    }

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-eval")) {

      Map<MergedGroupKey, AccumulatorGroup> globalGroups = new LinkedHashMap<>();

      // Phase 1: Collect doc IDs and non-eval key values per segment
      for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
        LeafReader reader = leafCtx.reader();

        // Collect matching doc IDs for this segment
        List<Integer> segDocIds = new ArrayList<>();
        engineSearcher
            .getIndexReader()
            .getContext()
            .equals(null); // no-op, just ensure context is available
        org.apache.lucene.search.Weight weight =
            engineSearcher.createWeight(
                engineSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        org.apache.lucene.search.Scorer scorer = weight.scorer(leafCtx);
        if (scorer == null) continue;

        DocIdSetIterator disi = scorer.iterator();
        int doc;
        while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          segDocIds.add(doc);
        }
        if (segDocIds.isEmpty()) continue;

        int segDocCount = segDocIds.size();

        // Phase 2: Read non-eval key values for all collected docs
        Object[] keyReaders = new Object[numKeys];
        for (int i = 0; i < numKeys; i++) {
          if (isEvalKey[i]) continue;
          KeyInfo ki = keyInfos.get(i);
          if (ki.isVarchar) keyReaders[i] = reader.getSortedSetDocValues(ki.name);
          else keyReaders[i] = reader.getSortedNumericDocValues(ki.name);
        }

        long[][] segKeyValues = new long[segDocCount][numKeys];
        boolean[][] segKeyNulls = new boolean[segDocCount][numKeys];

        for (int d = 0; d < segDocCount; d++) {
          int docId = segDocIds.get(d);
          for (int k = 0; k < numKeys; k++) {
            if (isEvalKey[k]) continue;
            KeyInfo ki = keyInfos.get(k);
            if (ki.isVarchar) {
              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
              if (dv != null && dv.advanceExact(docId)) {
                segKeyValues[d][k] = dv.nextOrd();
              } else {
                segKeyNulls[d][k] = true;
              }
            } else {
              SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[k];
              if (dv != null && dv.advanceExact(docId)) {
                long val = dv.nextValue();
                if (truncUnits[k] != null) val = truncateMillis(val, truncUnits[k]);
                else if (arithUnits[k] != null) val = applyArith(val, arithUnits[k]);
                segKeyValues[d][k] = val;
              } else {
                segKeyNulls[d][k] = true;
              }
            }
          }
        }

        // Phase 3: Build eval column blocks for the entire segment and evaluate expressions
        // Only build blocks for columns referenced by eval expressions
        BlockBuilder[] colBuilders = new BlockBuilder[evalColumns.size()];
        for (int c = 0; c < evalColumns.size(); c++) {
          colBuilders[c] = evalColTypes[c].createBlockBuilder(null, segDocCount);
        }

        // Open eval column DocValues
        SortedNumericDocValues[] evalNumDvs = new SortedNumericDocValues[evalColumns.size()];
        SortedSetDocValues[] evalVarDvs = new SortedSetDocValues[evalColumns.size()];
        for (int c = 0; c < evalColumns.size(); c++) {
          if (evalColTypes[c] instanceof VarcharType) {
            evalVarDvs[c] = reader.getSortedSetDocValues(evalColumns.get(c));
          } else {
            evalNumDvs[c] = reader.getSortedNumericDocValues(evalColumns.get(c));
          }
        }

        for (int d = 0; d < segDocCount; d++) {
          int docId = segDocIds.get(d);
          for (int c = 0; c < evalColumns.size(); c++) {
            if (evalColTypes[c] instanceof VarcharType) {
              SortedSetDocValues dv = evalVarDvs[c];
              if (dv != null && dv.advanceExact(docId)) {
                BytesRef bytes = dv.lookupOrd(dv.nextOrd());
                VarcharType.VARCHAR.writeSlice(
                    colBuilders[c], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              } else {
                VarcharType.VARCHAR.writeSlice(colBuilders[c], Slices.EMPTY_SLICE);
              }
            } else if (evalColTypes[c] instanceof DoubleType) {
              SortedNumericDocValues dv = evalNumDvs[c];
              if (dv != null && dv.advanceExact(docId)) {
                DoubleType.DOUBLE.writeDouble(
                    colBuilders[c], Double.longBitsToDouble(dv.nextValue()));
              } else {
                colBuilders[c].appendNull();
              }
            } else {
              SortedNumericDocValues dv = evalNumDvs[c];
              if (dv != null && dv.advanceExact(docId)) {
                BigintType.BIGINT.writeLong(colBuilders[c], dv.nextValue());
              } else {
                colBuilders[c].appendNull();
              }
            }
          }
        }

        Block[] colBlocks = new Block[evalColumns.size()];
        for (int c = 0; c < evalColumns.size(); c++) {
          colBlocks[c] = colBuilders[c].build();
        }
        Page evalPage = new Page(colBlocks);

        // Evaluate all eval expressions on the entire segment batch
        Block[] evalResultBlocks = new Block[numKeys];
        for (int k = 0; k < numKeys; k++) {
          if (isEvalKey[k]) {
            evalResultBlocks[k] = evalExprs[k].evaluate(evalPage);
          }
        }

        // Phase 4: Group and aggregate
        for (int d = 0; d < segDocCount; d++) {
          Object[] resolvedKeys = new Object[numKeys];
          for (int k = 0; k < numKeys; k++) {
            if (isEvalKey[k]) {
              Block block = evalResultBlocks[k];
              if (block.isNull(d)) {
                resolvedKeys[k] = null;
              } else {
                Type keyType = keyInfos.get(k).type;
                if (keyType instanceof VarcharType) {
                  io.airlift.slice.Slice slice = VarcharType.VARCHAR.getSlice(block, d);
                  resolvedKeys[k] =
                      new BytesRefKey(new BytesRef(slice.getBytes(), 0, slice.length()));
                } else if (keyType instanceof DoubleType) {
                  resolvedKeys[k] = Double.doubleToLongBits(DoubleType.DOUBLE.getDouble(block, d));
                } else {
                  resolvedKeys[k] = BigintType.BIGINT.getLong(block, d);
                }
              }
            } else if (segKeyNulls[d][k]) {
              resolvedKeys[k] = null;
            } else if (keyInfos.get(k).isVarchar) {
              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
              if (dv != null) {
                BytesRef bytes = dv.lookupOrd(segKeyValues[d][k]);
                resolvedKeys[k] = new BytesRefKey(bytes);
              } else {
                resolvedKeys[k] = new BytesRefKey(new BytesRef(""));
              }
            } else {
              resolvedKeys[k] = segKeyValues[d][k];
            }
          }

          MergedGroupKey mgk = new MergedGroupKey(resolvedKeys, keyInfos);
          AccumulatorGroup accGroup = globalGroups.get(mgk);
          if (accGroup == null) {
            accGroup = createAccumulatorGroup(specs);
            globalGroups.put(mgk, accGroup);
          }

          // Only COUNT(*) for now — Q40 only uses COUNT(*)
          for (int i = 0; i < numAggs; i++) {
            if (isCountStar[i]) {
              ((CountStarAccum) accGroup.accumulators[i]).count++;
            }
          }
        }
      }

      if (globalGroups.isEmpty()) {
        return List.of();
      }

      // Build output Page
      int numGroupKeys = groupByKeys.size();
      int totalColumns = numGroupKeys + numAggs;
      int groupCount = globalGroups.size();

      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      for (int i = 0; i < numGroupKeys; i++) {
        builders[i] = keyInfos.get(i).type.createBlockBuilder(null, groupCount);
      }
      for (int i = 0; i < numAggs; i++) {
        builders[numGroupKeys + i] =
            resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
      }

      for (Map.Entry<MergedGroupKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
        MergedGroupKey key = entry.getKey();
        AccumulatorGroup accGroup = entry.getValue();
        for (int k = 0; k < numGroupKeys; k++) {
          writeKeyValueForMerged(builders[k], keyInfos.get(k), key.values[k]);
        }
        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
        }
      }

      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) {
        blocks[i] = builders[i].build();
      }
      return List.of(new Page(blocks));
    }
  }

  /**
   * Path for GROUP BY keys that include at least one VARCHAR column. Uses SortedSetDocValues
   * ordinals as hash keys during per-segment aggregation. For single-segment indices (common), the
   * segment-local ordinals ARE the final result — ordinals are resolved to raw UTF-8 bytes only at
   * Page output time, completely bypassing MergedGroupKey and String allocation. For multi-segment
   * indices, resolves ordinals to BytesRefKey (raw UTF-8) for cross-segment merge, still avoiding
   * the expensive UTF-8 to Java char[] round-trip.
   */
  private static List<Page> executeWithVarcharKeys(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    // Pre-compute DATE_TRUNC, arithmetic, and EXTRACT units for numeric keys in the varchar path
    final String[] truncUnits = new String[keyInfos.size()];
    final String[] arithUnits = new String[keyInfos.size()];
    for (int i = 0; i < keyInfos.size(); i++) {
      KeyInfo ki = keyInfos.get(i);
      if ("date_trunc".equals(ki.exprFunc)) {
        truncUnits[i] = ki.exprUnit;
      } else if ("arith".equals(ki.exprFunc)) {
        arithUnits[i] = ki.exprUnit;
      } else if ("extract".equals(ki.exprFunc)) {
        arithUnits[i] = "E:" + ki.exprUnit;
      }
    }

    // Pre-compute aggregate dispatch types to eliminate instanceof chains in hot loop.
    // Same dispatch scheme as executeSingleKeyNumeric/executeTwoKeyNumeric:
    // 0=CountStar, 1=SumLong, 2=AvgLong, 3=Min, 4=Max, 5=CountDistinct,
    // 6=SumDouble, 7=AvgDouble
    final int numAggs = specs.size();
    final boolean[] isCountStar = new boolean[numAggs];
    final boolean[] isDoubleArg = new boolean[numAggs];
    final boolean[] isVarcharArg = new boolean[numAggs];
    final int[] accType = new int[numAggs];
    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      isDoubleArg[i] = spec.argType instanceof DoubleType;
      isVarcharArg[i] = spec.argType instanceof VarcharType;
      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        switch (spec.funcName) {
          case "COUNT":
            accType[i] = spec.isDistinct ? 5 : 0;
            break;
          case "SUM":
            accType[i] = isDoubleArg[i] ? 6 : 1;
            break;
          case "AVG":
            accType[i] = isDoubleArg[i] ? 7 : 2;
            break;
          case "MIN":
            accType[i] = 3;
            break;
          case "MAX":
            accType[i] = 4;
            break;
        }
      }
    }

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby")) {

      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      boolean singleSegment = (leaves.size() == 1);

      if (singleSegment && keyInfos.size() == 2) {
        // === Single-segment two-key fast path ===

        // Check if we can use the ordinal-indexed path: exactly one varchar key, one numeric
        // key (no date_trunc/arith), COUNT(*) only, bounded ordinals. This replaces hash table
        // operations with direct array indexing, eliminating hash computation and probing.
        boolean allCountStar = true;
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i]) {
            allCountStar = false;
            break;
          }
        }
        int varcharKeyIdx = -1, numericKeyIdx = -1;
        if (allCountStar && numAggs == 1) {
          for (int i = 0; i < 2; i++) {
            KeyInfo ki = keyInfos.get(i);
            if (ki.isVarchar && ki.exprFunc == null) {
              varcharKeyIdx = i;
            } else if (!ki.isVarchar && ki.exprFunc == null) {
              numericKeyIdx = i;
            }
          }
        }
        if (varcharKeyIdx >= 0 && numericKeyIdx >= 0) {
          // === Ordinal-indexed two-key COUNT(*) path ===
          // Uses the varchar ordinal as a direct array index and maintains a small per-ordinal
          // open-addressing map for the numeric key. Eliminates hash2() computation and two-key
          // hash probing — critical for Q15 where SearchPhrase has 18K ordinals but
          // SearchEngineID has only 39 distinct values.
          LeafReaderContext leafCtx = leaves.get(0);
          LeafReader reader = leafCtx.reader();
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();

          SortedSetDocValues varcharDv =
              reader.getSortedSetDocValues(keyInfos.get(varcharKeyIdx).name);
          SortedNumericDocValues numericDv =
              reader.getSortedNumericDocValues(keyInfos.get(numericKeyIdx).name);

          if (varcharDv != null && numericDv != null) {
            long valueCount = varcharDv.getValueCount();
            // Limit: for very high cardinality varchars, fall through to hash map path
            if (valueCount <= 500_000) {
              int numOrds = (int) valueCount;

              // Per-ordinal storage: for each ordinal, store (numericKey, count) pairs.
              // Use a flat layout: numericKeys[ord * MAX_KEYS_PER_ORD + 0..n] and
              // counts[ord * MAX_KEYS_PER_ORD + 0..n]. Since the numeric key has low
              // cardinality (typically <100), we use a small fixed capacity per ordinal
              // with linear scan. This is cache-friendly for small cardinalities.
              final int KEYS_PER_ORD = 64;

              // Heuristic: check numeric key's value range via PointValues. If the
              // range exceeds KEYS_PER_ORD, the field likely has high cardinality
              // (e.g., UserID) and will overflow the per-ordinal linear scan. Skip
              // to FlatTwoKeyMap to avoid wasted iteration and array allocation.
              // For low-cardinality fields (e.g., SearchEngineID range 0-39), the
              // ordinal-indexed path is much faster.
              boolean numericKeyLikelyHighCardinality = false;
              org.apache.lucene.index.PointValues pv =
                  reader.getPointValues(keyInfos.get(numericKeyIdx).name);
              if (pv != null) {
                byte[] minBytes = pv.getMinPackedValue();
                byte[] maxBytes = pv.getMaxPackedValue();
                if (minBytes != null && maxBytes != null && minBytes.length >= 8) {
                  // Decode big-endian long values (OpenSearch/Lucene uses big-endian
                  // encoding for numeric point values)
                  long minVal = 0, maxVal = 0;
                  for (int b = 0; b < 8; b++) {
                    minVal = (minVal << 8) | (minBytes[b] & 0xFFL);
                    maxVal = (maxVal << 8) | (maxBytes[b] & 0xFFL);
                  }
                  // Flip sign bit for signed long encoding used by Lucene
                  minVal ^= 0x8000000000000000L;
                  maxVal ^= 0x8000000000000000L;
                  long range = maxVal - minVal;
                  if (range < 0 || range > KEYS_PER_ORD) {
                    numericKeyLikelyHighCardinality = true;
                  }
                }
              }

              // If total memory would exceed ~32MB, fall through
              if (!numericKeyLikelyHighCardinality
                  && (long) numOrds * KEYS_PER_ORD * 16 <= 32_000_000L) {
                long[] numericKeys = new long[numOrds * KEYS_PER_ORD];
                long[] counts = new long[numOrds * KEYS_PER_ORD];
                int[] numKeysPerOrd = new int[numOrds];
                boolean overflowed = false;

                if (query instanceof MatchAllDocsQuery) {
                  if (liveDocs == null) {
                    for (int doc = 0; doc < maxDoc; doc++) {
                      if (!varcharDv.advanceExact(doc)) continue;
                      long ord = varcharDv.nextOrd();
                      if (!numericDv.advanceExact(doc)) continue;
                      long nk = numericDv.nextValue();
                      int base = (int) ord * KEYS_PER_ORD;
                      int n = numKeysPerOrd[(int) ord];
                      // Linear scan for matching numeric key
                      int idx = -1;
                      for (int j = 0; j < n; j++) {
                        if (numericKeys[base + j] == nk) {
                          idx = j;
                          break;
                        }
                      }
                      if (idx >= 0) {
                        counts[base + idx]++;
                      } else if (n < KEYS_PER_ORD) {
                        numericKeys[base + n] = nk;
                        counts[base + n] = 1;
                        numKeysPerOrd[(int) ord]++;
                      } else {
                        overflowed = true;
                        break;
                      }
                    }
                  } else {
                    for (int doc = 0; doc < maxDoc; doc++) {
                      if (!liveDocs.get(doc)) continue;
                      if (!varcharDv.advanceExact(doc)) continue;
                      long ord = varcharDv.nextOrd();
                      if (!numericDv.advanceExact(doc)) continue;
                      long nk = numericDv.nextValue();
                      int base = (int) ord * KEYS_PER_ORD;
                      int n = numKeysPerOrd[(int) ord];
                      int idx = -1;
                      for (int j = 0; j < n; j++) {
                        if (numericKeys[base + j] == nk) {
                          idx = j;
                          break;
                        }
                      }
                      if (idx >= 0) {
                        counts[base + idx]++;
                      } else if (n < KEYS_PER_ORD) {
                        numericKeys[base + n] = nk;
                        counts[base + n] = 1;
                        numKeysPerOrd[(int) ord]++;
                      } else {
                        overflowed = true;
                        break;
                      }
                    }
                  }
                } else {
                  // Filtered path using Lucene collector
                  final boolean[] overflowFlag = {false};
                  engineSearcher.search(
                      query,
                      new Collector() {
                        @Override
                        public LeafCollector getLeafCollector(LeafReaderContext context)
                            throws IOException {
                          return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) {}

                            @Override
                            public void collect(int doc) throws IOException {
                              if (overflowFlag[0]) return; // fast skip after overflow
                              if (!varcharDv.advanceExact(doc)) return;
                              long ord = varcharDv.nextOrd();
                              if (!numericDv.advanceExact(doc)) return;
                              long nk = numericDv.nextValue();
                              int base = (int) ord * KEYS_PER_ORD;
                              int n = numKeysPerOrd[(int) ord];
                              int idx = -1;
                              for (int j = 0; j < n; j++) {
                                if (numericKeys[base + j] == nk) {
                                  idx = j;
                                  break;
                                }
                              }
                              if (idx >= 0) {
                                counts[base + idx]++;
                              } else if (n < KEYS_PER_ORD) {
                                numericKeys[base + n] = nk;
                                counts[base + n] = 1;
                                numKeysPerOrd[(int) ord]++;
                              } else {
                                overflowFlag[0] = true;
                              }
                            }
                          };
                        }

                        @Override
                        public ScoreMode scoreMode() {
                          return ScoreMode.COMPLETE_NO_SCORES;
                        }
                      });
                  overflowed = overflowFlag[0];
                }

                if (overflowed) {
                  // Numeric key cardinality exceeded KEYS_PER_ORD per ordinal.
                  // Fall through to the FlatTwoKeyMap path below.
                } else {
                  // Count total groups for output sizing
                  int totalGroups = 0;
                  for (int o = 0; o < numOrds; o++) {
                    totalGroups += numKeysPerOrd[o];
                  }
                  if (totalGroups == 0) return List.of();

                  // Build output Page or apply top-N selection
                  int numGroupKeys = groupByKeys.size();
                  int totalColumns = numGroupKeys + numAggs;

                  if (sortAggIndex >= 0 && topN > 0 && topN < totalGroups) {
                    // Top-N via min-heap: select top-N groups by count
                    int n = (int) Math.min(topN, totalGroups);
                    // Heap stores (ord, keyIdx) encoded as a single long: (ord << 32) | keyIdx
                    long[] heap = new long[n];
                    long[] heapVals = new long[n];
                    int heapSize = 0;
                    for (int o = 0; o < numOrds; o++) {
                      int nk = numKeysPerOrd[o];
                      int base = o * KEYS_PER_ORD;
                      for (int j = 0; j < nk; j++) {
                        long cnt = counts[base + j];
                        if (heapSize < n) {
                          heap[heapSize] = ((long) o << 32) | j;
                          heapVals[heapSize] = cnt;
                          heapSize++;
                          // Sift up (min-heap for DESC, max-heap for ASC)
                          int k = heapSize - 1;
                          while (k > 0) {
                            int parent = (k - 1) >>> 1;
                            boolean swap =
                                sortAscending
                                    ? (heapVals[k] > heapVals[parent])
                                    : (heapVals[k] < heapVals[parent]);
                            if (swap) {
                              long tmpL = heap[parent];
                              heap[parent] = heap[k];
                              heap[k] = tmpL;
                              long tmpV = heapVals[parent];
                              heapVals[parent] = heapVals[k];
                              heapVals[k] = tmpV;
                              k = parent;
                            } else {
                              break;
                            }
                          }
                        } else {
                          boolean better =
                              sortAscending ? (cnt < heapVals[0]) : (cnt > heapVals[0]);
                          if (better) {
                            heap[0] = ((long) o << 32) | j;
                            heapVals[0] = cnt;
                            // Sift down
                            int k = 0;
                            while (true) {
                              int left = 2 * k + 1;
                              if (left >= heapSize) break;
                              int right = left + 1;
                              int target = left;
                              if (right < heapSize) {
                                boolean pickRight =
                                    sortAscending
                                        ? (heapVals[right] > heapVals[left])
                                        : (heapVals[right] < heapVals[left]);
                                if (pickRight) target = right;
                              }
                              boolean swap =
                                  sortAscending
                                      ? (heapVals[target] > heapVals[k])
                                      : (heapVals[target] < heapVals[k]);
                              if (swap) {
                                long tmpL = heap[k];
                                heap[k] = heap[target];
                                heap[target] = tmpL;
                                long tmpV = heapVals[k];
                                heapVals[k] = heapVals[target];
                                heapVals[target] = tmpV;
                                k = target;
                              } else {
                                break;
                              }
                            }
                          }
                        }
                      }
                    }
                    // Sort heap by value for output
                    for (int i = 1; i < heapSize; i++) {
                      long key = heap[i];
                      long keyVal = heapVals[i];
                      int j = i - 1;
                      while (j >= 0) {
                        boolean needsSwap =
                            sortAscending ? (heapVals[j] > keyVal) : (heapVals[j] < keyVal);
                        if (needsSwap) {
                          heap[j + 1] = heap[j];
                          heapVals[j + 1] = heapVals[j];
                          j--;
                        } else {
                          break;
                        }
                      }
                      heap[j + 1] = key;
                      heapVals[j + 1] = keyVal;
                    }

                    // Build output Page for top-N
                    BlockBuilder[] builders = new BlockBuilder[totalColumns];
                    for (int i = 0; i < numGroupKeys; i++) {
                      builders[i] = keyInfos.get(i).type.createBlockBuilder(null, heapSize);
                    }
                    builders[numGroupKeys] = BigintType.BIGINT.createBlockBuilder(null, heapSize);

                    for (int si = 0; si < heapSize; si++) {
                      long encoded = heap[si];
                      int ord = (int) (encoded >>> 32);
                      int keyIdx = (int) (encoded & 0xFFFFFFFFL);
                      long nk = numericKeys[ord * KEYS_PER_ORD + keyIdx];
                      long cnt = counts[ord * KEYS_PER_ORD + keyIdx];

                      // Write keys in correct column order
                      if (varcharKeyIdx == 0) {
                        BytesRef bytes = varcharDv.lookupOrd(ord);
                        VarcharType.VARCHAR.writeSlice(
                            builders[0],
                            Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
                        writeNumericKeyValue(builders[1], keyInfos.get(1), nk);
                      } else {
                        writeNumericKeyValue(builders[0], keyInfos.get(0), nk);
                        BytesRef bytes = varcharDv.lookupOrd(ord);
                        VarcharType.VARCHAR.writeSlice(
                            builders[1],
                            Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
                      }
                      BigintType.BIGINT.writeLong(builders[numGroupKeys], cnt);
                    }

                    Block[] blocks = new Block[totalColumns];
                    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
                    return List.of(new Page(blocks));
                  } else {
                    // No top-N: output all groups
                    BlockBuilder[] builders = new BlockBuilder[totalColumns];
                    for (int i = 0; i < numGroupKeys; i++) {
                      builders[i] = keyInfos.get(i).type.createBlockBuilder(null, totalGroups);
                    }
                    builders[numGroupKeys] =
                        BigintType.BIGINT.createBlockBuilder(null, totalGroups);
                    for (int o = 0; o < numOrds; o++) {
                      int nk = numKeysPerOrd[o];
                      if (nk == 0) continue;
                      int base = o * KEYS_PER_ORD;
                      BytesRef bytes = varcharDv.lookupOrd(o);
                      io.airlift.slice.Slice slice =
                          Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length);
                      for (int j = 0; j < nk; j++) {
                        if (varcharKeyIdx == 0) {
                          VarcharType.VARCHAR.writeSlice(builders[0], slice);
                          writeNumericKeyValue(builders[1], keyInfos.get(1), numericKeys[base + j]);
                        } else {
                          writeNumericKeyValue(builders[0], keyInfos.get(0), numericKeys[base + j]);
                          VarcharType.VARCHAR.writeSlice(builders[1], slice);
                        }
                        BigintType.BIGINT.writeLong(builders[numGroupKeys], counts[base + j]);
                      }
                    }
                    Block[] blocks = new Block[totalColumns];
                    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
                    return List.of(new Page(blocks));
                  }
                } // end of !overflowed else block
              }
            }
          }
          // Fall through to FlatTwoKeyMap path if ordinal-indexed path not applicable
        }

        // Check if all aggregates can use flat long[] storage
        boolean canUseFlatVarchar = true;
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i]) {
            AggSpec spec = specs.get(i);
            if (isVarcharArg[i] || isDoubleArg[i]) {
              canUseFlatVarchar = false;
              break;
            }
            switch (spec.funcName) {
              case "COUNT":
                if (spec.isDistinct) canUseFlatVarchar = false;
                break;
              case "SUM":
              case "AVG":
                // OK for long args
                break;
              default:
                canUseFlatVarchar = false;
                break;
            }
            if (!canUseFlatVarchar) break;
          }
        }

        if (canUseFlatVarchar) {
          // === Flat two-key varchar path ===
          // Stores accumulator state in contiguous long[] array, eliminating per-group
          // AccumulatorGroup + MergeableAccumulator allocation. Critical for Q17/Q18.
          final int[] flatAccOffset = new int[numAggs];
          int flatTotalSlots = 0;
          for (int i = 0; i < numAggs; i++) {
            flatAccOffset[i] = flatTotalSlots;
            if (isCountStar[i] || accType[i] == 0 || accType[i] == 1) {
              flatTotalSlots += 1;
            } else if (accType[i] == 2) { // AVG
              flatTotalSlots += 2;
            }
          }
          final int flatSlotsPerGroup = flatTotalSlots;
          final FlatTwoKeyMap flatMap = new FlatTwoKeyMap(flatSlotsPerGroup);
          final Object[][] keyReadersHolder = new Object[1][];
          final boolean key0Varchar = keyInfos.get(0).isVarchar;
          final boolean key1Varchar = keyInfos.get(1).isVarchar;

          if (query instanceof MatchAllDocsQuery) {
            // === MatchAllDocsQuery fast path ===
            // Iterate docs directly without Lucene's Collector/BulkScorer framework.
            // Eliminates virtual dispatch overhead per doc and BulkScorer construction.
            LeafReaderContext leafCtx = leaves.get(0);
            LeafReader reader = leafCtx.reader();
            int maxDoc = reader.maxDoc();
            Bits liveDocs = reader.getLiveDocs();

            // Open key readers
            Object[] keyReaders = new Object[2];
            for (int i = 0; i < 2; i++) {
              KeyInfo ki = keyInfos.get(i);
              if (ki.isVarchar) {
                keyReaders[i] = reader.getSortedSetDocValues(ki.name);
              } else {
                keyReaders[i] = reader.getSortedNumericDocValues(ki.name);
              }
            }
            keyReadersHolder[0] = keyReaders;

            // Open aggregate doc values
            SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
            for (int i = 0; i < numAggs; i++) {
              if (!isCountStar[i]) {
                numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
              }
            }

            // Check for count-star-only fast path (no aggregate DV reads needed)
            boolean allCountStarFlat = true;
            for (int i = 0; i < numAggs; i++) {
              if (!isCountStar[i]) {
                allCountStarFlat = false;
                break;
              }
            }

            if (allCountStarFlat) {
              // Ultra-fast path: COUNT(*) only -- no aggregate DV reads
              if (liveDocs == null) {
                for (int doc = 0; doc < maxDoc; doc++) {
                  long k0 = 0, k1 = 0;
                  if (key0Varchar) {
                    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                    if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                  } else {
                    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                    if (dv != null && dv.advanceExact(doc)) {
                      k0 = dv.nextValue();
                      if (truncUnits[0] != null) k0 = truncateMillis(k0, truncUnits[0]);
                      else if (arithUnits[0] != null) k0 = applyArith(k0, arithUnits[0]);
                    }
                  }
                  if (key1Varchar) {
                    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                    if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                  } else {
                    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                    if (dv != null && dv.advanceExact(doc)) {
                      k1 = dv.nextValue();
                      if (truncUnits[1] != null) k1 = truncateMillis(k1, truncUnits[1]);
                      else if (arithUnits[1] != null) k1 = applyArith(k1, arithUnits[1]);
                    }
                  }
                  int slot = flatMap.findOrInsert(k0, k1);
                  flatMap.accData[slot * flatSlotsPerGroup]++;
                }
              } else {
                for (int doc = 0; doc < maxDoc; doc++) {
                  if (!liveDocs.get(doc)) continue;
                  long k0 = 0, k1 = 0;
                  if (key0Varchar) {
                    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                    if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                  } else {
                    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                    if (dv != null && dv.advanceExact(doc)) {
                      k0 = dv.nextValue();
                      if (truncUnits[0] != null) k0 = truncateMillis(k0, truncUnits[0]);
                      else if (arithUnits[0] != null) k0 = applyArith(k0, arithUnits[0]);
                    }
                  }
                  if (key1Varchar) {
                    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                    if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                  } else {
                    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                    if (dv != null && dv.advanceExact(doc)) {
                      k1 = dv.nextValue();
                      if (truncUnits[1] != null) k1 = truncateMillis(k1, truncUnits[1]);
                      else if (arithUnits[1] != null) k1 = applyArith(k1, arithUnits[1]);
                    }
                  }
                  int slot = flatMap.findOrInsert(k0, k1);
                  flatMap.accData[slot * flatSlotsPerGroup]++;
                }
              }
            } else {
              // General MatchAllDocsQuery fast path with aggregate reads
              if (liveDocs == null) {
                for (int doc = 0; doc < maxDoc; doc++) {
                  long k0 = 0, k1 = 0;
                  if (key0Varchar) {
                    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                    if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                  } else {
                    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                    if (dv != null && dv.advanceExact(doc)) {
                      k0 = dv.nextValue();
                      if (truncUnits[0] != null) k0 = truncateMillis(k0, truncUnits[0]);
                      else if (arithUnits[0] != null) k0 = applyArith(k0, arithUnits[0]);
                    }
                  }
                  if (key1Varchar) {
                    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                    if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                  } else {
                    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                    if (dv != null && dv.advanceExact(doc)) {
                      k1 = dv.nextValue();
                      if (truncUnits[1] != null) k1 = truncateMillis(k1, truncUnits[1]);
                      else if (arithUnits[1] != null) k1 = applyArith(k1, arithUnits[1]);
                    }
                  }
                  int slot = flatMap.findOrInsert(k0, k1);
                  int base = slot * flatSlotsPerGroup;
                  for (int i = 0; i < numAggs; i++) {
                    int off = base + flatAccOffset[i];
                    if (isCountStar[i]) {
                      flatMap.accData[off]++;
                      continue;
                    }
                    SortedNumericDocValues aggDv = numericAggDvs[i];
                    if (aggDv != null && aggDv.advanceExact(doc)) {
                      long rawVal = aggDv.nextValue();
                      switch (accType[i]) {
                        case 0:
                          flatMap.accData[off]++;
                          break;
                        case 1:
                          flatMap.accData[off] += rawVal;
                          break;
                        case 2:
                          flatMap.accData[off] += rawVal;
                          flatMap.accData[off + 1]++;
                          break;
                      }
                    }
                  }
                }
              } else {
                for (int doc = 0; doc < maxDoc; doc++) {
                  if (!liveDocs.get(doc)) continue;
                  long k0 = 0, k1 = 0;
                  if (key0Varchar) {
                    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                    if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                  } else {
                    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                    if (dv != null && dv.advanceExact(doc)) {
                      k0 = dv.nextValue();
                      if (truncUnits[0] != null) k0 = truncateMillis(k0, truncUnits[0]);
                      else if (arithUnits[0] != null) k0 = applyArith(k0, arithUnits[0]);
                    }
                  }
                  if (key1Varchar) {
                    SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                    if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                  } else {
                    SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                    if (dv != null && dv.advanceExact(doc)) {
                      k1 = dv.nextValue();
                      if (truncUnits[1] != null) k1 = truncateMillis(k1, truncUnits[1]);
                      else if (arithUnits[1] != null) k1 = applyArith(k1, arithUnits[1]);
                    }
                  }
                  int slot = flatMap.findOrInsert(k0, k1);
                  int base = slot * flatSlotsPerGroup;
                  for (int i = 0; i < numAggs; i++) {
                    int off = base + flatAccOffset[i];
                    if (isCountStar[i]) {
                      flatMap.accData[off]++;
                      continue;
                    }
                    SortedNumericDocValues aggDv = numericAggDvs[i];
                    if (aggDv != null && aggDv.advanceExact(doc)) {
                      long rawVal = aggDv.nextValue();
                      switch (accType[i]) {
                        case 0:
                          flatMap.accData[off]++;
                          break;
                        case 1:
                          flatMap.accData[off] += rawVal;
                          break;
                        case 2:
                          flatMap.accData[off] += rawVal;
                          flatMap.accData[off + 1]++;
                          break;
                      }
                    }
                  }
                }
              }
            }
          } else {
            // === General Collector path for filtered queries ===
            engineSearcher.search(
                query,
                new Collector() {
                  @Override
                  public LeafCollector getLeafCollector(LeafReaderContext context)
                      throws IOException {
                    Object[] keyReaders = new Object[2];
                    for (int i = 0; i < 2; i++) {
                      KeyInfo ki = keyInfos.get(i);
                      if (ki.isVarchar) {
                        keyReaders[i] = context.reader().getSortedSetDocValues(ki.name);
                      } else {
                        keyReaders[i] = context.reader().getSortedNumericDocValues(ki.name);
                      }
                    }
                    keyReadersHolder[0] = keyReaders;

                    final SortedNumericDocValues[] numericAggDvs =
                        new SortedNumericDocValues[numAggs];
                    for (int i = 0; i < numAggs; i++) {
                      if (!isCountStar[i]) {
                        numericAggDvs[i] =
                            context.reader().getSortedNumericDocValues(specs.get(i).arg);
                      }
                    }

                    return new LeafCollector() {
                      @Override
                      public void setScorer(Scorable scorer) {}

                      @Override
                      public void collect(int doc) throws IOException {
                        long k0 = 0, k1 = 0;
                        if (key0Varchar) {
                          SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                          if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                        } else {
                          SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                          if (dv != null && dv.advanceExact(doc)) {
                            k0 = dv.nextValue();
                            if (truncUnits[0] != null) k0 = truncateMillis(k0, truncUnits[0]);
                            else if (arithUnits[0] != null) k0 = applyArith(k0, arithUnits[0]);
                          }
                        }
                        if (key1Varchar) {
                          SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                          if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                        } else {
                          SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                          if (dv != null && dv.advanceExact(doc)) {
                            k1 = dv.nextValue();
                            if (truncUnits[1] != null) k1 = truncateMillis(k1, truncUnits[1]);
                            else if (arithUnits[1] != null) k1 = applyArith(k1, arithUnits[1]);
                          }
                        }
                        int slot = flatMap.findOrInsert(k0, k1);
                        int base = slot * flatSlotsPerGroup;

                        for (int i = 0; i < numAggs; i++) {
                          int off = base + flatAccOffset[i];
                          if (isCountStar[i]) {
                            flatMap.accData[off]++;
                            continue;
                          }
                          SortedNumericDocValues aggDv = numericAggDvs[i];
                          if (aggDv != null && aggDv.advanceExact(doc)) {
                            long rawVal = aggDv.nextValue();
                            switch (accType[i]) {
                              case 0:
                                flatMap.accData[off]++;
                                break;
                              case 1:
                                flatMap.accData[off] += rawVal;
                                break;
                              case 2:
                                flatMap.accData[off] += rawVal;
                                flatMap.accData[off + 1]++;
                                break;
                            }
                          }
                        }
                      }
                    };
                  }

                  @Override
                  public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                  }
                });
          } // end of general Collector path

          if (flatMap.size == 0) return List.of();

          // Build output Page from FlatTwoKeyMap, resolving varchar ordinals to UTF-8.
          // When top-N is requested, use a min-heap to select only the top-N slots,
          // avoiding ordinal resolution and Page construction for the remaining groups.
          Object[] keyReaders = keyReadersHolder[0];
          int numGroupKeys = groupByKeys.size();
          int totalColumns = numGroupKeys + numAggs;

          // Determine which slots to output
          int[] outputSlots;
          int outputCount;

          if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
            // Top-N selection using a min-heap (for DESC) or max-heap (for ASC)
            // on the sort aggregate value from the flat accData array.
            int sortAccOff = flatAccOffset[sortAggIndex];
            int n = (int) Math.min(topN, flatMap.size);
            // Use an int[] heap storing slot indices, ordered by sort value
            int[] heap = new int[n];
            int heapSize = 0;

            for (int slot = 0; slot < flatMap.capacity; slot++) {
              if (!flatMap.occupied[slot]) continue;
              long val = flatMap.accData[slot * flatSlotsPerGroup + sortAccOff];
              if (heapSize < n) {
                heap[heapSize] = slot;
                heapSize++;
                // Sift up
                int k = heapSize - 1;
                while (k > 0) {
                  int parent = (k - 1) >>> 1;
                  long pVal = flatMap.accData[heap[parent] * flatSlotsPerGroup + sortAccOff];
                  long kVal = flatMap.accData[heap[k] * flatSlotsPerGroup + sortAccOff];
                  boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
                  if (swap) {
                    int tmp = heap[parent];
                    heap[parent] = heap[k];
                    heap[k] = tmp;
                    k = parent;
                  } else {
                    break;
                  }
                }
              } else {
                // Compare with heap root (the worst element in our top-N)
                long rootVal = flatMap.accData[heap[0] * flatSlotsPerGroup + sortAccOff];
                boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
                if (better) {
                  heap[0] = slot;
                  // Sift down
                  int k = 0;
                  while (true) {
                    int left = 2 * k + 1;
                    if (left >= heapSize) break;
                    int right = left + 1;
                    int target = left;
                    if (right < heapSize) {
                      long lv = flatMap.accData[heap[left] * flatSlotsPerGroup + sortAccOff];
                      long rv = flatMap.accData[heap[right] * flatSlotsPerGroup + sortAccOff];
                      boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                      if (pickRight) target = right;
                    }
                    long kVal = flatMap.accData[heap[k] * flatSlotsPerGroup + sortAccOff];
                    long tVal = flatMap.accData[heap[target] * flatSlotsPerGroup + sortAccOff];
                    boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
                    if (swap) {
                      int tmp = heap[k];
                      heap[k] = heap[target];
                      heap[target] = tmp;
                      k = target;
                    } else {
                      break;
                    }
                  }
                }
              }
            }
            // Sort the heap slots by value in the desired order for output
            // (for DESC: highest first; for ASC: lowest first)
            final int sOff = sortAccOff;
            final int spg = flatSlotsPerGroup;
            java.util.Arrays.sort(heap, 0, heapSize);
            // Re-sort by actual value (Arrays.sort on int[] doesn't take comparator)
            // Use a simple insertion sort for small N
            for (int i = 1; i < heapSize; i++) {
              int key = heap[i];
              long keyVal = flatMap.accData[key * spg + sOff];
              int j = i - 1;
              while (j >= 0) {
                long jVal = flatMap.accData[heap[j] * spg + sOff];
                boolean needsSwap = sortAscending ? (jVal > keyVal) : (jVal < keyVal);
                if (needsSwap) {
                  heap[j + 1] = heap[j];
                  j--;
                } else {
                  break;
                }
              }
              heap[j + 1] = key;
            }
            outputSlots = heap;
            outputCount = heapSize;
          } else if (topN > 0 && topN < flatMap.size) {
            // Limit without sort: just take the first topN occupied slots
            int n = (int) Math.min(topN, flatMap.size);
            outputSlots = new int[n];
            int idx = 0;
            for (int slot = 0; slot < flatMap.capacity && idx < n; slot++) {
              if (flatMap.occupied[slot]) {
                outputSlots[idx++] = slot;
              }
            }
            outputCount = idx;
          } else {
            // No top-N: output all slots
            outputSlots = new int[flatMap.size];
            int idx = 0;
            for (int slot = 0; slot < flatMap.capacity; slot++) {
              if (flatMap.occupied[slot]) {
                outputSlots[idx++] = slot;
              }
            }
            outputCount = idx;
          }

          BlockBuilder[] builders = new BlockBuilder[totalColumns];
          for (int i = 0; i < numGroupKeys; i++) {
            builders[i] = keyInfos.get(i).type.createBlockBuilder(null, outputCount);
          }
          for (int i = 0; i < numAggs; i++) {
            builders[numGroupKeys + i] =
                resolveAggOutputType(specs.get(i), columnTypeMap)
                    .createBlockBuilder(null, outputCount);
          }

          for (int si = 0; si < outputCount; si++) {
            int slot = outputSlots[si];
            // Write key 0
            long kv0 = flatMap.keys0[slot];
            if (keyInfos.get(0).isVarchar) {
              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
              if (dv != null) {
                BytesRef bytes = dv.lookupOrd(kv0);
                VarcharType.VARCHAR.writeSlice(
                    builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              } else {
                VarcharType.VARCHAR.writeSlice(builders[0], Slices.EMPTY_SLICE);
              }
            } else {
              writeNumericKeyValue(builders[0], keyInfos.get(0), kv0);
            }
            // Write key 1
            long kv1 = flatMap.keys1[slot];
            if (keyInfos.get(1).isVarchar) {
              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
              if (dv != null) {
                BytesRef bytes = dv.lookupOrd(kv1);
                VarcharType.VARCHAR.writeSlice(
                    builders[1], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              } else {
                VarcharType.VARCHAR.writeSlice(builders[1], Slices.EMPTY_SLICE);
              }
            } else {
              writeNumericKeyValue(builders[1], keyInfos.get(1), kv1);
            }
            // Write aggregates from flat storage
            int base = slot * flatSlotsPerGroup;
            for (int a = 0; a < numAggs; a++) {
              int off = base + flatAccOffset[a];
              switch (accType[a]) {
                case 0: // COUNT
                case 1: // SUM long
                  BigintType.BIGINT.writeLong(builders[numGroupKeys + a], flatMap.accData[off]);
                  break;
                case 2: // AVG long
                  long sum = flatMap.accData[off];
                  long cnt = flatMap.accData[off + 1];
                  if (cnt == 0) {
                    builders[numGroupKeys + a].appendNull();
                  } else {
                    DoubleType.DOUBLE.writeDouble(builders[numGroupKeys + a], (double) sum / cnt);
                  }
                  break;
              }
            }
          }

          Block[] blocks = new Block[totalColumns];
          for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
          return List.of(new Page(blocks));
        }

        // === Non-flat two-key path (has complex aggregates) ===
        final TwoKeyHashMap twoKeyMap = new TwoKeyHashMap(specs);
        final Object[][] keyReadersHolder = new Object[1][];

        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                Object[] keyReaders = new Object[2];
                for (int i = 0; i < 2; i++) {
                  KeyInfo ki = keyInfos.get(i);
                  if (ki.isVarchar) {
                    keyReaders[i] = context.reader().getSortedSetDocValues(ki.name);
                  } else {
                    keyReaders[i] = context.reader().getSortedNumericDocValues(ki.name);
                  }
                }
                keyReadersHolder[0] = keyReaders;

                final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    AggSpec spec = specs.get(i);
                    if (isVarcharArg[i]) {
                      varcharAggDvs[i] = context.reader().getSortedSetDocValues(spec.arg);
                    } else {
                      numericAggDvs[i] = context.reader().getSortedNumericDocValues(spec.arg);
                    }
                  }
                }

                // Pre-extract key readers with proper types
                final boolean key0Varchar = keyInfos.get(0).isVarchar;
                final boolean key1Varchar = keyInfos.get(1).isVarchar;

                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    long k0 = 0, k1 = 0;
                    if (key0Varchar) {
                      SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                      if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                    } else {
                      SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                      if (dv != null && dv.advanceExact(doc)) {
                        k0 = dv.nextValue();
                        if (truncUnits[0] != null) k0 = truncateMillis(k0, truncUnits[0]);
                      }
                    }
                    if (key1Varchar) {
                      SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                      if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                    } else {
                      SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                      if (dv != null && dv.advanceExact(doc)) {
                        k1 = dv.nextValue();
                        if (truncUnits[1] != null) k1 = truncateMillis(k1, truncUnits[1]);
                      }
                    }
                    AccumulatorGroup accGroup = twoKeyMap.getOrCreate(k0, k1);

                    // Inline accumulation with pre-computed dispatch flags
                    for (int i = 0; i < numAggs; i++) {
                      MergeableAccumulator acc = accGroup.accumulators[i];
                      if (isCountStar[i]) {
                        ((CountStarAccum) acc).count++;
                        continue;
                      }
                      // Handle varchar aggregate args
                      if (isVarcharArg[i]) {
                        SortedSetDocValues varcharDv = varcharAggDvs[i];
                        if (varcharDv != null && varcharDv.advanceExact(doc)) {
                          BytesRef bytes = varcharDv.lookupOrd(varcharDv.nextOrd());
                          String val = bytes.utf8ToString();
                          switch (accType[i]) {
                            case 5: // COUNT(DISTINCT)
                              ((CountDistinctAccum) acc).objectDistinctValues.add(val);
                              break;
                            case 3: // MIN
                              MinAccum ma = (MinAccum) acc;
                              if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                                ma.objectVal = val;
                                ma.hasValue = true;
                              }
                              break;
                            case 4: // MAX
                              MaxAccum xa = (MaxAccum) acc;
                              if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                                xa.objectVal = val;
                                xa.hasValue = true;
                              }
                              break;
                          }
                        }
                        continue;
                      }
                      SortedNumericDocValues aggDv = numericAggDvs[i];
                      if (aggDv != null && aggDv.advanceExact(doc)) {
                        long rawVal = aggDv.nextValue();
                        switch (accType[i]) {
                          case 0: // COUNT (non-star, non-distinct)
                            ((CountStarAccum) acc).count++;
                            break;
                          case 1: // SUM long
                            SumAccum sa = (SumAccum) acc;
                            sa.hasValue = true;
                            sa.longSum += rawVal;
                            break;
                          case 2: // AVG long
                            AvgAccum aa = (AvgAccum) acc;
                            aa.count++;
                            aa.longSum += rawVal;
                            break;
                          case 3: // MIN
                            MinAccum mna = (MinAccum) acc;
                            mna.hasValue = true;
                            if (isDoubleArg[i]) {
                              double d = Double.longBitsToDouble(rawVal);
                              if (d < mna.doubleVal) mna.doubleVal = d;
                            } else {
                              if (rawVal < mna.longVal) mna.longVal = rawVal;
                            }
                            break;
                          case 4: // MAX
                            MaxAccum mxa = (MaxAccum) acc;
                            mxa.hasValue = true;
                            if (isDoubleArg[i]) {
                              double d = Double.longBitsToDouble(rawVal);
                              if (d > mxa.doubleVal) mxa.doubleVal = d;
                            } else {
                              if (rawVal > mxa.longVal) mxa.longVal = rawVal;
                            }
                            break;
                          case 5: // COUNT(DISTINCT)
                            CountDistinctAccum cda = (CountDistinctAccum) acc;
                            if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
                            else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
                            break;
                          case 6: // SUM double
                            SumAccum sad = (SumAccum) acc;
                            sad.hasValue = true;
                            sad.doubleSum += Double.longBitsToDouble(rawVal);
                            break;
                          case 7: // AVG double
                            AvgAccum aad = (AvgAccum) acc;
                            aad.count++;
                            aad.doubleSum += Double.longBitsToDouble(rawVal);
                            break;
                        }
                      }
                    }
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });

        if (twoKeyMap.size == 0) {
          return List.of();
        }

        // Build output Page from TwoKeyHashMap, resolving varchar ordinals to UTF-8
        Object[] keyReaders = keyReadersHolder[0];
        int numGroupKeys = groupByKeys.size();
        int totalColumns = numGroupKeys + numAggs;
        int groupCount = twoKeyMap.size;

        BlockBuilder[] builders = new BlockBuilder[totalColumns];
        for (int i = 0; i < numGroupKeys; i++) {
          builders[i] = keyInfos.get(i).type.createBlockBuilder(null, groupCount);
        }
        for (int i = 0; i < numAggs; i++) {
          builders[numGroupKeys + i] =
              resolveAggOutputType(specs.get(i), columnTypeMap)
                  .createBlockBuilder(null, groupCount);
        }

        for (int slot = 0; slot < twoKeyMap.capacity; slot++) {
          if (twoKeyMap.occupied[slot]) {
            // Write key 0
            long kv0 = twoKeyMap.keys0[slot];
            if (keyInfos.get(0).isVarchar) {
              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
              if (dv != null) {
                BytesRef bytes = dv.lookupOrd(kv0);
                VarcharType.VARCHAR.writeSlice(
                    builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              } else {
                VarcharType.VARCHAR.writeSlice(builders[0], Slices.EMPTY_SLICE);
              }
            } else {
              writeNumericKeyValue(builders[0], keyInfos.get(0), kv0);
            }
            // Write key 1
            long kv1 = twoKeyMap.keys1[slot];
            if (keyInfos.get(1).isVarchar) {
              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
              if (dv != null) {
                BytesRef bytes = dv.lookupOrd(kv1);
                VarcharType.VARCHAR.writeSlice(
                    builders[1], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              } else {
                VarcharType.VARCHAR.writeSlice(builders[1], Slices.EMPTY_SLICE);
              }
            } else {
              writeNumericKeyValue(builders[1], keyInfos.get(1), kv1);
            }
            AccumulatorGroup accGroup = twoKeyMap.groups[slot];
            for (int a = 0; a < numAggs; a++) {
              accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
            }
          }
        }

        Block[] blocks = new Block[totalColumns];
        for (int i = 0; i < totalColumns; i++) {
          blocks[i] = builders[i].build();
        }
        return List.of(new Page(blocks));
      }

      if (singleSegment) {
        // === Single-segment N-key path (3+ keys) ===
        // Uses SegmentGroupKey map with ordinals for varchar keys.
        Map<SegmentGroupKey, AccumulatorGroup> segmentGroups = new HashMap<>();
        final Object[][] keyReadersHolder = new Object[1][];

        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                Object[] keyReaders = new Object[keyInfos.size()];
                for (int i = 0; i < keyInfos.size(); i++) {
                  KeyInfo ki = keyInfos.get(i);
                  if (ki.isVarchar) {
                    keyReaders[i] = context.reader().getSortedSetDocValues(ki.name);
                  } else {
                    keyReaders[i] = context.reader().getSortedNumericDocValues(ki.name);
                  }
                }
                keyReadersHolder[0] = keyReaders;

                final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    AggSpec spec = specs.get(i);
                    if (isVarcharArg[i]) {
                      varcharAggDvs[i] = context.reader().getSortedSetDocValues(spec.arg);
                    } else {
                      numericAggDvs[i] = context.reader().getSortedNumericDocValues(spec.arg);
                    }
                  }
                }

                final int numKeys = keyInfos.size();
                final NumericProbeKey probeKey = new NumericProbeKey(numKeys);

                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    probeKey.reset();
                    for (int k = 0; k < numKeys; k++) {
                      KeyInfo ki = keyInfos.get(k);
                      if (ki.isVarchar) {
                        SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
                        if (dv != null && dv.advanceExact(doc)) {
                          probeKey.set(k, dv.nextOrd());
                        } else {
                          probeKey.setNull(k);
                        }
                      } else {
                        SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[k];
                        if (dv != null && dv.advanceExact(doc)) {
                          long val = dv.nextValue();
                          if (truncUnits[k] != null) {
                            val = truncateMillis(val, truncUnits[k]);
                          } else if (arithUnits[k] != null) {
                            val = applyArith(val, arithUnits[k]);
                          }
                          probeKey.set(k, val);
                        } else {
                          probeKey.setNull(k);
                        }
                      }
                    }
                    probeKey.computeHash();

                    AccumulatorGroup accGroup = segmentGroups.get(probeKey);
                    if (accGroup == null) {
                      SegmentGroupKey immutableKey = probeKey.toImmutableKey();
                      accGroup = createAccumulatorGroup(specs);
                      segmentGroups.put(immutableKey, accGroup);
                    }

                    // Inline accumulation with pre-computed dispatch flags
                    for (int i = 0; i < numAggs; i++) {
                      MergeableAccumulator acc = accGroup.accumulators[i];
                      if (isCountStar[i]) {
                        ((CountStarAccum) acc).count++;
                        continue;
                      }
                      if (isVarcharArg[i]) {
                        SortedSetDocValues varcharDv = varcharAggDvs[i];
                        if (varcharDv != null && varcharDv.advanceExact(doc)) {
                          BytesRef bytes = varcharDv.lookupOrd(varcharDv.nextOrd());
                          String val = bytes.utf8ToString();
                          switch (accType[i]) {
                            case 5:
                              ((CountDistinctAccum) acc).objectDistinctValues.add(val);
                              break;
                            case 3:
                              MinAccum ma = (MinAccum) acc;
                              if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                                ma.objectVal = val;
                                ma.hasValue = true;
                              }
                              break;
                            case 4:
                              MaxAccum xa = (MaxAccum) acc;
                              if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                                xa.objectVal = val;
                                xa.hasValue = true;
                              }
                              break;
                          }
                        }
                        continue;
                      }
                      SortedNumericDocValues aggDv = numericAggDvs[i];
                      if (aggDv != null && aggDv.advanceExact(doc)) {
                        long rawVal = aggDv.nextValue();
                        switch (accType[i]) {
                          case 0:
                            ((CountStarAccum) acc).count++;
                            break;
                          case 1:
                            SumAccum sa = (SumAccum) acc;
                            sa.hasValue = true;
                            sa.longSum += rawVal;
                            break;
                          case 2:
                            AvgAccum aa = (AvgAccum) acc;
                            aa.count++;
                            aa.longSum += rawVal;
                            break;
                          case 3:
                            MinAccum mna = (MinAccum) acc;
                            mna.hasValue = true;
                            if (isDoubleArg[i]) {
                              double d = Double.longBitsToDouble(rawVal);
                              if (d < mna.doubleVal) mna.doubleVal = d;
                            } else {
                              if (rawVal < mna.longVal) mna.longVal = rawVal;
                            }
                            break;
                          case 4:
                            MaxAccum mxa = (MaxAccum) acc;
                            mxa.hasValue = true;
                            if (isDoubleArg[i]) {
                              double d = Double.longBitsToDouble(rawVal);
                              if (d > mxa.doubleVal) mxa.doubleVal = d;
                            } else {
                              if (rawVal > mxa.longVal) mxa.longVal = rawVal;
                            }
                            break;
                          case 5:
                            CountDistinctAccum cda = (CountDistinctAccum) acc;
                            if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
                            else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
                            break;
                          case 6:
                            SumAccum sad = (SumAccum) acc;
                            sad.hasValue = true;
                            sad.doubleSum += Double.longBitsToDouble(rawVal);
                            break;
                          case 7:
                            AvgAccum aad = (AvgAccum) acc;
                            aad.count++;
                            aad.doubleSum += Double.longBitsToDouble(rawVal);
                            break;
                        }
                      }
                    }
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });

        if (segmentGroups.isEmpty()) {
          return List.of();
        }

        // Build output Page directly from segment ordinals
        Object[] keyReaders = keyReadersHolder[0];
        int numGroupKeys = groupByKeys.size();
        int totalColumns = numGroupKeys + numAggs;
        int groupCount = segmentGroups.size();

        BlockBuilder[] builders = new BlockBuilder[totalColumns];
        for (int i = 0; i < numGroupKeys; i++) {
          builders[i] = keyInfos.get(i).type.createBlockBuilder(null, groupCount);
        }
        for (int i = 0; i < numAggs; i++) {
          builders[numGroupKeys + i] =
              resolveAggOutputType(specs.get(i), columnTypeMap)
                  .createBlockBuilder(null, groupCount);
        }

        for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry : segmentGroups.entrySet()) {
          SegmentGroupKey sgk = entry.getKey();
          AccumulatorGroup accGroup = entry.getValue();

          for (int k = 0; k < numGroupKeys; k++) {
            if (sgk.nulls[k]) {
              builders[k].appendNull();
            } else if (keyInfos.get(k).isVarchar) {
              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
              if (dv != null) {
                BytesRef bytes = dv.lookupOrd(sgk.values[k]);
                VarcharType.VARCHAR.writeSlice(
                    builders[k], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              } else {
                VarcharType.VARCHAR.writeSlice(builders[k], Slices.EMPTY_SLICE);
              }
            } else {
              writeNumericKeyValue(builders[k], keyInfos.get(k), sgk.values[k]);
            }
          }

          for (int a = 0; a < numAggs; a++) {
            accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
          }
        }

        Block[] blocks = new Block[totalColumns];
        for (int i = 0; i < totalColumns; i++) {
          blocks[i] = builders[i].build();
        }
        return List.of(new Page(blocks));
      }

      // === Multi-segment path ===
      // Uses BytesRefKey (raw UTF-8 bytes) for cross-segment merge to avoid String allocation
      Map<MergedGroupKey, AccumulatorGroup> globalGroups = new LinkedHashMap<>();

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              Object[] keyReaders = new Object[keyInfos.size()];
              for (int i = 0; i < keyInfos.size(); i++) {
                KeyInfo ki = keyInfos.get(i);
                if (ki.isVarchar) {
                  keyReaders[i] = context.reader().getSortedSetDocValues(ki.name);
                } else {
                  keyReaders[i] = context.reader().getSortedNumericDocValues(ki.name);
                }
              }

              // Open agg doc values with typed arrays to avoid per-doc casting
              final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
              final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i]) {
                  AggSpec spec = specs.get(i);
                  if (isVarcharArg[i]) {
                    varcharAggDvs[i] = context.reader().getSortedSetDocValues(spec.arg);
                  } else {
                    numericAggDvs[i] = context.reader().getSortedNumericDocValues(spec.arg);
                  }
                }
              }

              Map<SegmentGroupKey, AccumulatorGroup> segmentGroups = new HashMap<>();
              final int numKeys = keyInfos.size();
              final NumericProbeKey probeKey = new NumericProbeKey(numKeys);

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  probeKey.reset();
                  for (int k = 0; k < numKeys; k++) {
                    KeyInfo ki = keyInfos.get(k);
                    if (ki.isVarchar) {
                      SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
                      if (dv != null && dv.advanceExact(doc)) {
                        probeKey.set(k, dv.nextOrd());
                      } else {
                        probeKey.setNull(k);
                      }
                    } else {
                      SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[k];
                      if (dv != null && dv.advanceExact(doc)) {
                        long val = dv.nextValue();
                        if (truncUnits[k] != null) {
                          val = truncateMillis(val, truncUnits[k]);
                        } else if (arithUnits[k] != null) {
                          val = applyArith(val, arithUnits[k]);
                        }
                        probeKey.set(k, val);
                      } else {
                        probeKey.setNull(k);
                      }
                    }
                  }
                  probeKey.computeHash();

                  AccumulatorGroup accGroup = segmentGroups.get(probeKey);
                  if (accGroup == null) {
                    SegmentGroupKey immutableKey = probeKey.toImmutableKey();
                    accGroup = createAccumulatorGroup(specs);
                    segmentGroups.put(immutableKey, accGroup);
                  }

                  // Inline accumulation with pre-computed dispatch flags
                  for (int i = 0; i < numAggs; i++) {
                    MergeableAccumulator acc = accGroup.accumulators[i];
                    if (isCountStar[i]) {
                      ((CountStarAccum) acc).count++;
                      continue;
                    }
                    if (isVarcharArg[i]) {
                      SortedSetDocValues varcharDv = varcharAggDvs[i];
                      if (varcharDv != null && varcharDv.advanceExact(doc)) {
                        BytesRef bytes = varcharDv.lookupOrd(varcharDv.nextOrd());
                        String val = bytes.utf8ToString();
                        switch (accType[i]) {
                          case 5:
                            ((CountDistinctAccum) acc).objectDistinctValues.add(val);
                            break;
                          case 3:
                            MinAccum ma = (MinAccum) acc;
                            if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                              ma.objectVal = val;
                              ma.hasValue = true;
                            }
                            break;
                          case 4:
                            MaxAccum xa = (MaxAccum) acc;
                            if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                              xa.objectVal = val;
                              xa.hasValue = true;
                            }
                            break;
                        }
                      }
                      continue;
                    }
                    SortedNumericDocValues aggDv = numericAggDvs[i];
                    if (aggDv != null && aggDv.advanceExact(doc)) {
                      long rawVal = aggDv.nextValue();
                      switch (accType[i]) {
                        case 0:
                          ((CountStarAccum) acc).count++;
                          break;
                        case 1:
                          SumAccum sa = (SumAccum) acc;
                          sa.hasValue = true;
                          sa.longSum += rawVal;
                          break;
                        case 2:
                          AvgAccum aa = (AvgAccum) acc;
                          aa.count++;
                          aa.longSum += rawVal;
                          break;
                        case 3:
                          MinAccum mna = (MinAccum) acc;
                          mna.hasValue = true;
                          if (isDoubleArg[i]) {
                            double d = Double.longBitsToDouble(rawVal);
                            if (d < mna.doubleVal) mna.doubleVal = d;
                          } else {
                            if (rawVal < mna.longVal) mna.longVal = rawVal;
                          }
                          break;
                        case 4:
                          MaxAccum mxa = (MaxAccum) acc;
                          mxa.hasValue = true;
                          if (isDoubleArg[i]) {
                            double d = Double.longBitsToDouble(rawVal);
                            if (d > mxa.doubleVal) mxa.doubleVal = d;
                          } else {
                            if (rawVal > mxa.longVal) mxa.longVal = rawVal;
                          }
                          break;
                        case 5:
                          CountDistinctAccum cda = (CountDistinctAccum) acc;
                          if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
                          else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
                          break;
                        case 6:
                          SumAccum sad = (SumAccum) acc;
                          sad.hasValue = true;
                          sad.doubleSum += Double.longBitsToDouble(rawVal);
                          break;
                        case 7:
                          AvgAccum aad = (AvgAccum) acc;
                          aad.count++;
                          aad.doubleSum += Double.longBitsToDouble(rawVal);
                          break;
                      }
                    }
                  }
                }

                @Override
                public void finish() throws IOException {
                  for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry :
                      segmentGroups.entrySet()) {
                    SegmentGroupKey sgk = entry.getKey();
                    AccumulatorGroup segAccs = entry.getValue();

                    Object[] resolvedKeys = new Object[keyInfos.size()];
                    for (int k = 0; k < keyInfos.size(); k++) {
                      if (sgk.nulls[k]) {
                        resolvedKeys[k] = null;
                      } else if (keyInfos.get(k).isVarchar) {
                        SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
                        if (dv != null) {
                          BytesRef bytes = dv.lookupOrd(sgk.values[k]);
                          resolvedKeys[k] = new BytesRefKey(bytes);
                        } else {
                          resolvedKeys[k] = new BytesRefKey(new BytesRef(""));
                        }
                      } else {
                        resolvedKeys[k] = sgk.values[k];
                      }
                    }

                    MergedGroupKey mgk = new MergedGroupKey(resolvedKeys, keyInfos);
                    AccumulatorGroup existing = globalGroups.get(mgk);
                    if (existing == null) {
                      globalGroups.put(mgk, segAccs);
                    } else {
                      existing.merge(segAccs);
                    }
                  }
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });

      if (globalGroups.isEmpty()) {
        return List.of();
      }

      int numGroupKeys = groupByKeys.size();
      int totalColumns = numGroupKeys + numAggs;
      int groupCount = globalGroups.size();

      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      for (int i = 0; i < numGroupKeys; i++) {
        builders[i] = keyInfos.get(i).type.createBlockBuilder(null, groupCount);
      }
      for (int i = 0; i < numAggs; i++) {
        builders[numGroupKeys + i] =
            resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
      }

      for (Map.Entry<MergedGroupKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
        MergedGroupKey key = entry.getKey();
        AccumulatorGroup accGroup = entry.getValue();

        for (int k = 0; k < numGroupKeys; k++) {
          writeKeyValueForMerged(builders[k], keyInfos.get(k), key.values[k]);
        }

        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
        }
      }

      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) {
        blocks[i] = builders[i].build();
      }
      return List.of(new Page(blocks));
    }
  }

  /** Resolve output types for the fused GROUP BY aggregate. */
  public static List<Type> resolveOutputTypes(
      AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    List<Type> types = new ArrayList<>();

    // Group-by key types
    for (String key : aggNode.getGroupByKeys()) {
      Type type = columnTypeMap.get(key);
      if (type != null) {
        types.add(type);
      } else {
        // Check for DATE_TRUNC expression — output type is timestamp
        Matcher dtm = DATE_TRUNC_PATTERN.matcher(key);
        if (dtm.matches()) {
          types.add(TimestampType.TIMESTAMP_MILLIS);
        } else {
          // Check for EXTRACT expression — output type is BigintType
          Matcher em = EXTRACT_PATTERN.matcher(key);
          if (em.matches()) {
            types.add(BigintType.BIGINT);
          } else {
            // Check for arithmetic expression — output type is BigintType
            Matcher am = ARITH_EXPR_PATTERN.matcher(key);
            if (am.matches()) {
              types.add(BigintType.BIGINT);
            } else if (aggNode.getChild() instanceof EvalNode evalNode
                && evalNode.getOutputColumnNames().contains(key)) {
              // EvalNode-computed key — determine type by compilation
              int exprIdx = evalNode.getOutputColumnNames().indexOf(key);
              String exprStr = evalNode.getExpressions().get(exprIdx);
              types.add(compileAndGetType(exprStr, columnTypeMap));
            } else {
              types.add(BigintType.BIGINT);
            }
          }
        }
      }
    }

    // Aggregate output types
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        types.add(BigintType.BIGINT);
        continue;
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      String arg = m.group(3).trim();
      switch (funcName) {
        case "COUNT":
          types.add(BigintType.BIGINT);
          break;
        case "AVG":
          types.add(DoubleType.DOUBLE);
          break;
        case "SUM":
          Type inputType = columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
          types.add(inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT);
          break;
        case "MIN":
        case "MAX":
          types.add(columnTypeMap.getOrDefault(arg, BigintType.BIGINT));
          break;
        default:
          types.add(BigintType.BIGINT);
      }
    }
    return types;
  }

  // =========================================================================
  // Internal structures
  // =========================================================================

  private record AggSpec(String funcName, boolean isDistinct, String arg, Type argType) {}

  /**
   * Metadata for a single GROUP BY key.
   *
   * @param name the column name to read from DocValues (for DATE_TRUNC, this is the source column)
   * @param type the output type of this key
   * @param isVarchar whether this key uses SortedSetDocValues (VARCHAR)
   * @param exprFunc the expression function name, or null for plain column references. Currently
   *     supports "date_trunc".
   * @param exprUnit the unit parameter for expression functions (e.g., "minute" for date_trunc), or
   *     null for plain column references.
   */
  private record KeyInfo(
      String name, Type type, boolean isVarchar, String exprFunc, String exprUnit) {}

  /**
   * Segment-local group key using ordinals for VARCHAR and raw values for numerics. Uses long
   * arrays for minimal allocation and fast hashing. Also serves as the base class for {@link
   * NumericProbeKey} to share the hashCode/equals contract.
   */
  private static class SegmentGroupKey {
    final long[] values;
    final boolean[] nulls;
    int hash;

    /** Standard constructor: copies arrays to ensure immutability. */
    SegmentGroupKey(long[] values, boolean[] nulls) {
      this.values = values.clone();
      this.nulls = nulls.clone();
      int h = 1;
      for (int i = 0; i < values.length; i++) {
        if (nulls[i]) {
          h = h * 31;
        } else {
          h = h * 31 + Long.hashCode(values[i]);
        }
      }
      this.hash = h;
    }

    /** Pre-allocation constructor for mutable probe keys (no cloning). */
    SegmentGroupKey(int size) {
      this.values = new long[size];
      this.nulls = new boolean[size];
      this.hash = 0;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof SegmentGroupKey other)) return false;
      if (this.hash != other.hash) return false;
      for (int i = 0; i < values.length; i++) {
        if (nulls[i] != other.nulls[i]) return false;
        if (!nulls[i] && values[i] != other.values[i]) return false;
      }
      return true;
    }
  }

  /**
   * Mutable probe key for HashMap lookups in the numeric-only GROUP BY path. Reused across
   * documents to avoid per-doc array allocation. The key is populated via {@link #set}/{@link
   * #setNull}, then {@link #computeHash()} is called before using it for HashMap.get().
   *
   * <p>This class intentionally shares the same hashCode/equals contract as {@link SegmentGroupKey}
   * so it can be used to probe a {@code HashMap<SegmentGroupKey, ...>} without creating an
   * immutable key for every document. An immutable copy is only created via {@link
   * #toImmutableKey()} when a new group needs to be inserted.
   */
  private static final class NumericProbeKey extends SegmentGroupKey {

    NumericProbeKey(int size) {
      // Initialize with pre-allocated arrays (no cloning needed for mutable probe)
      super(size);
    }

    /** Reset all nulls flags to false for reuse. Values are overwritten by set(). */
    void reset() {
      // Only need to clear nulls; values will be overwritten
      for (int i = 0; i < nulls.length; i++) {
        nulls[i] = false;
      }
    }

    void set(int index, long value) {
      values[index] = value;
    }

    void setNull(int index) {
      nulls[index] = true;
    }

    void computeHash() {
      int h = 1;
      for (int i = 0; i < values.length; i++) {
        if (nulls[i]) {
          h = h * 31;
        } else {
          h = h * 31 + Long.hashCode(values[i]);
        }
      }
      this.hash = h;
    }

    /** Create an immutable copy for insertion into the HashMap. */
    SegmentGroupKey toImmutableKey() {
      return new SegmentGroupKey(values, nulls);
    }
  }

  /**
   * Cross-segment merged group key using resolved values. VARCHAR keys are stored as BytesRefKey
   * (raw UTF-8 bytes, avoiding String allocation), numeric keys as Long or Double.
   */
  private static final class MergedGroupKey {
    final Object[] values;
    private final int hash;

    MergedGroupKey(Object[] values, List<KeyInfo> keyInfos) {
      this.values = values;
      int h = 1;
      for (Object v : values) {
        h = 31 * h + (v == null ? 0 : v.hashCode());
      }
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof MergedGroupKey other)) return false;
      if (this.hash != other.hash || this.values.length != other.values.length) return false;
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) {
          if (other.values[i] != null) return false;
        } else if (!values[i].equals(other.values[i])) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Immutable byte-array based key for use in HashMaps, avoiding String allocation. Created from
   * Lucene's BytesRef (which is a reference into a shared buffer), this class copies the bytes and
   * pre-computes hashCode. Used as a replacement for String keys in varchar GROUP BY to eliminate
   * the UTF-8-to-Java-char[] round-trip during aggregation.
   */
  private static final class BytesRefKey {
    final byte[] bytes;
    private final int hash;

    BytesRefKey(BytesRef ref) {
      // Copy bytes since BytesRef points into shared Lucene buffer
      this.bytes = new byte[ref.length];
      System.arraycopy(ref.bytes, ref.offset, this.bytes, 0, ref.length);
      this.hash = java.util.Arrays.hashCode(this.bytes);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof BytesRefKey other)) return false;
      return this.hash == other.hash && java.util.Arrays.equals(this.bytes, other.bytes);
    }
  }

  /**
   * Open-addressing hash map with two primitive long keys. Uses linear probing with power-of-two
   * capacity for fast modulo via bitmask. Stores keys in parallel long arrays and AccumulatorGroups
   * in a parallel array, eliminating per-group Entry/SegmentGroupKey object allocation. Hash
   * function uses the Murmur3-inspired mix to reduce collisions for correlated key pairs.
   */
  private static final class TwoKeyHashMap {
    private static final int INITIAL_CAPACITY = 8192;
    private static final float LOAD_FACTOR = 0.7f;

    long[] keys0;
    long[] keys1;
    AccumulatorGroup[] groups;
    boolean[] occupied;
    int size;
    int capacity;
    private int threshold;
    private final List<AggSpec> specs;

    @SuppressWarnings("unchecked")
    TwoKeyHashMap(List<AggSpec> specs) {
      this.specs = specs;
      this.capacity = INITIAL_CAPACITY;
      this.keys0 = new long[capacity];
      this.keys1 = new long[capacity];
      this.groups = new AccumulatorGroup[capacity];
      this.occupied = new boolean[capacity];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    AccumulatorGroup getOrCreate(long key0, long key1) {
      int mask = capacity - 1;
      int slot = hash2(key0, key1) & mask;
      while (occupied[slot]) {
        if (keys0[slot] == key0 && keys1[slot] == key1) {
          return groups[slot];
        }
        slot = (slot + 1) & mask;
      }
      // New entry
      AccumulatorGroup accGroup = createAccumulatorGroup(specs);
      keys0[slot] = key0;
      keys1[slot] = key1;
      groups[slot] = accGroup;
      occupied[slot] = true;
      size++;
      if (size > threshold) {
        resize();
        // After resize, return from the new slot location
        return getExisting(key0, key1);
      }
      return accGroup;
    }

    private AccumulatorGroup getExisting(long key0, long key1) {
      int mask = capacity - 1;
      int slot = hash2(key0, key1) & mask;
      while (occupied[slot]) {
        if (keys0[slot] == key0 && keys1[slot] == key1) {
          return groups[slot];
        }
        slot = (slot + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      int newCapacity = capacity * 2;
      long[] newKeys0 = new long[newCapacity];
      long[] newKeys1 = new long[newCapacity];
      AccumulatorGroup[] newGroups = new AccumulatorGroup[newCapacity];
      boolean[] newOccupied = new boolean[newCapacity];
      int newMask = newCapacity - 1;

      for (int i = 0; i < capacity; i++) {
        if (occupied[i]) {
          int slot = hash2(keys0[i], keys1[i]) & newMask;
          while (newOccupied[slot]) {
            slot = (slot + 1) & newMask;
          }
          newKeys0[slot] = keys0[i];
          newKeys1[slot] = keys1[i];
          newGroups[slot] = groups[i];
          newOccupied[slot] = true;
        }
      }

      this.keys0 = newKeys0;
      this.keys1 = newKeys1;
      this.groups = newGroups;
      this.occupied = newOccupied;
      this.capacity = newCapacity;
      this.threshold = (int) (newCapacity * LOAD_FACTOR);
    }

    /** Combine two long keys into a single hash using Murmur3-inspired mixing. */
    private static int hash2(long k0, long k1) {
      long h = k0 * 0x9E3779B97F4A7C15L + k1;
      h ^= h >>> 33;
      h *= 0xff51afd7ed558ccdL;
      h ^= h >>> 33;
      return (int) h;
    }
  }

  /**
   * Open-addressing hash map with a single primitive long key. Uses linear probing with
   * power-of-two capacity for fast modulo via bitmask. Stores keys in a long array and
   * AccumulatorGroups in a parallel array, eliminating HashMap.Entry, SegmentGroupKey, and
   * NumericProbeKey allocation per group. Hash function uses Murmur3 finalizer for good
   * distribution of sequential integer keys (e.g., RegionID).
   */
  private static final class SingleKeyHashMap {
    private static final int INITIAL_CAPACITY = 4096;
    private static final float LOAD_FACTOR = 0.7f;

    long[] keys;
    AccumulatorGroup[] groups;
    boolean[] occupied;
    int size;
    int capacity;
    private int threshold;
    private final List<AggSpec> specs;

    SingleKeyHashMap(List<AggSpec> specs) {
      this.specs = specs;
      this.capacity = INITIAL_CAPACITY;
      this.keys = new long[capacity];
      this.groups = new AccumulatorGroup[capacity];
      this.occupied = new boolean[capacity];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    AccumulatorGroup getOrCreate(long key) {
      int mask = capacity - 1;
      int slot = hash1(key) & mask;
      while (occupied[slot]) {
        if (keys[slot] == key) {
          return groups[slot];
        }
        slot = (slot + 1) & mask;
      }
      // New entry
      AccumulatorGroup accGroup = createAccumulatorGroup(specs);
      keys[slot] = key;
      groups[slot] = accGroup;
      occupied[slot] = true;
      size++;
      if (size > threshold) {
        resize();
        return getExisting(key);
      }
      return accGroup;
    }

    private AccumulatorGroup getExisting(long key) {
      int mask = capacity - 1;
      int slot = hash1(key) & mask;
      while (occupied[slot]) {
        if (keys[slot] == key) {
          return groups[slot];
        }
        slot = (slot + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      int newCapacity = capacity * 2;
      long[] newKeys = new long[newCapacity];
      AccumulatorGroup[] newGroups = new AccumulatorGroup[newCapacity];
      boolean[] newOccupied = new boolean[newCapacity];
      int newMask = newCapacity - 1;

      for (int i = 0; i < capacity; i++) {
        if (occupied[i]) {
          int slot = hash1(keys[i]) & newMask;
          while (newOccupied[slot]) {
            slot = (slot + 1) & newMask;
          }
          newKeys[slot] = keys[i];
          newGroups[slot] = groups[i];
          newOccupied[slot] = true;
        }
      }

      this.keys = newKeys;
      this.groups = newGroups;
      this.occupied = newOccupied;
      this.capacity = newCapacity;
      this.threshold = (int) (newCapacity * LOAD_FACTOR);
    }

    /** Hash a single long key using Murmur3 finalizer for good distribution. */
    private static int hash1(long key) {
      key ^= key >>> 33;
      key *= 0xff51afd7ed558ccdL;
      key ^= key >>> 33;
      key *= 0xc4ceb9fe1a85ec53L;
      key ^= key >>> 33;
      return (int) key;
    }
  }

  /**
   * Open-addressing hash map with two long keys and contiguous flat accumulator storage. All
   * accumulator data is stored in a single {@code long[]} array at offset {@code slot *
   * slotsPerGroup}, eliminating per-group object allocation entirely. On miss, only the slot's
   * range within the contiguous array is initialized (default 0 from Java array initialization
   * handles most cases). Resize copies accumulator data using {@code System.arraycopy} for
   * efficiency.
   */
  private static final class FlatTwoKeyMap {
    private static final int INITIAL_CAPACITY = 8192;
    private static final float LOAD_FACTOR = 0.7f;

    long[] keys0;
    long[] keys1;
    boolean[] occupied;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    FlatTwoKeyMap(int slotsPerGroup) {
      this.slotsPerGroup = slotsPerGroup;
      this.capacity = INITIAL_CAPACITY;
      this.keys0 = new long[capacity];
      this.keys1 = new long[capacity];
      this.occupied = new boolean[capacity];
      this.accData = new long[capacity * slotsPerGroup];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    /**
     * Find existing slot or insert new entry for the given key pair. For a new entry, initializes
     * the slot in the contiguous accData array to zeros (already guaranteed by Java array
     * initialization or resize copy). Returns the slot index. The caller accumulates into accData
     * at slot*slotsPerGroup.
     */
    int findOrInsert(long key0, long key1) {
      int mask = capacity - 1;
      int h = TwoKeyHashMap.hash2(key0, key1) & mask;
      while (occupied[h]) {
        if (keys0[h] == key0 && keys1[h] == key1) {
          return h;
        }
        h = (h + 1) & mask;
      }
      // New entry
      keys0[h] = key0;
      keys1[h] = key1;
      occupied[h] = true;
      // accData[h*slotsPerGroup..] is already 0 from array init
      size++;
      if (size > threshold) {
        resize();
        // Find the slot in the new layout
        return findExisting(key0, key1);
      }
      return h;
    }

    private int findExisting(long key0, long key1) {
      int mask = capacity - 1;
      int h = TwoKeyHashMap.hash2(key0, key1) & mask;
      while (occupied[h]) {
        if (keys0[h] == key0 && keys1[h] == key1) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      int newCap = capacity * 2;
      long[] nk0 = new long[newCap];
      long[] nk1 = new long[newCap];
      boolean[] nocc = new boolean[newCap];
      long[] nacc = new long[newCap * slotsPerGroup];
      int nm = newCap - 1;
      for (int s = 0; s < capacity; s++) {
        if (occupied[s]) {
          int nh = TwoKeyHashMap.hash2(keys0[s], keys1[s]) & nm;
          while (nocc[nh]) nh = (nh + 1) & nm;
          nk0[nh] = keys0[s];
          nk1[nh] = keys1[s];
          nocc[nh] = true;
          System.arraycopy(accData, s * slotsPerGroup, nacc, nh * slotsPerGroup, slotsPerGroup);
        }
      }
      this.keys0 = nk0;
      this.keys1 = nk1;
      this.occupied = nocc;
      this.accData = nacc;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }
  }

  /**
   * Open-addressing hash map with a single long key and contiguous flat accumulator storage. All
   * accumulator data is stored in a single {@code long[]} array at offset {@code slot *
   * slotsPerGroup}, eliminating per-group object allocation entirely. Same concept as {@link
   * FlatTwoKeyMap} but for single-key GROUP BY.
   *
   * <p>This is the critical optimization for high-cardinality single-key GROUP BY queries like Q16
   * (GROUP BY UserID, COUNT(*)). With ~25K unique groups per shard, this saves ~75K object
   * allocations (AccumulatorGroup + MergeableAccumulator[] + CountStarAccum per group).
   */
  private static final class FlatSingleKeyMap {
    private static final int INITIAL_CAPACITY = 4096;
    private static final float LOAD_FACTOR = 0.7f;

    long[] keys;
    boolean[] occupied;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    FlatSingleKeyMap(int slotsPerGroup) {
      this.slotsPerGroup = slotsPerGroup;
      this.capacity = INITIAL_CAPACITY;
      this.keys = new long[capacity];
      this.occupied = new boolean[capacity];
      this.accData = new long[capacity * slotsPerGroup];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    /**
     * Find existing slot or insert new entry for the given key. Returns the slot index. The caller
     * accumulates into accData at slot*slotsPerGroup.
     */
    int findOrInsert(long key) {
      int mask = capacity - 1;
      int h = SingleKeyHashMap.hash1(key) & mask;
      while (occupied[h]) {
        if (keys[h] == key) {
          return h;
        }
        h = (h + 1) & mask;
      }
      // New entry
      keys[h] = key;
      occupied[h] = true;
      // accData[h*slotsPerGroup..] is already 0 from array init
      size++;
      if (size > threshold) {
        resize();
        return findExisting(key);
      }
      return h;
    }

    private int findExisting(long key) {
      int mask = capacity - 1;
      int h = SingleKeyHashMap.hash1(key) & mask;
      while (occupied[h]) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      int newCap = capacity * 2;
      long[] nk = new long[newCap];
      boolean[] nocc = new boolean[newCap];
      long[] nacc = new long[newCap * slotsPerGroup];
      int nm = newCap - 1;
      for (int s = 0; s < capacity; s++) {
        if (occupied[s]) {
          int nh = SingleKeyHashMap.hash1(keys[s]) & nm;
          while (nocc[nh]) nh = (nh + 1) & nm;
          nk[nh] = keys[s];
          nocc[nh] = true;
          System.arraycopy(accData, s * slotsPerGroup, nacc, nh * slotsPerGroup, slotsPerGroup);
        }
      }
      this.keys = nk;
      this.occupied = nocc;
      this.accData = nacc;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }
  }

  // =========================================================================
  // Accumulator infrastructure
  // =========================================================================

  /** Mergeable accumulator for cross-segment aggregation. */
  private interface MergeableAccumulator {
    void merge(MergeableAccumulator other);

    void writeTo(BlockBuilder builder);

    /**
     * Return a long value suitable for top-N sorting. For COUNT/SUM this is the accumulated value.
     * For AVG this is the count (as a proxy for significance). Default returns 0.
     */
    default long getSortValue() {
      return 0;
    }
  }

  /** Group of accumulators for a single group key. */
  private static final class AccumulatorGroup {
    final MergeableAccumulator[] accumulators;

    AccumulatorGroup(MergeableAccumulator[] accumulators) {
      this.accumulators = accumulators;
    }

    void merge(AccumulatorGroup other) {
      for (int i = 0; i < accumulators.length; i++) {
        accumulators[i].merge(other.accumulators[i]);
      }
    }
  }

  private static AccumulatorGroup createAccumulatorGroup(List<AggSpec> specs) {
    MergeableAccumulator[] accs = new MergeableAccumulator[specs.size()];
    for (int i = 0; i < specs.size(); i++) {
      accs[i] = createAccumulator(specs.get(i));
    }
    return new AccumulatorGroup(accs);
  }

  private static MergeableAccumulator createAccumulator(AggSpec spec) {
    switch (spec.funcName) {
      case "COUNT":
        if (spec.isDistinct) {
          return new CountDistinctAccum(spec.argType);
        }
        if ("*".equals(spec.arg)) {
          return new CountStarAccum();
        }
        return new CountStarAccum();
      case "SUM":
        return new SumAccum(spec.argType);
      case "MIN":
        return new MinAccum(spec.argType);
      case "MAX":
        return new MaxAccum(spec.argType);
      case "AVG":
        return new AvgAccum(spec.argType);
      default:
        throw new UnsupportedOperationException("Unsupported aggregate: " + spec.funcName);
    }
  }

  /**
   * Inline accumulation for single VARCHAR generic path. Uses pre-computed dispatch flags (accType,
   * isCountStar, isVarcharArg, isDoubleArg) for switch-based accumulation, eliminating instanceof
   * chains in the hot loop.
   */
  private static void collectVarcharGenericAccumulate(
      int doc,
      AccumulatorGroup accGroup,
      int numAggs,
      boolean[] isCountStar,
      boolean[] isVarcharArg,
      boolean[] isDoubleArg,
      int[] accType,
      SortedNumericDocValues[] numericAggDvs,
      SortedSetDocValues[] varcharAggDvs)
      throws IOException {
    for (int i = 0; i < numAggs; i++) {
      MergeableAccumulator acc = accGroup.accumulators[i];
      if (isCountStar[i]) {
        ((CountStarAccum) acc).count++;
        continue;
      }
      if (isVarcharArg[i]) {
        SortedSetDocValues varcharDv = varcharAggDvs[i];
        if (varcharDv != null && varcharDv.advanceExact(doc)) {
          BytesRef bytes = varcharDv.lookupOrd(varcharDv.nextOrd());
          String val = bytes.utf8ToString();
          switch (accType[i]) {
            case 5:
              ((CountDistinctAccum) acc).objectDistinctValues.add(val);
              break;
            case 3:
              MinAccum ma = (MinAccum) acc;
              if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                ma.objectVal = val;
                ma.hasValue = true;
              }
              break;
            case 4:
              MaxAccum xa = (MaxAccum) acc;
              if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                xa.objectVal = val;
                xa.hasValue = true;
              }
              break;
          }
        }
        continue;
      }
      SortedNumericDocValues aggDv = numericAggDvs[i];
      if (aggDv != null && aggDv.advanceExact(doc)) {
        long rawVal = aggDv.nextValue();
        switch (accType[i]) {
          case 0:
            ((CountStarAccum) acc).count++;
            break;
          case 1:
            SumAccum sa = (SumAccum) acc;
            sa.hasValue = true;
            sa.longSum += rawVal;
            break;
          case 2:
            AvgAccum aa = (AvgAccum) acc;
            aa.count++;
            aa.longSum += rawVal;
            break;
          case 3:
            MinAccum mna = (MinAccum) acc;
            mna.hasValue = true;
            if (isDoubleArg[i]) {
              double d = Double.longBitsToDouble(rawVal);
              if (d < mna.doubleVal) mna.doubleVal = d;
            } else {
              if (rawVal < mna.longVal) mna.longVal = rawVal;
            }
            break;
          case 4:
            MaxAccum mxa = (MaxAccum) acc;
            mxa.hasValue = true;
            if (isDoubleArg[i]) {
              double d = Double.longBitsToDouble(rawVal);
              if (d > mxa.doubleVal) mxa.doubleVal = d;
            } else {
              if (rawVal > mxa.longVal) mxa.longVal = rawVal;
            }
            break;
          case 5:
            CountDistinctAccum cda = (CountDistinctAccum) acc;
            if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
            else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
            break;
          case 6:
            SumAccum sad = (SumAccum) acc;
            sad.hasValue = true;
            sad.doubleSum += Double.longBitsToDouble(rawVal);
            break;
          case 7:
            AvgAccum aad = (AvgAccum) acc;
            aad.count++;
            aad.doubleSum += Double.longBitsToDouble(rawVal);
            break;
        }
      }
    }
  }

  /** Feed one doc's values into the accumulator group. */
  private static void accumulateDoc(
      int doc, AccumulatorGroup accGroup, List<AggSpec> specs, Object[] aggReaders)
      throws IOException {
    for (int i = 0; i < specs.size(); i++) {
      AggSpec spec = specs.get(i);
      MergeableAccumulator acc = accGroup.accumulators[i];

      if ("*".equals(spec.arg)) {
        ((CountStarAccum) acc).count++;
        continue;
      }

      if (spec.argType instanceof VarcharType) {
        SortedSetDocValues dv = (SortedSetDocValues) aggReaders[i];
        if (dv != null && dv.advanceExact(doc)) {
          BytesRef bytes = dv.lookupOrd(dv.nextOrd());
          String val = bytes.utf8ToString();
          if (acc instanceof CountDistinctAccum cda) {
            cda.objectDistinctValues.add(val);
          } else if (acc instanceof MinAccum ma) {
            if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
              ma.objectVal = val;
              ma.hasValue = true;
            }
          } else if (acc instanceof MaxAccum xa) {
            if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
              xa.objectVal = val;
              xa.hasValue = true;
            }
          }
        }
      } else {
        SortedNumericDocValues dv = (SortedNumericDocValues) aggReaders[i];
        if (dv != null && dv.advanceExact(doc)) {
          long rawVal = dv.nextValue();
          boolean isDouble = spec.argType instanceof DoubleType;
          boolean isTimestamp = spec.argType instanceof TimestampType;

          if (spec.isDistinct && acc instanceof CountDistinctAccum cda) {
            if (cda.usePrimitiveLong) {
              cda.longDistinctValues.add(rawVal);
            } else {
              cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
            }
          } else if (acc instanceof CountStarAccum csa) {
            csa.count++;
          } else if (acc instanceof SumAccum sa) {
            sa.hasValue = true;
            if (isDouble) {
              sa.doubleSum += Double.longBitsToDouble(rawVal);
            } else {
              sa.longSum += rawVal;
            }
          } else if (acc instanceof MinAccum ma) {
            ma.hasValue = true;
            if (isDouble) {
              double d = Double.longBitsToDouble(rawVal);
              if (d < ma.doubleVal) ma.doubleVal = d;
            } else {
              if (rawVal < ma.longVal) ma.longVal = rawVal;
            }
          } else if (acc instanceof MaxAccum xa) {
            xa.hasValue = true;
            if (isDouble) {
              double d = Double.longBitsToDouble(rawVal);
              if (d > xa.doubleVal) xa.doubleVal = d;
            } else {
              if (rawVal > xa.longVal) xa.longVal = rawVal;
            }
          } else if (acc instanceof AvgAccum aa) {
            aa.count++;
            if (isDouble) {
              aa.doubleSum += Double.longBitsToDouble(rawVal);
            } else {
              aa.longSum += rawVal;
            }
          }
        }
      }
    }
  }

  // =========================================================================
  // Accumulator implementations (mergeable)
  // =========================================================================

  private static class CountStarAccum implements MergeableAccumulator {
    long count = 0;

    @Override
    public void merge(MergeableAccumulator other) {
      count += ((CountStarAccum) other).count;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, count);
    }

    @Override
    public long getSortValue() {
      return count;
    }
  }

  private static class SumAccum implements MergeableAccumulator {
    final boolean isDouble;
    long longSum = 0;
    double doubleSum = 0;
    boolean hasValue = false;

    SumAccum(Type argType) {
      this.isDouble = argType instanceof DoubleType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      SumAccum o = (SumAccum) other;
      if (o.hasValue) {
        hasValue = true;
        longSum += o.longSum;
        doubleSum += o.doubleSum;
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isDouble) {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum);
      } else {
        BigintType.BIGINT.writeLong(builder, longSum);
      }
    }

    @Override
    public long getSortValue() {
      return isDouble ? Double.doubleToRawLongBits(doubleSum) : longSum;
    }
  }

  private static class MinAccum implements MergeableAccumulator {
    final Type argType;
    final boolean isDouble;
    final boolean isVarchar;
    final boolean isTimestamp;
    long longVal = Long.MAX_VALUE;
    double doubleVal = Double.MAX_VALUE;
    Object objectVal = null;
    boolean hasValue = false;

    MinAccum(Type argType) {
      this.argType = argType;
      this.isDouble = argType instanceof DoubleType;
      this.isVarchar = argType instanceof VarcharType;
      this.isTimestamp = argType instanceof TimestampType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      MinAccum o = (MinAccum) other;
      if (!o.hasValue) return;
      hasValue = true;
      if (isVarchar) {
        if (objectVal == null || ((String) o.objectVal).compareTo((String) objectVal) < 0) {
          objectVal = o.objectVal;
        }
      } else if (isDouble) {
        if (o.doubleVal < doubleVal) doubleVal = o.doubleVal;
      } else {
        if (o.longVal < longVal) longVal = o.longVal;
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isVarchar) {
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice((String) objectVal));
      } else if (isDouble) {
        DoubleType.DOUBLE.writeDouble(builder, doubleVal);
      } else if (isTimestamp) {
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, longVal * 1000L);
      } else {
        argType.writeLong(builder, longVal);
      }
    }

    @Override
    public long getSortValue() {
      return isDouble ? Double.doubleToRawLongBits(doubleVal) : longVal;
    }
  }

  private static class MaxAccum implements MergeableAccumulator {
    final Type argType;
    final boolean isDouble;
    final boolean isVarchar;
    final boolean isTimestamp;
    long longVal = Long.MIN_VALUE;
    double doubleVal = -Double.MAX_VALUE;
    Object objectVal = null;
    boolean hasValue = false;

    MaxAccum(Type argType) {
      this.argType = argType;
      this.isDouble = argType instanceof DoubleType;
      this.isVarchar = argType instanceof VarcharType;
      this.isTimestamp = argType instanceof TimestampType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      MaxAccum o = (MaxAccum) other;
      if (!o.hasValue) return;
      hasValue = true;
      if (isVarchar) {
        if (objectVal == null || ((String) o.objectVal).compareTo((String) objectVal) > 0) {
          objectVal = o.objectVal;
        }
      } else if (isDouble) {
        if (o.doubleVal > doubleVal) doubleVal = o.doubleVal;
      } else {
        if (o.longVal > longVal) longVal = o.longVal;
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isVarchar) {
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice((String) objectVal));
      } else if (isDouble) {
        DoubleType.DOUBLE.writeDouble(builder, doubleVal);
      } else if (isTimestamp) {
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, longVal * 1000L);
      } else {
        argType.writeLong(builder, longVal);
      }
    }

    @Override
    public long getSortValue() {
      return isDouble ? Double.doubleToRawLongBits(doubleVal) : longVal;
    }
  }

  private static class AvgAccum implements MergeableAccumulator {
    final boolean isDouble;
    long longSum = 0;
    double doubleSum = 0;
    long count = 0;

    AvgAccum(Type argType) {
      this.isDouble = argType instanceof DoubleType;
    }

    @Override
    public void merge(MergeableAccumulator other) {
      AvgAccum o = (AvgAccum) other;
      longSum += o.longSum;
      doubleSum += o.doubleSum;
      count += o.count;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (count == 0) {
        builder.appendNull();
      } else if (isDouble) {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum / count);
      } else {
        DoubleType.DOUBLE.writeDouble(builder, (double) longSum / count);
      }
    }

    @Override
    public long getSortValue() {
      // For AVG, use the count as a sort proxy (most useful for top-N by count)
      return count;
    }
  }

  /**
   * COUNT(DISTINCT) accumulator for grouped aggregation. Uses primitive LongOpenHashSet for numeric
   * non-double columns to avoid Long boxing overhead. Falls back to HashSet&lt;Object&gt; for
   * varchar and double types.
   */
  private static class CountDistinctAccum implements MergeableAccumulator {
    final Type argType;
    final boolean usePrimitiveLong;

    /** Primitive long set for numeric non-double columns (avoids Long boxing). */
    final LongOpenHashSet longDistinctValues;

    /** Fallback set for varchar and double types. */
    final Set<Object> objectDistinctValues;

    CountDistinctAccum(Type argType) {
      this.argType = argType;
      this.usePrimitiveLong = !(argType instanceof VarcharType) && !(argType instanceof DoubleType);
      this.longDistinctValues = usePrimitiveLong ? new LongOpenHashSet() : null;
      this.objectDistinctValues = usePrimitiveLong ? null : new HashSet<>();
    }

    @Override
    public void merge(MergeableAccumulator other) {
      CountDistinctAccum o = (CountDistinctAccum) other;
      if (usePrimitiveLong) {
        longDistinctValues.addAll(o.longDistinctValues);
      } else {
        objectDistinctValues.addAll(o.objectDistinctValues);
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      long count = usePrimitiveLong ? longDistinctValues.size() : objectDistinctValues.size();
      BigintType.BIGINT.writeLong(builder, count);
    }

    @Override
    public long getSortValue() {
      return usePrimitiveLong ? longDistinctValues.size() : objectDistinctValues.size();
    }
  }

  // =========================================================================
  // Output helpers
  // =========================================================================

  private static Type resolveAggOutputType(AggSpec spec, Map<String, Type> columnTypeMap) {
    switch (spec.funcName) {
      case "COUNT":
        return BigintType.BIGINT;
      case "AVG":
        return DoubleType.DOUBLE;
      case "SUM":
        Type inputType = columnTypeMap.getOrDefault(spec.arg, BigintType.BIGINT);
        return inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
      case "MIN":
      case "MAX":
        return columnTypeMap.getOrDefault(spec.arg, BigintType.BIGINT);
      default:
        return BigintType.BIGINT;
    }
  }

  /**
   * Write a group-by key value to a block builder for the multi-segment merged path. VARCHAR keys
   * are stored as BytesRefKey (raw UTF-8 bytes), numeric keys as Long.
   */
  private static void writeKeyValueForMerged(BlockBuilder builder, KeyInfo keyInfo, Object value) {
    if (value == null) {
      builder.appendNull();
      return;
    }
    Type type = keyInfo.type;
    if (type instanceof VarcharType) {
      if (value instanceof BytesRefKey brk) {
        VarcharType.VARCHAR.writeSlice(builder, Slices.wrappedBuffer(brk.bytes));
      } else {
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice((String) value));
      }
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, Double.longBitsToDouble((Long) value));
    } else if (type instanceof TimestampType) {
      TimestampType.TIMESTAMP_MILLIS.writeLong(builder, ((Long) value) * 1000L);
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, ((Long) value) == 1);
    } else {
      // BigintType, IntegerType, SmallintType, TinyintType
      type.writeLong(builder, (Long) value);
    }
  }

  /** Write a numeric group-by key value (raw long) to a block builder. */
  private static void writeNumericKeyValue(BlockBuilder builder, KeyInfo keyInfo, long value) {
    Type type = keyInfo.type;
    if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, Double.longBitsToDouble(value));
    } else if (type instanceof TimestampType) {
      TimestampType.TIMESTAMP_MILLIS.writeLong(builder, value * 1000L);
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, value == 1);
    } else {
      // BigintType, IntegerType, SmallintType, TinyintType
      type.writeLong(builder, value);
    }
  }
}
