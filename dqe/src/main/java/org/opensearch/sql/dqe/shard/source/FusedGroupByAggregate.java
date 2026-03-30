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
import java.util.Arrays;
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
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;
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

  // === Intra-shard parallelism configuration ===
  private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
  private static final int THREADS_PER_SHARD =
      Math.max(
          1,
          Runtime.getRuntime().availableProcessors() / Integer.getInteger("dqe.numLocalShards", 4));

  private static final java.util.concurrent.ForkJoinPool PARALLEL_POOL =
      new java.util.concurrent.ForkJoinPool(
          Runtime.getRuntime().availableProcessors(),
          java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory,
          null,
          true); // asyncMode=true for work-stealing

  private FusedGroupByAggregate() {}

  /** Expose the shared ForkJoinPool for intra-shard parallelism in other fast paths. */
  public static java.util.concurrent.ForkJoinPool getParallelPool() {
    return PARALLEL_POOL;
  }

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

    // Check all aggregate functions are supported and their arguments are resolvable
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        return false;
      }
      // Verify the aggregate argument is a physical column, *, or a supported expression
      String arg = m.group(3);
      if (!"*".equals(arg) && !columnTypeMap.containsKey(arg)) {
        // Check for length(col) pattern — supported as inline expression
        if (arg.startsWith("length(") && arg.endsWith(")")) {
          String innerCol = arg.substring(7, arg.length() - 1).trim();
          if (columnTypeMap.containsKey(innerCol)
              && columnTypeMap.get(innerCol) instanceof VarcharType) {
            continue; // length(varchar_col) is supported inline
          }
        }
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

      HashMap<String, AccumulatorGroup> globalGroups = new HashMap<>();

      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
      boolean canParallelize =
          !"off".equals(PARALLELISM_MODE) && numWorkers > 1 && leaves.size() > 1;

      if (!canParallelize) {
        // === Single-segment / sequential path (existing Collector approach) ===
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
      } else {
        // === Parallel multi-segment path ===
        @SuppressWarnings("unchecked")
        List<LeafReaderContext>[] workerSegments = new List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++) workerSegments[i] = new ArrayList<>();
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
        sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }

        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        Weight weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<HashMap<String, AccumulatorGroup>>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final List<LeafReaderContext> mySegments = workerSegments[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    HashMap<String, AccumulatorGroup> workerGroups = new HashMap<>();
                    try {
                      for (LeafReaderContext leafCtx : mySegments) {
                        processExprKeySegment(
                            leafCtx,
                            weight,
                            srcCol,
                            specs,
                            numAggs,
                            isCountStar,
                            isComputedArg,
                            isDoubleArg,
                            isVarcharArg,
                            accType,
                            keyBlockExpr,
                            computedArgBlockExprs,
                            workerGroups);
                      }
                    } catch (IOException e) {
                      throw new java.io.UncheckedIOException(e);
                    }
                    return workerGroups;
                  },
                  PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();

        for (int fi = 0; fi < futures.length; fi++) {
          HashMap<String, AccumulatorGroup> wMap = futures[fi].join();
          for (Map.Entry<String, AccumulatorGroup> e : wMap.entrySet()) {
            AccumulatorGroup existing = globalGroups.get(e.getKey());
            if (existing == null) {
              globalGroups.put(e.getKey(), e.getValue());
            } else {
              existing.merge(e.getValue());
            }
          }
          futures[fi] = null; // release worker map for GC
        }
      }

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

  /**
   * Process a single segment for the parallel expression-key GROUP BY path.
   * Pre-computes ordinal caches and scans docs via scorer, accumulating into workerGroups.
   */
  private static void processExprKeySegment(
      LeafReaderContext leafCtx,
      Weight weight,
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
      HashMap<String, AccumulatorGroup> workerGroups)
      throws IOException {
    Scorer scorer = weight.scorer(leafCtx);
    if (scorer == null) return;
    LeafReader reader = leafCtx.reader();
    SortedSetDocValues dv = reader.getSortedSetDocValues(srcCol);
    if (dv == null) return;
    long ordCount = dv.getValueCount();
    if (ordCount <= 0 || ordCount > 10_000_000) return;
    int ordCountInt = (int) ordCount;

    // Pre-compute expressions for each ordinal in this segment
    String[] ordToGroupKey = new String[ordCountInt];
    long[][] ordToComputedArg = new long[numAggs][];
    for (int i = 0; i < numAggs; i++) {
      if (isComputedArg[i]) {
        ordToComputedArg[i] = new long[ordCountInt];
      }
    }

    boolean[] isVarcharSameCol = new boolean[numAggs];
    boolean needRawStringCache = false;
    for (int i = 0; i < numAggs; i++) {
      if (!isCountStar[i] && !isComputedArg[i] && isVarcharArg[i]
          && specs.get(i).arg.equals(srcCol)) {
        isVarcharSameCol[i] = true;
        needRawStringCache = true;
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

    // Pre-wire ordinal -> AccumulatorGroup using worker-local map
    AccumulatorGroup[] ordGroups = new AccumulatorGroup[ordCountInt];
    for (int ord = 0; ord < ordCountInt; ord++) {
      String gk = ordToGroupKey[ord];
      if (gk != null) {
        AccumulatorGroup existing = workerGroups.get(gk);
        if (existing == null) {
          existing = createAccumulatorGroup(specs);
          workerGroups.put(gk, existing);
        }
        ordGroups[ord] = existing;
      }
    }

    // Open doc values for physical aggregate args
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

    // Scan docs via scorer — same accumulation logic as the Collector path
    DocIdSetIterator docIt = scorer.iterator();
    int doc;
    while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (!dv.advanceExact(doc)) continue;
      int ord = (int) dv.nextOrd();
      if (ord >= ordGroups.length) continue;
      AccumulatorGroup accGroup = ordGroups[ord];
      if (accGroup == null) continue;

      for (int i = 0; i < numAggs; i++) {
        MergeableAccumulator acc = accGroup.accumulators[i];
        if (isCountStar[i]) {
          ((CountStarAccum) acc).count++;
          continue;
        }
        if (isVarcharSameCol[i]) {
          String val = ordToRawString[ord];
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
          long val = ordToComputedArg[i][ord];
          switch (accType[i]) {
            case 0: ((CountStarAccum) acc).count++; break;
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
            case 0: ((CountStarAccum) acc).count++; break;
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
      // Handle length(col): resolve to underlying VARCHAR column, flag for inline computation
      boolean applyLength = false;
      if (arg.startsWith("length(") && arg.endsWith(")")) {
        String innerCol = arg.substring(7, arg.length() - 1).trim();
        if (columnTypeMap.containsKey(innerCol)
            && columnTypeMap.get(innerCol) instanceof VarcharType) {
          arg = innerCol;
          applyLength = true;
        }
      }
      Type argType;
      if (applyLength) {
        argType = BigintType.BIGINT; // length() returns long
      } else {
        argType = arg.equals("*") ? null : columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      }
      AggSpec spec = new AggSpec(funcName, isDistinct, arg, argType);
      spec.applyLength = applyLength;
      specs.add(spec);
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
            aggNode,
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
  /**
   * Extract equality comparisons from a condition expression tree (AND of col == const). Returns
   * false if the tree contains anything other than AND and EQUAL comparisons.
   */
  private static boolean extractEqualityComparisons(
      org.opensearch.sql.dqe.function.expression.BlockExpression expr,
      java.util.List<org.opensearch.sql.dqe.function.expression.ComparisonBlockExpression> out) {
    if (expr instanceof org.opensearch.sql.dqe.function.expression.ComparisonBlockExpression comp) {
      out.add(comp);
      return true;
    }
    if (expr instanceof org.opensearch.sql.dqe.function.expression.LogicalAndExpression andExpr) {
      return extractEqualityComparisons(andExpr.getLeft(), out)
          && extractEqualityComparisons(andExpr.getRight(), out);
    }
    return false;
  }

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

      // === Global ordinals multi-segment path ===
      // Use global ordinals when the OrdinalMap build cost is acceptable:
      // - Always for MatchAllDocsQuery (full scan, build cost amortized)
      // - For filtered queries when the field has ≤ 1M ordinals (build cost ~200ms)
      // - Skip for high-cardinality filtered queries (e.g., URL with 18M ordinals, build ~3s)
      OrdinalMap ordinalMap = null;
      if (query instanceof MatchAllDocsQuery) {
        ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
      } else {
        // Check first segment's ordinal count as estimate
        SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
        if (checkDv != null && checkDv.getValueCount() <= 1_000_000) {
          ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
        }
      }
      if (ordinalMap != null) {
        long globalOrdCount = ordinalMap.getValueCount();
        if (globalOrdCount > 0 && globalOrdCount <= 10_000_000) {
          long[] globalCounts = new long[(int) globalOrdCount];

          // Build Weight for filtered queries
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          Weight weight =
              (query instanceof MatchAllDocsQuery)
                  ? null
                  : luceneSearcher.createWeight(
                      luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

          // Scan all segments using global ordinals
          for (int segIdx = 0; segIdx < leaves.size(); segIdx++) {
            LeafReaderContext leafCtx = leaves.get(segIdx);

            if (weight != null) {
              // Filtered query: check scorer BEFORE opening DocValues
              Scorer scorer = weight.scorer(leafCtx);
              if (scorer == null) continue;
              SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
              if (dv == null) continue;
              LongValues segToGlobal = ordinalMap.getGlobalOrds(segIdx);
              DocIdSetIterator docIt = scorer.iterator();
              int doc;
              while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (dv.advanceExact(doc)) {
                  globalCounts[(int) segToGlobal.get(dv.nextOrd())]++;
                }
              }
            } else {
              // MatchAll: iterate all docs directly
              SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
              if (dv == null) continue;
              LongValues segToGlobal = ordinalMap.getGlobalOrds(segIdx);
              SortedDocValues sdv = DocValues.unwrapSingleton(dv);
              if (sdv != null) {
                while (sdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                  globalCounts[(int) segToGlobal.get(sdv.ordValue())]++;
                }
              } else {
                while (dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                  globalCounts[(int) segToGlobal.get(dv.nextOrd())]++;
                }
              }
            }
          }

          // Count non-zero groups
          int groupCount = 0;
          for (long c : globalCounts) {
            if (c > 0) groupCount++;
          }
          if (groupCount == 0) return List.of();

          // Prepare segment DVs for ordinal lookup
          SortedSetDocValues[] segDvs = new SortedSetDocValues[leaves.size()];
          for (int i = 0; i < leaves.size(); i++) {
            segDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
          }

          // Top-N selection
          if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
            int n = (int) Math.min(topN, groupCount);
            int[] heap = new int[n];
            long[] heapVals = new long[n];
            int heapSize = 0;

            for (int i = 0; i < globalCounts.length; i++) {
              if (globalCounts[i] == 0) continue;
              long cnt = globalCounts[i];
              if (heapSize < n) {
                heap[heapSize] = i;
                heapVals[heapSize] = cnt;
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
                boolean better = sortAscending ? (cnt < heapVals[0]) : (cnt > heapVals[0]);
                if (better) {
                  heap[0] = i;
                  heapVals[0] = cnt;
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
            for (int i = 0; i < heapSize; i++) {
              BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, heap[i]);
              VarcharType.VARCHAR.writeSlice(
                  keyBuilder, Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              BigintType.BIGINT.writeLong(countBuilder, heapVals[i]);
            }
            return List.of(new Page(keyBuilder.build(), countBuilder.build()));
          }

          // No top-N: output all groups
          BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
          BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, groupCount);
          for (int i = 0; i < globalCounts.length; i++) {
            if (globalCounts[i] > 0) {
              BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, i);
              VarcharType.VARCHAR.writeSlice(
                  keyBuilder, Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              BigintType.BIGINT.writeLong(countBuilder, globalCounts[i]);
            }
          }
          return List.of(new Page(keyBuilder.build(), countBuilder.build()));
        }
      }

      // === Parallel multi-segment path ===
      // Only parallelize when ordinal counts are manageable (< 500K per segment).
      // High-cardinality VARCHAR keys (e.g., URL with 18M unique values) create too many
      // BytesRefKey copies per worker, making merge overhead exceed the parallelism benefit.
      // Exception: filtered queries (not MatchAllDocsQuery) touch only a small subset of docs,
      // so per-segment ordinal arrays are feasible even with high cardinality — the merge
      // only resolves ordinals that actually appear in matching docs.
      boolean canParallelize =
          !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;
      if (canParallelize && query instanceof MatchAllDocsQuery) {
        SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
        if (checkDv != null && checkDv.getValueCount() > 500_000) {
          canParallelize = false;
        }
      }
      if (canParallelize) {
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        // Partition segments: largest-first to lightest worker
        @SuppressWarnings("unchecked")
        List<LeafReaderContext>[] workerSegments = new List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++) workerSegments[i] = new ArrayList<>();
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
        sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }

        // Create Weight for per-segment Scorers
        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        Weight weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<HashMap<BytesRefKey, long[]>>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final List<LeafReaderContext> mySegments = workerSegments[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    HashMap<BytesRefKey, long[]> workerCounts = new HashMap<>();
                    try {
                      for (LeafReaderContext leafCtx : mySegments) {
                        Scorer scorer = weight.scorer(leafCtx);
                        if (scorer == null) continue;
                        SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
                        if (dv == null) continue;
                        long ordCount = dv.getValueCount();
                        long[] ordCounts =
                            (ordCount > 0 && ordCount <= 1_000_000)
                                ? new long[(int) ordCount]
                                : null;
                        HashMap<Long, long[]> ordMap = (ordCounts == null) ? new HashMap<>() : null;

                        DocIdSetIterator docIt = scorer.iterator();
                        int doc;
                        while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                          if (dv.advanceExact(doc)) {
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

                        // Resolve ordinals and merge into worker map
                        if (ordCounts != null) {
                          for (int i = 0; i < ordCounts.length; i++) {
                            if (ordCounts[i] > 0) {
                              BytesRef bytes = dv.lookupOrd(i);
                              BytesRefKey key = new BytesRefKey(bytes);
                              long[] existing = workerCounts.get(key);
                              if (existing == null) {
                                workerCounts.put(key, new long[] {ordCounts[i]});
                              } else {
                                existing[0] += ordCounts[i];
                              }
                            }
                          }
                        } else if (ordMap != null) {
                          for (Map.Entry<Long, long[]> entry : ordMap.entrySet()) {
                            BytesRef bytes = dv.lookupOrd(entry.getKey());
                            BytesRefKey key = new BytesRefKey(bytes);
                            long[] existing = workerCounts.get(key);
                            if (existing == null) {
                              workerCounts.put(key, new long[] {entry.getValue()[0]});
                            } else {
                              existing[0] += entry.getValue()[0];
                            }
                          }
                        }
                      }
                    } catch (IOException e) {
                      throw new java.io.UncheckedIOException(e);
                    }
                    return workerCounts;
                  },
                  PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();

        HashMap<BytesRefKey, long[]> globalCounts = new HashMap<>();
        for (int fi = 0; fi < futures.length; fi++) {
          HashMap<BytesRefKey, long[]> wMap = futures[fi].join();
          for (Map.Entry<BytesRefKey, long[]> e : wMap.entrySet()) {
            long[] existing = globalCounts.get(e.getKey());
            if (existing == null) {
              globalCounts.put(e.getKey(), e.getValue());
            } else {
              existing[0] += e.getValue()[0];
            }
          }
          futures[fi] = null; // release worker map for GC
        }

        if (globalCounts.isEmpty()) return List.of();

        int groupCount = globalCounts.size();

        // Top-N selection for COUNT(*) single-varchar
        if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
          int n = (int) Math.min(topN, groupCount);
          @SuppressWarnings("unchecked")
          Map.Entry<BytesRefKey, long[]>[] heap = new Map.Entry[n];
          int heapSize = 0;
          for (Map.Entry<BytesRefKey, long[]> entry : globalCounts.entrySet()) {
            if (heapSize < n) {
              heap[heapSize++] = entry;
              int ki = heapSize - 1;
              while (ki > 0) {
                int parent = (ki - 1) >>> 1;
                boolean swap =
                    sortAscending
                        ? (heap[ki].getValue()[0] > heap[parent].getValue()[0])
                        : (heap[ki].getValue()[0] < heap[parent].getValue()[0]);
                if (swap) {
                  var tmp = heap[parent];
                  heap[parent] = heap[ki];
                  heap[ki] = tmp;
                  ki = parent;
                } else break;
              }
            } else {
              long val = entry.getValue()[0];
              boolean better =
                  sortAscending ? (val < heap[0].getValue()[0]) : (val > heap[0].getValue()[0]);
              if (better) {
                heap[0] = entry;
                int ki = 0;
                while (true) {
                  int left = 2 * ki + 1;
                  if (left >= heapSize) break;
                  int right = left + 1;
                  int target = left;
                  if (right < heapSize) {
                    boolean pickRight =
                        sortAscending
                            ? (heap[right].getValue()[0] > heap[left].getValue()[0])
                            : (heap[right].getValue()[0] < heap[left].getValue()[0]);
                    if (pickRight) target = right;
                  }
                  boolean swap =
                      sortAscending
                          ? (heap[target].getValue()[0] > heap[ki].getValue()[0])
                          : (heap[target].getValue()[0] < heap[ki].getValue()[0]);
                  if (swap) {
                    var tmp = heap[ki];
                    heap[ki] = heap[target];
                    heap[target] = tmp;
                    ki = target;
                  } else break;
                }
              }
            }
          }
          BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, heapSize);
          BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, heapSize);
          for (int i = 0; i < heapSize; i++) {
            VarcharType.VARCHAR.writeSlice(
                keyBuilder, Slices.wrappedBuffer(heap[i].getKey().bytes));
            BigintType.BIGINT.writeLong(countBuilder, heap[i].getValue()[0]);
          }
          return List.of(new Page(keyBuilder.build(), countBuilder.build()));
        }

        BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
        BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, groupCount);
        for (Map.Entry<BytesRefKey, long[]> entry : globalCounts.entrySet()) {
          VarcharType.VARCHAR.writeSlice(keyBuilder, Slices.wrappedBuffer(entry.getKey().bytes));
          BigintType.BIGINT.writeLong(countBuilder, entry.getValue()[0]);
        }
        return List.of(new Page(keyBuilder.build(), countBuilder.build()));
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

      // === Global ordinals multi-segment path ===
      // Use global ordinals for MatchAll or low-cardinality VARCHAR fields
      {
        OrdinalMap ordinalMap = null;
        if (query instanceof MatchAllDocsQuery) {
          ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
        } else {
          SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
          if (checkDv != null && checkDv.getValueCount() <= 1_000_000) {
            ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
          }
        }
        if (ordinalMap != null) {
          long globalOrdCount = ordinalMap.getValueCount();
          if (globalOrdCount > 0 && globalOrdCount <= 10_000_000) {
            AccumulatorGroup[] globalGroups = new AccumulatorGroup[(int) globalOrdCount];

            // Build Weight for filtered queries
            IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
            Weight weight =
                (query instanceof MatchAllDocsQuery)
                    ? null
                    : luceneSearcher.createWeight(
                        luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

            for (int segIdx = 0; segIdx < leaves.size(); segIdx++) {
              LeafReaderContext leafCtx = leaves.get(segIdx);

              // For filtered queries, check scorer BEFORE opening DocValues
              if (weight != null) {
                Scorer scorer = weight.scorer(leafCtx);
                if (scorer == null) continue; // No matching docs — skip segment entirely

                SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
                if (dv == null) continue;
                LongValues segToGlobal = ordinalMap.getGlobalOrds(segIdx);

                SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    AggSpec spec = specs.get(i);
                    if (isVarcharArg[i]) {
                      varcharAggDvs[i] = leafCtx.reader().getSortedSetDocValues(spec.arg);
                    } else {
                      numericAggDvs[i] = leafCtx.reader().getSortedNumericDocValues(spec.arg);
                    }
                  }
                }

                DocIdSetIterator docIt = scorer.iterator();
                int doc;
                while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (!dv.advanceExact(doc)) continue;
                  int globalOrd = (int) segToGlobal.get(dv.nextOrd());
                  AccumulatorGroup accGroup = globalGroups[globalOrd];
                  if (accGroup == null) {
                    accGroup = createAccumulatorGroup(specs);
                    globalGroups[globalOrd] = accGroup;
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
                // MatchAll: open DocValues and iterate all docs
                SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
                if (dv == null) continue;
                LongValues segToGlobal = ordinalMap.getGlobalOrds(segIdx);

                SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    AggSpec spec = specs.get(i);
                    if (isVarcharArg[i]) {
                      varcharAggDvs[i] = leafCtx.reader().getSortedSetDocValues(spec.arg);
                    } else {
                      numericAggDvs[i] = leafCtx.reader().getSortedNumericDocValues(spec.arg);
                    }
                  }
                }

                SortedDocValues sdv = DocValues.unwrapSingleton(dv);
                if (sdv != null) {
                  int doc;
                  while ((doc = sdv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    int globalOrd = (int) segToGlobal.get(sdv.ordValue());
                    AccumulatorGroup accGroup = globalGroups[globalOrd];
                    if (accGroup == null) {
                      accGroup = createAccumulatorGroup(specs);
                      globalGroups[globalOrd] = accGroup;
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
                    int globalOrd = (int) segToGlobal.get(dv.nextOrd());
                    AccumulatorGroup accGroup = globalGroups[globalOrd];
                    if (accGroup == null) {
                      accGroup = createAccumulatorGroup(specs);
                      globalGroups[globalOrd] = accGroup;
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
              }
            }

            // Count non-null groups
            int groupCount = 0;
            for (AccumulatorGroup g : globalGroups) {
              if (g != null) groupCount++;
            }
            if (groupCount == 0) return List.of();

            // Prepare segment DVs for ordinal lookup
            SortedSetDocValues[] segDvs = new SortedSetDocValues[leaves.size()];
            for (int i = 0; i < leaves.size(); i++) {
              segDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
            }

            int numGroupKeys = groupByKeys.size();
            int totalColumns = numGroupKeys + numAggs;

            // Top-N selection
            if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
              int n = (int) Math.min(topN, groupCount);
              int[] heap = new int[n];
              long[] heapVals = new long[n];
              int heapSize = 0;

              for (int i = 0; i < globalGroups.length; i++) {
                if (globalGroups[i] == null) continue;
                long val = globalGroups[i].accumulators[sortAggIndex].getSortValue();
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
              for (int i = 0; i < heapSize; i++) {
                BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, heap[i]);
                VarcharType.VARCHAR.writeSlice(
                    builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
                for (int a = 0; a < numAggs; a++) {
                  globalGroups[heap[i]].accumulators[a].writeTo(builders[numGroupKeys + a]);
                }
              }
              Block[] blocks = new Block[totalColumns];
              for (int j = 0; j < totalColumns; j++) blocks[j] = builders[j].build();
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
            for (int i = 0; i < globalGroups.length; i++) {
              if (globalGroups[i] == null) continue;
              BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, i);
              VarcharType.VARCHAR.writeSlice(
                  builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              for (int a = 0; a < numAggs; a++) {
                globalGroups[i].accumulators[a].writeTo(builders[numGroupKeys + a]);
              }
            }
            Block[] blocks = new Block[totalColumns];
            for (int j = 0; j < totalColumns; j++) blocks[j] = builders[j].build();
            return List.of(new Page(blocks));
          }
        }
      }

      // === Parallel multi-segment path ===
      // Only parallelize when ordinal counts are manageable (< 500K per segment).
      boolean canParallelizeGeneric =
          !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;
      if (canParallelizeGeneric) {
        SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
        if (checkDv != null && checkDv.getValueCount() > 500_000) {
          canParallelizeGeneric = false;
        }
      }
      if (canParallelizeGeneric) {
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        @SuppressWarnings("unchecked")
        List<LeafReaderContext>[] workerSegments = new List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++) workerSegments[i] = new ArrayList<>();
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
        sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }

        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        Weight weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<HashMap<BytesRefKey, AccumulatorGroup>>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final List<LeafReaderContext> mySegments = workerSegments[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    HashMap<BytesRefKey, AccumulatorGroup> workerGroups = new HashMap<>();
                    try {
                      for (LeafReaderContext leafCtx : mySegments) {
                        Scorer scorer = weight.scorer(leafCtx);
                        if (scorer == null) continue;
                        SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
                        if (dv == null) continue;
                        long ordCount = dv.getValueCount();
                        AccumulatorGroup[] ordGroups =
                            (ordCount > 0 && ordCount <= 1_000_000)
                                ? new AccumulatorGroup[(int) ordCount]
                                : null;
                        if (ordGroups == null) continue;

                        // Open agg doc values
                        SortedNumericDocValues[] numericAggDvs =
                            new SortedNumericDocValues[numAggs];
                        SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                        for (int i = 0; i < numAggs; i++) {
                          if (!isCountStar[i]) {
                            AggSpec spec = specs.get(i);
                            if (isVarcharArg[i]) {
                              varcharAggDvs[i] = leafCtx.reader().getSortedSetDocValues(spec.arg);
                            } else {
                              numericAggDvs[i] =
                                  leafCtx.reader().getSortedNumericDocValues(spec.arg);
                            }
                          }
                        }

                        DocIdSetIterator docIt = scorer.iterator();
                        int doc;
                        while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                          if (!dv.advanceExact(doc)) continue;
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

                        // Resolve and merge into worker map
                        for (int i = 0; i < ordGroups.length; i++) {
                          if (ordGroups[i] != null) {
                            BytesRef bytes = dv.lookupOrd(i);
                            BytesRefKey key = new BytesRefKey(bytes);
                            AccumulatorGroup existing = workerGroups.get(key);
                            if (existing == null) {
                              workerGroups.put(key, ordGroups[i]);
                            } else {
                              existing.merge(ordGroups[i]);
                            }
                          }
                        }
                      }
                    } catch (IOException e) {
                      throw new java.io.UncheckedIOException(e);
                    }
                    return workerGroups;
                  },
                  PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();

        HashMap<BytesRefKey, AccumulatorGroup> globalGroups = new HashMap<>();
        for (int fi = 0; fi < futures.length; fi++) {
          HashMap<BytesRefKey, AccumulatorGroup> wMap = futures[fi].join();
          for (Map.Entry<BytesRefKey, AccumulatorGroup> e : wMap.entrySet()) {
            AccumulatorGroup existing = globalGroups.get(e.getKey());
            if (existing == null) {
              globalGroups.put(e.getKey(), e.getValue());
            } else {
              existing.merge(e.getValue());
            }
          }
          futures[fi] = null; // release worker map for GC
        }

        if (globalGroups.isEmpty()) return List.of();

        // Build output
        int numGroupKeys = groupByKeys.size();
        int totalColumns = numGroupKeys + numAggs;
        int groupCount = globalGroups.size();
        BlockBuilder[] builders = new BlockBuilder[totalColumns];
        builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
        for (int i = 0; i < numAggs; i++) {
          builders[numGroupKeys + i] =
              resolveAggOutputType(specs.get(i), columnTypeMap)
                  .createBlockBuilder(null, groupCount);
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
            shard,
            query,
            keyInfos,
            specs,
            columnTypeMap,
            groupByKeys,
            arithUnits,
            sortAggIndex,
            sortAscending,
            topN);
      }
    }

    // Global map: SegmentGroupKey (long[]) -> AccumulatorGroup
    // For numeric-only keys, long values are globally unique (not ordinals),
    // so we can accumulate directly without per-segment resolution.
    Map<SegmentGroupKey, AccumulatorGroup> globalGroups = new HashMap<>();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric")) {

      final int numKeys = keyInfos.size();

      if (query instanceof MatchAllDocsQuery) {
        // MatchAll: use Collector path (no segment skipping benefit)
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues[] keyReaders = new SortedNumericDocValues[numKeys];
                for (int i = 0; i < numKeys; i++) {
                  keyReaders[i] =
                      context.reader().getSortedNumericDocValues(keyInfos.get(i).name());
                }
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
                final NumericProbeKey probeKey = new NumericProbeKey(numKeys);

                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
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
                    AccumulatorGroup accGroup = globalGroups.get(probeKey);
                    if (accGroup == null) {
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
      } else {
        // Filtered path: manual Weight+Scorer loop to skip empty segments early
        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        Weight weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        final NumericProbeKey probeKey = new NumericProbeKey(numKeys);

        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          Scorer scorer = weight.scorer(leafCtx);
          if (scorer == null) continue;

          LeafReader reader = leafCtx.reader();
          SortedNumericDocValues[] keyReaders = new SortedNumericDocValues[numKeys];
          for (int i = 0; i < numKeys; i++) {
            keyReaders[i] = reader.getSortedNumericDocValues(keyInfos.get(i).name());
          }
          Object[] aggReaders = new Object[specs.size()];
          for (int i = 0; i < specs.size(); i++) {
            AggSpec spec = specs.get(i);
            if ("*".equals(spec.arg)) {
              aggReaders[i] = null;
            } else if (spec.argType instanceof VarcharType) {
              aggReaders[i] = reader.getSortedSetDocValues(spec.arg);
            } else {
              aggReaders[i] = reader.getSortedNumericDocValues(spec.arg);
            }
          }

          DocIdSetIterator docIt = scorer.iterator();
          int doc;
          while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
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
            AccumulatorGroup accGroup = globalGroups.get(probeKey);
            if (accGroup == null) {
              SegmentGroupKey immutableKey = probeKey.toImmutableKey();
              accGroup = createAccumulatorGroup(specs);
              globalGroups.put(immutableKey, accGroup);
            }
            accumulateDoc(doc, accGroup, specs, aggReaders);
          }
        }
      }
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
      // Try single-bucket first (fastest path: columnar + parallel doc-range).
      // Only fall back to multi-bucket if the flat map overflows MAX_CAPACITY.
      // Previous approach used totalDocs/MAX_CAPACITY which overestimates unique groups
      // (e.g., 4.4M unique UserIDs in 25M docs would incorrectly trigger 2 buckets).
      try {
        return executeSingleKeyNumericFlat(
            shard, query, keyInfos, specs, columnTypeMap, groupByKeys,
            numAggs, isCountStar, accType, sortAggIndex, sortAscending, topN, 0, 1);
      } catch (RuntimeException overflowEx) {
        if (overflowEx.getMessage() == null
            || !overflowEx.getMessage().contains("GROUP BY exceeded memory limit")) {
          throw overflowEx;
        }
        // Flat map overflowed — use single-pass multi-bucket (reads data once)
        int numBuckets;
        try (org.opensearch.index.engine.Engine.Searcher estSearcher =
            shard.acquireSearcher("dqe-fused-groupby-estimate")) {
          long totalDocs = 0;
          for (LeafReaderContext leaf : estSearcher.getIndexReader().leaves()) {
            totalDocs += leaf.reader().maxDoc();
          }
          numBuckets = Math.max(2, (int) Math.ceil((double) totalDocs / FlatSingleKeyMap.MAX_CAPACITY));
        }
        return executeSingleKeyNumericFlatMultiBucket(
            shard, query, keyInfos, specs, columnTypeMap, groupByKeys,
            numAggs, isCountStar, accType, sortAggIndex, sortAscending, topN, numBuckets);
      }
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
            // Check if all numeric DocValues iterators are non-null (dense columns).
            // For dense columns, use nextDoc() sequential iteration which is much faster
            // than advanceExact() because it reads the compressed DocValues stream
            // sequentially instead of doing per-doc binary search/skip.
            boolean allDense = dv0 != null;
            boolean hasVarcharAgg = false;
            if (allDense) {
              for (int i = 0; i < numAggs; i++) {
                if (isCountStar[i]) continue;
                if (varcharAggDvs[i] != null) {
                  hasVarcharAgg = true;
                  break;
                }
                if (numericAggDvs[i] == null) {
                  allDense = false;
                  break;
                }
              }
            }
            if (allDense && !hasVarcharAgg && maxDoc > 0) {
              // Sequential nextDoc() path: advance all iterators in lockstep.
              // For dense columns, nextDoc() returns consecutive doc IDs 0, 1, 2, ...
              // This avoids the advanceExact() binary search overhead per doc per column.
              // Position all iterators at their first doc.
              int keyDoc = dv0.nextDoc();
              int[] aggDocs = new int[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i] && numericAggDvs[i] != null) {
                  aggDocs[i] = numericAggDvs[i].nextDoc();
                }
              }
              for (int doc = 0; doc < maxDoc; doc++) {
                // Read key: if the key iterator is at this doc, read value; otherwise use 0
                long key0;
                if (keyDoc == doc) {
                  key0 = dv0.nextValue();
                  keyDoc = dv0.nextDoc();
                } else {
                  key0 = 0;
                }
                AccumulatorGroup accGroup = singleKeyMap.getOrCreate(key0);
                for (int i = 0; i < numAggs; i++) {
                  MergeableAccumulator acc = accGroup.accumulators[i];
                  if (isCountStar[i]) {
                    ((CountStarAccum) acc).count++;
                    continue;
                  }
                  SortedNumericDocValues aggDv = numericAggDvs[i];
                  if (aggDocs[i] == doc) {
                    long rawVal = aggDv.nextValue();
                    aggDocs[i] = aggDv.nextDoc();
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
            } else {
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
        // Filtered path: manual Weight+Scorer loop to skip empty segments early
        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        Weight weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          Scorer scorer = weight.scorer(leafCtx);
          if (scorer == null) continue; // No matching docs in this segment

          LeafReader reader = leafCtx.reader();
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

          DocIdSetIterator docIt = scorer.iterator();
          int doc;
          while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
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
      String[] arithUnits,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
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
          // Filtered path: manual Weight+Scorer loop to skip empty segments early
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          Weight weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

          for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
            Scorer scorer = weight.scorer(leafCtx);
            if (scorer == null) continue;

            SortedNumericDocValues dv0 = leafCtx.reader().getSortedNumericDocValues(sourceCol);
            DocIdSetIterator docIt = scorer.iterator();
            int doc;
            while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
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

      if (singleKeyMap.size == 0) return List.of();

      return buildDerivedKeyResult(
          singleKeyMap,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          arithUnits,
          sortAggIndex,
          sortAscending,
          topN);
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
    final FlatSingleKeyMap flatMap;
    final String sourceCol = keyInfos.get(0).name();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-derived-1key-flat")) {

      // Pre-size main map based on total docs
      long totalDocsEst = 0;
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      for (LeafReaderContext leaf : leaves) {
        totalDocsEst += leaf.reader().maxDoc();
      }
      int mainInitCap = (int) Math.min(totalDocsEst * 10 / 7 + 1, 4_000_000);
      flatMap = new FlatSingleKeyMap(slotsPerGroup, mainInitCap);

      boolean canParallelize =
          !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1;
      boolean isMatchAll = query instanceof MatchAllDocsQuery;

      if (canParallelize && leaves.size() > 1) {
        // Parallel path: partition segments across workers, each with own FlatSingleKeyMap
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        @SuppressWarnings("unchecked")
        java.util.List<LeafReaderContext>[] workerSegments = new java.util.List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++)
          workerSegments[i] = new java.util.ArrayList<>();
        // Largest-first greedy assignment for balanced load
        java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
        sortedLeaves.sort(
            (a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }

        final Weight weight;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        } else {
          weight = null;
        }

        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<FlatSingleKeyMap>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final java.util.List<LeafReaderContext> mySegments = workerSegments[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup);
                    try {
                      for (LeafReaderContext leafCtx : mySegments) {
                        scanSegmentDerivedSingleKeyFlat(
                            leafCtx, weight, isMatchAll, localMap, sourceCol, specs,
                            numAggs, isCountStar, accType, accOffset, slotsPerGroup);
                      }
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                    return localMap;
                  },
                  PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();
        for (int fi = 0; fi < futures.length; fi++) {
          FlatSingleKeyMap workerMap = futures[fi].join();
          if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
          futures[fi] = null; // release worker map for GC
        }
      } else {
        // Sequential path: single FlatSingleKeyMap for all segments
        Weight weight = null;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        }
        for (LeafReaderContext leafCtx : leaves) {
          scanSegmentDerivedSingleKeyFlat(
              leafCtx, weight, isMatchAll, flatMap, sourceCol, specs,
              numAggs, isCountStar, accType, accOffset, slotsPerGroup);
        }
      }
    }

    if (flatMap.size == 0) return List.of();

    // Build output: expand single source key to all derived keys.
    // Plain source keys use their original type; derived (arith) keys use BigintType.
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
        if (flatMap.keys[slot] == FlatSingleKeyMap.EMPTY_KEY) continue;
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
              long kVal2 = flatMap.accData[heap[k] * slotsPerGroup + sortAccOff];
              long tVal = flatMap.accData[heap[target] * slotsPerGroup + sortAccOff];
              boolean swap = sortAscending ? (tVal > kVal2) : (tVal < kVal2);
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
        if (flatMap.keys[slot] != FlatSingleKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[flatMap.size];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.keys[slot] != FlatSingleKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    }

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      Type keyType = (arithUnits[i] != null) ? BigintType.BIGINT : keyInfos.get(i).type;
      builders[i] = keyType.createBlockBuilder(null, outputCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }
    for (int o = 0; o < outputCount; o++) {
      int slot = outputSlots[o];
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
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /**
   * Per-segment scan helper for the derived single-key flat path.
   * Handles both MatchAll (with/without liveDocs) and Filtered paths.
   */
  private static void scanSegmentDerivedSingleKeyFlat(
      LeafReaderContext leafCtx,
      Weight weight,
      boolean isMatchAll,
      FlatSingleKeyMap flatMap,
      String sourceCol,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      int slotsPerGroup)
      throws Exception {
    LeafReader reader = leafCtx.reader();
    SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(sourceCol);

    final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
    for (int i = 0; i < numAggs; i++) {
      if (!isCountStar[i]) {
        numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
      }
    }

    if (isMatchAll) {
      int maxDoc = reader.maxDoc();
      Bits liveDocs = reader.getLiveDocs();

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
    } else {
      // Filtered path: manual Weight+Scorer loop to skip empty segments early
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) return;

      DocIdSetIterator docIt = scorer.iterator();
      int doc;
      while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
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

  /** Build the result Page from a SingleKeyHashMap for derived-key GROUP BY. */
  private static List<Page> buildDerivedKeyResult(
      SingleKeyHashMap singleKeyMap,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      String[] arithUnits,
      int sortAggIndex,
      boolean sortAscending,
      long topN) {
    if (singleKeyMap.size == 0) return List.of();
    int numAggs = specs.size();
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
          heap[heapSize] = slot;
          heapSize++;
          int k = heapSize - 1;
          while (k > 0) {
            int parent = (k - 1) >>> 1;
            long pVal = singleKeyMap.groups[heap[parent]].accumulators[sortAggIndex].getSortValue();
            long kVal2 = singleKeyMap.groups[heap[k]].accumulators[sortAggIndex].getSortValue();
            boolean swap = sortAscending ? (kVal2 > pVal) : (kVal2 < pVal);
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
              long kVal2 = singleKeyMap.groups[heap[k]].accumulators[sortAggIndex].getSortValue();
              long tVal =
                  singleKeyMap.groups[heap[target]].accumulators[sortAggIndex].getSortValue();
              boolean swap = sortAscending ? (tVal > kVal2) : (tVal < kVal2);
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
    for (int i = 0; i < numGroupKeys; i++) {
      Type keyType = (arithUnits[i] != null) ? BigintType.BIGINT : keyInfos.get(i).type;
      builders[i] = keyType.createBlockBuilder(null, outputCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }
    for (int o = 0; o < outputCount; o++) {
      int slot = outputSlots[o];
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
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }
  /**
   * Single-pass multi-bucket aggregation for high-cardinality single numeric key GROUP BY.
   * Instead of N passes over all docs (one per bucket), does ONE pass routing each doc
   * to the correct bucket's FlatSingleKeyMap. Eliminates N-1 redundant full scans.
   */
  private static List<Page> executeSingleKeyNumericFlatMultiBucket(
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
      long topN,
      int numBuckets)
      throws Exception {

    final int[] accOffset = new int[numAggs];
    int totalSlots = 0;
    for (int i = 0; i < numAggs; i++) {
      accOffset[i] = totalSlots;
      switch (accType[i]) {
        case 0: case 1: totalSlots += 1; break;
        case 2: totalSlots += 2; break;
        default: throw new IllegalStateException("Unsupported accType: " + accType[i]);
      }
    }
    final int slotsPerGroup = totalSlots;

    // Allocate one FlatSingleKeyMap per bucket
    final FlatSingleKeyMap[] bucketMaps = new FlatSingleKeyMap[numBuckets];
    for (int b = 0; b < numBuckets; b++) {
      bucketMaps[b] = new FlatSingleKeyMap(slotsPerGroup);
    }

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-1key-flat-mb")) {

      java.util.List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      boolean isMatchAll = query instanceof MatchAllDocsQuery;

      Weight weight = null;
      if (!isMatchAll) {
        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        weight = luceneSearcher.createWeight(
            luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
      }

      boolean canParallelize =
          !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;

      if (canParallelize) {
        // Segment-parallel: each worker gets its own set of bucket maps
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        @SuppressWarnings("unchecked")
        java.util.List<LeafReaderContext>[] workerSegments = new java.util.List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++)
          workerSegments[i] = new java.util.ArrayList<>();
        java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
        sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }

        final Weight fWeight = weight;
        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<FlatSingleKeyMap[]>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final java.util.List<LeafReaderContext> mySegments = workerSegments[w];
          futures[w] = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            FlatSingleKeyMap[] localMaps = new FlatSingleKeyMap[numBuckets];
            for (int b = 0; b < numBuckets; b++) localMaps[b] = new FlatSingleKeyMap(slotsPerGroup);
            try {
              for (LeafReaderContext leafCtx : mySegments) {
                scanSegmentFlatSingleKeyMultiBucket(
                    leafCtx, fWeight, isMatchAll, localMaps, keyInfos, specs,
                    numAggs, isCountStar, accType, accOffset, slotsPerGroup, numBuckets);
              }
            } catch (Exception e) { throw new RuntimeException(e); }
            return localMaps;
          }, PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();
        for (int fi = 0; fi < futures.length; fi++) {
          FlatSingleKeyMap[] workerMaps = futures[fi].join();
          for (int b = 0; b < numBuckets; b++) {
            if (workerMaps[b].size > 0) bucketMaps[b].mergeFrom(workerMaps[b]);
          }
          futures[fi] = null; // release worker maps for GC
        }
      } else {
        // Sequential: single set of bucket maps
        for (LeafReaderContext leafCtx : leaves) {
          scanSegmentFlatSingleKeyMultiBucket(
              leafCtx, weight, isMatchAll, bucketMaps, keyInfos, specs,
              numAggs, isCountStar, accType, accOffset, slotsPerGroup, numBuckets);
        }
      }
    }

    // Merge all bucket maps into bucketMaps[0]
    for (int b = 1; b < numBuckets; b++) {
      if (bucketMaps[b].size > 0) bucketMaps[0].mergeFrom(bucketMaps[b]);
      bucketMaps[b] = null; // release merged bucket map for GC
    }
    FlatSingleKeyMap flatMap = bucketMaps[0];

    if (flatMap.size == 0) return List.of();

    // Output page construction (same as executeSingleKeyNumericFlat)
    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;
    int[] outputSlots;
    int outputCount;

    if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
      int sortAccOff = accOffset[sortAggIndex];
      int n = (int) Math.min(topN, flatMap.size);
      int[] heap = new int[n];
      int heapSize = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.keys[slot] == FlatSingleKeyMap.EMPTY_KEY) continue;
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
            if (swap) { int tmp = heap[parent]; heap[parent] = heap[k]; heap[k] = tmp; k = parent; }
            else break;
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
                if (sortAscending ? (rv > lv) : (rv < lv)) target = right;
              }
              long kVal = flatMap.accData[heap[k] * slotsPerGroup + sortAccOff];
              long tVal = flatMap.accData[heap[target] * slotsPerGroup + sortAccOff];
              if (sortAscending ? (tVal > kVal) : (tVal < kVal)) {
                int tmp = heap[k]; heap[k] = heap[target]; heap[target] = tmp; k = target;
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
        if (flatMap.keys[slot] != FlatSingleKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[flatMap.size];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.keys[slot] != FlatSingleKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
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
          case 0: case 1:
            BigintType.BIGINT.writeLong(builders[numGroupKeys + a], flatMap.accData[off]);
            break;
          case 2:
            long sum = flatMap.accData[off];
            long cnt = flatMap.accData[off + 1];
            if (cnt == 0) builders[numGroupKeys + a].appendNull();
            else DoubleType.DOUBLE.writeDouble(builders[numGroupKeys + a], (double) sum / cnt);
            break;
        }
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }
  /**
   * Single-pass multi-bucket scan: routes each doc to the correct bucket's FlatSingleKeyMap.
   * Eliminates the per-doc hash-partition filter skip that wastes DocValues reads.
   */
  private static void scanSegmentFlatSingleKeyMultiBucket(
      LeafReaderContext leafCtx,
      Weight weight,
      boolean isMatchAll,
      FlatSingleKeyMap[] bucketMaps,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      int slotsPerGroup,
      int numBuckets)
      throws Exception {
    LeafReader reader = leafCtx.reader();
    SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());

    final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
    final SortedSetDocValues[] lengthVarcharDvs = new SortedSetDocValues[numAggs];
    final int[][] ordLengthMaps = new int[numAggs][];
    final boolean[] aggApplyLength = new boolean[numAggs];
    for (int i = 0; i < numAggs; i++) {
      if (!isCountStar[i]) {
        if (specs.get(i).applyLength) {
          SortedSetDocValues varcharDv = reader.getSortedSetDocValues(specs.get(i).arg);
          lengthVarcharDvs[i] = varcharDv;
          aggApplyLength[i] = true;
          if (varcharDv != null) {
            int ordCount = (int) Math.min(varcharDv.getValueCount(), 10_000_000);
            int[] ordLengths = new int[ordCount];
            for (int ord = 0; ord < ordCount; ord++) {
              ordLengths[ord] = varcharDv.lookupOrd(ord).length;
            }
            ordLengthMaps[i] = ordLengths;
          }
        } else {
          numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
        }
      }
    }

    if (isMatchAll) {
      int maxDoc = reader.maxDoc();
      Bits liveDocs = reader.getLiveDocs();

      boolean allCountStar = true;
      boolean hasApplyLen = false;
      for (int i = 0; i < numAggs; i++) {
        if (!isCountStar[i]) allCountStar = false;
        if (aggApplyLength[i]) hasApplyLen = true;
      }

      if (allCountStar && dv0 != null && liveDocs == null) {
        // Ultra-fast COUNT(*)-only path: single scan routing to bucket maps
        int doc = dv0.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
          long key0 = dv0.nextValue();
          int bkt = (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets;
          int slot = bucketMaps[bkt].findOrInsert(key0);
          bucketMaps[bkt].accData[slot * slotsPerGroup + accOffset[0]]++;
          doc = dv0.nextDoc();
        }
      } else if (!hasApplyLen && dv0 != null && liveDocs == null) {
        // Numeric aggs path: lockstep iteration routing to bucket maps
        boolean allAggDense = true;
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i] && numericAggDvs[i] == null) { allAggDense = false; break; }
        }
        if (allAggDense) {
          int[] aggDocPos = new int[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) aggDocPos[i] = numericAggDvs[i].nextDoc();
          }
          int keyDoc = dv0.nextDoc();
          for (int doc = 0; doc < maxDoc; doc++) {
            long key0;
            if (keyDoc == doc) { key0 = dv0.nextValue(); keyDoc = dv0.nextDoc(); }
            else { key0 = 0; }
            int bkt = (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets;
            FlatSingleKeyMap map = bucketMaps[bkt];
            int slot = map.findOrInsert(key0);
            int base = slot * slotsPerGroup;
            for (int i = 0; i < numAggs; i++) {
              int off = base + accOffset[i];
              if (isCountStar[i]) { map.accData[off]++; continue; }
              if (aggDocPos[i] == doc) {
                long rawVal = numericAggDvs[i].nextValue();
                aggDocPos[i] = numericAggDvs[i].nextDoc();
                switch (accType[i]) {
                  case 0: map.accData[off]++; break;
                  case 1: map.accData[off] += rawVal; break;
                  case 2: map.accData[off] += rawVal; map.accData[off + 1]++; break;
                }
              }
            }
          }
        } else {
          // Fallback: advanceExact per doc
          for (int doc = 0; doc < maxDoc; doc++) {
            long key0 = 0;
            if (dv0.advanceExact(doc)) key0 = dv0.nextValue();
            int bkt = (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets;
            FlatSingleKeyMap map = bucketMaps[bkt];
            int slot = map.findOrInsert(key0);
            int base = slot * slotsPerGroup;
            for (int i = 0; i < numAggs; i++) {
              int off = base + accOffset[i];
              if (isCountStar[i]) { map.accData[off]++; continue; }
              if (numericAggDvs[i] != null && numericAggDvs[i].advanceExact(doc)) {
                long rawVal = numericAggDvs[i].nextValue();
                switch (accType[i]) {
                  case 0: map.accData[off]++; break;
                  case 1: map.accData[off] += rawVal; break;
                  case 2: map.accData[off] += rawVal; map.accData[off + 1]++; break;
                }
              }
            }
          }
        }
      } else {
        // General fallback with liveDocs/applyLength
        for (int doc = 0; doc < maxDoc; doc++) {
          if (liveDocs != null && !liveDocs.get(doc)) continue;
          long key0 = 0;
          if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
          int bkt = (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets;
          FlatSingleKeyMap map = bucketMaps[bkt];
          int slot = map.findOrInsert(key0);
          int base = slot * slotsPerGroup;
          for (int i = 0; i < numAggs; i++) {
            int off = base + accOffset[i];
            if (isCountStar[i]) { map.accData[off]++; continue; }
            if (aggApplyLength[i]) {
              if (lengthVarcharDvs[i] != null && lengthVarcharDvs[i].advanceExact(doc)) {
                int ord = (int) lengthVarcharDvs[i].nextOrd();
                long rawVal = (ordLengthMaps[i] != null && ord < ordLengthMaps[i].length)
                    ? ordLengthMaps[i][ord] : lengthVarcharDvs[i].lookupOrd(ord).length;
                switch (accType[i]) {
                  case 0: map.accData[off]++; break;
                  case 1: map.accData[off] += rawVal; break;
                  case 2: map.accData[off] += rawVal; map.accData[off + 1]++; break;
                }
              }
            } else {
              if (numericAggDvs[i] != null && numericAggDvs[i].advanceExact(doc)) {
                long rawVal = numericAggDvs[i].nextValue();
                switch (accType[i]) {
                  case 0: map.accData[off]++; break;
                  case 1: map.accData[off] += rawVal; break;
                  case 2: map.accData[off] += rawVal; map.accData[off + 1]++; break;
                }
              }
            }
          }
        }
      }
    } else {
      // Filtered path: use Weight+Scorer
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) return;
      DocIdSetIterator it = scorer.iterator();
      int doc;
      while ((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        long key0 = 0;
        if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
        int bkt = (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets;
        FlatSingleKeyMap map = bucketMaps[bkt];
        int slot = map.findOrInsert(key0);
        int base = slot * slotsPerGroup;
        for (int i = 0; i < numAggs; i++) {
          int off = base + accOffset[i];
          if (isCountStar[i]) { map.accData[off]++; continue; }
          if (aggApplyLength[i]) {
            if (lengthVarcharDvs[i] != null && lengthVarcharDvs[i].advanceExact(doc)) {
              int ord = (int) lengthVarcharDvs[i].nextOrd();
              long rawVal = (ordLengthMaps[i] != null && ord < ordLengthMaps[i].length)
                  ? ordLengthMaps[i][ord] : lengthVarcharDvs[i].lookupOrd(ord).length;
              switch (accType[i]) {
                case 0: map.accData[off]++; break;
                case 1: map.accData[off] += rawVal; break;
                case 2: map.accData[off] += rawVal; map.accData[off + 1]++; break;
              }
            }
          } else {
            if (numericAggDvs[i] != null && numericAggDvs[i].advanceExact(doc)) {
              long rawVal = numericAggDvs[i].nextValue();
              switch (accType[i]) {
                case 0: map.accData[off]++; break;
                case 1: map.accData[off] += rawVal; break;
                case 2: map.accData[off] += rawVal; map.accData[off + 1]++; break;
              }
            }
          }
        }
      }
    }
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
      long topN,
      int bucket,
      int numBuckets)
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
    final FlatSingleKeyMap flatMap;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-1key-flat")) {

      java.util.List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      boolean isMatchAll = query instanceof MatchAllDocsQuery;
      boolean canParallelize =
          !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;

      if (canParallelize) {
        // Check if doc-range parallelism applies: COUNT(*)-only, matchAll, no bucketing
        boolean allCountStar = true;
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i]) { allCountStar = false; break; }
        }

        if (allCountStar && isMatchAll && numBuckets <= 1) {
          // MatchAll COUNT(*) path: load columnar cache per segment, then scan.
          java.util.List<long[]> segKeyArrays = new java.util.ArrayList<>();
          java.util.List<Bits> segLiveDocs = new java.util.ArrayList<>();

          long totalDocs = 0;
          for (LeafReaderContext leafCtx : leaves) {
            long[] keyValues = loadNumericColumn(leafCtx, keyInfos.get(0).name());
            segKeyArrays.add(keyValues);
            segLiveDocs.add(leafCtx.reader().getLiveDocs());
            totalDocs += keyValues.length;
          }

          // For high-cardinality keys (totalDocs > 10M), scan sequentially into main map.
          // Parallel workers each build 4.4M-entry maps that thrash L3 cache and require
          // expensive mergeFrom. Sequential scan uses ONE map with better cache locality.
          if (totalDocs > 10_000_000) {
            // Pre-size main map larger to reduce resize probability
            flatMap = new FlatSingleKeyMap(slotsPerGroup, 8_000_000);
            for (int s = 0; s < segKeyArrays.size(); s++) {
              scanDocRangeFlatSingleKeyCountStar(
                  segKeyArrays.get(s), 0, segKeyArrays.get(s).length,
                  segLiveDocs.get(s), flatMap, accOffset[0], slotsPerGroup);
            }
          } else {
          // Doc-range parallel path for lower-cardinality keys
          flatMap = new FlatSingleKeyMap(slotsPerGroup, 4_000_000);
          int numWorkers = THREADS_PER_SHARD;
          long docsPerWorker = Math.max(1, (totalDocs + numWorkers - 1) / numWorkers);
          java.util.List<int[]> workUnits = new java.util.ArrayList<>();
          for (int s = 0; s < leaves.size(); s++) {
            int maxDoc = segKeyArrays.get(s).length;
            if (maxDoc == 0) continue;
            for (int start = 0; start < maxDoc; start += (int) docsPerWorker) {
              int end = (int) Math.min(start + docsPerWorker, maxDoc);
              workUnits.add(new int[]{s, start, end});
            }
          }

          int actualWorkers = workUnits.size();
          @SuppressWarnings("unchecked")
          java.util.concurrent.CompletableFuture<FlatSingleKeyMap>[] futures =
              new java.util.concurrent.CompletableFuture[actualWorkers];

          for (int w = 0; w < actualWorkers; w++) {
            final int[] unit = workUnits.get(w);
            final long[] keyValues = segKeyArrays.get(unit[0]);
            final Bits liveDocs = segLiveDocs.get(unit[0]);
            final int startDoc = unit[1];
            final int endDoc = unit[2];
            futures[w] =
                java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> {
                      int docCount = endDoc - startDoc;
                      int initCap = (int) Math.min((long) docCount * 10 / 7 + 1, 4_000_000);
                      FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup, initCap);
                      scanDocRangeFlatSingleKeyCountStar(
                          keyValues, startDoc, endDoc, liveDocs,
                          localMap, accOffset[0], slotsPerGroup);
                      return localMap;
                    },
                    PARALLEL_POOL);
          }

          java.util.concurrent.CompletableFuture.allOf(futures).join();
          for (int fi = 0; fi < futures.length; fi++) {
            FlatSingleKeyMap workerMap = futures[fi].join();
            if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
            futures[fi] = null;
          }
          } // end else (parallel path)
        } else {
        // Segment-parallel path: partition segments across workers, each with own FlatSingleKeyMap
        int mainInitCap = 4_000_000;
        flatMap = new FlatSingleKeyMap(slotsPerGroup, mainInitCap);
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        @SuppressWarnings("unchecked")
        java.util.List<LeafReaderContext>[] workerSegments = new java.util.List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++)
          workerSegments[i] = new java.util.ArrayList<>();
        // Largest-first greedy assignment for balanced load
        java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
        sortedLeaves.sort(
            (a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }

        final Weight weight;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        } else {
          weight = null;
        }

        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<FlatSingleKeyMap>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final java.util.List<LeafReaderContext> mySegments = workerSegments[w];
          final long myDocCount = workerDocCounts[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    // Pre-size to avoid resize cascades, capped at 1M to limit memory
                    int initCap = (int) Math.min(myDocCount * 10 / 7 + 1, 4_000_000);
                    FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup, initCap);
                    try {
                      for (LeafReaderContext leafCtx : mySegments) {
                        scanSegmentFlatSingleKey(
                            leafCtx, weight, isMatchAll, localMap, keyInfos, specs,
                            numAggs, isCountStar, accType, accOffset, slotsPerGroup,
                            bucket, numBuckets);
                      }
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                    return localMap;
                  },
                  PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();
        for (int fi = 0; fi < futures.length; fi++) {
          FlatSingleKeyMap workerMap = futures[fi].join();
          if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
          futures[fi] = null; // release worker map for GC
        }
        } // end segment-parallel else
      } else {
        // Sequential path: single FlatSingleKeyMap for all segments
        flatMap = new FlatSingleKeyMap(slotsPerGroup, 4_000_000);
        Weight weight = null;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        }
        for (LeafReaderContext leafCtx : leaves) {
          scanSegmentFlatSingleKey(
              leafCtx, weight, isMatchAll, flatMap, keyInfos, specs,
              numAggs, isCountStar, accType, accOffset, slotsPerGroup,
              bucket, numBuckets);
        }
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
        if (flatMap.keys[slot] == FlatSingleKeyMap.EMPTY_KEY) continue;
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
        if (flatMap.keys[slot] != FlatSingleKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[flatMap.size];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.keys[slot] != FlatSingleKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
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
   * Same as collectFlatSingleKeyDoc but handles applyLength: reads VARCHAR DocValues for length.
   */
  private static void collectFlatSingleKeyDocWithLength(
      int doc,
      SortedNumericDocValues dv0,
      FlatSingleKeyMap flatMap,
      int slotsPerGroup,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      SortedNumericDocValues[] numericAggDvs,
      SortedSetDocValues[] lengthVarcharDvs,
      int[][] ordLengthMaps,
      boolean[] aggApplyLength,
      int bucket,
      int numBuckets)
      throws IOException {
    long key0 = 0;
    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
    // Hash-partition filter: skip docs not in this bucket
    if (numBuckets > 1
        && (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets != bucket) {
      return;
    }
    int slot = flatMap.findOrInsert(key0);
    int base = slot * slotsPerGroup;

    for (int i = 0; i < numAggs; i++) {
      int off = base + accOffset[i];
      if (isCountStar[i]) {
        flatMap.accData[off]++;
        continue;
      }
      long rawVal = 0;
      boolean hasValue = false;
      if (aggApplyLength[i]) {
        // Use pre-computed ordinal→length map for O(1) per-doc lookup
        SortedSetDocValues varcharDv = lengthVarcharDvs[i];
        if (varcharDv != null && varcharDv.advanceExact(doc)) {
          int ord = (int) varcharDv.nextOrd();
          rawVal =
              (ordLengthMaps[i] != null && ord < ordLengthMaps[i].length)
                  ? ordLengthMaps[i][ord]
                  : varcharDv.lookupOrd(ord).length;
          hasValue = true;
        }
      } else {
        SortedNumericDocValues aggDv = numericAggDvs[i];
        if (aggDv != null && aggDv.advanceExact(doc)) {
          rawVal = aggDv.nextValue();
          hasValue = true;
        }
      }
      if (hasValue) {
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

  /**
   * Scan a single segment for the flat single-key numeric GROUP BY path. Handles both MatchAll
   * (direct DocValues iteration) and filtered (Weight+Scorer) paths. Accumulates into the provided
   * FlatSingleKeyMap.
   */
  private static void scanSegmentFlatSingleKey(
      LeafReaderContext leafCtx,
      Weight weight,
      boolean isMatchAll,
      FlatSingleKeyMap flatMap,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      int slotsPerGroup,
      int bucket,
      int numBuckets)
      throws Exception {
    LeafReader reader = leafCtx.reader();
    SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());

    final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
    final SortedSetDocValues[] lengthVarcharDvs = new SortedSetDocValues[numAggs];
    final int[][] ordLengthMaps = new int[numAggs][];
    final boolean[] aggApplyLength = new boolean[numAggs];
    for (int i = 0; i < numAggs; i++) {
      if (!isCountStar[i]) {
        if (specs.get(i).applyLength) {
          SortedSetDocValues varcharDv = reader.getSortedSetDocValues(specs.get(i).arg);
          lengthVarcharDvs[i] = varcharDv;
          aggApplyLength[i] = true;
          if (varcharDv != null) {
            int ordCount = (int) Math.min(varcharDv.getValueCount(), 10_000_000);
            int[] ordLengths = new int[ordCount];
            for (int ord = 0; ord < ordCount; ord++) {
              ordLengths[ord] = varcharDv.lookupOrd(ord).length;
            }
            ordLengthMaps[i] = ordLengths;
          }
        } else {
          numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
        }
      }
    }

    if (isMatchAll) {
      int maxDoc = reader.maxDoc();
      Bits liveDocs = reader.getLiveDocs();

      if (liveDocs == null && dv0 != null) {
        boolean allCountStar = true;
        boolean hasApplyLen = false;
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i]) allCountStar = false;
          if (aggApplyLength[i]) hasApplyLen = true;
        }

        if (allCountStar) {
          // Ultra-fast path: only COUNT(*), use nextDoc() on key column
          if (numBuckets <= 1) {
            // Columnar cache path: load key column into flat array for faster sequential access
            long[] keyValues = loadNumericColumn(leafCtx, keyInfos.get(0).name());
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0 = keyValues[doc];
              int slot = flatMap.findOrInsert(key0);
              flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
            }
          } else {
            int doc = dv0.nextDoc();
            while (doc != DocIdSetIterator.NO_MORE_DOCS) {
              long key0 = dv0.nextValue();
              if ((SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets == bucket) {
                int slot = flatMap.findOrInsert(key0);
                flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
              }
              doc = dv0.nextDoc();
            }
          }
        } else if (!hasApplyLen) {
          // Fast path: numeric aggs, use nextDoc() lockstep iteration.
          boolean allAggDense = true;
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i] && numericAggDvs[i] == null) {
              allAggDense = false;
              break;
            }
          }
          if (allAggDense) {
            int[] aggDocPos = new int[numAggs];
            for (int i = 0; i < numAggs; i++) {
              if (!isCountStar[i]) {
                aggDocPos[i] = numericAggDvs[i].nextDoc();
              }
            }
            int keyDoc = dv0.nextDoc();
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0;
              if (keyDoc == doc) {
                key0 = dv0.nextValue();
                keyDoc = dv0.nextDoc();
              } else {
                key0 = 0;
              }
              // Hash-partition filter: skip docs not in this bucket
              if (numBuckets > 1
                  && (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets != bucket) {
                // Still need to advance agg DocValues iterators to stay in sync
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i] && aggDocPos[i] == doc) {
                    numericAggDvs[i].nextValue();
                    aggDocPos[i] = numericAggDvs[i].nextDoc();
                  }
                }
                continue;
              }
              int slot = flatMap.findOrInsert(key0);
              int base = slot * slotsPerGroup;
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) {
                  flatMap.accData[off]++;
                  continue;
                }
                if (aggDocPos[i] == doc) {
                  long rawVal = numericAggDvs[i].nextValue();
                  aggDocPos[i] = numericAggDvs[i].nextDoc();
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
              collectFlatSingleKeyDocWithLength(
                  doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, lengthVarcharDvs,
                  ordLengthMaps, aggApplyLength, bucket, numBuckets);
            }
          }
        } else {
          // hasApplyLen path: check if we can use sequential nextDoc() lockstep
          boolean allAggDense = true;
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              if (aggApplyLength[i] && lengthVarcharDvs[i] == null) { allAggDense = false; break; }
              if (!aggApplyLength[i] && numericAggDvs[i] == null) { allAggDense = false; break; }
            }
          }
          if (allAggDense && numBuckets <= 1) {
            // Sequential lockstep for MatchAll + LENGTH aggs
            int keyDoc = dv0.nextDoc();
            int[] aggDocPos = new int[numAggs];
            int[] lenDocPos = new int[numAggs];
            for (int i = 0; i < numAggs; i++) {
              if (isCountStar[i]) continue;
              if (aggApplyLength[i]) {
                lenDocPos[i] = lengthVarcharDvs[i].nextDoc();
              } else {
                aggDocPos[i] = numericAggDvs[i].nextDoc();
              }
            }
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0 = 0;
              if (keyDoc == doc) {
                key0 = dv0.nextValue();
                keyDoc = dv0.nextDoc();
              }
              int slot = flatMap.findOrInsert(key0);
              int base = slot * slotsPerGroup;
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) {
                  flatMap.accData[off]++;
                  continue;
                }
                if (aggApplyLength[i]) {
                  if (lenDocPos[i] == doc) {
                    int ord = (int) lengthVarcharDvs[i].nextOrd();
                    long rawVal = (ordLengthMaps[i] != null && ord < ordLengthMaps[i].length)
                        ? ordLengthMaps[i][ord]
                        : lengthVarcharDvs[i].lookupOrd(ord).length;
                    lenDocPos[i] = lengthVarcharDvs[i].nextDoc();
                    switch (accType[i]) {
                      case 0: flatMap.accData[off]++; break;
                      case 1: flatMap.accData[off] += rawVal; break;
                      case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                    }
                  }
                } else {
                  if (aggDocPos[i] == doc) {
                    long rawVal = numericAggDvs[i].nextValue();
                    aggDocPos[i] = numericAggDvs[i].nextDoc();
                    switch (accType[i]) {
                      case 0: flatMap.accData[off]++; break;
                      case 1: flatMap.accData[off] += rawVal; break;
                      case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                    }
                  }
                }
              }
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              collectFlatSingleKeyDocWithLength(
                  doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, lengthVarcharDvs,
                  ordLengthMaps, aggApplyLength, bucket, numBuckets);
            }
          }
        }
      } else if (liveDocs == null) {
        for (int doc = 0; doc < maxDoc; doc++) {
          collectFlatSingleKeyDocWithLength(
              doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
              accType, accOffset, numericAggDvs, lengthVarcharDvs,
              ordLengthMaps, aggApplyLength, bucket, numBuckets);
        }
      } else {
        for (int doc = 0; doc < maxDoc; doc++) {
          if (liveDocs.get(doc)) {
            collectFlatSingleKeyDocWithLength(
                doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                accType, accOffset, numericAggDvs, lengthVarcharDvs,
                ordLengthMaps, aggApplyLength, bucket, numBuckets);
          }
        }
      }
    } else {
      // Filtered path
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) return;

      // Check selectivity: if filter matches <50% of docs, use bitset+nextDoc lockstep
      int maxDoc = reader.maxDoc();
      int estCount = weight.count(leafCtx);
      boolean hasApplyLen = false;
      for (int i = 0; i < numAggs; i++) {
        if (aggApplyLength[i]) { hasApplyLen = true; break; }
      }
      // For filtered queries, use forward-only DV iteration (avoids advanceExact binary search).
      // When selectivity > 90%, treat as near-MatchAll and use nextDoc lockstep.
      DocIdSetIterator docIt = scorer.iterator();
      int estCount2 = weight.count(leafCtx);
      int maxDoc2 = reader.maxDoc();
      boolean nearMatchAll = (estCount2 >= 0 && estCount2 > maxDoc2 * 9 / 10);

      if (nearMatchAll && numBuckets <= 1) {
        // Near-MatchAll optimization: collect matching docs into bitset, then use
        // MatchAll-style lockstep iteration with bitset check. Avoids advanceExact.
        org.apache.lucene.util.FixedBitSet matchBits = new org.apache.lucene.util.FixedBitSet(maxDoc2);
        int d;
        while ((d = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          matchBits.set(d);
        }
        if (dv0 != null) {
          // Initialize all DV iterators for lockstep
          int[] aggDocPos2 = new int[numAggs];
          int[] lenDocPos2 = new int[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (aggApplyLength[i] && lengthVarcharDvs[i] != null) {
              lenDocPos2[i] = lengthVarcharDvs[i].nextDoc();
              aggDocPos2[i] = DocIdSetIterator.NO_MORE_DOCS;
            } else if (!isCountStar[i] && numericAggDvs[i] != null) {
              aggDocPos2[i] = numericAggDvs[i].nextDoc();
              lenDocPos2[i] = DocIdSetIterator.NO_MORE_DOCS;
            } else {
              aggDocPos2[i] = DocIdSetIterator.NO_MORE_DOCS;
              lenDocPos2[i] = DocIdSetIterator.NO_MORE_DOCS;
            }
          }
          int keyDoc = dv0.nextDoc();
          for (int doc = 0; doc < maxDoc2; doc++) {
            if (!matchBits.get(doc)) {
              // Advance past non-matching docs
              if (keyDoc == doc) { dv0.nextValue(); keyDoc = dv0.nextDoc(); }
              for (int i = 0; i < numAggs; i++) {
                if (aggDocPos2[i] == doc) { numericAggDvs[i].nextValue(); aggDocPos2[i] = numericAggDvs[i].nextDoc(); }
                if (lenDocPos2[i] == doc) { lengthVarcharDvs[i].nextOrd(); lenDocPos2[i] = lengthVarcharDvs[i].nextDoc(); }
              }
              continue;
            }
            long key0 = (keyDoc == doc) ? dv0.nextValue() : 0;
            if (keyDoc == doc) keyDoc = dv0.nextDoc();
            int slot = flatMap.findOrInsert(key0);
            int base = slot * slotsPerGroup;
            for (int i = 0; i < numAggs; i++) {
              int off = base + accOffset[i];
              if (isCountStar[i]) { flatMap.accData[off]++; continue; }
              if (aggApplyLength[i]) {
                if (lenDocPos2[i] == doc) {
                  int ord = (int) lengthVarcharDvs[i].nextOrd();
                  long rawVal = (ordLengthMaps[i] != null && ord < ordLengthMaps[i].length)
                      ? ordLengthMaps[i][ord] : lengthVarcharDvs[i].lookupOrd(ord).length;
                  lenDocPos2[i] = lengthVarcharDvs[i].nextDoc();
                  switch (accType[i]) {
                    case 0: flatMap.accData[off]++; break;
                    case 1: flatMap.accData[off] += rawVal; break;
                    case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                  }
                }
              } else if (aggDocPos2[i] == doc) {
                long rawVal = numericAggDvs[i].nextValue();
                aggDocPos2[i] = numericAggDvs[i].nextDoc();
                switch (accType[i]) {
                  case 0: flatMap.accData[off]++; break;
                  case 1: flatMap.accData[off] += rawVal; break;
                  case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                }
              }
            }
          }
        }
      } else {
        // Standard filtered path with forward-only DV advance
        int dvPos0 = (dv0 != null) ? dv0.nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
        int[] aggDvPos = new int[numAggs];
        int[] lenDvPos = new int[numAggs];
        for (int i = 0; i < numAggs; i++) {
          if (aggApplyLength[i] && lengthVarcharDvs[i] != null) {
            lenDvPos[i] = lengthVarcharDvs[i].nextDoc();
            aggDvPos[i] = DocIdSetIterator.NO_MORE_DOCS;
          } else if (!isCountStar[i] && numericAggDvs[i] != null) {
            aggDvPos[i] = numericAggDvs[i].nextDoc();
            lenDvPos[i] = DocIdSetIterator.NO_MORE_DOCS;
          } else {
            aggDvPos[i] = DocIdSetIterator.NO_MORE_DOCS;
            lenDvPos[i] = DocIdSetIterator.NO_MORE_DOCS;
          }
        }
        int doc;
        while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          long key0 = 0;
          if (dvPos0 != DocIdSetIterator.NO_MORE_DOCS && dvPos0 < doc) {
            dvPos0 = dv0.advance(doc);
          }
          if (dvPos0 == doc) { key0 = dv0.nextValue(); dvPos0 = dv0.nextDoc(); }
          if (numBuckets > 1 && (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets != bucket) continue;
          int slot = flatMap.findOrInsert(key0);
          int base = slot * slotsPerGroup;
          for (int i = 0; i < numAggs; i++) {
            int off = base + accOffset[i];
            if (isCountStar[i]) { flatMap.accData[off]++; continue; }
            long rawVal = 0;
            boolean hasValue = false;
            if (aggApplyLength[i]) {
              if (lenDvPos[i] != DocIdSetIterator.NO_MORE_DOCS && lenDvPos[i] < doc) {
                lenDvPos[i] = lengthVarcharDvs[i].advance(doc);
              }
              if (lenDvPos[i] == doc) {
                int ord = (int) lengthVarcharDvs[i].nextOrd();
                rawVal = (ordLengthMaps[i] != null && ord < ordLengthMaps[i].length)
                    ? ordLengthMaps[i][ord] : lengthVarcharDvs[i].lookupOrd(ord).length;
                hasValue = true;
                lenDvPos[i] = lengthVarcharDvs[i].nextDoc();
              }
            } else {
              if (aggDvPos[i] != DocIdSetIterator.NO_MORE_DOCS && aggDvPos[i] < doc) {
                aggDvPos[i] = numericAggDvs[i].advance(doc);
              }
              if (aggDvPos[i] == doc) {
                rawVal = numericAggDvs[i].nextValue();
                hasValue = true;
                aggDvPos[i] = numericAggDvs[i].nextDoc();
              }
            }
            if (hasValue) {
              switch (accType[i]) {
                case 0: flatMap.accData[off]++; break;
                case 1: flatMap.accData[off] += rawVal; break;
                case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
              }
            }
          }
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
      // Try single-bucket first. Only fall back to multi-bucket on overflow.
      // Previous approach used totalDocs/MAX_CAPACITY which overestimates unique groups.
      try {
        return executeTwoKeyNumericFlat(
            shard, query, keyInfos, specs, columnTypeMap, groupByKeys,
            numAggs, isCountStar, accType, sortAggIndex, sortAscending, topN, 0, 1);
      } catch (RuntimeException overflowEx) {
        if (overflowEx.getMessage() == null
            || !overflowEx.getMessage().contains("GROUP BY exceeded memory limit")) {
          throw overflowEx;
        }
        // Flat map overflowed — fall back to multi-bucket multi-pass
        int numBuckets;
        try (org.opensearch.index.engine.Engine.Searcher estSearcher =
            shard.acquireSearcher("dqe-fused-groupby-estimate")) {
          long totalDocs = 0;
          for (LeafReaderContext leaf : estSearcher.getIndexReader().leaves()) {
            totalDocs += leaf.reader().maxDoc();
          }
          numBuckets = Math.max(2, (int) Math.ceil((double) totalDocs / FlatTwoKeyMap.MAX_CAPACITY));
        }
        int parallelBuckets = Math.min(numBuckets, THREADS_PER_SHARD);
        if (parallelBuckets > 1 && !"off".equals(PARALLELISM_MODE)) {
          @SuppressWarnings("unchecked")
          java.util.concurrent.CompletableFuture<List<Page>>[] futures =
              new java.util.concurrent.CompletableFuture[numBuckets];
          for (int b = 0; b < numBuckets; b++) {
            final int bkt = b;
            futures[b] =
                java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return executeTwoKeyNumericFlat(
                            shard, query, keyInfos, specs, columnTypeMap, groupByKeys,
                            numAggs, isCountStar, accType, sortAggIndex, sortAscending,
                            topN, bkt, numBuckets);
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    },
                    PARALLEL_POOL);
          }
          java.util.concurrent.CompletableFuture.allOf(futures).join();
          List<Page> allBucketResults = new ArrayList<>();
          for (var future : futures) {
            allBucketResults.addAll(future.join());
          }
          if (allBucketResults.isEmpty()) return List.of();
          if (allBucketResults.size() == 1) return allBucketResults;
          return mergePartitionedPages(
              allBucketResults, keyInfos, specs, columnTypeMap, groupByKeys,
              numAggs, accType, sortAggIndex, sortAscending, topN);
        }
        // Sequential fallback
        List<Page> allBucketResults = new ArrayList<>();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
          allBucketResults.addAll(executeTwoKeyNumericFlat(
              shard, query, keyInfos, specs, columnTypeMap, groupByKeys,
              numAggs, isCountStar, accType, sortAggIndex, sortAscending,
              topN, bucket, numBuckets));
        }
        if (allBucketResults.isEmpty()) return List.of();
        if (allBucketResults.size() == 1) return allBucketResults;
        return mergePartitionedPages(
            allBucketResults, keyInfos, specs, columnTypeMap, groupByKeys,
            numAggs, accType, sortAggIndex, sortAscending, topN);
      }
    }

    // Use TwoKeyHashMap with AccumulatorGroup objects and pre-computed dispatch
    final TwoKeyHashMap twoKeyMap = new TwoKeyHashMap(specs);

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-2key")) {

      if (query instanceof MatchAllDocsQuery) {
        // MatchAll: use Collector path
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
                    if (!(spec.argType instanceof VarcharType)) {
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
                    accumulateDocNumeric(
                        doc, accGroup, numAggs, isCountStar, isDoubleArg, accType, numericAggDvs);
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });
      } else {
        // Filtered path: manual Weight+Scorer loop to skip empty segments early
        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        Weight weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          Scorer scorer = weight.scorer(leafCtx);
          if (scorer == null) continue;

          LeafReader reader = leafCtx.reader();
          SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());
          SortedNumericDocValues dv1 = reader.getSortedNumericDocValues(keyInfos.get(1).name());
          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              AggSpec spec = specs.get(i);
              if (!(spec.argType instanceof VarcharType)) {
                numericAggDvs[i] = reader.getSortedNumericDocValues(spec.arg);
              }
            }
          }

          DocIdSetIterator docIt = scorer.iterator();
          int doc;
          while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            long key0 = 0, key1 = 0;
            if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
            if (dv1 != null && dv1.advanceExact(doc)) key1 = dv1.nextValue();
            AccumulatorGroup accGroup = twoKeyMap.getOrCreate(key0, key1);
            accumulateDocNumeric(
                doc, accGroup, numAggs, isCountStar, isDoubleArg, accType, numericAggDvs);
          }
        }
      }
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
      long topN,
      int bucket,
      int numBuckets)
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

    // Deduplicate DV readers: multiple aggs may reference the same column (e.g., AVG decomposed
    // into SUM(col) + COUNT(col)). Map each agg to a unique DV reader index to avoid redundant
    // advanceExact calls in the hot loop.
    final Map<String, Integer> dvColumnIndex = new HashMap<>();
    final String[] uniqueDvColumns; // distinct column names that need DV reads
    final int[] aggToDvIdx =
        new int[numAggs]; // maps agg index -> uniqueDvColumns index (-1 for COUNT*)
    {
      List<String> uniqueCols = new ArrayList<>();
      for (int i = 0; i < numAggs; i++) {
        if (isCountStar[i]) {
          aggToDvIdx[i] = -1;
          continue;
        }
        String col = specs.get(i).arg;
        Integer existing = dvColumnIndex.get(col);
        if (existing != null) {
          aggToDvIdx[i] = existing;
        } else {
          int idx = uniqueCols.size();
          dvColumnIndex.put(col, idx);
          uniqueCols.add(col);
          aggToDvIdx[i] = idx;
        }
      }
      uniqueDvColumns = uniqueCols.toArray(new String[0]);
    }
    final int numUniqueDvs = uniqueDvColumns.length;
    // Pre-check if any DV deduplication applies (skip overhead when no duplicates)
    int countStarCount = 0;
    for (int i = 0; i < numAggs; i++) {
      if (isCountStar[i]) countStarCount++;
    }
    final boolean hasDvDuplicates = numUniqueDvs < (numAggs - countStarCount);

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-2key-flat")) {

      java.util.List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      boolean isMatchAll = query instanceof MatchAllDocsQuery;
      boolean canParallelize =
          !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;

      if (canParallelize) {
        // Parallel path: partition segments across workers, each with own FlatTwoKeyMap
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        @SuppressWarnings("unchecked")
        java.util.List<LeafReaderContext>[] workerSegments = new java.util.List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++)
          workerSegments[i] = new java.util.ArrayList<>();
        // Largest-first greedy assignment for balanced load
        java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
        sortedLeaves.sort(
            (a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }

        final Weight weight;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        } else {
          weight = null;
        }

        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<FlatTwoKeyMap>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        // Capture effectively-final locals for lambda
        final boolean[] fIsCountStar = isCountStar;
        final int[] fAccType = accType;
        final int[] fAccOffset = accOffset;
        final int fSlotsPerGroup = slotsPerGroup;
        final int fBucket = bucket;
        final int fNumBuckets = numBuckets;

        for (int w = 0; w < numWorkers; w++) {
          final java.util.List<LeafReaderContext> mySegments = workerSegments[w];
          final long myDocCount = workerDocCounts[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    int initCap = (int) Math.min(myDocCount * 10 / 7 + 1, 4_000_000);
                    FlatTwoKeyMap localMap = new FlatTwoKeyMap(fSlotsPerGroup, initCap);
                    try {
                      for (LeafReaderContext leafCtx : mySegments) {
                        scanSegmentFlatTwoKey(
                            leafCtx, weight, isMatchAll, localMap, keyInfos, specs,
                            numAggs, fIsCountStar, fAccType, fAccOffset,
                            fSlotsPerGroup, fBucket, fNumBuckets);
                      }
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                    return localMap;
                  },
                  PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();
        for (int fi = 0; fi < futures.length; fi++) {
          FlatTwoKeyMap workerMap = futures[fi].join();
          if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
          futures[fi] = null; // release worker map for GC
        }
      } else {
        // Sequential fallback: single FlatTwoKeyMap for all segments
        Weight weight = null;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        }
        for (LeafReaderContext leafCtx : leaves) {
          scanSegmentFlatTwoKey(
              leafCtx, weight, isMatchAll, flatMap, keyInfos, specs,
              numAggs, isCountStar, accType, accOffset,
              slotsPerGroup, bucket, numBuckets);
        }
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
        if (flatMap.keys0[slot] == FlatTwoKeyMap.EMPTY_KEY) continue;
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
        if (flatMap.keys0[slot] != FlatTwoKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[flatMap.size];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.keys0[slot] != FlatTwoKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
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

  /**
   * Scan a single segment for the flat two-key numeric GROUP BY path. Handles both MatchAll and
   * Filtered paths, including DV deduplication, liveDocs, and bucket filtering. Extracted from
   * {@link #executeTwoKeyNumericFlat} to enable parallel segment processing.
   */
  private static void scanSegmentFlatTwoKey(
      LeafReaderContext leafCtx,
      Weight weight,
      boolean isMatchAll,
      FlatTwoKeyMap flatMap,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      int slotsPerGroup,
      int bucket,
      int numBuckets)
      throws Exception {
    LeafReader reader = leafCtx.reader();
    SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());
    SortedNumericDocValues dv1 = reader.getSortedNumericDocValues(keyInfos.get(1).name());

    // Deduplicate DV readers: multiple aggs may reference the same column
    final Map<String, Integer> dvColumnIndex = new HashMap<>();
    final List<String> uniqueCols = new ArrayList<>();
    final int[] aggToDvIdx = new int[numAggs];
    for (int i = 0; i < numAggs; i++) {
      if (isCountStar[i]) {
        aggToDvIdx[i] = -1;
        continue;
      }
      String col = specs.get(i).arg;
      Integer existing = dvColumnIndex.get(col);
      if (existing != null) {
        aggToDvIdx[i] = existing;
      } else {
        int idx = uniqueCols.size();
        dvColumnIndex.put(col, idx);
        uniqueCols.add(col);
        aggToDvIdx[i] = idx;
      }
    }
    final String[] uniqueDvColumns = uniqueCols.toArray(new String[0]);
    final int numUniqueDvs = uniqueDvColumns.length;
    int countStarCount = 0;
    for (int i = 0; i < numAggs; i++) {
      if (isCountStar[i]) countStarCount++;
    }
    final boolean hasDvDuplicates = numUniqueDvs < (numAggs - countStarCount);

    // Set up per-agg DV readers (non-dedup path)
    final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
    if (!hasDvDuplicates) {
      for (int i = 0; i < numAggs; i++) {
        if (!isCountStar[i]) {
          numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
        }
      }
    }

    if (isMatchAll) {
      int maxDoc = reader.maxDoc();
      Bits liveDocs = reader.getLiveDocs();

      if (hasDvDuplicates) {
        // Deduplicated path: read each unique column once per doc
        final SortedNumericDocValues[] uniqueDvReaders =
            new SortedNumericDocValues[numUniqueDvs];
        for (int i = 0; i < numUniqueDvs; i++) {
          uniqueDvReaders[i] = reader.getSortedNumericDocValues(uniqueDvColumns[i]);
        }
        final long[] dvValues = new long[numUniqueDvs];
        final boolean[] dvHasValue = new boolean[numUniqueDvs];

        if (liveDocs == null) {
          // Try nextDoc() lockstep for dense columns (avoids advanceExact binary search)
          boolean allDenseDedup = dv0 != null && dv1 != null;
          if (allDenseDedup) {
            for (int i = 0; i < numUniqueDvs; i++) {
              if (uniqueDvReaders[i] == null) { allDenseDedup = false; break; }
            }
          }
          if (allDenseDedup && maxDoc > 0) {
            int keyDoc0 = dv0.nextDoc();
            int keyDoc1 = dv1.nextDoc();
            int[] uniqueDvDocs = new int[numUniqueDvs];
            for (int i = 0; i < numUniqueDvs; i++) {
              uniqueDvDocs[i] = uniqueDvReaders[i].nextDoc();
            }
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0 = 0, key1 = 0;
              if (keyDoc0 == doc) { key0 = dv0.nextValue(); keyDoc0 = dv0.nextDoc(); }
              if (keyDoc1 == doc) { key1 = dv1.nextValue(); keyDoc1 = dv1.nextDoc(); }
              if (numBuckets > 1) {
                int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
                if (docBucket != bucket) continue;
              }
              int slot = flatMap.findOrInsert(key0, key1);
              int base = slot * slotsPerGroup;
              // Read each unique DV column once via lockstep
              for (int d = 0; d < numUniqueDvs; d++) {
                if (uniqueDvDocs[d] == doc) {
                  dvValues[d] = uniqueDvReaders[d].nextValue();
                  dvHasValue[d] = true;
                  uniqueDvDocs[d] = uniqueDvReaders[d].nextDoc();
                } else {
                  dvHasValue[d] = false;
                }
              }
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) { flatMap.accData[off]++; continue; }
                int dvIdx = aggToDvIdx[i];
                if (dvHasValue[dvIdx]) {
                  long rawVal = dvValues[dvIdx];
                  switch (accType[i]) {
                    case 0: flatMap.accData[off]++; break;
                    case 1: flatMap.accData[off] += rawVal; break;
                    case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                  }
                }
              }
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              collectFlatTwoKeyDocDedup(
                  doc, dv0, dv1, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, uniqueDvReaders, dvValues, dvHasValue,
                  aggToDvIdx, bucket, numBuckets);
            }
          }
        } else {
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs.get(doc)) {
              collectFlatTwoKeyDocDedup(
                  doc, dv0, dv1, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, uniqueDvReaders, dvValues, dvHasValue,
                  aggToDvIdx, bucket, numBuckets);
            }
          }
        }
      } else {
        // No duplicate DV columns: use original per-agg DV readers
        if (liveDocs == null) {
          // Try nextDoc() lockstep for dense columns (avoids advanceExact binary search)
          boolean allDense = dv0 != null && dv1 != null;
          if (allDense) {
            for (int i = 0; i < numAggs; i++) {
              if (!isCountStar[i] && numericAggDvs[i] == null) { allDense = false; break; }
            }
          }
          if (allDense && maxDoc > 0) {
            int keyDoc0 = dv0.nextDoc();
            int keyDoc1 = dv1.nextDoc();
            int[] aggDocs = new int[numAggs];
            for (int i = 0; i < numAggs; i++) {
              aggDocs[i] = isCountStar[i] ? DocIdSetIterator.NO_MORE_DOCS : numericAggDvs[i].nextDoc();
            }
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0 = 0, key1 = 0;
              if (keyDoc0 == doc) { key0 = dv0.nextValue(); keyDoc0 = dv0.nextDoc(); }
              if (keyDoc1 == doc) { key1 = dv1.nextValue(); keyDoc1 = dv1.nextDoc(); }
              if (numBuckets > 1) {
                int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
                if (docBucket != bucket) continue;
              }
              int slot = flatMap.findOrInsert(key0, key1);
              int base = slot * slotsPerGroup;
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) { flatMap.accData[off]++; continue; }
                if (aggDocs[i] == doc) {
                  long rawVal = numericAggDvs[i].nextValue();
                  aggDocs[i] = numericAggDvs[i].nextDoc();
                  switch (accType[i]) {
                    case 0: flatMap.accData[off]++; break;
                    case 1: flatMap.accData[off] += rawVal; break;
                    case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                  }
                }
              }
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              collectFlatTwoKeyDoc(
                  doc, dv0, dv1, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, bucket, numBuckets);
            }
          }
        } else {
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs.get(doc)) {
              collectFlatTwoKeyDoc(
                  doc, dv0, dv1, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, bucket, numBuckets);
            }
          }
        }
      }
    } else {
      // Filtered path: manual Weight+Scorer loop to skip empty segments early
      // (avoids DocValues opening for segments with no matching docs).
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) return; // No matching docs in this segment

      // Check selectivity: if filter matches <50% of docs, use bitset+nextDoc lockstep
      int maxDoc = reader.maxDoc();
      int estCount = weight.count(leafCtx);
      boolean useBitsetLockstep = false; // Disabled: causes EOFException on two-key DocValues

      // Columnar cache is counterproductive for filtered queries (loads full column
      // even when filter matches a small fraction). Use forward-only DV iteration
      // instead of advanceExact (avoids binary search per doc).
      if (!hasDvDuplicates && numBuckets <= 1) {
        DocIdSetIterator docIt = scorer.iterator();
        // Initialize DV positions for forward-only advance
        int dvPos0 = (dv0 != null) ? dv0.nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
        int dvPos1 = (dv1 != null) ? dv1.nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
        int[] aggDvPos = new int[numAggs];
        for (int i = 0; i < numAggs; i++) {
          aggDvPos[i] = (!isCountStar[i] && numericAggDvs[i] != null)
              ? numericAggDvs[i].nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
        }
        int doc;
        while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          long key0 = 0, key1 = 0;
          // Forward-advance key DVs to current doc
          if (dvPos0 != DocIdSetIterator.NO_MORE_DOCS && dvPos0 < doc) {
            dvPos0 = dv0.advance(doc);
          }
          if (dvPos0 == doc) { key0 = dv0.nextValue(); dvPos0 = dv0.nextDoc(); }
          if (dvPos1 != DocIdSetIterator.NO_MORE_DOCS && dvPos1 < doc) {
            dvPos1 = dv1.advance(doc);
          }
          if (dvPos1 == doc) { key1 = dv1.nextValue(); dvPos1 = dv1.nextDoc(); }
          int slot = flatMap.findOrInsert(key0, key1);
          int base = slot * slotsPerGroup;
          for (int i = 0; i < numAggs; i++) {
            int off = base + accOffset[i];
            if (isCountStar[i]) { flatMap.accData[off]++; continue; }
            if (aggDvPos[i] != DocIdSetIterator.NO_MORE_DOCS && aggDvPos[i] < doc) {
              aggDvPos[i] = numericAggDvs[i].advance(doc);
            }
            if (aggDvPos[i] == doc) {
              long rawVal = numericAggDvs[i].nextValue();
              aggDvPos[i] = numericAggDvs[i].nextDoc();
              switch (accType[i]) {
                case 0: flatMap.accData[off]++; break;
                case 1: flatMap.accData[off] += rawVal; break;
                case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
              }
            }
          }
        }
      } else if (useBitsetLockstep) {
        // Collect matching doc IDs into bitset
        FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
        DocIdSetIterator disi = scorer.iterator();
        for (int d = disi.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = disi.nextDoc()) {
          matchingDocs.set(d);
        }

        if (hasDvDuplicates) {
          final SortedNumericDocValues[] uniqueDvReaders =
              new SortedNumericDocValues[numUniqueDvs];
          boolean allDense = true;
          for (int i = 0; i < numUniqueDvs; i++) {
            uniqueDvReaders[i] = reader.getSortedNumericDocValues(uniqueDvColumns[i]);
            if (uniqueDvReaders[i] == null) allDense = false;
          }
          final long[] dvValues = new long[numUniqueDvs];
          final boolean[] dvHasValue = new boolean[numUniqueDvs];

          if (allDense) {
            // nextDoc lockstep with bitset check
            int keyDoc0 = dv0.nextDoc();
            int keyDoc1 = dv1.nextDoc();
            int[] uniqueDvDocs = new int[numUniqueDvs];
            for (int i = 0; i < numUniqueDvs; i++) {
              uniqueDvDocs[i] = uniqueDvReaders[i].nextDoc();
            }
            for (int doc = matchingDocs.nextSetBit(0); doc >= 0;
                doc = matchingDocs.nextSetBit(doc + 1)) {
              // Advance key iterators to current doc
              while (keyDoc0 != DocIdSetIterator.NO_MORE_DOCS && keyDoc0 < doc) { keyDoc0 = dv0.nextDoc(); }
              long key0 = 0;
              if (keyDoc0 == doc) { key0 = dv0.nextValue(); keyDoc0 = dv0.nextDoc(); }
              while (keyDoc1 != DocIdSetIterator.NO_MORE_DOCS && keyDoc1 < doc) { keyDoc1 = dv1.nextDoc(); }
              long key1 = 0;
              if (keyDoc1 == doc) { key1 = dv1.nextValue(); keyDoc1 = dv1.nextDoc(); }
              if (numBuckets > 1) {
                int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
                if (docBucket != bucket) continue;
              }
              int slot = flatMap.findOrInsert(key0, key1);
              int base = slot * slotsPerGroup;
              for (int d2 = 0; d2 < numUniqueDvs; d2++) {
                while (uniqueDvDocs[d2] != DocIdSetIterator.NO_MORE_DOCS && uniqueDvDocs[d2] < doc) {
                  uniqueDvDocs[d2] = uniqueDvReaders[d2].nextDoc();
                }
                if (uniqueDvDocs[d2] == doc) {
                  dvValues[d2] = uniqueDvReaders[d2].nextValue();
                  dvHasValue[d2] = true;
                  uniqueDvDocs[d2] = uniqueDvReaders[d2].nextDoc();
                } else {
                  dvHasValue[d2] = false;
                }
              }
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) { flatMap.accData[off]++; continue; }
                int dvIdx = aggToDvIdx[i];
                if (dvHasValue[dvIdx]) {
                  long rawVal = dvValues[dvIdx];
                  switch (accType[i]) {
                    case 0: flatMap.accData[off]++; break;
                    case 1: flatMap.accData[off] += rawVal; break;
                    case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                  }
                }
              }
            }
          } else {
            for (int doc = matchingDocs.nextSetBit(0); doc >= 0;
                doc = matchingDocs.nextSetBit(doc + 1)) {
              collectFlatTwoKeyDocDedup(
                  doc, dv0, dv1, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, uniqueDvReaders, dvValues, dvHasValue,
                  aggToDvIdx, bucket, numBuckets);
            }
          }
        } else {
          boolean allDense = true;
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i] && numericAggDvs[i] == null) { allDense = false; break; }
          }
          if (allDense) {
            int keyDoc0 = dv0.nextDoc();
            int keyDoc1 = dv1.nextDoc();
            int[] aggDocs = new int[numAggs];
            for (int i = 0; i < numAggs; i++) {
              aggDocs[i] = isCountStar[i] ? DocIdSetIterator.NO_MORE_DOCS : numericAggDvs[i].nextDoc();
            }
            for (int doc = matchingDocs.nextSetBit(0); doc >= 0;
                doc = matchingDocs.nextSetBit(doc + 1)) {
              while (keyDoc0 != DocIdSetIterator.NO_MORE_DOCS && keyDoc0 < doc) { keyDoc0 = dv0.nextDoc(); }
              long key0 = 0;
              if (keyDoc0 == doc) { key0 = dv0.nextValue(); keyDoc0 = dv0.nextDoc(); }
              while (keyDoc1 != DocIdSetIterator.NO_MORE_DOCS && keyDoc1 < doc) { keyDoc1 = dv1.nextDoc(); }
              long key1 = 0;
              if (keyDoc1 == doc) { key1 = dv1.nextValue(); keyDoc1 = dv1.nextDoc(); }
              if (numBuckets > 1) {
                int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
                if (docBucket != bucket) continue;
              }
              int slot = flatMap.findOrInsert(key0, key1);
              int base = slot * slotsPerGroup;
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) { flatMap.accData[off]++; continue; }
                while (aggDocs[i] != DocIdSetIterator.NO_MORE_DOCS && aggDocs[i] < doc) {
                  aggDocs[i] = numericAggDvs[i].nextDoc();
                }
                if (aggDocs[i] == doc) {
                  long rawVal = numericAggDvs[i].nextValue();
                  aggDocs[i] = numericAggDvs[i].nextDoc();
                  switch (accType[i]) {
                    case 0: flatMap.accData[off]++; break;
                    case 1: flatMap.accData[off] += rawVal; break;
                    case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                  }
                }
              }
            }
          } else {
            for (int doc = matchingDocs.nextSetBit(0); doc >= 0;
                doc = matchingDocs.nextSetBit(doc + 1)) {
              collectFlatTwoKeyDoc(
                  doc, dv0, dv1, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, bucket, numBuckets);
            }
          }
        }
      } else {
        // Broad filter or can't estimate: use original Scorer-based iteration
        DocIdSetIterator docIt = scorer.iterator();
        int doc;
        if (hasDvDuplicates) {
          final SortedNumericDocValues[] uniqueDvReaders =
              new SortedNumericDocValues[numUniqueDvs];
          for (int i = 0; i < numUniqueDvs; i++) {
            uniqueDvReaders[i] = reader.getSortedNumericDocValues(uniqueDvColumns[i]);
          }
          final long[] dvValues = new long[numUniqueDvs];
          final boolean[] dvHasValue = new boolean[numUniqueDvs];

          while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            collectFlatTwoKeyDocDedup(
                doc, dv0, dv1, flatMap, slotsPerGroup, numAggs, isCountStar,
                accType, accOffset, uniqueDvReaders, dvValues, dvHasValue,
                aggToDvIdx, bucket, numBuckets);
          }
        } else {
          while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            collectFlatTwoKeyDoc(
                doc, dv0, dv1, flatMap, slotsPerGroup, numAggs, isCountStar,
                accType, accOffset, numericAggDvs, bucket, numBuckets);
          }
        }
      }
    }
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
      SortedNumericDocValues[] numericAggDvs,
      int bucket,
      int numBuckets)
      throws IOException {
    long key0 = 0, key1 = 0;
    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
    if (dv1 != null && dv1.advanceExact(doc)) key1 = dv1.nextValue();
    // Hash-partition filter: skip docs not in this bucket
    if (numBuckets > 1) {
      int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
      if (docBucket != bucket) return;
    }
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
   * Process a single document with deduplicated DV reads. When multiple aggregates reference the
   * same column (e.g., SUM(col) + COUNT(col) from AVG decomposition), reads DocValues once and
   * caches the value for all referencing aggregates. Saves one DV advanceExact per duplicate column
   * per document.
   */
  private static void collectFlatTwoKeyDocDedup(
      int doc,
      SortedNumericDocValues dv0,
      SortedNumericDocValues dv1,
      FlatTwoKeyMap flatMap,
      int slotsPerGroup,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      SortedNumericDocValues[] uniqueDvReaders,
      long[] dvValues,
      boolean[] dvHasValue,
      int[] aggToDvIdx,
      int bucket,
      int numBuckets)
      throws IOException {
    long key0 = 0, key1 = 0;
    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
    if (dv1 != null && dv1.advanceExact(doc)) key1 = dv1.nextValue();
    // Hash-partition filter: skip docs not in this bucket
    if (numBuckets > 1) {
      int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
      if (docBucket != bucket) return;
    }
    int slot = flatMap.findOrInsert(key0, key1);
    int base = slot * slotsPerGroup;

    // Read each unique DV column once
    for (int d = 0; d < uniqueDvReaders.length; d++) {
      SortedNumericDocValues dv = uniqueDvReaders[d];
      if (dv != null && dv.advanceExact(doc)) {
        dvValues[d] = dv.nextValue();
        dvHasValue[d] = true;
      } else {
        dvHasValue[d] = false;
      }
    }

    // Apply cached values to all aggregates
    for (int i = 0; i < numAggs; i++) {
      int off = base + accOffset[i];
      if (isCountStar[i]) {
        flatMap.accData[off]++;
        continue;
      }
      int dvIdx = aggToDvIdx[i];
      if (dvHasValue[dvIdx]) {
        long rawVal = dvValues[dvIdx];
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
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    EvalNode evalNode = (aggNode.getChild() instanceof EvalNode en) ? en : null;
    TableScanNode scanNode = findChildTableScan(aggNode);

    // Identify eval keys and their source columns
    final int numKeys = keyInfos.size();
    final int numAggs = specs.size();
    final boolean[] isEvalKey = new boolean[numKeys];

    // First pass: collect ALL eval source columns for expression compilation
    Set<String> allEvalSourceColumns = new HashSet<>();
    if (evalNode != null) {
      for (int k = 0; k < numKeys; k++) {
        KeyInfo ki = keyInfos.get(k);
        if ("eval".equals(ki.exprFunc)) {
          isEvalKey[k] = true;
          int exprIdx = Integer.parseInt(ki.exprUnit);
          String exprStr = evalNode.getExpressions().get(exprIdx);
          for (String col : columnTypeMap.keySet()) {
            if (exprStr.contains(col)) {
              allEvalSourceColumns.add(col);
            }
          }
        }
      }
    }

    // Build column index map for expression compilation using ALL eval source columns
    List<String> allEvalColumns = new ArrayList<>(allEvalSourceColumns);
    java.util.Collections.sort(allEvalColumns); // Deterministic order
    Map<String, Integer> allColIndexMap = new HashMap<>();
    for (int i = 0; i < allEvalColumns.size(); i++) {
      allColIndexMap.put(allEvalColumns.get(i), i);
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

    // Compile eval expressions and optimize column references
    org.opensearch.sql.dqe.function.FunctionRegistry registry =
        org.opensearch.sql.dqe.function.BuiltinFunctions.createRegistry();
    io.trino.sql.parser.SqlParser sqlParser = new io.trino.sql.parser.SqlParser();
    org.opensearch.sql.dqe.function.expression.ExpressionCompiler compiler =
        new org.opensearch.sql.dqe.function.expression.ExpressionCompiler(
            registry, allColIndexMap, normalizedTypeMap);

    final org.opensearch.sql.dqe.function.expression.BlockExpression[] evalExprs =
        new org.opensearch.sql.dqe.function.expression.BlockExpression[numKeys];
    for (int k = 0; k < numKeys; k++) {
      if (isEvalKey[k]) {
        int exprIdx = Integer.parseInt(keyInfos.get(k).exprUnit);
        String exprStr = evalNode.getExpressions().get(exprIdx);
        io.trino.sql.tree.Expression ast = sqlParser.createExpression(exprStr);
        evalExprs[k] = compiler.compile(ast);
        // Optimization: if the eval expression is a simple column reference (e.g., URL AS Dst),
        // convert it to a regular (non-eval) key to avoid Block-based expression evaluation.
        // This uses direct DocValues reading which is much faster for VARCHAR columns.
        if (evalExprs[k]
            instanceof org.opensearch.sql.dqe.function.expression.ColumnReference colRef) {
          String colName = allEvalColumns.get(colRef.getColumnIndex());
          Type colType = columnTypeMap.getOrDefault(colName, BigintType.BIGINT);
          boolean colIsVarchar = colType instanceof VarcharType;
          keyInfos.set(k, new KeyInfo(colName, colType, colIsVarchar, null, null));
          isEvalKey[k] = false;
          evalExprs[k] = null;
        }
      }
    }

    // Second pass: rebuild eval source columns excluding columns only needed by
    // now-converted column references. This avoids building expensive Block objects
    // for columns that are now read directly via DocValues.
    Set<String> evalSourceColumns = new HashSet<>();
    if (evalNode != null) {
      for (int k = 0; k < numKeys; k++) {
        if (isEvalKey[k]) {
          int exprIdx = Integer.parseInt(keyInfos.get(k).exprUnit);
          String exprStr = evalNode.getExpressions().get(exprIdx);
          for (String col : columnTypeMap.keySet()) {
            if (exprStr.contains(col)) {
              evalSourceColumns.add(col);
            }
          }
        }
      }
    }
    List<String> evalColumns = new ArrayList<>(evalSourceColumns);
    java.util.Collections.sort(evalColumns);
    // Build a mapping from allEvalColumns indices (used by compiled expressions)
    // to the reduced evalColumns indices (used for actual Block building)
    Map<String, Integer> reducedColIndexMap = new HashMap<>();
    for (int i = 0; i < evalColumns.size(); i++) {
      reducedColIndexMap.put(evalColumns.get(i), i);
    }

    // Normalized types for the eval columns
    Type[] evalColTypes = new Type[evalColumns.size()];
    for (int c = 0; c < evalColumns.size(); c++) {
      Type t = normalizedTypeMap.getOrDefault(evalColumns.get(c), BigintType.BIGINT);
      evalColTypes[c] = t;
    }

    // Re-compile remaining eval expressions with reduced column set so column indices match
    // the reduced evalColumns used for Block building
    if (!reducedColIndexMap.equals(allColIndexMap)) {
      org.opensearch.sql.dqe.function.expression.ExpressionCompiler reducedCompiler =
          new org.opensearch.sql.dqe.function.expression.ExpressionCompiler(
              registry, reducedColIndexMap, normalizedTypeMap);
      for (int k = 0; k < numKeys; k++) {
        if (isEvalKey[k]) {
          int exprIdx = Integer.parseInt(keyInfos.get(k).exprUnit);
          String exprStr = evalNode.getExpressions().get(exprIdx);
          io.trino.sql.tree.Expression ast = sqlParser.createExpression(exprStr);
          evalExprs[k] = reducedCompiler.compile(ast);
        }
      }
    }

    // Detect simple CASE WHEN patterns for inline evaluation (bypasses Block materialization).
    // Pattern: CASE WHEN (col1 = const1 AND col2 = const2 ...) THEN varchar_col ELSE '' END
    // where condition columns are non-eval GROUP BY keys (already read from DocValues).
    final boolean[] inlineEvalKey = new boolean[numKeys];
    // For inline eval: maps condition column refs to their GROUP BY key index
    final int[][] inlineCondKeyIndices = new int[numKeys][];
    final long[][] inlineCondValues = new long[numKeys][];
    final String[] inlineResultColumn = new String[numKeys]; // Referer column name
    final boolean[] inlineResultIsVarchar = new boolean[numKeys];
    final BytesRefKey[] inlineElseValue = new BytesRefKey[numKeys];

    for (int k = 0; k < numKeys; k++) {
      if (!isEvalKey[k] || evalExprs[k] == null) continue;
      if (!(evalExprs[k]
          instanceof org.opensearch.sql.dqe.function.expression.CaseBlockExpression caseExpr))
        continue;
      if (caseExpr.getWhenConditions().size() != 1) continue;
      if (!(keyInfos.get(k).type instanceof VarcharType)) continue;

      // Check ELSE is a constant empty string
      if (!(caseExpr.getElseResult()
          instanceof org.opensearch.sql.dqe.function.expression.ConstantExpression elseConst))
        continue;
      String elseVal = elseConst.getValue() != null ? elseConst.getValue().toString() : "";

      // Check THEN is a column reference to a VARCHAR eval source column
      if (!(caseExpr.getThenResults().get(0)
          instanceof org.opensearch.sql.dqe.function.expression.ColumnReference thenColRef))
        continue;
      int thenColIdx = thenColRef.getColumnIndex();
      if (thenColIdx >= evalColumns.size()) continue;
      String thenColName = evalColumns.get(thenColIdx);
      if (!(thenColRef.getType() instanceof VarcharType)) continue;

      // Check condition: extract equality comparisons on non-eval key columns
      org.opensearch.sql.dqe.function.expression.BlockExpression cond =
          caseExpr.getWhenConditions().get(0);
      java.util.List<org.opensearch.sql.dqe.function.expression.ComparisonBlockExpression> comps =
          new ArrayList<>();
      if (!extractEqualityComparisons(cond, comps)) continue;

      // Verify all comparisons are: ColumnReference(eval_col) == ConstantExpression(long)
      // AND the eval_col maps to a non-eval GROUP BY key
      int[] condKeyIdx = new int[comps.size()];
      long[] condVals = new long[comps.size()];
      boolean allMatch = true;
      for (int c = 0; c < comps.size(); c++) {
        var comp = comps.get(c);
        if (comp.getOperator() != io.trino.sql.tree.ComparisonExpression.Operator.EQUAL) {
          allMatch = false;
          break;
        }
        // Find column ref and constant in either order
        org.opensearch.sql.dqe.function.expression.ColumnReference colRef = null;
        org.opensearch.sql.dqe.function.expression.ConstantExpression constExpr = null;
        if (comp.getLeft() instanceof org.opensearch.sql.dqe.function.expression.ColumnReference cr
            && comp.getRight()
                instanceof org.opensearch.sql.dqe.function.expression.ConstantExpression ce) {
          colRef = cr;
          constExpr = ce;
        } else if (comp.getRight()
                instanceof org.opensearch.sql.dqe.function.expression.ColumnReference cr2
            && comp.getLeft()
                instanceof org.opensearch.sql.dqe.function.expression.ConstantExpression ce2) {
          colRef = cr2;
          constExpr = ce2;
        }
        if (colRef == null || constExpr == null || !(constExpr.getValue() instanceof Number)) {
          allMatch = false;
          break;
        }
        // Map eval column index to GROUP BY key index
        String evalColName = evalColumns.get(colRef.getColumnIndex());
        int keyIdx = -1;
        for (int kk = 0; kk < numKeys; kk++) {
          if (!isEvalKey[kk] && keyInfos.get(kk).name.equals(evalColName)) {
            keyIdx = kk;
            break;
          }
        }
        if (keyIdx < 0) {
          allMatch = false;
          break;
        }
        condKeyIdx[c] = keyIdx;
        condVals[c] = ((Number) constExpr.getValue()).longValue();
      }
      if (!allMatch) continue;

      // Pattern matched! Enable inline evaluation for this key
      inlineEvalKey[k] = true;
      isEvalKey[k] = false; // Remove from Block-based eval
      inlineCondKeyIndices[k] = condKeyIdx;
      inlineCondValues[k] = condVals;
      inlineResultColumn[k] = thenColName;
      inlineResultIsVarchar[k] = true;
      inlineElseValue[k] =
          elseVal.isEmpty()
              ? new BytesRefKey(new BytesRef(""))
              : new BytesRefKey(new BytesRef(elseVal));
      // Inline CASE WHEN detected
    }

    // Rebuild eval columns after inline CASE WHEN detection removed some eval keys.
    // This ensures we don't build expensive Blocks for columns only used by inlined expressions.
    boolean hasAnyInline = false;
    for (int k = 0; k < numKeys; k++) {
      if (inlineEvalKey[k]) {
        hasAnyInline = true;
        break;
      }
    }
    if (hasAnyInline) {
      Set<String> remainingEvalSourceCols = new HashSet<>();
      if (evalNode != null) {
        for (int k = 0; k < numKeys; k++) {
          if (isEvalKey[k]) { // only still-eval keys
            int exprIdx = Integer.parseInt(keyInfos.get(k).exprUnit);
            String exprStr = evalNode.getExpressions().get(exprIdx);
            for (String col : columnTypeMap.keySet()) {
              if (exprStr.contains(col)) {
                remainingEvalSourceCols.add(col);
              }
            }
          }
        }
      }
      evalColumns = new ArrayList<>(remainingEvalSourceCols);
      java.util.Collections.sort(evalColumns);
      reducedColIndexMap = new HashMap<>();
      for (int i = 0; i < evalColumns.size(); i++) {
        reducedColIndexMap.put(evalColumns.get(i), i);
      }
      evalColTypes = new Type[evalColumns.size()];
      for (int c = 0; c < evalColumns.size(); c++) {
        Type t = normalizedTypeMap.getOrDefault(evalColumns.get(c), BigintType.BIGINT);
        evalColTypes[c] = t;
      }
      // Re-compile remaining eval expressions with the updated column set
      if (!reducedColIndexMap.isEmpty()) {
        org.opensearch.sql.dqe.function.expression.ExpressionCompiler inlineReducedCompiler =
            new org.opensearch.sql.dqe.function.expression.ExpressionCompiler(
                registry, reducedColIndexMap, normalizedTypeMap);
        for (int k = 0; k < numKeys; k++) {
          if (isEvalKey[k]) {
            int exprIdx = Integer.parseInt(keyInfos.get(k).exprUnit);
            String exprStr = evalNode.getExpressions().get(exprIdx);
            io.trino.sql.tree.Expression ast = sqlParser.createExpression(exprStr);
            evalExprs[k] = inlineReducedCompiler.compile(ast);
          }
        }
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

    // Pre-compute which non-eval keys are varchar for fast dispatch in grouping loop
    final boolean[] keyIsVarchar = new boolean[numKeys];
    for (int k = 0; k < numKeys; k++) {
      if (!isEvalKey[k] && !inlineEvalKey[k]) keyIsVarchar[k] = keyInfos.get(k).isVarchar;
    }

    // Shared empty BytesRefKey — avoids per-row allocation for empty-string CASE WHEN results
    final BytesRefKey emptyBytesRefKey = new BytesRefKey(new BytesRef(""));

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-eval")) {

      // Detect single-segment, all-long-keys case for flat open-addressing map
      // (avoids HashMap Entry/MergedGroupKey/Object[] allocation entirely)
      boolean useFlatLongMap = false;
      // Will be initialized if useFlatLongMap is true
      int flatCap = 0;
      long[] flatKeys = null; // flatKeys[slot * numKeys + k] = key k at slot
      AccumulatorGroup[] flatValues = null;
      int flatSize = 0;
      int flatThreshold = 0;
      long[] probeLongKeys = new long[numKeys]; // reusable probe buffer

      Map<Object, AccumulatorGroup> globalGroups = new LinkedHashMap<>(16384, 0.75f);
      ProbeGroupKey probeKey = new ProbeGroupKey(numKeys);

      // For single-segment ordinal resolution at output time
      SortedSetDocValues[] savedVarcharDvs = null;
      SortedSetDocValues[] savedInlineResultDvs = null;

      // Pre-compute Weight once before the segment loop (rewrite + createWeight is expensive)
      final org.apache.lucene.search.Weight cachedWeight =
          engineSearcher.createWeight(
              engineSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

      // Build global ordinal maps for multi-segment VARCHAR keys
      // This converts segment ordinals to global ordinals (longs), enabling flat long map
      List<LeafReaderContext> allLeaves = engineSearcher.getIndexReader().leaves();
      boolean multiSegment = allLeaves.size() > 1;
      OrdinalMap[] globalOrdMaps = null;
      boolean useGlobalOrds = false;
      SortedSetDocValues[][] segDvsForLookup = null;
      if (multiSegment) {
        globalOrdMaps = new OrdinalMap[numKeys];
        useGlobalOrds = true;
        for (int k = 0; k < numKeys; k++) {
          if (keyIsVarchar[k] || (inlineEvalKey[k] && inlineResultIsVarchar[k])) {
            String fieldName = keyIsVarchar[k] ? keyInfos.get(k).name : inlineResultColumn[k];
            globalOrdMaps[k] = buildGlobalOrdinalMap(allLeaves, fieldName);
            if (globalOrdMaps[k] == null) {
              useGlobalOrds = false;
              break;
            }
          }
        }
        if (useGlobalOrds) {
          // Pre-build per-segment DVs for lookupGlobalOrd in result-building phase
          segDvsForLookup = new SortedSetDocValues[numKeys][];
          for (int k = 0; k < numKeys; k++) {
            if (globalOrdMaps[k] != null) {
              String fieldName = keyIsVarchar[k] ? keyInfos.get(k).name : inlineResultColumn[k];
              segDvsForLookup[k] = new SortedSetDocValues[allLeaves.size()];
              for (int s = 0; s < allLeaves.size(); s++) {
                segDvsForLookup[k][s] = allLeaves.get(s).reader().getSortedSetDocValues(fieldName);
              }
            }
          }
        }
      }
      // Per-segment global ordinal mappings (populated inside segment loop)
      LongValues[] curSegGlobalMaps = useGlobalOrds ? new LongValues[numKeys] : null;

      // Phase 1: Collect doc IDs per segment
      for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
        LeafReader reader = leafCtx.reader();

        // Collect matching doc IDs into primitive int array (no boxing overhead)
        org.apache.lucene.search.Scorer scorer = cachedWeight.scorer(leafCtx);
        if (scorer == null) continue;

        int[] segDocIds = new int[256];
        int segDocCount = 0;
        DocIdSetIterator disi = scorer.iterator();
        int doc;
        while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          if (segDocCount == segDocIds.length) {
            segDocIds = java.util.Arrays.copyOf(segDocIds, segDocIds.length * 2);
          }
          segDocIds[segDocCount++] = doc;
        }
        if (segDocCount == 0) continue;

        // Track segment index for global ordinal mapping
        if (useGlobalOrds) {
          int segIdx = allLeaves.indexOf(leafCtx);
          for (int k = 0; k < numKeys; k++) {
            if (globalOrdMaps[k] != null) {
              curSegGlobalMaps[k] = globalOrdMaps[k].getGlobalOrds(segIdx);
            }
          }
        }

        // Treat as single-segment when global ordinals are available (all keys become longs)
        boolean singleSegment = (engineSearcher.getIndexReader().leaves().size() == 1) || useGlobalOrds;

        // Open non-eval key DocValues for inline reading during grouping phase
        SortedSetDocValues[] varcharKeyDvs = new SortedSetDocValues[numKeys];
        SortedNumericDocValues[] numericKeyDvs = new SortedNumericDocValues[numKeys];
        for (int k = 0; k < numKeys; k++) {
          if (isEvalKey[k] || inlineEvalKey[k]) continue;
          KeyInfo ki = keyInfos.get(k);
          if (ki.isVarchar) varcharKeyDvs[k] = reader.getSortedSetDocValues(ki.name);
          else numericKeyDvs[k] = reader.getSortedNumericDocValues(ki.name);
        }
        if (singleSegment) {
          savedVarcharDvs = varcharKeyDvs;
          // Check if ALL keys resolve to long in single-segment mode
          // (non-eval varchar → ordinal long, inline-eval varchar → ordinal long, numeric → long)
          boolean allKeysLong = true;
          for (int k = 0; k < numKeys; k++) {
            if (isEvalKey[k]) {
              // eval keys that produce VARCHAR via Block expressions can't be long
              Type kt = keyInfos.get(k).type;
              if (kt instanceof VarcharType) {
                allKeysLong = false;
                break;
              }
            }
            // inline eval + non-eval varchar + numeric all resolve to long in single-segment
          }
          if (allKeysLong && !useFlatLongMap) {
            useFlatLongMap = true;
            flatCap = 32768; // initial capacity for ~20K entries at 0.65 load
            flatKeys = new long[flatCap * numKeys];
            java.util.Arrays.fill(flatKeys, Long.MIN_VALUE); // sentinel
            flatValues = new AccumulatorGroup[flatCap];
            flatThreshold = (int) (flatCap * 0.65f);
          }
        }

        // Open DocValues for inline CASE WHEN result columns
        SortedSetDocValues[] inlineResultDvs = new SortedSetDocValues[numKeys];
        for (int k = 0; k < numKeys; k++) {
          if (inlineEvalKey[k] && inlineResultIsVarchar[k]) {
            inlineResultDvs[k] = reader.getSortedSetDocValues(inlineResultColumn[k]);
          }
        }

        if (singleSegment) savedInlineResultDvs = inlineResultDvs;

        // Check if any eval keys remain after column-reference optimization
        boolean hasRemainingEvalKeys = false;
        for (int k = 0; k < numKeys; k++) {
          if (isEvalKey[k]) {
            hasRemainingEvalKeys = true;
            break;
          }
        }

        // Open eval column DocValues (forward-only, reused across micro-batches)
        SortedNumericDocValues[] evalNumDvs = new SortedNumericDocValues[evalColumns.size()];
        SortedSetDocValues[] evalVarDvs = new SortedSetDocValues[evalColumns.size()];
        for (int c = 0; c < evalColumns.size(); c++) {
          if (evalColTypes[c] instanceof VarcharType) {
            evalVarDvs[c] = reader.getSortedSetDocValues(evalColumns.get(c));
          } else {
            evalNumDvs[c] = reader.getSortedNumericDocValues(evalColumns.get(c));
          }
        }

        // Process in micro-batches to bound memory for large VARCHAR columns (e.g., Referer URLs).
        // Each batch builds small Blocks (256 rows) instead of one huge Block (51K+ rows).
        final int EVAL_BATCH_SIZE = 256;

        for (int batchStart = 0; batchStart < segDocCount; batchStart += EVAL_BATCH_SIZE) {
          int batchEnd = Math.min(batchStart + EVAL_BATCH_SIZE, segDocCount);
          int batchCount = batchEnd - batchStart;

          // Build eval column Blocks for this micro-batch only
          Block[] evalResultBlocks = null;
          if (hasRemainingEvalKeys) {
            evalResultBlocks = new Block[numKeys];
            Page evalPage;
            if (!evalColumns.isEmpty()) {
              BlockBuilder[] colBuilders = new BlockBuilder[evalColumns.size()];
              for (int c = 0; c < evalColumns.size(); c++) {
                colBuilders[c] = evalColTypes[c].createBlockBuilder(null, batchCount);
              }
              for (int d = 0; d < batchCount; d++) {
                int docId = segDocIds[batchStart + d];
                for (int c = 0; c < evalColumns.size(); c++) {
                  if (evalColTypes[c] instanceof VarcharType) {
                    SortedSetDocValues dv = evalVarDvs[c];
                    if (dv != null && dv.advanceExact(docId)) {
                      BytesRef bytes = dv.lookupOrd(dv.nextOrd());
                      VarcharType.VARCHAR.writeSlice(
                          colBuilders[c],
                          Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
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
              evalPage = new Page(colBlocks);
            } else {
              BlockBuilder dummyBuilder = BigintType.BIGINT.createBlockBuilder(null, batchCount);
              for (int d = 0; d < batchCount; d++) {
                BigintType.BIGINT.writeLong(dummyBuilder, 0);
              }
              evalPage = new Page(dummyBuilder.build());
            }
            for (int k = 0; k < numKeys; k++) {
              if (isEvalKey[k]) {
                evalResultBlocks[k] = evalExprs[k].evaluate(evalPage);
              }
            }
          }

          // Group and aggregate for this micro-batch
          for (int d = 0; d < batchCount; d++) {
            int docId = segDocIds[batchStart + d];
            Object[] resolvedKeys = probeKey.values;
            // First pass: resolve non-eval and block-eval keys
            for (int k = 0; k < numKeys; k++) {
              if (inlineEvalKey[k]) continue; // resolved in second pass
              if (isEvalKey[k]) {
                Block block = evalResultBlocks[k];
                if (block.isNull(d)) {
                  resolvedKeys[k] = null;
                } else {
                  Type keyType = keyInfos.get(k).type;
                  if (keyType instanceof VarcharType) {
                    io.airlift.slice.Slice slice = VarcharType.VARCHAR.getSlice(block, d);
                    if (slice.length() == 0) {
                      resolvedKeys[k] = emptyBytesRefKey;
                    } else {
                      resolvedKeys[k] =
                          new BytesRefKey(new BytesRef(slice.getBytes(), 0, slice.length()));
                    }
                  } else if (keyType instanceof DoubleType) {
                    resolvedKeys[k] =
                        Double.doubleToLongBits(DoubleType.DOUBLE.getDouble(block, d));
                  } else {
                    resolvedKeys[k] = BigintType.BIGINT.getLong(block, d);
                  }
                }
              } else if (keyIsVarchar[k]) {
                SortedSetDocValues dv = varcharKeyDvs[k];
                if (dv != null && dv.advanceExact(docId)) {
                  if (singleSegment) {
                    long ord = dv.nextOrd();
                    resolvedKeys[k] = (useGlobalOrds && curSegGlobalMaps[k] != null) ? curSegGlobalMaps[k].get(ord) : ord;
                  } else {
                    BytesRef bytes = dv.lookupOrd(dv.nextOrd());
                    resolvedKeys[k] = new BytesRefKey(bytes);
                  }
                } else {
                  resolvedKeys[k] = singleSegment ? (Object) (-1L) : emptyBytesRefKey;
                }
              } else {
                SortedNumericDocValues dv = numericKeyDvs[k];
                if (dv != null && dv.advanceExact(docId)) {
                  long val = dv.nextValue();
                  if (truncUnits[k] != null) val = truncateMillis(val, truncUnits[k]);
                  else if (arithUnits[k] != null) val = applyArith(val, arithUnits[k]);
                  resolvedKeys[k] = val;
                } else {
                  resolvedKeys[k] = null;
                }
              }
            }
            // Second pass: resolve inline CASE WHEN keys using already-resolved key values
            for (int k = 0; k < numKeys; k++) {
              if (!inlineEvalKey[k]) continue;
              // Check condition: all referenced keys must equal their expected values
              boolean condTrue = true;
              int[] condIdx = inlineCondKeyIndices[k];
              long[] condVals = inlineCondValues[k];
              for (int c = 0; c < condIdx.length; c++) {
                Object keyVal = resolvedKeys[condIdx[c]];
                if (!(keyVal instanceof Long) || (Long) keyVal != condVals[c]) {
                  condTrue = false;
                  break;
                }
              }
              if (condTrue) {
                // Condition true: read result column from DocValues
                SortedSetDocValues dv = inlineResultDvs[k];
                if (dv != null && dv.advanceExact(docId)) {
                  long ord = dv.nextOrd();
                  if (singleSegment) {
                    resolvedKeys[k] = (useGlobalOrds && curSegGlobalMaps[k] != null) ? curSegGlobalMaps[k].get(ord) : ord;
                  } else {
                    BytesRef bytes = dv.lookupOrd(ord);
                    resolvedKeys[k] = new BytesRefKey(bytes);
                  }
                } else {
                  if (singleSegment) {
                    resolvedKeys[k] = -1L; // sentinel ordinal for missing/empty
                  } else {
                    resolvedKeys[k] = inlineElseValue[k];
                  }
                }
              } else {
                // Condition false: use else value (no DocValues read needed)
                if (singleSegment) {
                  resolvedKeys[k] = -2L; // sentinel ordinal for else branch
                } else {
                  resolvedKeys[k] = inlineElseValue[k];
                }
              }
            }

            AccumulatorGroup accGroup;
            if (useFlatLongMap) {
              // Flat open-addressing map path: zero allocation per row
              for (int k = 0; k < numKeys; k++) {
                Object v = resolvedKeys[k];
                probeLongKeys[k] = (v instanceof Long) ? (Long) v : 0L;
              }
              // Hash: combine all N keys
              long h = 0;
              for (int k = 0; k < numKeys; k++) {
                h = h * 0x9E3779B97F4A7C15L + probeLongKeys[k];
              }
              h ^= h >>> 33;
              h *= 0xff51afd7ed558ccdL;
              h ^= h >>> 33;
              int mask = flatCap - 1;
              int slot = (int) h & mask;
              while (true) {
                int base = slot * numKeys;
                if (flatValues[slot] == null) {
                  // Empty slot — insert new group
                  for (int k = 0; k < numKeys; k++) flatKeys[base + k] = probeLongKeys[k];
                  accGroup = createAccumulatorGroup(specs);
                  flatValues[slot] = accGroup;
                  flatSize++;
                  if (flatSize > flatThreshold) {
                    // Resize flat map
                    int newCap = flatCap * 2;
                    long[] nk = new long[newCap * numKeys];
                    java.util.Arrays.fill(nk, Long.MIN_VALUE);
                    AccumulatorGroup[] nv = new AccumulatorGroup[newCap];
                    int nm = newCap - 1;
                    for (int s = 0; s < flatCap; s++) {
                      if (flatValues[s] != null) {
                        long rh = 0;
                        for (int kk = 0; kk < numKeys; kk++) {
                          rh = rh * 0x9E3779B97F4A7C15L + flatKeys[s * numKeys + kk];
                        }
                        rh ^= rh >>> 33;
                        rh *= 0xff51afd7ed558ccdL;
                        rh ^= rh >>> 33;
                        int ns = (int) rh & nm;
                        while (nv[ns] != null) ns = (ns + 1) & nm;
                        int nb = ns * numKeys;
                        for (int kk = 0; kk < numKeys; kk++) {
                          nk[nb + kk] = flatKeys[s * numKeys + kk];
                        }
                        nv[ns] = flatValues[s];
                      }
                    }
                    flatKeys = nk;
                    flatValues = nv;
                    flatCap = newCap;
                    flatThreshold = (int) (newCap * 0.65f);
                    mask = flatCap - 1;
                  }
                  break;
                }
                // Check if this slot matches
                boolean match = true;
                for (int k = 0; k < numKeys; k++) {
                  if (flatKeys[base + k] != probeLongKeys[k]) {
                    match = false;
                    break;
                  }
                }
                if (match) {
                  accGroup = flatValues[slot];
                  break;
                }
                slot = (slot + 1) & mask;
              }
            } else {
              // Generic HashMap path
              probeKey.computeHash();
              accGroup = globalGroups.get(probeKey);
              if (accGroup == null) {
                MergedGroupKey mgk = probeKey.snapshot();
                accGroup = createAccumulatorGroup(specs);
                globalGroups.put(mgk, accGroup);
              }
            }

            for (int i = 0; i < numAggs; i++) {
              if (isCountStar[i]) {
                ((CountStarAccum) accGroup.accumulators[i]).count++;
              }
            }
          }
        }
      }

      // If using flat long map, build output directly from flat arrays
      if (useFlatLongMap && flatSize > 0) {
        int numGroupKeys = groupByKeys.size();
        int totalColumns = numGroupKeys + numAggs;

        // Select top-N from flat map using heap
        int effectiveTopN = (topN > 0) ? (int) Math.min(topN, flatSize) : flatSize;
        int[] topSlots;
        if (sortAggIndex >= 0 && topN > 0 && effectiveTopN < flatSize) {
          int n = effectiveTopN;
          int[] heap = new int[n];
          int heapSize = 0;
          for (int s = 0; s < flatCap; s++) {
            if (flatValues[s] == null) continue;
            long val = flatValues[s].accumulators[sortAggIndex].getSortValue();
            if (heapSize < n) {
              heap[heapSize++] = s;
              int ki = heapSize - 1;
              while (ki > 0) {
                int parent = (ki - 1) >>> 1;
                long pVal = flatValues[heap[parent]].accumulators[sortAggIndex].getSortValue();
                long kVal2 = flatValues[heap[ki]].accumulators[sortAggIndex].getSortValue();
                boolean swap = sortAscending ? (kVal2 > pVal) : (kVal2 < pVal);
                if (swap) {
                  int tmp = heap[parent];
                  heap[parent] = heap[ki];
                  heap[ki] = tmp;
                  ki = parent;
                } else break;
              }
            } else {
              long rootVal = flatValues[heap[0]].accumulators[sortAggIndex].getSortValue();
              boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
              if (better) {
                heap[0] = s;
                int ki = 0;
                while (true) {
                  int left = 2 * ki + 1;
                  if (left >= heapSize) break;
                  int right = left + 1;
                  int target = left;
                  if (right < heapSize) {
                    long lv = flatValues[heap[left]].accumulators[sortAggIndex].getSortValue();
                    long rv = flatValues[heap[right]].accumulators[sortAggIndex].getSortValue();
                    if (sortAscending ? (rv > lv) : (rv < lv)) target = right;
                  }
                  long kVal2 = flatValues[heap[ki]].accumulators[sortAggIndex].getSortValue();
                  long tVal = flatValues[heap[target]].accumulators[sortAggIndex].getSortValue();
                  if (sortAscending ? (tVal > kVal2) : (tVal < kVal2)) {
                    int tmp = heap[ki];
                    heap[ki] = heap[target];
                    heap[target] = tmp;
                    ki = target;
                  } else break;
                }
              }
            }
          }
          topSlots = java.util.Arrays.copyOf(heap, heapSize);
        } else {
          // No sort or all groups: collect all occupied slots
          topSlots = new int[flatSize];
          int idx = 0;
          for (int s = 0; s < flatCap; s++) {
            if (flatValues[s] != null) topSlots[idx++] = s;
          }
        }

        // Build output page
        BlockBuilder[] builders = new BlockBuilder[totalColumns];
        for (int i = 0; i < numGroupKeys; i++) {
          builders[i] = keyInfos.get(i).type.createBlockBuilder(null, topSlots.length);
        }
        for (int i = 0; i < numAggs; i++) {
          builders[numGroupKeys + i] =
              resolveAggOutputType(specs.get(i), columnTypeMap)
                  .createBlockBuilder(null, topSlots.length);
        }

        for (int s : topSlots) {
          int base = s * numKeys;
          for (int k = 0; k < numGroupKeys; k++) {
            long val = flatKeys[base + k];
            // Resolve ordinals to bytes for VARCHAR keys
            if (keyIsVarchar[k] && !isEvalKey[k] && !inlineEvalKey[k]) {
              if (val >= 0) {
                try {
                  BytesRef bytes;
                  if (useGlobalOrds && globalOrdMaps[k] != null) {
                    bytes = lookupGlobalOrd(globalOrdMaps[k], segDvsForLookup[k], val);
                  } else if (savedVarcharDvs != null && savedVarcharDvs[k] != null) {
                    bytes = savedVarcharDvs[k].lookupOrd(val);
                  } else {
                    bytes = new BytesRef("");
                  }
                  writeKeyValueForMerged(builders[k], keyInfos.get(k), new BytesRefKey(bytes));
                } catch (IOException e) {
                  writeKeyValueForMerged(builders[k], keyInfos.get(k), emptyBytesRefKey);
                }
              } else {
                writeKeyValueForMerged(builders[k], keyInfos.get(k), emptyBytesRefKey);
              }
            } else if (inlineEvalKey[k]
                && inlineResultIsVarchar[k]) {
              if (val == -1L) {
                writeKeyValueForMerged(builders[k], keyInfos.get(k), emptyBytesRefKey);
              } else if (val == -2L) {
                writeKeyValueForMerged(builders[k], keyInfos.get(k), inlineElseValue[k]);
              } else {
                try {
                  BytesRef bytes;
                  if (useGlobalOrds && globalOrdMaps[k] != null) {
                    bytes = lookupGlobalOrd(globalOrdMaps[k], segDvsForLookup[k], val);
                  } else if (savedInlineResultDvs != null && savedInlineResultDvs[k] != null) {
                    bytes = savedInlineResultDvs[k].lookupOrd(val);
                  } else {
                    bytes = new BytesRef("");
                  }
                  writeKeyValueForMerged(builders[k], keyInfos.get(k), new BytesRefKey(bytes));
                } catch (IOException e) {
                  writeKeyValueForMerged(builders[k], keyInfos.get(k), emptyBytesRefKey);
                }
              }
            } else {
              writeKeyValueForMerged(builders[k], keyInfos.get(k), val);
            }
          }
          AccumulatorGroup ag = flatValues[s];
          for (int a = 0; a < numAggs; a++) {
            ag.accumulators[a].writeTo(builders[numGroupKeys + a]);
          }
        }

        Block[] blocks = new Block[totalColumns];
        for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
        return List.of(new Page(blocks));
      }

      if (globalGroups.isEmpty()) {
        return List.of();
      }

      // Apply top-N selection if requested, to reduce output volume
      int numGroupKeys = groupByKeys.size();
      int totalColumns = numGroupKeys + numAggs;

      java.util.Collection<Map.Entry<Object, AccumulatorGroup>> outputEntries;
      if (sortAggIndex >= 0 && topN > 0 && topN < globalGroups.size()) {
        int n = (int) Math.min(topN, globalGroups.size());
        // Use a min-heap (for DESC) or max-heap (for ASC) to select top-N entries
        @SuppressWarnings("unchecked")
        Map.Entry<Object, AccumulatorGroup>[] heap = new Map.Entry[n];
        int heapSize = 0;
        for (Map.Entry<Object, AccumulatorGroup> entry : globalGroups.entrySet()) {
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
                boolean swp = sortAscending ? (tVal > kVal) : (tVal < kVal);
                if (swp) {
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
      } else if (topN > 0 && topN < globalGroups.size()) {
        // LIMIT without ORDER BY: just take the first topN entries
        List<Map.Entry<Object, AccumulatorGroup>> limited = new ArrayList<>();
        int count = 0;
        for (Map.Entry<Object, AccumulatorGroup> entry : globalGroups.entrySet()) {
          if (count++ >= topN) break;
          limited.add(entry);
        }
        outputEntries = limited;
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

      for (Map.Entry<Object, AccumulatorGroup> entry : outputEntries) {
        MergedGroupKey key = (MergedGroupKey) entry.getKey();
        AccumulatorGroup accGroup = entry.getValue();
        for (int k = 0; k < numGroupKeys; k++) {
          Object val = key.values[k];
          // Resolve ordinals to bytes for single-segment VARCHAR keys (non-eval)
          if (val instanceof Long
              && keyIsVarchar[k]
              && !isEvalKey[k]
              && !inlineEvalKey[k]
              && savedVarcharDvs != null) {
            long ord = (Long) val;
            if (ord >= 0 && savedVarcharDvs[k] != null) {
              try {
                BytesRef bytes = savedVarcharDvs[k].lookupOrd(ord);
                val = new BytesRefKey(bytes);
              } catch (IOException e) {
                val = emptyBytesRefKey;
              }
            } else {
              val = emptyBytesRefKey;
            }
          }
          // Resolve ordinals for single-segment inline eval VARCHAR keys
          if (val instanceof Long
              && inlineEvalKey[k]
              && inlineResultIsVarchar[k]
              && savedInlineResultDvs != null) {
            long ord = (Long) val;
            if (ord == -1L) {
              val = emptyBytesRefKey;
            } else if (ord == -2L) {
              val = inlineElseValue[k];
            } else if (savedInlineResultDvs[k] != null) {
              try {
                BytesRef bytes = savedInlineResultDvs[k].lookupOrd(ord);
                val = new BytesRefKey(bytes);
              } catch (IOException e) {
                val = emptyBytesRefKey;
              }
            } else {
              val = emptyBytesRefKey;
            }
          }
          writeKeyValueForMerged(builders[k], keyInfos.get(k), val);
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

        // Try the ordinal-indexed two-key COUNT(*) path first (extracted to keep this
        // method below the JVM's HugeMethodLimit of 8000 bytecodes for C2 JIT compilation).
        List<Page> ordinalResult =
            tryOrdinalIndexedTwoKeyCountStar(
                engineSearcher,
                query,
                leaves.get(0),
                keyInfos,
                numAggs,
                isCountStar,
                groupByKeys,
                sortAggIndex,
                sortAscending,
                topN);
        if (ordinalResult != null) {
          return ordinalResult;
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
              // When we have LIMIT without ORDER BY (sortAggIndex < 0 && topN > 0),
              // use capped insertion: once we have enough groups, skip new groups
              // entirely. This is correct because any N groups are valid output
              // and avoids hash map growth for high-cardinality queries like Q18.
              final boolean useCapped = (sortAggIndex < 0 && topN > 0);
              final int groupCap = useCapped ? (int) topN : Integer.MAX_VALUE;
              if (liveDocs == null) {
                if (useCapped
                    && truncUnits[0] == null
                    && arithUnits[0] == null
                    && truncUnits[1] == null
                    && arithUnits[1] == null) {
                  // Two-phase capped path: fill groups, then count-only with key-set filter.
                  // Phase 1: Insert docs until cap groups are filled.
                  int doc = 0;
                  for (; doc < maxDoc && flatMap.size < groupCap; doc++) {
                    long k0 = 0, k1 = 0;
                    if (key0Varchar) {
                      SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                      if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                    } else {
                      SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                      if (dv != null && dv.advanceExact(doc)) k0 = dv.nextValue();
                    }
                    if (key1Varchar) {
                      SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                      if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                    } else {
                      SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                      if (dv != null && dv.advanceExact(doc)) k1 = dv.nextValue();
                    }
                    int slot = flatMap.findOrInsert(k0, k1);
                    flatMap.accData[slot * flatSlotsPerGroup]++;
                  }

                  // Phase 2: Groups are full. Extract distinct k0 values for quick pre-filter.
                  // Read only key0 first; skip doc if k0 doesn't match any known group.
                  // This saves the second DV read for 99%+ of docs in high-cardinality queries.
                  int numGroups = flatMap.size;
                  if (numGroups > 0 && numGroups <= 64 && doc < maxDoc) {
                    // Build set of known k0 values from the capped groups
                    long[] knownK0 = new long[numGroups];
                    int k0Count = 0;
                    for (int s = 0; s < flatMap.capacity; s++) {
                      if (flatMap.keys0[s] != FlatTwoKeyMap.EMPTY_KEY) {
                        // Deduplicate k0 values
                        long v = flatMap.keys0[s];
                        boolean found = false;
                        for (int j = 0; j < k0Count; j++) {
                          if (knownK0[j] == v) {
                            found = true;
                            break;
                          }
                        }
                        if (!found) knownK0[k0Count++] = v;
                      }
                    }
                    final int numK0 = k0Count;
                    final long[] k0Set = knownK0;

                    for (; doc < maxDoc; doc++) {
                      long k0 = 0;
                      if (key0Varchar) {
                        SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                        if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                      } else {
                        SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                        if (dv != null && dv.advanceExact(doc)) k0 = dv.nextValue();
                      }
                      // Quick filter: if k0 doesn't match any capped group, skip entirely
                      boolean k0Match = false;
                      for (int j = 0; j < numK0; j++) {
                        if (k0Set[j] == k0) {
                          k0Match = true;
                          break;
                        }
                      }
                      if (!k0Match) continue;

                      // k0 matches; read k1 and do full hash lookup
                      long k1 = 0;
                      if (key1Varchar) {
                        SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                        if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                      } else {
                        SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                        if (dv != null && dv.advanceExact(doc)) k1 = dv.nextValue();
                      }
                      int slot = flatMap.findOrInsertCapped(k0, k1, groupCap);
                      if (slot >= 0) {
                        flatMap.accData[slot * flatSlotsPerGroup]++;
                      }
                    }
                  } else {
                    // Fallback for >64 groups or remaining docs
                    for (; doc < maxDoc; doc++) {
                      long k0 = 0, k1 = 0;
                      if (key0Varchar) {
                        SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
                        if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
                      } else {
                        SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[0];
                        if (dv != null && dv.advanceExact(doc)) k0 = dv.nextValue();
                      }
                      if (key1Varchar) {
                        SortedSetDocValues dv = (SortedSetDocValues) keyReaders[1];
                        if (dv != null && dv.advanceExact(doc)) k1 = dv.nextOrd();
                      } else {
                        SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[1];
                        if (dv != null && dv.advanceExact(doc)) k1 = dv.nextValue();
                      }
                      int slot = flatMap.findOrInsertCapped(k0, k1, groupCap);
                      if (slot >= 0) {
                        flatMap.accData[slot * flatSlotsPerGroup]++;
                      }
                    }
                  }
                } else {
                  // Original path: no two-phase optimization
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
                    int slot =
                        useCapped
                            ? flatMap.findOrInsertCapped(k0, k1, groupCap)
                            : flatMap.findOrInsert(k0, k1);
                    if (slot >= 0) {
                      flatMap.accData[slot * flatSlotsPerGroup]++;
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
                  int slot =
                      useCapped
                          ? flatMap.findOrInsertCapped(k0, k1, groupCap)
                          : flatMap.findOrInsert(k0, k1);
                  if (slot >= 0) {
                    flatMap.accData[slot * flatSlotsPerGroup]++;
                  }
                }
              }
            } else {
              // General MatchAllDocsQuery fast path with aggregate reads
              final boolean useCappedGen = (sortAggIndex < 0 && topN > 0);
              final int groupCapGen = useCappedGen ? (int) topN : Integer.MAX_VALUE;
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
                  int slot =
                      useCappedGen
                          ? flatMap.findOrInsertCapped(k0, k1, groupCapGen)
                          : flatMap.findOrInsert(k0, k1);
                  if (slot < 0) continue;
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
                  int slot =
                      useCappedGen
                          ? flatMap.findOrInsertCapped(k0, k1, groupCapGen)
                          : flatMap.findOrInsert(k0, k1);
                  if (slot < 0) continue;
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
            // === Filtered query path: bitset for selective, Collector for broad ===
            // Bitset pre-collection avoids Collector virtual dispatch but costs an
            // extra pass. Only beneficial when filter is selective (<50% of docs).
            LeafReaderContext leafCtx = leaves.get(0);
            LeafReader reader = leafCtx.reader();
            int maxDoc = reader.maxDoc();
            Weight weight = engineSearcher.createWeight(
                engineSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            int estCount = weight.count(leafCtx);
            boolean useDirectBitset = (estCount >= 0 && estCount < maxDoc / 2);

            if (useDirectBitset) {
              // Selective filter: collect doc IDs into bitset, then iterate directly
              FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
              Scorer scorer = weight.scorer(leafCtx);
              if (scorer != null) {
                DocIdSetIterator disi = scorer.iterator();
                for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS;
                    doc = disi.nextDoc()) {
                  matchingDocs.set(doc);
                }
              }
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
              SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i]) {
                  numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
                }
              }
              final boolean useCapped = (sortAggIndex < 0 && topN > 0);
              final int groupCap = useCapped ? (int) topN : Integer.MAX_VALUE;
              for (int doc = matchingDocs.nextSetBit(0); doc >= 0;
                  doc = matchingDocs.nextSetBit(doc + 1)) {
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
                int slot = useCapped
                    ? flatMap.findOrInsertCapped(k0, k1, groupCap)
                    : flatMap.findOrInsert(k0, k1);
                if (slot < 0) continue;
                int base = slot * flatSlotsPerGroup;
                for (int i = 0; i < numAggs; i++) {
                  int off = base + flatAccOffset[i];
                  if (isCountStar[i]) { flatMap.accData[off]++; continue; }
                  SortedNumericDocValues aggDv = numericAggDvs[i];
                  if (aggDv != null && aggDv.advanceExact(doc)) {
                    long rawVal = aggDv.nextValue();
                    switch (accType[i]) {
                      case 0: flatMap.accData[off]++; break;
                      case 1: flatMap.accData[off] += rawVal; break;
                      case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                    }
                  }
                }
              }
            } else {
              // Broad filter or unknown count: use Collector path (single pass)
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
                          int slot =
                              (sortAggIndex < 0 && topN > 0)
                                  ? flatMap.findOrInsertCapped(k0, k1, (int) topN)
                                  : flatMap.findOrInsert(k0, k1);
                          if (slot < 0) return;
                          int base = slot * flatSlotsPerGroup;
                          for (int i = 0; i < numAggs; i++) {
                            int off = base + flatAccOffset[i];
                            if (isCountStar[i]) { flatMap.accData[off]++; continue; }
                            SortedNumericDocValues aggDv = numericAggDvs[i];
                            if (aggDv != null && aggDv.advanceExact(doc)) {
                              long rawVal = aggDv.nextValue();
                              switch (accType[i]) {
                                case 0: flatMap.accData[off]++; break;
                                case 1: flatMap.accData[off] += rawVal; break;
                                case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
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
          } // end of filtered query path

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
              if (flatMap.keys0[slot] == FlatTwoKeyMap.EMPTY_KEY) continue;
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
              if (flatMap.keys0[slot] != FlatTwoKeyMap.EMPTY_KEY) {
                outputSlots[idx++] = slot;
              }
            }
            outputCount = idx;
          } else {
            // No top-N: output all slots
            outputSlots = new int[flatMap.size];
            int idx = 0;
            for (int slot = 0; slot < flatMap.capacity; slot++) {
              if (flatMap.keys0[slot] != FlatTwoKeyMap.EMPTY_KEY) {
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

      // === N-key path (3+ keys or multi-segment fallback) ===
      // Extracted to a separate method to keep executeWithVarcharKeys below the JIT
      // HugeMethodLimit (8000 bytecodes). Without this extraction, the JVM's C2 compiler
      // skips optimization of the entire method, causing 5-10x regression on 2-key queries.
      return executeNKeyVarcharPath(
          engineSearcher,
          query,
          singleSegment,
          keyInfos,
          specs,
          numAggs,
          isCountStar,
          isDoubleArg,
          isVarcharArg,
          accType,
          truncUnits,
          arithUnits,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN,
          columnTypeMap);
    }
  }

  /**
   * Ordinal-indexed two-key COUNT(*) path, extracted from executeWithVarcharKeys to keep that
   * method below the JVM's HugeMethodLimit (8000 bytecodes) for C2 JIT compilation.
   *
   * <p>Uses the varchar ordinal as a direct array index and maintains a small per-ordinal
   * open-addressing map for the numeric key. Eliminates hash2() computation and two-key hash
   * probing -- critical for Q15 where SearchPhrase has 18K ordinals but SearchEngineID has only 39
   * distinct values.
   *
   * @return the result pages, or {@code null} if this path is not applicable (caller should fall
   *     through to FlatTwoKeyMap)
   */
  private static List<Page> tryOrdinalIndexedTwoKeyCountStar(
      org.opensearch.index.engine.Engine.Searcher engineSearcher,
      Query query,
      LeafReaderContext leafCtx,
      List<KeyInfo> keyInfos,
      int numAggs,
      boolean[] isCountStar,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {
    // Check precondition: exactly one varchar key, one numeric key (no date_trunc/arith), all
    // COUNT(*)
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
    if (varcharKeyIdx < 0 || numericKeyIdx < 0) {
      return null; // not applicable
    }

    LeafReader reader = leafCtx.reader();
    int maxDoc = reader.maxDoc();
    Bits liveDocs = reader.getLiveDocs();

    SortedSetDocValues varcharDv = reader.getSortedSetDocValues(keyInfos.get(varcharKeyIdx).name);
    SortedNumericDocValues numericDv =
        reader.getSortedNumericDocValues(keyInfos.get(numericKeyIdx).name);

    if (varcharDv == null || numericDv == null) {
      return null;
    }
    long valueCount = varcharDv.getValueCount();
    if (valueCount > 500_000) {
      return null;
    }
    int numOrds = (int) valueCount;

    final int KEYS_PER_ORD = 64;

    // Heuristic: check numeric key's value range via PointValues
    boolean numericKeyLikelyHighCardinality = false;
    org.apache.lucene.index.PointValues pv =
        reader.getPointValues(keyInfos.get(numericKeyIdx).name);
    if (pv != null) {
      byte[] minBytes = pv.getMinPackedValue();
      byte[] maxBytes = pv.getMaxPackedValue();
      if (minBytes != null && maxBytes != null && minBytes.length >= 8) {
        long minVal = 0, maxVal = 0;
        for (int b = 0; b < 8; b++) {
          minVal = (minVal << 8) | (minBytes[b] & 0xFFL);
          maxVal = (maxVal << 8) | (maxBytes[b] & 0xFFL);
        }
        minVal ^= 0x8000000000000000L;
        maxVal ^= 0x8000000000000000L;
        long range = maxVal - minVal;
        if (range < 0 || range > KEYS_PER_ORD) {
          numericKeyLikelyHighCardinality = true;
        }
      }
    }

    if (numericKeyLikelyHighCardinality || (long) numOrds * KEYS_PER_ORD * 16 > 32_000_000L) {
      return null;
    }

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
      final SortedSetDocValues fVarcharDv = varcharDv;
      final SortedNumericDocValues fNumericDv = numericDv;
      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  if (overflowFlag[0]) return;
                  if (!fVarcharDv.advanceExact(doc)) return;
                  long ord = fVarcharDv.nextOrd();
                  if (!fNumericDv.advanceExact(doc)) return;
                  long nk = fNumericDv.nextValue();
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
      return null; // fall through to FlatTwoKeyMap
    }

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
            boolean better = sortAscending ? (cnt < heapVals[0]) : (cnt > heapVals[0]);
            if (better) {
              heap[0] = ((long) o << 32) | j;
              heapVals[0] = cnt;
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
          boolean needsSwap = sortAscending ? (heapVals[j] > keyVal) : (heapVals[j] < keyVal);
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

        if (varcharKeyIdx == 0) {
          BytesRef bytes = varcharDv.lookupOrd(ord);
          VarcharType.VARCHAR.writeSlice(
              builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
          writeNumericKeyValue(builders[1], keyInfos.get(1), nk);
        } else {
          writeNumericKeyValue(builders[0], keyInfos.get(0), nk);
          BytesRef bytes = varcharDv.lookupOrd(ord);
          VarcharType.VARCHAR.writeSlice(
              builders[1], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
        }
        BigintType.BIGINT.writeLong(builders[numGroupKeys], cnt);
      }

      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
      return List.of(new Page(blocks));
    } else {
      // No top-N or LIMIT-only: output all groups (or first topN if LIMIT set)
      int outputGroups = (topN > 0 && topN < totalGroups) ? (int) topN : totalGroups;
      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      for (int i = 0; i < numGroupKeys; i++) {
        builders[i] = keyInfos.get(i).type.createBlockBuilder(null, outputGroups);
      }
      builders[numGroupKeys] = BigintType.BIGINT.createBlockBuilder(null, outputGroups);
      int emitted = 0;
      for (int o = 0; o < numOrds && emitted < outputGroups; o++) {
        int nk = numKeysPerOrd[o];
        if (nk == 0) continue;
        int base = o * KEYS_PER_ORD;
        BytesRef bytes = varcharDv.lookupOrd(o);
        io.airlift.slice.Slice slice =
            Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length);
        for (int j = 0; j < nk && emitted < outputGroups; j++) {
          if (varcharKeyIdx == 0) {
            VarcharType.VARCHAR.writeSlice(builders[0], slice);
            writeNumericKeyValue(builders[1], keyInfos.get(1), numericKeys[base + j]);
          } else {
            writeNumericKeyValue(builders[0], keyInfos.get(0), numericKeys[base + j]);
            VarcharType.VARCHAR.writeSlice(builders[1], slice);
          }
          BigintType.BIGINT.writeLong(builders[numGroupKeys], counts[base + j]);
          emitted++;
        }
      }
      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
      return List.of(new Page(blocks));
    }
  }

  /**
   * N-key varchar GROUP BY path, extracted from executeWithVarcharKeys to keep that method below
   * the JVM's HugeMethodLimit (8000 bytecodes) for C2 JIT compilation.
   *
   * <p>Handles both single-segment (3+ keys) and multi-segment cases with SegmentGroupKey /
   * MergedGroupKey maps. Includes optional top-N pre-filtering via min-heap.
   */

  /**
   * Merge result Pages from multiple hash-partitioned aggregation buckets. Each bucket produces a
   * Page with its top-K rows; this method merges them by sorting on the aggregate column and taking
   * the global top-K.
   */
  private static List<Page> mergePartitionedPages(
      List<Page> bucketPages,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int numAggs,
      int[] accType,
      int sortAggIndex,
      boolean sortAscending,
      long topN) {

    // Count total rows across all bucket pages
    int totalRows = 0;
    for (Page p : bucketPages) {
      totalRows += p.getPositionCount();
    }
    if (totalRows == 0) return List.of();

    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Resolve column types for output
    Type[] colTypes = new Type[totalColumns];
    for (int c = 0; c < numGroupKeys; c++) {
      colTypes[c] = keyInfos.get(c).type;
    }
    for (int c = 0; c < numAggs; c++) {
      colTypes[numGroupKeys + c] = resolveAggOutputType(specs.get(c), columnTypeMap);
    }

    // If no sorting or topN, just concatenate all pages
    if (sortAggIndex < 0 || topN <= 0) {
      int limit = topN > 0 ? (int) Math.min(topN, totalRows) : totalRows;
      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      for (int c = 0; c < totalColumns; c++) {
        builders[c] = colTypes[c].createBlockBuilder(null, limit);
      }
      int written = 0;
      for (Page p : bucketPages) {
        for (int row = 0; row < p.getPositionCount() && written < limit; row++) {
          for (int c = 0; c < totalColumns; c++) {
            copyBlockValue(p.getBlock(c), row, builders[c], colTypes[c]);
          }
          written++;
        }
      }
      Block[] blocks = new Block[totalColumns];
      for (int c = 0; c < totalColumns; c++) blocks[c] = builders[c].build();
      return List.of(new Page(blocks));
    }

    // Sort by aggregate column and take topN. Build index array pointing to (page, row).
    int[] pageIdx = new int[totalRows];
    int[] rowIdx = new int[totalRows];
    int idx = 0;
    for (int pi = 0; pi < bucketPages.size(); pi++) {
      Page p = bucketPages.get(pi);
      for (int r = 0; r < p.getPositionCount(); r++) {
        pageIdx[idx] = pi;
        rowIdx[idx] = r;
        idx++;
      }
    }

    // Extract sort values (the aggregate column at sortAggIndex).
    // Sort column is always BIGINT (COUNT/SUM) in the flat accumulator path.
    int sortCol = numGroupKeys + sortAggIndex;
    Type sortType = colTypes[sortCol];
    long[] sortValues = new long[totalRows];
    for (int i = 0; i < totalRows; i++) {
      Page p = bucketPages.get(pageIdx[i]);
      Block block = p.getBlock(sortCol);
      sortValues[i] = sortType.getLong(block, rowIdx[i]);
    }

    // Use top-N with min-heap for DESC (or max-heap for ASC)
    int n = (int) Math.min(topN, totalRows);
    int[] heap = new int[n];
    int heapSize = 0;
    for (int i = 0; i < totalRows; i++) {
      long val = sortValues[i];
      if (heapSize < n) {
        heap[heapSize] = i;
        heapSize++;
        // Sift up
        int k = heapSize - 1;
        while (k > 0) {
          int parent = (k - 1) >>> 1;
          long pVal = sortValues[heap[parent]];
          long kVal = sortValues[heap[k]];
          boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
          if (swap) {
            int tmp = heap[parent];
            heap[parent] = heap[k];
            heap[k] = tmp;
            k = parent;
          } else break;
        }
      } else {
        long rootVal = sortValues[heap[0]];
        boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
        if (better) {
          heap[0] = i;
          // Sift down
          int k = 0;
          while (true) {
            int left = 2 * k + 1;
            if (left >= heapSize) break;
            int right = left + 1;
            int target = left;
            if (right < heapSize) {
              long lv = sortValues[heap[left]];
              long rv = sortValues[heap[right]];
              boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
              if (pickRight) target = right;
            }
            long kVal = sortValues[heap[k]];
            long tVal = sortValues[heap[target]];
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

    // Sort the heap entries by value for ordered output
    for (int i = 1; i < heapSize; i++) {
      int key = heap[i];
      long keyVal = sortValues[key];
      int j = i - 1;
      while (j >= 0) {
        long jVal = sortValues[heap[j]];
        boolean needsSwap = sortAscending ? (jVal > keyVal) : (jVal < keyVal);
        if (needsSwap) {
          heap[j + 1] = heap[j];
          j--;
        } else break;
      }
      heap[j + 1] = key;
    }

    // Build output page from selected rows
    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int c = 0; c < totalColumns; c++) {
      builders[c] = colTypes[c].createBlockBuilder(null, heapSize);
    }
    for (int i = 0; i < heapSize; i++) {
      int gi = heap[i];
      Page p = bucketPages.get(pageIdx[gi]);
      int row = rowIdx[gi];
      for (int c = 0; c < totalColumns; c++) {
        copyBlockValue(p.getBlock(c), row, builders[c], colTypes[c]);
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int c = 0; c < totalColumns; c++) blocks[c] = builders[c].build();
    return List.of(new Page(blocks));
  }

  /** Copy a single value from a Block at the given position into a BlockBuilder, using the Type. */
  private static void copyBlockValue(Block source, int position, BlockBuilder dest, Type type) {
    if (source.isNull(position)) {
      dest.appendNull();
    } else if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(dest, VarcharType.VARCHAR.getSlice(source, position));
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(dest, DoubleType.DOUBLE.getDouble(source, position));
    } else {
      // BIGINT, INTEGER, SMALLINT, TINYINT, TIMESTAMP — all use writeLong
      type.writeLong(dest, type.getLong(source, position));
    }
  }

  /**
   * Fast flat 3-key path for GROUP BY with 3 keys and COUNT(*)/SUM/AVG aggregates. Supports
   * parallel multi-segment execution via ForkJoinPool workers, each with its own FlatThreeKeyMap.
   *
   * @return result pages, or null if the path is not applicable (caller falls through)
   */
  private static List<Page> executeThreeKeyFlat(
      org.opensearch.index.engine.Engine.Searcher engineSearcher,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      String[] truncUnits,
      String[] arithUnits,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN,
      Map<String, Type> columnTypeMap,
      int bucket,
      int numBuckets)
      throws Exception {

    // Pre-compute flat accumulator offsets (same scheme as 2-key path)
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
    final FlatThreeKeyMap flatMap = new FlatThreeKeyMap(flatSlotsPerGroup);

    List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
    boolean isMatchAll = query instanceof MatchAllDocsQuery;
    boolean anyVarcharKey =
        keyInfos.get(0).isVarchar || keyInfos.get(1).isVarchar || keyInfos.get(2).isVarchar;
    // Parallel only when all keys are numeric (ordinals are segment-local for varchar)
    boolean canParallelize =
        !"off".equals(PARALLELISM_MODE)
            && THREADS_PER_SHARD > 1
            && leaves.size() > 1
            && !anyVarcharKey;

    if (canParallelize) {
      int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
      @SuppressWarnings("unchecked")
      java.util.List<LeafReaderContext>[] workerSegments = new java.util.List[numWorkers];
      long[] workerDocCounts = new long[numWorkers];
      for (int i = 0; i < numWorkers; i++)
        workerSegments[i] = new java.util.ArrayList<>();
      // Largest-first greedy assignment for balanced load
      java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
      sortedLeaves.sort(
          (a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
      for (LeafReaderContext leaf : sortedLeaves) {
        int lightest = 0;
        for (int i = 1; i < numWorkers; i++) {
          if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
        }
        workerSegments[lightest].add(leaf);
        workerDocCounts[lightest] += leaf.reader().maxDoc();
      }

      final Weight weight;
      if (!isMatchAll) {
        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
      } else {
        weight = null;
      }

      @SuppressWarnings("unchecked")
      java.util.concurrent.CompletableFuture<FlatThreeKeyMap>[] futures =
          new java.util.concurrent.CompletableFuture[numWorkers];

      for (int w = 0; w < numWorkers; w++) {
        final java.util.List<LeafReaderContext> mySegments = workerSegments[w];
        futures[w] =
            java.util.concurrent.CompletableFuture.supplyAsync(
                () -> {
                  FlatThreeKeyMap localMap = new FlatThreeKeyMap(flatSlotsPerGroup);
                  try {
                    for (LeafReaderContext leafCtx : mySegments) {
                      scanSegmentFlatThreeKey(
                          leafCtx, weight, isMatchAll, localMap, keyInfos, specs,
                          numAggs, isCountStar, accType, flatAccOffset,
                          flatSlotsPerGroup, truncUnits, arithUnits, bucket, numBuckets);
                    }
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                  return localMap;
                },
                PARALLEL_POOL);
      }

      java.util.concurrent.CompletableFuture.allOf(futures).join();
      for (int fi = 0; fi < futures.length; fi++) {
        FlatThreeKeyMap workerMap = futures[fi].join();
        if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
        futures[fi] = null; // release worker map for GC
      }
    } else {
      // Sequential fallback
      Weight weight = null;
      if (!isMatchAll) {
        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
      }
      for (LeafReaderContext leafCtx : leaves) {
        scanSegmentFlatThreeKey(
            leafCtx, weight, isMatchAll, flatMap, keyInfos, specs,
            numAggs, isCountStar, accType, flatAccOffset,
            flatSlotsPerGroup, truncUnits, arithUnits, bucket, numBuckets);
      }
    }

    if (flatMap.size == 0) return List.of();

    // Build output Page from FlatThreeKeyMap, resolving varchar ordinals to UTF-8.
    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Determine which slots to output (top-N selection if applicable)
    int[] outputSlots;
    int outputCount;

    if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
      // Top-N selection using a min-heap (for DESC) or max-heap (for ASC)
      int sortAccOff = flatAccOffset[sortAggIndex];
      int n = (int) Math.min(topN, flatMap.size);
      int[] heap = new int[n];
      int heapSize = 0;

      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.keys0[slot] == FlatThreeKeyMap.EMPTY_KEY) continue;
        long val = flatMap.accData[slot * flatSlotsPerGroup + sortAccOff];
        if (heapSize < n) {
          heap[heapSize] = slot;
          heapSize++;
          int ki = heapSize - 1;
          while (ki > 0) {
            int parent = (ki - 1) >>> 1;
            long pVal = flatMap.accData[heap[parent] * flatSlotsPerGroup + sortAccOff];
            long kVal = flatMap.accData[heap[ki] * flatSlotsPerGroup + sortAccOff];
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              int tmp = heap[parent];
              heap[parent] = heap[ki];
              heap[ki] = tmp;
              ki = parent;
            } else break;
          }
        } else {
          long rootVal = flatMap.accData[heap[0] * flatSlotsPerGroup + sortAccOff];
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = slot;
            int ki = 0;
            while (true) {
              int left = 2 * ki + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = flatMap.accData[heap[left] * flatSlotsPerGroup + sortAccOff];
                long rv = flatMap.accData[heap[right] * flatSlotsPerGroup + sortAccOff];
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = flatMap.accData[heap[ki] * flatSlotsPerGroup + sortAccOff];
              long tVal = flatMap.accData[heap[target] * flatSlotsPerGroup + sortAccOff];
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                int tmp = heap[ki];
                heap[ki] = heap[target];
                heap[target] = tmp;
                ki = target;
              } else break;
            }
          }
        }
      }
      // Sort the heap by actual value for ordered output
      final int sOff = sortAccOff;
      final int spg = flatSlotsPerGroup;
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
          } else break;
        }
        heap[j + 1] = key;
      }
      outputSlots = heap;
      outputCount = heapSize;
    } else if (topN > 0 && topN < flatMap.size) {
      int n = (int) Math.min(topN, flatMap.size);
      outputSlots = new int[n];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity && idx < n; slot++) {
        if (flatMap.keys0[slot] != FlatThreeKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[flatMap.size];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.keys0[slot] != FlatThreeKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
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

    // Re-open varchar key readers for ordinal-to-string resolution in output building.
    // Only needed for single-segment path (multi-segment requires all-numeric keys).
    Object[] outputKeyReaders = new Object[3];
    for (int ki = 0; ki < 3; ki++) {
      if (keyInfos.get(ki).isVarchar) {
        outputKeyReaders[ki] =
            leaves.get(0).reader().getSortedSetDocValues(keyInfos.get(ki).name);
      }
    }

    for (int si = 0; si < outputCount; si++) {
      int slot = outputSlots[si];
      long[] keyVals = {flatMap.keys0[slot], flatMap.keys1[slot], flatMap.keys2[slot]};
      for (int ki = 0; ki < 3; ki++) {
        if (keyInfos.get(ki).isVarchar) {
          SortedSetDocValues dv = (SortedSetDocValues) outputKeyReaders[ki];
          if (dv != null) {
            BytesRef bytes = dv.lookupOrd(keyVals[ki]);
            VarcharType.VARCHAR.writeSlice(
                builders[ki], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
          } else {
            VarcharType.VARCHAR.writeSlice(builders[ki], Slices.EMPTY_SLICE);
          }
        } else {
          writeNumericKeyValue(builders[ki], keyInfos.get(ki), keyVals[ki]);
        }
      }
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

  /**
   * Scan a single segment for the flat three-key GROUP BY path. Handles both MatchAll and
   * filtered paths, including varchar/numeric key reading, truncation/arithmetic transforms,
   * and bucket filtering. Extracted from {@link #executeThreeKeyFlat} to enable parallel
   * segment processing.
   */
  private static void scanSegmentFlatThreeKey(
      LeafReaderContext leafCtx,
      Weight weight,
      boolean isMatchAll,
      FlatThreeKeyMap flatMap,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] flatAccOffset,
      int flatSlotsPerGroup,
      String[] truncUnits,
      String[] arithUnits,
      int bucket,
      int numBuckets)
      throws Exception {

    LeafReader reader = leafCtx.reader();
    final boolean key0Varchar = keyInfos.get(0).isVarchar;
    final boolean key1Varchar = keyInfos.get(1).isVarchar;
    final boolean key2Varchar = keyInfos.get(2).isVarchar;

    // Open key readers for this segment
    Object[] keyReaders = new Object[3];
    for (int i = 0; i < 3; i++) {
      KeyInfo ki = keyInfos.get(i);
      if (ki.isVarchar) {
        keyReaders[i] = reader.getSortedSetDocValues(ki.name);
      } else {
        keyReaders[i] = reader.getSortedNumericDocValues(ki.name);
      }
    }

    boolean allCountStar = true;
    for (int i = 0; i < numAggs; i++) {
      if (!isCountStar[i]) { allCountStar = false; break; }
    }

    // Open aggregate DV readers (only if not all COUNT(*))
    SortedNumericDocValues[] numericAggDvs = null;
    if (!allCountStar) {
      numericAggDvs = new SortedNumericDocValues[numAggs];
      for (int i = 0; i < numAggs; i++) {
        if (!isCountStar[i]) {
          numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
        }
      }
    }

    if (isMatchAll) {
      int maxDoc = reader.maxDoc();
      Bits liveDocs = reader.getLiveDocs();

      for (int doc = 0; doc < maxDoc; doc++) {
        if (liveDocs != null && !liveDocs.get(doc)) continue;
        long k0 = 0, k1 = 0, k2 = 0;
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
        if (key2Varchar) {
          SortedSetDocValues dv = (SortedSetDocValues) keyReaders[2];
          if (dv != null && dv.advanceExact(doc)) k2 = dv.nextOrd();
        } else {
          SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[2];
          if (dv != null && dv.advanceExact(doc)) {
            k2 = dv.nextValue();
            if (truncUnits[2] != null) k2 = truncateMillis(k2, truncUnits[2]);
            else if (arithUnits[2] != null) k2 = applyArith(k2, arithUnits[2]);
          }
        }
        if (numBuckets > 1) {
          int docBucket = (FlatThreeKeyMap.hash3(k0, k1, k2) & 0x7FFFFFFF) % numBuckets;
          if (docBucket != bucket) continue;
        }
        int slot = flatMap.findOrInsert(k0, k1, k2);
        if (allCountStar) {
          flatMap.accData[slot * flatSlotsPerGroup]++;
        } else {
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
                case 0: flatMap.accData[off]++; break;
                case 1: flatMap.accData[off] += rawVal; break;
                case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
              }
            }
          }
        }
      }
    } else {
      // Filtered path: use Weight+Scorer per segment
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) return;
      DocIdSetIterator docIt = scorer.iterator();
      int doc;
      while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        long k0 = 0, k1 = 0, k2 = 0;
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
        if (key2Varchar) {
          SortedSetDocValues dv = (SortedSetDocValues) keyReaders[2];
          if (dv != null && dv.advanceExact(doc)) k2 = dv.nextOrd();
        } else {
          SortedNumericDocValues dv = (SortedNumericDocValues) keyReaders[2];
          if (dv != null && dv.advanceExact(doc)) {
            k2 = dv.nextValue();
            if (truncUnits[2] != null) k2 = truncateMillis(k2, truncUnits[2]);
            else if (arithUnits[2] != null) k2 = applyArith(k2, arithUnits[2]);
          }
        }
        if (numBuckets > 1) {
          int docBucket = (FlatThreeKeyMap.hash3(k0, k1, k2) & 0x7FFFFFFF) % numBuckets;
          if (docBucket != bucket) continue;
        }
        int slot = flatMap.findOrInsert(k0, k1, k2);
        if (allCountStar) {
          flatMap.accData[slot * flatSlotsPerGroup]++;
        } else {
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
                case 0: flatMap.accData[off]++; break;
                case 1: flatMap.accData[off] += rawVal; break;
                case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
              }
            }
          }
        }
      }
    }
  }

  private static List<Page> executeNKeyVarcharPath(
      org.opensearch.index.engine.Engine.Searcher engineSearcher,
      Query query,
      boolean singleSegment,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      boolean[] isDoubleArg,
      boolean[] isVarcharArg,
      int[] accType,
      String[] truncUnits,
      String[] arithUnits,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN,
      Map<String, Type> columnTypeMap)
      throws Exception {

    // === 3-key flat fast path (single or multi-segment for numeric keys) ===
    // For exactly 3 keys with flat-compatible agg types (COUNT/SUM/AVG, no DISTINCT),
    // use FlatThreeKeyMap with parallel segment scanning to avoid per-group object allocation.
    // Multi-segment only when all keys are numeric (varchar ordinals are segment-local).
    if (keyInfos.size() == 3) {
      boolean anyVarcharKey =
          keyInfos.get(0).isVarchar || keyInfos.get(1).isVarchar || keyInfos.get(2).isVarchar;
      if (singleSegment || !anyVarcharKey) {
        boolean canUseFlatThreeKey = true;
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i]) {
            AggSpec spec = specs.get(i);
            if (isVarcharArg[i] || isDoubleArg[i]) {
              canUseFlatThreeKey = false;
              break;
            }
            switch (spec.funcName) {
              case "COUNT":
                if (spec.isDistinct) canUseFlatThreeKey = false;
                break;
              case "SUM":
              case "AVG":
                break;
              default:
                canUseFlatThreeKey = false;
                break;
            }
            if (!canUseFlatThreeKey) break;
          }
        }
        if (canUseFlatThreeKey) {
          long totalDocs = 0;
          for (LeafReaderContext leaf : engineSearcher.getIndexReader().leaves()) {
            totalDocs += leaf.reader().maxDoc();
          }
          int threeKeyBuckets =
              Math.max(1, (int) Math.ceil((double) totalDocs / FlatThreeKeyMap.MAX_CAPACITY));

          if (threeKeyBuckets <= 1) {
            List<Page> flatResult =
                executeThreeKeyFlat(
                    engineSearcher,
                    query,
                    keyInfos,
                    specs,
                    numAggs,
                    isCountStar,
                    accType,
                    truncUnits,
                    arithUnits,
                    groupByKeys,
                    sortAggIndex,
                    sortAscending,
                    topN,
                    columnTypeMap,
                    0,
                    1);
            if (flatResult != null) {
              return flatResult;
            }
          } else {
            List<Page> allBucketResults = new ArrayList<>();
            for (int bkt = 0; bkt < threeKeyBuckets; bkt++) {
              List<Page> bucketPages =
                  executeThreeKeyFlat(
                      engineSearcher,
                      query,
                      keyInfos,
                      specs,
                      numAggs,
                      isCountStar,
                      accType,
                      truncUnits,
                      arithUnits,
                      groupByKeys,
                      sortAggIndex,
                      sortAscending,
                      topN,
                      columnTypeMap,
                      bkt,
                      threeKeyBuckets);
              if (bucketPages != null) {
                allBucketResults.addAll(bucketPages);
              }
            }
            if (!allBucketResults.isEmpty()) {
              if (allBucketResults.size() == 1) return allBucketResults;
              return mergePartitionedPages(
                  allBucketResults,
                  keyInfos,
                  specs,
                  columnTypeMap,
                  groupByKeys,
                  numAggs,
                  accType,
                  sortAggIndex,
                  sortAscending,
                  topN);
            }
          }
        }
      }
    }

    if (singleSegment) {

      // === Single-segment N-key path (3+ keys) ===
      // Uses SegmentGroupKey map with ordinals for varchar keys.
      Map<SegmentGroupKey, AccumulatorGroup> segmentGroups = new HashMap<>();
      final Object[][] keyReadersHolder = new Object[1][];
      // Early termination cap for LIMIT without ORDER BY
      final boolean singleSegEarlyTerm = (sortAggIndex < 0 && topN > 0);
      final int singleSegGroupCap = singleSegEarlyTerm ? (int) topN : Integer.MAX_VALUE;

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
                    // Early termination: skip new groups once we have enough
                    if (segmentGroups.size() >= singleSegGroupCap) {
                      throw new org.apache.lucene.search.CollectionTerminatedException();
                    }
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

      // If top-N is requested and fewer than total groups, select top-N entries via min-heap.
      // This avoids constructing a full Page with all groups and dramatically reduces the
      // number of rows sent to the coordinator (e.g., Q19 with 30K+ groups -> 1000 groups).
      java.util.Collection<Map.Entry<SegmentGroupKey, AccumulatorGroup>> outputEntries;
      if (sortAggIndex >= 0 && topN > 0 && topN < segmentGroups.size()) {
        int n = (int) Math.min(topN, segmentGroups.size());
        @SuppressWarnings("unchecked")
        Map.Entry<SegmentGroupKey, AccumulatorGroup>[] heap = new Map.Entry[n];
        int heapSize = 0;
        for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry : segmentGroups.entrySet()) {
          long val = entry.getValue().accumulators[sortAggIndex].getSortValue();
          if (heapSize < n) {
            heap[heapSize++] = entry;
            int ki = heapSize - 1;
            while (ki > 0) {
              int parent = (ki - 1) >>> 1;
              long pVal = heap[parent].getValue().accumulators[sortAggIndex].getSortValue();
              long kVal = heap[ki].getValue().accumulators[sortAggIndex].getSortValue();
              boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
              if (swap) {
                var tmp = heap[parent];
                heap[parent] = heap[ki];
                heap[ki] = tmp;
                ki = parent;
              } else break;
            }
          } else {
            long rootVal = heap[0].getValue().accumulators[sortAggIndex].getSortValue();
            boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
            if (better) {
              heap[0] = entry;
              int ki = 0;
              while (true) {
                int left = 2 * ki + 1;
                if (left >= heapSize) break;
                int right = left + 1;
                int target = left;
                if (right < heapSize) {
                  long lv = heap[left].getValue().accumulators[sortAggIndex].getSortValue();
                  long rv = heap[right].getValue().accumulators[sortAggIndex].getSortValue();
                  boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                  if (pickRight) target = right;
                }
                long kVal = heap[ki].getValue().accumulators[sortAggIndex].getSortValue();
                long tVal = heap[target].getValue().accumulators[sortAggIndex].getSortValue();
                boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
                if (swap) {
                  var tmp = heap[ki];
                  heap[ki] = heap[target];
                  heap[target] = tmp;
                  ki = target;
                } else break;
              }
            }
          }
        }
        outputEntries = java.util.Arrays.asList(heap).subList(0, heapSize);
      } else {
        outputEntries = segmentGroups.entrySet();
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

    // === Global ordinals multi-segment path ===
    // Build OrdinalMap for each VARCHAR key. With global ordinals, all keys
    // become longs, enabling a single global HashMap without cross-segment merge.
    // Only use when sortAggIndex >= 0 AND topN > 0 (ORDER BY agg DESC LIMIT N) —
    // this enables top-K pruning that keeps the HashMap small. Without ORDER BY,
    // all ~6M groups go into the HashMap and the parallel docrange path is faster.
    // Use global ordinals for MatchAll always, or filtered queries when VARCHAR keys have ≤1M
    // ordinals
    boolean useGlobalOrdMultiKey = sortAggIndex >= 0 && topN > 0;
    if (useGlobalOrdMultiKey && !(query instanceof MatchAllDocsQuery)) {
      // Check if all VARCHAR keys have low cardinality (OrdinalMap build cost acceptable)
      List<LeafReaderContext> checkLeaves = engineSearcher.getIndexReader().leaves();
      for (int k = 0; k < keyInfos.size(); k++) {
        if (keyInfos.get(k).isVarchar && checkLeaves.size() > 0) {
          SortedSetDocValues checkDv =
              checkLeaves.get(0).reader().getSortedSetDocValues(keyInfos.get(k).name);
          if (checkDv == null || checkDv.getValueCount() > 1_000_000) {
            useGlobalOrdMultiKey = false;
            break;
          }
        }
      }
    }
    if (useGlobalOrdMultiKey) {
      List<LeafReaderContext> globalOrdLeaves = engineSearcher.getIndexReader().leaves();
      OrdinalMap[] ordinalMaps = new OrdinalMap[keyInfos.size()];
      boolean canUseGlobalOrd = globalOrdLeaves.size() > 1;
      if (canUseGlobalOrd) {
        for (int k = 0; k < keyInfos.size(); k++) {
          if (keyInfos.get(k).isVarchar) {
            ordinalMaps[k] = buildGlobalOrdinalMap(globalOrdLeaves, keyInfos.get(k).name);
            if (ordinalMaps[k] == null) {
              canUseGlobalOrd = false;
              break;
            }
          }
        }
      }

      if (canUseGlobalOrd) {
        final int numKeys = keyInfos.size();

        // Prepare per-segment global ordinal mappings
        LongValues[][] segToGlobalMaps = new LongValues[globalOrdLeaves.size()][numKeys];
        for (int segIdx = 0; segIdx < globalOrdLeaves.size(); segIdx++) {
          for (int k = 0; k < numKeys; k++) {
            if (keyInfos.get(k).isVarchar) {
              segToGlobalMaps[segIdx][k] = ordinalMaps[k].getGlobalOrds(segIdx);
            }
          }
        }

        // Partition segments among workers (largest-first load balancing)
        int numWorkers = Math.min(THREADS_PER_SHARD, globalOrdLeaves.size());
        @SuppressWarnings("unchecked")
        List<Integer>[] workerSegments = new List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++) workerSegments[i] = new ArrayList<>();

        // Sort segment indices by doc count descending
        Integer[] segIndices = new Integer[globalOrdLeaves.size()];
        for (int i = 0; i < segIndices.length; i++) segIndices[i] = i;
        java.util.Arrays.sort(
            segIndices,
            (a, b) ->
                Integer.compare(
                    globalOrdLeaves.get(b).reader().maxDoc(),
                    globalOrdLeaves.get(a).reader().maxDoc()));
        for (int idx : segIndices) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(idx);
          workerDocCounts[lightest] += globalOrdLeaves.get(idx).reader().maxDoc();
        }

        // Build Weight for filtered queries
        final Weight globalOrdWeight;
        if (query instanceof MatchAllDocsQuery) {
          globalOrdWeight = null;
        } else {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          globalOrdWeight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        }

        // Parallel scan with per-worker HashMaps
        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<Map<SegmentGroupKey, AccumulatorGroup>>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final List<Integer> mySegments = workerSegments[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    // Pre-size HashMap to avoid resizes. Use total docs / numWorkers as estimate.
                    long totalDocs = 0;
                    for (long dc : workerDocCounts) totalDocs += dc;
                    long estDocs = totalDocs / numWorkers;
                    int initCap = Math.min((int) (estDocs / 2), 4_000_000);
                    Map<SegmentGroupKey, AccumulatorGroup> workerMap =
                        new HashMap<>(Math.max(initCap, 16384), 0.75f);
                    NumericProbeKey probeKey = new NumericProbeKey(numKeys);
                    try {
                      for (int segIdx : mySegments) {
                        LeafReaderContext leafCtx = globalOrdLeaves.get(segIdx);
                        LongValues[] segGlobalMaps = segToGlobalMaps[segIdx];

                        // Open key DocValues
                        Object[] keyReaders = new Object[numKeys];
                        for (int i = 0; i < numKeys; i++) {
                          KeyInfo ki = keyInfos.get(i);
                          if (ki.isVarchar) {
                            keyReaders[i] = leafCtx.reader().getSortedSetDocValues(ki.name);
                          } else {
                            keyReaders[i] = leafCtx.reader().getSortedNumericDocValues(ki.name);
                          }
                        }

                        // Open agg DocValues
                        SortedNumericDocValues[] numericAggDvs =
                            new SortedNumericDocValues[numAggs];
                        SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                        for (int i = 0; i < numAggs; i++) {
                          if (!isCountStar[i]) {
                            AggSpec spec = specs.get(i);
                            if (isVarcharArg[i]) {
                              varcharAggDvs[i] = leafCtx.reader().getSortedSetDocValues(spec.arg);
                            } else {
                              numericAggDvs[i] =
                                  leafCtx.reader().getSortedNumericDocValues(spec.arg);
                            }
                          }
                        }

                        // Iterate docs — use Scorer for filtered queries, direct for MatchAll
                        DocIdSetIterator docIter;
                        if (globalOrdWeight != null) {
                          Scorer scorer = globalOrdWeight.scorer(leafCtx);
                          if (scorer == null) continue; // no matching docs in this segment
                          docIter = scorer.iterator();
                        } else {
                          // MatchAll: iterate all docs
                          LeafReader reader = leafCtx.reader();
                          Bits liveDocs = reader.getLiveDocs();
                          int maxDoc = reader.maxDoc();
                          docIter =
                              new DocIdSetIterator() {
                                int current = -1;

                                @Override
                                public int docID() {
                                  return current;
                                }

                                @Override
                                public int nextDoc() {
                                  while (++current < maxDoc) {
                                    if (liveDocs == null || liveDocs.get(current)) return current;
                                  }
                                  return NO_MORE_DOCS;
                                }

                                @Override
                                public int advance(int target) {
                                  current = target - 1;
                                  return nextDoc();
                                }

                                @Override
                                public long cost() {
                                  return maxDoc;
                                }
                              };
                        }

                        int doc;
                        while ((doc = docIter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

                          probeKey.reset();
                          for (int k = 0; k < numKeys; k++) {
                            KeyInfo ki = keyInfos.get(k);
                            if (ki.isVarchar) {
                              SortedSetDocValues dv = (SortedSetDocValues) keyReaders[k];
                              if (dv != null && dv.advanceExact(doc)) {
                                long segOrd = dv.nextOrd();
                                long globalOrd = segGlobalMaps[k].get(segOrd);
                                probeKey.set(k, globalOrd);
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

                          AccumulatorGroup accGroup = workerMap.get(probeKey);
                          if (accGroup == null) {
                            SegmentGroupKey immutableKey = probeKey.toImmutableKey();
                            accGroup = createAccumulatorGroup(specs);
                            workerMap.put(immutableKey, accGroup);
                          }

                          // Inline accumulation
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
                                  else
                                    cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
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
                      }
                    } catch (IOException e) {
                      throw new java.io.UncheckedIOException(e);
                    }
                    return workerMap;
                  },
                  PARALLEL_POOL);
        }

        // Wait for all workers, merge maps
        java.util.concurrent.CompletableFuture.allOf(futures).join();
        // Use largest worker map as base to avoid copying
        Map<SegmentGroupKey, AccumulatorGroup>[] workerResults = new Map[numWorkers];
        int largestIdx = 0;
        for (int i = 0; i < numWorkers; i++) {
          workerResults[i] = futures[i].join();
          futures[i] = null; // release future for GC
          if (workerResults[i].size() > workerResults[largestIdx].size()) largestIdx = i;
        }
        Map<SegmentGroupKey, AccumulatorGroup> globalGroups = workerResults[largestIdx];
        for (int i = 0; i < numWorkers; i++) {
          if (i == largestIdx) continue;
          mergeGroupMaps(globalGroups, workerResults[i]);
          workerResults[i] = null; // release merged worker map for GC
        }

        if (globalGroups.isEmpty()) {
          return List.of();
        }

        // Build output: resolve global ordinals to strings for VARCHAR keys
        int numGroupKeys = groupByKeys.size();
        int totalColumns = numGroupKeys + numAggs;

        // Prepare segment DVs for global ordinal lookup
        SortedSetDocValues[][] segDvsForLookup = new SortedSetDocValues[numKeys][];
        for (int k = 0; k < numKeys; k++) {
          if (keyInfos.get(k).isVarchar) {
            segDvsForLookup[k] = new SortedSetDocValues[globalOrdLeaves.size()];
            for (int s = 0; s < globalOrdLeaves.size(); s++) {
              segDvsForLookup[k][s] =
                  globalOrdLeaves.get(s).reader().getSortedSetDocValues(keyInfos.get(k).name);
            }
          }
        }

        // Top-N selection
        java.util.Collection<Map.Entry<SegmentGroupKey, AccumulatorGroup>> outputEntries;
        if (sortAggIndex >= 0 && topN > 0 && topN < globalGroups.size()) {
          int n = (int) Math.min(topN, globalGroups.size());
          @SuppressWarnings("unchecked")
          Map.Entry<SegmentGroupKey, AccumulatorGroup>[] heap = new Map.Entry[n];
          int heapSize = 0;
          for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
            long val = entry.getValue().accumulators[sortAggIndex].getSortValue();
            if (heapSize < n) {
              heap[heapSize++] = entry;
              int ki = heapSize - 1;
              while (ki > 0) {
                int parent = (ki - 1) >>> 1;
                long pVal = heap[parent].getValue().accumulators[sortAggIndex].getSortValue();
                long kVal = heap[ki].getValue().accumulators[sortAggIndex].getSortValue();
                boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
                if (swap) {
                  var tmp = heap[parent];
                  heap[parent] = heap[ki];
                  heap[ki] = tmp;
                  ki = parent;
                } else break;
              }
            } else {
              long rootVal = heap[0].getValue().accumulators[sortAggIndex].getSortValue();
              boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
              if (better) {
                heap[0] = entry;
                int ki = 0;
                while (true) {
                  int left = 2 * ki + 1;
                  if (left >= heapSize) break;
                  int right = left + 1;
                  int target = left;
                  if (right < heapSize) {
                    long lv = heap[left].getValue().accumulators[sortAggIndex].getSortValue();
                    long rv = heap[right].getValue().accumulators[sortAggIndex].getSortValue();
                    boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                    if (pickRight) target = right;
                  }
                  long kVal = heap[ki].getValue().accumulators[sortAggIndex].getSortValue();
                  long tVal = heap[target].getValue().accumulators[sortAggIndex].getSortValue();
                  boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
                  if (swap) {
                    var tmp = heap[ki];
                    heap[ki] = heap[target];
                    heap[target] = tmp;
                    ki = target;
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
              resolveAggOutputType(specs.get(i), columnTypeMap)
                  .createBlockBuilder(null, outputCount);
        }

        for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry : outputEntries) {
          SegmentGroupKey sgk = entry.getKey();
          AccumulatorGroup accGroup = entry.getValue();

          for (int k = 0; k < numGroupKeys; k++) {
            if (sgk.nulls[k]) {
              builders[k].appendNull();
            } else if (keyInfos.get(k).isVarchar) {
              BytesRef bytes = lookupGlobalOrd(ordinalMaps[k], segDvsForLookup[k], sgk.values[k]);
              VarcharType.VARCHAR.writeSlice(
                  builders[k], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
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
    }

    // === Multi-segment 2-key global-ordinal flat path (LIMIT without ORDER BY) ===
    // For 2-key GROUP BY (numeric + varchar) with LIMIT but no ORDER BY,
    // use global ordinals + FlatTwoKeyMap with early termination.
    // Stops scanning as soon as enough distinct groups are found.
    // Only activated for LIMIT-without-ORDER-BY since the global ordinal lookup
    // per doc adds overhead that's not justified for full-scan ORDER BY queries.
    if (sortAggIndex < 0
        && topN > 0
        && keyInfos.size() == 2
        && truncUnits[0] == null
        && arithUnits[0] == null
        && truncUnits[1] == null
        && arithUnits[1] == null) {
      boolean canUseFlatGlobalOrd = true;
      // Check: exactly one varchar key, one numeric key, and flat-compatible aggs
      int varcharKeyIdx = -1;
      for (int i = 0; i < 2; i++) {
        if (keyInfos.get(i).isVarchar) varcharKeyIdx = i;
      }
      if (varcharKeyIdx < 0) canUseFlatGlobalOrd = false;
      for (int i = 0; i < numAggs && canUseFlatGlobalOrd; i++) {
        if (!isCountStar[i]) {
          AggSpec spec = specs.get(i);
          if (isVarcharArg[i] || isDoubleArg[i]) {
            canUseFlatGlobalOrd = false;
          } else {
            switch (spec.funcName) {
              case "COUNT":
                if (spec.isDistinct) canUseFlatGlobalOrd = false;
                break;
              case "SUM":
              case "AVG":
                break;
              default:
                canUseFlatGlobalOrd = false;
                break;
            }
          }
        }
      }
      if (canUseFlatGlobalOrd) {
        List<Page> result =
            executeMultiSegGlobalOrdFlatTwoKey(
                engineSearcher,
                query,
                keyInfos,
                specs,
                numAggs,
                isCountStar,
                accType,
                varcharKeyIdx,
                groupByKeys,
                sortAggIndex,
                sortAscending,
                topN,
                columnTypeMap);
        if (result != null) {
          return result;
        }
      }
    }

    // === Parallel multi-segment path (Approach B: doc-range) ===
    if ("docrange".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1) {
      return executeNKeyVarcharParallelDocRange(
          engineSearcher,
          query,
          keyInfos,
          specs,
          numAggs,
          isCountStar,
          isDoubleArg,
          isVarcharArg,
          accType,
          truncUnits,
          arithUnits,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN,
          columnTypeMap);
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

    return buildMergedGroupOutput(
        globalGroups,
        keyInfos,
        specs,
        numAggs,
        groupByKeys,
        sortAggIndex,
        sortAscending,
        topN,
        columnTypeMap);
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

  /** Build output Pages from a MergedGroupKey map with optional top-N selection. */
  private static List<Page> buildMergedGroupOutput(
      Map<MergedGroupKey, AccumulatorGroup> globalGroups,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN,
      Map<String, Type> columnTypeMap) {

    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // If top-N is requested and fewer than total groups, select top-N entries via min-heap.
    java.util.Collection<Map.Entry<MergedGroupKey, AccumulatorGroup>> outputEntries;
    if (sortAggIndex >= 0 && topN > 0 && topN < globalGroups.size()) {
      int n = (int) Math.min(topN, globalGroups.size());
      @SuppressWarnings("unchecked")
      Map.Entry<MergedGroupKey, AccumulatorGroup>[] heap = new Map.Entry[n];
      int heapSize = 0;
      for (Map.Entry<MergedGroupKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
        long val = entry.getValue().accumulators[sortAggIndex].getSortValue();
        if (heapSize < n) {
          heap[heapSize++] = entry;
          int ki = heapSize - 1;
          while (ki > 0) {
            int parent = (ki - 1) >>> 1;
            long pVal = heap[parent].getValue().accumulators[sortAggIndex].getSortValue();
            long kVal = heap[ki].getValue().accumulators[sortAggIndex].getSortValue();
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              var tmp = heap[parent];
              heap[parent] = heap[ki];
              heap[ki] = tmp;
              ki = parent;
            } else break;
          }
        } else {
          long rootVal = heap[0].getValue().accumulators[sortAggIndex].getSortValue();
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = entry;
            int ki = 0;
            while (true) {
              int left = 2 * ki + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = heap[left].getValue().accumulators[sortAggIndex].getSortValue();
                long rv = heap[right].getValue().accumulators[sortAggIndex].getSortValue();
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = heap[ki].getValue().accumulators[sortAggIndex].getSortValue();
              long tVal = heap[target].getValue().accumulators[sortAggIndex].getSortValue();
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                var tmp = heap[ki];
                heap[ki] = heap[target];
                heap[target] = tmp;
                ki = target;
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

    for (Map.Entry<MergedGroupKey, AccumulatorGroup> entry : outputEntries) {
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

  /**
   * Doc-range parallel GROUP BY for the N-key varchar path. For each segment, collects matching doc
   * IDs, then splits across workers.
   */
  private static List<Page> executeNKeyVarcharParallelDocRange(
      org.opensearch.index.engine.Engine.Searcher engineSearcher,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      boolean[] isDoubleArg,
      boolean[] isVarcharArg,
      int[] accType,
      String[] truncUnits,
      String[] arithUnits,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN,
      Map<String, Type> columnTypeMap)
      throws Exception {

    List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
    int numWorkers = Math.min(THREADS_PER_SHARD, Runtime.getRuntime().availableProcessors());
    final int numKeys = keyInfos.size();

    // Create Weight for getting Scorers
    IndexSearcher searcher = new IndexSearcher(engineSearcher.getIndexReader());
    Weight weight =
        searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    // Early termination for LIMIT without ORDER BY: any N groups are valid.
    final boolean earlyTerminate = (sortAggIndex < 0 && topN > 0);
    final java.util.concurrent.atomic.AtomicBoolean stopFlag =
        earlyTerminate ? new java.util.concurrent.atomic.AtomicBoolean(false) : null;

    // Per-worker global maps
    @SuppressWarnings("unchecked")
    Map<MergedGroupKey, AccumulatorGroup>[] workerMaps = new Map[numWorkers];
    for (int i = 0; i < numWorkers; i++) {
      workerMaps[i] = new LinkedHashMap<>();
    }

    // Process each segment: collect matching doc IDs, split among workers
    for (LeafReaderContext leafCtx : leaves) {
      // Early termination: skip remaining segments once we have enough groups
      if (earlyTerminate && stopFlag.get()) break;

      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) continue;

      // Collect all matching doc IDs for this segment
      DocIdSetIterator docIt = scorer.iterator();
      int[] matchedDocs;
      int matchCount = 0;
      {
        int capacity = Math.min(leafCtx.reader().maxDoc(), 65536);
        int[] buf = new int[capacity];
        int d;
        while ((d = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          if (matchCount >= buf.length) {
            buf = java.util.Arrays.copyOf(buf, buf.length * 2);
          }
          buf[matchCount++] = d;
        }
        matchedDocs = buf;
      }

      if (matchCount == 0) continue;

      // Split doc IDs among workers
      int chunkSize = (matchCount + numWorkers - 1) / numWorkers;
      int actualWorkers = Math.min(numWorkers, (matchCount + chunkSize - 1) / chunkSize);

      @SuppressWarnings("unchecked")
      java.util.concurrent.CompletableFuture<Void>[] segFutures =
          new java.util.concurrent.CompletableFuture[actualWorkers];

      for (int w = 0; w < actualWorkers; w++) {
        final int startIdx = w * chunkSize;
        final int endIdx = Math.min(startIdx + chunkSize, matchCount);
        final int workerIdx = w;
        final int[] docs = matchedDocs;
        final LeafReaderContext ctx = leafCtx;

        segFutures[w] =
            java.util.concurrent.CompletableFuture.runAsync(
                () -> {
                  try {
                    // Open key DocValues (each worker needs its own instances)
                    Object[] keyReaders = new Object[numKeys];
                    for (int i = 0; i < numKeys; i++) {
                      KeyInfo ki = keyInfos.get(i);
                      if (ki.isVarchar) {
                        keyReaders[i] = ctx.reader().getSortedSetDocValues(ki.name);
                      } else {
                        keyReaders[i] = ctx.reader().getSortedNumericDocValues(ki.name);
                      }
                    }

                    // Open agg DocValues
                    SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                    SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                    for (int i = 0; i < numAggs; i++) {
                      if (!isCountStar[i]) {
                        AggSpec spec = specs.get(i);
                        if (isVarcharArg[i]) {
                          varcharAggDvs[i] = ctx.reader().getSortedSetDocValues(spec.arg);
                        } else {
                          numericAggDvs[i] = ctx.reader().getSortedNumericDocValues(spec.arg);
                        }
                      }
                    }

                    Map<SegmentGroupKey, AccumulatorGroup> segmentGroups = new HashMap<>();
                    NumericProbeKey probeKey = new NumericProbeKey(numKeys);
                    // For early termination: cap per-worker segment groups
                    final int groupCap = earlyTerminate ? (int) topN : Integer.MAX_VALUE;

                    for (int idx = startIdx; idx < endIdx; idx++) {
                      // Check shared stop flag periodically (every 4096 docs) to exit early
                      if (earlyTerminate && (idx & 0xFFF) == 0 && stopFlag.get()) break;

                      int doc = docs[idx];
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
                        // Early termination: stop scanning once we have enough groups.
                        // Without ORDER BY, any N groups are valid — no need to continue
                        // accumulating counts for existing groups either.
                        if (segmentGroups.size() >= groupCap) break;
                        SegmentGroupKey immutableKey = probeKey.toImmutableKey();
                        accGroup = createAccumulatorGroup(specs);
                        segmentGroups.put(immutableKey, accGroup);
                      }

                      // Inline accumulation
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
                                double dbl = Double.longBitsToDouble(rawVal);
                                if (dbl < mna.doubleVal) mna.doubleVal = dbl;
                              } else {
                                if (rawVal < mna.longVal) mna.longVal = rawVal;
                              }
                              break;
                            case 4:
                              MaxAccum mxa = (MaxAccum) acc;
                              mxa.hasValue = true;
                              if (isDoubleArg[i]) {
                                double dbl = Double.longBitsToDouble(rawVal);
                                if (dbl > mxa.doubleVal) mxa.doubleVal = dbl;
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

                    // Resolve ordinals and merge into worker's global map
                    for (Map.Entry<SegmentGroupKey, AccumulatorGroup> entry :
                        segmentGroups.entrySet()) {
                      SegmentGroupKey sgk = entry.getKey();
                      AccumulatorGroup segAccs = entry.getValue();

                      Object[] resolvedKeys = new Object[numKeys];
                      for (int k = 0; k < numKeys; k++) {
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
                      Map<MergedGroupKey, AccumulatorGroup> wMap = workerMaps[workerIdx];
                      AccumulatorGroup existing = wMap.get(mgk);
                      if (existing == null) {
                        wMap.put(mgk, segAccs);
                      } else {
                        existing.merge(segAccs);
                      }
                    }
                  } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                  }
                },
                PARALLEL_POOL);
      }

      // Wait for this segment's workers to finish before moving to next segment
      java.util.concurrent.CompletableFuture.allOf(segFutures).join();

      // Early termination: check if we have enough groups across all workers
      if (earlyTerminate) {
        int totalGroups = 0;
        for (Map<MergedGroupKey, AccumulatorGroup> wMap : workerMaps) {
          totalGroups += wMap.size();
        }
        if (totalGroups >= topN) {
          stopFlag.set(true);
        }
      }
    }

    // Merge all worker maps
    Map<MergedGroupKey, AccumulatorGroup> globalGroups = new LinkedHashMap<>();
    for (Map<MergedGroupKey, AccumulatorGroup> wMap : workerMaps) {
      mergeMergedGroupMaps(globalGroups, wMap);
    }

    if (globalGroups.isEmpty()) {
      return List.of();
    }

    return buildMergedGroupOutput(
        globalGroups,
        keyInfos,
        specs,
        numAggs,
        groupByKeys,
        sortAggIndex,
        sortAscending,
        topN,
        columnTypeMap);
  }

  /**
   * Multi-segment 2-key GROUP BY using global ordinals + FlatTwoKeyMap. For (numeric + varchar) key
   * pairs, converts segment-local ordinals to global ordinals using a cached OrdinalMap, then
   * accumulates into FlatTwoKeyMap per worker. This avoids per-group object allocation AND
   * per-segment ordinal resolution, only resolving ordinals to strings for the final top-N output
   * groups.
   *
   * @return result pages, or null if the path is not applicable (e.g., too many global ordinals)
   */
  private static List<Page> executeMultiSegGlobalOrdFlatTwoKey(
      org.opensearch.index.engine.Engine.Searcher engineSearcher,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int varcharKeyIdx,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN,
      Map<String, Type> columnTypeMap)
      throws Exception {

    List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
    int numericKeyIdx = 1 - varcharKeyIdx;

    // Build global ordinal map for the varchar key
    String varcharField = keyInfos.get(varcharKeyIdx).name;
    OrdinalMap ordinalMap = buildGlobalOrdinalMap(leaves, varcharField);
    if (ordinalMap == null) return null;
    long globalOrdCount = ordinalMap.getValueCount();
    // Safety check: don't use this path if global ordinals are too large
    if (globalOrdCount > 50_000_000) return null;

    // Compute flat accumulator layout
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

    int numWorkers = Math.min(THREADS_PER_SHARD, Runtime.getRuntime().availableProcessors());

    // Per-worker global FlatTwoKeyMaps: key0=numericKey, key1=globalOrd
    // Workers accumulate across all segments into the same map.
    // Early termination for LIMIT without ORDER BY
    final boolean earlyTerminate = (sortAggIndex < 0 && topN > 0);
    final int groupCap = earlyTerminate ? (int) topN : -1;

    // === Ultra-fast path: LIMIT without ORDER BY — sequential scan with early stop ===
    // For queries like Q18 (GROUP BY UserID, SearchPhrase LIMIT 10 with no ORDER BY),
    // any N groups are valid output. Scan segments sequentially, stop after finding
    // enough groups. No parallelism overhead, no doc-ID collection.
    if (earlyTerminate) {
      FlatTwoKeyMap flatMap = new FlatTwoKeyMap(flatSlotsPerGroup);
      for (int segIdx = 0; segIdx < leaves.size(); segIdx++) {
        if (flatMap.size >= groupCap) break;
        LeafReaderContext leafCtx = leaves.get(segIdx);
        LeafReader reader = leafCtx.reader();
        int maxDoc = reader.maxDoc();
        Bits liveDocs = reader.getLiveDocs();
        LongValues segToGlobal = ordinalMap.getGlobalOrds(segIdx);

        SortedSetDocValues varcharDv =
            reader.getSortedSetDocValues(keyInfos.get(varcharKeyIdx).name);
        SortedNumericDocValues numericDv =
            reader.getSortedNumericDocValues(keyInfos.get(numericKeyIdx).name);

        // Open aggregate DocValues
        SortedNumericDocValues[] aggDvs = new SortedNumericDocValues[numAggs];
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i]) {
            aggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
          }
        }

        for (int doc = 0; doc < maxDoc; doc++) {
          if (liveDocs != null && !liveDocs.get(doc)) continue;

          long numericKey = 0;
          long globalOrd = 0;
          if (numericDv != null && numericDv.advanceExact(doc)) {
            numericKey = numericDv.nextValue();
          }
          if (varcharDv != null && varcharDv.advanceExact(doc)) {
            long segOrd = varcharDv.nextOrd();
            globalOrd = segToGlobal.get(segOrd);
          }

          int slot = flatMap.findOrInsertCapped(numericKey, globalOrd, groupCap);
          if (slot < 0) break; // cap reached
          int base = slot * flatSlotsPerGroup;

          for (int i = 0; i < numAggs; i++) {
            if (isCountStar[i]) {
              flatMap.accData[base + flatAccOffset[i]]++;
            } else {
              SortedNumericDocValues aggDv = aggDvs[i];
              if (aggDv != null && aggDv.advanceExact(doc)) {
                long rawVal = aggDv.nextValue();
                switch (accType[i]) {
                  case 0:
                    flatMap.accData[base + flatAccOffset[i]]++;
                    break;
                  case 1:
                    flatMap.accData[base + flatAccOffset[i]] += rawVal;
                    break;
                  case 2:
                    flatMap.accData[base + flatAccOffset[i]] += rawVal;
                    flatMap.accData[base + flatAccOffset[i] + 1]++;
                    break;
                }
              }
            }
          }
        }
      }

      if (flatMap.size == 0) return List.of();

      // Build output: resolve global ordinals only for the found groups
      SortedSetDocValues[] segDvsEarly = new SortedSetDocValues[leaves.size()];
      for (int si = 0; si < leaves.size(); si++) {
        segDvsEarly[si] = leaves.get(si).reader().getSortedSetDocValues(varcharField);
      }
      int numGroupKeysEarly = groupByKeys.size();
      int totalColumnsEarly = numGroupKeysEarly + numAggs;
      int outputCountEarly = Math.min(flatMap.size, groupCap);

      BlockBuilder[] bldrs = new BlockBuilder[totalColumnsEarly];
      for (int i = 0; i < numGroupKeysEarly; i++) {
        KeyInfo ki = keyInfos.get(i);
        bldrs[i] =
            ki.isVarchar
                ? VarcharType.VARCHAR.createBlockBuilder(null, outputCountEarly)
                : BigintType.BIGINT.createBlockBuilder(null, outputCountEarly);
      }
      for (int i = 0; i < numAggs; i++) {
        bldrs[numGroupKeysEarly + i] = BigintType.BIGINT.createBlockBuilder(null, outputCountEarly);
      }

      int written = 0;
      for (int slot = 0; slot < flatMap.capacity && written < outputCountEarly; slot++) {
        if (flatMap.keys0[slot] == FlatTwoKeyMap.EMPTY_KEY) continue;
        long numericKey = flatMap.keys0[slot];
        long gOrd = flatMap.keys1[slot];
        int base = slot * flatSlotsPerGroup;

        BigintType.BIGINT.writeLong(bldrs[numericKeyIdx], numericKey);
        BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvsEarly, gOrd);
        if (bytes != null) {
          VarcharType.VARCHAR.writeSlice(
              bldrs[varcharKeyIdx], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
        } else {
          VarcharType.VARCHAR.writeSlice(bldrs[varcharKeyIdx], Slices.EMPTY_SLICE);
        }
        for (int i = 0; i < numAggs; i++) {
          if (accType[i] == 2) {
            long sum = flatMap.accData[base + flatAccOffset[i]];
            long count = flatMap.accData[base + flatAccOffset[i] + 1];
            if (count > 0) {
              BigintType.BIGINT.writeLong(
                  bldrs[numGroupKeysEarly + i], Double.doubleToLongBits((double) sum / count));
            } else {
              bldrs[numGroupKeysEarly + i].appendNull();
            }
          } else {
            BigintType.BIGINT.writeLong(
                bldrs[numGroupKeysEarly + i], flatMap.accData[base + flatAccOffset[i]]);
          }
        }
        written++;
      }

      Block[] blks = new Block[totalColumnsEarly];
      for (int i = 0; i < totalColumnsEarly; i++) blks[i] = bldrs[i].build();
      return List.of(new Page(blks));
    }

    final java.util.concurrent.atomic.AtomicBoolean stopFlag = null;

    FlatTwoKeyMap[] workerMaps = new FlatTwoKeyMap[numWorkers];
    for (int i = 0; i < numWorkers; i++) {
      workerMaps[i] = new FlatTwoKeyMap(flatSlotsPerGroup);
    }

    // Create Weight for getting Scorers
    IndexSearcher searcher = new IndexSearcher(engineSearcher.getIndexReader());
    Weight weight =
        searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    // Process each segment: collect matching doc IDs, split among workers
    for (int segIdx = 0; segIdx < leaves.size(); segIdx++) {
      // Early termination: skip remaining segments once we have enough groups
      if (earlyTerminate && stopFlag.get()) break;

      LeafReaderContext leafCtx = leaves.get(segIdx);
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) continue;

      // Get segment-to-global ordinal mapping for this segment
      final LongValues segToGlobal = ordinalMap.getGlobalOrds(segIdx);

      // Collect all matching doc IDs for this segment
      DocIdSetIterator docIt = scorer.iterator();
      int matchCount = 0;
      int cap = Math.min(leafCtx.reader().maxDoc(), 65536);
      int[] buf = new int[cap];
      int d;
      while ((d = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (matchCount >= buf.length) {
          buf = java.util.Arrays.copyOf(buf, buf.length * 2);
        }
        buf[matchCount++] = d;
      }
      final int[] matchedDocs = buf;
      final int segMatchCount = matchCount;

      if (segMatchCount == 0) continue;

      // Split doc IDs among workers
      int chunkSize = (segMatchCount + numWorkers - 1) / numWorkers;
      int actualWorkers = Math.min(numWorkers, (segMatchCount + chunkSize - 1) / chunkSize);

      @SuppressWarnings("unchecked")
      java.util.concurrent.CompletableFuture<Void>[] segFutures =
          new java.util.concurrent.CompletableFuture[actualWorkers];

      for (int w = 0; w < actualWorkers; w++) {
        final int startIdx = w * chunkSize;
        final int endIdx = Math.min(startIdx + chunkSize, segMatchCount);
        final int workerIdx = w;
        final LeafReaderContext ctx = leafCtx;

        segFutures[w] =
            java.util.concurrent.CompletableFuture.runAsync(
                () -> {
                  try {
                    // Open key DocValues for this worker
                    SortedSetDocValues varcharDv =
                        ctx.reader().getSortedSetDocValues(keyInfos.get(varcharKeyIdx).name);
                    SortedNumericDocValues numericDv =
                        ctx.reader().getSortedNumericDocValues(keyInfos.get(numericKeyIdx).name);

                    // Open aggregate DocValues
                    SortedNumericDocValues[] aggDvs = new SortedNumericDocValues[numAggs];
                    for (int i = 0; i < numAggs; i++) {
                      if (!isCountStar[i]) {
                        aggDvs[i] = ctx.reader().getSortedNumericDocValues(specs.get(i).arg);
                      }
                    }

                    FlatTwoKeyMap flatMap = workerMaps[workerIdx];

                    // Initialize DV positions for forward-only advance
                    int numDvPos = (numericDv != null) ? numericDv.nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
                    int varDvPos = (varcharDv != null) ? varcharDv.nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
                    int[] aggDvPos = new int[numAggs];
                    for (int i = 0; i < numAggs; i++) {
                      aggDvPos[i] = (!isCountStar[i] && aggDvs[i] != null)
                          ? aggDvs[i].nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
                    }

                    for (int idx = startIdx; idx < endIdx; idx++) {
                      // Check stop flag periodically
                      if (earlyTerminate && (idx & 0xFFF) == 0 && stopFlag.get()) break;

                      int doc = matchedDocs[idx];
                      long numericKey = 0;
                      long globalOrd = 0;

                      // Forward-advance numeric key DV
                      if (numDvPos != DocIdSetIterator.NO_MORE_DOCS && numDvPos < doc) {
                        numDvPos = numericDv.advance(doc);
                      }
                      if (numDvPos == doc) {
                        numericKey = numericDv.nextValue();
                        numDvPos = numericDv.nextDoc();
                      }
                      // Forward-advance varchar key DV
                      if (varDvPos != DocIdSetIterator.NO_MORE_DOCS && varDvPos < doc) {
                        varDvPos = varcharDv.advance(doc);
                      }
                      if (varDvPos == doc) {
                        long segOrd = varcharDv.nextOrd();
                        globalOrd = segToGlobal.get(segOrd);
                        varDvPos = varcharDv.nextDoc();
                      }

                      // FlatTwoKeyMap keys: key0=numericKey, key1=globalOrd
                      int slot;
                      if (earlyTerminate) {
                        slot = flatMap.findOrInsertCapped(numericKey, globalOrd, groupCap);
                        if (slot < 0) break; // cap reached — stop scanning
                      } else {
                        slot = flatMap.findOrInsert(numericKey, globalOrd);
                      }
                      int base = slot * flatSlotsPerGroup;

                      // Inline accumulation with forward-only DV advance
                      for (int i = 0; i < numAggs; i++) {
                        if (isCountStar[i]) {
                          flatMap.accData[base + flatAccOffset[i]]++;
                        } else {
                          if (aggDvPos[i] != DocIdSetIterator.NO_MORE_DOCS && aggDvPos[i] < doc) {
                            aggDvPos[i] = aggDvs[i].advance(doc);
                          }
                          if (aggDvPos[i] == doc) {
                            long rawVal = aggDvs[i].nextValue();
                            aggDvPos[i] = aggDvs[i].nextDoc();
                            switch (accType[i]) {
                              case 0: // COUNT non-star
                                flatMap.accData[base + flatAccOffset[i]]++;
                                break;
                              case 1: // SUM long
                                flatMap.accData[base + flatAccOffset[i]] += rawVal;
                                break;
                              case 2: // AVG
                                flatMap.accData[base + flatAccOffset[i]] += rawVal;
                                flatMap.accData[base + flatAccOffset[i] + 1]++;
                                break;
                            }
                          }
                        }
                      }
                    }
                  } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                  }
                },
                PARALLEL_POOL);
      }

      java.util.concurrent.CompletableFuture.allOf(segFutures).join();

      // Check if any worker has enough groups for early termination
      if (earlyTerminate && !stopFlag.get()) {
        for (FlatTwoKeyMap wMap : workerMaps) {
          if (wMap.size >= groupCap) {
            stopFlag.set(true);
            break;
          }
        }
      }
    }

    // Merge all worker FlatTwoKeyMaps into a single map
    // Use workerMaps[0] as the base and merge others into it
    FlatTwoKeyMap merged = workerMaps[0];
    for (int w = 1; w < numWorkers; w++) {
      FlatTwoKeyMap other = workerMaps[w];
      for (int slot = 0; slot < other.capacity; slot++) {
        if (other.keys0[slot] == FlatTwoKeyMap.EMPTY_KEY) continue;
        int mSlot = merged.findOrInsert(other.keys0[slot], other.keys1[slot]);
        int mBase = mSlot * flatSlotsPerGroup;
        int oBase = slot * flatSlotsPerGroup;
        for (int s = 0; s < flatSlotsPerGroup; s++) {
          merged.accData[mBase + s] += other.accData[oBase + s];
        }
      }
    }

    if (merged.size == 0) {
      return List.of();
    }

    // Apply TopN if needed: iterate merged map, keep top-N by sort agg
    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Collect slots, optionally applying TopN
    int[] outputSlots;
    int outputCount;
    if (sortAggIndex >= 0 && topN > 0 && topN < merged.size) {
      int n = (int) topN;
      int[] heapSlots = new int[n];
      int heapSize = 0;
      int sai = flatAccOffset[sortAggIndex];
      for (int slot = 0; slot < merged.capacity; slot++) {
        if (merged.keys0[slot] == FlatTwoKeyMap.EMPTY_KEY) continue;
        long val = merged.accData[slot * flatSlotsPerGroup + sai];
        if (heapSize < n) {
          heapSlots[heapSize++] = slot;
          // Sift up (min-heap for DESC, max-heap for ASC)
          int ki = heapSize - 1;
          while (ki > 0) {
            int parent = (ki - 1) >>> 1;
            long pVal = merged.accData[heapSlots[parent] * flatSlotsPerGroup + sai];
            long kVal = merged.accData[heapSlots[ki] * flatSlotsPerGroup + sai];
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              int tmp = heapSlots[parent];
              heapSlots[parent] = heapSlots[ki];
              heapSlots[ki] = tmp;
              ki = parent;
            } else break;
          }
        } else {
          long heapTopVal = merged.accData[heapSlots[0] * flatSlotsPerGroup + sai];
          boolean replace = sortAscending ? (val < heapTopVal) : (val > heapTopVal);
          if (replace) {
            heapSlots[0] = slot;
            // Sift down
            int parent = 0;
            while (true) {
              int left = 2 * parent + 1;
              int right = left + 1;
              int target = parent;
              long targetVal = merged.accData[heapSlots[target] * flatSlotsPerGroup + sai];
              if (left < n) {
                long lVal = merged.accData[heapSlots[left] * flatSlotsPerGroup + sai];
                boolean better = sortAscending ? (lVal > targetVal) : (lVal < targetVal);
                if (better) {
                  target = left;
                  targetVal = lVal;
                }
              }
              if (right < n) {
                long rVal = merged.accData[heapSlots[right] * flatSlotsPerGroup + sai];
                boolean better = sortAscending ? (rVal > targetVal) : (rVal < targetVal);
                if (better) {
                  target = right;
                }
              }
              if (target == parent) break;
              int tmp = heapSlots[parent];
              heapSlots[parent] = heapSlots[target];
              heapSlots[target] = tmp;
              parent = target;
            }
          }
        }
      }
      // Sort heap slots by value for output
      final int fsai = sai;
      final FlatTwoKeyMap fm = merged;
      Integer[] sortableSlots = new Integer[heapSize];
      for (int i = 0; i < heapSize; i++) sortableSlots[i] = heapSlots[i];
      java.util.Arrays.sort(
          sortableSlots,
          (a, b) -> {
            long av = fm.accData[a * flatSlotsPerGroup + fsai];
            long bv = fm.accData[b * flatSlotsPerGroup + fsai];
            return sortAscending ? Long.compare(av, bv) : Long.compare(bv, av);
          });
      outputSlots = new int[heapSize];
      for (int i = 0; i < heapSize; i++) outputSlots[i] = sortableSlots[i];
      outputCount = heapSize;
    } else {
      // Collect all occupied slots
      outputSlots = new int[merged.size];
      int idx = 0;
      for (int slot = 0; slot < merged.capacity; slot++) {
        if (merged.keys0[slot] != FlatTwoKeyMap.EMPTY_KEY) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    }

    // Build Page: resolve global ordinals to strings only for output groups
    // Get segment DocValues for ordinal lookup
    SortedSetDocValues[] segDvs = new SortedSetDocValues[leaves.size()];
    for (int si = 0; si < leaves.size(); si++) {
      segDvs[si] = leaves.get(si).reader().getSortedSetDocValues(varcharField);
    }

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      KeyInfo ki = keyInfos.get(i);
      if (ki.isVarchar) {
        builders[i] = VarcharType.VARCHAR.createBlockBuilder(null, outputCount);
      } else {
        builders[i] = BigintType.BIGINT.createBlockBuilder(null, outputCount);
      }
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] = BigintType.BIGINT.createBlockBuilder(null, outputCount);
    }

    for (int oi = 0; oi < outputCount; oi++) {
      int slot = outputSlots[oi];
      long numericKey = merged.keys0[slot]; // key0 = numericKey
      long globalOrd = merged.keys1[slot]; // key1 = globalOrd

      // Write numeric key
      BigintType.BIGINT.writeLong(builders[numericKeyIdx], numericKey);

      // Write varchar key: resolve global ordinal to bytes
      BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, globalOrd);
      if (bytes != null) {
        VarcharType.VARCHAR.writeSlice(
            builders[varcharKeyIdx], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
      } else {
        VarcharType.VARCHAR.writeSlice(builders[varcharKeyIdx], Slices.EMPTY_SLICE);
      }

      // Write aggregates
      int base = slot * flatSlotsPerGroup;
      for (int i = 0; i < numAggs; i++) {
        if (accType[i] == 2) { // AVG
          long sum = merged.accData[base + flatAccOffset[i]];
          long count = merged.accData[base + flatAccOffset[i] + 1];
          if (count > 0) {
            double avg = (double) sum / count;
            BigintType.BIGINT.writeLong(builders[numGroupKeys + i], Double.doubleToLongBits(avg));
          } else {
            builders[numGroupKeys + i].appendNull();
          }
        } else {
          BigintType.BIGINT.writeLong(
              builders[numGroupKeys + i], merged.accData[base + flatAccOffset[i]]);
        }
      }
    }

    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }

  /**
   * Merge a source SegmentGroupKey→AccumulatorGroup map into a target map. For matching keys,
   * accumulators are merged in place.
   */
  private static void mergeGroupMaps(
      Map<SegmentGroupKey, AccumulatorGroup> target,
      Map<SegmentGroupKey, AccumulatorGroup> source) {
    for (Map.Entry<SegmentGroupKey, AccumulatorGroup> e : source.entrySet()) {
      AccumulatorGroup existing = target.get(e.getKey());
      if (existing == null) {
        target.put(e.getKey(), e.getValue());
      } else {
        existing.merge(e.getValue());
      }
    }
  }

  /**
   * Merge a source MergedGroupKey→AccumulatorGroup map into a target map. For matching keys,
   * accumulators are merged in place.
   */
  private static void mergeMergedGroupMaps(
      Map<MergedGroupKey, AccumulatorGroup> target, Map<MergedGroupKey, AccumulatorGroup> source) {
    for (Map.Entry<MergedGroupKey, AccumulatorGroup> e : source.entrySet()) {
      AccumulatorGroup existing = target.get(e.getKey());
      if (existing == null) {
        target.put(e.getKey(), e.getValue());
      } else {
        existing.merge(e.getValue());
      }
    }
  }

  /**
   * Build a Lucene OrdinalMap for a VARCHAR field across all segments. Maps segment-local ordinals
   * to a global ordinal space.
   */
  // Cache for OrdinalMaps keyed by (IndexReader CacheKey + field name).
  // Uses IndexReader.CacheHelper.getKey() for collision-free identity, and registers
  // a close listener to evict entries when the IndexReader is closed/refreshed.
  // Bounded to 64 entries as a safety net against unbounded growth.
  private static final java.util.concurrent.ConcurrentHashMap<Object, OrdinalMap>
      ORDINAL_MAP_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

  public static OrdinalMap buildGlobalOrdinalMap(List<LeafReaderContext> leaves, String fieldName)
      throws IOException {
    // Use the top-level IndexReader's CacheHelper key for collision-free identity.
    // LeafReaders don't change within a DirectoryReader's lifetime.
    org.apache.lucene.index.IndexReader topReader = leaves.get(0).parent.reader();
    org.apache.lucene.index.IndexReader.CacheHelper cacheHelper = topReader.getReaderCacheHelper();
    if (cacheHelper == null) {
      // No cache helper available — build without caching
      return buildOrdinalMapUncached(leaves, fieldName);
    }
    Object readerKey = cacheHelper.getKey();
    // Composite key: readerKey + fieldName
    String cacheKey =
        System.identityHashCode(readerKey) + ":" + readerKey.hashCode() + ":" + fieldName;

    OrdinalMap cached = ORDINAL_MAP_CACHE.get(cacheKey);
    if (cached != null) return cached;

    OrdinalMap map = buildOrdinalMapUncached(leaves, fieldName);
    if (map == null) return null;

    // Evict on reader close to prevent memory leaks
    ORDINAL_MAP_CACHE.put(cacheKey, map);
    try {
      cacheHelper.addClosedListener(
          key -> {
            ORDINAL_MAP_CACHE
                .entrySet()
                .removeIf(
                    e ->
                        e.getKey()
                            .toString()
                            .startsWith(System.identityHashCode(key) + ":" + key.hashCode() + ":"));
          });
    } catch (Exception ignored) {
      // Listener already registered or reader closed — cache entry will be orphaned
      // but bounded by size limit below
    }

    // Safety: bound cache size (shouldn't happen with close listener, but just in case)
    if (ORDINAL_MAP_CACHE.size() > 64) {
      ORDINAL_MAP_CACHE.clear();
    }
    return map;
  }

  private static OrdinalMap buildOrdinalMapUncached(
      List<LeafReaderContext> leaves, String fieldName) throws IOException {
    SortedSetDocValues[] subs = new SortedSetDocValues[leaves.size()];
    for (int i = 0; i < leaves.size(); i++) {
      subs[i] = leaves.get(i).reader().getSortedSetDocValues(fieldName);
      if (subs[i] == null) return null;
    }
    return OrdinalMap.build(null, subs, PackedInts.DEFAULT);
  }

  /** Look up the term bytes for a global ordinal using OrdinalMap reverse mapping. */
  private static BytesRef lookupGlobalOrd(
      OrdinalMap ordinalMap, SortedSetDocValues[] segmentDvs, long globalOrd) throws IOException {
    int segIdx = ordinalMap.getFirstSegmentNumber(globalOrd);
    long segOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
    if (segmentDvs[segIdx] == null) return new BytesRef("");
    return segmentDvs[segIdx].lookupOrd(segOrd);
  }

  // =========================================================================
  // Internal structures
  // =========================================================================

  private static final class AggSpec {
    final String funcName;
    final boolean isDistinct;
    final String arg;
    final Type argType;
    boolean applyLength; // length(col) inline computation flag

    AggSpec(String funcName, boolean isDistinct, String arg, Type argType) {
      this.funcName = funcName;
      this.isDistinct = isDistinct;
      this.arg = arg;
      this.argType = argType;
    }
  }

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

    MergedGroupKey(Object[] values, int precomputedHash) {
      this.values = values;
      this.hash = precomputedHash;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj instanceof MergedGroupKey other) {
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
      if (obj instanceof ProbeGroupKey probe) {
        if (this.hash != probe.hash || this.values.length != probe.size) return false;
        for (int i = 0; i < values.length; i++) {
          if (values[i] == null) {
            if (probe.values[i] != null) return false;
          } else if (!values[i].equals(probe.values[i])) {
            return false;
          }
        }
        return true;
      }
      return false;
    }
  }

  /**
   * Mutable group key for probe-first HashMap lookups. Avoids allocating a new Object[] and
   * MergedGroupKey for every row — only creates an immutable copy when the key is not found in the
   * map (cache miss). For duplicate rows (~68% of Q40 traffic), this eliminates two object
   * allocations per row.
   */
  private static final class ProbeGroupKey {
    Object[] values;
    int hash;
    final int size;

    ProbeGroupKey(int numKeys) {
      this.values = new Object[numKeys];
      this.size = numKeys;
    }

    void computeHash() {
      int h = 1;
      for (int i = 0; i < size; i++) {
        Object v = values[i];
        h = 31 * h + (v == null ? 0 : v.hashCode());
      }
      this.hash = h;
    }

    /** Create an immutable snapshot for insertion into the map. */
    MergedGroupKey snapshot() {
      Object[] copy = new Object[size];
      System.arraycopy(values, 0, copy, 0, size);
      return new MergedGroupKey(copy, hash);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj instanceof MergedGroupKey other) {
        if (this.hash != other.hash || this.size != other.values.length) return false;
        for (int i = 0; i < size; i++) {
          if (values[i] == null) {
            if (other.values[i] != null) return false;
          } else if (!values[i].equals(other.values[i])) {
            return false;
          }
        }
        return true;
      }
      return false;
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
    private static final int MAX_CAPACITY = 16_000_000;

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
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
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
    private static final int MAX_CAPACITY = 16_000_000;

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
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
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
    private static final int MAX_CAPACITY = 16_000_000;
    static final long EMPTY_KEY = Long.MIN_VALUE;

    long[] keys0;
    long[] keys1;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    FlatTwoKeyMap(int slotsPerGroup) {
      this(slotsPerGroup, INITIAL_CAPACITY);
    }

    FlatTwoKeyMap(int slotsPerGroup, int initialCapacity) {
      this.slotsPerGroup = slotsPerGroup;
      int cap = Integer.highestOneBit(Math.max(initialCapacity, INITIAL_CAPACITY) - 1) << 1;
      if (cap < INITIAL_CAPACITY) cap = INITIAL_CAPACITY;
      this.capacity = cap;
      this.keys0 = new long[capacity];
      Arrays.fill(keys0, EMPTY_KEY);
      this.keys1 = new long[capacity];
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
      while (keys0[h] != EMPTY_KEY) {
        if (keys0[h] == key0 && keys1[h] == key1) {
          return h;
        }
        h = (h + 1) & mask;
      }
      // New entry
      keys0[h] = key0;
      keys1[h] = key1;
      // accData[h*slotsPerGroup..] is already 0 from array init
      size++;
      if (size > threshold) {
        resize();
        // Find the slot in the new layout
        return findExisting(key0, key1);
      }
      return h;
    }

    /**
     * Find existing slot or insert new entry, but refuse new insertions once size >= cap. Returns
     * the slot index if the key already exists or was just inserted; returns -1 if the key is new
     * and the map already has {@code cap} groups.
     *
     * <p>This enables LIMIT-without-ORDER-BY early-close: once we have enough groups, skip new
     * groups entirely, avoiding hash map growth for high-cardinality queries.
     */
    int findOrInsertCapped(long key0, long key1, int cap) {
      int mask = capacity - 1;
      int h = TwoKeyHashMap.hash2(key0, key1) & mask;
      while (keys0[h] != EMPTY_KEY) {
        if (keys0[h] == key0 && keys1[h] == key1) {
          return h;
        }
        h = (h + 1) & mask;
      }
      // New entry -- reject if capped
      if (size >= cap) {
        return -1;
      }
      keys0[h] = key0;
      keys1[h] = key1;
      size++;
      if (size > threshold) {
        resize();
        return findExisting(key0, key1);
      }
      return h;
    }

    private int findExisting(long key0, long key1) {
      int mask = capacity - 1;
      int h = TwoKeyHashMap.hash2(key0, key1) & mask;
      while (keys0[h] != EMPTY_KEY) {
        if (keys0[h] == key0 && keys1[h] == key1) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
      int newCap = capacity * 2;
      long[] nk0 = new long[newCap];
      Arrays.fill(nk0, EMPTY_KEY);
      long[] nk1 = new long[newCap];
      long[] nacc = new long[newCap * slotsPerGroup];
      int nm = newCap - 1;
      for (int s = 0; s < capacity; s++) {
        if (keys0[s] != EMPTY_KEY) {
          int nh = TwoKeyHashMap.hash2(keys0[s], keys1[s]) & nm;
          while (nk0[nh] != EMPTY_KEY) nh = (nh + 1) & nm;
          nk0[nh] = keys0[s];
          nk1[nh] = keys1[s];
          System.arraycopy(accData, s * slotsPerGroup, nacc, nh * slotsPerGroup, slotsPerGroup);
        }
      }
      this.keys0 = nk0;
      this.keys1 = nk1;
      this.accData = nacc;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }

    /** Merge all entries from another FlatTwoKeyMap into this one, summing accumulator slots. */
    void mergeFrom(FlatTwoKeyMap other) {
      for (int s = 0; s < other.capacity; s++) {
        if (other.keys0[s] == EMPTY_KEY) continue;
        int slot = findOrInsert(other.keys0[s], other.keys1[s]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = s * other.slotsPerGroup;
        for (int j = 0; j < slotsPerGroup; j++) {
          accData[dstBase + j] += other.accData[srcBase + j];
        }
      }
    }
  }

  /**
   * Open-addressing hash map with three primitive long keys and contiguous flat accumulator
   * storage, analogous to {@link FlatTwoKeyMap}. Used for 3-key GROUP BY queries (e.g., Q19: GROUP
   * BY UserID, extract(minute FROM EventTime), SearchPhrase) to avoid per-group object allocation
   * (SegmentGroupKey, AccumulatorGroup, MergeableAccumulator).
   */
  private static final class FlatThreeKeyMap {
    private static final int INITIAL_CAPACITY = 8192;
    private static final float LOAD_FACTOR = 0.7f;
    private static final int MAX_CAPACITY = 16_000_000;
    static final long EMPTY_KEY = Long.MIN_VALUE;

    long[] keys0;
    long[] keys1;
    long[] keys2;
    long[] accData;
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    FlatThreeKeyMap(int slotsPerGroup) {
      this.slotsPerGroup = slotsPerGroup;
      this.capacity = INITIAL_CAPACITY;
      this.keys0 = new long[capacity];
      Arrays.fill(keys0, EMPTY_KEY);
      this.keys1 = new long[capacity];
      this.keys2 = new long[capacity];
      this.accData = new long[capacity * slotsPerGroup];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    static int hash3(long k0, long k1, long k2) {
      long h = k0 * 0x9E3779B97F4A7C15L + k1;
      h = h * 0xff51afd7ed558ccdL + k2;
      h ^= h >>> 33;
      h *= 0xff51afd7ed558ccdL;
      h ^= h >>> 33;
      return (int) h;
    }

    int findOrInsert(long key0, long key1, long key2) {
      int mask = capacity - 1;
      int h = hash3(key0, key1, key2) & mask;
      while (keys0[h] != EMPTY_KEY) {
        if (keys0[h] == key0 && keys1[h] == key1 && keys2[h] == key2) {
          return h;
        }
        h = (h + 1) & mask;
      }
      keys0[h] = key0;
      keys1[h] = key1;
      keys2[h] = key2;
      size++;
      if (size > threshold) {
        resize();
        return findExisting(key0, key1, key2);
      }
      return h;
    }

    private int findExisting(long key0, long key1, long key2) {
      int mask = capacity - 1;
      int h = hash3(key0, key1, key2) & mask;
      while (keys0[h] != EMPTY_KEY) {
        if (keys0[h] == key0 && keys1[h] == key1 && keys2[h] == key2) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
      int newCap = capacity * 2;
      long[] nk0 = new long[newCap];
      Arrays.fill(nk0, EMPTY_KEY);
      long[] nk1 = new long[newCap];
      long[] nk2 = new long[newCap];
      long[] nacc = new long[newCap * slotsPerGroup];
      int nm = newCap - 1;
      for (int s = 0; s < capacity; s++) {
        if (keys0[s] != EMPTY_KEY) {
          int nh = hash3(keys0[s], keys1[s], keys2[s]) & nm;
          while (nk0[nh] != EMPTY_KEY) nh = (nh + 1) & nm;
          nk0[nh] = keys0[s];
          nk1[nh] = keys1[s];
          nk2[nh] = keys2[s];
          System.arraycopy(accData, s * slotsPerGroup, nacc, nh * slotsPerGroup, slotsPerGroup);
        }
      }
      this.keys0 = nk0;
      this.keys1 = nk1;
      this.keys2 = nk2;
      this.accData = nacc;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }

    /** Merge all entries from another FlatThreeKeyMap into this one, summing accumulator slots. */
    void mergeFrom(FlatThreeKeyMap other) {
      for (int s = 0; s < other.capacity; s++) {
        if (other.keys0[s] == EMPTY_KEY) continue;
        int slot = findOrInsert(other.keys0[s], other.keys1[s], other.keys2[s]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = s * other.slotsPerGroup;
        for (int j = 0; j < slotsPerGroup; j++) {
          accData[dstBase + j] += other.accData[srcBase + j];
        }
      }
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
    private static final int MAX_CAPACITY = 16_000_000;
    static final long EMPTY_KEY = Long.MIN_VALUE;

    long[] keys;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    FlatSingleKeyMap(int slotsPerGroup) {
      this(slotsPerGroup, INITIAL_CAPACITY);
    }

    FlatSingleKeyMap(int slotsPerGroup, int initialCapacity) {
      this.slotsPerGroup = slotsPerGroup;
      // Round up to next power of 2
      int cap = Integer.highestOneBit(Math.max(initialCapacity, INITIAL_CAPACITY) - 1) << 1;
      if (cap < INITIAL_CAPACITY) cap = INITIAL_CAPACITY;
      this.capacity = cap;
      this.keys = new long[capacity];
      Arrays.fill(keys, EMPTY_KEY);
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
      while (keys[h] != EMPTY_KEY) {
        if (keys[h] == key) {
          return h;
        }
        h = (h + 1) & mask;
      }
      // New entry
      keys[h] = key;
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
      while (keys[h] != EMPTY_KEY) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
      int newCap = capacity * 2;
      long[] nk = new long[newCap];
      Arrays.fill(nk, EMPTY_KEY);
      long[] nacc = new long[newCap * slotsPerGroup];
      int nm = newCap - 1;
      for (int s = 0; s < capacity; s++) {
        if (keys[s] != EMPTY_KEY) {
          int nh = SingleKeyHashMap.hash1(keys[s]) & nm;
          while (nk[nh] != EMPTY_KEY) nh = (nh + 1) & nm;
          nk[nh] = keys[s];
          System.arraycopy(accData, s * slotsPerGroup, nacc, nh * slotsPerGroup, slotsPerGroup);
        }
      }
      this.keys = nk;
      this.accData = nacc;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }

    /** Merge all entries from another FlatSingleKeyMap into this one, adding accData element-wise. */
    void mergeFrom(FlatSingleKeyMap other) {
      for (int s = 0; s < other.capacity; s++) {
        if (other.keys[s] == EMPTY_KEY) continue;
        int slot = findOrInsert(other.keys[s]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = s * other.slotsPerGroup;
        for (int j = 0; j < slotsPerGroup; j++) {
          accData[dstBase + j] += other.accData[srcBase + j];
        }
      }
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

  /**
   * Accumulate a single doc for two-key numeric GROUP BY with pre-computed dispatch flags. Uses
   * switch-based dispatch instead of instanceof chains for better JIT optimization.
   */
  private static void accumulateDocNumeric(
      int doc,
      AccumulatorGroup accGroup,
      int numAggs,
      boolean[] isCountStar,
      boolean[] isDoubleArg,
      int[] accType,
      SortedNumericDocValues[] numericAggDvs)
      throws IOException {
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
      // Start with small capacity (16) to minimize allocation for high-cardinality GROUP BY
      // where most groups have few distinct values. The set resizes dynamically as needed.
      this.longDistinctValues = usePrimitiveLong ? new LongOpenHashSet(16) : null;
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

  /**
   * Scan a doc range [startDoc, endDoc) of a pre-loaded key column for COUNT(*) aggregation.
   * Used by the doc-range parallel path to split a single segment across multiple workers.
   */
  private static void scanDocRangeFlatSingleKeyCountStar(
      long[] keyValues, int startDoc, int endDoc, Bits liveDocs,
      FlatSingleKeyMap flatMap, int accOffset0, int slotsPerGroup) {
    if (liveDocs == null) {
      for (int doc = startDoc; doc < endDoc; doc++) {
        int slot = flatMap.findOrInsert(keyValues[doc]);
        flatMap.accData[slot * slotsPerGroup + accOffset0]++;
      }
    } else {
      for (int doc = startDoc; doc < endDoc; doc++) {
        if (!liveDocs.get(doc)) continue;
        int slot = flatMap.findOrInsert(keyValues[doc]);
        flatMap.accData[slot * slotsPerGroup + accOffset0]++;
      }
    }
  }

  /** Load a numeric DocValues column into a contiguous long[] for fast sequential access. */
  public static long[] loadNumericColumn(LeafReaderContext leafCtx, String fieldName)
      throws IOException {
    int maxDoc = leafCtx.reader().maxDoc();
    long[] values = new long[maxDoc];
    SortedNumericDocValues dv =
        DocValues.getSortedNumeric(leafCtx.reader(), fieldName);
    int doc = dv.nextDoc();
    while (doc != DocIdSetIterator.NO_MORE_DOCS) {
      values[doc] = dv.nextValue();
      doc = dv.nextDoc();
    }
    return values;
  }
}
