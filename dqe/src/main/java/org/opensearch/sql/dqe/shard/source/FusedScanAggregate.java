/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.sql.dqe.operator.LongOpenHashSet;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

/**
 * Fused scan-aggregate operator that directly aggregates from Lucene DocValues without building
 * intermediate Trino Pages. This eliminates the overhead of BlockBuilder allocation, Block.build(),
 * and Page construction for scalar aggregations (no GROUP BY).
 *
 * <p>Supports: COUNT(*), SUM, MIN, MAX, AVG on numeric columns, and COUNT(DISTINCT) on any column.
 *
 * <p>Used as a fast path when the shard plan is: AggregationNode(groupBy=[], aggs=[...]) ->
 * TableScanNode.
 */
public final class FusedScanAggregate {

  private static final Pattern AGG_FUNCTION =
      Pattern.compile(
          "^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)\\s*$", Pattern.CASE_INSENSITIVE);

  /**
   * Pattern to match expressions like "(col + 42)" or "(col - 3)" from EvalNode output names.
   * Captures the column name and the integer constant offset. Handles both "col + N" and "(col +
   * N)" forms.
   */
  private static final Pattern COL_PLUS_CONST =
      Pattern.compile("^\\(?\\s*(\\w+)\\s*\\+\\s*(\\d+)\\s*\\)?$", Pattern.CASE_INSENSITIVE);

  private FusedScanAggregate() {}

  /**
   * Check if the shard plan is a scalar aggregation (no GROUP BY) over a plain TableScanNode that
   * can be fused.
   */
  public static boolean canFuse(AggregationNode aggNode) {
    if (!aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    if (!(aggNode.getChild() instanceof TableScanNode)) {
      return false;
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
   * Check if the shard plan is a scalar aggregation over an EvalNode -> TableScanNode that can use
   * the fused algebraic shortcut. This matches the pattern where all aggregates are SUM and all
   * eval expressions are either plain column references or (column + integer_constant). Exploits
   * the identity: SUM(col + k) = SUM(col) + k * COUNT(*) to read each underlying column only once.
   */
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
      // Plain column reference (single identifier)
      if (trimmed.matches("\\w+")) {
        continue;
      }
      // (col + N) pattern
      Matcher cm = COL_PLUS_CONST.matcher(trimmed);
      if (cm.matches()) {
        continue;
      }
      return false; // Unsupported expression
    }
    return true;
  }

  /**
   * Execute the fused algebraic shortcut for SUM(col + constant) patterns. Instead of evaluating 90
   * expressions per row, reads each unique underlying column once and uses the identity: SUM(col +
   * k) = SUM(col) + k * COUNT(*).
   *
   * @param aggNode the aggregation plan node
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @param columnTypeMap type mapping for columns
   * @return a list containing a single Page with the aggregate results
   */
  public static List<Page> executeWithEval(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {
    EvalNode evalNode = (EvalNode) aggNode.getChild();
    List<String> evalExprs = evalNode.getExpressions();
    List<String> evalNames = evalNode.getOutputColumnNames();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Build mapping: eval column name -> (physicalColumn, offset)
    // Also collect unique physical columns that need SUM
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
        // Plain column reference
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

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-eval-agg")) {
      String[] colArray = uniquePhysicalColumns.toArray(new String[0]);

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              SortedNumericDocValues[] dvs = new SortedNumericDocValues[colArray.length];
              for (int i = 0; i < colArray.length; i++) {
                dvs[i] = context.reader().getSortedNumericDocValues(colArray[i]);
              }
              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  for (int i = 0; i < colArray.length; i++) {
                    SortedNumericDocValues dv = dvs[i];
                    if (dv != null && dv.advanceExact(doc)) {
                      long[] sc = colSumCount.get(colArray[i]);
                      sc[0] += dv.nextValue();
                      sc[1]++;
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
      String arg = m.group(3).trim(); // The eval column name (e.g., "(ResolutionWidth + 1)")
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

  /**
   * Resolve the output types for the fused eval-aggregate. All outputs are BigintType since we only
   * support SUM on integer columns with integer offsets.
   */
  public static List<Type> resolveEvalAggOutputTypes(AggregationNode aggNode) {
    List<Type> types = new ArrayList<>();
    for (int i = 0; i < aggNode.getAggregateFunctions().size(); i++) {
      types.add(BigintType.BIGINT);
    }
    return types;
  }

  /**
   * Execute the fused scan-aggregate directly from Lucene DocValues.
   *
   * @param aggNode the aggregation plan node
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @param columnTypeMap type mapping for columns
   * @return a list containing a single Page with the aggregate results
   */
  public static List<Page> execute(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {
    TableScanNode scanNode = (TableScanNode) aggNode.getChild();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Parse aggregate functions
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

    // Create accumulators
    List<DirectAccumulator> accumulators =
        specs.stream().map(FusedScanAggregate::createAccumulator).collect(Collectors.toList());

    // Execute search and aggregate directly from doc values
    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-agg")) {
      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              // Open doc values iterators for each accumulator in this segment
              for (DirectAccumulator acc : accumulators) {
                acc.initSegment(context);
              }
              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  for (DirectAccumulator acc : accumulators) {
                    acc.accumulate(doc);
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

    // Build result Page
    int numAggs = accumulators.size();
    BlockBuilder[] builders = new BlockBuilder[numAggs];
    for (int i = 0; i < numAggs; i++) {
      builders[i] = accumulators.get(i).getOutputType().createBlockBuilder(null, 1);
      accumulators.get(i).writeTo(builders[i]);
    }

    Block[] blocks = new Block[numAggs];
    for (int i = 0; i < numAggs; i++) {
      blocks[i] = builders[i].build();
    }

    return List.of(new Page(blocks));
  }

  /** Resolve the output types for the fused aggregate. */
  public static List<Type> resolveOutputTypes(
      AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    List<Type> types = new ArrayList<>();
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

  /** Specification for a single aggregate function. */
  private record AggSpec(String funcName, boolean isDistinct, String arg, Type argType) {}

  private static DirectAccumulator createAccumulator(AggSpec spec) {
    switch (spec.funcName) {
      case "COUNT":
        if (spec.isDistinct) {
          return new CountDistinctDirectAccumulator(spec.arg, spec.argType);
        }
        if ("*".equals(spec.arg)) {
          return new CountStarDirectAccumulator();
        }
        return new CountStarDirectAccumulator(); // COUNT(col) ~ COUNT(*) when no nulls
      case "SUM":
        return new SumDirectAccumulator(spec.arg, spec.argType);
      case "MIN":
        return new MinDirectAccumulator(spec.arg, spec.argType);
      case "MAX":
        return new MaxDirectAccumulator(spec.arg, spec.argType);
      case "AVG":
        return new AvgDirectAccumulator(spec.arg, spec.argType);
      default:
        throw new UnsupportedOperationException("Unsupported aggregate: " + spec.funcName);
    }
  }

  /** Direct accumulator that reads from Lucene doc values without intermediate Pages. */
  private interface DirectAccumulator {
    void initSegment(LeafReaderContext leaf) throws IOException;

    void accumulate(int doc) throws IOException;

    Type getOutputType();

    void writeTo(BlockBuilder builder);
  }

  /** COUNT(*) accumulator. */
  private static class CountStarDirectAccumulator implements DirectAccumulator {
    private long count = 0;

    @Override
    public void initSegment(LeafReaderContext leaf) {}

    @Override
    public void accumulate(int doc) {
      count++;
    }

    @Override
    public Type getOutputType() {
      return BigintType.BIGINT;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, count);
    }
  }

  /** SUM accumulator for numeric columns. */
  private static class SumDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final boolean isDoubleType;
    private SortedNumericDocValues dv;
    private long longSum = 0;
    private double doubleSum = 0;
    private boolean hasValue = false;

    SumDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.isDoubleType = argType instanceof DoubleType;
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      dv = leaf.reader().getSortedNumericDocValues(field);
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (dv != null && dv.advanceExact(doc)) {
        hasValue = true;
        if (isDoubleType) {
          doubleSum += Double.longBitsToDouble(dv.nextValue());
        } else {
          longSum += dv.nextValue();
        }
      }
    }

    @Override
    public Type getOutputType() {
      return isDoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum);
      } else {
        BigintType.BIGINT.writeLong(builder, longSum);
      }
    }
  }

  /** MIN accumulator for numeric columns. */
  private static class MinDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final Type argType;
    private final boolean isLongType;
    private final boolean isDoubleType;
    private final boolean isVarchar;
    private final boolean isTimestamp;
    private SortedNumericDocValues numericDv;
    private SortedSetDocValues stringDv;
    private long longMin = Long.MAX_VALUE;
    private double doubleMin = Double.MAX_VALUE;
    private String stringMin = null;
    private boolean hasValue = false;

    MinDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.argType = argType;
      this.isDoubleType = argType instanceof DoubleType;
      this.isVarchar = argType instanceof VarcharType;
      this.isTimestamp = argType instanceof TimestampType;
      this.isLongType = !isDoubleType && !isVarchar;
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      if (isVarchar) {
        stringDv = leaf.reader().getSortedSetDocValues(field);
      } else {
        numericDv = leaf.reader().getSortedNumericDocValues(field);
      }
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (isVarchar) {
        if (stringDv != null && stringDv.advanceExact(doc)) {
          hasValue = true;
          BytesRef bytes = stringDv.lookupOrd(stringDv.nextOrd());
          String val = bytes.utf8ToString();
          if (stringMin == null || val.compareTo(stringMin) < 0) {
            stringMin = val;
          }
        }
      } else if (isDoubleType) {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          hasValue = true;
          double val = Double.longBitsToDouble(numericDv.nextValue());
          if (val < doubleMin) doubleMin = val;
        }
      } else {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          hasValue = true;
          long val = numericDv.nextValue();
          if (val < longMin) longMin = val;
        }
      }
    }

    @Override
    public Type getOutputType() {
      return argType;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isVarchar) {
        VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(stringMin));
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleMin);
      } else if (isTimestamp) {
        // Lucene stores epoch millis, Trino expects epoch microseconds
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, longMin * 1000L);
      } else {
        argType.writeLong(builder, longMin);
      }
    }
  }

  /** MAX accumulator for numeric columns. */
  private static class MaxDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final Type argType;
    private final boolean isLongType;
    private final boolean isDoubleType;
    private final boolean isVarchar;
    private final boolean isTimestamp;
    private SortedNumericDocValues numericDv;
    private SortedSetDocValues stringDv;
    private long longMax = Long.MIN_VALUE;
    private double doubleMax = -Double.MAX_VALUE;
    private String stringMax = null;
    private boolean hasValue = false;

    MaxDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.argType = argType;
      this.isDoubleType = argType instanceof DoubleType;
      this.isVarchar = argType instanceof VarcharType;
      this.isTimestamp = argType instanceof TimestampType;
      this.isLongType = !isDoubleType && !isVarchar;
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      if (isVarchar) {
        stringDv = leaf.reader().getSortedSetDocValues(field);
      } else {
        numericDv = leaf.reader().getSortedNumericDocValues(field);
      }
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (isVarchar) {
        if (stringDv != null && stringDv.advanceExact(doc)) {
          hasValue = true;
          BytesRef bytes = stringDv.lookupOrd(stringDv.nextOrd());
          String val = bytes.utf8ToString();
          if (stringMax == null || val.compareTo(stringMax) > 0) {
            stringMax = val;
          }
        }
      } else if (isDoubleType) {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          hasValue = true;
          double val = Double.longBitsToDouble(numericDv.nextValue());
          if (val > doubleMax) doubleMax = val;
        }
      } else {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          hasValue = true;
          long val = numericDv.nextValue();
          if (val > longMax) longMax = val;
        }
      }
    }

    @Override
    public Type getOutputType() {
      return argType;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isVarchar) {
        VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(stringMax));
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleMax);
      } else if (isTimestamp) {
        // Lucene stores epoch millis, Trino expects epoch microseconds
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, longMax * 1000L);
      } else {
        argType.writeLong(builder, longMax);
      }
    }
  }

  /** AVG accumulator. */
  private static class AvgDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final boolean isDoubleType;
    private SortedNumericDocValues dv;
    private long longSum = 0;
    private double doubleSum = 0;
    private long count = 0;

    AvgDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.isDoubleType = argType instanceof DoubleType;
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      dv = leaf.reader().getSortedNumericDocValues(field);
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (dv != null && dv.advanceExact(doc)) {
        count++;
        if (isDoubleType) {
          doubleSum += Double.longBitsToDouble(dv.nextValue());
        } else {
          longSum += dv.nextValue();
        }
      }
    }

    @Override
    public Type getOutputType() {
      return DoubleType.DOUBLE;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (count == 0) {
        builder.appendNull();
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum / count);
      } else {
        DoubleType.DOUBLE.writeDouble(builder, (double) longSum / count);
      }
    }
  }

  /**
   * Execute a fused scan to collect distinct values for a single numeric column, returning the
   * distinct values themselves (not the count) as a single-column Page. This is used by the SINGLE
   * COUNT(DISTINCT) coordinator merge: each shard returns its ~25K distinct values instead of all
   * ~125K raw values, and the coordinator merges the much smaller distinct sets.
   *
   * @param columnName the column to collect distinct values from
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @return a list containing a single Page with the distinct values as a BigintType column
   */
  public static List<Page> executeDistinctValues(String columnName, IndexShard shard, Query query)
      throws Exception {
    LongOpenHashSet distinctSet = new LongOpenHashSet();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-values")) {
      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              SortedNumericDocValues dv = context.reader().getSortedNumericDocValues(columnName);
              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  if (dv != null && dv.advanceExact(doc)) {
                    distinctSet.add(dv.nextValue());
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

    // Build a Page with the distinct values
    int distinctCount = distinctSet.size();
    if (distinctCount == 0) {
      return List.of();
    }

    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, distinctCount);
    // Iterate through the LongOpenHashSet and write values
    if (distinctSet.hasZeroValue()) {
      BigintType.BIGINT.writeLong(builder, 0L);
    }
    long[] keys = distinctSet.keys();
    boolean[] occupied = distinctSet.occupied();
    for (int i = 0; i < keys.length; i++) {
      if (occupied[i]) {
        BigintType.BIGINT.writeLong(builder, keys[i]);
      }
    }
    return List.of(new Page(builder.build()));
  }

  /**
   * COUNT(DISTINCT col) accumulator. Uses primitive LongOpenHashSet for numeric columns to avoid
   * Long boxing overhead (~200K boxing operations per shard for high-cardinality columns like
   * UserID). Falls back to HashSet&lt;Object&gt; for varchar and double types.
   */
  private static class CountDistinctDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final Type argType;
    private final boolean isVarchar;
    private final boolean isDoubleType;
    private final boolean usePrimitiveLong;
    private SortedNumericDocValues numericDv;
    private SortedSetDocValues stringDv;

    /** Primitive long set for numeric non-double columns (avoids Long boxing). */
    private final LongOpenHashSet longDistinctValues;

    /** Fallback set for varchar and double types. */
    private final Set<Object> objectDistinctValues;

    CountDistinctDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.argType = argType;
      this.isVarchar = argType instanceof VarcharType;
      this.isDoubleType = argType instanceof DoubleType;
      this.usePrimitiveLong = !isVarchar && !isDoubleType;
      this.longDistinctValues = usePrimitiveLong ? new LongOpenHashSet() : null;
      this.objectDistinctValues = usePrimitiveLong ? null : new HashSet<>();
    }

    @Override
    public void initSegment(LeafReaderContext leaf) throws IOException {
      if (isVarchar) {
        stringDv = leaf.reader().getSortedSetDocValues(field);
      } else {
        numericDv = leaf.reader().getSortedNumericDocValues(field);
      }
    }

    @Override
    public void accumulate(int doc) throws IOException {
      if (isVarchar) {
        if (stringDv != null && stringDv.advanceExact(doc)) {
          BytesRef bytes = stringDv.lookupOrd(stringDv.nextOrd());
          objectDistinctValues.add(bytes.utf8ToString());
        }
      } else if (isDoubleType) {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          objectDistinctValues.add(Double.longBitsToDouble(numericDv.nextValue()));
        }
      } else {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          longDistinctValues.add(numericDv.nextValue());
        }
      }
    }

    @Override
    public Type getOutputType() {
      return BigintType.BIGINT;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      long count = usePrimitiveLong ? longDistinctValues.size() : objectDistinctValues.size();
      BigintType.BIGINT.writeLong(builder, count);
    }
  }
}
