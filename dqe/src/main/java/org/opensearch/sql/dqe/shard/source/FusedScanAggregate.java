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
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
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

  /** COUNT(DISTINCT col) accumulator. */
  private static class CountDistinctDirectAccumulator implements DirectAccumulator {
    private final String field;
    private final Type argType;
    private final boolean isVarchar;
    private final boolean isDoubleType;
    private SortedNumericDocValues numericDv;
    private SortedSetDocValues stringDv;
    private final Set<Object> distinctValues = new HashSet<>();

    CountDistinctDirectAccumulator(String field, Type argType) {
      this.field = field;
      this.argType = argType;
      this.isVarchar = argType instanceof VarcharType;
      this.isDoubleType = argType instanceof DoubleType;
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
          distinctValues.add(bytes.utf8ToString());
        }
      } else if (isDoubleType) {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          distinctValues.add(Double.longBitsToDouble(numericDv.nextValue()));
        }
      } else {
        if (numericDv != null && numericDv.advanceExact(doc)) {
          distinctValues.add(numericDv.nextValue());
        }
      }
    }

    @Override
    public Type getOutputType() {
      return BigintType.BIGINT;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, distinctValues.size());
    }
  }
}
