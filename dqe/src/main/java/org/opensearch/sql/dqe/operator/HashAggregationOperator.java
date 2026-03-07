/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.dqe.function.aggregate.AggregateAccumulatorFactory;

/**
 * Physical operator that performs hash-based aggregation. Buffers all input from the child
 * operator, groups rows by key columns, computes aggregates, and emits the result as a single page.
 *
 * <p>Supported aggregate functions: COUNT, SUM, MIN, MAX, AVG.
 */
public class HashAggregationOperator implements Operator {

  private final Operator source;
  private final List<Integer> groupByColumnIndices;
  private final List<AggregateFunction> aggregateFunctions;
  private final List<Type> columnTypes;
  private boolean finished;

  /**
   * Create a HashAggregationOperator.
   *
   * @param source child operator providing input pages
   * @param groupByColumnIndices indices of columns to group by
   * @param aggregateFunctions list of aggregate functions to compute
   * @param columnTypes types of all columns in the input pages
   */
  public HashAggregationOperator(
      Operator source,
      List<Integer> groupByColumnIndices,
      List<AggregateFunction> aggregateFunctions,
      List<Type> columnTypes) {
    this.source = source;
    this.groupByColumnIndices = groupByColumnIndices;
    this.aggregateFunctions = aggregateFunctions;
    this.columnTypes = columnTypes;
    this.finished = false;
  }

  /**
   * Create a HashAggregationOperator using standalone AggregateAccumulatorFactory instances. Each
   * factory specifies the column index it operates on and wraps into the existing AggregateFunction
   * interface.
   *
   * @param source child operator providing input pages
   * @param groupByColumnIndices indices of columns to group by
   * @param accumulatorFactories list of accumulator factories with their column indices
   * @param aggColumnIndices column index for each aggregate (-1 for COUNT(*))
   * @param columnTypes types of all columns in the input pages
   */
  public HashAggregationOperator(
      Operator source,
      List<Integer> groupByColumnIndices,
      List<AggregateAccumulatorFactory> accumulatorFactories,
      List<Integer> aggColumnIndices,
      List<Type> columnTypes) {
    this.source = source;
    this.groupByColumnIndices = groupByColumnIndices;
    this.columnTypes = columnTypes;
    this.finished = false;

    // Wrap each factory into the existing AggregateFunction interface
    this.aggregateFunctions = new ArrayList<>();
    for (int i = 0; i < accumulatorFactories.size(); i++) {
      AggregateAccumulatorFactory factory = accumulatorFactories.get(i);
      int colIdx = aggColumnIndices.get(i);
      this.aggregateFunctions.add(
          new AggregateFunction() {
            @Override
            public Accumulator createAccumulator() {
              return new FactoryAccumulatorAdapter(factory.createAccumulator(), colIdx);
            }

            @Override
            public Type getOutputType() {
              return factory.getOutputType();
            }
          });
    }
  }

  @Override
  public Page processNextBatch() {
    if (finished) {
      return null;
    }
    finished = true;

    // Drain all pages from source and group rows
    Map<List<Object>, List<Accumulator>> groups = new LinkedHashMap<>();

    Page page;
    while ((page = source.processNextBatch()) != null) {
      for (int pos = 0; pos < page.getPositionCount(); pos++) {
        List<Object> groupKey = extractGroupKey(page, pos);

        groups.computeIfAbsent(
            groupKey,
            k -> {
              List<Accumulator> accumulators = new ArrayList<>();
              for (AggregateFunction func : aggregateFunctions) {
                accumulators.add(func.createAccumulator());
              }
              return accumulators;
            });

        List<Accumulator> accumulators = groups.get(groupKey);
        for (int i = 0; i < accumulators.size(); i++) {
          accumulators.get(i).add(page, pos);
        }
      }
    }

    if (groups.isEmpty()) {
      return null;
    }

    // Build result page: group-by columns + aggregate columns
    int totalColumns = groupByColumnIndices.size() + aggregateFunctions.size();
    BlockBuilder[] builders = new BlockBuilder[totalColumns];

    // Create builders for group-by columns
    for (int i = 0; i < groupByColumnIndices.size(); i++) {
      Type type = columnTypes.get(groupByColumnIndices.get(i));
      builders[i] = type.createBlockBuilder(null, groups.size());
    }

    // Create builders for aggregate columns
    for (int i = 0; i < aggregateFunctions.size(); i++) {
      Type outputType = aggregateFunctions.get(i).getOutputType();
      builders[groupByColumnIndices.size() + i] =
          outputType.createBlockBuilder(null, groups.size());
    }

    // Write results
    for (Map.Entry<List<Object>, List<Accumulator>> entry : groups.entrySet()) {
      List<Object> key = entry.getKey();
      List<Accumulator> accumulators = entry.getValue();

      // Write group-by key values
      for (int i = 0; i < key.size(); i++) {
        Type type = columnTypes.get(groupByColumnIndices.get(i));
        writeValue(builders[i], type, key.get(i));
      }

      // Write aggregate results
      for (int i = 0; i < accumulators.size(); i++) {
        accumulators.get(i).writeTo(builders[groupByColumnIndices.size() + i]);
      }
    }

    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  private List<Object> extractGroupKey(Page page, int position) {
    List<Object> key = new ArrayList<>(groupByColumnIndices.size());
    for (int colIdx : groupByColumnIndices) {
      Block block = page.getBlock(colIdx);
      if (block.isNull(position)) {
        key.add(null);
      } else {
        key.add(readValue(block, position, columnTypes.get(colIdx)));
      }
    }
    return key;
  }

  static Object readValue(Block block, int position, Type type) {
    if (block.isNull(position)) {
      return null;
    }
    if (type instanceof BigintType) {
      return BigintType.BIGINT.getLong(block, position);
    } else if (type instanceof IntegerType) {
      return IntegerType.INTEGER.getLong(block, position);
    } else if (type instanceof SmallintType) {
      return (long) SmallintType.SMALLINT.getLong(block, position);
    } else if (type instanceof TinyintType) {
      return (long) TinyintType.TINYINT.getLong(block, position);
    } else if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, position);
    } else if (type instanceof VarcharType) {
      return VarcharType.VARCHAR.getSlice(block, position).toStringUtf8();
    } else if (type instanceof TimestampType) {
      return type.getLong(block, position);
    } else if (type instanceof BooleanType) {
      return BooleanType.BOOLEAN.getBoolean(block, position) ? 1L : 0L;
    }
    // Fallback: try getLong for any other numeric types
    try {
      return type.getLong(block, position);
    } catch (Exception e) {
      throw new UnsupportedOperationException("Unsupported type for aggregation: " + type);
    }
  }

  private static void writeValue(BlockBuilder builder, Type type, Object value) {
    if (value == null) {
      builder.appendNull();
    } else if (type instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, ((Number) value).longValue());
    } else if (type instanceof IntegerType) {
      IntegerType.INTEGER.writeLong(builder, ((Number) value).intValue());
    } else if (type instanceof SmallintType) {
      SmallintType.SMALLINT.writeLong(builder, ((Number) value).shortValue());
    } else if (type instanceof TinyintType) {
      TinyintType.TINYINT.writeLong(builder, ((Number) value).byteValue());
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, ((Number) value).doubleValue());
    } else if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(value.toString()));
    } else if (type instanceof TimestampType) {
      type.writeLong(builder, ((Number) value).longValue());
    } else {
      // Fallback: try writeLong for other numeric types
      try {
        type.writeLong(builder, ((Number) value).longValue());
      } catch (Exception e) {
        throw new UnsupportedOperationException("Unsupported type for aggregation: " + type);
      }
    }
  }

  @Override
  public void close() {
    source.close();
  }

  /** Describes an aggregate function with the column it operates on and its output type. */
  public interface AggregateFunction {
    Accumulator createAccumulator();

    Type getOutputType();
  }

  /** Stateful accumulator that processes row values and produces an aggregate result. */
  public interface Accumulator {
    void add(Page page, int position);

    void writeTo(BlockBuilder builder);
  }

  /** COUNT(*) accumulator. Counts all rows regardless of column values. */
  public static class CountAccumulator implements Accumulator {
    private long count = 0;

    @Override
    public void add(Page page, int position) {
      count++;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, count);
    }
  }

  /** COUNT(DISTINCT column) accumulator. Counts distinct non-null values using a HashSet. */
  public static class CountDistinctAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private final java.util.HashSet<Object> distinctValues = new java.util.HashSet<>();

    public CountDistinctAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (!block.isNull(position)) {
        distinctValues.add(readValue(block, position, inputType));
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, distinctValues.size());
    }
  }

  /** SUM accumulator for numeric columns. Produces a BIGINT result for integer types. */
  public static class SumAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private double sum = 0;
    private boolean hasValue = false;

    public SumAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (!block.isNull(position)) {
        hasValue = true;
        if (inputType instanceof DoubleType) {
          sum += DoubleType.DOUBLE.getDouble(block, position);
        } else {
          // All integer types (BigintType, IntegerType, SmallintType, TinyintType)
          sum += inputType.getLong(block, position);
        }
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (inputType instanceof DoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, sum);
      } else {
        BigintType.BIGINT.writeLong(builder, (long) sum);
      }
    }
  }

  /** MIN accumulator for comparable columns. */
  public static class MinAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private Object min = null;

    public MinAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (block.isNull(position)) {
        return;
      }
      Object value = readValue(block, position, inputType);
      if (min == null || compare(value, min) < 0) {
        min = value;
      }
    }

    @SuppressWarnings("unchecked")
    private int compare(Object a, Object b) {
      return ((Comparable<Object>) a).compareTo(b);
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (min == null) {
        builder.appendNull();
      } else {
        writeValue(builder, inputType, min);
      }
    }
  }

  /** MAX accumulator for comparable columns. */
  public static class MaxAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private Object max = null;

    public MaxAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (block.isNull(position)) {
        return;
      }
      Object value = readValue(block, position, inputType);
      if (max == null || compare(value, max) > 0) {
        max = value;
      }
    }

    @SuppressWarnings("unchecked")
    private int compare(Object a, Object b) {
      return ((Comparable<Object>) a).compareTo(b);
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (max == null) {
        builder.appendNull();
      } else {
        writeValue(builder, inputType, max);
      }
    }
  }

  /**
   * AVG accumulator. Produces a DOUBLE result. For integer input types, uses java.math.BigDecimal
   * accumulation to maintain precision for large values (e.g., UserID averaging over millions of
   * rows).
   */
  public static class AvgAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private final boolean isIntegerType;
    private double doubleSum = 0;
    private java.math.BigDecimal bigDecimalSum = java.math.BigDecimal.ZERO;
    private long count = 0;

    public AvgAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
      this.isIntegerType = !(inputType instanceof DoubleType);
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (!block.isNull(position)) {
        count++;
        if (isIntegerType) {
          bigDecimalSum =
              bigDecimalSum.add(java.math.BigDecimal.valueOf(inputType.getLong(block, position)));
        } else {
          doubleSum += DoubleType.DOUBLE.getDouble(block, position);
        }
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (count == 0) {
        builder.appendNull();
      } else if (isIntegerType) {
        double avg =
            bigDecimalSum
                .divide(java.math.BigDecimal.valueOf(count), 10, java.math.RoundingMode.HALF_UP)
                .doubleValue();
        DoubleType.DOUBLE.writeDouble(builder, avg);
      } else {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum / count);
      }
    }
  }

  /** Factory for COUNT(DISTINCT column) aggregate. */
  public static AggregateFunction countDistinct(int columnIndex, Type inputType) {
    return new AggregateFunction() {
      @Override
      public Accumulator createAccumulator() {
        return new CountDistinctAccumulator(columnIndex, inputType);
      }

      @Override
      public Type getOutputType() {
        return BigintType.BIGINT;
      }
    };
  }

  /** Factory for COUNT(*) aggregate. */
  public static AggregateFunction count() {
    return new AggregateFunction() {
      @Override
      public Accumulator createAccumulator() {
        return new CountAccumulator();
      }

      @Override
      public Type getOutputType() {
        return BigintType.BIGINT;
      }
    };
  }

  /** Factory for SUM(column) aggregate. */
  public static AggregateFunction sum(int columnIndex, Type inputType) {
    return new AggregateFunction() {
      @Override
      public Accumulator createAccumulator() {
        return new SumAccumulator(columnIndex, inputType);
      }

      @Override
      public Type getOutputType() {
        if (inputType instanceof DoubleType) {
          return DoubleType.DOUBLE;
        }
        return BigintType.BIGINT;
      }
    };
  }

  /** Factory for MIN(column) aggregate. */
  public static AggregateFunction min(int columnIndex, Type inputType) {
    return new AggregateFunction() {
      @Override
      public Accumulator createAccumulator() {
        return new MinAccumulator(columnIndex, inputType);
      }

      @Override
      public Type getOutputType() {
        return inputType;
      }
    };
  }

  /** Factory for MAX(column) aggregate. */
  public static AggregateFunction max(int columnIndex, Type inputType) {
    return new AggregateFunction() {
      @Override
      public Accumulator createAccumulator() {
        return new MaxAccumulator(columnIndex, inputType);
      }

      @Override
      public Type getOutputType() {
        return inputType;
      }
    };
  }

  /** Factory for AVG(column) aggregate. */
  public static AggregateFunction avg(int columnIndex, Type inputType) {
    return new AggregateFunction() {
      @Override
      public Accumulator createAccumulator() {
        return new AvgAccumulator(columnIndex, inputType);
      }

      @Override
      public Type getOutputType() {
        return DoubleType.DOUBLE;
      }
    };
  }

  /**
   * Adapter that bridges the standalone {@link
   * org.opensearch.sql.dqe.function.aggregate.Accumulator} (Block-level) to the row-at-a-time
   * {@link Accumulator} interface used by this operator.
   */
  private static class FactoryAccumulatorAdapter implements Accumulator {

    private final org.opensearch.sql.dqe.function.aggregate.Accumulator delegate;
    private final int columnIndex;

    FactoryAccumulatorAdapter(
        org.opensearch.sql.dqe.function.aggregate.Accumulator delegate, int columnIndex) {
      this.delegate = delegate;
      this.columnIndex = columnIndex;
    }

    @Override
    public void add(Page page, int position) {
      // For row-at-a-time: extract single-position block and delegate
      Block block = columnIndex >= 0 ? page.getBlock(columnIndex) : null;
      if (block == null) {
        // COUNT(*) — just count; use a non-null dummy
        delegate.addBlock(page.getBlock(0).getSingleValueBlock(position), 1);
      } else {
        delegate.addBlock(block.getSingleValueBlock(position), 1);
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      delegate.writeFinalTo(builder);
    }
  }
}
