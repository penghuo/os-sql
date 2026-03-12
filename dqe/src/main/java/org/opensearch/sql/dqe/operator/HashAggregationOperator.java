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

    // Fast path: no GROUP BY columns — scalar aggregation over all rows.
    // Avoids per-row group key allocation, HashMap lookups, and virtual dispatch.
    if (groupByColumnIndices.isEmpty()) {
      return processScalarAggregation();
    }

    return processGroupedAggregation();
  }

  /**
   * Scalar aggregation fast path: no GROUP BY columns. Creates accumulators once and feeds entire
   * pages through them using batch-oriented addPage, eliminating per-row overhead.
   */
  private Page processScalarAggregation() {
    List<Accumulator> accumulators = new ArrayList<>(aggregateFunctions.size());
    for (AggregateFunction func : aggregateFunctions) {
      accumulators.add(func.createAccumulator());
    }

    boolean hasData = false;
    Page page;
    while ((page = source.processNextBatch()) != null) {
      hasData = true;
      int positionCount = page.getPositionCount();
      for (int i = 0; i < accumulators.size(); i++) {
        accumulators.get(i).addPage(page, positionCount);
      }
    }

    if (!hasData) {
      return null;
    }

    // Build result page with just aggregate columns
    int totalColumns = aggregateFunctions.size();
    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < totalColumns; i++) {
      builders[i] = aggregateFunctions.get(i).getOutputType().createBlockBuilder(null, 1);
      accumulators.get(i).writeTo(builders[i]);
    }

    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  /** Grouped aggregation: hash-based grouping with batch-oriented accumulator processing. */
  private Page processGroupedAggregation() {
    // Fast path: single long/integer group-by key uses a specialized open-addressing hash map
    // that avoids GroupKey object allocation, Object[] arrays, and Long boxing per row.
    if (groupByColumnIndices.size() == 1) {
      Type keyType = columnTypes.get(groupByColumnIndices.get(0));
      if (isLongKeyType(keyType)) {
        return processSingleLongKeyAggregation();
      }
      // Fast path: single VARCHAR group-by key uses HashMap<String, Accumulators> directly,
      // avoiding GroupKey/Object[] allocation per row. Common for GROUP BY on computed
      // string expressions like REGEXP_REPLACE.
      if (keyType instanceof VarcharType) {
        return processSingleVarcharKeyAggregation();
      }
    }

    return processGenericGroupedAggregation();
  }

  /** Check if a type can be represented as a long key for the specialized aggregation path. */
  private static boolean isLongKeyType(Type type) {
    return type instanceof BigintType
        || type instanceof IntegerType
        || type instanceof SmallintType
        || type instanceof TinyintType
        || type instanceof TimestampType;
  }

  /**
   * Specialized aggregation for single long/integer group-by key. Uses an open-addressing hash map
   * with primitive long keys to eliminate GroupKey allocation, Object[] creation, Long boxing, and
   * polymorphic hashCode/equals per row.
   */
  private Page processSingleLongKeyAggregation() {
    int keyColIdx = groupByColumnIndices.get(0);
    Type keyType = columnTypes.get(keyColIdx);
    LongKeyHashMap longMap = new LongKeyHashMap();

    Page page;
    while ((page = source.processNextBatch()) != null) {
      int positionCount = page.getPositionCount();
      Block keyBlock = page.getBlock(keyColIdx);

      for (int pos = 0; pos < positionCount; pos++) {
        if (keyBlock.isNull(pos)) {
          // Null keys go into a special null-key group
          List<Accumulator> accumulators = longMap.getOrCreateNullGroup(aggregateFunctions);
          for (int i = 0; i < accumulators.size(); i++) {
            accumulators.get(i).add(page, pos);
          }
        } else {
          long key = keyType.getLong(keyBlock, pos);
          List<Accumulator> accumulators = longMap.getOrCreate(key, aggregateFunctions);
          for (int i = 0; i < accumulators.size(); i++) {
            accumulators.get(i).add(page, pos);
          }
        }
      }
    }

    int groupCount = longMap.size;
    boolean hasNullGroup = longMap.nullGroupAccumulators != null;
    if (hasNullGroup) {
      groupCount++;
    }

    if (groupCount == 0) {
      return null;
    }

    // Build result page
    int totalColumns = 1 + aggregateFunctions.size();
    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    builders[0] = keyType.createBlockBuilder(null, groupCount);
    for (int i = 0; i < aggregateFunctions.size(); i++) {
      builders[1 + i] =
          aggregateFunctions.get(i).getOutputType().createBlockBuilder(null, groupCount);
    }

    // Write non-null groups
    for (int slot = 0; slot < longMap.capacity; slot++) {
      if (longMap.occupied[slot]) {
        writeValue(builders[0], keyType, longMap.keys[slot]);
        List<Accumulator> accumulators = longMap.values[slot];
        for (int i = 0; i < accumulators.size(); i++) {
          accumulators.get(i).writeTo(builders[1 + i]);
        }
      }
    }

    // Write null group if present
    if (hasNullGroup) {
      builders[0].appendNull();
      for (int i = 0; i < longMap.nullGroupAccumulators.size(); i++) {
        longMap.nullGroupAccumulators.get(i).writeTo(builders[1 + i]);
      }
    }

    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  /**
   * Specialized aggregation for single VARCHAR group-by key. Uses HashMap&lt;String,
   * List&lt;Accumulator&gt;&gt; directly, avoiding the GroupKey/Object[] allocation per row that
   * the generic path requires. For queries like GROUP BY REGEXP_REPLACE(...) with ~20K distinct
   * groups from 125K rows, this eliminates ~105K GroupKey objects and ~105K Object[] arrays (only
   * ~20K unique entries in the map vs 125K lookups with generic GroupKey).
   */
  private Page processSingleVarcharKeyAggregation() {
    int keyColIdx = groupByColumnIndices.get(0);
    Map<String, List<Accumulator>> groups = new LinkedHashMap<>();
    List<Accumulator> nullGroupAccumulators = null;

    Page page;
    while ((page = source.processNextBatch()) != null) {
      int positionCount = page.getPositionCount();
      Block keyBlock = page.getBlock(keyColIdx);

      for (int pos = 0; pos < positionCount; pos++) {
        if (keyBlock.isNull(pos)) {
          if (nullGroupAccumulators == null) {
            nullGroupAccumulators = new ArrayList<>(aggregateFunctions.size());
            for (AggregateFunction func : aggregateFunctions) {
              nullGroupAccumulators.add(func.createAccumulator());
            }
          }
          for (int i = 0; i < nullGroupAccumulators.size(); i++) {
            nullGroupAccumulators.get(i).add(page, pos);
          }
        } else {
          String key = VarcharType.VARCHAR.getSlice(keyBlock, pos).toStringUtf8();
          List<Accumulator> accumulators =
              groups.computeIfAbsent(
                  key,
                  k -> {
                    List<Accumulator> accs = new ArrayList<>(aggregateFunctions.size());
                    for (AggregateFunction func : aggregateFunctions) {
                      accs.add(func.createAccumulator());
                    }
                    return accs;
                  });
          for (int i = 0; i < accumulators.size(); i++) {
            accumulators.get(i).add(page, pos);
          }
        }
      }
    }

    int groupCount = groups.size();
    boolean hasNullGroup = nullGroupAccumulators != null;
    if (hasNullGroup) {
      groupCount++;
    }
    if (groupCount == 0) {
      return null;
    }

    // Build result page
    int totalColumns = 1 + aggregateFunctions.size();
    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
    for (int i = 0; i < aggregateFunctions.size(); i++) {
      builders[1 + i] =
          aggregateFunctions.get(i).getOutputType().createBlockBuilder(null, groupCount);
    }

    for (Map.Entry<String, List<Accumulator>> entry : groups.entrySet()) {
      VarcharType.VARCHAR.writeSlice(
          builders[0], io.airlift.slice.Slices.utf8Slice(entry.getKey()));
      List<Accumulator> accumulators = entry.getValue();
      for (int i = 0; i < accumulators.size(); i++) {
        accumulators.get(i).writeTo(builders[1 + i]);
      }
    }

    if (hasNullGroup) {
      builders[0].appendNull();
      for (int i = 0; i < nullGroupAccumulators.size(); i++) {
        nullGroupAccumulators.get(i).writeTo(builders[1 + i]);
      }
    }

    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  /**
   * Open-addressing hash map with primitive long keys. Uses linear probing with power-of-two
   * capacity for fast modulo via bitmask. Eliminates object allocation per group key lookup.
   */
  static final class LongKeyHashMap {
    private static final int INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.7f;

    long[] keys;

    @SuppressWarnings("unchecked")
    List<Accumulator>[] values;

    boolean[] occupied;
    int size;
    int capacity;
    private int threshold;

    /** Accumulator list for null group key (handled separately from the long-key map). */
    List<Accumulator> nullGroupAccumulators;

    @SuppressWarnings("unchecked")
    LongKeyHashMap() {
      this.capacity = INITIAL_CAPACITY;
      this.keys = new long[capacity];
      this.values = new List[capacity];
      this.occupied = new boolean[capacity];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    List<Accumulator> getOrCreate(long key, List<AggregateFunction> aggregateFunctions) {
      int slot = findSlot(key);
      if (occupied[slot] && keys[slot] == key) {
        return values[slot];
      }
      // New entry
      List<Accumulator> accs = createAccumulators(aggregateFunctions);
      keys[slot] = key;
      values[slot] = accs;
      occupied[slot] = true;
      size++;
      if (size > threshold) {
        resize();
        // After resize, return from the new slot location
        return values[findSlot(key)];
      }
      return accs;
    }

    List<Accumulator> getOrCreateNullGroup(List<AggregateFunction> aggregateFunctions) {
      if (nullGroupAccumulators == null) {
        nullGroupAccumulators = createAccumulators(aggregateFunctions);
      }
      return nullGroupAccumulators;
    }

    private int findSlot(long key) {
      int mask = capacity - 1;
      int slot = Long.hashCode(key) & mask;
      while (occupied[slot] && keys[slot] != key) {
        slot = (slot + 1) & mask;
      }
      return slot;
    }

    @SuppressWarnings("unchecked")
    private void resize() {
      int newCapacity = capacity * 2;
      long[] newKeys = new long[newCapacity];
      List<Accumulator>[] newValues = new List[newCapacity];
      boolean[] newOccupied = new boolean[newCapacity];
      int newMask = newCapacity - 1;

      for (int i = 0; i < capacity; i++) {
        if (occupied[i]) {
          int slot = Long.hashCode(keys[i]) & newMask;
          while (newOccupied[slot]) {
            slot = (slot + 1) & newMask;
          }
          newKeys[slot] = keys[i];
          newValues[slot] = values[i];
          newOccupied[slot] = true;
        }
      }

      this.keys = newKeys;
      this.values = newValues;
      this.occupied = newOccupied;
      this.capacity = newCapacity;
      this.threshold = (int) (newCapacity * LOAD_FACTOR);
    }

    private static List<Accumulator> createAccumulators(
        List<AggregateFunction> aggregateFunctions) {
      List<Accumulator> accs = new ArrayList<>(aggregateFunctions.size());
      for (AggregateFunction func : aggregateFunctions) {
        accs.add(func.createAccumulator());
      }
      return accs;
    }
  }

  /** Generic grouped aggregation for multi-key or non-long-key GROUP BY. */
  private Page processGenericGroupedAggregation() {
    // Drain all pages from source and group rows
    Map<GroupKey, List<Accumulator>> groups = new LinkedHashMap<>();

    Page page;
    while ((page = source.processNextBatch()) != null) {
      int positionCount = page.getPositionCount();

      // Pre-fetch blocks for group-by columns and accumulator columns once per page
      Block[] groupBlocks = new Block[groupByColumnIndices.size()];
      Type[] groupTypes = new Type[groupByColumnIndices.size()];
      for (int g = 0; g < groupByColumnIndices.size(); g++) {
        int colIdx = groupByColumnIndices.get(g);
        groupBlocks[g] = page.getBlock(colIdx);
        groupTypes[g] = columnTypes.get(colIdx);
      }

      for (int pos = 0; pos < positionCount; pos++) {
        GroupKey groupKey = extractGroupKeyFast(groupBlocks, groupTypes, pos);

        List<Accumulator> accumulators =
            groups.computeIfAbsent(
                groupKey,
                k -> {
                  List<Accumulator> accs = new ArrayList<>(aggregateFunctions.size());
                  for (AggregateFunction func : aggregateFunctions) {
                    accs.add(func.createAccumulator());
                  }
                  return accs;
                });

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

    // Pre-compute group types once for result writing
    Type[] resultGroupTypes = new Type[groupByColumnIndices.size()];
    for (int i = 0; i < groupByColumnIndices.size(); i++) {
      resultGroupTypes[i] = columnTypes.get(groupByColumnIndices.get(i));
    }

    // Write results
    for (Map.Entry<GroupKey, List<Accumulator>> entry : groups.entrySet()) {
      GroupKey key = entry.getKey();
      List<Accumulator> accumulators = entry.getValue();

      // Write group-by key values
      key.writeTo(builders, resultGroupTypes);

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

  /**
   * Extract a group key using pre-fetched blocks and types. Uses the compact GroupKey class instead
   * of ArrayList&lt;Object&gt; to reduce allocation and improve hash/equals performance.
   */
  private GroupKey extractGroupKeyFast(Block[] groupBlocks, Type[] groupTypes, int position) {
    Object[] values = new Object[groupBlocks.length];
    for (int i = 0; i < groupBlocks.length; i++) {
      Block block = groupBlocks[i];
      if (block.isNull(position)) {
        values[i] = null;
      } else {
        values[i] = readValue(block, position, groupTypes[i]);
      }
    }
    return new GroupKey(values);
  }

  /**
   * Compact group key using an Object array with pre-computed hash code. Avoids ArrayList
   * allocation overhead and redundant hash computation on every map lookup.
   */
  static final class GroupKey {
    final Object[] values;
    private final int hash;

    GroupKey(Object[] values) {
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
      if (!(obj instanceof GroupKey)) return false;
      GroupKey other = (GroupKey) obj;
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

    /** Write group key values to block builders. */
    void writeTo(BlockBuilder[] builders, Type[] types) {
      for (int i = 0; i < values.length; i++) {
        writeValue(builders[i], types[i], values[i]);
      }
    }
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
      if (value instanceof io.airlift.slice.Slice slice) {
        VarcharType.VARCHAR.writeSlice(builder, slice);
      } else {
        VarcharType.VARCHAR.writeSlice(
            builder, io.airlift.slice.Slices.utf8Slice(value.toString()));
      }
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

    /**
     * Process all positions in a page in batch. Default implementation delegates to add() per row,
     * but subclasses can override for better performance by fetching the block once.
     */
    default void addPage(Page page, int positionCount) {
      for (int pos = 0; pos < positionCount; pos++) {
        add(page, pos);
      }
    }

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
    public void addPage(Page page, int positionCount) {
      count += positionCount;
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      BigintType.BIGINT.writeLong(builder, count);
    }
  }

  /**
   * COUNT(DISTINCT column) accumulator. Uses primitive LongOpenHashSet for numeric non-double
   * columns to avoid Long boxing overhead. Falls back to HashSet&lt;Object&gt; for varchar and
   * double types.
   */
  public static class CountDistinctAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private final boolean usePrimitiveLong;

    /** Primitive long set for numeric non-double columns (avoids Long boxing). */
    private final LongOpenHashSet longDistinctValues;

    /** Fallback set for varchar and double types. */
    private final java.util.HashSet<Object> objectDistinctValues;

    public CountDistinctAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
      this.usePrimitiveLong =
          !(inputType instanceof VarcharType) && !(inputType instanceof DoubleType);
      this.longDistinctValues = usePrimitiveLong ? new LongOpenHashSet() : null;
      this.objectDistinctValues = usePrimitiveLong ? null : new java.util.HashSet<>();
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (!block.isNull(position)) {
        if (usePrimitiveLong) {
          longDistinctValues.add(inputType.getLong(block, position));
        } else {
          objectDistinctValues.add(readValue(block, position, inputType));
        }
      }
    }

    @Override
    public void addPage(Page page, int positionCount) {
      Block block = page.getBlock(columnIndex);
      if (usePrimitiveLong) {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            longDistinctValues.add(inputType.getLong(block, pos));
          }
        }
      } else {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            objectDistinctValues.add(readValue(block, pos, inputType));
          }
        }
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      long count = usePrimitiveLong ? longDistinctValues.size() : objectDistinctValues.size();
      BigintType.BIGINT.writeLong(builder, count);
    }
  }

  /** SUM accumulator for numeric columns. Uses long for integer types to avoid precision loss. */
  public static class SumAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private final boolean isIntegerType;
    private long longSum = 0;
    private double doubleSum = 0;
    private boolean hasValue = false;

    public SumAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
      this.isIntegerType = !(inputType instanceof DoubleType);
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (!block.isNull(position)) {
        hasValue = true;
        if (isIntegerType) {
          longSum += inputType.getLong(block, position);
        } else {
          doubleSum += DoubleType.DOUBLE.getDouble(block, position);
        }
      }
    }

    @Override
    public void addPage(Page page, int positionCount) {
      Block block = page.getBlock(columnIndex);
      if (isIntegerType) {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            hasValue = true;
            longSum += inputType.getLong(block, pos);
          }
        }
      } else {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            hasValue = true;
            doubleSum += DoubleType.DOUBLE.getDouble(block, pos);
          }
        }
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isIntegerType) {
        BigintType.BIGINT.writeLong(builder, longSum);
      } else {
        DoubleType.DOUBLE.writeDouble(builder, doubleSum);
      }
    }
  }

  /** MIN accumulator for comparable columns. */
  public static class MinAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private final boolean isLongType;
    private final boolean isDoubleType;
    private long longMin = Long.MAX_VALUE;
    private double doubleMin = Double.MAX_VALUE;
    private Object objectMin = null;
    private boolean hasValue = false;

    public MinAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
      this.isLongType =
          inputType instanceof BigintType
              || inputType instanceof IntegerType
              || inputType instanceof SmallintType
              || inputType instanceof TinyintType
              || inputType instanceof TimestampType;
      this.isDoubleType = inputType instanceof DoubleType;
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (block.isNull(position)) {
        return;
      }
      hasValue = true;
      if (isLongType) {
        long val = inputType.getLong(block, position);
        if (val < longMin) longMin = val;
      } else if (isDoubleType) {
        double val = DoubleType.DOUBLE.getDouble(block, position);
        if (val < doubleMin) doubleMin = val;
      } else {
        Object value = readValue(block, position, inputType);
        if (objectMin == null || compare(value, objectMin) < 0) {
          objectMin = value;
        }
      }
    }

    @Override
    public void addPage(Page page, int positionCount) {
      Block block = page.getBlock(columnIndex);
      if (isLongType) {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            hasValue = true;
            long val = inputType.getLong(block, pos);
            if (val < longMin) longMin = val;
          }
        }
      } else if (isDoubleType) {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            hasValue = true;
            double val = DoubleType.DOUBLE.getDouble(block, pos);
            if (val < doubleMin) doubleMin = val;
          }
        }
      } else {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            hasValue = true;
            Object value = readValue(block, pos, inputType);
            if (objectMin == null || compare(value, objectMin) < 0) {
              objectMin = value;
            }
          }
        }
      }
    }

    @SuppressWarnings("unchecked")
    private int compare(Object a, Object b) {
      return ((Comparable<Object>) a).compareTo(b);
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isLongType) {
        writeValue(builder, inputType, longMin);
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleMin);
      } else {
        writeValue(builder, inputType, objectMin);
      }
    }
  }

  /** MAX accumulator for comparable columns. */
  public static class MaxAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private final boolean isLongType;
    private final boolean isDoubleType;
    private long longMax = Long.MIN_VALUE;
    private double doubleMax = -Double.MAX_VALUE;
    private Object objectMax = null;
    private boolean hasValue = false;

    public MaxAccumulator(int columnIndex, Type inputType) {
      this.columnIndex = columnIndex;
      this.inputType = inputType;
      this.isLongType =
          inputType instanceof BigintType
              || inputType instanceof IntegerType
              || inputType instanceof SmallintType
              || inputType instanceof TinyintType
              || inputType instanceof TimestampType;
      this.isDoubleType = inputType instanceof DoubleType;
    }

    @Override
    public void add(Page page, int position) {
      Block block = page.getBlock(columnIndex);
      if (block.isNull(position)) {
        return;
      }
      hasValue = true;
      if (isLongType) {
        long val = inputType.getLong(block, position);
        if (val > longMax) longMax = val;
      } else if (isDoubleType) {
        double val = DoubleType.DOUBLE.getDouble(block, position);
        if (val > doubleMax) doubleMax = val;
      } else {
        Object value = readValue(block, position, inputType);
        if (objectMax == null || compare(value, objectMax) > 0) {
          objectMax = value;
        }
      }
    }

    @Override
    public void addPage(Page page, int positionCount) {
      Block block = page.getBlock(columnIndex);
      if (isLongType) {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            hasValue = true;
            long val = inputType.getLong(block, pos);
            if (val > longMax) longMax = val;
          }
        }
      } else if (isDoubleType) {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            hasValue = true;
            double val = DoubleType.DOUBLE.getDouble(block, pos);
            if (val > doubleMax) doubleMax = val;
          }
        }
      } else {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            hasValue = true;
            Object value = readValue(block, pos, inputType);
            if (objectMax == null || compare(value, objectMax) > 0) {
              objectMax = value;
            }
          }
        }
      }
    }

    @SuppressWarnings("unchecked")
    private int compare(Object a, Object b) {
      return ((Comparable<Object>) a).compareTo(b);
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (!hasValue) {
        builder.appendNull();
      } else if (isLongType) {
        writeValue(builder, inputType, longMax);
      } else if (isDoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, doubleMax);
      } else {
        writeValue(builder, inputType, objectMax);
      }
    }
  }

  /**
   * AVG accumulator. Produces a DOUBLE result. Uses long accumulation for integer types to match
   * ClickHouse/Trino overflow semantics (Int64 wrapping), then divides by count as double.
   */
  public static class AvgAccumulator implements Accumulator {
    private final int columnIndex;
    private final Type inputType;
    private final boolean isIntegerType;
    private long longSum = 0;
    private double doubleSum = 0;
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
          longSum += inputType.getLong(block, position);
        } else {
          doubleSum += DoubleType.DOUBLE.getDouble(block, position);
        }
      }
    }

    @Override
    public void addPage(Page page, int positionCount) {
      Block block = page.getBlock(columnIndex);
      if (isIntegerType) {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            count++;
            longSum += inputType.getLong(block, pos);
          }
        }
      } else {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            count++;
            doubleSum += DoubleType.DOUBLE.getDouble(block, pos);
          }
        }
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (count == 0) {
        builder.appendNull();
      } else if (isIntegerType) {
        DoubleType.DOUBLE.writeDouble(builder, (double) longSum / count);
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
    public void addPage(Page page, int positionCount) {
      // Batch path: pass the entire block to the delegate, avoiding per-row
      // getSingleValueBlock allocation.
      Block block = columnIndex >= 0 ? page.getBlock(columnIndex) : page.getBlock(0);
      delegate.addBlock(block, positionCount);
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      delegate.writeFinalTo(builder);
    }
  }
}
