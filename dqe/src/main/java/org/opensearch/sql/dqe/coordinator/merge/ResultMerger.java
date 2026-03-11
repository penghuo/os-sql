/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.merge;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.sql.dqe.operator.HashAggregationOperator;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;

/**
 * Merges partial results from multiple shards on the coordinator. Works directly with Trino Pages
 * instead of JSON strings. Supports three merge modes:
 *
 * <ol>
 *   <li><b>Passthrough</b>: Non-aggregate queries -- concatenate Pages from all shards.
 *   <li><b>Aggregation merge</b>: Run FINAL aggregation over partial results from shards.
 *   <li><b>Sorted merge with limit</b>: Merge pre-sorted shard results and apply final limit.
 * </ol>
 */
public class ResultMerger {

  /**
   * Pattern to extract the aggregate function name and optional column from expressions like {@code
   * COUNT(*)}, {@code SUM(amount)}, {@code MIN(price)}, {@code MAX(salary)}.
   */
  private static final Pattern AGG_PATTERN =
      Pattern.compile(
          "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$", Pattern.CASE_INSENSITIVE);

  /** Merge shard results for a non-aggregate query. Simple concatenation of all pages. */
  public List<Page> mergePassthrough(List<List<Page>> shardResults) {
    List<Page> allPages = new ArrayList<>();
    for (List<Page> shardPages : shardResults) {
      allPages.addAll(shardPages);
    }
    return allPages;
  }

  /**
   * Merge shard results with final aggregation. Groups rows by the group-by keys and applies the
   * appropriate merge operation for each aggregate function:
   *
   * <ul>
   *   <li>{@code COUNT(*)} -- SUM the partial counts
   *   <li>{@code SUM(col)} -- SUM the partial sums
   *   <li>{@code MIN(col)} -- MIN of partial mins
   *   <li>{@code MAX(col)} -- MAX of partial maxes
   * </ul>
   *
   * @param shardResults pages from each shard
   * @param finalAggNode the FINAL aggregation node with group-by keys and functions
   * @param columnTypes the Trino types for the output columns (group-by keys + agg results)
   * @return merged result pages
   */
  public List<Page> mergeAggregation(
      List<List<Page>> shardResults, AggregationNode finalAggNode, List<Type> columnTypes) {

    List<String> groupByKeys = finalAggNode.getGroupByKeys();
    List<String> aggregateFunctions = finalAggNode.getAggregateFunctions();
    int numGroupByCols = groupByKeys.size();
    int numAggCols = aggregateFunctions.size();
    int totalCols = numGroupByCols + numAggCols;

    // Fast path: numeric-only group keys with simple aggregates (COUNT/SUM only).
    // Directly merges shard Pages using primitive long arrays, avoiding Object allocation,
    // boxing, and HashMap overhead that dominates merge time for multi-key GROUP BY.
    if (canUseFastNumericMerge(numGroupByCols, columnTypes, aggregateFunctions)) {
      return mergeAggregationFastNumeric(
          shardResults, numGroupByCols, numAggCols, columnTypes, aggregateFunctions);
    }

    // Fast path: single VARCHAR key with COUNT/SUM-only aggregates (all BigintType output).
    // Uses HashMap<String, long[]> directly, avoiding GroupKey allocation and Accumulator overhead.
    if (canUseFastVarcharMerge(numGroupByCols, columnTypes, aggregateFunctions)) {
      return mergeAggregationFastVarchar(shardResults, numGroupByCols, numAggCols, columnTypes);
    }

    // Build merge functions that operate on the partial aggregation output columns.
    // The shard Pages have layout: [groupBy0, groupBy1, ..., agg0, agg1, ...].
    // For FINAL merge: COUNT/SUM→sum, MIN→min, MAX→max, AVG→weighted_avg.
    List<HashAggregationOperator.AggregateFunction> mergeFunctions = new ArrayList<>();
    int countColIdx = -1; // companion COUNT column for AVG weighting
    for (int a = 0; a < numAggCols; a++) {
      Matcher m = AGG_PATTERN.matcher(aggregateFunctions.get(a));
      if (m.matches()
          && "COUNT".equals(m.group(1).toUpperCase(Locale.ROOT))
          && m.group(2) == null) {
        countColIdx = numGroupByCols + a;
      }
    }

    for (int a = 0; a < numAggCols; a++) {
      int colIdx = numGroupByCols + a;
      Matcher m = AGG_PATTERN.matcher(aggregateFunctions.get(a));
      String funcName = m.matches() ? m.group(1).toUpperCase(Locale.ROOT) : "SUM";
      Type colType = columnTypes.get(colIdx);

      switch (funcName) {
        case "COUNT":
        case "SUM":
          mergeFunctions.add(HashAggregationOperator.sum(colIdx, colType));
          break;
        case "MIN":
          mergeFunctions.add(HashAggregationOperator.min(colIdx, colType));
          break;
        case "MAX":
          mergeFunctions.add(HashAggregationOperator.max(colIdx, colType));
          break;
        case "AVG":
          // AVG merge: weighted average using companion COUNT column
          // For now, use sum (accumulates avg values) and fix at the end
          if (countColIdx >= 0) {
            int finalCountColIdx = countColIdx;
            int finalColIdx = colIdx;
            mergeFunctions.add(
                new HashAggregationOperator.AggregateFunction() {
                  @Override
                  public HashAggregationOperator.Accumulator createAccumulator() {
                    return new WeightedAvgMergeAccumulator(finalColIdx, finalCountColIdx, colType);
                  }

                  @Override
                  public Type getOutputType() {
                    return DoubleType.DOUBLE;
                  }
                });
          } else {
            mergeFunctions.add(HashAggregationOperator.avg(colIdx, colType));
          }
          break;
        default:
          mergeFunctions.add(HashAggregationOperator.sum(colIdx, colType));
      }
    }

    // Build group-by indices (0..numGroupBy-1)
    List<Integer> groupByIndices = new ArrayList<>();
    for (int i = 0; i < numGroupByCols; i++) {
      groupByIndices.add(i);
    }

    // Feed all shard pages into a HashAggregationOperator for efficient merge
    List<Page> allPages = mergePassthrough(shardResults);
    HashAggregationOperator mergeOp =
        new HashAggregationOperator(
            new org.opensearch.sql.dqe.operator.Operator() {
              private int idx = 0;

              @Override
              public Page processNextBatch() {
                return idx < allPages.size() ? allPages.get(idx++) : null;
              }

              @Override
              public void close() {}
            },
            groupByIndices,
            mergeFunctions,
            columnTypes);

    List<Page> result = new ArrayList<>();
    Page page;
    while ((page = mergeOp.processNextBatch()) != null) {
      result.add(page);
    }
    return result;
  }

  /**
   * Check if we can use the fast numeric merge path. Requirements:
   *
   * <ul>
   *   <li>All group-by key types are long-representable (BigintType, IntegerType, TimestampType,
   *       SmallintType, TinyintType, BooleanType)
   *   <li>All aggregate functions are COUNT, SUM, or AVG with BigintType or DoubleType output
   *   <li>AVG requires a companion non-distinct COUNT column for weighted merge
   * </ul>
   */
  private boolean canUseFastNumericMerge(
      int numGroupByCols, List<Type> columnTypes, List<String> aggregateFunctions) {
    // Check all group-by keys are long-representable
    for (int i = 0; i < numGroupByCols; i++) {
      Type t = columnTypes.get(i);
      if (!(t instanceof BigintType
          || t instanceof io.trino.spi.type.IntegerType
          || t instanceof io.trino.spi.type.TimestampType
          || t instanceof io.trino.spi.type.SmallintType
          || t instanceof io.trino.spi.type.TinyintType
          || t instanceof BooleanType)) {
        return false;
      }
    }
    // Check all aggregates are COUNT, SUM (BigintType), or AVG (DoubleType)
    boolean hasAvg = false;
    boolean hasCount = false;
    for (int a = 0; a < aggregateFunctions.size(); a++) {
      Matcher m = AGG_PATTERN.matcher(aggregateFunctions.get(a));
      if (!m.matches()) return false;
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      if (isDistinct) return false;
      Type aggType = columnTypes.get(numGroupByCols + a);
      if ("COUNT".equals(funcName)) {
        if (!(aggType instanceof BigintType)) return false;
        hasCount = true;
      } else if ("SUM".equals(funcName)) {
        if (!(aggType instanceof BigintType)) return false;
      } else if ("AVG".equals(funcName)) {
        if (!(aggType instanceof DoubleType)) return false;
        hasAvg = true;
      } else {
        return false; // MIN, MAX not supported on fast path
      }
    }
    // AVG requires a companion COUNT for weighted merge
    if (hasAvg && !hasCount) return false;
    return true;
  }

  /**
   * Fast numeric merge: directly iterates over shard Pages using primitive long/double operations.
   * Uses an open-addressing hash map with long[] keys to avoid Object allocation per row. Supports
   * COUNT (long sum), SUM (long sum), and AVG (weighted average using companion COUNT column).
   */
  private List<Page> mergeAggregationFastNumeric(
      List<List<Page>> shardResults,
      int numGroupByCols,
      int numAggCols,
      List<Type> columnTypes,
      List<String> aggregateFunctions) {

    int totalCols = numGroupByCols + numAggCols;

    // Classify aggregates: identify AVG columns and their companion COUNT column
    boolean[] isDoubleAgg = new boolean[numAggCols];
    boolean[] isAvgAgg = new boolean[numAggCols];
    int countAggIdx = -1; // index within the agg columns (not absolute column index)
    for (int a = 0; a < numAggCols; a++) {
      Matcher m = AGG_PATTERN.matcher(aggregateFunctions.get(a));
      if (m.matches()) {
        String funcName = m.group(1).toUpperCase(Locale.ROOT);
        if ("AVG".equals(funcName)) {
          isAvgAgg[a] = true;
          isDoubleAgg[a] = true;
        } else if ("COUNT".equals(funcName) && m.group(2) == null) {
          countAggIdx = a;
        }
      }
      if (!isAvgAgg[a]) {
        isDoubleAgg[a] = columnTypes.get(numGroupByCols + a) instanceof DoubleType;
      }
    }
    // For AVG weighted merge: we accumulate (avg * count) as weightedSum, then divide by count
    // at the end. Use the double[] array for both AVG weighted sums and regular double sums.
    final int finalCountAggIdx = countAggIdx;

    // Open-addressing hash map: long[] key -> long[] longAggValues + double[] doubleAggValues
    int capacity = 1024;
    float loadFactor = 0.7f;
    int threshold = (int) (capacity * loadFactor);
    int size = 0;
    long[][] mapKeys = new long[capacity][];
    long[][] mapLongAggs = new long[capacity][];
    double[][] mapDoubleAggs = new double[capacity][];
    boolean[] mapOccupied = new boolean[capacity];

    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        int positionCount = page.getPositionCount();

        // Pre-fetch blocks once per page
        Block[] keyBlocks = new Block[numGroupByCols];
        for (int k = 0; k < numGroupByCols; k++) {
          keyBlocks[k] = page.getBlock(k);
        }
        Block[] aggBlocks = new Block[numAggCols];
        for (int a = 0; a < numAggCols; a++) {
          aggBlocks[a] = page.getBlock(numGroupByCols + a);
        }

        for (int pos = 0; pos < positionCount; pos++) {
          // Extract key values as longs
          long[] keyValues = new long[numGroupByCols];
          for (int k = 0; k < numGroupByCols; k++) {
            keyValues[k] = columnTypes.get(k).getLong(keyBlocks[k], pos);
          }

          // Hash the key
          int hash = 1;
          for (int k = 0; k < numGroupByCols; k++) {
            hash = hash * 31 + Long.hashCode(keyValues[k]);
          }

          // Probe the map
          int mask = capacity - 1;
          int slot = hash & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              // New group
              mapKeys[slot] = keyValues;
              long[] longAggs = new long[numAggCols];
              double[] doubleAggs = new double[numAggCols];
              // Read COUNT value first (needed for AVG weighting)
              long countVal = 0;
              if (finalCountAggIdx >= 0 && !aggBlocks[finalCountAggIdx].isNull(pos)) {
                countVal = BigintType.BIGINT.getLong(aggBlocks[finalCountAggIdx], pos);
              }
              for (int a = 0; a < numAggCols; a++) {
                if (!aggBlocks[a].isNull(pos)) {
                  if (isAvgAgg[a]) {
                    // Store weighted sum (avg * count) for proper cross-shard merge
                    doubleAggs[a] = DoubleType.DOUBLE.getDouble(aggBlocks[a], pos) * countVal;
                  } else if (isDoubleAgg[a]) {
                    doubleAggs[a] = DoubleType.DOUBLE.getDouble(aggBlocks[a], pos);
                  } else {
                    longAggs[a] = BigintType.BIGINT.getLong(aggBlocks[a], pos);
                  }
                }
              }
              mapLongAggs[slot] = longAggs;
              mapDoubleAggs[slot] = doubleAggs;
              mapOccupied[slot] = true;
              size++;

              // Resize if needed
              if (size > threshold) {
                int newCapacity = capacity * 2;
                long[][] newKeys = new long[newCapacity][];
                long[][] newLongAggs = new long[newCapacity][];
                double[][] newDoubleAggs = new double[newCapacity][];
                boolean[] newOccupied = new boolean[newCapacity];
                int newMask = newCapacity - 1;
                for (int s = 0; s < capacity; s++) {
                  if (mapOccupied[s]) {
                    int h = 1;
                    for (int k = 0; k < numGroupByCols; k++) {
                      h = h * 31 + Long.hashCode(mapKeys[s][k]);
                    }
                    int ns = h & newMask;
                    while (newOccupied[ns]) ns = (ns + 1) & newMask;
                    newKeys[ns] = mapKeys[s];
                    newLongAggs[ns] = mapLongAggs[s];
                    newDoubleAggs[ns] = mapDoubleAggs[s];
                    newOccupied[ns] = true;
                  }
                }
                capacity = newCapacity;
                mask = newCapacity - 1;
                threshold = (int) (newCapacity * loadFactor);
                mapKeys = newKeys;
                mapLongAggs = newLongAggs;
                mapDoubleAggs = newDoubleAggs;
                mapOccupied = newOccupied;
              }
              break;
            }

            // Check if key matches
            long[] existing = mapKeys[slot];
            boolean match = true;
            for (int k = 0; k < numGroupByCols; k++) {
              if (existing[k] != keyValues[k]) {
                match = false;
                break;
              }
            }
            if (match) {
              // Existing group: accumulate
              // Read COUNT value first (needed for AVG weighting)
              long countVal2 = 0;
              if (finalCountAggIdx >= 0 && !aggBlocks[finalCountAggIdx].isNull(pos)) {
                countVal2 = BigintType.BIGINT.getLong(aggBlocks[finalCountAggIdx], pos);
              }
              for (int a = 0; a < numAggCols; a++) {
                if (!aggBlocks[a].isNull(pos)) {
                  if (isAvgAgg[a]) {
                    // Accumulate weighted sum (avg * count)
                    mapDoubleAggs[slot][a] +=
                        DoubleType.DOUBLE.getDouble(aggBlocks[a], pos) * countVal2;
                  } else if (isDoubleAgg[a]) {
                    mapDoubleAggs[slot][a] += DoubleType.DOUBLE.getDouble(aggBlocks[a], pos);
                  } else {
                    mapLongAggs[slot][a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
                  }
                }
              }
              break;
            }
            slot = (slot + 1) & mask;
          }
        }
      }
    }

    if (size == 0) {
      return List.of();
    }

    // Build result Page. For AVG columns, divide accumulated weighted sum by total count.
    BlockBuilder[] builders = new BlockBuilder[totalCols];
    for (int i = 0; i < totalCols; i++) {
      builders[i] = columnTypes.get(i).createBlockBuilder(null, size);
    }
    for (int s = 0; s < capacity; s++) {
      if (mapOccupied[s]) {
        for (int k = 0; k < numGroupByCols; k++) {
          columnTypes.get(k).writeLong(builders[k], mapKeys[s][k]);
        }
        for (int a = 0; a < numAggCols; a++) {
          if (isAvgAgg[a]) {
            // Divide weighted sum by total count to get correct merged AVG
            long totalCount = (finalCountAggIdx >= 0) ? mapLongAggs[s][finalCountAggIdx] : 1;
            double avgValue = totalCount > 0 ? mapDoubleAggs[s][a] / totalCount : 0.0;
            DoubleType.DOUBLE.writeDouble(builders[numGroupByCols + a], avgValue);
          } else if (isDoubleAgg[a]) {
            DoubleType.DOUBLE.writeDouble(builders[numGroupByCols + a], mapDoubleAggs[s][a]);
          } else {
            BigintType.BIGINT.writeLong(builders[numGroupByCols + a], mapLongAggs[s][a]);
          }
        }
      }
    }

    Block[] blocks = new Block[totalCols];
    for (int i = 0; i < totalCols; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }

  /**
   * Check if we can use the fast varchar merge path. Requirements:
   *
   * <ul>
   *   <li>Single group-by key of VarcharType
   *   <li>All aggregate functions are COUNT or SUM with BigintType output
   * </ul>
   */
  private boolean canUseFastVarcharMerge(
      int numGroupByCols, List<Type> columnTypes, List<String> aggregateFunctions) {
    if (numGroupByCols != 1) return false;
    if (!(columnTypes.get(0) instanceof VarcharType)) return false;
    for (int a = 0; a < aggregateFunctions.size(); a++) {
      Matcher m = AGG_PATTERN.matcher(aggregateFunctions.get(a));
      if (!m.matches()) return false;
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      if (isDistinct) return false;
      Type aggType = columnTypes.get(numGroupByCols + a);
      if ("COUNT".equals(funcName) || "SUM".equals(funcName)) {
        if (!(aggType instanceof BigintType)) return false;
      } else {
        return false;
      }
    }
    return true;
  }

  /**
   * Fast varchar merge: uses HashMap&lt;String, long[]&gt; to merge single-VARCHAR-key GROUP BY
   * results with COUNT/SUM aggregates. Reads VARCHAR Slices directly from blocks and accumulates
   * long values, avoiding GroupKey object allocation, Accumulator creation, and virtual dispatch
   * per row.
   */
  private List<Page> mergeAggregationFastVarchar(
      List<List<Page>> shardResults, int numGroupByCols, int numAggCols, List<Type> columnTypes) {

    int totalCols = numGroupByCols + numAggCols;
    java.util.HashMap<String, long[]> groups = new java.util.HashMap<>();

    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        int positionCount = page.getPositionCount();
        Block keyBlock = page.getBlock(0);
        Block[] aggBlocks = new Block[numAggCols];
        for (int a = 0; a < numAggCols; a++) {
          aggBlocks[a] = page.getBlock(numGroupByCols + a);
        }

        for (int pos = 0; pos < positionCount; pos++) {
          if (keyBlock.isNull(pos)) continue;
          String key = VarcharType.VARCHAR.getSlice(keyBlock, pos).toStringUtf8();
          long[] aggs = groups.get(key);
          if (aggs == null) {
            aggs = new long[numAggCols];
            for (int a = 0; a < numAggCols; a++) {
              if (!aggBlocks[a].isNull(pos)) {
                aggs[a] = BigintType.BIGINT.getLong(aggBlocks[a], pos);
              }
            }
            groups.put(key, aggs);
          } else {
            for (int a = 0; a < numAggCols; a++) {
              if (!aggBlocks[a].isNull(pos)) {
                aggs[a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
              }
            }
          }
        }
      }
    }

    if (groups.isEmpty()) {
      return List.of();
    }

    int groupCount = groups.size();
    BlockBuilder[] builders = new BlockBuilder[totalCols];
    builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
    for (int a = 0; a < numAggCols; a++) {
      builders[numGroupByCols + a] = BigintType.BIGINT.createBlockBuilder(null, groupCount);
    }

    for (java.util.Map.Entry<String, long[]> entry : groups.entrySet()) {
      VarcharType.VARCHAR.writeSlice(
          builders[0], io.airlift.slice.Slices.utf8Slice(entry.getKey()));
      long[] aggs = entry.getValue();
      for (int a = 0; a < numAggCols; a++) {
        BigintType.BIGINT.writeLong(builders[numGroupByCols + a], aggs[a]);
      }
    }

    Block[] blocks = new Block[totalCols];
    for (int i = 0; i < totalCols; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }

  /**
   * Accumulator for merging AVG values using weighted averaging. Each input row has (partial_avg,
   * partial_count). The merged result is sum(avg_i * count_i) / sum(count_i).
   */
  private static class WeightedAvgMergeAccumulator implements HashAggregationOperator.Accumulator {
    private final int avgColIdx;
    private final int countColIdx;
    private final Type avgType;
    private double weightedSum = 0;
    private long totalCount = 0;

    WeightedAvgMergeAccumulator(int avgColIdx, int countColIdx, Type avgType) {
      this.avgColIdx = avgColIdx;
      this.countColIdx = countColIdx;
      this.avgType = avgType;
    }

    @Override
    public void add(Page page, int position) {
      Block avgBlock = page.getBlock(avgColIdx);
      Block countBlock = page.getBlock(countColIdx);
      if (!avgBlock.isNull(position) && !countBlock.isNull(position)) {
        double avg = DoubleType.DOUBLE.getDouble(avgBlock, position);
        long count = BigintType.BIGINT.getLong(countBlock, position);
        weightedSum += avg * count;
        totalCount += count;
      }
    }

    @Override
    public void writeTo(BlockBuilder builder) {
      if (totalCount == 0) {
        builder.appendNull();
      } else {
        DoubleType.DOUBLE.writeDouble(builder, weightedSum / totalCount);
      }
    }
  }

  /**
   * Convenience overload that uses SQL-standard null ordering defaults: NULLS LAST for ASC, NULLS
   * FIRST for DESC.
   */
  public List<Page> mergeSorted(
      List<List<Page>> shardResults,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Type> columnTypes,
      long limit) {
    return mergeSorted(
        shardResults,
        sortColumnIndices,
        ascending,
        defaultNullsFirst(ascending),
        columnTypes,
        limit);
  }

  /**
   * Merge-sort shard results for ORDER BY with limit. For small limits (less than half the total
   * rows), uses a bounded max-heap (O(n log k)) instead of full sort (O(n log n)). For larger
   * limits, falls back to full sort.
   *
   * @param shardResults pages from each shard
   * @param sortColumnIndices column indices to sort by
   * @param ascending sort directions for each column
   * @param nullsFirst true to place nulls before non-null values, false for nulls last (per column)
   * @param columnTypes Trino types for each column
   * @param limit maximum number of rows to return
   * @return sorted and limited result pages
   */
  public List<Page> mergeSorted(
      List<List<Page>> shardResults,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Boolean> nullsFirst,
      List<Type> columnTypes,
      long limit) {

    int numCols = columnTypes.size();

    // Flatten all pages into rows
    List<Object[]> allRows = new ArrayList<>();
    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
          Object[] row = new Object[numCols];
          for (int col = 0; col < numCols; col++) {
            row[col] = extractValue(page, col, pos, columnTypes.get(col));
          }
          allRows.add(row);
        }
      }
    }

    if (allRows.isEmpty()) {
      return List.of();
    }

    Comparator<Object[]> comparator = buildComparator(sortColumnIndices, ascending, nullsFirst);
    int k = (int) Math.min(limit, allRows.size());

    List<Object[]> limited;
    if (k < allRows.size() / 2 && k < 10000) {
      // Bounded heap path: O(n log k) — efficient for small limits
      PriorityQueue<Object[]> heap = new PriorityQueue<>(k + 1, comparator.reversed());
      for (Object[] row : allRows) {
        heap.offer(row);
        if (heap.size() > k) {
          heap.poll();
        }
      }
      // Extract and sort the top-k elements
      Object[] topRows = heap.toArray(new Object[0]);
      Object[][] typedRows = new Object[topRows.length][];
      for (int i = 0; i < topRows.length; i++) {
        typedRows[i] = (Object[]) topRows[i];
      }
      java.util.Arrays.sort(typedRows, comparator);
      limited = java.util.Arrays.asList(typedRows);
    } else {
      // Full sort path
      allRows.sort(comparator);
      limited = allRows.subList(0, k);
    }

    return List.of(buildPage(limited, columnTypes));
  }

  /** Extract a typed value from a Page at the given column and row position. */
  private Object extractValue(Page page, int channel, int position, Type type) {
    Block block = page.getBlock(channel);
    if (block.isNull(position)) {
      return null;
    }
    if (type instanceof BigintType) {
      return BigintType.BIGINT.getLong(block, position);
    } else if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, position);
    } else if (type instanceof BooleanType) {
      return BooleanType.BOOLEAN.getBoolean(block, position);
    } else if (type instanceof VarcharType) {
      return VarcharType.VARCHAR.getSlice(block, position).toStringUtf8();
    } else {
      // Default: try getLong for integer-like types
      return type.getLong(block, position);
    }
  }

  /** Build a string key from the group-by column values of a row. */
  private String buildGroupKey(Object[] row, int numGroupByCols) {
    if (numGroupByCols == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numGroupByCols; i++) {
      if (i > 0) {
        sb.append('\0');
      }
      Object val = row[i];
      sb.append(val == null ? "null" : val.toString());
    }
    return sb.toString();
  }

  /**
   * Merge a single aggregate expression across all rows in a group.
   *
   * @param aggExpr the aggregate expression string (e.g., "COUNT(*)", "AVG(col)")
   * @param groupRows all rows for this group from all shards
   * @param colIdx column index of this aggregate in the row array
   * @param allAggExprs all aggregate expressions (for finding companion COUNT for AVG)
   * @param numGroupByCols number of group-by columns (offset for agg column indices)
   */
  private Object mergeAggregate(
      String aggExpr,
      List<Object[]> groupRows,
      int colIdx,
      List<String> allAggExprs,
      int numGroupByCols) {
    Matcher matcher = AGG_PATTERN.matcher(aggExpr);
    if (!matcher.matches()) {
      throw new UnsupportedOperationException("Unsupported aggregate expression: " + aggExpr);
    }
    String funcName = matcher.group(1).toUpperCase(Locale.ROOT);

    switch (funcName) {
      case "COUNT":
        return sumValues(groupRows, colIdx);
      case "SUM":
        return sumValues(groupRows, colIdx);
      case "MIN":
        return minValue(groupRows, colIdx);
      case "MAX":
        return maxValue(groupRows, colIdx);
      case "AVG":
        return weightedAvgValues(groupRows, colIdx, allAggExprs, numGroupByCols);
      default:
        throw new UnsupportedOperationException("Unsupported aggregate function: " + funcName);
    }
  }

  /** Sum numeric values for a given column across all rows. */
  private Number sumValues(List<Object[]> rows, int colIdx) {
    long sum = 0;
    boolean hasDouble = false;
    double dSum = 0.0;

    for (Object[] row : rows) {
      Number val = (Number) row[colIdx];
      if (val != null) {
        if (val instanceof Double || val instanceof Float) {
          hasDouble = true;
          dSum += val.doubleValue();
        } else {
          sum += val.longValue();
          dSum += val.doubleValue();
        }
      }
    }
    return hasDouble ? dSum : sum;
  }

  /**
   * Find the minimum value for a given column across all rows. Supports both numeric and string
   * types.
   */
  @SuppressWarnings("unchecked")
  private Object minValue(List<Object[]> rows, int colIdx) {
    Object min = null;
    for (Object[] row : rows) {
      Object val = row[colIdx];
      if (val != null) {
        if (min == null || ((Comparable<Object>) val).compareTo(min) < 0) {
          min = val;
        }
      }
    }
    return min;
  }

  /**
   * Find the maximum value for a given column across all rows. Supports both numeric and string
   * types.
   */
  @SuppressWarnings("unchecked")
  private Object maxValue(List<Object[]> rows, int colIdx) {
    Object max = null;
    for (Object[] row : rows) {
      Object val = row[colIdx];
      if (val != null) {
        if (max == null || ((Comparable<Object>) val).compareTo(max) > 0) {
          max = val;
        }
      }
    }
    return max;
  }

  /**
   * Compute a weighted average using the companion COUNT column for proper multi-shard merge. Each
   * shard emits a partial average and a partial count. The correct merge is: sum(avg_i * count_i) /
   * sum(count_i). If no COUNT column is found, falls back to simple averaging (correct for single
   * shard).
   */
  private Number weightedAvgValues(
      List<Object[]> rows, int avgColIdx, List<String> allAggExprs, int numGroupByCols) {
    // Find a companion COUNT(*) or COUNT(col) column for weighting
    int countColIdx = findCountColumnIndex(allAggExprs, numGroupByCols);

    if (countColIdx >= 0) {
      // Weighted average: sum(avg * count) / sum(count)
      double weightedSum = 0.0;
      long totalCount = 0;
      for (Object[] row : rows) {
        Number avg = (Number) row[avgColIdx];
        Number cnt = (Number) row[countColIdx];
        if (avg != null && cnt != null) {
          weightedSum += avg.doubleValue() * cnt.longValue();
          totalCount += cnt.longValue();
        }
      }
      return totalCount > 0 ? weightedSum / totalCount : null;
    }

    // Fallback: simple average (only correct for single shard)
    double sum = 0.0;
    int count = 0;
    for (Object[] row : rows) {
      Number val = (Number) row[avgColIdx];
      if (val != null) {
        sum += val.doubleValue();
        count++;
      }
    }
    return count > 0 ? sum / count : null;
  }

  /**
   * Find the column index of a COUNT(*) or non-distinct COUNT aggregate in the function list.
   * Returns -1 if not found.
   */
  private int findCountColumnIndex(List<String> aggExprs, int numGroupByCols) {
    for (int i = 0; i < aggExprs.size(); i++) {
      Matcher m = AGG_PATTERN.matcher(aggExprs.get(i));
      if (m.matches()) {
        String func = m.group(1).toUpperCase(Locale.ROOT);
        boolean isDistinct = m.group(2) != null;
        if ("COUNT".equals(func) && !isDistinct) {
          return numGroupByCols + i;
        }
      }
    }
    return -1;
  }

  /**
   * Build a composite comparator for sorting rows by multiple column indices with independent sort
   * directions and null orderings.
   */
  @SuppressWarnings("unchecked")
  private Comparator<Object[]> buildComparator(
      List<Integer> sortColumnIndices, List<Boolean> ascending, List<Boolean> nullsFirst) {

    Comparator<Object[]> comparator = (a, b) -> 0;

    for (int i = 0; i < sortColumnIndices.size(); i++) {
      int colIdx = sortColumnIndices.get(i);
      boolean asc = ascending.get(i);
      boolean nf = nullsFirst.get(i);

      Comparator<Object[]> keyComparator =
          (row1, row2) -> {
            Object v1 = row1[colIdx];
            Object v2 = row2[colIdx];
            if (v1 == null && v2 == null) return 0;
            if (v1 == null) return nf ? -1 : 1;
            if (v2 == null) return nf ? 1 : -1;

            int cmp;
            if (v1 instanceof Comparable && v2 instanceof Comparable) {
              cmp = ((Comparable<Object>) v1).compareTo(v2);
            } else {
              cmp = v1.toString().compareTo(v2.toString());
            }
            return asc ? cmp : -cmp;
          };

      comparator = comparator.thenComparing(keyComparator);
    }
    return comparator;
  }

  /** Build a Page from a list of rows (Object arrays) using the given column types. */
  private Page buildPage(List<Object[]> rows, List<Type> columnTypes) {
    int numCols = columnTypes.size();
    BlockBuilder[] builders = new BlockBuilder[numCols];
    for (int i = 0; i < numCols; i++) {
      builders[i] = columnTypes.get(i).createBlockBuilder(null, rows.size());
    }

    for (Object[] row : rows) {
      for (int col = 0; col < numCols; col++) {
        Object val = row[col];
        if (val == null) {
          builders[col].appendNull();
        } else {
          appendValue(builders[col], columnTypes.get(col), val);
        }
      }
    }

    io.trino.spi.block.Block[] blocks = new io.trino.spi.block.Block[numCols];
    for (int i = 0; i < numCols; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  /** Append a typed value to a BlockBuilder. */
  private void appendValue(BlockBuilder builder, Type type, Object value) {
    if (type instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, ((Number) value).longValue());
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, ((Number) value).doubleValue());
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, (Boolean) value);
    } else if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(value.toString()));
    } else {
      // Default: try writeLong for integer-like types
      type.writeLong(builder, ((Number) value).longValue());
    }
  }

  /** Derive Trino-compatible null ordering: NULLS LAST for both ASC and DESC. */
  private static List<Boolean> defaultNullsFirst(List<Boolean> ascending) {
    List<Boolean> defaults = new ArrayList<>(ascending.size());
    for (int i = 0; i < ascending.size(); i++) {
      defaults.add(false); // Trino defaults to NULLS LAST
    }
    return defaults;
  }
}
