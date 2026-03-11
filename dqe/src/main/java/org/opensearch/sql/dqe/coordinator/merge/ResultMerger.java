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

  /**
   * Fused merge + sort + limit for aggregation queries. Instead of building a full Page with all
   * groups and then sorting, this method performs the top-N selection directly on the raw hash map
   * data and builds only the final top-N Page. For high-cardinality GROUP BY with small LIMIT
   * (e.g., 22K groups with LIMIT 10), this avoids building BlockBuilder entries for 22K rows and
   * sorting Integer objects for 22K elements.
   *
   * <p>Falls back to separate merge + sort when the fast path is not applicable.
   *
   * @param shardResults pages from each shard
   * @param finalAggNode the FINAL aggregation node
   * @param columnTypes Trino types for output columns
   * @param sortColumnIndices column indices to sort by (in the agg output)
   * @param ascending sort directions per column
   * @param nullsFirst null ordering per column
   * @param limit maximum rows to return (must be positive)
   * @return merged, sorted, and limited result pages
   */
  public List<Page> mergeAggregationAndSort(
      List<List<Page>> shardResults,
      AggregationNode finalAggNode,
      List<Type> columnTypes,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Boolean> nullsFirst,
      long limit) {

    List<String> groupByKeys = finalAggNode.getGroupByKeys();
    List<String> aggregateFunctions = finalAggNode.getAggregateFunctions();
    int numGroupByCols = groupByKeys.size();
    int numAggCols = aggregateFunctions.size();

    // Fast numeric path with fused top-N
    if (canUseFastNumericMerge(numGroupByCols, columnTypes, aggregateFunctions)) {
      return mergeAggregationFastNumericWithSort(
          shardResults,
          numGroupByCols,
          numAggCols,
          columnTypes,
          aggregateFunctions,
          sortColumnIndices,
          ascending,
          nullsFirst,
          limit);
    }

    // Fast mixed path with fused top-N
    if (canUseFastMixedMerge(numGroupByCols, columnTypes, aggregateFunctions)) {
      return mergeAggregationFastMixedWithSort(
          shardResults,
          numGroupByCols,
          numAggCols,
          columnTypes,
          aggregateFunctions,
          sortColumnIndices,
          ascending,
          nullsFirst,
          limit);
    }

    // Fallback: separate merge + sort
    List<Page> merged = mergeAggregation(shardResults, finalAggNode, columnTypes);
    return mergeSorted(
        List.of(merged), sortColumnIndices, ascending, nullsFirst, columnTypes, limit);
  }

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

    // Fast path: mixed numeric+varchar keys with COUNT/SUM-only aggregates (Q15, Q31, etc.).
    // Uses HashMap with a compound MixedKey that avoids GroupKey/Accumulator overhead.
    if (canUseFastMixedMerge(numGroupByCols, columnTypes, aggregateFunctions)) {
      return mergeAggregationFastMixed(
          shardResults, numGroupByCols, numAggCols, columnTypes, aggregateFunctions);
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
   * Fused fast numeric merge with top-N sort. Same hash map building as {@link
   * #mergeAggregationFastNumeric}, but instead of building a Page with ALL groups, performs top-N
   * selection directly on the hash map slots and only builds a Page with the top-N rows.
   *
   * <p>For Q31 (22K groups, LIMIT 10), this avoids building 22K BlockBuilder entries and sorting
   * 22K Integer objects. Instead: scan 22K slots with a bounded int heap of size 10, then build
   * only 10 BlockBuilder entries.
   */
  private List<Page> mergeAggregationFastNumericWithSort(
      List<List<Page>> shardResults,
      int numGroupByCols,
      int numAggCols,
      List<Type> columnTypes,
      List<String> aggregateFunctions,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Boolean> nullsFirst,
      long limit) {

    int totalCols = numGroupByCols + numAggCols;

    // Classify aggregates (same as mergeAggregationFastNumeric)
    boolean[] isDoubleAgg = new boolean[numAggCols];
    boolean[] isAvgAgg = new boolean[numAggCols];
    int countAggIdx = -1;
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
    final int finalCountAggIdx = countAggIdx;

    // === Build hash map (identical to mergeAggregationFastNumeric) ===
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
        Block[] keyBlocks = new Block[numGroupByCols];
        for (int k = 0; k < numGroupByCols; k++) {
          keyBlocks[k] = page.getBlock(k);
        }
        Block[] aggBlocks = new Block[numAggCols];
        for (int a = 0; a < numAggCols; a++) {
          aggBlocks[a] = page.getBlock(numGroupByCols + a);
        }

        for (int pos = 0; pos < positionCount; pos++) {
          long[] keyValues = new long[numGroupByCols];
          for (int k = 0; k < numGroupByCols; k++) {
            keyValues[k] = columnTypes.get(k).getLong(keyBlocks[k], pos);
          }
          int hash = 1;
          for (int k = 0; k < numGroupByCols; k++) {
            hash = hash * 31 + Long.hashCode(keyValues[k]);
          }
          int mask = capacity - 1;
          int slot = hash & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              mapKeys[slot] = keyValues;
              long[] longAggs = new long[numAggCols];
              double[] doubleAggs = new double[numAggCols];
              long countVal = 0;
              if (finalCountAggIdx >= 0 && !aggBlocks[finalCountAggIdx].isNull(pos)) {
                countVal = BigintType.BIGINT.getLong(aggBlocks[finalCountAggIdx], pos);
              }
              for (int a = 0; a < numAggCols; a++) {
                if (!aggBlocks[a].isNull(pos)) {
                  if (isAvgAgg[a]) {
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
            long[] existing = mapKeys[slot];
            boolean match = true;
            for (int k = 0; k < numGroupByCols; k++) {
              if (existing[k] != keyValues[k]) {
                match = false;
                break;
              }
            }
            if (match) {
              long countVal2 = 0;
              if (finalCountAggIdx >= 0 && !aggBlocks[finalCountAggIdx].isNull(pos)) {
                countVal2 = BigintType.BIGINT.getLong(aggBlocks[finalCountAggIdx], pos);
              }
              for (int a = 0; a < numAggCols; a++) {
                if (!aggBlocks[a].isNull(pos)) {
                  if (isAvgAgg[a]) {
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

    // === Top-N selection directly on hash map slots ===
    int k = (int) Math.min(limit, size);

    // Resolve sort value extractor: for each sort key, determine which array and index to read.
    // Sort keys reference the output columns: [groupBy0, ..., agg0, agg1, ...].
    // Group keys -> mapKeys[slot][keyIdx], Agg long -> mapLongAggs[slot][aggIdx],
    // Agg double -> mapDoubleAggs[slot][aggIdx], AVG -> compute from doubleAggs/longAggs.
    final int numSortKeys = sortColumnIndices.size();
    final boolean[] sortAsc = new boolean[numSortKeys];
    final boolean[] sortNf = new boolean[numSortKeys];
    final int[] sortKeyGroup = new int[numSortKeys]; // -1 if agg, else group key index
    final int[] sortKeyAgg = new int[numSortKeys]; // -1 if group, else agg index
    final boolean[] sortKeyIsDouble = new boolean[numSortKeys];
    final boolean[] sortKeyIsAvg = new boolean[numSortKeys];

    for (int i = 0; i < numSortKeys; i++) {
      sortAsc[i] = ascending.get(i);
      sortNf[i] = nullsFirst.get(i);
      int colIdx = sortColumnIndices.get(i);
      if (colIdx < numGroupByCols) {
        sortKeyGroup[i] = colIdx;
        sortKeyAgg[i] = -1;
      } else {
        sortKeyGroup[i] = -1;
        int aggIdx = colIdx - numGroupByCols;
        sortKeyAgg[i] = aggIdx;
        sortKeyIsDouble[i] = isDoubleAgg[aggIdx];
        sortKeyIsAvg[i] = isAvgAgg[aggIdx];
      }
    }

    // Capture final references for lambda
    final long[][] fMapKeys = mapKeys;
    final long[][] fMapLongAggs = mapLongAggs;
    final double[][] fMapDoubleAggs = mapDoubleAggs;
    final boolean[] fMapOccupied = mapOccupied;
    final int fCapacity = capacity;
    final int fFinalCountAggIdx = finalCountAggIdx;

    // Compare two hash map slots by the sort key values
    Comparator<Integer> slotComparator =
        (s1, s2) -> {
          for (int i = 0; i < numSortKeys; i++) {
            long v1, v2;
            if (sortKeyGroup[i] >= 0) {
              v1 = fMapKeys[s1][sortKeyGroup[i]];
              v2 = fMapKeys[s2][sortKeyGroup[i]];
            } else if (sortKeyIsAvg[i]) {
              // AVG: compute weighted average for comparison
              int aggIdx = sortKeyAgg[i];
              long cnt1 = fFinalCountAggIdx >= 0 ? fMapLongAggs[s1][fFinalCountAggIdx] : 1;
              long cnt2 = fFinalCountAggIdx >= 0 ? fMapLongAggs[s2][fFinalCountAggIdx] : 1;
              double avg1 = cnt1 > 0 ? fMapDoubleAggs[s1][aggIdx] / cnt1 : 0.0;
              double avg2 = cnt2 > 0 ? fMapDoubleAggs[s2][aggIdx] / cnt2 : 0.0;
              int cmp = Double.compare(avg1, avg2);
              if (cmp != 0) return sortAsc[i] ? cmp : -cmp;
              continue;
            } else if (sortKeyIsDouble[i]) {
              double d1 = fMapDoubleAggs[s1][sortKeyAgg[i]];
              double d2 = fMapDoubleAggs[s2][sortKeyAgg[i]];
              int cmp = Double.compare(d1, d2);
              if (cmp != 0) return sortAsc[i] ? cmp : -cmp;
              continue;
            } else {
              v1 = fMapLongAggs[s1][sortKeyAgg[i]];
              v2 = fMapLongAggs[s2][sortKeyAgg[i]];
            }
            int cmp = Long.compare(v1, v2);
            if (cmp != 0) return sortAsc[i] ? cmp : -cmp;
          }
          return 0;
        };

    // Bounded max-heap: the worst element is at the top for O(n log k) selection
    PriorityQueue<Integer> heap = new PriorityQueue<>(k + 1, slotComparator.reversed());
    for (int s = 0; s < fCapacity; s++) {
      if (fMapOccupied[s]) {
        heap.offer(s);
        if (heap.size() > k) {
          heap.poll();
        }
      }
    }

    // Extract top-k slots in sorted order
    int heapSize = heap.size();
    Integer[] topSlots = new Integer[heapSize];
    for (int i = heapSize - 1; i >= 0; i--) {
      topSlots[i] = heap.poll();
    }
    java.util.Arrays.sort(topSlots, slotComparator);

    // Build output Page only for the top-k entries
    BlockBuilder[] builders = new BlockBuilder[totalCols];
    for (int i = 0; i < totalCols; i++) {
      builders[i] = columnTypes.get(i).createBlockBuilder(null, heapSize);
    }
    for (int slot : topSlots) {
      for (int ki = 0; ki < numGroupByCols; ki++) {
        columnTypes.get(ki).writeLong(builders[ki], fMapKeys[slot][ki]);
      }
      for (int a = 0; a < numAggCols; a++) {
        if (isAvgAgg[a]) {
          long totalCount = (fFinalCountAggIdx >= 0) ? fMapLongAggs[slot][fFinalCountAggIdx] : 1;
          double avgValue = totalCount > 0 ? fMapDoubleAggs[slot][a] / totalCount : 0.0;
          DoubleType.DOUBLE.writeDouble(builders[numGroupByCols + a], avgValue);
        } else if (isDoubleAgg[a]) {
          DoubleType.DOUBLE.writeDouble(builders[numGroupByCols + a], fMapDoubleAggs[slot][a]);
        } else {
          BigintType.BIGINT.writeLong(builders[numGroupByCols + a], fMapLongAggs[slot][a]);
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
   * Fused fast mixed-key merge with top-N sort. Same hash map building as {@link
   * #mergeAggregationFastMixed}, but performs top-N selection directly on the hash map entries and
   * only builds a Page with the top-N rows.
   */
  private List<Page> mergeAggregationFastMixedWithSort(
      List<List<Page>> shardResults,
      int numGroupByCols,
      int numAggCols,
      List<Type> columnTypes,
      List<String> aggregateFunctions,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Boolean> nullsFirst,
      long limit) {

    int totalCols = numGroupByCols + numAggCols;

    // Classify keys
    boolean[] isVarcharKey = new boolean[numGroupByCols];
    for (int k = 0; k < numGroupByCols; k++) {
      isVarcharKey[k] = columnTypes.get(k) instanceof VarcharType;
    }

    // Classify aggregates
    boolean[] isAvgAgg = new boolean[numAggCols];
    int countAggIdx = -1;
    for (int a = 0; a < numAggCols; a++) {
      Matcher m = AGG_PATTERN.matcher(aggregateFunctions.get(a));
      if (m.matches()) {
        String funcName = m.group(1).toUpperCase(Locale.ROOT);
        if ("AVG".equals(funcName)) {
          isAvgAgg[a] = true;
        } else if ("COUNT".equals(funcName) && m.group(2) == null) {
          countAggIdx = a;
        }
      }
    }
    boolean hasAvg = false;
    for (boolean b : isAvgAgg) {
      if (b) {
        hasAvg = true;
        break;
      }
    }

    // Build HashMap (same as mergeAggregationFastMixed)
    int estimatedGroups = 0;
    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        estimatedGroups += page.getPositionCount();
      }
    }
    java.util.HashMap<MixedMergeKey, long[]> groups =
        new java.util.HashMap<>(Math.max(estimatedGroups * 3 / 4, 16));
    java.util.HashMap<MixedMergeKey, double[]> doubleGroups =
        hasAvg ? new java.util.HashMap<>() : null;
    final int fCountAggIdx = countAggIdx;

    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        int positionCount = page.getPositionCount();
        Block[] keyBlocks = new Block[numGroupByCols];
        for (int k = 0; k < numGroupByCols; k++) {
          keyBlocks[k] = page.getBlock(k);
        }
        Block[] aggBlocks = new Block[numAggCols];
        for (int a = 0; a < numAggCols; a++) {
          aggBlocks[a] = page.getBlock(numGroupByCols + a);
        }
        for (int pos = 0; pos < positionCount; pos++) {
          long[] numericKeys = new long[numGroupByCols];
          io.airlift.slice.Slice[] sliceKeys = new io.airlift.slice.Slice[numGroupByCols];
          int hash = 1;
          for (int k = 0; k < numGroupByCols; k++) {
            if (isVarcharKey[k]) {
              io.airlift.slice.Slice slice = VarcharType.VARCHAR.getSlice(keyBlocks[k], pos);
              sliceKeys[k] = slice;
              hash = hash * 31 + slice.hashCode();
            } else {
              long val = columnTypes.get(k).getLong(keyBlocks[k], pos);
              numericKeys[k] = val;
              hash = hash * 31 + Long.hashCode(val);
            }
          }
          MixedMergeKey key = new MixedMergeKey(numericKeys, sliceKeys, isVarcharKey, hash);
          long[] aggs = groups.get(key);
          if (aggs == null) {
            aggs = new long[numAggCols];
            groups.put(key, aggs);
            if (hasAvg) {
              doubleGroups.put(key, new double[numAggCols]);
            }
          }
          for (int a = 0; a < numAggCols; a++) {
            if (isAvgAgg[a]) {
              double avg = DoubleType.DOUBLE.getDouble(aggBlocks[a], pos);
              long count =
                  fCountAggIdx >= 0 ? BigintType.BIGINT.getLong(aggBlocks[fCountAggIdx], pos) : 1;
              doubleGroups.get(key)[a] += avg * count;
            } else {
              aggs[a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
            }
          }
        }
      }
    }

    if (groups.isEmpty()) return List.of();

    // === Top-N selection on HashMap entries ===
    int k = (int) Math.min(limit, groups.size());
    final int numSortKeys = sortColumnIndices.size();

    // Build a list of entries for top-N selection
    @SuppressWarnings("unchecked")
    java.util.Map.Entry<MixedMergeKey, long[]>[] entries =
        groups.entrySet().toArray(new java.util.Map.Entry[0]);

    Comparator<java.util.Map.Entry<MixedMergeKey, long[]>> entryComparator =
        (e1, e2) -> {
          for (int i = 0; i < numSortKeys; i++) {
            int colIdx = sortColumnIndices.get(i);
            boolean asc = ascending.get(i);
            if (colIdx < numGroupByCols) {
              // Group key comparison
              if (isVarcharKey[colIdx]) {
                int cmp = e1.getKey().sliceKeys[colIdx].compareTo(e2.getKey().sliceKeys[colIdx]);
                if (cmp != 0) return asc ? cmp : -cmp;
              } else {
                int cmp =
                    Long.compare(e1.getKey().numericKeys[colIdx], e2.getKey().numericKeys[colIdx]);
                if (cmp != 0) return asc ? cmp : -cmp;
              }
            } else {
              int aggIdx = colIdx - numGroupByCols;
              if (isAvgAgg[aggIdx]) {
                long cnt1 = fCountAggIdx >= 0 ? e1.getValue()[fCountAggIdx] : 1;
                long cnt2 = fCountAggIdx >= 0 ? e2.getValue()[fCountAggIdx] : 1;
                double avg1 = cnt1 > 0 ? doubleGroups.get(e1.getKey())[aggIdx] / cnt1 : 0.0;
                double avg2 = cnt2 > 0 ? doubleGroups.get(e2.getKey())[aggIdx] / cnt2 : 0.0;
                int cmp = Double.compare(avg1, avg2);
                if (cmp != 0) return asc ? cmp : -cmp;
              } else {
                int cmp = Long.compare(e1.getValue()[aggIdx], e2.getValue()[aggIdx]);
                if (cmp != 0) return asc ? cmp : -cmp;
              }
            }
          }
          return 0;
        };

    // Bounded heap on entries
    PriorityQueue<java.util.Map.Entry<MixedMergeKey, long[]>> heap =
        new PriorityQueue<>(k + 1, entryComparator.reversed());
    for (java.util.Map.Entry<MixedMergeKey, long[]> entry : entries) {
      heap.offer(entry);
      if (heap.size() > k) heap.poll();
    }

    // Extract in sorted order
    int heapSize = heap.size();
    @SuppressWarnings("unchecked")
    java.util.Map.Entry<MixedMergeKey, long[]>[] topEntries = new java.util.Map.Entry[heapSize];
    for (int i = heapSize - 1; i >= 0; i--) {
      topEntries[i] = heap.poll();
    }
    java.util.Arrays.sort(topEntries, entryComparator);

    // Build output Page only for top-k
    BlockBuilder[] builders = new BlockBuilder[totalCols];
    for (int ki = 0; ki < numGroupByCols; ki++) {
      builders[ki] = columnTypes.get(ki).createBlockBuilder(null, heapSize);
    }
    for (int a = 0; a < numAggCols; a++) {
      Type outType = isAvgAgg[a] ? DoubleType.DOUBLE : BigintType.BIGINT;
      builders[numGroupByCols + a] = outType.createBlockBuilder(null, heapSize);
    }
    for (java.util.Map.Entry<MixedMergeKey, long[]> entry : topEntries) {
      MixedMergeKey key = entry.getKey();
      long[] aggs = entry.getValue();
      for (int ki = 0; ki < numGroupByCols; ki++) {
        if (isVarcharKey[ki]) {
          VarcharType.VARCHAR.writeSlice(builders[ki], key.sliceKeys[ki]);
        } else {
          columnTypes.get(ki).writeLong(builders[ki], key.numericKeys[ki]);
        }
      }
      for (int a = 0; a < numAggCols; a++) {
        if (isAvgAgg[a]) {
          long totalCount = fCountAggIdx >= 0 ? aggs[fCountAggIdx] : 1;
          double avg = totalCount > 0 ? doubleGroups.get(key)[a] / totalCount : 0.0;
          DoubleType.DOUBLE.writeDouble(builders[numGroupByCols + a], avg);
        } else {
          BigintType.BIGINT.writeLong(builders[numGroupByCols + a], aggs[a]);
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
   * Fast varchar merge: uses HashMap&lt;Slice, long[]&gt; to merge single-VARCHAR-key GROUP BY
   * results with COUNT/SUM aggregates. Uses Slice keys directly from blocks to avoid String
   * intermediate allocation. Slices provide proper hashCode/equals and are written directly to
   * BlockBuilder on output, eliminating double String-to-Slice conversion.
   */
  private List<Page> mergeAggregationFastVarchar(
      List<List<Page>> shardResults, int numGroupByCols, int numAggCols, List<Type> columnTypes) {

    int totalCols = numGroupByCols + numAggCols;

    // Estimate initial capacity from total rows across all shards
    int estimatedGroups = 0;
    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        estimatedGroups += page.getPositionCount();
      }
    }
    // Groups <= total rows; use 75% as heuristic (many groups overlap across shards)
    java.util.HashMap<io.airlift.slice.Slice, long[]> groups =
        new java.util.HashMap<>(Math.max(estimatedGroups * 3 / 4, 16));

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
          // Use Slice directly as key — avoids String allocation and double conversion.
          // The Slice view is safe to use as a HashMap key without copying because
          // all source pages (and their backing blocks) are kept alive via the
          // shardResults parameter on the call stack throughout the merge + output
          // building. The Slice's hashCode/equals correctly reference the underlying
          // block bytes.
          io.airlift.slice.Slice keySlice = VarcharType.VARCHAR.getSlice(keyBlock, pos);
          long[] aggs = groups.get(keySlice);
          if (aggs == null) {
            aggs = new long[numAggCols];
            for (int a = 0; a < numAggCols; a++) {
              if (!aggBlocks[a].isNull(pos)) {
                aggs[a] = BigintType.BIGINT.getLong(aggBlocks[a], pos);
              }
            }
            groups.put(keySlice, aggs);
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

    for (java.util.Map.Entry<io.airlift.slice.Slice, long[]> entry : groups.entrySet()) {
      // Write Slice directly — no String intermediate needed
      VarcharType.VARCHAR.writeSlice(builders[0], entry.getKey());
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
   * Check if we can use the fast mixed-key merge path. Requirements:
   *
   * <ul>
   *   <li>All group-by key types are either long-representable or VarcharType
   *   <li>At least one key is VarcharType (otherwise the fast numeric path handles it)
   *   <li>All aggregate functions are COUNT or SUM with BigintType output, or AVG with a companion
   *       COUNT
   * </ul>
   */
  private boolean canUseFastMixedMerge(
      int numGroupByCols, List<Type> columnTypes, List<String> aggregateFunctions) {
    if (numGroupByCols < 1) return false;
    boolean hasVarchar = false;
    for (int i = 0; i < numGroupByCols; i++) {
      Type t = columnTypes.get(i);
      if (t instanceof VarcharType) {
        hasVarchar = true;
      } else if (!(t instanceof BigintType
          || t instanceof io.trino.spi.type.IntegerType
          || t instanceof io.trino.spi.type.TimestampType
          || t instanceof io.trino.spi.type.SmallintType
          || t instanceof io.trino.spi.type.TinyintType
          || t instanceof BooleanType)) {
        return false;
      }
    }
    if (!hasVarchar) return false; // Use fast numeric path instead
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
    if (hasAvg && !hasCount) return false;
    return true;
  }

  /**
   * Fast mixed-key merge: handles GROUP BY with any combination of numeric and varchar keys, with
   * COUNT/SUM/AVG aggregates. Uses a compound key (MixedMergeKey) that stores numeric keys as longs
   * and varchar keys as Slices, with pre-computed hash. This avoids the overhead of the generic
   * HashAggregationOperator (GroupKey allocation, Accumulator interface dispatch, Object-based key
   * extraction).
   *
   * <p>For AVG aggregates, accumulates (avg * count) as weightedSum in the double[] array and
   * divides by total count at output time, similar to the fast numeric merge path.
   */
  private List<Page> mergeAggregationFastMixed(
      List<List<Page>> shardResults,
      int numGroupByCols,
      int numAggCols,
      List<Type> columnTypes,
      List<String> aggregateFunctions) {

    int totalCols = numGroupByCols + numAggCols;

    // Classify keys: which are varchar, which are numeric
    boolean[] isVarcharKey = new boolean[numGroupByCols];
    for (int k = 0; k < numGroupByCols; k++) {
      isVarcharKey[k] = columnTypes.get(k) instanceof VarcharType;
    }

    // Classify aggregates: identify AVG columns and their companion COUNT column
    boolean[] isAvgAgg = new boolean[numAggCols];
    int countAggIdx = -1;
    for (int a = 0; a < numAggCols; a++) {
      Matcher m = AGG_PATTERN.matcher(aggregateFunctions.get(a));
      if (m.matches()) {
        String funcName = m.group(1).toUpperCase(Locale.ROOT);
        if ("AVG".equals(funcName)) {
          isAvgAgg[a] = true;
        } else if ("COUNT".equals(funcName) && m.group(2) == null) {
          countAggIdx = a;
        }
      }
    }
    boolean hasAvg = false;
    for (boolean b : isAvgAgg) {
      if (b) {
        hasAvg = true;
        break;
      }
    }

    // Estimate initial capacity
    int estimatedGroups = 0;
    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        estimatedGroups += page.getPositionCount();
      }
    }

    // HashMap with compound key -> long[] aggs (+ double[] for AVG)
    java.util.HashMap<MixedMergeKey, long[]> groups =
        new java.util.HashMap<>(Math.max(estimatedGroups * 3 / 4, 16));
    // Parallel double map for AVG weighted sums (only allocated if needed)
    java.util.HashMap<MixedMergeKey, double[]> doubleGroups =
        hasAvg ? new java.util.HashMap<>() : null;

    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        int positionCount = page.getPositionCount();

        Block[] keyBlocks = new Block[numGroupByCols];
        for (int k = 0; k < numGroupByCols; k++) {
          keyBlocks[k] = page.getBlock(k);
        }
        Block[] aggBlocks = new Block[numAggCols];
        for (int a = 0; a < numAggCols; a++) {
          aggBlocks[a] = page.getBlock(numGroupByCols + a);
        }

        for (int pos = 0; pos < positionCount; pos++) {
          // Build compound key
          long[] numericKeys = new long[numGroupByCols];
          io.airlift.slice.Slice[] sliceKeys = new io.airlift.slice.Slice[numGroupByCols];
          int hash = 1;
          for (int k = 0; k < numGroupByCols; k++) {
            if (isVarcharKey[k]) {
              io.airlift.slice.Slice slice = VarcharType.VARCHAR.getSlice(keyBlocks[k], pos);
              sliceKeys[k] = slice;
              hash = hash * 31 + slice.hashCode();
            } else {
              long val = columnTypes.get(k).getLong(keyBlocks[k], pos);
              numericKeys[k] = val;
              hash = hash * 31 + Long.hashCode(val);
            }
          }

          MixedMergeKey key = new MixedMergeKey(numericKeys, sliceKeys, isVarcharKey, hash);

          long[] aggs = groups.get(key);
          if (aggs == null) {
            aggs = new long[numAggCols];
            groups.put(key, aggs);
            if (hasAvg) {
              doubleGroups.put(key, new double[numAggCols]);
            }
          }

          // Accumulate
          for (int a = 0; a < numAggCols; a++) {
            if (isAvgAgg[a]) {
              // AVG: weighted accumulation
              double avg = DoubleType.DOUBLE.getDouble(aggBlocks[a], pos);
              long count =
                  countAggIdx >= 0 ? BigintType.BIGINT.getLong(aggBlocks[countAggIdx], pos) : 1;
              double[] dAggs = doubleGroups.get(key);
              dAggs[a] += avg * count;
            } else {
              aggs[a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
            }
          }
        }
      }
    }

    if (groups.isEmpty()) return List.of();

    // Build output Page
    int groupCount = groups.size();
    BlockBuilder[] builders = new BlockBuilder[totalCols];
    for (int k = 0; k < numGroupByCols; k++) {
      builders[k] = columnTypes.get(k).createBlockBuilder(null, groupCount);
    }
    for (int a = 0; a < numAggCols; a++) {
      Type outType = isAvgAgg[a] ? DoubleType.DOUBLE : BigintType.BIGINT;
      builders[numGroupByCols + a] = outType.createBlockBuilder(null, groupCount);
    }

    for (java.util.Map.Entry<MixedMergeKey, long[]> entry : groups.entrySet()) {
      MixedMergeKey key = entry.getKey();
      long[] aggs = entry.getValue();

      // Write keys
      for (int k = 0; k < numGroupByCols; k++) {
        if (isVarcharKey[k]) {
          VarcharType.VARCHAR.writeSlice(builders[k], key.sliceKeys[k]);
        } else {
          columnTypes.get(k).writeLong(builders[k], key.numericKeys[k]);
        }
      }

      // Write aggregates
      for (int a = 0; a < numAggCols; a++) {
        if (isAvgAgg[a]) {
          double[] dAggs = doubleGroups.get(key);
          // Get total count from the companion COUNT column
          long totalCount = countAggIdx >= 0 ? aggs[countAggIdx] : 1;
          double avg = totalCount > 0 ? dAggs[a] / totalCount : 0.0;
          DoubleType.DOUBLE.writeDouble(builders[numGroupByCols + a], avg);
        } else {
          BigintType.BIGINT.writeLong(builders[numGroupByCols + a], aggs[a]);
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
   * Compound key for mixed numeric+varchar GROUP BY merge. Stores numeric keys as longs and varchar
   * keys as Slices (avoiding String allocation). Pre-computes hashCode for fast HashMap lookups.
   */
  private static final class MixedMergeKey {
    final long[] numericKeys;
    final io.airlift.slice.Slice[] sliceKeys;
    final boolean[] isVarchar;
    private final int hash;

    MixedMergeKey(
        long[] numericKeys, io.airlift.slice.Slice[] sliceKeys, boolean[] isVarchar, int hash) {
      this.numericKeys = numericKeys;
      this.sliceKeys = sliceKeys;
      this.isVarchar = isVarchar;
      this.hash = hash;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof MixedMergeKey other)) return false;
      if (this.hash != other.hash) return false;
      for (int i = 0; i < isVarchar.length; i++) {
        if (isVarchar[i]) {
          if (!sliceKeys[i].equals(other.sliceKeys[i])) return false;
        } else {
          if (numericKeys[i] != other.numericKeys[i]) return false;
        }
      }
      return true;
    }
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
   * <p>When the input is a single Page (common for post-aggregation sort), uses a zero-allocation
   * block-level sort that compares values directly from Block objects using an index array,
   * avoiding the expensive per-row Object[] extraction and boxing that dominates sort time for
   * large result sets.
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

    // Fast path: if all input is in a single Page, use block-level sort (no Object[] extraction).
    // This is the common case for post-aggregation sort where mergeAggregation returns one Page.
    Page singlePage = extractSinglePage(shardResults);
    if (singlePage != null && singlePage.getPositionCount() > 0) {
      return sortSinglePage(
          singlePage, sortColumnIndices, ascending, nullsFirst, columnTypes, limit);
    }

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

  /**
   * Extract a single Page if the shard results contain exactly one non-empty page. Returns null if
   * there are multiple non-empty pages or no pages at all.
   */
  private static Page extractSinglePage(List<List<Page>> shardResults) {
    Page found = null;
    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        if (page.getPositionCount() > 0) {
          if (found != null) {
            return null; // Multiple non-empty pages
          }
          found = page;
        }
      }
    }
    return found;
  }

  /**
   * Sort a single Page using block-level comparisons with an index array, avoiding per-row Object[]
   * extraction. For small limits (LIMIT N with N much smaller than total rows), uses a bounded heap
   * on position indices for O(n log k) selection. Otherwise, sorts all indices and takes top-k.
   *
   * <p>This avoids the dominant cost of the generic mergeSorted path: N * numCols Object
   * allocations, N boxing operations for numeric types, and N String allocations for VARCHAR
   * columns. Instead, comparisons are performed directly on Block data (getLong, getDouble,
   * getSlice) and only the final k rows are materialized to the output Page.
   */
  private List<Page> sortSinglePage(
      Page page,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Boolean> nullsFirst,
      List<Type> columnTypes,
      long limit) {

    int totalRows = page.getPositionCount();
    int k = (int) Math.min(limit, totalRows);
    if (k <= 0) {
      return List.of();
    }

    // Pre-fetch sort column blocks and types for fast access
    int numSortKeys = sortColumnIndices.size();
    Block[] sortBlocks = new Block[numSortKeys];
    Type[] sortTypes = new Type[numSortKeys];
    boolean[] asc = new boolean[numSortKeys];
    boolean[] nf = new boolean[numSortKeys];
    for (int i = 0; i < numSortKeys; i++) {
      int colIdx = sortColumnIndices.get(i);
      sortBlocks[i] = page.getBlock(colIdx);
      sortTypes[i] = columnTypes.get(colIdx);
      asc[i] = ascending.get(i);
      nf[i] = nullsFirst.get(i);
    }

    // Build a comparator on position indices that reads directly from blocks
    java.util.Comparator<Integer> posComparator =
        (pos1, pos2) -> {
          for (int s = 0; s < numSortKeys; s++) {
            Block blk = sortBlocks[s];
            boolean null1 = blk.isNull(pos1);
            boolean null2 = blk.isNull(pos2);
            if (null1 && null2) continue;
            if (null1) return nf[s] ? -1 : 1;
            if (null2) return nf[s] ? 1 : -1;

            int cmp;
            Type t = sortTypes[s];
            if (t instanceof BigintType
                || t instanceof io.trino.spi.type.IntegerType
                || t instanceof io.trino.spi.type.SmallintType
                || t instanceof io.trino.spi.type.TinyintType
                || t instanceof io.trino.spi.type.TimestampType) {
              long v1 = t.getLong(blk, pos1);
              long v2 = t.getLong(blk, pos2);
              cmp = Long.compare(v1, v2);
            } else if (t instanceof DoubleType) {
              double v1 = DoubleType.DOUBLE.getDouble(blk, pos1);
              double v2 = DoubleType.DOUBLE.getDouble(blk, pos2);
              cmp = Double.compare(v1, v2);
            } else if (t instanceof VarcharType) {
              io.airlift.slice.Slice s1 = VarcharType.VARCHAR.getSlice(blk, pos1);
              io.airlift.slice.Slice s2 = VarcharType.VARCHAR.getSlice(blk, pos2);
              cmp = s1.compareTo(s2);
            } else if (t instanceof BooleanType) {
              boolean b1 = BooleanType.BOOLEAN.getBoolean(blk, pos1);
              boolean b2 = BooleanType.BOOLEAN.getBoolean(blk, pos2);
              cmp = Boolean.compare(b1, b2);
            } else {
              // Fallback: compare as longs
              long v1 = t.getLong(blk, pos1);
              long v2 = t.getLong(blk, pos2);
              cmp = Long.compare(v1, v2);
            }
            if (cmp != 0) {
              return asc[s] ? cmp : -cmp;
            }
          }
          return 0;
        };

    // Select top-k positions
    int[] selectedPositions;
    if (k < totalRows / 2 && k < 10000) {
      // Bounded heap on position indices: O(n log k)
      PriorityQueue<Integer> heap = new PriorityQueue<>(k + 1, posComparator.reversed());
      for (int pos = 0; pos < totalRows; pos++) {
        heap.offer(pos);
        if (heap.size() > k) {
          heap.poll();
        }
      }
      selectedPositions = new int[heap.size()];
      int idx = heap.size() - 1;
      while (!heap.isEmpty()) {
        selectedPositions[idx--] = heap.poll();
      }
      // The heap gives us top-k in reverse order; sort the selected positions
      Integer[] boxed = new Integer[selectedPositions.length];
      for (int i = 0; i < selectedPositions.length; i++) boxed[i] = selectedPositions[i];
      java.util.Arrays.sort(boxed, posComparator);
      for (int i = 0; i < boxed.length; i++) selectedPositions[i] = boxed[i];
    } else {
      // Full sort: create index array and sort
      Integer[] indices = new Integer[totalRows];
      for (int i = 0; i < totalRows; i++) indices[i] = i;
      java.util.Arrays.sort(indices, posComparator);
      selectedPositions = new int[k];
      for (int i = 0; i < k; i++) selectedPositions[i] = indices[i];
    }

    // Build output Page by copying selected positions from the source blocks
    int numCols = columnTypes.size();
    BlockBuilder[] builders = new BlockBuilder[numCols];
    for (int col = 0; col < numCols; col++) {
      builders[col] = columnTypes.get(col).createBlockBuilder(null, selectedPositions.length);
    }

    for (int pos : selectedPositions) {
      for (int col = 0; col < numCols; col++) {
        Block srcBlock = page.getBlock(col);
        if (srcBlock.isNull(pos)) {
          builders[col].appendNull();
        } else {
          Type t = columnTypes.get(col);
          if (t instanceof BigintType) {
            BigintType.BIGINT.writeLong(builders[col], BigintType.BIGINT.getLong(srcBlock, pos));
          } else if (t instanceof DoubleType) {
            DoubleType.DOUBLE.writeDouble(
                builders[col], DoubleType.DOUBLE.getDouble(srcBlock, pos));
          } else if (t instanceof VarcharType) {
            VarcharType.VARCHAR.writeSlice(
                builders[col], VarcharType.VARCHAR.getSlice(srcBlock, pos));
          } else if (t instanceof BooleanType) {
            BooleanType.BOOLEAN.writeBoolean(
                builders[col], BooleanType.BOOLEAN.getBoolean(srcBlock, pos));
          } else {
            // Fallback for other types (integer, smallint, tinyint, timestamp)
            t.writeLong(builders[col], t.getLong(srcBlock, pos));
          }
        }
      }
    }

    Block[] blocks = new Block[numCols];
    for (int i = 0; i < numCols; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
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
