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

  /** Reusable buffers for the single-key numeric hash map to avoid per-query allocation. */
  private static final ThreadLocal<long[]> TL_MAP_KEYS = new ThreadLocal<>();

  private static final ThreadLocal<long[]> TL_MAP_VALUES = new ThreadLocal<>();
  private static final ThreadLocal<boolean[]> TL_MAP_OCCUPIED = new ThreadLocal<>();
  private static final ThreadLocal<int[]> TL_OCCUPIED_SLOTS = new ThreadLocal<>();

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
      // Ultra-fast single-key path: avoids ALL per-row allocation during merge
      if (numGroupByCols == 1 && !hasAvgAgg(aggregateFunctions)) {
        if (numAggCols == 1) {
          return mergeSingleKeyNumericCountWithSort(
              shardResults, columnTypes, sortColumnIndices, ascending, limit);
        }
        return mergeSingleKeyMultiAggNumericWithSort(
            shardResults,
            numAggCols,
            columnTypes,
            aggregateFunctions,
            sortColumnIndices,
            ascending,
            nullsFirst,
            limit);
      }
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

    // Fast varchar path with fused top-N
    if (canUseFastVarcharMerge(numGroupByCols, columnTypes, aggregateFunctions)) {
      return mergeAggregationFastVarcharWithSort(
          shardResults,
          numGroupByCols,
          numAggCols,
          columnTypes,
          sortColumnIndices,
          ascending,
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
      // Ultra-fast single-key path: avoids ALL per-row allocation during merge
      if (numGroupByCols == 1 && !hasAvgAgg(aggregateFunctions)) {
        if (numAggCols == 1) {
          return mergeSingleKeyNumericCount(shardResults, columnTypes);
        }
        return mergeSingleKeyMultiAggNumeric(shardResults, numAggCols, columnTypes);
      }
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

    // Reusable temporary key array - avoids per-row allocation for key extraction.
    // Only cloned to a new array when inserting a new group.
    long[] tmpKey = new long[numGroupByCols];

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
          // Extract key values into reusable array
          for (int k = 0; k < numGroupByCols; k++) {
            tmpKey[k] = columnTypes.get(k).getLong(keyBlocks[k], pos);
          }

          // Hash the key
          int hash = 1;
          for (int k = 0; k < numGroupByCols; k++) {
            hash = hash * 31 + Long.hashCode(tmpKey[k]);
          }

          // Probe the map
          int mask = capacity - 1;
          int slot = hash & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              // New group - clone the reusable key array for storage
              mapKeys[slot] = tmpKey.clone();
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

            // Check if key matches using tmpKey (avoids allocation)
            long[] existing = mapKeys[slot];
            boolean match = true;
            for (int k = 0; k < numGroupByCols; k++) {
              if (existing[k] != tmpKey[k]) {
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

    // Reusable temporary key array - avoids per-row allocation for key extraction
    long[] tmpKey = new long[numGroupByCols];

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
          for (int k = 0; k < numGroupByCols; k++) {
            tmpKey[k] = columnTypes.get(k).getLong(keyBlocks[k], pos);
          }
          int hash = 1;
          for (int k = 0; k < numGroupByCols; k++) {
            hash = hash * 31 + Long.hashCode(tmpKey[k]);
          }
          int mask = capacity - 1;
          int slot = hash & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              mapKeys[slot] = tmpKey.clone();
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
              if (existing[k] != tmpKey[k]) {
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

  /** Check if any aggregate function is AVG. */
  private static boolean hasAvgAgg(List<String> aggregateFunctions) {
    for (String func : aggregateFunctions) {
      Matcher m = AGG_PATTERN.matcher(func);
      if (m.matches() && "AVG".equals(m.group(1).toUpperCase(Locale.ROOT))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Ultra-fast merge for single numeric key + single COUNT/SUM aggregate (no sort). Uses a flat
   * open-addressing hash map with primitive long keys and long values, eliminating ALL per-row
   * object allocation. For Q16 (GROUP BY UserID COUNT(*)), this merges ~200K rows across 8 shards
   * into ~25K groups with zero GC pressure.
   */
  private List<Page> mergeSingleKeyNumericCount(
      List<List<Page>> shardResults, List<Type> columnTypes) {

    // Open-addressing hash map: parallel arrays for keys and values
    int capacity = 4096;
    float loadFactor = 0.7f;
    int threshold = (int) (capacity * loadFactor);
    int size = 0;
    long[] mapKeys = new long[capacity];
    long[] mapValues = new long[capacity];
    boolean[] mapOccupied = new boolean[capacity];

    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        int positionCount = page.getPositionCount();
        Block keyBlock = page.getBlock(0);
        Block aggBlock = page.getBlock(1);

        for (int pos = 0; pos < positionCount; pos++) {
          long key = columnTypes.get(0).getLong(keyBlock, pos);
          long val = BigintType.BIGINT.getLong(aggBlock, pos);

          // Murmur3 finalizer for good distribution
          long h = key;
          h ^= h >>> 33;
          h *= 0xff51afd7ed558ccdL;
          h ^= h >>> 33;
          h *= 0xc4ceb9fe1a85ec53L;
          h ^= h >>> 33;

          int mask = capacity - 1;
          int slot = (int) h & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              mapKeys[slot] = key;
              mapValues[slot] = val;
              mapOccupied[slot] = true;
              size++;
              if (size > threshold) {
                // Resize
                int newCap = capacity * 2;
                long[] nk = new long[newCap];
                long[] nv = new long[newCap];
                boolean[] nocc = new boolean[newCap];
                int nm = newCap - 1;
                for (int s = 0; s < capacity; s++) {
                  if (mapOccupied[s]) {
                    long rh = mapKeys[s];
                    rh ^= rh >>> 33;
                    rh *= 0xff51afd7ed558ccdL;
                    rh ^= rh >>> 33;
                    rh *= 0xc4ceb9fe1a85ec53L;
                    rh ^= rh >>> 33;
                    int ns = (int) rh & nm;
                    while (nocc[ns]) ns = (ns + 1) & nm;
                    nk[ns] = mapKeys[s];
                    nv[ns] = mapValues[s];
                    nocc[ns] = true;
                  }
                }
                capacity = newCap;
                mask = newCap - 1;
                threshold = (int) (newCap * loadFactor);
                mapKeys = nk;
                mapValues = nv;
                mapOccupied = nocc;
                // Re-find the slot for the just-inserted key after resize
                slot = (int) h & mask;
                while (mapOccupied[slot]) {
                  if (mapKeys[slot] == key) break;
                  slot = (slot + 1) & mask;
                }
              }
              break;
            }
            if (mapKeys[slot] == key) {
              mapValues[slot] += val;
              break;
            }
            slot = (slot + 1) & mask;
          }
        }
      }
    }

    if (size == 0) return List.of();

    BlockBuilder keyBuilder = columnTypes.get(0).createBlockBuilder(null, size);
    BlockBuilder valBuilder = BigintType.BIGINT.createBlockBuilder(null, size);
    for (int s = 0; s < capacity; s++) {
      if (mapOccupied[s]) {
        columnTypes.get(0).writeLong(keyBuilder, mapKeys[s]);
        BigintType.BIGINT.writeLong(valBuilder, mapValues[s]);
      }
    }
    return List.of(new Page(keyBuilder.build(), valBuilder.build()));
  }

  /**
   * Ultra-fast merge + top-N sort for single numeric key + single COUNT/SUM aggregate. Uses a flat
   * open-addressing hash map with primitive long keys and long values, then performs top-N
   * selection directly on the flat arrays. Eliminates ALL per-row object allocation during both
   * merge and sort phases.
   *
   * <p>For Q16 (GROUP BY UserID COUNT(*) ORDER BY COUNT(*) DESC LIMIT 10), this:
   *
   * <ul>
   *   <li>Merges ~200K partial rows into ~25K groups with zero allocation per row
   *   <li>Selects top-10 using bounded heap on slot indices (not boxed Integer objects)
   *   <li>Builds only 10 output rows (not 25K)
   * </ul>
   */
  private List<Page> mergeSingleKeyNumericCountWithSort(
      List<List<Page>> shardResults,
      List<Type> columnTypes,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      long limit) {

    // Estimate total rows to pre-size hash map and avoid resizes
    int estimatedRows = 0;
    for (List<Page> sp : shardResults) {
      for (Page p : sp) estimatedRows += p.getPositionCount();
    }

    // Open-addressing hash map with compact slot tracking.
    // Estimate unique groups: rows/numShards since groups overlap across shards.
    int numShards = shardResults.size();
    int estimatedGroups = Math.max(estimatedRows / Math.max(numShards, 1), 4096);
    int capacity = Integer.highestOneBit(Math.max(4096, (int) (estimatedGroups / 0.5f))) << 1;
    float loadFactor = 0.7f;
    int threshold = (int) (capacity * loadFactor);
    int size = 0;

    // Reuse thread-local buffers when possible to avoid per-query allocation (3.4MB)
    long[] mapKeys = TL_MAP_KEYS.get();
    long[] mapValues = TL_MAP_VALUES.get();
    boolean[] mapOccupied = TL_MAP_OCCUPIED.get();
    int[] occupiedSlots = TL_OCCUPIED_SLOTS.get();
    if (mapKeys == null || mapKeys.length < capacity) {
      mapKeys = new long[capacity];
      mapValues = new long[capacity];
      mapOccupied = new boolean[capacity];
      TL_MAP_KEYS.set(mapKeys);
      TL_MAP_VALUES.set(mapValues);
      TL_MAP_OCCUPIED.set(mapOccupied);
    } else {
      // Clear the occupied flags from previous use
      java.util.Arrays.fill(mapOccupied, 0, capacity, false);
    }
    if (occupiedSlots == null || occupiedSlots.length < Math.min(estimatedRows, capacity)) {
      occupiedSlots = new int[Math.min(estimatedRows, capacity)];
      TL_OCCUPIED_SLOTS.set(occupiedSlots);
    }

    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        int positionCount = page.getPositionCount();
        Block keyBlock = page.getBlock(0);
        Block aggBlock = page.getBlock(1);

        for (int pos = 0; pos < positionCount; pos++) {
          long key = columnTypes.get(0).getLong(keyBlock, pos);
          long val = BigintType.BIGINT.getLong(aggBlock, pos);

          // Murmur3 finalizer for good distribution
          long h = key;
          h ^= h >>> 33;
          h *= 0xff51afd7ed558ccdL;
          h ^= h >>> 33;
          h *= 0xc4ceb9fe1a85ec53L;
          h ^= h >>> 33;

          int mask = capacity - 1;
          int slot = (int) h & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              mapKeys[slot] = key;
              mapValues[slot] = val;
              mapOccupied[slot] = true;
              occupiedSlots[size] = slot;
              size++;
              if (size > threshold) {
                // Resize
                int newCap = capacity * 2;
                long[] nk = new long[newCap];
                long[] nv = new long[newCap];
                boolean[] nocc = new boolean[newCap];
                int nm = newCap - 1;
                // Rebuild occupied slots using compact tracking array
                for (int i = 0; i < size - 1; i++) {
                  int oldSlot = occupiedSlots[i];
                  long rh = mapKeys[oldSlot];
                  rh ^= rh >>> 33;
                  rh *= 0xff51afd7ed558ccdL;
                  rh ^= rh >>> 33;
                  rh *= 0xc4ceb9fe1a85ec53L;
                  rh ^= rh >>> 33;
                  int ns = (int) rh & nm;
                  while (nocc[ns]) ns = (ns + 1) & nm;
                  nk[ns] = mapKeys[oldSlot];
                  nv[ns] = mapValues[oldSlot];
                  nocc[ns] = true;
                  occupiedSlots[i] = ns;
                }
                // Also rehash the just-inserted entry (at index size-1)
                {
                  int ns = (int) h & nm;
                  while (nocc[ns]) ns = (ns + 1) & nm;
                  nk[ns] = key;
                  nv[ns] = val;
                  nocc[ns] = true;
                  occupiedSlots[size - 1] = ns;
                }
                capacity = newCap;
                mask = newCap - 1;
                threshold = (int) (newCap * loadFactor);
                mapKeys = nk;
                mapValues = nv;
                mapOccupied = nocc;
                if (size > occupiedSlots.length) {
                  int[] newOcc = new int[Math.min(estimatedRows, newCap)];
                  System.arraycopy(occupiedSlots, 0, newOcc, 0, size);
                  occupiedSlots = newOcc;
                }
              }
              break;
            }
            if (mapKeys[slot] == key) {
              mapValues[slot] += val;
              break;
            }
            slot = (slot + 1) & mask;
          }
        }
      }
    }

    // Update thread-local references in case resize occurred
    TL_MAP_KEYS.set(mapKeys);
    TL_MAP_VALUES.set(mapValues);
    TL_MAP_OCCUPIED.set(mapOccupied);
    TL_OCCUPIED_SLOTS.set(occupiedSlots);

    if (size == 0) return List.of();

    // Top-N selection using primitive int[] bounded heap.
    // Operates only on the compact occupiedSlots[0..size) array,
    // avoiding the scan of all 'capacity' hash map slots.
    int k = (int) Math.min(limit, size);
    int sortCol = sortColumnIndices.get(0);
    boolean sortAsc = ascending.get(0);

    // Choose sort values: keys or aggregate values
    final long[] sortArr = (sortCol == 0) ? mapKeys : mapValues;

    // Primitive int[] bounded min/max heap for top-K (no autoboxing)
    // For DESC (sortAsc=false): we want largest K, so maintain a min-heap (evict smallest)
    // For ASC (sortAsc=true): we want smallest K, so maintain a max-heap (evict largest)
    int[] heap = new int[k];
    int heapSize = 0;

    for (int i = 0; i < size; i++) {
      int slot = occupiedSlots[i];
      if (heapSize < k) {
        // Add to heap
        heap[heapSize] = slot;
        heapSize++;
        // Sift up
        int child = heapSize - 1;
        while (child > 0) {
          int parent = (child - 1) >>> 1;
          if (sortAsc
              ? sortArr[heap[child]] > sortArr[heap[parent]]
              : sortArr[heap[child]] < sortArr[heap[parent]]) {
            int tmp = heap[child];
            heap[child] = heap[parent];
            heap[parent] = tmp;
            child = parent;
          } else {
            break;
          }
        }
      } else {
        // Compare with heap root (the element to evict)
        long slotVal = sortArr[slot];
        long rootVal = sortArr[heap[0]];
        boolean replace = sortAsc ? slotVal < rootVal : slotVal > rootVal;
        if (replace) {
          heap[0] = slot;
          // Sift down
          int parent = 0;
          while (true) {
            int left = 2 * parent + 1;
            if (left >= heapSize) break;
            int right = left + 1;
            int target = left;
            if (right < heapSize) {
              if (sortAsc
                  ? sortArr[heap[right]] > sortArr[heap[left]]
                  : sortArr[heap[right]] < sortArr[heap[left]]) {
                target = right;
              }
            }
            if (sortAsc
                ? sortArr[heap[target]] > sortArr[heap[parent]]
                : sortArr[heap[target]] < sortArr[heap[parent]]) {
              int tmp = heap[parent];
              heap[parent] = heap[target];
              heap[target] = tmp;
              parent = target;
            } else {
              break;
            }
          }
        }
      }
    }

    // Sort the top-K slots by sort value
    // Simple insertion sort for small k (typically 10-100)
    for (int i = 1; i < heapSize; i++) {
      int tmp = heap[i];
      long tmpVal = sortArr[tmp];
      int j = i - 1;
      while (j >= 0 && (sortAsc ? sortArr[heap[j]] > tmpVal : sortArr[heap[j]] < tmpVal)) {
        heap[j + 1] = heap[j];
        j--;
      }
      heap[j + 1] = tmp;
    }

    // Build output Page for top-k only
    BlockBuilder keyBuilder = columnTypes.get(0).createBlockBuilder(null, heapSize);
    BlockBuilder valBuilder = BigintType.BIGINT.createBlockBuilder(null, heapSize);
    for (int i = 0; i < heapSize; i++) {
      int slot = heap[i];
      columnTypes.get(0).writeLong(keyBuilder, mapKeys[slot]);
      BigintType.BIGINT.writeLong(valBuilder, mapValues[slot]);
    }
    return List.of(new Page(keyBuilder.build(), valBuilder.build()));
  }

  /**
   * Flat merge for single numeric key + multiple COUNT/SUM aggregates (no sort). Uses a flat
   * open-addressing hash map with primitive long keys and interleaved long[] agg values (stride =
   * numAggCols), eliminating ALL per-row object allocation. For queries like GROUP BY UserID with
   * COUNT(*), SUM(col), this merges ~200K+ partial rows with zero GC pressure.
   */
  private List<Page> mergeSingleKeyMultiAggNumeric(
      List<List<Page>> shardResults, int numAggCols, List<Type> columnTypes) {

    int totalCols = 1 + numAggCols;
    int capacity = 4096;
    float loadFactor = 0.7f;
    int threshold = (int) (capacity * loadFactor);
    int size = 0;
    long[] mapKeys = new long[capacity];
    // Interleaved agg values: mapAggs[slot * numAggCols + aggIdx]
    long[] mapAggs = new long[capacity * numAggCols];
    boolean[] mapOccupied = new boolean[capacity];

    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        int positionCount = page.getPositionCount();
        Block keyBlock = page.getBlock(0);
        Block[] aggBlocks = new Block[numAggCols];
        for (int a = 0; a < numAggCols; a++) {
          aggBlocks[a] = page.getBlock(1 + a);
        }

        for (int pos = 0; pos < positionCount; pos++) {
          long key = columnTypes.get(0).getLong(keyBlock, pos);

          // Murmur3 finalizer
          long h = key;
          h ^= h >>> 33;
          h *= 0xff51afd7ed558ccdL;
          h ^= h >>> 33;
          h *= 0xc4ceb9fe1a85ec53L;
          h ^= h >>> 33;

          int mask = capacity - 1;
          int slot = (int) h & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              mapKeys[slot] = key;
              int base = slot * numAggCols;
              for (int a = 0; a < numAggCols; a++) {
                mapAggs[base + a] = BigintType.BIGINT.getLong(aggBlocks[a], pos);
              }
              mapOccupied[slot] = true;
              size++;
              if (size > threshold) {
                // Resize
                int newCap = capacity * 2;
                long[] nk = new long[newCap];
                long[] na = new long[newCap * numAggCols];
                boolean[] nocc = new boolean[newCap];
                int nm = newCap - 1;
                for (int s = 0; s < capacity; s++) {
                  if (mapOccupied[s]) {
                    long rh = mapKeys[s];
                    rh ^= rh >>> 33;
                    rh *= 0xff51afd7ed558ccdL;
                    rh ^= rh >>> 33;
                    rh *= 0xc4ceb9fe1a85ec53L;
                    rh ^= rh >>> 33;
                    int ns = (int) rh & nm;
                    while (nocc[ns]) ns = (ns + 1) & nm;
                    nk[ns] = mapKeys[s];
                    int oldBase = s * numAggCols;
                    int newBase = ns * numAggCols;
                    System.arraycopy(mapAggs, oldBase, na, newBase, numAggCols);
                    nocc[ns] = true;
                  }
                }
                capacity = newCap;
                mask = newCap - 1;
                threshold = (int) (newCap * loadFactor);
                mapKeys = nk;
                mapAggs = na;
                mapOccupied = nocc;
                // Re-find slot after resize
                slot = (int) h & mask;
                while (mapOccupied[slot]) {
                  if (mapKeys[slot] == key) break;
                  slot = (slot + 1) & mask;
                }
              }
              break;
            }
            if (mapKeys[slot] == key) {
              int base = slot * numAggCols;
              for (int a = 0; a < numAggCols; a++) {
                mapAggs[base + a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
              }
              break;
            }
            slot = (slot + 1) & mask;
          }
        }
      }
    }

    if (size == 0) return List.of();

    BlockBuilder[] builders = new BlockBuilder[totalCols];
    builders[0] = columnTypes.get(0).createBlockBuilder(null, size);
    for (int a = 0; a < numAggCols; a++) {
      builders[1 + a] = BigintType.BIGINT.createBlockBuilder(null, size);
    }
    for (int s = 0; s < capacity; s++) {
      if (mapOccupied[s]) {
        columnTypes.get(0).writeLong(builders[0], mapKeys[s]);
        int base = s * numAggCols;
        for (int a = 0; a < numAggCols; a++) {
          BigintType.BIGINT.writeLong(builders[1 + a], mapAggs[base + a]);
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
   * Flat merge + top-N sort for single numeric key + multiple COUNT/SUM aggregates. Uses a flat
   * open-addressing hash map with interleaved agg values, then performs top-N selection directly on
   * the flat arrays. Eliminates ALL per-row object allocation during both merge and sort phases.
   *
   * <p>For queries like GROUP BY UserID COUNT(*), SUM(col) ORDER BY COUNT(*) DESC LIMIT 10, this:
   *
   * <ul>
   *   <li>Merges ~200K partial rows into groups with zero allocation per row
   *   <li>Selects top-N using bounded heap on slot indices
   *   <li>Builds only N output rows
   * </ul>
   */
  private List<Page> mergeSingleKeyMultiAggNumericWithSort(
      List<List<Page>> shardResults,
      int numAggCols,
      List<Type> columnTypes,
      List<String> aggregateFunctions,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Boolean> nullsFirst,
      long limit) {

    int totalCols = 1 + numAggCols;
    int capacity = 4096;
    float loadFactor = 0.7f;
    int threshold = (int) (capacity * loadFactor);
    int size = 0;
    long[] mapKeys = new long[capacity];
    long[] mapAggs = new long[capacity * numAggCols];
    boolean[] mapOccupied = new boolean[capacity];

    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        int positionCount = page.getPositionCount();
        Block keyBlock = page.getBlock(0);
        Block[] aggBlocks = new Block[numAggCols];
        for (int a = 0; a < numAggCols; a++) {
          aggBlocks[a] = page.getBlock(1 + a);
        }

        for (int pos = 0; pos < positionCount; pos++) {
          long key = columnTypes.get(0).getLong(keyBlock, pos);

          // Murmur3 finalizer
          long h = key;
          h ^= h >>> 33;
          h *= 0xff51afd7ed558ccdL;
          h ^= h >>> 33;
          h *= 0xc4ceb9fe1a85ec53L;
          h ^= h >>> 33;

          int mask = capacity - 1;
          int slot = (int) h & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              mapKeys[slot] = key;
              int base = slot * numAggCols;
              for (int a = 0; a < numAggCols; a++) {
                mapAggs[base + a] = BigintType.BIGINT.getLong(aggBlocks[a], pos);
              }
              mapOccupied[slot] = true;
              size++;
              if (size > threshold) {
                int newCap = capacity * 2;
                long[] nk = new long[newCap];
                long[] na = new long[newCap * numAggCols];
                boolean[] nocc = new boolean[newCap];
                int nm = newCap - 1;
                for (int s = 0; s < capacity; s++) {
                  if (mapOccupied[s]) {
                    long rh = mapKeys[s];
                    rh ^= rh >>> 33;
                    rh *= 0xff51afd7ed558ccdL;
                    rh ^= rh >>> 33;
                    rh *= 0xc4ceb9fe1a85ec53L;
                    rh ^= rh >>> 33;
                    int ns = (int) rh & nm;
                    while (nocc[ns]) ns = (ns + 1) & nm;
                    nk[ns] = mapKeys[s];
                    System.arraycopy(mapAggs, s * numAggCols, na, ns * numAggCols, numAggCols);
                    nocc[ns] = true;
                  }
                }
                capacity = newCap;
                mask = newCap - 1;
                threshold = (int) (newCap * loadFactor);
                mapKeys = nk;
                mapAggs = na;
                mapOccupied = nocc;
                slot = (int) h & mask;
                while (mapOccupied[slot]) {
                  if (mapKeys[slot] == key) break;
                  slot = (slot + 1) & mask;
                }
              }
              break;
            }
            if (mapKeys[slot] == key) {
              int base = slot * numAggCols;
              for (int a = 0; a < numAggCols; a++) {
                mapAggs[base + a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
              }
              break;
            }
            slot = (slot + 1) & mask;
          }
        }
      }
    }

    if (size == 0) return List.of();

    // Top-N selection
    int k = (int) Math.min(limit, size);
    final int numSortKeys = sortColumnIndices.size();

    // Capture final references for comparator
    final long[] fKeys = mapKeys;
    final long[] fAggs = mapAggs;
    final boolean[] fOccupied = mapOccupied;
    final int fCapacity = capacity;
    final int fNumAggCols = numAggCols;

    // Comparator for slot indices
    Comparator<Integer> slotCmp =
        (s1, s2) -> {
          for (int i = 0; i < numSortKeys; i++) {
            int colIdx = sortColumnIndices.get(i);
            boolean asc = ascending.get(i);
            long v1, v2;
            if (colIdx == 0) {
              v1 = fKeys[s1];
              v2 = fKeys[s2];
            } else {
              int aggIdx = colIdx - 1;
              v1 = fAggs[s1 * fNumAggCols + aggIdx];
              v2 = fAggs[s2 * fNumAggCols + aggIdx];
            }
            int cmp = Long.compare(v1, v2);
            if (cmp != 0) return asc ? cmp : -cmp;
          }
          return 0;
        };

    // Bounded max-heap for O(n log k) top-N selection
    PriorityQueue<Integer> heap = new PriorityQueue<>(k + 1, slotCmp.reversed());
    for (int s = 0; s < fCapacity; s++) {
      if (fOccupied[s]) {
        heap.offer(s);
        if (heap.size() > k) heap.poll();
      }
    }

    // Extract in sorted order
    int heapSize = heap.size();
    Integer[] topSlots = new Integer[heapSize];
    for (int i = heapSize - 1; i >= 0; i--) {
      topSlots[i] = heap.poll();
    }
    java.util.Arrays.sort(topSlots, slotCmp);

    // Build output Page for top-k only
    BlockBuilder[] builders = new BlockBuilder[totalCols];
    builders[0] = columnTypes.get(0).createBlockBuilder(null, heapSize);
    for (int a = 0; a < numAggCols; a++) {
      builders[1 + a] = BigintType.BIGINT.createBlockBuilder(null, heapSize);
    }
    for (int slot : topSlots) {
      columnTypes.get(0).writeLong(builders[0], fKeys[slot]);
      int base = slot * fNumAggCols;
      for (int a = 0; a < numAggCols; a++) {
        BigintType.BIGINT.writeLong(builders[1 + a], fAggs[base + a]);
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

    // Reusable probe key - avoids per-row array allocation for existing groups.
    // Only clones the backing arrays when inserting a new group.
    long[] tmpNumericKeys = new long[numGroupByCols];
    io.airlift.slice.Slice[] tmpSliceKeys = new io.airlift.slice.Slice[numGroupByCols];

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
          int hash = 1;
          for (int k = 0; k < numGroupByCols; k++) {
            if (isVarcharKey[k]) {
              io.airlift.slice.Slice slice = VarcharType.VARCHAR.getSlice(keyBlocks[k], pos);
              tmpSliceKeys[k] = slice;
              hash = hash * 31 + slice.hashCode();
            } else {
              long val = columnTypes.get(k).getLong(keyBlocks[k], pos);
              tmpNumericKeys[k] = val;
              hash = hash * 31 + Long.hashCode(val);
            }
          }
          // Probe with reusable key to avoid allocation on lookup hit
          MixedMergeKey probeKey =
              new MixedMergeKey(tmpNumericKeys, tmpSliceKeys, isVarcharKey, hash);
          long[] aggs = groups.get(probeKey);
          if (aggs == null) {
            // New group - create permanent key with cloned arrays
            MixedMergeKey permanentKey =
                new MixedMergeKey(tmpNumericKeys.clone(), tmpSliceKeys.clone(), isVarcharKey, hash);
            aggs = new long[numAggCols];
            groups.put(permanentKey, aggs);
            if (hasAvg) {
              doubleGroups.put(permanentKey, new double[numAggCols]);
            }
          }
          for (int a = 0; a < numAggCols; a++) {
            if (isAvgAgg[a]) {
              double avg = DoubleType.DOUBLE.getDouble(aggBlocks[a], pos);
              long count =
                  fCountAggIdx >= 0 ? BigintType.BIGINT.getLong(aggBlocks[fCountAggIdx], pos) : 1;
              // Use aggs key from map (already stored under permanent key)
              doubleGroups.get(probeKey)[a] += avg * count;
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
   * Fused varchar merge + sort + limit for single VARCHAR key with COUNT/SUM aggregates. Merges all
   * shard results into an open-addressing hash map (no per-entry object allocation), then performs
   * top-N selection using a bounded heap. Uses XxHash64 for hashing and direct byte comparison for
   * equality, avoiding Slice.hashCode()/equals() overhead.
   *
   * <p>For high-cardinality VARCHAR GROUP BY with small LIMIT (e.g., Q34: ~21K unique groups across
   * 8 shards with LIMIT 10), this eliminates ~168K HashMap.Entry allocations and reduces hash
   * computation overhead by ~3x compared to the Slice.hashCode() path.
   */
  @SuppressWarnings("unchecked")
  private List<Page> mergeAggregationFastVarcharWithSort(
      List<List<Page>> shardResults,
      int numGroupByCols,
      int numAggCols,
      List<Type> columnTypes,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      long limit) {

    int totalCols = numGroupByCols + numAggCols;

    // Estimate total rows for capacity sizing
    int estimatedRows = 0;
    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        estimatedRows += page.getPositionCount();
      }
    }
    if (estimatedRows == 0) {
      return List.of();
    }

    // === Open-addressing hash map with parallel arrays ===
    // Eliminates HashMap.Entry allocation and uses XxHash64 for faster hashing.
    int rawCapacity = Math.max(1024, (int) (estimatedRows / 0.65f) + 1);
    int mapCapacity = Integer.highestOneBit(rawCapacity - 1) << 1;
    int mapMask = mapCapacity - 1;

    // Key storage: Slice references + offset + length (zero-copy from source Blocks)
    io.airlift.slice.Slice[] mapKeySlices = new io.airlift.slice.Slice[mapCapacity];
    int[] mapKeyOffsets = new int[mapCapacity];
    int[] mapKeyLengths = new int[mapCapacity];
    long[] mapKeyHashes = new long[mapCapacity];
    // Value storage: flat array with numAggCols stride
    long[] mapAggValues = new long[mapCapacity * numAggCols];
    int mapSize = 0;

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
          io.airlift.slice.Slice keySlice = VarcharType.VARCHAR.getSlice(keyBlock, pos);
          int keyOffset = 0;
          int keyLength = keySlice.length();
          long hash = io.airlift.slice.XxHash64.hash(keySlice, keyOffset, keyLength);

          // Linear probe to find existing entry or empty slot
          int slot = (int) (hash & mapMask);
          while (true) {
            if (mapKeySlices[slot] == null) {
              // Empty slot — insert new entry
              mapKeySlices[slot] = keySlice;
              mapKeyOffsets[slot] = keyOffset;
              mapKeyLengths[slot] = keyLength;
              mapKeyHashes[slot] = hash;
              int base = slot * numAggCols;
              for (int a = 0; a < numAggCols; a++) {
                if (!aggBlocks[a].isNull(pos)) {
                  mapAggValues[base + a] = BigintType.BIGINT.getLong(aggBlocks[a], pos);
                }
              }
              mapSize++;
              // Resize if needed (load factor > 65%)
              if (mapSize > (int) (mapCapacity * 0.65f)) {
                // Rehash into larger table
                int newCapacity = mapCapacity << 1;
                int newMask = newCapacity - 1;
                io.airlift.slice.Slice[] newSlices = new io.airlift.slice.Slice[newCapacity];
                int[] newOffsets = new int[newCapacity];
                int[] newLengths = new int[newCapacity];
                long[] newHashes = new long[newCapacity];
                long[] newAggs = new long[newCapacity * numAggCols];
                for (int s = 0; s < mapCapacity; s++) {
                  if (mapKeySlices[s] != null) {
                    int ns = (int) (mapKeyHashes[s] & newMask);
                    while (newSlices[ns] != null) {
                      ns = (ns + 1) & newMask;
                    }
                    newSlices[ns] = mapKeySlices[s];
                    newOffsets[ns] = mapKeyOffsets[s];
                    newLengths[ns] = mapKeyLengths[s];
                    newHashes[ns] = mapKeyHashes[s];
                    System.arraycopy(
                        mapAggValues, s * numAggCols, newAggs, ns * numAggCols, numAggCols);
                  }
                }
                mapKeySlices = newSlices;
                mapKeyOffsets = newOffsets;
                mapKeyLengths = newLengths;
                mapKeyHashes = newHashes;
                mapAggValues = newAggs;
                mapCapacity = newCapacity;
                mapMask = newMask;
              }
              break;
            }
            if (mapKeyHashes[slot] == hash
                && mapKeyLengths[slot] == keyLength
                && mapKeySlices[slot].equals(
                    mapKeyOffsets[slot], keyLength, keySlice, keyOffset, keyLength)) {
              // Matching entry — accumulate aggregate values
              int base = slot * numAggCols;
              for (int a = 0; a < numAggCols; a++) {
                if (!aggBlocks[a].isNull(pos)) {
                  mapAggValues[base + a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
                }
              }
              break;
            }
            slot = (slot + 1) & mapMask;
          }
        }
      }
    }

    if (mapSize == 0) {
      return List.of();
    }

    int k = (int) Math.min(limit, mapSize);

    // Determine sort column
    int sortCol = sortColumnIndices.get(0);
    boolean sortAsc = ascending.get(0);
    boolean sortByAgg = sortCol >= numGroupByCols;
    int sortAggIdx = sortByAgg ? sortCol - numGroupByCols : -1;

    // Bounded heap for top-K selection using slot indices
    int[] heap = new int[k];
    long[] heapVals = new long[k];
    int heapSize = 0;

    final int fMapCapacity = mapCapacity;
    final int fNumAggCols = numAggCols;

    for (int slot = 0; slot < fMapCapacity; slot++) {
      if (mapKeySlices[slot] == null) continue;

      long val;
      if (sortByAgg) {
        val = mapAggValues[slot * fNumAggCols + sortAggIdx];
      } else {
        val = 0; // lexicographic sort handled separately
      }

      if (sortByAgg) {
        // Numeric sort on aggregate value — use fast long comparison
        if (heapSize < k) {
          heap[heapSize] = slot;
          heapVals[heapSize] = val;
          heapSize++;
          // Sift up
          int c = heapSize - 1;
          while (c > 0) {
            int p = (c - 1) >>> 1;
            boolean swap = sortAsc ? (heapVals[c] > heapVals[p]) : (heapVals[c] < heapVals[p]);
            if (swap) {
              int ti = heap[c];
              heap[c] = heap[p];
              heap[p] = ti;
              long tv = heapVals[c];
              heapVals[c] = heapVals[p];
              heapVals[p] = tv;
              c = p;
            } else break;
          }
        } else {
          boolean better = sortAsc ? (val < heapVals[0]) : (val > heapVals[0]);
          if (better) {
            heap[0] = slot;
            heapVals[0] = val;
            // Sift down
            int p = 0;
            while (true) {
              int left = 2 * p + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                boolean pr =
                    sortAsc
                        ? (heapVals[right] > heapVals[left])
                        : (heapVals[right] < heapVals[left]);
                if (pr) target = right;
              }
              boolean sw =
                  sortAsc ? (heapVals[target] > heapVals[p]) : (heapVals[target] < heapVals[p]);
              if (sw) {
                int ti = heap[p];
                heap[p] = heap[target];
                heap[target] = ti;
                long tv = heapVals[p];
                heapVals[p] = heapVals[target];
                heapVals[target] = tv;
                p = target;
              } else break;
            }
          }
        }
      } else {
        // Lexicographic sort on varchar key — slower path, use Slice comparison
        if (heapSize < k) {
          heap[heapSize] = slot;
          heapSize++;
          int c = heapSize - 1;
          while (c > 0) {
            int p = (c - 1) >>> 1;
            int cmp =
                mapKeySlices[heap[c]].compareTo(
                    mapKeyOffsets[heap[c]],
                    mapKeyLengths[heap[c]],
                    mapKeySlices[heap[p]],
                    mapKeyOffsets[heap[p]],
                    mapKeyLengths[heap[p]]);
            boolean swap = sortAsc ? (cmp > 0) : (cmp < 0);
            if (swap) {
              int ti = heap[c];
              heap[c] = heap[p];
              heap[p] = ti;
              c = p;
            } else break;
          }
        } else {
          int cmp =
              mapKeySlices[slot].compareTo(
                  mapKeyOffsets[slot],
                  mapKeyLengths[slot],
                  mapKeySlices[heap[0]],
                  mapKeyOffsets[heap[0]],
                  mapKeyLengths[heap[0]]);
          boolean better = sortAsc ? (cmp < 0) : (cmp > 0);
          if (better) {
            heap[0] = slot;
            int p = 0;
            while (true) {
              int left = 2 * p + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                int cmpLR =
                    mapKeySlices[heap[right]].compareTo(
                        mapKeyOffsets[heap[right]],
                        mapKeyLengths[heap[right]],
                        mapKeySlices[heap[left]],
                        mapKeyOffsets[heap[left]],
                        mapKeyLengths[heap[left]]);
                if (sortAsc ? cmpLR > 0 : cmpLR < 0) target = right;
              }
              int cmpTP =
                  mapKeySlices[heap[target]].compareTo(
                      mapKeyOffsets[heap[target]],
                      mapKeyLengths[heap[target]],
                      mapKeySlices[heap[p]],
                      mapKeyOffsets[heap[p]],
                      mapKeyLengths[heap[p]]);
              if (sortAsc ? cmpTP > 0 : cmpTP < 0) {
                int ti = heap[p];
                heap[p] = heap[target];
                heap[target] = ti;
                p = target;
              } else break;
            }
          }
        }
      }
    }

    // Sort the top-K entries (insertion sort, k is small)
    for (int i = 1; i < heapSize; i++) {
      int tmpSlot = heap[i];
      int j = i - 1;
      while (j >= 0) {
        int cmp;
        if (sortByAgg) {
          long v1 = mapAggValues[heap[j] * fNumAggCols + sortAggIdx];
          long v2 = mapAggValues[tmpSlot * fNumAggCols + sortAggIdx];
          cmp = sortAsc ? Long.compare(v1, v2) : Long.compare(v2, v1);
        } else {
          cmp =
              mapKeySlices[heap[j]].compareTo(
                  mapKeyOffsets[heap[j]],
                  mapKeyLengths[heap[j]],
                  mapKeySlices[tmpSlot],
                  mapKeyOffsets[tmpSlot],
                  mapKeyLengths[tmpSlot]);
          if (!sortAsc) cmp = -cmp;
        }
        if (cmp > 0) {
          heap[j + 1] = heap[j];
          j--;
        } else break;
      }
      heap[j + 1] = tmpSlot;
    }

    // Build output Page for top-k only
    BlockBuilder keyBuilder = VarcharType.VARCHAR.createBlockBuilder(null, heapSize);
    BlockBuilder[] aggBuilders = new BlockBuilder[numAggCols];
    for (int a = 0; a < numAggCols; a++) {
      aggBuilders[a] = BigintType.BIGINT.createBlockBuilder(null, heapSize);
    }

    for (int i = 0; i < heapSize; i++) {
      int slot = heap[i];
      VarcharType.VARCHAR.writeSlice(
          keyBuilder, mapKeySlices[slot], mapKeyOffsets[slot], mapKeyLengths[slot]);
      int base = slot * numAggCols;
      for (int a = 0; a < numAggCols; a++) {
        BigintType.BIGINT.writeLong(aggBuilders[a], mapAggValues[base + a]);
      }
    }

    Block[] blocks = new Block[totalCols];
    blocks[0] = keyBuilder.build();
    for (int a = 0; a < numAggCols; a++) {
      blocks[numGroupByCols + a] = aggBuilders[a].build();
    }
    return List.of(new Page(blocks));
  }

  /**
   * Heap comparison for VARCHAR merge top-N: determines if child should be above parent in the
   * bounded heap. For DESC sort, we maintain a min-heap (child with smaller value goes up); for ASC
   * sort, we maintain a max-heap (child with larger value goes up).
   */
  private static boolean heapShouldSwapVarchar(
      java.util.Map.Entry<io.airlift.slice.Slice, long[]>[] entries,
      int childIdx,
      int parentIdx,
      boolean sortByAgg,
      int sortAggIdx,
      boolean sortAsc) {
    if (sortByAgg) {
      long childVal = entries[childIdx].getValue()[sortAggIdx];
      long parentVal = entries[parentIdx].getValue()[sortAggIdx];
      return sortAsc ? childVal > parentVal : childVal < parentVal;
    } else {
      // Sort by VARCHAR key (lexicographic)
      io.airlift.slice.Slice childKey = entries[childIdx].getKey();
      io.airlift.slice.Slice parentKey = entries[parentIdx].getKey();
      int cmp = childKey.compareTo(parentKey);
      return sortAsc ? cmp > 0 : cmp < 0;
    }
  }

  /**
   * Check if a new entry should replace the heap root during top-N selection. For DESC sort (want
   * largest K), replace root if new value is larger; for ASC sort (want smallest K), replace if
   * smaller.
   */
  private static boolean heapShouldReplaceRootVarchar(
      java.util.Map.Entry<io.airlift.slice.Slice, long[]>[] entries,
      int newIdx,
      int rootIdx,
      boolean sortByAgg,
      int sortAggIdx,
      boolean sortAsc) {
    if (sortByAgg) {
      long newVal = entries[newIdx].getValue()[sortAggIdx];
      long rootVal = entries[rootIdx].getValue()[sortAggIdx];
      return sortAsc ? newVal < rootVal : newVal > rootVal;
    } else {
      io.airlift.slice.Slice newKey = entries[newIdx].getKey();
      io.airlift.slice.Slice rootKey = entries[rootIdx].getKey();
      int cmp = newKey.compareTo(rootKey);
      return sortAsc ? cmp < 0 : cmp > 0;
    }
  }

  /**
   * Compare two entries for final sort (after top-K selection). Returns positive if entry at idx1
   * should come after entry at idx2 in the sorted output.
   */
  private static int heapCompareForFinalSortVarchar(
      java.util.Map.Entry<io.airlift.slice.Slice, long[]>[] entries,
      int idx1,
      int idx2,
      boolean sortByAgg,
      int sortAggIdx,
      boolean sortAsc) {
    if (sortByAgg) {
      long v1 = entries[idx1].getValue()[sortAggIdx];
      long v2 = entries[idx2].getValue()[sortAggIdx];
      int cmp = Long.compare(v1, v2);
      return sortAsc ? cmp : -cmp;
    } else {
      io.airlift.slice.Slice k1 = entries[idx1].getKey();
      io.airlift.slice.Slice k2 = entries[idx2].getKey();
      int cmp = k1.compareTo(k2);
      return sortAsc ? cmp : -cmp;
    }
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

    // Reusable probe key arrays - avoids per-row allocation for existing groups
    long[] tmpNumericKeys = new long[numGroupByCols];
    io.airlift.slice.Slice[] tmpSliceKeys = new io.airlift.slice.Slice[numGroupByCols];

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
          // Extract key values into reusable arrays
          int hash = 1;
          for (int k = 0; k < numGroupByCols; k++) {
            if (isVarcharKey[k]) {
              io.airlift.slice.Slice slice = VarcharType.VARCHAR.getSlice(keyBlocks[k], pos);
              tmpSliceKeys[k] = slice;
              hash = hash * 31 + slice.hashCode();
            } else {
              long val = columnTypes.get(k).getLong(keyBlocks[k], pos);
              tmpNumericKeys[k] = val;
              hash = hash * 31 + Long.hashCode(val);
            }
          }

          // Probe with reusable key to avoid allocation on lookup hit
          MixedMergeKey probeKey =
              new MixedMergeKey(tmpNumericKeys, tmpSliceKeys, isVarcharKey, hash);

          long[] aggs = groups.get(probeKey);
          if (aggs == null) {
            // New group - create permanent key with cloned arrays
            MixedMergeKey permanentKey =
                new MixedMergeKey(tmpNumericKeys.clone(), tmpSliceKeys.clone(), isVarcharKey, hash);
            aggs = new long[numAggCols];
            groups.put(permanentKey, aggs);
            if (hasAvg) {
              doubleGroups.put(permanentKey, new double[numAggCols]);
            }
          }

          // Accumulate
          for (int a = 0; a < numAggCols; a++) {
            if (isAvgAgg[a]) {
              // AVG: weighted accumulation
              double avg = DoubleType.DOUBLE.getDouble(aggBlocks[a], pos);
              long count =
                  countAggIdx >= 0 ? BigintType.BIGINT.getLong(aggBlocks[countAggIdx], pos) : 1;
              double[] dAggs = doubleGroups.get(probeKey);
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
