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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    // Flatten all pages into a combined list of rows represented as Object arrays
    List<Object[]> allRows = new ArrayList<>();
    for (List<Page> shardPages : shardResults) {
      for (Page page : shardPages) {
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
          Object[] row = new Object[totalCols];
          for (int col = 0; col < totalCols; col++) {
            row[col] = extractValue(page, col, pos, columnTypes.get(col));
          }
          allRows.add(row);
        }
      }
    }

    // Group by the group-by key values
    Map<String, List<Object[]>> groups = new HashMap<>();
    for (Object[] row : allRows) {
      String key = buildGroupKey(row, numGroupByCols);
      groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
    }

    // Merge each group
    List<Object[]> mergedRows = new ArrayList<>();
    for (List<Object[]> groupRows : groups.values()) {
      Object[] mergedRow = new Object[totalCols];

      // Copy group-by key values from the first row in the group
      if (!groupRows.isEmpty()) {
        Object[] firstRow = groupRows.get(0);
        for (int i = 0; i < numGroupByCols; i++) {
          mergedRow[i] = firstRow[i];
        }
      }

      // Merge each aggregate function
      for (int a = 0; a < numAggCols; a++) {
        int colIdx = numGroupByCols + a;
        mergedRow[colIdx] = mergeAggregate(aggregateFunctions.get(a), groupRows, colIdx);
      }

      mergedRows.add(mergedRow);
    }

    // Build result page from merged rows
    return List.of(buildPage(mergedRows, columnTypes));
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
   * Merge-sort shard results for ORDER BY with limit. Combines all shard pages, sorts by the
   * specified sort column indices and directions, and returns the first {@code limit} rows.
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

    // Flatten all pages
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

    // Sort
    Comparator<Object[]> comparator = buildComparator(sortColumnIndices, ascending, nullsFirst);
    allRows.sort(comparator);

    // Apply limit
    List<Object[]> limited = allRows.subList(0, (int) Math.min(limit, allRows.size()));

    // Build result page
    if (limited.isEmpty()) {
      return List.of();
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

  /** Merge a single aggregate expression across all rows in a group. */
  private Object mergeAggregate(String aggExpr, List<Object[]> groupRows, int colIdx) {
    Matcher matcher = AGG_PATTERN.matcher(aggExpr);
    if (!matcher.matches()) {
      throw new UnsupportedOperationException("Unsupported aggregate expression: " + aggExpr);
    }
    String funcName = matcher.group(1).toUpperCase(Locale.ROOT);
    boolean isDistinct = matcher.group(2) != null;

    switch (funcName) {
      case "COUNT":
        if (isDistinct) {
          return sumValues(groupRows, colIdx);
        }
        return sumValues(groupRows, colIdx);
      case "SUM":
        return sumValues(groupRows, colIdx);
      case "MIN":
        return minValue(groupRows, colIdx);
      case "MAX":
        return maxValue(groupRows, colIdx);
      case "AVG":
        return avgValues(groupRows, colIdx);
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
   * Average numeric values for a given column across all rows. Note: this averages the partial
   * averages from each shard, which is only correct for single-shard indices or when all shards
   * have equal row counts.
   */
  private Number avgValues(List<Object[]> rows, int colIdx) {
    double sum = 0.0;
    int count = 0;
    for (Object[] row : rows) {
      Number val = (Number) row[colIdx];
      if (val != null) {
        sum += val.doubleValue();
        count++;
      }
    }
    return count > 0 ? sum / count : null;
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
