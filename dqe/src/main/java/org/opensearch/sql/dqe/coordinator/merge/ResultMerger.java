/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.merge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;

/**
 * Merges partial results from multiple shards on the coordinator. Supports three merge modes:
 *
 * <ol>
 *   <li><b>Passthrough</b>: Non-aggregate queries -- concatenate JSON rows from all shards.
 *   <li><b>Aggregation merge</b>: Run FINAL aggregation over partial results from shards.
 *   <li><b>Sorted merge with limit</b>: Merge pre-sorted shard results and apply final limit.
 * </ol>
 */
public class ResultMerger {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAPS =
      new TypeReference<>() {};

  /**
   * Pattern to extract the aggregate function name and optional column from expressions like {@code
   * COUNT(*)}, {@code SUM(amount)}, {@code MIN(price)}, {@code MAX(salary)}.
   */
  private static final Pattern AGG_PATTERN =
      Pattern.compile("^(COUNT|SUM|MIN|MAX)\\((.+)\\)$", Pattern.CASE_INSENSITIVE);

  /** Merge shard results for a non-aggregate query. Simple concatenation of all rows. */
  public List<Map<String, Object>> mergePassthrough(List<String> shardJsonResults) {
    List<Map<String, Object>> allRows = new ArrayList<>();
    for (String json : shardJsonResults) {
      allRows.addAll(parseJsonRows(json));
    }
    return allRows;
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
   */
  public List<Map<String, Object>> mergeAggregation(
      List<String> shardJsonResults, AggregationNode finalAggNode) {

    List<String> groupByKeys = finalAggNode.getGroupByKeys();
    List<String> aggregateFunctions = finalAggNode.getAggregateFunctions();

    // Collect all rows from all shards
    List<Map<String, Object>> allRows = new ArrayList<>();
    for (String json : shardJsonResults) {
      allRows.addAll(parseJsonRows(json));
    }

    // Group by the group-by key values. For global aggregations (no group-by keys),
    // all rows belong to a single group with an empty key.
    Map<String, List<Map<String, Object>>> groups =
        allRows.stream().collect(Collectors.groupingBy(row -> buildGroupKey(row, groupByKeys)));

    // Merge each group
    List<Map<String, Object>> result = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> entry : groups.entrySet()) {
      List<Map<String, Object>> groupRows = entry.getValue();
      Map<String, Object> mergedRow = new LinkedHashMap<>();

      // Copy group-by key values from the first row in the group
      if (!groupRows.isEmpty()) {
        Map<String, Object> firstRow = groupRows.get(0);
        for (String key : groupByKeys) {
          mergedRow.put(key, firstRow.get(key));
        }
      }

      // Merge each aggregate function
      for (String aggExpr : aggregateFunctions) {
        mergedRow.put(aggExpr, mergeAggregate(aggExpr, groupRows));
      }

      result.add(mergedRow);
    }
    return result;
  }

  /**
   * Merge-sort shard results for ORDER BY with limit. Combines all shard rows, sorts by the
   * specified sort keys and directions, and returns the first {@code limit} rows.
   */
  public List<Map<String, Object>> mergeSorted(
      List<String> shardJsonResults, List<String> sortKeys, List<Boolean> ascending, long limit) {

    List<Map<String, Object>> allRows = new ArrayList<>();
    for (String json : shardJsonResults) {
      allRows.addAll(parseJsonRows(json));
    }

    Comparator<Map<String, Object>> comparator = buildComparator(sortKeys, ascending);
    allRows.sort(comparator);

    return allRows.subList(0, (int) Math.min(limit, allRows.size()));
  }

  /** Parse a JSON array string into a list of row maps. */
  private List<Map<String, Object>> parseJsonRows(String json) {
    try {
      return MAPPER.readValue(json, LIST_OF_MAPS);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse shard JSON result: " + e.getMessage(), e);
    }
  }

  /** Build a string key from the group-by column values of a row. */
  private String buildGroupKey(Map<String, Object> row, List<String> groupByKeys) {
    if (groupByKeys.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < groupByKeys.size(); i++) {
      if (i > 0) {
        sb.append('\0');
      }
      Object val = row.get(groupByKeys.get(i));
      sb.append(val == null ? "null" : val.toString());
    }
    return sb.toString();
  }

  /** Merge a single aggregate expression across all rows in a group. */
  private Number mergeAggregate(String aggExpr, List<Map<String, Object>> groupRows) {
    Matcher matcher = AGG_PATTERN.matcher(aggExpr);
    if (!matcher.matches()) {
      throw new UnsupportedOperationException("Unsupported aggregate expression: " + aggExpr);
    }
    String funcName = matcher.group(1).toUpperCase(Locale.ROOT);

    switch (funcName) {
      case "COUNT":
      case "SUM":
        return sumValues(aggExpr, groupRows);
      case "MIN":
        return minValue(aggExpr, groupRows);
      case "MAX":
        return maxValue(aggExpr, groupRows);
      default:
        throw new UnsupportedOperationException("Unsupported aggregate function: " + funcName);
    }
  }

  /** Sum numeric values for a given column across all rows. */
  private Number sumValues(String column, List<Map<String, Object>> rows) {
    long sum = 0;
    boolean hasDouble = false;
    double dSum = 0.0;

    for (Map<String, Object> row : rows) {
      Number val = (Number) row.get(column);
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

  /** Find the minimum numeric value for a given column across all rows. */
  private Number minValue(String column, List<Map<String, Object>> rows) {
    double min = Double.MAX_VALUE;
    boolean hasDouble = false;
    long longMin = Long.MAX_VALUE;

    for (Map<String, Object> row : rows) {
      Number val = (Number) row.get(column);
      if (val != null) {
        if (val instanceof Double || val instanceof Float) {
          hasDouble = true;
          min = Math.min(min, val.doubleValue());
        } else {
          longMin = Math.min(longMin, val.longValue());
          min = Math.min(min, val.doubleValue());
        }
      }
    }
    return hasDouble ? min : longMin;
  }

  /** Find the maximum numeric value for a given column across all rows. */
  private Number maxValue(String column, List<Map<String, Object>> rows) {
    double max = -Double.MAX_VALUE;
    boolean hasDouble = false;
    long longMax = Long.MIN_VALUE;

    for (Map<String, Object> row : rows) {
      Number val = (Number) row.get(column);
      if (val != null) {
        if (val instanceof Double || val instanceof Float) {
          hasDouble = true;
          max = Math.max(max, val.doubleValue());
        } else {
          longMax = Math.max(longMax, val.longValue());
          max = Math.max(max, val.doubleValue());
        }
      }
    }
    return hasDouble ? max : longMax;
  }

  /**
   * Build a composite comparator for sorting rows by multiple keys with independent sort
   * directions.
   */
  @SuppressWarnings("unchecked")
  private Comparator<Map<String, Object>> buildComparator(
      List<String> sortKeys, List<Boolean> ascending) {

    Comparator<Map<String, Object>> comparator = (a, b) -> 0;

    for (int i = 0; i < sortKeys.size(); i++) {
      String key = sortKeys.get(i);
      boolean asc = ascending.get(i);

      Comparator<Map<String, Object>> keyComparator =
          (row1, row2) -> {
            Object v1 = row1.get(key);
            Object v2 = row2.get(key);
            if (v1 == null && v2 == null) return 0;
            if (v1 == null) return 1;
            if (v2 == null) return -1;

            if (v1 instanceof Comparable && v2 instanceof Comparable) {
              return ((Comparable<Object>) v1).compareTo(v2);
            }
            return v1.toString().compareTo(v2.toString());
          };

      if (!asc) {
        keyComparator = keyComparator.reversed();
      }
      comparator = comparator.thenComparing(keyComparator);
    }
    return comparator;
  }
}
