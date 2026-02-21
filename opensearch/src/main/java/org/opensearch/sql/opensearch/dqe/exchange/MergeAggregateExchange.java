/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Merges partial aggregate states from multiple shards into final results.
 *
 * <p>Each shard computes partial aggregates (e.g., local COUNT, local SUM). The coordinator
 * combines them using aggregation-specific {@link MergeFunction}s.
 *
 * <p>Rows are grouped by the first {@code groupCount} columns. For each group, the merge functions
 * are applied to the aggregate columns. Note that {@link MergeFunction#SUM_DIV_COUNT} consumes TWO
 * consecutive input columns (sum, count) and produces ONE output column.
 */
public class MergeAggregateExchange extends Exchange {

  private final List<MergeFunction> mergeFunctions;
  private final int groupCount;
  private List<Object[]> mergedRows = Collections.emptyList();

  /**
   * Creates a MergeAggregateExchange.
   *
   * @param shardPlan the plan fragment to execute on each shard
   * @param rowType the output row type
   * @param mergeFunctions one MergeFunction per output aggregate column
   * @param groupCount the number of leading columns used as group keys
   */
  public MergeAggregateExchange(
      RelNode shardPlan,
      RelDataType rowType,
      List<MergeFunction> mergeFunctions,
      int groupCount) {
    super(shardPlan, rowType);
    this.mergeFunctions = mergeFunctions;
    this.groupCount = groupCount;
  }

  @Override
  public void setShardResults(List<ShardResult> results) {
    // Group rows by the first groupCount columns across all shards
    Map<GroupKey, List<Object[]>> groups = new LinkedHashMap<>();
    for (ShardResult result : results) {
      for (Object[] row : result.getRows()) {
        GroupKey key = new GroupKey(row, groupCount);
        groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      }
    }

    List<Object[]> output = new ArrayList<>();
    for (Map.Entry<GroupKey, List<Object[]>> entry : groups.entrySet()) {
      Object[] merged = mergeGroup(entry.getKey(), entry.getValue());
      output.add(merged);
    }
    this.mergedRows = output;
  }

  @Override
  public Iterator<Object[]> scan() {
    return mergedRows.iterator();
  }

  /** Returns the merge functions. */
  public List<MergeFunction> getMergeFunctions() {
    return mergeFunctions;
  }

  /** Returns the group count. */
  public int getGroupCount() {
    return groupCount;
  }

  /**
   * Merges all rows belonging to a single group into one output row.
   *
   * <p>The output row has groupCount key columns followed by one column per MergeFunction. The
   * input columns start at index groupCount and advance according to each MergeFunction's
   * consumption (SUM_DIV_COUNT consumes 2 input columns; all others consume 1).
   */
  private Object[] mergeGroup(GroupKey key, List<Object[]> rows) {
    int outputSize = groupCount + mergeFunctions.size();
    Object[] output = new Object[outputSize];

    // Copy group key columns
    System.arraycopy(key.values, 0, output, 0, groupCount);

    // Merge aggregate columns
    int inputIdx = groupCount;
    for (int i = 0; i < mergeFunctions.size(); i++) {
      MergeFunction func = mergeFunctions.get(i);
      output[groupCount + i] = applyMerge(func, rows, inputIdx);
      // SUM_DIV_COUNT consumes two input columns; others consume one
      inputIdx += (func == MergeFunction.SUM_DIV_COUNT) ? 2 : 1;
    }

    return output;
  }

  /**
   * Applies a single merge function across all rows for a specific aggregate column.
   *
   * @param func the merge function to apply
   * @param rows all partial rows for this group
   * @param colIdx the starting column index in the input rows
   * @return the merged value
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Object applyMerge(MergeFunction func, List<Object[]> rows, int colIdx) {
    switch (func) {
      case SUM_COUNTS:
        return sumNumbersWithDefault(rows, colIdx);
      case SUM_SUMS:
        return sumNumbersWithDefault(rows, colIdx);
      case SUM_DIV_COUNT:
        {
          Number totalSum = sumNumbers(rows, colIdx);
          Number totalCount = sumNumbers(rows, colIdx + 1);
          if (totalSum == null || totalCount == null) {
            return null;
          }
          long countVal = totalCount.longValue();
          if (countVal == 0) {
            return null;
          }
          return totalSum.doubleValue() / countVal;
        }
      case MIN_OF:
        {
          Comparable result = null;
          for (Object[] row : rows) {
            Object val = row[colIdx];
            if (val != null) {
              Comparable cval = (Comparable) val;
              if (result == null || cval.compareTo(result) < 0) {
                result = cval;
              }
            }
          }
          return result;
        }
      case MAX_OF:
        {
          Comparable result = null;
          for (Object[] row : rows) {
            Object val = row[colIdx];
            if (val != null) {
              Comparable cval = (Comparable) val;
              if (result == null || cval.compareTo(result) > 0) {
                result = cval;
              }
            }
          }
          return result;
        }
      default:
        throw new UnsupportedOperationException("Unknown merge function: " + func);
    }
  }

  /**
   * Sums numeric values at a specific column index across all rows. Returns 0 if all values are
   * null. This matches OpenSearch aggregation behavior where SUM and COUNT return 0 for empty or
   * all-null inputs, rather than SQL-standard NULL.
   */
  private Number sumNumbersWithDefault(List<Object[]> rows, int colIdx) {
    Number result = sumNumbers(rows, colIdx);
    return result != null ? result : 0L;
  }

  /**
   * Sums numeric values at a specific column index across all rows. Returns null if all values are
   * null.
   */
  private Number sumNumbers(List<Object[]> rows, int colIdx) {
    boolean allNull = true;
    double sum = 0.0;
    boolean hasDoubleOrFloat = false;

    for (Object[] row : rows) {
      Object val = row[colIdx];
      if (val != null) {
        allNull = false;
        Number num = (Number) val;
        sum += num.doubleValue();
        if (val instanceof Double || val instanceof Float) {
          hasDoubleOrFloat = true;
        }
      }
    }

    if (allNull) {
      return null;
    }

    // Preserve integer semantics when all inputs are integral types
    if (!hasDoubleOrFloat && sum == Math.floor(sum) && !Double.isInfinite(sum)) {
      return (long) sum;
    }
    return sum;
  }

  /** A group key wrapping the first N columns of a row for use as a Map key. */
  private static class GroupKey {
    final Object[] values;

    GroupKey(Object[] row, int groupCount) {
      this.values = new Object[groupCount];
      System.arraycopy(row, 0, this.values, 0, groupCount);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof GroupKey)) {
        return false;
      }
      return Arrays.equals(values, ((GroupKey) o).values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }
}
