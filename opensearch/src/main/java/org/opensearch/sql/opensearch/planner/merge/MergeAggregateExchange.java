/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Exchange operator that merges partial aggregate states from multiple shards.
 *
 * <p>Each shard computes partial aggregates (e.g., partial SUM, partial COUNT). The coordinator
 * merges these using function-specific logic:
 *
 * <ul>
 *   <li>SUM: sum of partial sums
 *   <li>COUNT: sum of partial counts
 *   <li>AVG: sum(partial_sums) / sum(partial_counts)
 *   <li>MIN: min of partial mins
 *   <li>MAX: max of partial maxes
 *   <li>STDDEV/VAR: Welford merge of (count, sum, sumOfSquares) partial columns
 *   <li>DISTINCT_COUNT_APPROX: HLL binary state merge
 *   <li>PERCENTILE_APPROX: t-digest binary state merge
 * </ul>
 */
public class MergeAggregateExchange extends Exchange {

    /**
     * Tracks the merge strategy for each original aggregate call.
     */
    public enum MergeStrategy {
        /** Simple function merge (SUM, COUNT, MIN, MAX). */
        SIMPLE,
        /** Welford merge for STDDEV/VAR (uses 3 partial columns: count, sum, sumSquares). */
        WELFORD,
        /** Binary state merge for HLL/t-digest (uses serialized binary state). */
        BINARY_STATE
    }

    private final ImmutableBitSet groupSet;
    private final ImmutableList<AggregateCall> aggCalls;
    private final ImmutableList<MergeStrategy> mergeStrategies;

    public MergeAggregateExchange(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggCalls,
            List<MergeStrategy> mergeStrategies) {
        super(cluster, traits, input);
        this.groupSet = groupSet;
        this.aggCalls = ImmutableList.copyOf(aggCalls);
        this.mergeStrategies = ImmutableList.copyOf(mergeStrategies);
    }

    /**
     * Legacy constructor that computes merge strategies automatically from the aggregate calls.
     */
    public MergeAggregateExchange(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggCalls) {
        this(cluster, traits, input, groupSet, aggCalls, computeMergeStrategies(aggCalls));
    }

    /**
     * Convenience factory that creates a MergeAggregateExchange with enumerable convention traits.
     */
    public static MergeAggregateExchange create(
            RelNode input, ImmutableBitSet groupSet, List<AggregateCall> aggCalls) {
        RelOptCluster cluster = input.getCluster();
        ImmutableList<MergeStrategy> strategies = computeMergeStrategies(aggCalls);
        return new MergeAggregateExchange(
                cluster, enumerableTraitSet(cluster), input, groupSet, aggCalls, strategies);
    }

    /**
     * Computes the merge strategy for each aggregate call based on its kind and name.
     */
    private static ImmutableList<MergeStrategy> computeMergeStrategies(
            List<AggregateCall> aggCalls) {
        ImmutableList.Builder<MergeStrategy> builder = ImmutableList.builder();
        for (AggregateCall call : aggCalls) {
            SqlKind kind = call.getAggregation().getKind();
            if (kind == SqlKind.STDDEV_SAMP || kind == SqlKind.STDDEV_POP
                    || kind == SqlKind.VAR_SAMP || kind == SqlKind.VAR_POP) {
                builder.add(MergeStrategy.WELFORD);
            } else {
                String name = call.getAggregation().getName();
                if ("DISTINCT_COUNT_APPROX".equalsIgnoreCase(name)
                        || "PERCENTILE_APPROX".equalsIgnoreCase(name)) {
                    builder.add(MergeStrategy.BINARY_STATE);
                } else {
                    builder.add(MergeStrategy.SIMPLE);
                }
            }
        }
        return builder.build();
    }

    @Override
    public String getExchangeType() {
        return "MERGE_AGGREGATE";
    }

    public ImmutableBitSet getGroupSet() {
        return groupSet;
    }

    public ImmutableList<AggregateCall> getAggCalls() {
        return aggCalls;
    }

    public ImmutableList<MergeStrategy> getMergeStrategies() {
        return mergeStrategies;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("groupSet", groupSet)
                .item("aggCalls", aggCalls)
                .item("mergeStrategies", mergeStrategies);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MergeAggregateExchange(
                getCluster(), traitSet, sole(inputs), groupSet, aggCalls, mergeStrategies);
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        if (shardResults == null || shardResults.isEmpty()) {
            return Linq4j.emptyEnumerable();
        }

        int numGroupKeys = groupSet.cardinality();

        // Collect all rows from all shards
        List<Object[]> allRows = new ArrayList<>();
        for (ShardResult result : shardResults) {
            allRows.addAll(result.getRows());
        }

        if (allRows.isEmpty()) {
            return Linq4j.emptyEnumerable();
        }

        // Group rows by group-key columns using a string key for grouping
        Map<String, List<Object[]>> groups = new LinkedHashMap<>();
        for (Object[] row : allRows) {
            String groupKey = buildGroupKey(row, numGroupKeys);
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(row);
        }

        // Merge each group
        List<Object[]> result = new ArrayList<>(groups.size());
        for (List<Object[]> groupRows : groups.values()) {
            result.add(mergeGroup(groupRows, numGroupKeys));
        }

        return new AbstractEnumerable<@Nullable Object>() {
            @Override
            public Enumerator<@Nullable Object> enumerator() {
                return new Enumerator<>() {
                    private int index = -1;

                    @Override
                    public Object current() {
                        return result.get(index);
                    }

                    @Override
                    public boolean moveNext() {
                        return ++index < result.size();
                    }

                    @Override
                    public void reset() {
                        index = -1;
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }

    /** Builds a string key from the group-key columns of a row. */
    private String buildGroupKey(Object[] row, int numGroupKeys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numGroupKeys; i++) {
            if (i > 0) {
                sb.append('\0');
            }
            sb.append(row[i] == null ? "\1NULL\1" : row[i].toString());
        }
        return sb.toString();
    }

    /**
     * Merges partial aggregate rows for a single group. The partial row layout is: [groupKey1, ...,
     * groupKeyN, partialAgg1, partialAgg2, ...] where the number of partial columns per original
     * aggregate depends on the kind and merge strategy:
     *
     * <ul>
     *   <li>AVG (SIMPLE): 2 partial columns (SUM + COUNT) → 1 output (SUM/COUNT)
     *   <li>SUM/COUNT/MIN/MAX (SIMPLE): 1 partial column → 1 output
     *   <li>STDDEV/VAR (WELFORD): 3 partial columns (COUNT + SUM + SUM_SQ) → 1 output
     *   <li>BINARY_STATE: 1 partial column → 1 output
     * </ul>
     *
     * <p>The output row has: [groupKey1, ..., groupKeyN, finalAgg1, finalAgg2, ...]
     */
    private Object[] mergeGroup(List<Object[]> groupRows, int numGroupKeys) {
        // Count output columns: group keys + one output per original agg call
        int outputSize = numGroupKeys + aggCalls.size();
        Object[] output = new Object[outputSize];

        // Copy group keys from first row
        Object[] firstRow = groupRows.get(0);
        System.arraycopy(firstRow, 0, output, 0, numGroupKeys);

        // Merge aggregates — partialColIdx tracks position in the partial row
        int partialColIdx = numGroupKeys;
        for (int aggIdx = 0; aggIdx < aggCalls.size(); aggIdx++) {
            AggregateCall aggCall = aggCalls.get(aggIdx);
            MergeStrategy strategy = mergeStrategies.get(aggIdx);
            SqlKind kind = aggCall.getAggregation().getKind();

            switch (strategy) {
                case SIMPLE:
                    if (kind == SqlKind.AVG) {
                        // AVG decomposed to SUM + COUNT: 2 partial columns → 1 output
                        output[numGroupKeys + aggIdx] =
                                mergeAvg(groupRows, partialColIdx);
                        partialColIdx += 2;
                    } else {
                        // SUM, COUNT, MIN, MAX: 1 partial column → 1 output
                        output[numGroupKeys + aggIdx] =
                                mergeSimple(groupRows, partialColIdx, kind);
                        partialColIdx++;
                    }
                    break;
                case WELFORD:
                    // STDDEV/VAR decomposed to COUNT + SUM + SUM_SQ: 3 partial columns → 1 output
                    output[numGroupKeys + aggIdx] =
                            mergeWelford(groupRows, partialColIdx, kind);
                    partialColIdx += 3;
                    break;
                case BINARY_STATE:
                    // Binary state merge (HLL/t-digest) not yet implemented
                    throw new UnsupportedOperationException(
                            "BINARY_STATE merge not yet implemented for: "
                                    + aggCall.getAggregation().getName());
                default:
                    throw new IllegalStateException("Unknown merge strategy: " + strategy);
            }
        }

        return output;
    }

    /**
     * Merges a SIMPLE aggregate column across all group rows. COUNT values are summed; SUM values
     * are summed; MIN takes the minimum; MAX takes the maximum. AVG is handled separately in
     * mergeGroup since it consumes 2 partial columns.
     */
    private Object mergeSimple(List<Object[]> groupRows, int colIdx, SqlKind kind) {
        switch (kind) {
            case COUNT:
            case SUM:
            case SUM0:
                return sumColumn(groupRows, colIdx);
            case MIN:
                return minColumn(groupRows, colIdx);
            case MAX:
                return maxColumn(groupRows, colIdx);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported SIMPLE merge for: " + kind);
        }
    }

    /**
     * Merges AVG aggregate: partial has SUM at colIdx and COUNT at colIdx+1. Returns SUM/COUNT.
     * Note: the partialColIdx advance for AVG is 2, not 1, handled in mergeGroup.
     */
    private Object mergeAvg(List<Object[]> groupRows, int sumColIdx) {
        double totalSum = 0;
        long totalCount = 0;
        for (Object[] row : groupRows) {
            Number sum = (Number) row[sumColIdx];
            Number count = (Number) row[sumColIdx + 1];
            if (sum != null) {
                totalSum += sum.doubleValue();
            }
            if (count != null) {
                totalCount += count.longValue();
            }
        }
        return totalCount == 0 ? null : totalSum / totalCount;
    }

    /**
     * Merges WELFORD aggregate (STDDEV/VAR). Partial has 3 columns at partialColIdx: count, sum,
     * sumOfSquares. Computes M2 = totalSumSq - totalSum^2 / totalCount, then derives the final
     * statistic based on the SqlKind.
     */
    private Object mergeWelford(List<Object[]> groupRows, int partialColIdx, SqlKind kind) {
        long totalCount = 0;
        double totalSum = 0;
        double totalSumSq = 0;

        for (Object[] row : groupRows) {
            Number count = (Number) row[partialColIdx];
            Number sum = (Number) row[partialColIdx + 1];
            Number sumSq = (Number) row[partialColIdx + 2];
            if (count != null) {
                totalCount += count.longValue();
            }
            if (sum != null) {
                totalSum += sum.doubleValue();
            }
            if (sumSq != null) {
                totalSumSq += sumSq.doubleValue();
            }
        }

        if (totalCount == 0) {
            return null;
        }

        double m2 = totalSumSq - (totalSum * totalSum) / totalCount;

        switch (kind) {
            case VAR_POP:
                return m2 / totalCount;
            case VAR_SAMP:
                return totalCount <= 1 ? null : m2 / (totalCount - 1);
            case STDDEV_POP:
                return Math.sqrt(m2 / totalCount);
            case STDDEV_SAMP:
                return totalCount <= 1 ? null : Math.sqrt(m2 / (totalCount - 1));
            default:
                throw new IllegalStateException("Unexpected WELFORD kind: " + kind);
        }
    }

    @SuppressWarnings("unchecked")
    private Object sumColumn(List<Object[]> rows, int colIdx) {
        double sum = 0;
        boolean hasValue = false;
        for (Object[] row : rows) {
            Object val = row[colIdx];
            if (val instanceof Number) {
                sum += ((Number) val).doubleValue();
                hasValue = true;
            }
        }
        if (!hasValue) {
            return null;
        }
        // Preserve integer type if all values are integral
        Object firstNonNull = null;
        for (Object[] row : rows) {
            if (row[colIdx] != null) {
                firstNonNull = row[colIdx];
                break;
            }
        }
        if (firstNonNull instanceof Long || firstNonNull instanceof Integer) {
            return (long) sum;
        }
        return sum;
    }

    @SuppressWarnings("unchecked")
    private Object minColumn(List<Object[]> rows, int colIdx) {
        Comparable<Object> min = null;
        for (Object[] row : rows) {
            Object val = row[colIdx];
            if (val != null) {
                if (min == null || ((Comparable<Object>) val).compareTo((Object) min) < 0) {
                    min = (Comparable<Object>) val;
                }
            }
        }
        return min;
    }

    @SuppressWarnings("unchecked")
    private Object maxColumn(List<Object[]> rows, int colIdx) {
        Comparable<Object> max = null;
        for (Object[] row : rows) {
            Object val = row[colIdx];
            if (val != null) {
                if (max == null || ((Comparable<Object>) val).compareTo((Object) max) > 0) {
                    max = (Comparable<Object>) val;
                }
            }
        }
        return max;
    }
}
