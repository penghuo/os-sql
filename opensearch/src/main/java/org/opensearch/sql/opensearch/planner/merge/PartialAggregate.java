/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Shard-side partial aggregation node for distributed query execution.
 *
 * <p>Supports decomposable aggregation functions:
 * <ul>
 *   <li>COUNT, SUM, SUM0, MIN, MAX — kept as-is (partial and full forms are identical)
 *   <li>AVG — decomposed into SUM + COUNT
 *   <li>STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP — decomposed into COUNT + SUM + SUM(x*x)
 *       for numerically stable coordinator-side merge via Welford/Chan's algorithm
 *   <li>DISTINCT_COUNT_APPROX, PERCENTILE_APPROX — kept as-is (UDAFs handle partial state
 *       internally via HLL and t-digest respectively)
 * </ul>
 */
public class PartialAggregate extends Aggregate {

    private PartialAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    }

    /**
     * Creates a PartialAggregate from an existing Aggregate node.
     *
     * <p>Decomposition rules:
     * <ul>
     *   <li>AVG → SUM + COUNT
     *   <li>STDDEV_SAMP/STDDEV_POP/VAR_SAMP/VAR_POP → COUNT + SUM + SUM(x*x)
     *   <li>DISTINCT_COUNT_APPROX/PERCENTILE_APPROX → kept as-is (UDAF handles partial state)
     *   <li>All others (SUM, COUNT, MIN, MAX) → kept as-is
     * </ul>
     *
     * @param aggregate the original Aggregate node to decompose
     * @return a PartialAggregate, or null if any agg call is not decomposable
     */
    public static PartialAggregate create(Aggregate aggregate) {
        ImmutableList.Builder<AggregateCall> partialCalls = ImmutableList.builder();

        for (AggregateCall call : aggregate.getAggCallList()) {
            if (!isDecomposable(call)) {
                return null;
            }
            SqlKind kind = call.getAggregation().getKind();
            if (kind == SqlKind.AVG) {
                // Decompose AVG into SUM and COUNT on the shard side
                partialCalls.add(decomposeSumForAvg(call));
                partialCalls.add(decomposeCountForAvg(call));
            } else if (kind == SqlKind.STDDEV_SAMP || kind == SqlKind.STDDEV_POP
                    || kind == SqlKind.VAR_SAMP || kind == SqlKind.VAR_POP) {
                // Decompose STDDEV/VAR into COUNT + SUM + SUM(x*x) on the shard side.
                // The coordinator merges using: M2 = sumOfSquares - sum*sum/count,
                // then computes the final STDDEV/VAR from M2 and count.
                partialCalls.add(decomposeCountForStddev(call));
                partialCalls.add(decomposeSumForStddev(call));
                partialCalls.add(decomposeSumSquaresForStddev(call));
            } else if (isApproxUdaf(call)) {
                // DISTINCT_COUNT_APPROX and PERCENTILE_APPROX handle their own partial state
                // internally (HLL and t-digest respectively). Keep the call as-is on shards;
                // the coordinator will merge the binary states.
                partialCalls.add(call);
            } else {
                partialCalls.add(call);
            }
        }

        List<AggregateCall> calls = partialCalls.build();
        RelOptCluster cluster = aggregate.getCluster();
        RelDataType rowType = derivePartialRowType(cluster, aggregate, calls);

        return new PartialAggregate(
                cluster,
                aggregate.getTraitSet(),
                aggregate.getInput(),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                calls);
    }

    /**
     * Checks whether an aggregate call can be decomposed for partial aggregation.
     *
     * <p>Supported kinds: COUNT, SUM, SUM0, AVG, MIN, MAX, STDDEV_SAMP, STDDEV_POP, VAR_SAMP,
     * VAR_POP, and custom UDAFs DISTINCT_COUNT_APPROX and PERCENTILE_APPROX.
     */
    public static boolean isDecomposable(AggregateCall call) {
        SqlKind kind = call.getAggregation().getKind();
        if (kind == SqlKind.COUNT
                || kind == SqlKind.SUM
                || kind == SqlKind.SUM0
                || kind == SqlKind.AVG
                || kind == SqlKind.MIN
                || kind == SqlKind.MAX
                || kind == SqlKind.STDDEV_SAMP
                || kind == SqlKind.STDDEV_POP
                || kind == SqlKind.VAR_SAMP
                || kind == SqlKind.VAR_POP) {
            return true;
        }
        // Check for custom decomposable UDAFs
        return isApproxUdaf(call);
    }

    /**
     * Returns true if the aggregate call is an approximate UDAF that handles its own partial state.
     */
    private static boolean isApproxUdaf(AggregateCall call) {
        String funcName = call.getAggregation().getName();
        return "DISTINCT_COUNT_APPROX".equalsIgnoreCase(funcName)
                || "PERCENTILE_APPROX".equalsIgnoreCase(funcName);
    }

    /**
     * For AVG decomposition: create a SUM call over the same arguments.
     */
    private static AggregateCall decomposeSumForAvg(AggregateCall avgCall) {
        return AggregateCall.create(
                SqlStdOperatorTable.SUM,
                avgCall.isDistinct(),
                avgCall.isApproximate(),
                avgCall.ignoreNulls(),
                avgCall.rexList,
                avgCall.getArgList(),
                avgCall.filterArg,
                avgCall.distinctKeys,
                avgCall.collation,
                avgCall.getType(),
                avgCall.getName() + "$sum");
    }

    /**
     * For AVG decomposition: create a COUNT call over the same arguments.
     */
    private static AggregateCall decomposeCountForAvg(AggregateCall avgCall) {
        return AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                avgCall.isDistinct(),
                avgCall.isApproximate(),
                avgCall.ignoreNulls(),
                avgCall.rexList,
                avgCall.getArgList(),
                avgCall.filterArg,
                avgCall.distinctKeys,
                avgCall.collation,
                avgCall.getType(),
                avgCall.getName() + "$count");
    }

    /**
     * For STDDEV/VAR decomposition: create a COUNT call over the same arguments. Note that COUNT
     * returns BIGINT, not the original STDDEV/VAR type (which is typically DOUBLE).
     */
    private static AggregateCall decomposeCountForStddev(AggregateCall call) {
        return AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                call.rexList,
                call.getArgList(),
                call.filterArg,
                call.distinctKeys,
                call.collation,
                org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY
                        .createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT),
                call.getName() + "$count");
    }

    /**
     * For STDDEV/VAR decomposition: create a SUM call over the same arguments.
     */
    private static AggregateCall decomposeSumForStddev(AggregateCall call) {
        return AggregateCall.create(
                SqlStdOperatorTable.SUM,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                call.rexList,
                call.getArgList(),
                call.filterArg,
                call.distinctKeys,
                call.collation,
                call.getType(),
                call.getName() + "$sum");
    }

    /**
     * For STDDEV/VAR decomposition: create a SUM call over x*x (sum of squares).
     *
     * <p>This reuses the same argument index since the actual squaring happens at the execution
     * layer. The coordinator combines count, sum, and sum_of_squares to compute M2 via:
     * {@code M2 = sumOfSquares - sum * sum / count}
     */
    private static AggregateCall decomposeSumSquaresForStddev(AggregateCall call) {
        return AggregateCall.create(
                SqlStdOperatorTable.SUM,
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                call.rexList,
                call.getArgList(),
                call.filterArg,
                call.distinctKeys,
                call.collation,
                call.getType(),
                call.getName() + "$sumSquares");
    }

    /**
     * Derives the row type for the partial aggregate output. Group keys come first, followed by
     * the partial aggregate columns.
     */
    private static RelDataType derivePartialRowType(
            RelOptCluster cluster,
            Aggregate aggregate,
            List<AggregateCall> partialCalls) {
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        // Add group key columns
        RelDataType inputRowType = aggregate.getInput().getRowType();
        for (int groupIdx : aggregate.getGroupSet()) {
            RelDataTypeField field = inputRowType.getFieldList().get(groupIdx);
            builder.add(field);
        }

        // Add partial aggregate columns
        for (AggregateCall call : partialCalls) {
            builder.add(call.getName(), call.getType());
        }

        return builder.build();
    }

    /**
     * Returns the merge function for a given partial aggregate call kind. Used by
     * MergeAggregateExchange to determine how to merge partial states.
     *
     * <p>STDDEV/VAR kinds are decomposed into COUNT+SUM+SUM(x*x), so their individual partial
     * columns are merged using SUM. The final STDDEV/VAR computation from the merged partials
     * happens in MergeAggregateExchange.
     *
     * @param kind the SqlKind of the partial aggregate
     * @return the SqlAggFunction to use for merging
     */
    public static SqlAggFunction getMergeFunction(SqlKind kind) {
        return switch (kind) {
            case COUNT -> SqlStdOperatorTable.SUM; // merge counts by summing
            case SUM, SUM0 -> SqlStdOperatorTable.SUM;
            case MIN -> SqlStdOperatorTable.MIN;
            case MAX -> SqlStdOperatorTable.MAX;
            // STDDEV/VAR partial columns (count, sum, sumSquares) are all merged by SUM.
            // The final variance/stddev computation is done in MergeAggregateExchange.
            case STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP -> SqlStdOperatorTable.SUM;
            default ->
                    throw new UnsupportedOperationException(
                            "Merge function not supported for: " + kind);
        };
    }

    @Override
    public PartialAggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        return new PartialAggregate(
                getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("partial", true);
    }
}
