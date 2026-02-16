/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.CountAggregator;
import org.opensearch.sql.expression.aggregation.MaxAggregator;
import org.opensearch.sql.expression.aggregation.MinAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.aggregation.SumAggregator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.distributed.DistributedExchangeOperator;
import org.opensearch.sql.planner.physical.distributed.DistributedHashJoinOperator;
import org.opensearch.sql.planner.physical.distributed.DistributedSortOperator;
import org.opensearch.sql.planner.physical.distributed.FinalAggregationOperator;
import org.opensearch.sql.planner.physical.distributed.JoinType;
import org.opensearch.sql.planner.physical.distributed.PartialAggregationOperator;

/**
 * Converts a Calcite {@link RelNode} tree and its associated {@link Fragment} plan into a
 * distributed {@link PhysicalPlan} tree using the Phase 3 distributed operators.
 *
 * <p>The converter creates two kinds of PhysicalPlan trees:
 *
 * <ul>
 *   <li><b>Coordinator plan:</b> The top-level plan that runs on the coordinator node, reading from
 *       {@link DistributedExchangeOperator} nodes that receive data from source fragments.
 *   <li><b>Source fragment plan:</b> Per-shard plans that scan local data and apply shard-local
 *       operations (partial aggregation, local sort, etc.).
 * </ul>
 */
public class DistributedPlanConverter {

    /**
     * Builds the coordinator-side PhysicalPlan tree for the given distributed query. The returned
     * plan includes {@link DistributedExchangeOperator} nodes as leaf inputs; their exchange
     * buffers must be fed externally by the transport layer.
     *
     * @param relNode the Calcite logical plan root
     * @param fragment the root fragment from FragmentPlanner (must be SINGLE or HASH type)
     * @param sourceCount the number of source fragments (shards) that will feed data
     * @return the coordinator PhysicalPlan tree
     */
    public PhysicalPlan buildCoordinatorPlan(RelNode relNode, Fragment fragment, int sourceCount) {
        if (relNode instanceof LogicalAggregate) {
            return buildAggregationPlan((LogicalAggregate) relNode, fragment, sourceCount);
        } else if (relNode instanceof LogicalSort) {
            return buildSortPlan((LogicalSort) relNode, fragment, sourceCount);
        } else if (relNode instanceof LogicalJoin) {
            return buildJoinPlan((LogicalJoin) relNode, fragment, sourceCount);
        }

        // For non-exchange operators, recurse into children
        for (RelNode input : relNode.getInputs()) {
            if (containsExchangeOperator(input)) {
                return buildCoordinatorPlan(input, fragment, sourceCount);
            }
        }

        // Fallback: no exchange needed, return a simple exchange gather
        ExchangeSpec exchangeSpec = ExchangeSpec.gather();
        return new DistributedExchangeOperator(exchangeSpec, sourceCount);
    }

    /**
     * Builds the source-side PhysicalPlan tree for partial aggregation on a shard.
     *
     * @param aggregate the LogicalAggregate from the Calcite plan
     * @param shardInput the PhysicalPlan that scans shard-local data
     * @return a PartialAggregationOperator wrapping the shard input
     */
    public PhysicalPlan buildSourceAggregationPlan(
            LogicalAggregate aggregate, PhysicalPlan shardInput) {
        List<NamedAggregator> aggregators = extractNamedAggregators(aggregate);
        List<NamedExpression> groupByExprs = extractGroupByExpressions(aggregate);
        return new PartialAggregationOperator(shardInput, aggregators, groupByExprs);
    }

    /**
     * Extracts the list of {@link NamedAggregator} from a Calcite {@link LogicalAggregate}.
     *
     * @param aggregate the Calcite aggregate node
     * @return the list of NamedAggregator instances
     */
    public List<NamedAggregator> extractNamedAggregators(LogicalAggregate aggregate) {
        List<NamedAggregator> result = new ArrayList<>();
        RelDataType inputRowType = aggregate.getInput().getRowType();
        RelDataType outputRowType = aggregate.getRowType();
        int groupCount = aggregate.getGroupCount();

        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            AggregateCall call = aggregate.getAggCallList().get(i);
            String outputName = outputRowType.getFieldList().get(groupCount + i).getName();

            // Build argument expressions
            List<Expression> argExprs = new ArrayList<>();
            if (call.getArgList().isEmpty()) {
                // COUNT(*) - use a dummy reference
                argExprs.add(new ReferenceExpression("*", ExprCoreType.INTEGER));
            } else {
                for (int argIndex : call.getArgList()) {
                    RelDataTypeField field = inputRowType.getFieldList().get(argIndex);
                    ExprCoreType exprType = mapSqlTypeToExprType(field.getType());
                    argExprs.add(new ReferenceExpression(field.getName(), exprType));
                }
            }

            // Determine return type from the aggregate output
            ExprCoreType returnType = mapSqlTypeToExprType(
                    outputRowType.getFieldList().get(groupCount + i).getType());

            // Create the appropriate aggregator
            NamedAggregator namedAgg = createNamedAggregator(
                    outputName, call.getAggregation().getKind(), argExprs, returnType);
            result.add(namedAgg);
        }
        return result;
    }

    /**
     * Extracts group-by expressions from a Calcite {@link LogicalAggregate}.
     *
     * @param aggregate the Calcite aggregate node
     * @return the list of NamedExpression for group-by fields
     */
    public List<NamedExpression> extractGroupByExpressions(LogicalAggregate aggregate) {
        List<NamedExpression> result = new ArrayList<>();
        RelDataType inputRowType = aggregate.getInput().getRowType();

        for (int groupIndex : aggregate.getGroupSet()) {
            RelDataTypeField field = inputRowType.getFieldList().get(groupIndex);
            ExprCoreType exprType = mapSqlTypeToExprType(field.getType());
            ReferenceExpression ref = new ReferenceExpression(field.getName(), exprType);
            result.add(new NamedExpression(field.getName(), ref));
        }
        return result;
    }

    /**
     * Extracts sort specification from a Calcite {@link LogicalSort}.
     *
     * @param sort the Calcite sort node
     * @return list of (SortOption, Expression) pairs
     */
    public List<Pair<SortOption, Expression>> extractSortSpec(LogicalSort sort) {
        List<Pair<SortOption, Expression>> result = new ArrayList<>();
        RelDataType inputRowType = sort.getInput().getRowType();

        for (RelFieldCollation collation : sort.getCollation().getFieldCollations()) {
            int fieldIndex = collation.getFieldIndex();
            RelDataTypeField field = inputRowType.getFieldList().get(fieldIndex);
            ExprCoreType exprType = mapSqlTypeToExprType(field.getType());
            ReferenceExpression ref = new ReferenceExpression(field.getName(), exprType);

            SortOption option;
            if (collation.getDirection() == RelFieldCollation.Direction.DESCENDING) {
                option = SortOption.DEFAULT_DESC;
            } else {
                option = SortOption.DEFAULT_ASC;
            }
            result.add(Pair.of(option, ref));
        }
        return result;
    }

    /**
     * Extracts the LIMIT value from a Calcite {@link LogicalSort}, if present.
     *
     * @param sort the Calcite sort node
     * @return the limit as OptionalLong
     */
    public OptionalLong extractLimit(LogicalSort sort) {
        if (sort.fetch != null) {
            if (sort.fetch instanceof RexLiteral) {
                Number value = (Number) ((RexLiteral) sort.fetch).getValue();
                if (value != null) {
                    return OptionalLong.of(value.longValue());
                }
            }
        }
        return OptionalLong.empty();
    }

    /**
     * Extracts join key expressions from a Calcite {@link LogicalJoin}.
     *
     * @param join the Calcite join node
     * @param side 0 for left side keys, 1 for right side keys
     * @return the list of join key expressions
     */
    public List<Expression> extractJoinKeys(LogicalJoin join, int side) {
        List<Expression> keys = new ArrayList<>();
        RexNode condition = join.getCondition();
        extractJoinKeysFromCondition(condition, join, side, keys);
        return keys;
    }

    /**
     * Maps a Calcite {@link LogicalJoin}'s join type to the distributed {@link JoinType}.
     *
     * @param join the Calcite join node
     * @return the distributed JoinType
     */
    public JoinType mapJoinType(LogicalJoin join) {
        switch (join.getJoinType()) {
            case INNER:
                return JoinType.INNER;
            case LEFT:
                return JoinType.LEFT;
            case RIGHT:
                return JoinType.RIGHT;
            case FULL:
                // FULL OUTER not directly supported, fall back to LEFT
                return JoinType.LEFT;
            default:
                return JoinType.CROSS;
        }
    }

    private PhysicalPlan buildAggregationPlan(
            LogicalAggregate aggregate, Fragment fragment, int sourceCount) {
        ExchangeSpec exchangeSpec = fragment.getExchangeSpec() != null
                ? fragment.getExchangeSpec() : ExchangeSpec.gather();

        DistributedExchangeOperator exchange =
                new DistributedExchangeOperator(exchangeSpec, sourceCount);

        List<NamedAggregator> aggregators = extractNamedAggregators(aggregate);
        List<NamedExpression> groupByExprs = extractGroupByExpressions(aggregate);

        return new FinalAggregationOperator(exchange, aggregators, groupByExprs);
    }

    private PhysicalPlan buildSortPlan(LogicalSort sort, Fragment fragment, int sourceCount) {
        // Check if there's a nested exchange-requiring operator
        RelNode sortInput = sort.getInput();
        PhysicalPlan inputPlan;

        if (sortInput instanceof LogicalAggregate) {
            // Sort over aggregate: build the aggregation plan first, then sort on top
            inputPlan = buildAggregationPlan((LogicalAggregate) sortInput, fragment, sourceCount);
        } else {
            ExchangeSpec exchangeSpec = fragment.getExchangeSpec() != null
                    ? fragment.getExchangeSpec() : ExchangeSpec.gather();
            inputPlan = new DistributedExchangeOperator(exchangeSpec, sourceCount);
        }

        List<Pair<SortOption, Expression>> sortSpec = extractSortSpec(sort);
        OptionalLong limit = extractLimit(sort);

        return new DistributedSortOperator(inputPlan, sortSpec, limit);
    }

    private PhysicalPlan buildJoinPlan(LogicalJoin join, Fragment fragment, int sourceCount) {
        // For join, we need two exchange operators (one per side)
        ExchangeSpec exchangeSpec = fragment.getExchangeSpec() != null
                ? fragment.getExchangeSpec() : ExchangeSpec.gather();

        // Each side gets its own exchange buffer
        DistributedExchangeOperator buildExchange =
                new DistributedExchangeOperator(exchangeSpec, sourceCount);
        DistributedExchangeOperator probeExchange =
                new DistributedExchangeOperator(exchangeSpec, sourceCount);

        List<Expression> buildKeys = extractJoinKeys(join, 0);
        List<Expression> probeKeys = extractJoinKeys(join, 1);
        JoinType joinType = mapJoinType(join);

        return new DistributedHashJoinOperator(
                buildExchange, probeExchange, buildKeys, probeKeys, joinType);
    }

    @SuppressWarnings("unchecked")
    private NamedAggregator createNamedAggregator(
            String name, SqlKind kind, List<Expression> argExprs, ExprCoreType returnType) {
        // The raw-type cast is necessary because each concrete Aggregator uses its own state type
        // (e.g., CountState, SumState) but NamedAggregator requires Aggregator<AggregationState>.
        // This is safe because NamedAggregator delegates all state operations to the concrete impl.
        switch (kind) {
            case COUNT:
                return new NamedAggregator(name,
                        (Aggregator) new CountAggregator(argExprs, ExprCoreType.LONG));
            case SUM:
            case SUM0:
                return new NamedAggregator(name,
                        (Aggregator) new SumAggregator(argExprs, returnType));
            case AVG:
                return new NamedAggregator(name,
                        (Aggregator) new AvgAggregator(argExprs, ExprCoreType.DOUBLE));
            case MIN:
                return new NamedAggregator(name,
                        (Aggregator) new MinAggregator(argExprs, returnType));
            case MAX:
                return new NamedAggregator(name,
                        (Aggregator) new MaxAggregator(argExprs, returnType));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported aggregation function for distributed execution: " + kind);
        }
    }

    /**
     * Maps a Calcite RelDataType to an OpenSearch ExprCoreType. Uses a best-effort mapping based on
     * the SQL type name.
     */
    public static ExprCoreType mapSqlTypeToExprType(RelDataType relDataType) {
        switch (relDataType.getSqlTypeName()) {
            case BOOLEAN:
                return ExprCoreType.BOOLEAN;
            case TINYINT:
                return ExprCoreType.BYTE;
            case SMALLINT:
                return ExprCoreType.SHORT;
            case INTEGER:
                return ExprCoreType.INTEGER;
            case BIGINT:
                return ExprCoreType.LONG;
            case FLOAT:
            case REAL:
                return ExprCoreType.FLOAT;
            case DOUBLE:
            case DECIMAL:
                return ExprCoreType.DOUBLE;
            case VARCHAR:
            case CHAR:
                return ExprCoreType.STRING;
            case DATE:
                return ExprCoreType.DATE;
            case TIME:
                return ExprCoreType.TIME;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ExprCoreType.TIMESTAMP;
            default:
                return ExprCoreType.UNKNOWN;
        }
    }

    private boolean containsExchangeOperator(RelNode relNode) {
        if (relNode instanceof LogicalAggregate
                || relNode instanceof LogicalSort
                || relNode instanceof LogicalJoin) {
            return true;
        }
        for (RelNode input : relNode.getInputs()) {
            if (containsExchangeOperator(input)) {
                return true;
            }
        }
        return false;
    }

    private void extractJoinKeysFromCondition(
            RexNode condition, LogicalJoin join, int side, List<Expression> keys) {
        if (condition instanceof org.apache.calcite.rex.RexCall) {
            org.apache.calcite.rex.RexCall call = (org.apache.calcite.rex.RexCall) condition;
            if (call.getKind() == SqlKind.EQUALS && call.getOperands().size() == 2) {
                // Equi-join: extract field from the requested side
                RexNode operand = call.getOperands().get(side);
                if (operand instanceof RexInputRef) {
                    int index = ((RexInputRef) operand).getIndex();
                    // Adjust index for right side (subtract left field count)
                    RelDataType rowType;
                    if (side == 0) {
                        rowType = join.getLeft().getRowType();
                    } else {
                        rowType = join.getRight().getRowType();
                        index -= join.getLeft().getRowType().getFieldCount();
                    }
                    if (index >= 0 && index < rowType.getFieldList().size()) {
                        RelDataTypeField field = rowType.getFieldList().get(index);
                        ExprCoreType exprType = mapSqlTypeToExprType(field.getType());
                        keys.add(new ReferenceExpression(field.getName(), exprType));
                    }
                }
            } else if (call.getKind() == SqlKind.AND) {
                // Multiple equi-join conditions
                for (RexNode operand : call.getOperands()) {
                    extractJoinKeysFromCondition(operand, join, side, keys);
                }
            }
        }
    }
}
