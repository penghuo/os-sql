/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.opensearch.dqe.exchange.MergeFunction;

/**
 * Decomposes a Calcite {@link Aggregate} into a shard-side partial aggregate and coordinator-side
 * merge functions.
 *
 * <p>If all aggregate calls are decomposable (COUNT, SUM, AVG, MIN, MAX without DISTINCT), returns
 * a {@link PartialAggregateSpec}. Otherwise returns {@link Optional#empty()}, meaning the entire
 * aggregate must run on the coordinator.
 */
public final class PartialAggregate {

  private PartialAggregate() {}

  /**
   * Attempt to decompose an Aggregate into partial + final.
   *
   * @param agg the Calcite Aggregate to decompose
   * @return a {@link PartialAggregateSpec} if the aggregate is decomposable, or empty if not
   */
  public static Optional<PartialAggregateSpec> decompose(Aggregate agg) {
    List<AggregateCall> shardCalls = new ArrayList<>();
    List<MergeFunction> mergeFunctions = new ArrayList<>();
    RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();
    RelDataType inputRowType = agg.getInput().getRowType();

    for (AggregateCall call : agg.getAggCallList()) {
      if (call.isDistinct()) {
        return Optional.empty();
      }

      SqlKind kind = call.getAggregation().getKind();
      switch (kind) {
        case COUNT:
          shardCalls.add(call);
          mergeFunctions.add(MergeFunction.SUM_COUNTS);
          break;

        case SUM:
          shardCalls.add(call);
          mergeFunctions.add(MergeFunction.SUM_SUMS);
          break;

        case AVG:
          // AVG decomposes into two shard-side calls: SUM and COUNT.
          RelDataType sumType = inferSumReturnType(call, inputRowType, typeFactory);
          RelDataType countType = countReturnType(typeFactory);
          AggregateCall sumForAvg =
              AggregateCall.create(
                  SqlStdOperatorTable.SUM,
                  false,
                  call.isApproximate(),
                  false,
                  call.rexList,
                  call.getArgList(),
                  -1,
                  call.distinctKeys,
                  call.collation,
                  sumType,
                  call.getName() + "__sum");
          AggregateCall countForAvg =
              AggregateCall.create(
                  SqlStdOperatorTable.COUNT,
                  false,
                  call.isApproximate(),
                  false,
                  call.rexList,
                  call.getArgList(),
                  -1,
                  call.distinctKeys,
                  call.collation,
                  countType,
                  call.getName() + "__count");
          shardCalls.add(sumForAvg);
          shardCalls.add(countForAvg);
          mergeFunctions.add(MergeFunction.SUM_DIV_COUNT);
          break;

        case MIN:
          shardCalls.add(call);
          mergeFunctions.add(MergeFunction.MIN_OF);
          break;

        case MAX:
          shardCalls.add(call);
          mergeFunctions.add(MergeFunction.MAX_OF);
          break;

        default:
          return Optional.empty();
      }
    }

    int groupCount = agg.getGroupCount();

    // Use copy() to create the shard aggregate, which constructs a new LogicalAggregate
    // with the modified aggregate call list while preserving traits and structure.
    Aggregate shardAggregate =
        agg.copy(
            agg.getTraitSet(),
            agg.getInput(),
            agg.getGroupSet(),
            agg.getGroupSets(),
            shardCalls);

    return Optional.of(new PartialAggregateSpec(shardAggregate, mergeFunctions, groupCount));
  }

  /**
   * Infers the return type of SUM when decomposing AVG. Calcite's SUM preserves the input type (as
   * nullable). For example, SUM(INTEGER) returns nullable INTEGER.
   */
  private static RelDataType inferSumReturnType(
      AggregateCall originalCall,
      RelDataType inputRowType,
      RelDataTypeFactory typeFactory) {
    if (!originalCall.getArgList().isEmpty()) {
      RelDataType argType =
          inputRowType.getFieldList().get(originalCall.getArgList().get(0)).getType();
      return typeFactory.createTypeWithNullability(argType, true);
    }
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT), true);
  }

  /**
   * Returns the Calcite-canonical return type for COUNT: BIGINT NOT NULL.
   */
  private static RelDataType countReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT), false);
  }
}
