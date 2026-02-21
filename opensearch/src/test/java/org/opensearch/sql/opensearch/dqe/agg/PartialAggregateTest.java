/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.agg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.dqe.exchange.MergeFunction;

class PartialAggregateTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private RelBuilder relBuilder;

  private final RelDataType rowType =
      typeFactory.createStructType(
          List.of(
              typeFactory.createSqlType(SqlTypeName.INTEGER),
              typeFactory.createSqlType(SqlTypeName.BIGINT),
              typeFactory.createSqlType(SqlTypeName.DOUBLE)),
          List.of("x", "y", "z"));

  @BeforeEach
  void setUp() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    root.add(
        "t",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory tf) {
            return rowType;
          }
        });
    relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build());
  }

  /** Helper to build an Aggregate using AggregateCall.create with explicit argList. */
  private Aggregate buildAggregate(List<AggregateCall> calls, int... groupKeys) {
    org.apache.calcite.util.ImmutableBitSet groupSet =
        org.apache.calcite.util.ImmutableBitSet.of(groupKeys);
    RelNode scan = relBuilder.scan("t").build();
    relBuilder.push(scan);
    return (Aggregate) relBuilder.aggregate(relBuilder.groupKey(groupSet), calls).build();
  }

  /** Helper to create an AggregateCall with explicit args. */
  private AggregateCall makeCall(
      org.apache.calcite.sql.SqlAggFunction function,
      boolean distinct,
      RelDataType returnType,
      String name,
      int... argIndices) {
    return AggregateCall.create(
        function,
        distinct,
        false,
        false,
        ImmutableList.of(),
        ImmutableList.copyOf(
            java.util.Arrays.stream(argIndices).boxed().collect(java.util.stream.Collectors.toList())),
        -1,
        null,
        RelCollations.EMPTY,
        returnType,
        name);
  }

  @Test
  @DisplayName("COUNT(*) decomposes to shard COUNT with SUM_COUNTS merge")
  void countStar_decomposes() {
    AggregateCall countStar =
        makeCall(
            SqlStdOperatorTable.COUNT,
            false,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate agg = buildAggregate(List.of(countStar));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    PartialAggregateSpec spec = result.get();
    assertEquals(List.of(MergeFunction.SUM_COUNTS), spec.getMergeFunctions());
    assertEquals(0, spec.getGroupCount());

    Aggregate shardAgg = spec.getShardAggregate();
    assertEquals(1, shardAgg.getAggCallList().size());
    assertEquals(SqlKind.COUNT, shardAgg.getAggCallList().get(0).getAggregation().getKind());
  }

  @Test
  @DisplayName("COUNT(field) decomposes to shard COUNT(field) with SUM_COUNTS merge")
  void countField_decomposes() {
    AggregateCall countField =
        makeCall(
            SqlStdOperatorTable.COUNT,
            false,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt",
            0);
    Aggregate agg = buildAggregate(List.of(countField));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    PartialAggregateSpec spec = result.get();
    assertEquals(List.of(MergeFunction.SUM_COUNTS), spec.getMergeFunctions());

    Aggregate shardAgg = spec.getShardAggregate();
    assertEquals(1, shardAgg.getAggCallList().size());
    AggregateCall shardCall = shardAgg.getAggCallList().get(0);
    assertEquals(SqlKind.COUNT, shardCall.getAggregation().getKind());
    assertEquals(List.of(0), shardCall.getArgList());
  }

  @Test
  @DisplayName("COUNT(DISTINCT x) returns empty (not decomposable)")
  void countDistinct_returnsEmpty() {
    AggregateCall countDistinct =
        makeCall(
            SqlStdOperatorTable.COUNT,
            true,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt_distinct",
            0);
    Aggregate agg = buildAggregate(List.of(countDistinct));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertFalse(result.isPresent());
  }

  @Test
  @DisplayName("AVG(x) decomposes to shard SUM + COUNT (2 columns) with SUM_DIV_COUNT merge")
  void avg_decomposes() {
    AggregateCall avgCall =
        makeCall(
            SqlStdOperatorTable.AVG,
            false,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "avg_x",
            0);
    Aggregate agg = buildAggregate(List.of(avgCall));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    PartialAggregateSpec spec = result.get();
    assertEquals(List.of(MergeFunction.SUM_DIV_COUNT), spec.getMergeFunctions());
    assertEquals(0, spec.getGroupCount());

    Aggregate shardAgg = spec.getShardAggregate();
    // AVG expands to SUM + COUNT = 2 shard calls
    assertEquals(2, shardAgg.getAggCallList().size());
    assertEquals(SqlKind.SUM, shardAgg.getAggCallList().get(0).getAggregation().getKind());
    assertEquals(SqlKind.COUNT, shardAgg.getAggCallList().get(1).getAggregation().getKind());
  }

  @Test
  @DisplayName("SUM(x), MIN(y), MAX(z) all decompose in same aggregate")
  void sumMinMax_allDecompose() {
    AggregateCall sumCall =
        makeCall(
            SqlStdOperatorTable.SUM,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            "sum_x",
            0);
    AggregateCall minCall =
        makeCall(
            SqlStdOperatorTable.MIN,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            "min_y",
            1);
    AggregateCall maxCall =
        makeCall(
            SqlStdOperatorTable.MAX,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.DOUBLE), true),
            "max_z",
            2);
    Aggregate agg = buildAggregate(List.of(sumCall, minCall, maxCall));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    PartialAggregateSpec spec = result.get();
    assertEquals(
        List.of(MergeFunction.SUM_SUMS, MergeFunction.MIN_OF, MergeFunction.MAX_OF),
        spec.getMergeFunctions());
    assertEquals(0, spec.getGroupCount());

    Aggregate shardAgg = spec.getShardAggregate();
    assertEquals(3, shardAgg.getAggCallList().size());
    assertEquals(SqlKind.SUM, shardAgg.getAggCallList().get(0).getAggregation().getKind());
    assertEquals(SqlKind.MIN, shardAgg.getAggCallList().get(1).getAggregation().getKind());
    assertEquals(SqlKind.MAX, shardAgg.getAggCallList().get(2).getAggregation().getKind());
  }

  @Test
  @DisplayName("Non-decomposable aggregate (VAR_POP) returns empty")
  void nonDecomposable_returnsEmpty() {
    AggregateCall varPopCall =
        makeCall(
            SqlStdOperatorTable.VAR_POP,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "var_pop_x",
            0);
    Aggregate agg = buildAggregate(List.of(varPopCall));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertFalse(result.isPresent());
  }

  @Test
  @DisplayName("Mixed decomposable + non-decomposable returns empty")
  void mixedDecomposableAndNon_returnsEmpty() {
    AggregateCall countStar =
        makeCall(
            SqlStdOperatorTable.COUNT,
            false,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    AggregateCall varPopCall =
        makeCall(
            SqlStdOperatorTable.VAR_POP,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "var_pop_x",
            0);
    Aggregate agg = buildAggregate(List.of(countStar, varPopCall));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertFalse(result.isPresent());
  }

  @Test
  @DisplayName("Aggregate with GROUP BY keys preserves group count")
  void aggregateWithGroupBy_preservesGroupCount() {
    AggregateCall countStar =
        makeCall(
            SqlStdOperatorTable.COUNT,
            false,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    AggregateCall sumCall =
        makeCall(
            SqlStdOperatorTable.SUM,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.DOUBLE), true),
            "sum_z",
            2);
    Aggregate agg = buildAggregate(List.of(countStar, sumCall), 0, 1);
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    PartialAggregateSpec spec = result.get();
    assertEquals(2, spec.getGroupCount());
    assertEquals(
        List.of(MergeFunction.SUM_COUNTS, MergeFunction.SUM_SUMS), spec.getMergeFunctions());

    Aggregate shardAgg = spec.getShardAggregate();
    assertEquals(2, shardAgg.getGroupCount());
  }

  @Test
  @DisplayName("Global aggregate (0 GROUP BY keys) decomposes correctly")
  void globalAggregate_decomposes() {
    AggregateCall countStar =
        makeCall(
            SqlStdOperatorTable.COUNT,
            false,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    AggregateCall sumCall =
        makeCall(
            SqlStdOperatorTable.SUM,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            "sum_x",
            0);
    AggregateCall avgCall =
        makeCall(
            SqlStdOperatorTable.AVG,
            false,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "avg_x",
            0);
    AggregateCall minCall =
        makeCall(
            SqlStdOperatorTable.MIN,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "min_x",
            0);
    AggregateCall maxCall =
        makeCall(
            SqlStdOperatorTable.MAX,
            false,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "max_x",
            0);
    Aggregate agg = buildAggregate(List.of(countStar, sumCall, avgCall, minCall, maxCall));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    PartialAggregateSpec spec = result.get();
    assertEquals(0, spec.getGroupCount());
    assertEquals(
        List.of(
            MergeFunction.SUM_COUNTS,
            MergeFunction.SUM_SUMS,
            MergeFunction.SUM_DIV_COUNT,
            MergeFunction.MIN_OF,
            MergeFunction.MAX_OF),
        spec.getMergeFunctions());

    // AVG expands to SUM + COUNT, so 5 original calls become 6 shard calls
    Aggregate shardAgg = spec.getShardAggregate();
    assertEquals(6, shardAgg.getAggCallList().size());
  }

  @Test
  @DisplayName("AVG shard aggregate produces correctly named SUM and COUNT columns")
  void avgShardColumns_correctlyNamed() {
    AggregateCall avgCall =
        makeCall(
            SqlStdOperatorTable.AVG,
            false,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "avg_x",
            0);
    Aggregate agg = buildAggregate(List.of(avgCall));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    Aggregate shardAgg = result.get().getShardAggregate();
    assertEquals("avg_x__sum", shardAgg.getAggCallList().get(0).getName());
    assertEquals("avg_x__count", shardAgg.getAggCallList().get(1).getName());
  }

  @Test
  @DisplayName("Shard aggregate preserves the same input as the original aggregate")
  void shardAggregate_preservesInput() {
    AggregateCall countStar =
        makeCall(
            SqlStdOperatorTable.COUNT,
            false,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate agg = buildAggregate(List.of(countStar));
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    Aggregate shardAgg = result.get().getShardAggregate();
    assertEquals(agg.getInput(), shardAgg.getInput());
  }

  @Test
  @DisplayName("Shard aggregate preserves GROUP BY sets from original aggregate")
  void shardAggregate_preservesGroupSets() {
    AggregateCall countStar =
        makeCall(
            SqlStdOperatorTable.COUNT,
            false,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    Aggregate agg = buildAggregate(List.of(countStar), 0);
    Optional<PartialAggregateSpec> result = PartialAggregate.decompose(agg);

    assertTrue(result.isPresent());
    Aggregate shardAgg = result.get().getShardAggregate();
    assertEquals(agg.getGroupSet(), shardAgg.getGroupSet());
    assertEquals(agg.getGroupSets(), shardAgg.getGroupSets());
  }
}
