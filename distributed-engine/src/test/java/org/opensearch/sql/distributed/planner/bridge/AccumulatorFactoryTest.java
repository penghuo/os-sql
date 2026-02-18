/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.operator.aggregation.Accumulator;
import org.opensearch.sql.distributed.operator.aggregation.AvgAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.CountAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.MaxAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.MinAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.SumAccumulator;

class AccumulatorFactoryTest {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
  private static final RelDataType BIGINT_TYPE = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
  private static final RelDataType DOUBLE_TYPE = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);

  @Test
  @DisplayName("COUNT(*) → CountAccumulator")
  void testCountStar() {
    // COUNT(*) has empty argList
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            List.of(),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            BIGINT_TYPE,
            "cnt");

    Supplier<Accumulator> supplier = AccumulatorFactory.createAccumulatorSupplier(aggCall);
    Accumulator acc = supplier.get();
    assertInstanceOf(CountAccumulator.class, acc);
    assertEquals(-1, AccumulatorFactory.resolveInputChannel(aggCall));
  }

  @Test
  @DisplayName("COUNT(column) → CountAccumulator")
  void testCountColumn() {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            RelCollations.EMPTY,
            BIGINT_TYPE,
            "cnt");

    Supplier<Accumulator> supplier = AccumulatorFactory.createAccumulatorSupplier(aggCall);
    Accumulator acc = supplier.get();
    assertInstanceOf(CountAccumulator.class, acc);
    assertEquals(1, AccumulatorFactory.resolveInputChannel(aggCall));
  }

  @Test
  @DisplayName("SUM → SumAccumulator")
  void testSum() {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            DOUBLE_TYPE,
            "total");

    Accumulator acc = AccumulatorFactory.createAccumulatorSupplier(aggCall).get();
    assertInstanceOf(SumAccumulator.class, acc);
    assertEquals(0, AccumulatorFactory.resolveInputChannel(aggCall));
  }

  @Test
  @DisplayName("AVG → AvgAccumulator")
  void testAvg() {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            DOUBLE_TYPE,
            "avg_val");

    assertInstanceOf(
        AvgAccumulator.class, AccumulatorFactory.createAccumulatorSupplier(aggCall).get());
  }

  @Test
  @DisplayName("MIN → MinAccumulator")
  void testMin() {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.MIN,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            DOUBLE_TYPE,
            "min_val");

    assertInstanceOf(
        MinAccumulator.class, AccumulatorFactory.createAccumulatorSupplier(aggCall).get());
  }

  @Test
  @DisplayName("MAX → MaxAccumulator")
  void testMax() {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.MAX,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            DOUBLE_TYPE,
            "max_val");

    assertInstanceOf(
        MaxAccumulator.class, AccumulatorFactory.createAccumulatorSupplier(aggCall).get());
  }

  @Test
  @DisplayName("Unsupported aggregate throws")
  void testUnsupported() {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.COLLECT,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            BIGINT_TYPE,
            "coll");

    assertThrows(
        UnsupportedOperationException.class,
        () -> AccumulatorFactory.createAccumulatorSupplier(aggCall));
  }
}
