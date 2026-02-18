/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.PageProjection;

class RexPageProjectionFactoryTest {

  private static RexBuilder rexBuilder;
  private static RelDataType bigintType;
  private static RelDataType doubleType;
  private static RelDataType varcharType;

  @BeforeAll
  static void setup() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
  }

  @Test
  @DisplayName("Column pass-through projection")
  void testColumnProjection() {
    RexNode ref = rexBuilder.makeInputRef(bigintType, 0);
    List<PageProjection> projections = RexPageProjectionFactory.createProjections(List.of(ref));

    assertEquals(1, projections.size());

    long[] values = {10, 20, 30};
    Page page = new Page(new LongArrayBlock(3, Optional.empty(), values));
    boolean[] selected = {true, true, true};

    Block result = projections.get(0).project(page, selected);
    assertInstanceOf(LongArrayBlock.class, result);
    LongArrayBlock longResult = (LongArrayBlock) result;
    assertEquals(10, longResult.getLong(0));
    assertEquals(20, longResult.getLong(1));
    assertEquals(30, longResult.getLong(2));
  }

  @Test
  @DisplayName("Integer literal projection")
  void testLiteralProjection() {
    RexNode literal = rexBuilder.makeBigintLiteral(new java.math.BigDecimal(42));
    PageProjection projection = RexPageProjectionFactory.createProjection(literal);

    long[] values = {1, 2, 3};
    Page page = new Page(new LongArrayBlock(3, Optional.empty(), values));
    boolean[] selected = {true, true, true};

    Block result = projection.project(page, selected);
    // makeBigintLiteral creates DECIMAL type, which maps to DoubleArrayBlock
    assertInstanceOf(DoubleArrayBlock.class, result);
    DoubleArrayBlock doubleResult = (DoubleArrayBlock) result;
    assertEquals(3, doubleResult.getPositionCount());
    assertEquals(42.0, doubleResult.getDouble(0), 0.001);
    assertEquals(42.0, doubleResult.getDouble(1), 0.001);
    assertEquals(42.0, doubleResult.getDouble(2), 0.001);
  }

  @Test
  @DisplayName("Arithmetic PLUS projection: col0 + col1")
  void testArithmeticPlus() {
    RexNode plus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeInputRef(bigintType, 1));

    PageProjection projection = RexPageProjectionFactory.createProjection(plus);

    long[] col0 = {10, 20, 30};
    long[] col1 = {1, 2, 3};
    Page page =
        new Page(
            new LongArrayBlock(3, Optional.empty(), col0),
            new LongArrayBlock(3, Optional.empty(), col1));
    boolean[] selected = {true, true, true};

    Block result = projection.project(page, selected);
    assertInstanceOf(DoubleArrayBlock.class, result);
    DoubleArrayBlock doubleResult = (DoubleArrayBlock) result;
    assertEquals(11.0, doubleResult.getDouble(0), 0.001);
    assertEquals(22.0, doubleResult.getDouble(1), 0.001);
    assertEquals(33.0, doubleResult.getDouble(2), 0.001);
  }

  @Test
  @DisplayName("Arithmetic MULTIPLY projection: col0 * literal")
  void testArithmeticMultiply() {
    RexNode multiply =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(3)));

    PageProjection projection = RexPageProjectionFactory.createProjection(multiply);

    long[] values = {2, 4, 6};
    Page page = new Page(new LongArrayBlock(3, Optional.empty(), values));
    boolean[] selected = {true, true, true};

    Block result = projection.project(page, selected);
    assertInstanceOf(DoubleArrayBlock.class, result);
    DoubleArrayBlock doubleResult = (DoubleArrayBlock) result;
    assertEquals(6.0, doubleResult.getDouble(0), 0.001);
    assertEquals(12.0, doubleResult.getDouble(1), 0.001);
    assertEquals(18.0, doubleResult.getDouble(2), 0.001);
  }

  @Test
  @DisplayName("CAST pass-through")
  void testCastPassThrough() {
    // CAST(col0 AS DOUBLE) should just project col0
    RexNode cast = rexBuilder.makeCast(doubleType, rexBuilder.makeInputRef(bigintType, 0));

    PageProjection projection = RexPageProjectionFactory.createProjection(cast);

    long[] values = {100, 200};
    Page page = new Page(new LongArrayBlock(2, Optional.empty(), values));
    boolean[] selected = {true, true};

    Block result = projection.project(page, selected);
    // Should pass through column 0
    assertInstanceOf(LongArrayBlock.class, result);
  }

  @Test
  @DisplayName("Null in arithmetic produces null")
  void testNullInArithmetic() {
    RexNode plus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeInputRef(bigintType, 1));

    PageProjection projection = RexPageProjectionFactory.createProjection(plus);

    long[] col0 = {10, 0, 30};
    boolean[] col0Nulls = {false, true, false};
    long[] col1 = {1, 2, 3};
    Page page =
        new Page(
            new LongArrayBlock(3, Optional.of(col0Nulls), col0),
            new LongArrayBlock(3, Optional.empty(), col1));
    boolean[] selected = {true, true, true};

    Block result = projection.project(page, selected);
    assertInstanceOf(DoubleArrayBlock.class, result);
    DoubleArrayBlock doubleResult = (DoubleArrayBlock) result;
    assertFalse(doubleResult.isNull(0));
    assertTrue(doubleResult.isNull(1));
    assertFalse(doubleResult.isNull(2));
    assertEquals(11.0, doubleResult.getDouble(0), 0.001);
    assertEquals(33.0, doubleResult.getDouble(2), 0.001);
  }

  @Test
  @DisplayName("Selected positions filtering")
  void testSelectedPositions() {
    RexNode ref = rexBuilder.makeInputRef(bigintType, 0);
    PageProjection projection = RexPageProjectionFactory.createProjection(ref);

    long[] values = {10, 20, 30, 40, 50};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), values));
    // Only select positions 0, 2, 4
    boolean[] selected = {true, false, true, false, true};

    Block result = projection.project(page, selected);
    assertEquals(3, result.getPositionCount());
  }

  @Test
  @DisplayName("Unsupported expression throws")
  void testUnsupportedExpression() {
    // LIKE is not supported
    RexNode like =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LIKE,
            rexBuilder.makeInputRef(varcharType, 0),
            rexBuilder.makeLiteral("test%"));

    assertThrows(
        UnsupportedOperationException.class, () -> RexPageProjectionFactory.createProjection(like));
  }
}
