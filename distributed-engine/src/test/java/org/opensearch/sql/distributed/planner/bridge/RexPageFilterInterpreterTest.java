/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import static org.junit.jupiter.api.Assertions.*;

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
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.PageFilter;

class RexPageFilterInterpreterTest {

  private static RexBuilder rexBuilder;
  private static RelDataType bigintType;
  private static RelDataType boolType;

  @BeforeAll
  static void setup() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
  }

  @Test
  @DisplayName("column > literal filter")
  void testGreaterThanLiteral() {
    // col0 > 3
    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(3)));

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);

    long[] values = {1, 2, 3, 4, 5};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), values));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {false, false, false, true, true}, result);
  }

  @Test
  @DisplayName("column = literal filter")
  void testEqualsLiteral() {
    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(3)));

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);
    long[] values = {1, 2, 3, 4, 3};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), values));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {false, false, true, false, true}, result);
  }

  @Test
  @DisplayName("AND filter: col0 > 1 AND col0 < 4")
  void testAndFilter() {
    RexNode gt =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(1)));
    RexNode lt =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(4)));
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.AND, gt, lt);

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);
    long[] values = {1, 2, 3, 4, 5};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), values));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {false, true, true, false, false}, result);
  }

  @Test
  @DisplayName("OR filter: col0 = 1 OR col0 = 5")
  void testOrFilter() {
    RexNode eq1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(1)));
    RexNode eq5 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(5)));
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.OR, eq1, eq5);

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);
    long[] values = {1, 2, 3, 4, 5};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), values));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {true, false, false, false, true}, result);
  }

  @Test
  @DisplayName("NOT filter: NOT (col0 = 3)")
  void testNotFilter() {
    RexNode eq3 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(3)));
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.NOT, eq3);

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);
    long[] values = {1, 2, 3, 4, 5};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), values));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {true, true, false, true, true}, result);
  }

  @Test
  @DisplayName("IS NULL filter")
  void testIsNullFilter() {
    RexNode predicate =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef(bigintType, 0));

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);
    long[] values = {1, 0, 3, 0, 5};
    boolean[] nulls = {false, true, false, true, false};
    Page page = new Page(new LongArrayBlock(5, Optional.of(nulls), values));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {false, true, false, true, false}, result);
  }

  @Test
  @DisplayName("IS NOT NULL filter")
  void testIsNotNullFilter() {
    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef(bigintType, 0));

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);
    long[] values = {1, 0, 3, 0, 5};
    boolean[] nulls = {false, true, false, true, false};
    Page page = new Page(new LongArrayBlock(5, Optional.of(nulls), values));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {true, false, true, false, true}, result);
  }

  @Test
  @DisplayName("Null values in comparison return false")
  void testNullInComparison() {
    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeBigintLiteral(new java.math.BigDecimal(0)));

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);
    long[] values = {1, 0, 3};
    boolean[] nulls = {false, true, false};
    Page page = new Page(new LongArrayBlock(3, Optional.of(nulls), values));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {true, false, true}, result);
  }

  @Test
  @DisplayName("Column vs column comparison")
  void testColumnVsColumn() {
    // col0 > col1
    RexNode predicate =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(bigintType, 0),
            rexBuilder.makeInputRef(bigintType, 1));

    PageFilter filter = RexPageFilterInterpreter.toPageFilter(predicate);
    long[] col0 = {5, 2, 3};
    long[] col1 = {3, 4, 3};
    Page page =
        new Page(
            new LongArrayBlock(3, Optional.empty(), col0),
            new LongArrayBlock(3, Optional.empty(), col1));

    boolean[] result = filter.filter(page);
    assertArrayEquals(new boolean[] {true, false, false}, result);
  }
}
