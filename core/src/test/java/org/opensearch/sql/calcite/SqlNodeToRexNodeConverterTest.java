/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.OpenSearchTypeSystem;
import org.opensearch.sql.executor.QueryType;

class SqlNodeToRexNodeConverterTest {

  private CalcitePlanContext context;
  private SqlNodeToRexNodeConverter converter;

  @BeforeEach
  void setUp() {
    FrameworkConfig config =
        Frameworks.newConfigBuilder().typeSystem(OpenSearchTypeSystem.INSTANCE).build();
    context = CalcitePlanContext.create(config, SysLimit.DEFAULT, QueryType.PPL);
    converter = new SqlNodeToRexNodeConverter(context);
  }

  @Test
  void convertStringLiteral() {
    SqlLiteral literal = SqlLiteral.createCharString("abc", SqlParserPos.ZERO);

    RexNode result = converter.convert(literal);
    assertInstanceOf(RexLiteral.class, result);
    assertEquals("abc", ((RexLiteral) result).getValueAs(String.class));
  }

  @Test
  void convertNumericLiteral() {
    SqlLiteral literal = SqlLiteral.createExactNumeric("42", SqlParserPos.ZERO);

    RexNode result = converter.convert(literal);
    assertInstanceOf(RexLiteral.class, result);
    assertEquals(BigDecimal.valueOf(42), ((RexLiteral) result).getValueAs(BigDecimal.class));
  }

  @Test
  void convertBooleanLiteral() {
    SqlLiteral literal = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);

    RexNode result = converter.convert(literal);
    assertInstanceOf(RexLiteral.class, result);
    assertTrue(((RexLiteral) result).getValueAs(Boolean.class));
  }

  @Test
  void convertLowerFunctionCall() {
    SqlLiteral literal = SqlLiteral.createCharString("abc", SqlParserPos.ZERO);
    SqlNode call =
        new SqlBasicCall(SqlStdOperatorTable.LOWER, new SqlNode[] {literal}, SqlParserPos.ZERO);

    RexNode result = converter.convert(call);
    assertInstanceOf(RexCall.class, result);

    RexCall rexCall = (RexCall) result;
    assertEquals(SqlStdOperatorTable.LOWER, rexCall.getOperator());
    assertEquals(1, rexCall.getOperands().size());
    assertEquals("abc", ((RexLiteral) rexCall.getOperands().getFirst()).getValueAs(String.class));
  }

  @Test
  void convertNestedFunctionCall() {
    SqlLiteral first = SqlLiteral.createCharString("a", SqlParserPos.ZERO);
    SqlLiteral second = SqlLiteral.createCharString("b", SqlParserPos.ZERO);
    SqlNode concat =
        new SqlBasicCall(
            SqlLibraryOperators.CONCAT_FUNCTION, new SqlNode[] {first, second}, SqlParserPos.ZERO);
    SqlNode lower =
        new SqlBasicCall(SqlStdOperatorTable.LOWER, new SqlNode[] {concat}, SqlParserPos.ZERO);

    RexNode result = converter.convert(lower);
    assertInstanceOf(RexCall.class, result);

    RexCall outer = (RexCall) result;
    assertEquals(SqlStdOperatorTable.LOWER, outer.getOperator());
    RexNode innerOperand = outer.getOperands().getFirst();
    assertInstanceOf(RexCall.class, innerOperand);

    RexCall inner = (RexCall) innerOperand;
    assertEquals(SqlLibraryOperators.CONCAT_FUNCTION, inner.getOperator());
    assertEquals(
        List.of("a", "b"),
        inner.getOperands().stream()
            .map(op -> ((RexLiteral) op).getValueAs(String.class))
            .toList());
  }
}
