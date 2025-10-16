/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

public class CoercionUtilsTest {

  private static final PPLTypeChecker INTEGER_CHECKER = new StubTypeChecker(ExprCoreType.INTEGER);
  private static final PPLTypeChecker DOUBLE_CHECKER = new StubTypeChecker(ExprCoreType.DOUBLE);
  private static final PPLTypeChecker ARRAY_CHECKER = new StubTypeChecker(ExprCoreType.ARRAY);

  private final RexBuilder rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);

  @Test
  public void castArgumentsShouldSafeCastStringToInteger() {
    RexNode varcharLiteral = makeVarcharLiteral("123");

    List<RexNode> casted =
        CoercionUtils.castArguments(rexBuilder, INTEGER_CHECKER, List.of(varcharLiteral));

    assertNotNull(casted);
    assertThat(casted.get(0), instanceOf(RexCall.class));
    RexCall castCall = (RexCall) casted.get(0);
    assertEquals(SqlKind.SAFE_CAST, castCall.getKind());
    assertEquals(SqlTypeName.INTEGER, castCall.getType().getSqlTypeName());
    assertThat(castCall.getOperands().get(0), instanceOf(RexLiteral.class));
    assertEquals("123", ((RexLiteral) castCall.getOperands().get(0)).getValueAs(String.class));
  }

  @Test
  public void castArgumentsShouldSafeCastStringToDouble() {
    RexNode varcharLiteral = makeVarcharLiteral("3.14");

    List<RexNode> casted =
        CoercionUtils.castArguments(rexBuilder, DOUBLE_CHECKER, List.of(varcharLiteral));

    assertNotNull(casted);
    assertThat(casted.get(0), instanceOf(RexCall.class));
    RexCall castCall = (RexCall) casted.get(0);
    assertEquals(SqlKind.SAFE_CAST, castCall.getKind());
    assertEquals(SqlTypeName.DOUBLE, castCall.getType().getSqlTypeName());
  }

  @Test
  public void castArgumentsUsesOriginalNodeWhenTypeAlreadyMatches() {
    RexNode integerLiteral = makeIntegerLiteral(42);

    List<RexNode> casted =
        CoercionUtils.castArguments(rexBuilder, INTEGER_CHECKER, List.of(integerLiteral));

    assertNotNull(casted);
    assertSame(integerLiteral, casted.get(0));
  }

  @Test
  public void castArgumentsReturnsNullWhenCastNotPossible() {
    RexNode varcharLiteral = makeVarcharLiteral("text");

    List<RexNode> casted =
        CoercionUtils.castArguments(rexBuilder, ARRAY_CHECKER, List.of(varcharLiteral));

    assertNull(casted);
  }

  private RexNode makeVarcharLiteral(String value) {
    return rexBuilder.makeLiteral(
        value, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
  }

  private RexNode makeIntegerLiteral(int value) {
    return rexBuilder.makeExactLiteral(
        BigDecimal.valueOf(value), rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
  }

  private static final class StubTypeChecker implements PPLTypeChecker {
    private final List<List<ExprType>> parameterTypes;

    private StubTypeChecker(ExprType... exprTypes) {
      this.parameterTypes = List.of(Arrays.asList(exprTypes));
    }

    @Override
    public boolean checkOperandTypes(List<RelDataType> types) {
      return true;
    }

    @Override
    public String getAllowedSignatures() {
      return "";
    }

    @Override
    public List<List<ExprType>> getParameterTypes() {
      return parameterTypes;
    }
  }
}
