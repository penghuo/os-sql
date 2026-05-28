/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for {@link ExtendedRexBuilder#makeCast}. The legacy SAFE_CAST/UDT-cast injection
 * (BINARY placeholder for VARCHAR→VARBINARY, IP/DATE/TIME/TIMESTAMP rewrites, NUMBER_TO_STRING for
 * approximate→character) was removed in favor of SqlValidator-driven coercion after RelToSql
 * round-trip; these tests now just verify that {@code makeCast} produces a well-formed RexNode and
 * falls through to {@link RexBuilder#makeCast}.
 */
class ExtendedRexBuilderTest {

  private static final RexBuilder REX_BUILDER =
      new ExtendedRexBuilder(new RexBuilder(TYPE_FACTORY));

  @Test
  void castVarcharToIntegerProducesCastNode() {
    RexNode varcharLiteral = REX_BUILDER.makeLiteral("42");
    RelDataType integer = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RexNode result = REX_BUILDER.makeCast(integer, varcharLiteral);

    assertNotNull(result);
    if (result instanceof RexCall call) {
      assertEquals(
          "BINARY".equals(call.getOperator().getName()),
          false,
          "VARCHAR → INTEGER must not emit BINARY placeholder");
    }
  }

  @Test
  void castIntegerToVarbinaryProducesCastNode() {
    RexNode intLiteral = REX_BUILDER.makeExactLiteral(java.math.BigDecimal.ONE);
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);

    RexNode result = REX_BUILDER.makeCast(varbinary, intLiteral);

    assertNotNull(result);
    if (result instanceof RexCall call) {
      assertEquals(
          "BINARY".equals(call.getOperator().getName()),
          false,
          "non-VARCHAR → VARBINARY must not emit BINARY placeholder");
    }
  }
}
