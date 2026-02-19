/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests that unsupported patterns throw UnsupportedPatternException to trigger DSL fallback. */
class ConvertUnsupportedTest extends ConverterTestBase {

  @Test
  @DisplayName("LogicalJoin converts to JoinNode")
  void convertJoinProducesJoinNode() {
    var left = createTableScan("users", "id", "name");
    var right = createTableScan("orders", "user_id", "amount");
    var join =
        LogicalJoin.create(
            left,
            right,
            List.of(),
            rexBuilder.makeLiteral(true),
            java.util.Set.of(),
            JoinRelType.INNER);

    PlanNode result = converter.convert(join);

    assertInstanceOf(JoinNode.class, result);
    JoinNode joinNode = (JoinNode) result;
    assertEquals(JoinRelType.INNER, joinNode.getJoinType());
    assertEquals(2, joinNode.getSources().size());
  }

  @Test
  @DisplayName("LogicalUnion throws UnsupportedPatternException")
  void convertUnionThrows() {
    var left = createTableScan("test1", "name", "age");
    var right = createTableScan("test2", "name", "age");
    var union = LogicalUnion.create(List.of(left, right), true);

    UnsupportedPatternException ex =
        assertThrows(UnsupportedPatternException.class, () -> converter.convert(union));

    assertTrue(ex.getMessage().contains("LogicalUnion"));
  }

  @Test
  @DisplayName("LogicalJoin with equi-join extracts key columns")
  void joinExtractsKeys() {
    var left = createTableScan("users", "id", "name");
    var right = createTableScan("orders", "user_id", "amount");

    // Build an equi-join condition: left.$0 = right.$0 (which is $2 in combined row)
    var condition =
        rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(
                typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(
                typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), 2));

    var join =
        LogicalJoin.create(
            left, right, List.of(), condition, java.util.Set.of(), JoinRelType.INNER);

    PlanNode result = converter.convert(join);

    assertInstanceOf(JoinNode.class, result);
    JoinNode joinNode = (JoinNode) result;
    assertEquals(List.of(0), joinNode.getLeftJoinKeys());
    assertEquals(List.of(0), joinNode.getRightJoinKeys());
  }
}
