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
  @DisplayName("LogicalJoin throws UnsupportedPatternException")
  void convertJoinThrows() {
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

    UnsupportedPatternException ex =
        assertThrows(UnsupportedPatternException.class, () -> converter.convert(join));

    assertTrue(ex.getMessage().contains("LogicalJoin"));
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
  @DisplayName("UnsupportedPatternException message includes RelNode type")
  void exceptionIncludesType() {
    var left = createTableScan("users", "id");
    var right = createTableScan("orders", "user_id");
    var join =
        LogicalJoin.create(
            left,
            right,
            List.of(),
            rexBuilder.makeLiteral(true),
            java.util.Set.of(),
            JoinRelType.INNER);

    UnsupportedPatternException ex =
        assertThrows(UnsupportedPatternException.class, () -> converter.convert(join));

    assertNotNull(ex.getMessage());
    assertFalse(ex.getMessage().isEmpty());
  }
}
