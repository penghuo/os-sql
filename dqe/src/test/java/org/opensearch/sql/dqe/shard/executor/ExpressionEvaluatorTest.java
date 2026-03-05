/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Expression;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;

@DisplayName("ExpressionEvaluator")
class ExpressionEvaluatorTest {

  private final DqeSqlParser parser = new DqeSqlParser();

  @Nested
  @DisplayName("BIGINT comparisons")
  class BigintComparisons {

    private final Map<String, Integer> columnIndexMap = Map.of("status", 0);
    private final Map<String, Type> columnTypeMap = Map.of("status", BigintType.BIGINT);
    private final ExpressionEvaluator evaluator =
        new ExpressionEvaluator(columnIndexMap, columnTypeMap);

    // Page: status = [100, 200, 300, 500]
    private final Page page = buildBigintPage(100L, 200L, 300L, 500L);

    @Test
    void equalityMatches() {
      Expression expr = parser.parseExpression("status = 200");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 1));
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 2));
    }

    @Test
    void greaterThan() {
      Expression expr = parser.parseExpression("status > 200");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0));
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 2));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 3));
    }

    @Test
    void greaterThanOrEqual() {
      Expression expr = parser.parseExpression("status >= 200");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 1));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 2));
    }

    @Test
    void lessThan() {
      Expression expr = parser.parseExpression("status < 200");
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 0));
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1));
    }

    @Test
    void lessThanOrEqual() {
      Expression expr = parser.parseExpression("status <= 200");
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 0));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 1));
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 2));
    }

    @Test
    void notEqual() {
      Expression expr = parser.parseExpression("status <> 200");
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 0));
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 2));
    }
  }

  @Nested
  @DisplayName("String comparisons")
  class StringComparisons {

    private final Map<String, Integer> columnIndexMap = Map.of("category", 0);
    private final Map<String, Type> columnTypeMap = Map.of("category", VarcharType.VARCHAR);
    private final ExpressionEvaluator evaluator =
        new ExpressionEvaluator(columnIndexMap, columnTypeMap);

    // Page: category = ["error", "info", "warn"]
    private final Page page = buildVarcharPage("error", "info", "warn");

    @Test
    void stringEquality() {
      Expression expr = parser.parseExpression("category = 'error'");
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 0));
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1));
    }

    @Test
    void stringInequality() {
      Expression expr = parser.parseExpression("category <> 'info'");
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 0));
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 2));
    }
  }

  @Nested
  @DisplayName("Double comparisons")
  class DoubleComparisons {

    private final Map<String, Integer> columnIndexMap = Map.of("amount", 0);
    private final Map<String, Type> columnTypeMap = Map.of("amount", DoubleType.DOUBLE);
    private final ExpressionEvaluator evaluator =
        new ExpressionEvaluator(columnIndexMap, columnTypeMap);

    // Page: amount = [1.5, 5.0, 10.0]
    private final Page page = buildDoublePage(1.5, 5.0, 10.0);

    @Test
    void doubleGreaterThan() {
      Expression expr = parser.parseExpression("amount > 5.0");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0));
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 2));
    }

    @Test
    void doubleEquality() {
      Expression expr = parser.parseExpression("amount = 5E0");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 1));
    }
  }

  @Nested
  @DisplayName("Logical operators")
  class LogicalOperators {

    private final Map<String, Integer> columnIndexMap = Map.of("status", 0, "category", 1);
    private final Map<String, Type> columnTypeMap =
        Map.of("status", BigintType.BIGINT, "category", VarcharType.VARCHAR);
    private final ExpressionEvaluator evaluator =
        new ExpressionEvaluator(columnIndexMap, columnTypeMap);

    // Page: status = [500, 200], category = ["error", "info"]
    private final Page page =
        buildTwoColumnPage(new long[] {500, 200}, new String[] {"error", "info"});

    @Test
    void andBothTrue() {
      Expression expr = parser.parseExpression("status = 500 AND category = 'error'");
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 0));
    }

    @Test
    void andOneFalse() {
      Expression expr = parser.parseExpression("status = 500 AND category = 'error'");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1));
    }

    @Test
    void orOneTrue() {
      Expression expr = parser.parseExpression("status = 500 OR category = 'info'");
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 0));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 1));
    }

    @Test
    void notExpression() {
      Expression expr = parser.parseExpression("NOT (status = 500)");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 1));
    }
  }

  @Nested
  @DisplayName("NULL handling")
  class NullHandling {

    private final Map<String, Integer> columnIndexMap = Map.of("status", 0);
    private final Map<String, Type> columnTypeMap = Map.of("status", BigintType.BIGINT);
    private final ExpressionEvaluator evaluator =
        new ExpressionEvaluator(columnIndexMap, columnTypeMap);

    // Page: status = [null, 200]
    private final Page page = buildBigintPageWithNull();

    @Test
    void comparisonWithNullReturnsFalse() {
      Expression expr = parser.parseExpression("status > 100");
      // null > 100 -> null, treated as false
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0));
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 1));
    }

    @Test
    void evaluateReturnsNullForNullComparison() {
      Expression expr = parser.parseExpression("status > 100");
      assertNull(evaluator.evaluate(expr, page, 0));
    }
  }

  @Nested
  @DisplayName("Arithmetic in predicates")
  class ArithmeticPredicates {

    private final Map<String, Integer> columnIndexMap = Map.of("status", 0);
    private final Map<String, Type> columnTypeMap = Map.of("status", BigintType.BIGINT);
    private final ExpressionEvaluator evaluator =
        new ExpressionEvaluator(columnIndexMap, columnTypeMap);

    // Page: status = [400, 500, 600]
    private final Page page = buildBigintPage(400L, 500L, 600L);

    @Test
    void additionInPredicate() {
      Expression expr = parser.parseExpression("(status + 100) > 600");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0)); // 500 > 600 = false
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1)); // 600 > 600 = false
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 2)); // 700 > 600 = true
    }

    @Test
    void multiplicationInPredicate() {
      Expression expr = parser.parseExpression("(status * 2) > 1000");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0)); // 800 > 1000 = false
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1)); // 1000 > 1000 = false
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 2)); // 1200 > 1000 = true
    }

    @Test
    void subtractionInPredicate() {
      Expression expr = parser.parseExpression("(status - 100) = 400");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0)); // 300 = 400
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 1)); // 400 = 400
    }
  }

  @Nested
  @DisplayName("Double arithmetic in predicates")
  class DoubleArithmeticPredicates {

    private final Map<String, Integer> columnIndexMap = Map.of("amount", 0);
    private final Map<String, Type> columnTypeMap = Map.of("amount", DoubleType.DOUBLE);
    private final ExpressionEvaluator evaluator =
        new ExpressionEvaluator(columnIndexMap, columnTypeMap);

    // Page: amount = [3.0, 5.0, 8.0]
    private final Page page = buildDoublePage(3.0, 5.0, 8.0);

    @Test
    void doubleMultiplicationInPredicate() {
      Expression expr = parser.parseExpression("(amount * 2E0) > 10E0");
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 0)); // 6.0 > 10.0 = false
      assertFalse(evaluator.evaluateAsBoolean(expr, page, 1)); // 10.0 > 10.0 = false
      assertTrue(evaluator.evaluateAsBoolean(expr, page, 2)); // 16.0 > 10.0 = true
    }
  }

  // ---- Page builder helpers ----

  private static Page buildBigintPage(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(builder.build());
  }

  private static Page buildVarcharPage(String... values) {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, values.length);
    for (String v : values) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(v));
    }
    return new Page(builder.build());
  }

  private static Page buildDoublePage(double... values) {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, values.length);
    for (double v : values) {
      DoubleType.DOUBLE.writeDouble(builder, v);
    }
    return new Page(builder.build());
  }

  private static Page buildBigintPageWithNull() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 2);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 200L);
    return new Page(builder.build());
  }

  private static Page buildTwoColumnPage(long[] longs, String[] strings) {
    BlockBuilder longBuilder = BigintType.BIGINT.createBlockBuilder(null, longs.length);
    for (long v : longs) {
      BigintType.BIGINT.writeLong(longBuilder, v);
    }
    BlockBuilder strBuilder = VarcharType.VARCHAR.createBlockBuilder(null, strings.length);
    for (String v : strings) {
      VarcharType.VARCHAR.writeSlice(strBuilder, Slices.utf8Slice(v));
    }
    return new Page(longBuilder.build(), strBuilder.build());
  }
}
