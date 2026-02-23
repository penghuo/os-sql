/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.types.DqeTypes;

class ExpressionEvaluatorTests {

  private Page createBigintPage(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(values.length, builder.build());
  }

  private Page createBigintPageWithNull(Long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (Long v : values) {
      if (v == null) {
        builder.appendNull();
      } else {
        BigintType.BIGINT.writeLong(builder, v);
      }
    }
    return new Page(values.length, builder.build());
  }

  private Page createVarcharPage(String... values) {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, values.length);
    for (String v : values) {
      if (v == null) {
        builder.appendNull();
      } else {
        VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(v));
      }
    }
    return new Page(values.length, builder.build());
  }

  @Test
  @DisplayName("evaluate long literal returns value")
  void evaluateLongLiteral() {
    TypedExpression typed = new TypedExpression(new LongLiteral("42"), DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    Page page = createBigintPage(0); // dummy page with 1 row
    assertEquals(42L, eval.evaluate(page, 0));
  }

  @Test
  @DisplayName("evaluate string literal returns value")
  void evaluateStringLiteral() {
    TypedExpression typed = new TypedExpression(new StringLiteral("hello"), DqeTypes.VARCHAR);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    Page page = createBigintPage(0);
    assertEquals("hello", eval.evaluate(page, 0));
  }

  @Test
  @DisplayName("evaluate boolean literal returns value")
  void evaluateBooleanLiteral() {
    TypedExpression typed = new TypedExpression(BooleanLiteral.TRUE_LITERAL, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    Page page = createBigintPage(0);
    assertEquals(true, eval.evaluate(page, 0));
  }

  @Test
  @DisplayName("evaluate null literal returns null")
  void evaluateNullLiteral() {
    TypedExpression typed = new TypedExpression(new NullLiteral(), DqeTypes.VARCHAR);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    Page page = createBigintPage(0);
    assertNull(eval.evaluate(page, 0));
  }

  @Test
  @DisplayName("evaluate column reference reads from page")
  void evaluateColumnReference() {
    TypedExpression typed = new TypedExpression(new Identifier("age"), DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of("age", 0));

    Page page = createBigintPage(25, 30);
    assertEquals(25L, eval.evaluate(page, 0));
    assertEquals(30L, eval.evaluate(page, 1));
  }

  @Test
  @DisplayName("evaluate column reference returns null for null position")
  void evaluateColumnReferenceNull() {
    TypedExpression typed = new TypedExpression(new Identifier("age"), DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of("age", 0));

    Page page = createBigintPageWithNull(null, 30L);
    assertNull(eval.evaluate(page, 0));
    assertEquals(30L, eval.evaluate(page, 1));
  }

  @Test
  @DisplayName("comparison: EQUAL returns true for equal values")
  void comparisonEqual() {
    ComparisonExpression cmp =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL, new LongLiteral("10"), new LongLiteral("10"));
    TypedExpression typed = new TypedExpression(cmp, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(true, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("comparison: LESS_THAN works correctly")
  void comparisonLessThan() {
    ComparisonExpression cmp =
        new ComparisonExpression(
            ComparisonExpression.Operator.LESS_THAN, new LongLiteral("5"), new LongLiteral("10"));
    TypedExpression typed = new TypedExpression(cmp, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(true, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("comparison: null operand returns null (three-valued logic)")
  void comparisonNullReturnsNull() {
    ComparisonExpression cmp =
        new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL, new NullLiteral(), new LongLiteral("10"));
    TypedExpression typed = new TypedExpression(cmp, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertNull(eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("arithmetic ADD")
  void arithmeticAdd() {
    ArithmeticBinaryExpression arith =
        new ArithmeticBinaryExpression(
            ArithmeticBinaryExpression.Operator.ADD, new LongLiteral("3"), new LongLiteral("4"));
    TypedExpression typed = new TypedExpression(arith, DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(7L, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("arithmetic SUBTRACT")
  void arithmeticSubtract() {
    ArithmeticBinaryExpression arith =
        new ArithmeticBinaryExpression(
            ArithmeticBinaryExpression.Operator.SUBTRACT,
            new LongLiteral("10"),
            new LongLiteral("3"));
    TypedExpression typed = new TypedExpression(arith, DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(7L, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("arithmetic MULTIPLY")
  void arithmeticMultiply() {
    ArithmeticBinaryExpression arith =
        new ArithmeticBinaryExpression(
            ArithmeticBinaryExpression.Operator.MULTIPLY,
            new LongLiteral("5"),
            new LongLiteral("3"));
    TypedExpression typed = new TypedExpression(arith, DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(15L, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("arithmetic DIVIDE by zero throws")
  void arithmeticDivideByZero() {
    ArithmeticBinaryExpression arith =
        new ArithmeticBinaryExpression(
            ArithmeticBinaryExpression.Operator.DIVIDE,
            new LongLiteral("10"),
            new LongLiteral("0"));
    TypedExpression typed = new TypedExpression(arith, DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertThrows(DqeException.class, () -> eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("arithmetic with null operand returns null")
  void arithmeticWithNull() {
    ArithmeticBinaryExpression arith =
        new ArithmeticBinaryExpression(
            ArithmeticBinaryExpression.Operator.ADD, new NullLiteral(), new LongLiteral("10"));
    TypedExpression typed = new TypedExpression(arith, DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertNull(eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("logical AND: true AND true = true")
  void logicalAndTrueTrue() {
    LogicalExpression logical =
        new LogicalExpression(
            LogicalExpression.Operator.AND,
            List.of(BooleanLiteral.TRUE_LITERAL, BooleanLiteral.TRUE_LITERAL));
    TypedExpression typed = new TypedExpression(logical, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(true, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("logical AND: true AND false = false")
  void logicalAndTrueFalse() {
    LogicalExpression logical =
        new LogicalExpression(
            LogicalExpression.Operator.AND,
            List.of(BooleanLiteral.TRUE_LITERAL, BooleanLiteral.FALSE_LITERAL));
    TypedExpression typed = new TypedExpression(logical, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(false, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("logical OR: false OR true = true")
  void logicalOrFalseTrue() {
    LogicalExpression logical =
        new LogicalExpression(
            LogicalExpression.Operator.OR,
            List.of(BooleanLiteral.FALSE_LITERAL, BooleanLiteral.TRUE_LITERAL));
    TypedExpression typed = new TypedExpression(logical, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(true, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("IS NULL for null value returns true")
  void isNullForNull() {
    IsNullPredicate isNull = new IsNullPredicate(new NullLiteral());
    TypedExpression typed = new TypedExpression(isNull, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(true, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("IS NULL for non-null value returns false")
  void isNullForNonNull() {
    IsNullPredicate isNull = new IsNullPredicate(new LongLiteral("42"));
    TypedExpression typed = new TypedExpression(isNull, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(false, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("IS NOT NULL for non-null value returns true")
  void isNotNullForNonNull() {
    IsNotNullPredicate isNotNull = new IsNotNullPredicate(new LongLiteral("42"));
    TypedExpression typed = new TypedExpression(isNotNull, DqeTypes.BOOLEAN);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(true, eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("evaluateAll produces block with correct values")
  void evaluateAllProducesBlock() {
    TypedExpression typed = new TypedExpression(new Identifier("val"), DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of("val", 0));

    Page page = createBigintPage(10, 20, 30);
    Block result = eval.evaluateAll(page);

    assertEquals(3, result.getPositionCount());
    assertEquals(10L, result.getLong(0, 0));
    assertEquals(20L, result.getLong(1, 0));
    assertEquals(30L, result.getLong(2, 0));
  }

  @Test
  @DisplayName("writeValue writes null correctly")
  void writeValueNull() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    ExpressionEvaluator.writeValue(builder, BigintType.BIGINT, null);
    Block block = builder.build();

    assertTrue(block.isNull(0));
  }

  @Test
  @DisplayName("writeValue writes bigint correctly")
  void writeValueBigint() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    ExpressionEvaluator.writeValue(builder, BigintType.BIGINT, 42L);
    Block block = builder.build();

    assertEquals(42L, block.getLong(0, 0));
  }

  @Test
  @DisplayName("writeValue writes varchar correctly")
  void writeValueVarchar() {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 1);
    ExpressionEvaluator.writeValue(builder, VarcharType.VARCHAR, "hello");
    Block block = builder.build();

    assertEquals("hello", block.getSlice(0, 0, block.getSliceLength(0)).toStringUtf8());
  }

  @Test
  @DisplayName("writeValue writes boolean correctly")
  void writeValueBoolean() {
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 1);
    ExpressionEvaluator.writeValue(builder, BooleanType.BOOLEAN, true);
    Block block = builder.build();

    assertTrue(BooleanType.BOOLEAN.getBoolean(block, 0));
  }

  @Test
  @DisplayName("unknown column throws DqeException")
  void unknownColumnThrows() {
    TypedExpression typed = new TypedExpression(new Identifier("missing"), DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of("age", 0));

    assertThrows(DqeException.class, () -> eval.evaluate(createBigintPage(0), 0));
  }

  @Test
  @DisplayName("getOutputType returns the expression type")
  void getOutputType() {
    TypedExpression typed = new TypedExpression(new LongLiteral("1"), DqeTypes.BIGINT);
    ExpressionEvaluator eval = new ExpressionEvaluator(typed, Map.of());

    assertEquals(DqeTypes.BIGINT, eval.getOutputType());
  }
}
