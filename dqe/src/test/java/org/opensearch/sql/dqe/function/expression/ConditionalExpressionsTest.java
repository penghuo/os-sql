/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Expression;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.function.FunctionRegistry;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;

@DisplayName("Conditional and pattern expressions")
class ConditionalExpressionsTest {

  private final DqeSqlParser parser = new DqeSqlParser();
  private ExpressionCompiler compiler;
  private Page testPage;

  @BeforeEach
  void setUp() {
    FunctionRegistry registry = new FunctionRegistry();
    Map<String, Integer> columnIndexMap = Map.of("status", 0, "category", 1);
    Map<String, Type> columnTypeMap =
        Map.of("status", BigintType.BIGINT, "category", VarcharType.VARCHAR);
    compiler = new ExpressionCompiler(registry, columnIndexMap, columnTypeMap);

    // status = [100, 200, 500], category = ["error", null, "info"]
    BlockBuilder statusBuilder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(statusBuilder, 100L);
    BigintType.BIGINT.writeLong(statusBuilder, 200L);
    BigintType.BIGINT.writeLong(statusBuilder, 500L);

    BlockBuilder catBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 3);
    VarcharType.VARCHAR.writeSlice(catBuilder, Slices.utf8Slice("error"));
    catBuilder.appendNull();
    VarcharType.VARCHAR.writeSlice(catBuilder, Slices.utf8Slice("info"));

    testPage = new Page(statusBuilder.build(), catBuilder.build());
  }

  @Test
  @DisplayName("CAST(42 AS DOUBLE) produces DOUBLE block")
  void castIntToDouble() {
    Expression expr = parser.parseExpression("CAST(42 AS DOUBLE) = 42E0");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
  }

  @Test
  @DisplayName("CAST('123' AS BIGINT) produces BIGINT block")
  void castStringToBigint() {
    Expression expr = parser.parseExpression("CAST('123' AS BIGINT) = 123");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
  }

  @Test
  @DisplayName("CASE WHEN status > 200 THEN 'error' ELSE 'ok' END")
  void searchedCaseExpression() {
    Expression expr =
        parser.parseExpression("CASE WHEN status > 200 THEN 'error' ELSE 'ok' END = 'ok'");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("COALESCE(category, 'default') returns first non-null")
  void coalesceExpression() {
    Expression expr = parser.parseExpression("COALESCE(category, 'default') = 'default'");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("NULLIF(status, 200) returns null when equal")
  void nullIfExpression() {
    Expression expr = parser.parseExpression("NULLIF(status, 200) IS NULL");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("category LIKE '%err%' matches pattern")
  void likeExpression() {
    Expression expr = parser.parseExpression("category LIKE '%err%'");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertTrue(result.isNull(1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("status IN (200, 500) checks membership")
  void inExpression() {
    Expression expr = parser.parseExpression("status IN (200, 500)");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 2));
  }
}
