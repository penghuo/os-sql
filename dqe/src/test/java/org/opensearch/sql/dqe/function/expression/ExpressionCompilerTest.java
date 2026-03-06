/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.function.FunctionKind;
import org.opensearch.sql.dqe.function.FunctionMetadata;
import org.opensearch.sql.dqe.function.FunctionRegistry;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;

@DisplayName("ExpressionCompiler")
class ExpressionCompilerTest {

  private final DqeSqlParser parser = new DqeSqlParser();
  private ExpressionCompiler compiler;

  // Test page: status (BIGINT) = [100, 200, 300], category (VARCHAR) = ["error", "info", "warn"]
  private Page testPage;

  @BeforeEach
  void setUp() {
    FunctionRegistry registry = new FunctionRegistry();
    registry.register(
        FunctionMetadata.builder()
            .name("upper")
            .argumentTypes(List.of(VarcharType.VARCHAR))
            .returnType(VarcharType.VARCHAR)
            .kind(FunctionKind.SCALAR)
            .scalarImplementation(
                (args, posCount) -> {
                  Block input = args[0];
                  BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, posCount);
                  for (int i = 0; i < posCount; i++) {
                    if (input.isNull(i)) {
                      builder.appendNull();
                    } else {
                      String val =
                          VarcharType.VARCHAR.getSlice(input, i).toStringUtf8().toUpperCase();
                      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(val));
                    }
                  }
                  return builder.build();
                })
            .build());

    Map<String, Integer> columnIndexMap = Map.of("status", 0, "category", 1);
    Map<String, Type> columnTypeMap =
        Map.of("status", BigintType.BIGINT, "category", VarcharType.VARCHAR);
    compiler = new ExpressionCompiler(registry, columnIndexMap, columnTypeMap);

    BlockBuilder statusBuilder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(statusBuilder, 100L);
    BigintType.BIGINT.writeLong(statusBuilder, 200L);
    BigintType.BIGINT.writeLong(statusBuilder, 300L);

    BlockBuilder catBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 3);
    VarcharType.VARCHAR.writeSlice(catBuilder, Slices.utf8Slice("error"));
    VarcharType.VARCHAR.writeSlice(catBuilder, Slices.utf8Slice("info"));
    VarcharType.VARCHAR.writeSlice(catBuilder, Slices.utf8Slice("warn"));

    testPage = new Page(statusBuilder.build(), catBuilder.build());
  }

  @Test
  @DisplayName("Identifier compiles to ColumnReference")
  void identifierCompilation() {
    BlockExpression compiled = compiler.compile(new Identifier("status"));
    assertInstanceOf(ColumnReference.class, compiled);
    assertEquals(BigintType.BIGINT, compiled.getType());
  }

  @Test
  @DisplayName("LongLiteral comparison evaluates correctly")
  void longLiteralCompilation() {
    Expression expr = parser.parseExpression("status = 42");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
  }

  @Test
  @DisplayName("StringLiteral comparison evaluates correctly")
  void stringLiteralCompilation() {
    Expression expr = parser.parseExpression("category = 'error'");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 1));
  }

  @Test
  @DisplayName("ComparisonExpression compiles and evaluates")
  void comparisonCompilation() {
    Expression expr = parser.parseExpression("status > 200");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("ArithmeticBinaryExpression compiles and evaluates")
  void arithmeticCompilation() {
    Expression expr = parser.parseExpression("(status + 100) > 300");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("LogicalExpression AND compiles and evaluates")
  void logicalAndCompilation() {
    Expression expr = parser.parseExpression("status > 100 AND status < 300");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("LogicalExpression OR compiles and evaluates")
  void logicalOrCompilation() {
    Expression expr = parser.parseExpression("status = 100 OR status = 300");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("NotExpression compiles and evaluates")
  void notCompilation() {
    Expression expr = parser.parseExpression("NOT (status = 200)");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("FunctionCall compiles via registry and evaluates")
  void functionCallCompilation() {
    Expression expr = parser.parseExpression("upper(category) = 'ERROR'");
    BlockExpression compiled = compiler.compile(expr);
    Block result = compiled.evaluate(testPage);
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("FunctionCall inserts implicit cast when arg type differs from parameter type")
  void functionCallWithImplicitCoercion() {
    // Register abs(DOUBLE) -> DOUBLE
    FunctionRegistry absRegistry = new FunctionRegistry();
    absRegistry.register(
        FunctionMetadata.builder()
            .name("abs")
            .argumentTypes(List.of(DoubleType.DOUBLE))
            .returnType(DoubleType.DOUBLE)
            .kind(FunctionKind.SCALAR)
            .scalarImplementation(
                (args, posCount) -> {
                  Block input = args[0];
                  BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, posCount);
                  for (int i = 0; i < posCount; i++) {
                    if (input.isNull(i)) {
                      builder.appendNull();
                    } else {
                      double val = DoubleType.DOUBLE.getDouble(input, i);
                      DoubleType.DOUBLE.writeDouble(builder, Math.abs(val));
                    }
                  }
                  return builder.build();
                })
            .build());

    // Compiler with BIGINT column "status"
    ExpressionCompiler absCompiler =
        new ExpressionCompiler(
            absRegistry, Map.of("status", 0), Map.of("status", BigintType.BIGINT));

    // Build page: status (BIGINT) = [-200, 200, 100]
    BlockBuilder statusBuilder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(statusBuilder, -200L);
    BigintType.BIGINT.writeLong(statusBuilder, 200L);
    BigintType.BIGINT.writeLong(statusBuilder, 100L);
    Page absPage = new Page(statusBuilder.build());

    // Compile abs(status) = 200 — requires implicit BIGINT->DOUBLE cast
    Expression expr = parser.parseExpression("abs(status) = 200");
    BlockExpression compiled = absCompiler.compile(expr);
    Block result = compiled.evaluate(absPage);

    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0)); // abs(-200) = 200
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1)); // abs(200) = 200
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2)); // abs(100) != 200
  }
}
