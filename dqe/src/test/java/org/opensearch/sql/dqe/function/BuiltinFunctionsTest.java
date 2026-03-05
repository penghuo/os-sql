/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("BuiltinFunctions registry")
class BuiltinFunctionsTest {

  @Test
  @DisplayName("Registers all built-in scalar functions")
  void registersAllBuiltins() {
    FunctionRegistry registry = BuiltinFunctions.createRegistry();

    // String functions
    assertDoesNotThrow(() -> registry.resolve("upper", List.of(VarcharType.VARCHAR)));
    assertDoesNotThrow(() -> registry.resolve("lower", List.of(VarcharType.VARCHAR)));
    assertDoesNotThrow(() -> registry.resolve("length", List.of(VarcharType.VARCHAR)));
    assertDoesNotThrow(
        () -> registry.resolve("concat", List.of(VarcharType.VARCHAR, VarcharType.VARCHAR)));
    assertDoesNotThrow(() -> registry.resolve("reverse", List.of(VarcharType.VARCHAR)));

    // Math functions
    assertDoesNotThrow(() -> registry.resolve("abs", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(() -> registry.resolve("ceil", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(() -> registry.resolve("sqrt", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(() -> registry.resolve("pi", List.of()));

    // Date/time functions
    assertDoesNotThrow(() -> registry.resolve("year", List.of(TimestampType.TIMESTAMP_MILLIS)));
    assertDoesNotThrow(() -> registry.resolve("month", List.of(TimestampType.TIMESTAMP_MILLIS)));
    assertDoesNotThrow(() -> registry.resolve("now", List.of()));

    // Trig functions
    assertDoesNotThrow(() -> registry.resolve("sin", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(() -> registry.resolve("cos", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(
        () -> registry.resolve("atan2", List.of(DoubleType.DOUBLE, DoubleType.DOUBLE)));

    // Aliases
    assertDoesNotThrow(() -> registry.resolve("ceiling", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(
        () ->
            registry.resolve(
                "substr", List.of(VarcharType.VARCHAR, BigintType.BIGINT, BigintType.BIGINT)));
    assertDoesNotThrow(
        () -> registry.resolve("pow", List.of(DoubleType.DOUBLE, DoubleType.DOUBLE)));
  }

  @Test
  @DisplayName("Registers all aggregate functions")
  void registersAllAggregates() {
    FunctionRegistry registry = BuiltinFunctions.createRegistry();

    assertDoesNotThrow(() -> registry.resolve("count", List.of()));
    assertDoesNotThrow(() -> registry.resolve("count", List.of(BigintType.BIGINT)));
    assertDoesNotThrow(() -> registry.resolve("sum", List.of(BigintType.BIGINT)));
    assertDoesNotThrow(() -> registry.resolve("sum", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(() -> registry.resolve("avg", List.of(BigintType.BIGINT)));
    assertDoesNotThrow(() -> registry.resolve("avg", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(() -> registry.resolve("min", List.of(BigintType.BIGINT)));
    assertDoesNotThrow(() -> registry.resolve("max", List.of(VarcharType.VARCHAR)));
    assertDoesNotThrow(() -> registry.resolve("stddev", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(() -> registry.resolve("variance", List.of(DoubleType.DOUBLE)));
    assertDoesNotThrow(() -> registry.resolve("bool_and", List.of(BooleanType.BOOLEAN)));
    assertDoesNotThrow(() -> registry.resolve("bool_or", List.of(BooleanType.BOOLEAN)));

    // Verify aggregate metadata has accumulatorFactory
    ResolvedFunction sumResolved = registry.resolve("sum", List.of(BigintType.BIGINT));
    FunctionMetadata sumMeta = registry.getMetadata(sumResolved);
    assertNotNull(sumMeta.getAggregateAccumulatorFactory());
  }
}
