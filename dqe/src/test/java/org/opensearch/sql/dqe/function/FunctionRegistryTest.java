/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("FunctionRegistry resolution and coercion")
class FunctionRegistryTest {

  private FunctionRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new FunctionRegistry();
    registry.register(
        FunctionMetadata.builder()
            .name("upper")
            .argumentTypes(List.of(VarcharType.VARCHAR))
            .returnType(VarcharType.VARCHAR)
            .kind(FunctionKind.SCALAR)
            .build());
    registry.register(
        FunctionMetadata.builder()
            .name("abs")
            .argumentTypes(List.of(BigintType.BIGINT))
            .returnType(BigintType.BIGINT)
            .kind(FunctionKind.SCALAR)
            .build());
    registry.register(
        FunctionMetadata.builder()
            .name("abs")
            .argumentTypes(List.of(DoubleType.DOUBLE))
            .returnType(DoubleType.DOUBLE)
            .kind(FunctionKind.SCALAR)
            .build());
  }

  @Test
  @DisplayName("Exact match resolution")
  void exactMatchResolution() {
    ResolvedFunction resolved = registry.resolve("upper", List.of(VarcharType.VARCHAR));
    assertEquals("upper", resolved.getName());
    assertEquals(VarcharType.VARCHAR, resolved.getReturnType());
  }

  @Test
  @DisplayName("Case-insensitive lookup")
  void caseInsensitiveLookup() {
    ResolvedFunction resolved = registry.resolve("UPPER", List.of(VarcharType.VARCHAR));
    assertEquals("upper", resolved.getName());
  }

  @Test
  @DisplayName("Implicit coercion: INTEGER arg matches BIGINT parameter")
  void integerCoercesToBigint() {
    ResolvedFunction resolved = registry.resolve("abs", List.of(IntegerType.INTEGER));
    assertEquals("abs", resolved.getName());
    assertEquals(BigintType.BIGINT, resolved.getReturnType());
  }

  @Test
  @DisplayName("Implicit coercion: BIGINT arg matches DOUBLE parameter")
  void bigintCoercesToDouble() {
    FunctionRegistry reg = new FunctionRegistry();
    reg.register(
        FunctionMetadata.builder()
            .name("sqrt")
            .argumentTypes(List.of(DoubleType.DOUBLE))
            .returnType(DoubleType.DOUBLE)
            .kind(FunctionKind.SCALAR)
            .build());

    ResolvedFunction resolved = reg.resolve("sqrt", List.of(BigintType.BIGINT));
    assertEquals("sqrt", resolved.getName());
    assertEquals(DoubleType.DOUBLE, resolved.getReturnType());
  }

  @Test
  @DisplayName("Prefers exact match over coercion")
  void prefersExactMatch() {
    ResolvedFunction resolved = registry.resolve("abs", List.of(BigintType.BIGINT));
    assertEquals(BigintType.BIGINT, resolved.getReturnType());

    ResolvedFunction resolvedDouble = registry.resolve("abs", List.of(DoubleType.DOUBLE));
    assertEquals(DoubleType.DOUBLE, resolvedDouble.getReturnType());
  }

  @Test
  @DisplayName("Throws for unknown function name")
  void throwsForUnknownFunction() {
    assertThrows(IllegalArgumentException.class, () -> registry.resolve("unknown", List.of()));
  }

  @Test
  @DisplayName("Throws when no overload matches argument types")
  void throwsWhenNoOverloadMatches() {
    assertThrows(
        IllegalArgumentException.class,
        () -> registry.resolve("upper", List.of(BigintType.BIGINT)));
  }

  @Test
  @DisplayName("Resolves zero-arg function")
  void zeroArgFunction() {
    registry.register(
        FunctionMetadata.builder()
            .name("pi")
            .argumentTypes(List.of())
            .returnType(DoubleType.DOUBLE)
            .kind(FunctionKind.SCALAR)
            .build());
    ResolvedFunction resolved = registry.resolve("pi", List.of());
    assertEquals("pi", resolved.getName());
  }

  @Test
  @DisplayName("getMetadata returns full metadata for resolved function")
  void getMetadataReturnsFullInfo() {
    ResolvedFunction resolved = registry.resolve("upper", List.of(VarcharType.VARCHAR));
    FunctionMetadata metadata = registry.getMetadata(resolved);
    assertEquals("upper", metadata.getName());
    assertEquals(VarcharType.VARCHAR, metadata.getReturnType());
  }
}
