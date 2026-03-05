/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("ResolvedFunction serialization and equality")
class ResolvedFunctionTest {

  @Test
  @DisplayName("Round-trip serialization preserves all fields")
  void roundTripSerialization() throws IOException {
    ResolvedFunction original =
        new ResolvedFunction(
            "upper", List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, FunctionKind.SCALAR);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    ResolvedFunction deserialized = new ResolvedFunction(in);

    assertEquals(original, deserialized);
    assertEquals("upper", deserialized.getName());
    assertEquals(List.of(VarcharType.VARCHAR), deserialized.getArgumentTypes());
    assertEquals(VarcharType.VARCHAR, deserialized.getReturnType());
    assertEquals(FunctionKind.SCALAR, deserialized.getKind());
  }

  @Test
  @DisplayName("Round-trip with multiple argument types")
  void roundTripMultipleArgs() throws IOException {
    ResolvedFunction original =
        new ResolvedFunction(
            "concat",
            List.of(VarcharType.VARCHAR, VarcharType.VARCHAR),
            VarcharType.VARCHAR,
            FunctionKind.SCALAR);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    ResolvedFunction deserialized = new ResolvedFunction(in);

    assertEquals(original, deserialized);
  }

  @Test
  @DisplayName("Round-trip for aggregate function")
  void roundTripAggregate() throws IOException {
    ResolvedFunction original =
        new ResolvedFunction(
            "sum", List.of(BigintType.BIGINT), BigintType.BIGINT, FunctionKind.AGGREGATE);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    ResolvedFunction deserialized = new ResolvedFunction(in);

    assertEquals(original, deserialized);
    assertEquals(FunctionKind.AGGREGATE, deserialized.getKind());
  }

  @Test
  @DisplayName("Equality is based on name, argumentTypes, and kind")
  void equalityContract() {
    ResolvedFunction f1 =
        new ResolvedFunction(
            "abs", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, FunctionKind.SCALAR);
    ResolvedFunction f2 =
        new ResolvedFunction(
            "abs", List.of(DoubleType.DOUBLE), DoubleType.DOUBLE, FunctionKind.SCALAR);
    ResolvedFunction f3 =
        new ResolvedFunction(
            "abs", List.of(BigintType.BIGINT), BigintType.BIGINT, FunctionKind.SCALAR);

    assertEquals(f1, f2);
    assertEquals(f1.hashCode(), f2.hashCode());
    assertNotEquals(f1, f3);
  }
}
