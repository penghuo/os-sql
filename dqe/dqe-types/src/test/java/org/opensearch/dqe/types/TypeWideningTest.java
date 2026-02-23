/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("TypeWidening")
class TypeWideningTest {

  @Test
  @DisplayName("Empty set returns empty")
  void emptySet() {
    assertTrue(TypeWidening.widen("field", Set.of()).isEmpty());
  }

  @Test
  @DisplayName("Single type returns itself")
  void singleType() {
    assertEquals(
        Optional.of(DqeTypes.VARCHAR), TypeWidening.widen("field", Set.of(DqeTypes.VARCHAR)));
  }

  @Test
  @DisplayName("INTEGER + BIGINT -> BIGINT")
  void integerBigint() {
    assertEquals(
        Optional.of(DqeTypes.BIGINT),
        TypeWidening.widen("field", Set.of(DqeTypes.INTEGER, DqeTypes.BIGINT)));
  }

  @Test
  @DisplayName("TINYINT + SMALLINT + INTEGER -> INTEGER")
  void threeIntegerTypes() {
    assertEquals(
        Optional.of(DqeTypes.INTEGER),
        TypeWidening.widen("field", Set.of(DqeTypes.TINYINT, DqeTypes.SMALLINT, DqeTypes.INTEGER)));
  }

  @Test
  @DisplayName("REAL + DOUBLE -> DOUBLE")
  void realDouble() {
    assertEquals(
        Optional.of(DqeTypes.DOUBLE),
        TypeWidening.widen("field", Set.of(DqeTypes.REAL, DqeTypes.DOUBLE)));
  }

  @Test
  @DisplayName("INTEGER + DOUBLE -> DOUBLE")
  void integerDouble() {
    assertEquals(
        Optional.of(DqeTypes.DOUBLE),
        TypeWidening.widen("field", Set.of(DqeTypes.INTEGER, DqeTypes.DOUBLE)));
  }

  @Test
  @DisplayName("TIMESTAMP(3) + TIMESTAMP(9) -> TIMESTAMP(9)")
  void timestampWidening() {
    assertEquals(
        Optional.of(DqeTypes.TIMESTAMP_NANOS),
        TypeWidening.widen("field", Set.of(DqeTypes.TIMESTAMP_MILLIS, DqeTypes.TIMESTAMP_NANOS)));
  }

  @Test
  @DisplayName("VARCHAR + BIGINT -> empty (incompatible)")
  void incompatibleTypes() {
    assertTrue(TypeWidening.widen("field", Set.of(DqeTypes.VARCHAR, DqeTypes.BIGINT)).isEmpty());
  }

  @Test
  @DisplayName("BOOLEAN + INTEGER -> empty (incompatible)")
  void booleanInteger() {
    assertTrue(TypeWidening.widen("field", Set.of(DqeTypes.BOOLEAN, DqeTypes.INTEGER)).isEmpty());
  }

  @Test
  @DisplayName("Two identical DECIMAL types widen to same")
  void sameDecimal() {
    DqeType dec1 = DqeTypes.decimal(10, 2);
    DqeType dec2 = DqeTypes.decimal(10, 2);
    // Use HashSet to allow logically-equal but distinct instances
    Set<DqeType> types = new java.util.HashSet<>();
    types.add(dec1);
    types.add(dec2);
    assertEquals(Optional.of(dec1), TypeWidening.widen("field", types));
  }

  @Test
  @DisplayName("DECIMAL(10,2) + DECIMAL(15,5) -> DECIMAL(13,5)")
  void decimalWidening() {
    DqeType dec1 = DqeTypes.decimal(10, 2);
    DqeType dec2 = DqeTypes.decimal(15, 5);
    Optional<DqeType> result = TypeWidening.widen("field", Set.of(dec1, dec2));
    assertTrue(result.isPresent());
    // scale = max(2, 5) = 5
    assertEquals(5, result.get().getScale());
    // integer part = max(10-2, 15-5) = max(8, 10) = 10; precision = 10 + 5 = 15
    assertEquals(15, result.get().getPrecision());
  }

  @Test
  @DisplayName("TINYINT + INTEGER + BIGINT -> BIGINT (3+ type widening)")
  void threeIntegerTypesWidening() {
    Set<DqeType> types = Set.of(DqeTypes.TINYINT, DqeTypes.INTEGER, DqeTypes.BIGINT);
    assertEquals(Optional.of(DqeTypes.BIGINT), TypeWidening.widen("field", types));
  }

  @Test
  @DisplayName("TINYINT + SMALLINT + INTEGER + BIGINT -> BIGINT (4 type widening)")
  void fourIntegerTypesWidening() {
    Set<DqeType> types =
        Set.of(DqeTypes.TINYINT, DqeTypes.SMALLINT, DqeTypes.INTEGER, DqeTypes.BIGINT);
    assertEquals(Optional.of(DqeTypes.BIGINT), TypeWidening.widen("field", types));
  }
}
