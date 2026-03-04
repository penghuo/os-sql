/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.common.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@DisplayName("OpenSearch → Trino type mapping")
class TypeMappingTest {

  @ParameterizedTest
  @CsvSource({
    "keyword, VARCHAR",
    "text, VARCHAR",
    "long, BIGINT",
    "integer, INTEGER",
    "short, SMALLINT",
    "byte, TINYINT",
    "double, DOUBLE",
    "float, REAL",
    "boolean, BOOLEAN",
    "date, TIMESTAMP(3)",
    "ip, VARCHAR",
    "binary, VARBINARY"
  })
  @DisplayName("Map scalar OpenSearch types to Trino types")
  void mapScalarTypes(String osType, String expectedTrinoName) {
    var trinoType = TypeMapping.toTrinoType(osType);
    assertEquals(expectedTrinoName, trinoType.getDisplayName().toUpperCase());
  }

  @Test
  @DisplayName("Unknown type throws IllegalArgumentException")
  void unknownTypeThrows() {
    assertThrows(IllegalArgumentException.class, () -> TypeMapping.toTrinoType("unsupported_xyz"));
  }
}
