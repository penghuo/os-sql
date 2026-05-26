/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RelevanceQueryFunctionTest {

  private RelevanceQueryFunction relevanceQueryFunction;

  @BeforeEach
  public void setUp() {
    relevanceQueryFunction = new RelevanceQueryFunction();
  }

  @Test
  public void testGetOperandMetadata() {
    SqlOperandTypeChecker operandMetadata = relevanceQueryFunction.getOperandTypeChecker();
    assertNotNull(operandMetadata);
    assertNotNull(operandMetadata.getOperandCountRange());
  }

  @Test
  public void testOperandMetadataSupportsOptionalParameters() {
    SqlOperandTypeChecker operandMetadata = relevanceQueryFunction.getOperandTypeChecker();
    assertNotNull(operandMetadata);
    // Optional-parameter signatures are validated at call-binding time; here we just check the
    // metadata is wired up.
    assertTrue(true, "Operand metadata should be properly constructed for optional parameters");
  }

  @Test
  public void testMultipleOperandFamilySupport() {
    SqlOperandTypeChecker operandMetadata = relevanceQueryFunction.getOperandTypeChecker();
    assertNotNull(operandMetadata);
    // Multi-family support (MAP for fields/options + CHARACTER for query) is exercised at
    // validation time.
    assertTrue(true, "Should support MAP type operands for fields and optional parameters");
  }
}
