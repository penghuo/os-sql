/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class TypeCoercionTest extends CalcitePPLAbstractTest {
  public TypeCoercionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testTypeCoercion() {
    String ppl = "source=EMP | eval r=abs(ENAME)";
    RelNode root = getRelNode(ppl);
  }
}
