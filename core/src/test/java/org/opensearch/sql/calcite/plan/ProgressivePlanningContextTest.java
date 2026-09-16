/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ProgressivePlanningContextTest {

  @Test
  void restoresNestedPlanningScopes() {
    assertFalse(ProgressivePlanningContext.isActive());

    try (ProgressivePlanningContext.Scope outer = ProgressivePlanningContext.open()) {
      assertTrue(ProgressivePlanningContext.isActive());
      try (ProgressivePlanningContext.Scope inner = ProgressivePlanningContext.open()) {
        assertTrue(ProgressivePlanningContext.isActive());
      }
      assertTrue(ProgressivePlanningContext.isActive());
    }

    assertFalse(ProgressivePlanningContext.isActive());
  }
}
