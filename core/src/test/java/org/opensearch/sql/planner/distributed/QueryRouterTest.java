/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QueryRouterTest {

  @Mock FragmentPlanner fragmentPlanner;
  @Mock CardinalityEstimator cardinalityEstimator;
  @Mock RelNode relNode;

  @Test
  @DisplayName("shouldDistribute returns false when distributed is disabled")
  void disabledReturnsFalse() {
    QueryRouter router = new QueryRouter(false, fragmentPlanner, cardinalityEstimator);
    assertFalse(router.shouldDistribute(relNode, "test_index"));
  }

  @Test
  @DisplayName("shouldDistribute returns false when plan does not require exchange")
  void noExchangeReturnsFalse() {
    when(fragmentPlanner.requiresExchange(relNode)).thenReturn(false);

    QueryRouter router = new QueryRouter(true, fragmentPlanner, cardinalityEstimator);
    assertFalse(router.shouldDistribute(relNode, "test_index"));
  }

  @Test
  @DisplayName("shouldDistribute returns false when doc count is below threshold")
  void belowThresholdReturnsFalse() {
    when(fragmentPlanner.requiresExchange(relNode)).thenReturn(true);
    when(cardinalityEstimator.getIndexDocCount("small_index")).thenReturn(500L);

    QueryRouter router = new QueryRouter(true, fragmentPlanner, cardinalityEstimator);
    assertFalse(router.shouldDistribute(relNode, "small_index"));
  }

  @Test
  @DisplayName("shouldDistribute returns true when all conditions are met")
  void allConditionsMetReturnsTrue() {
    when(fragmentPlanner.requiresExchange(relNode)).thenReturn(true);
    when(cardinalityEstimator.getIndexDocCount("large_index")).thenReturn(1_000_000L);

    QueryRouter router = new QueryRouter(true, fragmentPlanner, cardinalityEstimator);
    assertTrue(router.shouldDistribute(relNode, "large_index"));
  }

  @Test
  @DisplayName("shouldDistribute returns true when doc count is exactly at threshold")
  void exactThresholdReturnsTrue() {
    when(fragmentPlanner.requiresExchange(relNode)).thenReturn(true);
    when(cardinalityEstimator.getIndexDocCount("medium_index"))
        .thenReturn(QueryRouter.DEFAULT_DOC_COUNT_THRESHOLD);

    QueryRouter router = new QueryRouter(true, fragmentPlanner, cardinalityEstimator);
    assertTrue(router.shouldDistribute(relNode, "medium_index"));
  }

  @Test
  @DisplayName("shouldDistribute returns true when doc count is unknown (-1)")
  void unknownDocCountDefaultsToDistributed() {
    when(fragmentPlanner.requiresExchange(relNode)).thenReturn(true);
    when(cardinalityEstimator.getIndexDocCount("unknown_index")).thenReturn(-1L);

    QueryRouter router = new QueryRouter(true, fragmentPlanner, cardinalityEstimator);
    assertTrue(router.shouldDistribute(relNode, "unknown_index"));
  }

  @Test
  @DisplayName("custom threshold should be respected")
  void customThresholdRespected() {
    when(fragmentPlanner.requiresExchange(relNode)).thenReturn(true);
    when(cardinalityEstimator.getIndexDocCount("test_index")).thenReturn(50_000L);

    // Custom threshold of 100_000 - 50k docs should NOT distribute
    QueryRouter router = new QueryRouter(true, fragmentPlanner, cardinalityEstimator, 100_000L);
    assertFalse(router.shouldDistribute(relNode, "test_index"));

    // Custom threshold of 10_000 - 50k docs SHOULD distribute
    QueryRouter router2 = new QueryRouter(true, fragmentPlanner, cardinalityEstimator, 10_000L);
    assertTrue(router2.shouldDistribute(relNode, "test_index"));
  }
}
