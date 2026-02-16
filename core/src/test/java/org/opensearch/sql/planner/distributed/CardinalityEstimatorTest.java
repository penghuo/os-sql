/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CardinalityEstimatorTest {

  @Mock ShardSplitManager splitManager;
  @Mock RelNode relNode;
  @Mock RelMetadataQuery mq;
  @Mock org.apache.calcite.plan.RelOptCluster cluster;

  CardinalityEstimator estimator;

  @BeforeEach
  void setUp() {
    estimator = new CardinalityEstimator(splitManager);
  }

  @Test
  @DisplayName("estimateRowCount should return Calcite metadata row count when available")
  void estimateRowCountFromMetadata() {
    when(relNode.getCluster()).thenReturn(cluster);
    when(cluster.getMetadataQuery()).thenReturn(mq);
    when(relNode.estimateRowCount(mq)).thenReturn(5000.0);

    assertEquals(5000L, estimator.estimateRowCount(relNode));
  }

  @Test
  @DisplayName("estimateRowCount should return -1 when metadata returns NaN")
  void estimateRowCountReturnsNegativeForNaN() {
    when(relNode.getCluster()).thenReturn(cluster);
    when(cluster.getMetadataQuery()).thenReturn(mq);
    when(relNode.estimateRowCount(mq)).thenReturn(Double.NaN);

    assertEquals(-1L, estimator.estimateRowCount(relNode));
  }

  @Test
  @DisplayName("estimateRowCount should return -1 when metadata throws exception")
  void estimateRowCountReturnsNegativeOnException() {
    when(relNode.getCluster()).thenThrow(new RuntimeException("no metadata"));

    assertEquals(-1L, estimator.estimateRowCount(relNode));
  }

  @Test
  @DisplayName("getIndexDocCount should delegate to ShardSplitManager")
  void getIndexDocCountDelegatesToSplitManager() {
    when(splitManager.getDocCount("my_index")).thenReturn(100_000L);

    assertEquals(100_000L, estimator.getIndexDocCount("my_index"));
  }

  @Test
  @DisplayName("estimateRowCount should return -1 for infinite row count")
  void estimateRowCountReturnsNegativeForInfinite() {
    when(relNode.getCluster()).thenReturn(cluster);
    when(cluster.getMetadataQuery()).thenReturn(mq);
    when(relNode.estimateRowCount(mq)).thenReturn(Double.POSITIVE_INFINITY);

    assertEquals(-1L, estimator.estimateRowCount(relNode));
  }

  @Test
  @DisplayName("estimateRowCount should handle zero row count")
  void estimateRowCountHandlesZero() {
    when(relNode.getCluster()).thenReturn(cluster);
    when(cluster.getMetadataQuery()).thenReturn(mq);
    when(relNode.estimateRowCount(mq)).thenReturn(0.0);

    assertEquals(0L, estimator.estimateRowCount(relNode));
  }
}
