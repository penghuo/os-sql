/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dqe.analyzer.AnalyzedQuery;
import org.opensearch.dqe.analyzer.sort.OperatorSelectionRule.PipelineStrategy;
import org.opensearch.dqe.analyzer.sort.PipelineDecision;
import org.opensearch.dqe.analyzer.sort.SortSpecification;
import org.opensearch.dqe.exchange.gather.ExchangeSourceOperator;
import org.opensearch.dqe.execution.driver.Pipeline;
import org.opensearch.dqe.execution.operator.LimitOperator;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.execution.operator.SortOperator;
import org.opensearch.dqe.execution.operator.TopNOperator;
import org.opensearch.dqe.memory.QueryMemoryBudget;

@ExtendWith(MockitoExtension.class)
class CoordinatorPipelineBuilderTests {

  @Mock private ExchangeSourceOperator source;
  @Mock private AnalyzedQuery analyzed;
  @Mock private QueryMemoryBudget budget;

  @BeforeEach
  void setUp() {
    OperatorContext ctx = new OperatorContext("q1", 0, 0, 0, "ExchangeSource", budget);
    when(source.getOperatorContext()).thenReturn(ctx);
    when(analyzed.getOutputColumnNames()).thenReturn(List.of("name", "age"));
  }

  @Test
  @DisplayName("SCAN_ONLY produces single-operator pipeline")
  void scanOnlyProducesSingleOperator() {
    PipelineDecision decision =
        new PipelineDecision(
            PipelineStrategy.SCAN_ONLY,
            List.of(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty());
    when(analyzed.getPipelineDecision()).thenReturn(decision);

    Pipeline pipeline = CoordinatorPipelineBuilder.build(source, analyzed, budget);
    assertEquals(1, pipeline.getOperators().size());
    assertTrue(pipeline.getOperators().get(0) instanceof ExchangeSourceOperator);
  }

  @Test
  @DisplayName("LIMIT_ONLY adds LimitOperator")
  void limitOnlyAddsLimitOperator() {
    PipelineDecision decision =
        new PipelineDecision(
            PipelineStrategy.LIMIT_ONLY,
            List.of(),
            OptionalLong.of(10),
            OptionalLong.of(0),
            OptionalLong.empty());
    when(analyzed.getPipelineDecision()).thenReturn(decision);

    Pipeline pipeline = CoordinatorPipelineBuilder.build(source, analyzed, budget);
    assertEquals(2, pipeline.getOperators().size());
    assertTrue(pipeline.getOperators().get(1) instanceof LimitOperator);
  }

  @Test
  @DisplayName("FULL_SORT adds SortOperator")
  void fullSortAddsSortOperator() {
    SortSpecification sortSpec = mock(SortSpecification.class);
    PipelineDecision decision =
        new PipelineDecision(
            PipelineStrategy.FULL_SORT,
            List.of(sortSpec),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty());
    when(analyzed.getPipelineDecision()).thenReturn(decision);

    Pipeline pipeline = CoordinatorPipelineBuilder.build(source, analyzed, budget);
    assertEquals(2, pipeline.getOperators().size());
    assertTrue(pipeline.getOperators().get(1) instanceof SortOperator);
  }

  @Test
  @DisplayName("TOP_N adds TopNOperator")
  void topNAddsTopNOperator() {
    SortSpecification sortSpec = mock(SortSpecification.class);
    PipelineDecision decision =
        new PipelineDecision(
            PipelineStrategy.TOP_N,
            List.of(sortSpec),
            OptionalLong.of(5),
            OptionalLong.empty(),
            OptionalLong.of(5));
    when(analyzed.getPipelineDecision()).thenReturn(decision);

    Pipeline pipeline = CoordinatorPipelineBuilder.build(source, analyzed, budget);
    assertEquals(2, pipeline.getOperators().size());
    assertTrue(pipeline.getOperators().get(1) instanceof TopNOperator);
  }

  @Test
  @DisplayName("TOP_N_WITH_OFFSET adds TopN + Limit operators")
  void topNWithOffsetAddsTopNAndLimit() {
    SortSpecification sortSpec = mock(SortSpecification.class);
    PipelineDecision decision =
        new PipelineDecision(
            PipelineStrategy.TOP_N_WITH_OFFSET,
            List.of(sortSpec),
            OptionalLong.of(5),
            OptionalLong.of(10),
            OptionalLong.of(15));
    when(analyzed.getPipelineDecision()).thenReturn(decision);

    Pipeline pipeline = CoordinatorPipelineBuilder.build(source, analyzed, budget);
    assertEquals(3, pipeline.getOperators().size());
    assertTrue(pipeline.getOperators().get(1) instanceof TopNOperator);
    assertTrue(pipeline.getOperators().get(2) instanceof LimitOperator);
  }
}
