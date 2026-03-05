/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.sort;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.types.DqeTypes;

@DisplayName("OperatorSelectionRule")
class OperatorSelectionRuleTest {

  private OperatorSelectionRule rule;
  private List<SortSpecification> sortSpecs;

  @BeforeEach
  void setUp() {
    rule = new OperatorSelectionRule();
    DqeColumnHandle col = new DqeColumnHandle("age", "age", DqeTypes.INTEGER, true, null, false);
    sortSpecs =
        List.of(
            new SortSpecification(
                col,
                DqeTypes.INTEGER,
                SortSpecification.SortDirection.ASC,
                SortSpecification.NullOrdering.NULLS_LAST));
  }

  @Test
  @DisplayName("No ORDER BY, no LIMIT -> SCAN_ONLY")
  void scanOnly() {
    PipelineDecision decision = rule.decide(List.of(), OptionalLong.empty(), OptionalLong.empty());
    assertEquals(OperatorSelectionRule.PipelineStrategy.SCAN_ONLY, decision.getStrategy());
    assertTrue(decision.getEffectiveTopN().isEmpty());
  }

  @Test
  @DisplayName("LIMIT only -> LIMIT_ONLY")
  void limitOnly() {
    PipelineDecision decision = rule.decide(List.of(), OptionalLong.of(10), OptionalLong.empty());
    assertEquals(OperatorSelectionRule.PipelineStrategy.LIMIT_ONLY, decision.getStrategy());
    assertTrue(decision.getEffectiveTopN().isEmpty());
  }

  @Test
  @DisplayName("ORDER BY without LIMIT -> FULL_SORT")
  void fullSort() {
    PipelineDecision decision = rule.decide(sortSpecs, OptionalLong.empty(), OptionalLong.empty());
    assertEquals(OperatorSelectionRule.PipelineStrategy.FULL_SORT, decision.getStrategy());
    assertTrue(decision.getEffectiveTopN().isEmpty());
  }

  @Test
  @DisplayName("ORDER BY + LIMIT -> TOP_N")
  void topN() {
    PipelineDecision decision = rule.decide(sortSpecs, OptionalLong.of(10), OptionalLong.empty());
    assertEquals(OperatorSelectionRule.PipelineStrategy.TOP_N, decision.getStrategy());
    assertEquals(10L, decision.getEffectiveTopN().getAsLong());
  }

  @Test
  @DisplayName("ORDER BY + LIMIT + OFFSET -> TOP_N_WITH_OFFSET")
  void topNWithOffset() {
    PipelineDecision decision = rule.decide(sortSpecs, OptionalLong.of(10), OptionalLong.of(20));
    assertEquals(OperatorSelectionRule.PipelineStrategy.TOP_N_WITH_OFFSET, decision.getStrategy());
    assertEquals(30L, decision.getEffectiveTopN().getAsLong());
  }

  @Test
  @DisplayName("OFFSET of 0 treated as no offset")
  void offsetZero() {
    PipelineDecision decision = rule.decide(sortSpecs, OptionalLong.of(10), OptionalLong.of(0));
    assertEquals(OperatorSelectionRule.PipelineStrategy.TOP_N, decision.getStrategy());
  }

  @Test
  @DisplayName("Null sort specs treated as empty")
  void nullSortSpecs() {
    PipelineDecision decision = rule.decide(null, OptionalLong.empty(), OptionalLong.empty());
    assertEquals(OperatorSelectionRule.PipelineStrategy.SCAN_ONLY, decision.getStrategy());
  }
}
