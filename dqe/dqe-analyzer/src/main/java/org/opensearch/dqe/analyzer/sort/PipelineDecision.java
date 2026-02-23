/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.sort;

import java.util.List;
import java.util.OptionalLong;

/** The result of operator selection: strategy plus computed parameters. */
public class PipelineDecision {

  private final OperatorSelectionRule.PipelineStrategy strategy;
  private final List<SortSpecification> sortSpecs;
  private final OptionalLong limit;
  private final OptionalLong offset;
  private final OptionalLong effectiveTopN;

  public PipelineDecision(
      OperatorSelectionRule.PipelineStrategy strategy,
      List<SortSpecification> sortSpecs,
      OptionalLong limit,
      OptionalLong offset,
      OptionalLong effectiveTopN) {
    this.strategy = strategy;
    this.sortSpecs = sortSpecs != null ? List.copyOf(sortSpecs) : List.of();
    this.limit = limit;
    this.offset = offset;
    this.effectiveTopN = effectiveTopN;
  }

  public OperatorSelectionRule.PipelineStrategy getStrategy() {
    return strategy;
  }

  public List<SortSpecification> getSortSpecifications() {
    return sortSpecs;
  }

  public OptionalLong getLimit() {
    return limit;
  }

  public OptionalLong getOffset() {
    return offset;
  }

  /**
   * The effective N for TopN operators (N for TOP_N, N+M for TOP_N_WITH_OFFSET). Empty for
   * strategies that don't use TopN.
   */
  public OptionalLong getEffectiveTopN() {
    return effectiveTopN;
  }

  @Override
  public String toString() {
    return "PipelineDecision{" + strategy + ", limit=" + limit + ", offset=" + offset + "}";
  }
}
