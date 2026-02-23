/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.sort;

import java.util.List;
import java.util.OptionalLong;

/**
 * Determines the execution operator pipeline based on the query pattern. Implements the operator
 * selection table from A-7:
 *
 * <table>
 * <tr><td>ORDER BY without LIMIT</td><td>Scan-Filter-Project</td><td>Gather-SortOperator</td></tr>
 * <tr><td>ORDER BY col LIMIT N</td><td>...TopN(N)</td><td>Gather-TopN(N)</td></tr>
 * <tr><td>ORDER BY col LIMIT N OFFSET M</td><td>...TopN(N+M)</td><td>Gather-TopN(N+M)-Limit(skip M, take N)</td></tr>
 * <tr><td>LIMIT N without ORDER BY</td><td>...Limit(N)</td><td>Gather-Limit(N)</td></tr>
 * </table>
 */
public class OperatorSelectionRule {

  /** Pipeline strategies for different query patterns. */
  public enum PipelineStrategy {
    /** No ORDER BY, no LIMIT: plain scan. */
    SCAN_ONLY,
    /** ORDER BY without LIMIT: full sort at coordinator. */
    FULL_SORT,
    /** ORDER BY + LIMIT N: TopN at shard and coordinator. */
    TOP_N,
    /** ORDER BY + LIMIT N OFFSET M: TopN(N+M) + final Limit(skip M, take N). */
    TOP_N_WITH_OFFSET,
    /** LIMIT N without ORDER BY: Limit at shard and coordinator. */
    LIMIT_ONLY
  }

  public OperatorSelectionRule() {}

  /**
   * Determines the pipeline strategy based on sort and limit specifications.
   *
   * @param sortSpecs the resolved sort specifications (empty if no ORDER BY)
   * @param limit the LIMIT value (empty if no LIMIT)
   * @param offset the OFFSET value (empty if no OFFSET)
   * @return the pipeline decision
   */
  public PipelineDecision decide(
      List<SortSpecification> sortSpecs, OptionalLong limit, OptionalLong offset) {
    boolean hasOrderBy = sortSpecs != null && !sortSpecs.isEmpty();
    boolean hasLimit = limit.isPresent();
    boolean hasOffset = offset.isPresent() && offset.getAsLong() > 0;

    if (!hasOrderBy && !hasLimit) {
      return new PipelineDecision(
          PipelineStrategy.SCAN_ONLY, sortSpecs, limit, offset, OptionalLong.empty());
    }

    if (!hasOrderBy && hasLimit) {
      return new PipelineDecision(
          PipelineStrategy.LIMIT_ONLY, sortSpecs, limit, offset, OptionalLong.empty());
    }

    if (hasOrderBy && !hasLimit) {
      return new PipelineDecision(
          PipelineStrategy.FULL_SORT, sortSpecs, limit, offset, OptionalLong.empty());
    }

    // hasOrderBy && hasLimit
    if (hasOffset) {
      long n = limit.getAsLong();
      long m = offset.getAsLong();
      long effectiveN = n + m;
      return new PipelineDecision(
          PipelineStrategy.TOP_N_WITH_OFFSET,
          sortSpecs,
          limit,
          offset,
          OptionalLong.of(effectiveN));
    }

    return new PipelineDecision(
        PipelineStrategy.TOP_N, sortSpecs, limit, offset, OptionalLong.of(limit.getAsLong()));
  }
}
