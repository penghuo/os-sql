/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.planner.stage.StageId;

@ToString
@Getter
@EqualsAndHashCode(callSuper = true)
public class LogicalStageState extends LogicalPlan {
  @Getter
  private final StageId stageId;

  /**
   * Constructor of LogicalRelation.
   */
  public LogicalStageState(StageId stageId) {
    super(ImmutableList.of());
    this.stageId = stageId;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitStageState(this, context);
  }
}
