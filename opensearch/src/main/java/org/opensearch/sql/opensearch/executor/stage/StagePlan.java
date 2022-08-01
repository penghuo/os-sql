/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.stage;

import lombok.Data;
import org.opensearch.sql.planner.logical.LogicalPlan;

@Data
public class StagePlan {
  private final StageId stageId;
  private final LogicalPlan plan;
}
