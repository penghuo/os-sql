/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.stage;

import java.util.stream.Collectors;
import lombok.Data;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalWrite;

@Data
public class StagePlan {
  private final StageId stageId;
  private final LogicalPlan plan;
  private final StagePlan child;

  public ExecutionEngine.Schema getOutputSchema() {
    LogicalProject project = plan.accept(new ProjectVisitor(), null);
    return project == null? null : new ExecutionEngine.Schema(project.getProjectList().stream()
        .map(expr -> new ExecutionEngine.Schema.Column(expr.getName(),
            expr.getAlias(), expr.type())).collect(Collectors.toList()));
  }


  private static class ProjectVisitor extends LogicalPlanNodeVisitor<LogicalProject, Void> {
    @Override
    public LogicalProject visitProject(LogicalProject plan, Void context) {
      return plan;
    }
  }
}
