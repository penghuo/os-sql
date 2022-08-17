/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.stage;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Data;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalStageState;

@Data
public class StagePlan {
  private final StageId stageId;
  private final LogicalPlan plan;
  private final StagePlan child;

  public ExecutionEngine.Schema getOutputSchema() {
    AtomicBoolean hasStageState = new AtomicBoolean(false);
    plan.accept(new StageStateVisitor(),hasStageState);
    return hasStageState.get() ? new ExecutionEngine.Schema(Arrays.asList(
        new ExecutionEngine.Schema.Column("status", "status", ExprCoreType.INTEGER),
        new ExecutionEngine.Schema.Column("size", "size", ExprCoreType.INTEGER))) : null;
  }

  private static class StageStateVisitor extends LogicalPlanNodeVisitor<Void, AtomicBoolean> {
    @Override
    public Void visitNode(LogicalPlan plan, AtomicBoolean context) {
      plan.getChild().forEach(child -> child.accept(this, context));
      return null;
    }

    @Override
    public Void visitStageState(LogicalStageState plan, AtomicBoolean context) {
      context.set(true);
      return null;
    }
  }

//  public ExecutionEngine.Schema getOutputSchema() {
//    LogicalProject project = plan.accept(new ProjectVisitor(), null);
//    return project == null? null : new ExecutionEngine.Schema(project.getProjectList().stream()
//        .map(expr -> new ExecutionEngine.Schema.Column(expr.getName(),
//            expr.getAlias(), expr.type())).collect(Collectors.toList()));
//  }
//
//  private static class ProjectVisitor extends LogicalPlanNodeVisitor<LogicalProject, Void> {
//    @Override
//    public LogicalProject visitProject(LogicalProject plan, Void context) {
//      return plan;
//    }
//  }
}
