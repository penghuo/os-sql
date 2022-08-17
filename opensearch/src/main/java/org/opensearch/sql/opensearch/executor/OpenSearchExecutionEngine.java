/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.executor;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.executor.scheduler.ExecutionSchedule;
import org.opensearch.sql.opensearch.executor.stage.StageExecution;
import org.opensearch.sql.opensearch.executor.stage.StageOutput;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalStageState;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.planner.splits.SplitManager;
import org.opensearch.sql.planner.stage.StagePlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.stage.StageStateTable;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch execution engine implementation. */
@RequiredArgsConstructor
public class OpenSearchExecutionEngine implements ExecutionEngine {

  private final OpenSearchClient client;

  private final ExecutionProtector executionProtector;

  @Override
  public void execute(PhysicalPlan physicalPlan, ResponseListener<QueryResponse> listener) {
    PhysicalPlan plan = executionProtector.protect(physicalPlan);
    client.schedule(
        () -> {
          try {
            List<ExprValue> result = new ArrayList<>();
            plan.open();

            while (plan.hasNext()) {
              result.add(plan.next());
            }

            QueryResponse response = new QueryResponse(physicalPlan.schema(), result);
            listener.onResponse(response);
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  public void newExecute(StagePlan stagePlan, ResponseListener<QueryResponse> listener) {
    List<StageExecution> stageExecutions = new ArrayList<>();
    StagePlan tmp = stagePlan;
    while (tmp != null) {
      stageExecutions.add(buildStageExecution(tmp));
      tmp = stagePlan.getChild();
    }

    // todo, we assume the first stage is the final stage
    stageExecutions.get(0).addOutputListener(listener);

    // schedule the execution
    ExecutionSchedule executionSchedule = new ExecutionSchedule(stageExecutions);
    executionSchedule.execute();
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    client.schedule(() -> {
      try {
        Explain openSearchExplain = new Explain() {
          @Override
          public ExplainResponseNode visitTableScan(TableScanOperator node, Object context) {
            return explain(node, context, explainNode -> {
              explainNode.setDescription(ImmutableMap.of("request", node.explain()));
            });
          }
        };

        listener.onResponse(openSearchExplain.apply(plan));
      } catch (Exception e) {
        listener.onFailure(e);
      }
    });
  }

  private StageExecution buildStageExecution(StagePlan stagePlan) {
    SplitManagerContext ctx = new SplitManagerContext();
    stagePlan.getPlan().accept(new SplitManagerBuilder(), ctx);
    final Schema outputSchema = stagePlan.getOutputSchema();
    return new StageExecution(
        stagePlan.getStageId(),
        stagePlan.getPlan(),
        ctx.getWrite() != null ? ctx.getWrite() : ctx.getRead(),
        client.getNodeClient(),
        outputSchema == null ? null : new StageOutput(outputSchema));
  }

  private static class SplitManagerBuilder extends LogicalPlanNodeVisitor<Void,
      SplitManagerContext> {
    @Override
    public Void visitNode(LogicalPlan plan, SplitManagerContext context) {
      plan.getChild().forEach(child -> child.accept(this, context));
      return null;
    }

    @Override
    public Void visitWrite(LogicalWrite plan, SplitManagerContext context) {
      context.setWrite(plan.getTable().getSplitManager());
      return null;
    }

    @Override
    public Void visitRelation(LogicalRelation plan, SplitManagerContext context) {
      context.setRead(plan.getTable().getSplitManager());
      return null;
    }

    @Override
    public Void visitStageState(LogicalStageState plan, SplitManagerContext context) {
      context.setRead(StageStateTable.INSTANCE.getSplitManager());
      return null;
    }
  }

  @Data
  static class SplitManagerContext {
    SplitManager read;
    SplitManager write;
  }
}
