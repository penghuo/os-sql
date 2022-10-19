/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.planner.logical.LogicalRemote.RemoteType.SOURCE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.executor.stage.SequenceStageScheduler;
import org.opensearch.sql.executor.stage.StageExecution;
import org.opensearch.sql.executor.task.TaskId;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.aggregation.SumAggregator;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.executor.scheduler.OpenSearchTaskScheduler;
import org.opensearch.sql.opensearch.executor.transport.dataplane.QLDataReader;
import org.opensearch.sql.opensearch.executor.transport.dataplane.QLDataService;
import org.opensearch.sql.opensearch.executor.transport.dataplane.QLDataWriter;
import org.opensearch.sql.opensearch.planner.physical.RemoteSinkOperator;
import org.opensearch.sql.opensearch.planner.physical.RemoteSourceOperator;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalOutput;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRemote;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch execution engine implementation. */
@RequiredArgsConstructor
public class OpenSearchExecutionEngine implements ExecutionEngine {

  private final OpenSearchClient client;

  private final ClusterService clusterService;

  private final ExecutionProtector executionProtector;

  /**
   * execute logical plan.
   *
   * @param plan
   * @return
   */
  @Override
  public void newExecute(LogicalPlan plan, ResponseListener<QueryResponse> listener) {
    SequenceStageScheduler stageScheduler = new SequenceStageScheduler();

    LogicalPlan output = new LogicalOutput(plan, listener::onResponse);
    stageScheduler.schedule(plan(output), listener::onFailure);
  }

  /**
   * plan logical plan to stage execution.
   *
   * @param plan
   * @return
   */
  @Override
  public List<StageExecution> plan(LogicalPlan plan) {
    List<StageExecution> stageExecutionList = new ArrayList<>();
    topDownTraverse(plan, stageExecutionList);
    if (stageExecutionList.isEmpty()) {
      stageExecutionList.add(
          new StageExecution(
              plan, new OpenSearchTaskScheduler(clusterService, client.getNodeClient())));
    }
    return stageExecutionList;
  }

  // todo. need improvement.
  private void topDownTraverse(LogicalPlan plan, List<StageExecution> stageExecutionList) {
    if (plan == null || plan.getChild().isEmpty()) {
      return;
    }
    LogicalPlan child = plan.getChild().get(0);
    if (child instanceof LogicalWrite) {
      LogicalPlan remoteSource = LogicalPlanDSL.aggregation(new LogicalRemote(
              LogicalRemote.RemoteType.SOURCE),
          ImmutableList.of(
              DSL.named("totalCount", new SumAggregator(
                  Collections.singletonList(DSL.ref("count", INTEGER)),
                  INTEGER))
          ),
          Collections.emptyList()
      );
      plan.replaceChildPlans(Collections.singletonList(remoteSource));

      stageExecutionList.add(
          new StageExecution(
              plan, new OpenSearchTaskScheduler(clusterService, client.getNodeClient())));

      LogicalPlan remoteSink = new LogicalRemote(child, LogicalRemote.RemoteType.SINK);
      stageExecutionList.add(
          new StageExecution(
              remoteSink, new OpenSearchTaskScheduler(clusterService, client.getNodeClient())));
    } else {
      topDownTraverse(child, stageExecutionList);
    }
  }

  /**
   * Generate optimal physical plan for logical plan. If no table involved,
   * translate logical plan to physical by default implementor.
   * TODO: for now just delegate entire logical plan to storage engine.
   *
   * @param plan logical plan
   * @return optimal physical plan
   */
  public PhysicalPlan implement(LogicalPlan plan, List<Object> context) {
    List<Table> tables = findTable(plan);
    Table writeTable = tables.get(1);
    Table sourceTable = tables.get(0);

    if (sourceTable == null && writeTable == null) {
      return plan.accept(
          new ExecutorImplementor(),
          new Context(
              (Set<TaskId>) context.get(0),
              (NodeClient) context.get(1),
              (QLDataService) context.get(2),
              (TaskId) context.get(3)));
    }

    if (sourceTable == null) {
      // values case, no sourceTable.
      return writeTable.implement(writeTable.optimize(plan));
    } else if (writeTable == null) {
      // DQL
      return sourceTable.implement(sourceTable.optimize(plan));
    } else {
      // DML
      return sourceTable.optimize(plan).accept(new WriteReadImplementor(writeTable, sourceTable),
          new Context((Set<TaskId>) context.get(0), (NodeClient) context.get(1),
              (QLDataService) context.get(2), (TaskId) context.get(3)));
    }
  }

  private List<Table> findTable(LogicalPlan plan) {
    // 0: sourceTable, 1: writeTable
    List<Table> ctx = new ArrayList<>(2);
    ctx.add(0, null);
    ctx.add(1, null);

    plan.accept(new LogicalPlanNodeVisitor<Void, List<Table>>() {
      @Override
      public Void visitNode(LogicalPlan plan, List<Table> context) {
        List<LogicalPlan> children = plan.getChild();
        if (children.isEmpty()) {
          return null;
        }
        return children.get(0).accept(this, context);
      }

      @Override
      public Void visitWrite(LogicalWrite plan, List<Table> context) {
        context.set(1, plan.getTable());
        return super.visitWrite(plan, context);
      }

      @Override
      public Void visitRelation(LogicalRelation plan, List<Table> context) {
        context.set(0, plan.getTable());
        return null;
      }
    }, ctx);
    return ctx;
  }

  @Data
  private static class Context {
    private final Set<TaskId> sourceTaskIds;
    private final NodeClient client;
    private final QLDataService dataService;
    private final TaskId taskId;
  }

  private static class ExecutorImplementor extends DefaultImplementor<Context> {
    @Override
    public PhysicalPlan visitRemote(LogicalRemote node, Context context) {
      if (SOURCE == node.getType()) {
        return new RemoteSourceOperator(new QLDataReader(context.sourceTaskIds, context.client));
      } else {
        return new RemoteSinkOperator(
            visitChild(node, context), new QLDataWriter(context.dataService), context.getTaskId());
      }
    }
  }


  @RequiredArgsConstructor
  private static class WriteReadImplementor extends DefaultImplementor<Context> {

    private final Table writeTable;
    private final Table sourceTable;

    @Override
    public PhysicalPlan visitRemote(LogicalRemote node, Context context) {
      if (SOURCE == node.getType()) {
        return new RemoteSourceOperator(new QLDataReader(context.sourceTaskIds, context.client));
      } else {
        return new RemoteSinkOperator(
            visitChild(node, context), new QLDataWriter(context.dataService), context.getTaskId());
      }
    }

    @Override
    public PhysicalPlan visitWrite(LogicalWrite node, Context ctx) {
      final PhysicalPlan child = node.getChild().get(0).accept(this, ctx);
      return writeTable.implement(node, child);
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation plan, Context ctx) {
      return sourceTable.implement(plan);
    }
  }

  @Override
  public QueryResponse execute(PhysicalPlan physicalPlan) {
    try (PhysicalPlan plan = executionProtector.protect(physicalPlan)) {
      List<ExprValue> result = new ArrayList<>();
      plan.open();

      while (plan.hasNext()) {
        result.add(plan.next());
      }
      return new QueryResponse(physicalPlan.schema(), result);
    }
  }

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

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            Explain openSearchExplain =
                new Explain() {
                  @Override
                  public ExplainResponseNode visitTableScan(
                      TableScanOperator node, Object context) {
                    return explain(
                        node,
                        context,
                        explainNode -> {
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
}
