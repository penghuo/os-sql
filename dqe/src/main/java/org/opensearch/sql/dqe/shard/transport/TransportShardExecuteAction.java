/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action that executes a DQE plan fragment on a shard. The coordinator sends a serialized
 * plan fragment to this action, which deserializes it, builds an operator pipeline via {@link
 * LocalExecutionPlanner}, drains all pages, and returns the result as serialized Trino Pages.
 */
public class TransportShardExecuteAction
    extends HandledTransportAction<ActionRequest, ShardExecuteResponse> {

  private final Function<TableScanNode, Operator> scanFactory;
  private final Map<String, Type> columnTypeMap;

  /**
   * Constructor for plugin wiring with dependency injection.
   *
   * @param transportService the transport service
   * @param actionFilters action filters
   * @param scanFactory factory that creates a leaf Operator for a given TableScanNode
   * @param columnTypeMap mapping from column name to Trino Type
   */
  @Inject
  public TransportShardExecuteAction(
      TransportService transportService,
      ActionFilters actionFilters,
      Function<TableScanNode, Operator> scanFactory,
      Map<String, Type> columnTypeMap) {
    super(ShardExecuteAction.NAME, transportService, actionFilters, ShardExecuteRequest::new);
    this.scanFactory = scanFactory;
    this.columnTypeMap = columnTypeMap;
  }

  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<ShardExecuteResponse> listener) {
    ShardExecuteRequest req = ShardExecuteRequest.fromActionRequest(request);
    try {
      // 1. Deserialize plan fragment
      DqePlanNode plan =
          DqePlanNode.readPlanNode(
              new InputStreamStreamInput(new ByteArrayInputStream(req.getSerializedFragment())));

      // 2. Build operator pipeline
      LocalExecutionPlanner planner = new LocalExecutionPlanner(scanFactory, columnTypeMap);
      Operator pipeline = plan.accept(planner, null);

      // 3. Execute: drain pages
      List<Page> pages = new ArrayList<>();
      Page page;
      while ((page = pipeline.processNextBatch()) != null) {
        pages.add(page);
      }
      pipeline.close();

      // 4. Resolve column types from the plan
      List<Type> columnTypes = resolveColumnTypes(plan);

      // 5. Return Page-based response
      listener.onResponse(new ShardExecuteResponse(pages, columnTypes));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Resolve column types from the plan node by walking the tree to determine output column names
   * and mapping them to types.
   */
  private List<Type> resolveColumnTypes(DqePlanNode node) {
    List<String> columnNames = resolveColumnNames(node);
    List<Type> types = new ArrayList<>();
    for (String col : columnNames) {
      types.add(columnTypeMap.getOrDefault(col, BigintType.BIGINT));
    }
    return types;
  }

  /**
   * Resolve column names from the root plan node. Walks down through unary nodes to find the
   * effective output column list.
   */
  static List<String> resolveColumnNames(DqePlanNode node) {
    if (node instanceof TableScanNode) {
      return ((TableScanNode) node).getColumns();
    }
    if (node instanceof ProjectNode) {
      return ((ProjectNode) node).getOutputColumns();
    }
    if (node instanceof AggregationNode) {
      AggregationNode agg = (AggregationNode) node;
      List<String> names = new ArrayList<>(agg.getGroupByKeys());
      names.addAll(agg.getAggregateFunctions());
      return names;
    }
    // For filter, limit, sort: delegate to child
    List<DqePlanNode> children = node.getChildren();
    if (!children.isEmpty()) {
      return resolveColumnNames(children.get(0));
    }
    return List.of();
  }
}
