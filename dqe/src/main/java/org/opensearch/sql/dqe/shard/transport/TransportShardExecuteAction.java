/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action that executes a DQE plan fragment on a shard. The coordinator sends a serialized
 * plan fragment to this action, which deserializes it, builds an operator pipeline via {@link
 * LocalExecutionPlanner}, drains all pages, and returns the result as JSON.
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

      // 3. Execute: drain pages and convert to row maps
      List<String> columnNames = resolveColumnNames(plan);
      List<Map<String, Object>> rows = new ArrayList<>();
      Page page;
      while ((page = pipeline.processNextBatch()) != null) {
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
          Map<String, Object> row = new LinkedHashMap<>();
          for (int col = 0; col < page.getChannelCount(); col++) {
            String colName = col < columnNames.size() ? columnNames.get(col) : "col_" + col;
            Object value = extractValue(page, col, pos);
            row.put(colName, value);
          }
          rows.add(row);
        }
      }
      pipeline.close();

      // 4. Convert to JSON and return
      String json = toJson(rows);
      listener.onResponse(new ShardExecuteResponse(json));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Resolve column names from the root plan node. Walks down through unary nodes to find the
   * effective output column list.
   */
  private List<String> resolveColumnNames(DqePlanNode node) {
    if (node instanceof TableScanNode) {
      return ((TableScanNode) node).getColumns();
    }
    if (node instanceof org.opensearch.sql.dqe.planner.plan.ProjectNode) {
      return ((org.opensearch.sql.dqe.planner.plan.ProjectNode) node).getOutputColumns();
    }
    if (node instanceof org.opensearch.sql.dqe.planner.plan.AggregationNode) {
      org.opensearch.sql.dqe.planner.plan.AggregationNode agg =
          (org.opensearch.sql.dqe.planner.plan.AggregationNode) node;
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

  /**
   * Extract a typed value from a Page at the given column and row position. Uses the column type
   * map when available, otherwise infers from block behavior.
   */
  private Object extractValue(Page page, int channel, int position) {
    if (page.getBlock(channel).isNull(position)) {
      return null;
    }
    // Try to determine the type from our column type map or use a simple heuristic
    try {
      return BigintType.BIGINT.getLong(page.getBlock(channel), position);
    } catch (Exception e) {
      // Fall back to other types
    }
    try {
      return VarcharType.VARCHAR.getSlice(page.getBlock(channel), position).toStringUtf8();
    } catch (Exception e) {
      // Fall back
    }
    try {
      return DoubleType.DOUBLE.getDouble(page.getBlock(channel), position);
    } catch (Exception e) {
      // Fall back
    }
    try {
      return BooleanType.BOOLEAN.getBoolean(page.getBlock(channel), position);
    } catch (Exception e) {
      // Fall back
    }
    return page.getBlock(channel).toString();
  }

  /** Convert a list of row maps to a JSON array string. Minimal implementation for MVP. */
  static String toJson(List<Map<String, Object>> rows) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < rows.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("{");
      Map<String, Object> row = rows.get(i);
      int j = 0;
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (j > 0) {
          sb.append(",");
        }
        sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
        appendJsonValue(sb, entry.getValue());
        j++;
      }
      sb.append("}");
    }
    sb.append("]");
    return sb.toString();
  }

  private static void appendJsonValue(StringBuilder sb, Object value) {
    if (value == null) {
      sb.append("null");
    } else if (value instanceof Number) {
      sb.append(value);
    } else if (value instanceof Boolean) {
      sb.append(value);
    } else {
      sb.append("\"").append(escapeJson(value.toString())).append("\"");
    }
  }

  private static String escapeJson(String s) {
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
