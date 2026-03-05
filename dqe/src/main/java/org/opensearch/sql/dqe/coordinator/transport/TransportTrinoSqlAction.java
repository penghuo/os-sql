/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.dqe.coordinator.fragment.PlanFragment;
import org.opensearch.sql.dqe.coordinator.fragment.PlanFragmenter;
import org.opensearch.sql.dqe.coordinator.merge.ResultMerger;
import org.opensearch.sql.dqe.coordinator.metadata.OpenSearchMetadata;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.planner.LogicalPlanner;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Coordinator transport action that orchestrates the full DQE query pipeline: parse, plan,
 * fragment, execute (locally for MVP), merge, and format the response.
 *
 * <p>For the MVP, all shard fragments are executed locally on the coordinator thread using {@link
 * LocalExecutionPlanner}. Actual distributed transport dispatch to remote shards will be added
 * post-MVP.
 */
public class TransportTrinoSqlAction
    extends HandledTransportAction<ActionRequest, TrinoSqlResponse> {

  private static final Logger LOG = LogManager.getLogger();

  private final ClusterService clusterService;
  private final Function<TableScanNode, Operator> scanFactory;

  /**
   * Constructor for plugin wiring with dependency injection.
   *
   * @param transportService the transport service
   * @param actionFilters action filters
   * @param clusterService the cluster service for metadata and routing
   * @param scanFactory factory that creates a leaf Operator for a given TableScanNode
   */
  @Inject
  public TransportTrinoSqlAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      Function<TableScanNode, Operator> scanFactory) {
    super(TrinoSqlAction.NAME, transportService, actionFilters, TrinoSqlRequest::new);
    this.clusterService = clusterService;
    this.scanFactory = scanFactory;
  }

  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TrinoSqlResponse> listener) {
    TrinoSqlRequest sqlReq = TrinoSqlRequest.fromActionRequest(request);
    try {
      // 1. Parse
      DqeSqlParser parser = new DqeSqlParser();
      Statement stmt = parser.parse(sqlReq.getQuery());

      // 2. Resolve metadata
      OpenSearchMetadata metadata = new OpenSearchMetadata(clusterService);

      // 3. Plan
      DqePlanNode plan = LogicalPlanner.plan(stmt, metadata::getTableInfo);

      // 4. Explain mode: return the plan description
      if (sqlReq.isExplain()) {
        listener.onResponse(new TrinoSqlResponse(formatExplain(plan)));
        return;
      }

      // 5. Fragment
      PlanFragmenter fragmenter = new PlanFragmenter();
      PlanFragmenter.FragmentResult fragments = fragmenter.fragment(plan, clusterService.state());

      // 6. Build column type map from metadata for operator execution
      String indexName = findIndexName(plan);
      TableInfo tableInfo = metadata.getTableInfo(indexName);
      Map<String, Type> columnTypeMap =
          tableInfo.columns().stream()
              .collect(
                  Collectors.toMap(TableInfo.ColumnInfo::name, TableInfo.ColumnInfo::trinoType));

      // 7. Execute each shard fragment locally (MVP - no actual transport dispatch)
      List<String> shardResults = new ArrayList<>();
      for (PlanFragment frag : fragments.shardFragments()) {
        LocalExecutionPlanner execPlanner = new LocalExecutionPlanner(scanFactory, columnTypeMap);
        Operator pipeline = frag.shardPlan().accept(execPlanner, null);

        List<String> columnNames = resolveColumnNames(frag.shardPlan());
        List<Map<String, Object>> rows = drainPipelineToRows(pipeline, columnNames);
        pipeline.close();

        shardResults.add(rowsToJson(rows));
      }

      // 8. Merge
      ResultMerger merger = new ResultMerger();
      List<Map<String, Object>> finalRows;
      if (fragments.coordinatorPlan() != null
          && fragments.coordinatorPlan() instanceof AggregationNode) {
        finalRows =
            merger.mergeAggregation(shardResults, (AggregationNode) fragments.coordinatorPlan());
      } else {
        finalRows = merger.mergePassthrough(shardResults);
      }

      // 9. Format response
      String responseJson = formatResponse(finalRows, plan);
      listener.onResponse(new TrinoSqlResponse(responseJson));

    } catch (Exception e) {
      LOG.error("Error executing Trino SQL query: {}", sqlReq.getQuery(), e);
      listener.onFailure(e);
    }
  }

  /** Convert a list of row maps to a JSON array string. */
  static String rowsToJson(List<Map<String, Object>> rows) {
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

  /**
   * Drain all pages from an operator pipeline and convert them into a list of row maps using the
   * column names and type-based extraction.
   */
  private List<Map<String, Object>> drainPipelineToRows(
      Operator pipeline, List<String> columnNames) {
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
    return rows;
  }

  /**
   * Extract a typed value from a Page at the given column and row position. Tries BIGINT, VARCHAR,
   * DOUBLE, BOOLEAN in sequence.
   */
  private Object extractValue(Page page, int channel, int position) {
    if (page.getBlock(channel).isNull(position)) {
      return null;
    }
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

  /**
   * Resolve column names from the root plan node by walking the tree to find effective output
   * columns.
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

  /** Walk the plan tree to find the TableScanNode and extract the index name. */
  private String findIndexName(DqePlanNode plan) {
    String indexName =
        plan.accept(
            new DqePlanVisitor<String, Void>() {
              @Override
              public String visitTableScan(TableScanNode node, Void context) {
                return node.getIndexName();
              }

              @Override
              public String visitPlan(DqePlanNode node, Void context) {
                for (DqePlanNode child : node.getChildren()) {
                  String result = child.accept(this, context);
                  if (result != null) {
                    return result;
                  }
                }
                return null;
              }
            },
            null);

    if (indexName == null) {
      throw new IllegalArgumentException("Plan does not contain a TableScanNode");
    }
    return indexName;
  }

  /**
   * Format the explain output for a logical plan. Produces a JSON object describing the plan tree.
   */
  static String formatExplain(DqePlanNode plan) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"plan\":\"");
    sb.append(escapeJson(describePlan(plan, 0)));
    sb.append("\"}");
    return sb.toString();
  }

  /** Recursively describe a plan node as a human-readable string. */
  private static String describePlan(DqePlanNode node, int indent) {
    StringBuilder sb = new StringBuilder();
    String prefix = "  ".repeat(indent);
    sb.append(prefix).append(node.getClass().getSimpleName());

    if (node instanceof TableScanNode scan) {
      sb.append("[")
          .append(scan.getIndexName())
          .append(", cols=")
          .append(scan.getColumns())
          .append("]");
    } else if (node instanceof ProjectNode proj) {
      sb.append("[cols=").append(proj.getOutputColumns()).append("]");
    } else if (node instanceof AggregationNode agg) {
      sb.append("[groupBy=")
          .append(agg.getGroupByKeys())
          .append(", aggs=")
          .append(agg.getAggregateFunctions())
          .append(", step=")
          .append(agg.getStep())
          .append("]");
    }

    for (DqePlanNode child : node.getChildren()) {
      sb.append("\\n");
      sb.append(describePlan(child, indent + 1));
    }
    return sb.toString();
  }

  /**
   * Format the query result as a JSON response matching the OpenSearch SQL response format:
   *
   * <pre>
   * {
   *   "schema": [{"name": "col", "type": "long"}, ...],
   *   "datarows": [[val1, val2], ...],
   *   "total": N,
   *   "size": N,
   *   "status": 200
   * }
   * </pre>
   */
  static String formatResponse(List<Map<String, Object>> rows, DqePlanNode plan) {
    List<String> columns = resolveColumnNames(plan);
    StringBuilder sb = new StringBuilder();
    sb.append("{\"schema\":[");

    // Build schema from column names
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      String colName = columns.get(i);
      String type = inferType(rows, colName);
      sb.append("{\"name\":\"")
          .append(escapeJson(colName))
          .append("\",\"type\":\"")
          .append(type)
          .append("\"}");
    }
    sb.append("],\"datarows\":[");

    // Build data rows
    for (int r = 0; r < rows.size(); r++) {
      if (r > 0) {
        sb.append(",");
      }
      sb.append("[");
      Map<String, Object> row = rows.get(r);
      for (int c = 0; c < columns.size(); c++) {
        if (c > 0) {
          sb.append(",");
        }
        Object val = row.get(columns.get(c));
        appendJsonValue(sb, val);
      }
      sb.append("]");
    }

    sb.append("],\"total\":").append(rows.size());
    sb.append(",\"size\":").append(rows.size());
    sb.append(",\"status\":200}");
    return sb.toString();
  }

  /** Infer the type name for a column by looking at the first non-null value in the rows. */
  private static String inferType(List<Map<String, Object>> rows, String columnName) {
    for (Map<String, Object> row : rows) {
      Object val = row.get(columnName);
      if (val != null) {
        if (val instanceof Long || val instanceof Integer) {
          return "long";
        } else if (val instanceof Double || val instanceof Float) {
          return "double";
        } else if (val instanceof Boolean) {
          return "boolean";
        } else {
          return "keyword";
        }
      }
    }
    return "keyword";
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
    if (s == null) {
      return "null";
    }
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
