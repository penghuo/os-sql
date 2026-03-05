/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Statement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.sql.dqe.common.config.DqeSettings;
import org.opensearch.sql.dqe.coordinator.fragment.PlanFragment;
import org.opensearch.sql.dqe.coordinator.fragment.PlanFragmenter;
import org.opensearch.sql.dqe.coordinator.merge.ResultMerger;
import org.opensearch.sql.dqe.coordinator.metadata.OpenSearchMetadata;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.planner.LogicalPlanner;
import org.opensearch.sql.dqe.planner.optimizer.PlanOptimizer;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteAction;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteRequest;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteResponse;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Coordinator transport action that orchestrates the full DQE query pipeline: parse, plan,
 * fragment, dispatch to shards via transport, merge Pages, and format the response.
 *
 * <p>Shard plan fragments are dispatched to their target nodes via {@link TransportService}. Each
 * shard executes its fragment locally and returns serialized Trino Pages via {@link
 * ShardExecuteResponse}.
 */
public class TransportTrinoSqlAction
    extends HandledTransportAction<ActionRequest, TrinoSqlResponse> {

  private static final Logger LOG = LogManager.getLogger();

  private final ClusterService clusterService;
  private final TransportService transportService;

  /** Tracks the dynamic cluster setting for whether DQE is enabled. */
  private volatile boolean dqeEnabled;

  /**
   * Constructor for plugin wiring with dependency injection.
   *
   * @param transportService the transport service for dispatching shard requests
   * @param actionFilters action filters
   * @param clusterService the cluster service for metadata and routing
   */
  @Inject
  public TransportTrinoSqlAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService) {
    super(TrinoSqlAction.NAME, transportService, actionFilters, TrinoSqlRequest::new);
    this.clusterService = clusterService;
    this.transportService = transportService;

    // Initialize from current settings and register a listener for dynamic updates
    this.dqeEnabled = DqeSettings.DQE_ENABLED.get(clusterService.getSettings());
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(DqeSettings.DQE_ENABLED, enabled -> this.dqeEnabled = enabled);
  }

  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TrinoSqlResponse> listener) {
    // Guard: check if DQE is enabled (uses volatile field updated by cluster settings listener)
    if (!dqeEnabled) {
      listener.onFailure(
          new IllegalAccessException("DQE is disabled. Set plugins.dqe.enabled to true."));
      return;
    }

    TrinoSqlRequest sqlReq = TrinoSqlRequest.fromActionRequest(request);
    try {
      // 1. Parse
      DqeSqlParser parser = new DqeSqlParser();
      Statement stmt = parser.parse(sqlReq.getQuery());

      // 2. Resolve metadata
      OpenSearchMetadata metadata = new OpenSearchMetadata(clusterService);

      // 3. Plan
      DqePlanNode plan = LogicalPlanner.plan(stmt, metadata::getTableInfo);

      // 4. Optimize
      PlanOptimizer optimizer = new PlanOptimizer();
      DqePlanNode optimizedPlan = optimizer.optimize(plan);

      // 5. Explain mode: return the plan description
      if (sqlReq.isExplain()) {
        listener.onResponse(new TrinoSqlResponse(formatExplain(optimizedPlan)));
        return;
      }

      // 6. Fragment
      PlanFragmenter fragmenter = new PlanFragmenter();
      PlanFragmenter.FragmentResult fragments =
          fragmenter.fragment(optimizedPlan, clusterService.state());

      // 7. Build column type information from metadata
      String indexName = findIndexName(optimizedPlan);
      TableInfo tableInfo = metadata.getTableInfo(indexName);
      Map<String, Type> columnTypeMap =
          tableInfo.columns().stream()
              .collect(
                  Collectors.toMap(TableInfo.ColumnInfo::name, TableInfo.ColumnInfo::trinoType));

      List<String> columnNames = resolveColumnNames(optimizedPlan);
      List<Type> columnTypes = new ArrayList<>();
      for (String col : columnNames) {
        columnTypes.add(columnTypeMap.getOrDefault(col, BigintType.BIGINT));
      }

      // 8. Dispatch to shards via transport
      List<PlanFragment> shardFragments = fragments.shardFragments();
      DqePlanNode coordinatorPlan = fragments.coordinatorPlan();

      long timeoutMillis =
          DqeSettings.QUERY_TIMEOUT.get(clusterService.getSettings()).millis();

      GroupedActionListener<ShardExecuteResponse> groupedListener =
          new GroupedActionListener<>(
              ActionListener.wrap(
                  responses -> {
                    try {
                      // 9. Merge Page-based results
                      List<List<Page>> shardPages =
                          responses.stream()
                              .map(ShardExecuteResponse::getPages)
                              .collect(Collectors.toList());

                      ResultMerger merger = new ResultMerger();
                      List<Page> mergedPages;
                      if (coordinatorPlan instanceof AggregationNode aggNode) {
                        mergedPages = merger.mergeAggregation(shardPages, aggNode, columnTypes);
                      } else {
                        mergedPages = merger.mergePassthrough(shardPages);
                      }

                      // 10. Apply coordinator-level LIMIT (shards each apply their own limit,
                      //     but the merged result may exceed the global limit)
                      long globalLimit = findGlobalLimit(optimizedPlan);
                      if (globalLimit >= 0) {
                        mergedPages = applyGlobalLimit(mergedPages, globalLimit);
                      }

                      // 11. Format response (Page -> JSON for REST client)
                      String responseJson = formatResponse(mergedPages, columnNames, columnTypes);
                      listener.onResponse(new TrinoSqlResponse(responseJson));
                    } catch (Exception e) {
                      listener.onFailure(e);
                    }
                  },
                  listener::onFailure),
              shardFragments.size());

      // Dispatch each fragment to its target node
      for (PlanFragment frag : shardFragments) {
        BytesStreamOutput planOut = new BytesStreamOutput();
        DqePlanNode.writePlanNode(planOut, frag.shardPlan());
        byte[] serializedPlan = planOut.bytes().toBytesRef().bytes;

        ShardExecuteRequest shardReq =
            new ShardExecuteRequest(
                serializedPlan, frag.indexName(), frag.shardId(), timeoutMillis);

        // Resolve target node
        DiscoveryNode targetNode = clusterService.state().nodes().get(frag.nodeId());

        // Send via transport
        transportService.sendRequest(
            targetNode,
            ShardExecuteAction.NAME,
            shardReq,
            new TransportResponseHandler<ShardExecuteResponse>() {
              @Override
              public ShardExecuteResponse read(StreamInput in) throws IOException {
                return new ShardExecuteResponse(in);
              }

              @Override
              public void handleResponse(ShardExecuteResponse response) {
                groupedListener.onResponse(response);
              }

              @Override
              public void handleException(TransportException exp) {
                groupedListener.onFailure(exp);
              }

              @Override
              public String executor() {
                return ThreadPool.Names.SAME;
              }
            });
      }

    } catch (Exception e) {
      LOG.error("Error executing Trino SQL query: {}", sqlReq.getQuery(), e);
      listener.onFailure(e);
    }
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
          .append(scan.getColumns());
      if (scan.getDslFilter() != null) {
        sb.append(", dslFilter=").append(scan.getDslFilter());
      }
      sb.append("]");
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
   * Format the query result as a JSON response matching the OpenSearch SQL response format. Works
   * directly with Trino Pages.
   *
   * @param pages the merged result pages
   * @param columnNames the output column names
   * @param columnTypes the Trino types for each column
   * @return JSON response string
   */
  static String formatResponse(List<Page> pages, List<String> columnNames, List<Type> columnTypes) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"schema\":[");

    // Build schema from column names and types
    for (int i = 0; i < columnNames.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      String colName = columnNames.get(i);
      String type = trinoTypeToOpenSearchType(columnTypes.get(i));
      sb.append("{\"name\":\"")
          .append(escapeJson(colName))
          .append("\",\"type\":\"")
          .append(type)
          .append("\"}");
    }
    sb.append("],\"datarows\":[");

    // Build data rows from Pages
    int totalRows = 0;
    boolean firstRow = true;
    for (Page page : pages) {
      for (int pos = 0; pos < page.getPositionCount(); pos++) {
        if (!firstRow) {
          sb.append(",");
        }
        firstRow = false;
        sb.append("[");
        for (int col = 0; col < columnNames.size(); col++) {
          if (col > 0) {
            sb.append(",");
          }
          if (col < page.getChannelCount()) {
            appendJsonValue(sb, extractValue(page, col, pos, columnTypes.get(col)));
          } else {
            sb.append("null");
          }
        }
        sb.append("]");
        totalRows++;
      }
    }

    sb.append("],\"total\":").append(totalRows);
    sb.append(",\"size\":").append(totalRows);
    sb.append(",\"status\":200}");
    return sb.toString();
  }

  /** Extract a typed value from a Page at the given column and row position. */
  private static Object extractValue(Page page, int channel, int position, Type type) {
    Block block = page.getBlock(channel);
    if (block.isNull(position)) {
      return null;
    }
    if (type instanceof BigintType) {
      return BigintType.BIGINT.getLong(block, position);
    } else if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, position);
    } else if (type instanceof BooleanType) {
      return BooleanType.BOOLEAN.getBoolean(block, position);
    } else if (type instanceof VarcharType) {
      return VarcharType.VARCHAR.getSlice(block, position).toStringUtf8();
    } else {
      // Default: try getLong for other numeric types
      try {
        return type.getLong(block, position);
      } catch (Exception e) {
        return block.toString();
      }
    }
  }

  /** Map Trino Type to OpenSearch type name for schema output. */
  private static String trinoTypeToOpenSearchType(Type type) {
    if (type instanceof BigintType) {
      return "long";
    } else if (type instanceof DoubleType) {
      return "double";
    } else if (type instanceof BooleanType) {
      return "boolean";
    } else if (type instanceof VarcharType) {
      return "keyword";
    } else {
      return "keyword";
    }
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

  /**
   * Walk the plan tree to find a LimitNode and return its count. Returns -1 if no limit is present.
   */
  static long findGlobalLimit(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<Long, Void>() {
          @Override
          public Long visitPlan(DqePlanNode node, Void context) {
            if (node instanceof LimitNode limitNode) {
              return limitNode.getCount();
            }
            for (DqePlanNode child : node.getChildren()) {
              Long result = child.accept(this, context);
              if (result >= 0) {
                return result;
              }
            }
            return -1L;
          }
        },
        null);
  }

  /**
   * Apply a global row limit to merged pages. Trims the list of pages so that at most {@code
   * limit} total rows are retained.
   */
  static List<Page> applyGlobalLimit(List<Page> pages, long limit) {
    List<Page> result = new ArrayList<>();
    long remaining = limit;
    for (Page page : pages) {
      if (remaining <= 0) {
        break;
      }
      if (page.getPositionCount() <= remaining) {
        result.add(page);
        remaining -= page.getPositionCount();
      } else {
        // Trim this page to the remaining count
        result.add(page.getRegion(0, (int) remaining));
        remaining = 0;
      }
    }
    return result;
  }
}
