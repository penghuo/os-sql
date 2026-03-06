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
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.opensearch.sql.dqe.function.BuiltinFunctions;
import org.opensearch.sql.dqe.function.FunctionRegistry;
import org.opensearch.sql.dqe.function.expression.BlockExpression;
import org.opensearch.sql.dqe.function.expression.ExpressionCompiler;
import org.opensearch.sql.dqe.planner.LogicalPlanner;
import org.opensearch.sql.dqe.planner.optimizer.PlanOptimizer;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
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

      // 4. Optimize
      PlanOptimizer optimizer = new PlanOptimizer();
      DqePlanNode optimizedPlan = optimizer.optimize(plan);

      // 5. Fragment (needed for both explain and execute)
      PlanFragmenter fragmenter = new PlanFragmenter();
      PlanFragmenter.FragmentResult fragments =
          fragmenter.fragment(optimizedPlan, clusterService.state());

      // 6. Explain mode: return logical plan, optimized plan, and fragments
      if (sqlReq.isExplain()) {
        listener.onResponse(new TrinoSqlResponse(formatExplain(plan, optimizedPlan, fragments)));
        return;
      }

      // 7. Build column type information from metadata
      String indexName = findIndexName(optimizedPlan);
      TableInfo tableInfo = metadata.getTableInfo(indexName);
      Map<String, Type> columnTypeMap =
          tableInfo.columns().stream()
              .collect(
                  Collectors.toMap(TableInfo.ColumnInfo::name, TableInfo.ColumnInfo::trinoType));

      // Internal column names (for type lookup) vs display names (for response schema)
      List<String> internalColumnNames = resolveColumnNames(optimizedPlan);
      List<String> columnNames;
      if (stmt instanceof Query query2
          && query2.getQueryBody() instanceof QuerySpecification querySpec2) {
        columnNames =
            LogicalPlanner.extractDisplayColumnNames(
                querySpec2, tableInfo.columns().stream().map(TableInfo.ColumnInfo::name).toList());
      } else {
        columnNames = internalColumnNames;
      }
      // Use internal names for type lookup (aliases don't exist in the type map).
      // For computed expressions (e.g., "(count_long * price_double)"), the column name
      // won't exist in the type map. In that case, infer the result type by compiling
      // the expression and checking its output type.
      List<Type> columnTypes =
          resolveColumnTypes(internalColumnNames, columnTypeMap, optimizedPlan);

      // 8. Dispatch to shards via transport
      List<PlanFragment> shardFragments = fragments.shardFragments();
      DqePlanNode coordinatorPlan = fragments.coordinatorPlan();

      long timeoutMillis = DqeSettings.QUERY_TIMEOUT.get(clusterService.getSettings()).millis();

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
                        // Check if we need sorted merge
                        SortNode sortNode = findSortNode(optimizedPlan);
                        if (sortNode != null) {
                          // Use internal column names (which include sort-only columns
                          // appended by LogicalPlanner) to resolve sort key indices.
                          List<Integer> sortIndices =
                              sortNode.getSortKeys().stream()
                                  .map(internalColumnNames::indexOf)
                                  .collect(Collectors.toList());
                          // The sort limit must account for the global OFFSET so
                          // that enough rows survive the merge-sort for the
                          // subsequent applyGlobalOffset to skip correctly.
                          long rawLimit = findGlobalLimit(optimizedPlan);
                          long sortLimit =
                              rawLimit >= 0
                                  ? rawLimit + findGlobalOffset(optimizedPlan)
                                  : Long.MAX_VALUE;
                          mergedPages =
                              merger.mergeSorted(
                                  shardPages,
                                  sortIndices,
                                  sortNode.getAscending(),
                                  sortNode.getNullsFirst(),
                                  columnTypes,
                                  sortLimit);
                        } else {
                          mergedPages = merger.mergePassthrough(shardPages);
                        }
                      }

                      // 10. Apply coordinator-level OFFSET + LIMIT
                      long globalOffset = findGlobalOffset(optimizedPlan);
                      if (globalOffset > 0) {
                        mergedPages = applyGlobalOffset(mergedPages, globalOffset);
                      }
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

  /**
   * Resolve Trino types for the given column names. For plain column names that exist in the type
   * map, the mapped type is used directly. For computed expression column names (e.g., arithmetic
   * expressions like "(count_long * price_double)"), the result type is inferred by compiling the
   * expression and checking the output type of the resulting {@link BlockExpression}.
   *
   * @param columnNames the internal column names (may include expression strings)
   * @param columnTypeMap mapping from physical column names to Trino types
   * @param plan the optimized plan tree (used to find EvalNode expressions)
   * @return list of resolved types, one per column name
   */
  static List<Type> resolveColumnTypes(
      List<String> columnNames, Map<String, Type> columnTypeMap, DqePlanNode plan) {
    // Check if any column name is a computed expression (not in the type map).
    // If so, we need to compile those expressions to infer their output types.
    boolean hasComputed = false;
    for (String col : columnNames) {
      if (!columnTypeMap.containsKey(col)) {
        hasComputed = true;
        break;
      }
    }

    if (!hasComputed) {
      // Fast path: all columns are plain column references
      List<Type> types = new ArrayList<>();
      for (String col : columnNames) {
        types.add(columnTypeMap.getOrDefault(col, BigintType.BIGINT));
      }
      return types;
    }

    // Find the EvalNode in the plan to get expression strings and their output column names
    EvalNode evalNode = findEvalNode(plan);
    Map<String, String> columnNameToExpression = new HashMap<>();
    if (evalNode != null) {
      List<String> evalOutputNames = evalNode.getOutputColumnNames();
      List<String> evalExpressions = evalNode.getExpressions();
      for (int i = 0; i < evalOutputNames.size(); i++) {
        columnNameToExpression.put(evalOutputNames.get(i), evalExpressions.get(i));
      }
    }

    // Build column index and type maps for expression compilation. The indices correspond
    // to the TableScanNode columns (all physical columns of the table).
    TableScanNode scanNode = findTableScanNode(plan);
    List<String> tableColumns = scanNode != null ? scanNode.getColumns() : List.of();
    Map<String, Integer> columnIndexMap = new HashMap<>();
    for (int i = 0; i < tableColumns.size(); i++) {
      columnIndexMap.put(tableColumns.get(i), i);
    }

    FunctionRegistry registry = BuiltinFunctions.createRegistry();
    ExpressionCompiler compiler = new ExpressionCompiler(registry, columnIndexMap, columnTypeMap);
    DqeSqlParser exprParser = new DqeSqlParser();

    List<Type> types = new ArrayList<>();
    for (String col : columnNames) {
      if (columnTypeMap.containsKey(col)) {
        types.add(columnTypeMap.get(col));
      } else {
        // Try to infer the type by compiling the expression
        String exprStr = columnNameToExpression.getOrDefault(col, col);
        try {
          io.trino.sql.tree.Expression expr = exprParser.parseExpression(exprStr);
          BlockExpression blockExpr = compiler.compile(expr);
          types.add(blockExpr.getType());
        } catch (Exception e) {
          // If parsing/compilation fails, fall back to BIGINT
          LOG.warn(
              "Could not infer type for column '{}', defaulting to BIGINT: {}",
              col,
              e.getMessage());
          types.add(BigintType.BIGINT);
        }
      }
    }
    return types;
  }

  /** Walk the plan tree to find the EvalNode. Returns null if none. */
  static EvalNode findEvalNode(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<EvalNode, Void>() {
          @Override
          public EvalNode visitEval(EvalNode node, Void context) {
            return node;
          }

          @Override
          public EvalNode visitPlan(DqePlanNode node, Void context) {
            for (DqePlanNode child : node.getChildren()) {
              EvalNode result = child.accept(this, context);
              if (result != null) {
                return result;
              }
            }
            return null;
          }
        },
        null);
  }

  /** Walk the plan tree to find the TableScanNode. Returns null if none. */
  static TableScanNode findTableScanNode(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<TableScanNode, Void>() {
          @Override
          public TableScanNode visitTableScan(TableScanNode node, Void context) {
            return node;
          }

          @Override
          public TableScanNode visitPlan(DqePlanNode node, Void context) {
            for (DqePlanNode child : node.getChildren()) {
              TableScanNode result = child.accept(this, context);
              if (result != null) {
                return result;
              }
            }
            return null;
          }
        },
        null);
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
   * Format the explain output showing all three plan stages: logical plan (before optimization),
   * optimized plan (after optimization), and per-shard fragments with coordinator merge plan.
   */
  static String formatExplain(
      DqePlanNode logicalPlan, DqePlanNode optimizedPlan, PlanFragmenter.FragmentResult fragments) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");

    // 1. Logical plan (before optimization)
    sb.append("\"logical_plan\":");
    planToJson(sb, logicalPlan);

    // 2. Optimized plan (after optimization)
    sb.append(",\"optimized_plan\":");
    planToJson(sb, optimizedPlan);

    // 3. Fragments (per-shard plans + coordinator plan)
    sb.append(",\"fragments\":[");
    List<PlanFragment> frags = fragments.shardFragments();
    for (int i = 0; i < frags.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      PlanFragment f = frags.get(i);
      sb.append("{\"shard_id\":").append(f.shardId());
      sb.append(",\"node_id\":\"").append(escapeJson(f.nodeId())).append("\"");
      sb.append(",\"index\":\"").append(escapeJson(f.indexName())).append("\"");
      sb.append(",\"plan\":");
      planToJson(sb, f.shardPlan());
      sb.append("}");
    }
    sb.append("]");

    // 4. Coordinator merge plan (null for non-aggregate queries)
    sb.append(",\"coordinator_plan\":");
    if (fragments.coordinatorPlan() != null) {
      planToJson(sb, fragments.coordinatorPlan());
    } else {
      sb.append("null");
    }

    sb.append("}");
    return sb.toString();
  }

  /** Convert a plan node tree to a JSON object recursively. */
  private static void planToJson(StringBuilder sb, DqePlanNode node) {
    sb.append("{\"node\":\"").append(node.getClass().getSimpleName()).append("\"");

    // Node-specific attributes
    if (node instanceof TableScanNode scan) {
      sb.append(",\"index\":\"").append(escapeJson(scan.getIndexName())).append("\"");
      sb.append(",\"columns\":").append(toJsonArray(scan.getColumns()));
      if (scan.getDslFilter() != null) {
        sb.append(",\"dsl_filter\":").append(scan.getDslFilter());
      }
    } else if (node instanceof FilterNode filter) {
      sb.append(",\"predicate\":\"").append(escapeJson(filter.getPredicateString())).append("\"");
    } else if (node instanceof ProjectNode proj) {
      sb.append(",\"columns\":").append(toJsonArray(proj.getOutputColumns()));
    } else if (node instanceof AggregationNode agg) {
      sb.append(",\"group_by\":").append(toJsonArray(agg.getGroupByKeys()));
      sb.append(",\"aggregates\":").append(toJsonArray(agg.getAggregateFunctions()));
      sb.append(",\"step\":\"").append(agg.getStep().name()).append("\"");
    } else if (node instanceof SortNode sort) {
      sb.append(",\"sort_keys\":").append(toJsonArray(sort.getSortKeys()));
      sb.append(",\"ascending\":").append(sort.getAscending());
      sb.append(",\"nulls_first\":").append(sort.getNullsFirst());
    } else if (node instanceof LimitNode limit) {
      sb.append(",\"count\":").append(limit.getCount());
    }

    // Children
    List<DqePlanNode> children = node.getChildren();
    if (!children.isEmpty()) {
      sb.append(",\"child\":");
      planToJson(sb, children.get(0));
    }

    sb.append("}");
  }

  /** Convert a list of strings to a JSON array. */
  private static String toJsonArray(List<String> items) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("\"").append(escapeJson(items.get(i))).append("\"");
    }
    sb.append("]");
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
    } else if (type instanceof IntegerType) {
      return (int) IntegerType.INTEGER.getLong(block, position);
    } else if (type instanceof SmallintType) {
      return (short) SmallintType.SMALLINT.getLong(block, position);
    } else if (type instanceof TinyintType) {
      return (byte) TinyintType.TINYINT.getLong(block, position);
    } else if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, position);
    } else if (type instanceof RealType) {
      // RealType stores as int bits of float
      long bits = RealType.REAL.getLong(block, position);
      return (double) Float.intBitsToFloat((int) bits);
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
    } else if (type instanceof IntegerType) {
      return "integer";
    } else if (type instanceof SmallintType) {
      return "short";
    } else if (type instanceof TinyintType) {
      return "byte";
    } else if (type instanceof DoubleType) {
      return "double";
    } else if (type instanceof RealType) {
      return "float";
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

  /** Walk the plan tree to find a SortNode. Returns null if none. */
  static SortNode findSortNode(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<SortNode, Void>() {
          @Override
          public SortNode visitPlan(DqePlanNode node, Void context) {
            if (node instanceof SortNode sortNode) {
              return sortNode;
            }
            for (DqePlanNode child : node.getChildren()) {
              SortNode result = child.accept(this, context);
              if (result != null) {
                return result;
              }
            }
            return null;
          }
        },
        null);
  }

  /** Walk the plan tree to find a LimitNode and return its offset. Returns 0 if none. */
  static long findGlobalOffset(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<Long, Void>() {
          @Override
          public Long visitPlan(DqePlanNode node, Void context) {
            if (node instanceof LimitNode limitNode) {
              return limitNode.getOffset();
            }
            for (DqePlanNode child : node.getChildren()) {
              Long result = child.accept(this, context);
              if (result > 0) {
                return result;
              }
            }
            return 0L;
          }
        },
        null);
  }

  /** Skip the first {@code offset} rows from merged pages. */
  static List<Page> applyGlobalOffset(List<Page> pages, long offset) {
    List<Page> result = new ArrayList<>();
    long remaining = offset;
    for (Page page : pages) {
      if (remaining <= 0) {
        result.add(page);
      } else if (page.getPositionCount() <= remaining) {
        remaining -= page.getPositionCount();
      } else {
        result.add(page.getRegion((int) remaining, page.getPositionCount() - (int) remaining));
        remaining = 0;
      }
    }
    return result;
  }

  /**
   * Apply a global row limit to merged pages. Trims the list of pages so that at most {@code limit}
   * total rows are retained.
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
