/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.dqe.common.config.DqeSettings;
import org.opensearch.sql.dqe.coordinator.metadata.OpenSearchMetadata;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo.ColumnInfo;
import org.opensearch.sql.dqe.function.BuiltinFunctions;
import org.opensearch.sql.dqe.function.FunctionRegistry;
import org.opensearch.sql.dqe.function.expression.BlockExpression;
import org.opensearch.sql.dqe.function.expression.ExpressionCompiler;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner;
import org.opensearch.sql.dqe.shard.source.ColumnHandle;
import org.opensearch.sql.dqe.shard.source.OpenSearchPageSource;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Transport action that executes a DQE plan fragment on a shard. The coordinator sends a serialized
 * plan fragment to this action, which deserializes it, builds an operator pipeline via {@link
 * LocalExecutionPlanner}, drains all pages, and returns the result as serialized Trino Pages.
 *
 * <p>The production {@link Inject @Inject} constructor takes only standard Guice-injectable
 * dependencies ({@link NodeClient}, {@link ClusterService}). The scan factory and column type map
 * are built dynamically inside {@link #doExecute} from the request's index metadata, so no custom
 * Guice bindings are required.
 */
public class TransportShardExecuteAction
    extends HandledTransportAction<ActionRequest, ShardExecuteResponse> {

  /** Thread pool name for shard-level DQE execution. */
  public static final String DQE_THREAD_POOL_NAME = "dqe-shard-executor";

  /** NodeClient for executing search requests on the local node (production path). */
  private final NodeClient client;

  /** ClusterService for resolving index metadata (production path). */
  private final ClusterService clusterService;

  /**
   * Scan factory supplied directly (test path only). When non-null, the action uses this factory
   * and the companion {@link #columnTypeMap} instead of building them from cluster metadata.
   */
  private final Function<TableScanNode, Operator> scanFactory;

  /** Column type map supplied directly (test path only). */
  private final Map<String, Type> columnTypeMap;

  /**
   * Production constructor for plugin wiring with Guice dependency injection. All parameters are
   * standard OpenSearch injectable types. Execution is routed to the {@value #DQE_THREAD_POOL_NAME}
   * thread pool.
   *
   * @param transportService the transport service
   * @param actionFilters action filters
   * @param client the node-local client for executing search requests
   * @param clusterService cluster service for resolving index metadata
   */
  @Inject
  public TransportShardExecuteAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService) {
    super(
        ShardExecuteAction.NAME,
        transportService,
        actionFilters,
        ShardExecuteRequest::new,
        DQE_THREAD_POOL_NAME);
    this.client = client;
    this.clusterService = clusterService;
    this.scanFactory = null;
    this.columnTypeMap = null;
  }

  /**
   * Test constructor that accepts a scan factory and column type map directly, bypassing the need
   * for a real NodeClient and ClusterService.
   *
   * @param transportService the transport service
   * @param actionFilters action filters
   * @param scanFactory factory that creates a leaf Operator for a given TableScanNode
   * @param columnTypeMap mapping from column name to Trino Type
   */
  TransportShardExecuteAction(
      TransportService transportService,
      ActionFilters actionFilters,
      Function<TableScanNode, Operator> scanFactory,
      Map<String, Type> columnTypeMap) {
    super(ShardExecuteAction.NAME, transportService, actionFilters, ShardExecuteRequest::new);
    this.client = null;
    this.clusterService = null;
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

      // 2. Resolve scan factory and column types
      final Function<TableScanNode, Operator> effectiveScanFactory;
      final Map<String, Type> effectiveColumnTypeMap;

      if (scanFactory != null) {
        // Test path: use directly injected factory and type map
        effectiveScanFactory = scanFactory;
        effectiveColumnTypeMap = columnTypeMap != null ? columnTypeMap : Map.of();
      } else {
        // Production path: build from cluster metadata and NodeClient
        String indexName = findIndexName(plan);
        OpenSearchMetadata metadata = new OpenSearchMetadata(clusterService);
        TableInfo tableInfo = metadata.getTableInfo(indexName);
        effectiveColumnTypeMap =
            tableInfo.columns().stream()
                .collect(Collectors.toMap(ColumnInfo::name, ColumnInfo::trinoType));

        Settings nodeSettings = clusterService.getSettings();
        int batchSize = DqeSettings.PAGE_BATCH_SIZE.get(nodeSettings);
        effectiveScanFactory = buildScanFactory(req, effectiveColumnTypeMap, batchSize);
      }

      // 3. Build operator pipeline
      LocalExecutionPlanner planner =
          new LocalExecutionPlanner(effectiveScanFactory, effectiveColumnTypeMap);
      Operator pipeline = plan.accept(planner, null);

      // 4. Execute: drain pages
      List<Page> pages = new ArrayList<>();
      Page page;
      while ((page = pipeline.processNextBatch()) != null) {
        pages.add(page);
      }
      pipeline.close();

      // 5. Resolve column types from the plan
      List<Type> columnTypes = resolveColumnTypes(plan, effectiveColumnTypeMap);

      // 6. Return Page-based response
      listener.onResponse(new ShardExecuteResponse(pages, columnTypes));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Build a scan factory that creates an {@link OpenSearchPageSource} for the given shard. The
   * factory wraps the {@link NodeClient} in a {@link OpenSearchPageSource.SearchClient} adapter.
   *
   * @param req the shard execute request
   * @param typeMap mapping from column name to Trino Type
   * @param batchSize number of documents per scroll batch
   */
  private Function<TableScanNode, Operator> buildScanFactory(
      ShardExecuteRequest req, Map<String, Type> typeMap, int batchSize) {
    OpenSearchPageSource.SearchClient searchClient =
        new OpenSearchPageSource.SearchClient() {
          @Override
          public SearchResponse search(SearchRequest request) {
            return client.search(request).actionGet();
          }

          @Override
          public SearchResponse searchScroll(SearchScrollRequest request) {
            return client.searchScroll(request).actionGet();
          }

          @Override
          public ClearScrollResponse clearScroll(ClearScrollRequest request) {
            return client.clearScroll(request).actionGet();
          }
        };

    return node -> {
      List<ColumnHandle> columns =
          node.getColumns().stream()
              .map(col -> new ColumnHandle(col, typeMap.getOrDefault(col, BigintType.BIGINT)))
              .collect(Collectors.toList());

      SearchSourceBuilder dsl = new SearchSourceBuilder();
      if (node.getDslFilter() != null) {
        dsl.query(QueryBuilders.wrapperQuery(node.getDslFilter()));
      }

      // Apply _source filtering to only fetch needed columns
      if (node.getColumns().isEmpty()) {
        dsl.fetchSource(false); // No columns needed (e.g., COUNT(*))
      } else {
        dsl.fetchSource(node.getColumns().toArray(new String[0]), null);
      }

      return new OpenSearchPageSource(
          searchClient, node.getIndexName(), req.getShardId(), dsl, columns, batchSize);
    };
  }

  /**
   * Walk the plan tree to find the index name from the leaf {@link TableScanNode}.
   *
   * @throws IllegalStateException if no TableScanNode is found
   */
  static String findIndexName(DqePlanNode node) {
    if (node instanceof TableScanNode) {
      return ((TableScanNode) node).getIndexName();
    }
    List<DqePlanNode> children = node.getChildren();
    if (!children.isEmpty()) {
      return findIndexName(children.get(0));
    }
    throw new IllegalStateException("Plan tree contains no TableScanNode");
  }

  /**
   * Resolve column types from the plan node by walking the tree to determine output column names
   * and mapping them to types. For computed expression column names (e.g., arithmetic expressions
   * like "(count_long * price_double)"), the result type is inferred by compiling the expression
   * and checking the output type.
   */
  private List<Type> resolveColumnTypes(DqePlanNode node, Map<String, Type> typeMap) {
    List<String> columnNames = resolveColumnNames(node);

    // Check if any column name is a computed expression (not in the type map)
    boolean hasComputed = false;
    for (String col : columnNames) {
      if (!typeMap.containsKey(col)) {
        hasComputed = true;
        break;
      }
    }

    if (!hasComputed) {
      // Fast path: all columns are plain column references
      List<Type> types = new ArrayList<>();
      for (String col : columnNames) {
        types.add(typeMap.getOrDefault(col, BigintType.BIGINT));
      }
      return types;
    }

    // Find the EvalNode in the plan to get expression strings and their output column names
    EvalNode evalNode = findEvalNode(node);
    Map<String, String> columnNameToExpression = new HashMap<>();
    if (evalNode != null) {
      List<String> evalOutputNames = evalNode.getOutputColumnNames();
      List<String> evalExpressions = evalNode.getExpressions();
      for (int i = 0; i < evalOutputNames.size(); i++) {
        columnNameToExpression.put(evalOutputNames.get(i), evalExpressions.get(i));
      }
    }

    // Build column index and type maps for expression compilation
    TableScanNode scanNode = findTableScanNode(node);
    List<String> tableColumns = scanNode != null ? scanNode.getColumns() : List.of();
    Map<String, Integer> columnIndexMap = new HashMap<>();
    for (int i = 0; i < tableColumns.size(); i++) {
      columnIndexMap.put(tableColumns.get(i), i);
    }

    FunctionRegistry registry = BuiltinFunctions.createRegistry();
    ExpressionCompiler compiler = new ExpressionCompiler(registry, columnIndexMap, typeMap);
    DqeSqlParser exprParser = new DqeSqlParser();

    List<Type> types = new ArrayList<>();
    for (String col : columnNames) {
      if (typeMap.containsKey(col)) {
        types.add(typeMap.get(col));
      } else {
        // Try aggregate output type inference first (e.g., MIN(URL) → VarcharType)
        Type aggType = inferAggregateOutputType(col, typeMap);
        if (aggType != null) {
          types.add(aggType);
          continue;
        }
        // Try to infer the type by compiling the expression
        String exprStr = columnNameToExpression.getOrDefault(col, col);
        try {
          io.trino.sql.tree.Expression expr = exprParser.parseExpression(exprStr);
          BlockExpression blockExpr = compiler.compile(expr);
          types.add(blockExpr.getType());
        } catch (Exception e) {
          // If parsing/compilation fails, fall back to BIGINT
          types.add(BigintType.BIGINT);
        }
      }
    }
    return types;
  }

  /**
   * Infer the output type of an aggregate function expression like "count(*)", "min(URL)",
   * "sum(amount)". Returns null if the column name is not an aggregate expression.
   */
  private static final java.util.regex.Pattern SHARD_AGG_TYPE_PATTERN =
      java.util.regex.Pattern.compile(
          "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$",
          java.util.regex.Pattern.CASE_INSENSITIVE);

  private static Type inferAggregateOutputType(String colName, Map<String, Type> columnTypeMap) {
    java.util.regex.Matcher m = SHARD_AGG_TYPE_PATTERN.matcher(colName);
    if (!m.matches()) {
      return null;
    }
    String funcName = m.group(1).toUpperCase(java.util.Locale.ROOT);
    String arg = m.group(3).trim();

    switch (funcName) {
      case "COUNT":
        return BigintType.BIGINT;
      case "AVG":
        return DoubleType.DOUBLE;
      case "SUM":
        {
          Type inputType = columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
          return inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
        }
      case "MIN":
      case "MAX":
        return columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      default:
        return null;
    }
  }

  /** Walk the plan tree to find the EvalNode. Returns null if none. */
  private static EvalNode findEvalNode(DqePlanNode plan) {
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
  private static TableScanNode findTableScanNode(DqePlanNode plan) {
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
