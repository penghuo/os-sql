/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import com.google.common.base.Suppliers;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.calcite.utils.TimewrapPivot;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.ResourceLimitExceededException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.executor.PartialResultResponseListener;
import org.opensearch.sql.executor.PartialResultResponseListener.UpdateMode;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileScope;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.functions.DistinctCountApproxAggFunction;
import org.opensearch.sql.opensearch.functions.GeoIpFunction;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.transport.client.node.NodeClient;
import tools.jackson.databind.ObjectMapper;

/** OpenSearch execution engine implementation. */
public class OpenSearchExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(OpenSearchExecutionEngine.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final OpenSearchClient client;

  private final ExecutionProtector executionProtector;
  private final PlanSerializer planSerializer;

  public OpenSearchExecutionEngine(
      OpenSearchClient client,
      ExecutionProtector executionProtector,
      PlanSerializer planSerializer) {
    this.client = client;
    this.executionProtector = executionProtector;
    this.planSerializer = planSerializer;
    registerOpenSearchFunctions();
  }

  @Override
  public void execute(PhysicalPlan physicalPlan, ResponseListener<QueryResponse> listener) {
    execute(physicalPlan, ExecutionContext.emptyExecutionContext(), listener);
  }

  @Override
  public void execute(
      PhysicalPlan physicalPlan,
      ExecutionContext context,
      ResponseListener<QueryResponse> listener) {
    PhysicalPlan plan = executionProtector.protect(physicalPlan);
    client.schedule(
        () -> {
          try {
            List<ExprValue> result = new ArrayList<>();

            context.getSplit().ifPresent(plan::add);
            plan.open();

            Integer querySizeLimit = context.getQuerySizeLimit();
            while (plan.hasNext() && (querySizeLimit == null || result.size() < querySizeLimit)) {
              result.add(plan.next());
            }

            QueryResponse response =
                new QueryResponse(
                    physicalPlan.schema(), result, planSerializer.convertToCursor(plan));
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
                          explainNode.setDescription(Map.of("request", node.explain()));
                        });
                  }
                };

            listener.onResponse(openSearchExplain.apply(plan));
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            plan.close();
          }
        });
  }

  private Hook.Closeable getPhysicalPlanInHook(
      AtomicReference<String> physical, SqlExplainLevel level) {
    return Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
        obj -> {
          RelRoot relRoot = (RelRoot) obj;
          physical.set(RelOptUtil.toString(relRoot.rel, level));
        });
  }

  private Hook.Closeable getOptimizedPlanInHook(AtomicReference<RelNode> optimizedPlan) {
    return Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
        (java.util.function.Consumer<Object>) obj -> optimizedPlan.set(((RelRoot) obj).rel));
  }

  private Hook.Closeable getCodegenInHook(AtomicReference<String> codegen) {
    return Hook.JAVA_PLAN.addThread(
        obj -> {
          codegen.set((String) obj);
        });
  }

  /**
   * Parse sourceBuilder JSON strings within the physical plan tree to objects. This finds any
   * sourceBuilder fields (which are serialized as JSON strings by RelJsonWriter) and parses them to
   * JSON objects for easier client consumption.
   */
  @SuppressWarnings("unchecked")
  private void parseSourceBuilderInPhysicalTree(Object physicalTree) {
    try {
      if (!(physicalTree instanceof Map)) {
        return;
      }
      Map<String, Object> tree = (Map<String, Object>) physicalTree;
      Object relsObj = tree.get("rels");
      if (!(relsObj instanceof List)) {
        return;
      }

      List<Object> rels = (List<Object>) relsObj;
      for (Object relObj : rels) {
        if (!(relObj instanceof Map)) {
          continue;
        }
        Map<String, Object> rel = (Map<String, Object>) relObj;

        // Parse sourceBuilder if it exists as a JSON string
        Object sourceBuilderObj = rel.get("sourceBuilder");
        if (sourceBuilderObj instanceof String) {
          try {
            String sourceBuilderJson = (String) sourceBuilderObj;
            Object parsed = objectMapper.readValue(sourceBuilderJson, Object.class);
            rel.put("sourceBuilder", parsed);
          } catch (Exception e) {
            logger.debug("Failed to parse sourceBuilder JSON: {}", e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to parse sourceBuilder in physical tree: " + e.getMessage());
    }
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    explain(rel, mode, null, context, listener);
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainMode mode,
      Format format,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            if (format == Format.JSON_TREE) {
              // Use RelJsonWriter for structured JSON tree output
              try {
                RelJsonWriter logicalWriter = new RelJsonWriter();
                rel.explain(logicalWriter);
                String logicalJson = logicalWriter.asString();

                AtomicReference<String> physicalJson = new AtomicReference<>();
                AtomicReference<Exception> physicalError = new AtomicReference<>();
                SqlExplainLevel level =
                    mode == ExplainMode.COST
                        ? SqlExplainLevel.ALL_ATTRIBUTES
                        : SqlExplainLevel.EXPPLAN_ATTRIBUTES;

                try (Hook.Closeable closeable =
                    Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(
                        obj -> {
                          try {
                            RelRoot relRoot = (RelRoot) obj;
                            RelJsonWriter physicalWriter = new RelJsonWriter();
                            relRoot.rel.explain(physicalWriter);
                            physicalJson.set(physicalWriter.asString());
                          } catch (Exception e) {
                            physicalError.set(e);
                          }
                        })) {
                  // triggers the hook
                  OpenSearchRelRunners.run(context, CalciteToolsHelper.optimize(rel, context));
                }

                if (physicalError.get() != null) {
                  throw physicalError.get();
                }

                // Parse JSON strings to objects for structured output
                Object logicalTree = objectMapper.readValue(logicalJson, Object.class);
                Object physicalTree = objectMapper.readValue(physicalJson.get(), Object.class);

                // Parse sourceBuilder JSON if present in physical plan
                parseSourceBuilderInPhysicalTree(physicalTree);

                ExplainResponseNodeV2 response =
                    new ExplainResponseNodeV2(logicalJson, physicalJson.get(), null);
                response.setLogicalTree(logicalTree);
                response.setPhysicalTree(physicalTree);

                listener.onResponse(new ExplainResponse(response));
              } catch (Exception e) {
                // RelJsonWriter can't handle some custom types (e.g., SystemLimitType enum)
                listener.onFailure(
                    new UnsupportedOperationException(
                        "Cannot serialize plan to json_tree format: " + e.getMessage(), e));
                return;
              }
            } else {
              // Original string format for json/yaml
              if (mode == ExplainMode.SIMPLE) {
                String logical = RelOptUtil.toString(rel, SqlExplainLevel.NO_ATTRIBUTES);
                listener.onResponse(
                    new ExplainResponse(new ExplainResponseNodeV2(logical, null, null)));
              } else {
                SqlExplainLevel level =
                    mode == ExplainMode.COST
                        ? SqlExplainLevel.ALL_ATTRIBUTES
                        : SqlExplainLevel.EXPPLAN_ATTRIBUTES;
                String logical = RelOptUtil.toString(rel, level);
                AtomicReference<String> physical = new AtomicReference<>();
                AtomicReference<String> javaCode = new AtomicReference<>();
                try (Hook.Closeable closeable = getPhysicalPlanInHook(physical, level)) {
                  if (mode == ExplainMode.EXTENDED) {
                    getCodegenInHook(javaCode);
                    CalcitePlanContext.skipEncoding.set(true);
                  }
                  // triggers the hook
                  OpenSearchRelRunners.run(context, CalciteToolsHelper.optimize(rel, context));
                }
                listener.onResponse(
                    new ExplainResponse(
                        new ExplainResponseNodeV2(logical, physical.get(), javaCode.get())));
              }
            }
          } catch (Exception e) {
            listener.onFailure(e);
          } finally {
            CalcitePlanContext.skipEncoding.remove();
          }
        });
  }

  @Override
  public void execute(
      RelNode rel, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    client.schedule(
        () -> {
          AtomicReference<RelNode> optimizedPlan = new AtomicReference<>();
          try (Hook.Closeable ignored = getOptimizedPlanInHook(optimizedPlan);
              PreparedStatement statement = OpenSearchRelRunners.run(context, rel)) {
            QueryResponse response;
            try (ProfileScope executePhase = ProfileScope.open(MetricName.EXECUTE)) {
              PartialResultResponseListener partialListener =
                  listener instanceof PartialResultResponseListener
                      ? (PartialResultResponseListener) listener
                      : null;
              PartialResultPlan partialPlan =
                  partialListener == null
                      ? PartialResultPlan.NONE
                      : classifyPartialResultPlan(rel, optimizedPlan.get());

              CalciteRootResultMaterializer sourceMaterializer =
                  partialPlan == PartialResultPlan.SOURCE_REPLACE
                      ? createSourceMaterializer(statement, rel.getRowType())
                      : null;
              if (partialPlan == PartialResultPlan.SOURCE_REPLACE && sourceMaterializer == null) {
                partialPlan = PartialResultPlan.NONE;
              }
              if (partialPlan.updateMode != null) {
                partialListener.onPartialResultMode(partialPlan.updateMode);
              }
              PartialResultContext.Scope partialScope =
                  sourceMaterializer == null
                      ? null
                      : PartialResultContext.open(
                          rows ->
                              partialListener.onPartial(
                                  sourceMaterializer.response(
                                      sourceMaterializer.materializeSourceRows(rows))));
              try (partialScope;
                  ResultSet result = statement.executeQuery()) {
                response =
                    buildResultSet(
                        result,
                        rel.getRowType(),
                        context.sysLimit.querySizeLimit(),
                        partialListener,
                        partialPlan);
              }
            }
            listener.onResponse(response);
          } catch (SQLException e) {
            if (isPitContextLimitReached(e)) {
              // reason (title) comes from the wrapped cause's message; keep it short and put the
              // explanation and remedy in details.
              ResourceLimitExceededException pitException =
                  new ResourceLimitExceededException(
                      "Too many open Point-In-Time (PIT) contexts on this node.", e);
              throw ErrorReport.wrap(pitException)
                  .code(ErrorCode.RESOURCE_LIMIT_EXCEEDED)
                  .details(
                      "This query opened a Point-In-Time (PIT) context on each shard and reached"
                          + " the limit set by [search.max_open_pit_context]. Increase that"
                          + " setting.")
                  .build();
            }
            throw new RuntimeException(e);
          }
        });
  }

  private static CalciteRootResultMaterializer createSourceMaterializer(
      PreparedStatement statement, RelDataType rowType) {
    try {
      ResultSetMetaData metaData = statement.getMetaData();
      return metaData == null ? null : new CalciteRootResultMaterializer(metaData, rowType);
    } catch (SQLException e) {
      logger.debug("Calcite JDBC metadata is unavailable before query execution", e);
      return null;
    }
  }

  /**
   * Substring of the error OpenSearch's {@code SearchService} raises when a node has no free PIT
   * contexts. The engine opens a PIT (one reader context per shard) to page over a query it cannot
   * push down -- e.g. a {@code stats} that groups by a text field with no {@code keyword} sub-field
   * -- and a busy node exhausts its per-node budget. The raw failure is an opaque internal message,
   * so it is replaced with an actionable one when this marker appears anywhere in the cause chain.
   */
  private static final String PIT_CONTEXT_LIMIT_MARKER = "too many Point In Time contexts";

  /** Package-private for testing. Walks the cause chain guarding against self-referential loops. */
  static boolean isPitContextLimitReached(Throwable t) {
    for (Throwable cause = t;
        cause != null && cause != cause.getCause();
        cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null && message.contains(PIT_CONTEXT_LIMIT_MARKER)) {
        return true;
      }
    }
    return false;
  }

  private QueryResponse buildResultSet(
      ResultSet resultSet,
      RelDataType rowTypes,
      Integer querySizeLimit,
      PartialResultResponseListener listener,
      PartialResultPlan partialPlan)
      throws SQLException {
    CalciteRootResultMaterializer materializer =
        new CalciteRootResultMaterializer(resultSet.getMetaData(), rowTypes);
    List<ExprValue> values = new ArrayList<>();
    CalciteRootPartialResultCollector rootCollector =
        listener != null && partialPlan.hasRootProducer()
            ? new CalciteRootPartialResultCollector(listener, partialPlan.updateMode, materializer)
            : null;
    while (resultSet.next() && (querySizeLimit == null || values.size() < querySizeLimit)) {
      ExprValue row = materializer.readRow(resultSet);
      values.add(row);
      if (rootCollector != null) {
        rootCollector.onRow(row, values);
      }
    }

    if (rootCollector != null) {
      rootCollector.finish();
    }

    QueryResponse materialized = materializer.response(values);
    List<Column> columns = materialized.getSchema().getColumns();
    // Timewrap post-processing: pivot unpivoted rows into period columns. The pivot is shared with
    // the analytics route (AnalyticsExecutionEngine) so both engines produce identical output.
    if (TimewrapPivot.isTimewrap()) {
      try {
        TimewrapPivot.Result pivoted =
            TimewrapPivot.pivot(
                columns,
                values,
                CalcitePlanContext.timewrapUnitName.get(),
                CalcitePlanContext.timewrapSeries.get());
        columns = pivoted.columns();
        values = pivoted.values();
      } finally {
        CalcitePlanContext.clearTimewrapSignals();
      }
    }

    Schema schema = new Schema(columns);
    QueryResponse response = new QueryResponse(schema, values, null);
    response.setWarnings(CalcitePlanContext.drainWarnings());
    return response;
  }

  private enum PartialResultPlan {
    NONE(null),
    ROOT_APPEND(UpdateMode.APPEND),
    ROOT_REPLACE(UpdateMode.REPLACE),
    SOURCE_REPLACE(UpdateMode.REPLACE);

    private final UpdateMode updateMode;

    PartialResultPlan(UpdateMode updateMode) {
      this.updateMode = updateMode;
    }

    private boolean hasRootProducer() {
      return this == ROOT_APPEND || this == ROOT_REPLACE;
    }
  }

  private static PartialResultPlan classifyPartialResultPlan(
      RelNode logicalPlan, RelNode physicalPlan) {
    if (physicalPlan == null || TimewrapPivot.isTimewrap()) {
      return PartialResultPlan.NONE;
    }
    if (isStablePrefixQuery(logicalPlan, physicalPlan)) {
      return PartialResultPlan.ROOT_APPEND;
    }
    if (supportsCompositePartialResults(physicalPlan)) {
      return PartialResultPlan.ROOT_REPLACE;
    }
    if (supportsAggregationSnapshots(physicalPlan)) {
      return PartialResultPlan.SOURCE_REPLACE;
    }
    return PartialResultPlan.NONE;
  }

  static boolean isStablePrefixQuery(RelNode logicalPlan, RelNode physicalPlan) {
    return !containsBlockingOrAggregation(logicalPlan)
        && !containsBlockingOrAggregation(physicalPlan)
        && supportsStableRootRows(physicalPlan);
  }

  private static boolean containsBlockingOrAggregation(RelNode rel) {
    if (rel == null) {
      return true;
    }
    if (rel instanceof Aggregate
        || isBlockingSort(rel)
        || rel instanceof Window
        || rel instanceof Join
        || rel instanceof SetOp) {
      return true;
    }
    if (rel instanceof TableScan) {
      return false;
    }
    return !(rel instanceof SingleRel && isRowLocalUnary(rel))
        || containsBlockingOrAggregation(rel.getInput(0));
  }

  static boolean supportsStableRootRows(RelNode rel) {
    if (rel instanceof CalciteEnumerableIndexScan scan) {
      return scan.getPushDownContext().getAggSpec() == null;
    }
    return rel instanceof SingleRel
        && isRowLocalUnary(rel)
        && supportsStableRootRows(rel.getInput(0));
  }

  static boolean supportsAggregationSnapshots(RelNode rel) {
    if (!(rel instanceof CalciteEnumerableIndexScan scan)) {
      return false;
    }
    var aggSpec = scan.getPushDownContext().getAggSpec();
    return aggSpec != null && !aggSpec.isCompositeAggregation();
  }

  static boolean supportsCompositePartialResults(RelNode rel) {
    if (rel instanceof CalciteEnumerableIndexScan scan) {
      var aggSpec = scan.getPushDownContext().getAggSpec();
      return aggSpec != null && aggSpec.isCompositeAggregation();
    }
    return rel instanceof SingleRel
        && isRowLocalUnary(rel)
        && supportsCompositePartialResults(rel.getInput(0));
  }

  private static boolean isBlockingSort(RelNode rel) {
    return rel instanceof Sort sort && !sort.getCollation().getFieldCollations().isEmpty();
  }

  private static boolean isRowLocalUnary(RelNode rel) {
    return rel instanceof Project
        || rel instanceof Filter
        || rel instanceof Calc
        || rel instanceof LogicalSystemLimit
        || (rel instanceof Sort && !isBlockingSort(rel));
  }

  /** Registers opensearch-dependent functions */
  private void registerOpenSearchFunctions() {
    Optional<NodeClient> nodeClient = client.getNodeClient();
    if (nodeClient.isPresent()) {
      SqlUserDefinedFunction geoIpFunction =
          new GeoIpFunction(nodeClient.get()).toUDF(BuiltinFunctionName.GEOIP.name());
      PPLFuncImpTable.INSTANCE.registerExternalOperator(BuiltinFunctionName.GEOIP, geoIpFunction);
      OperatorTable.addOperator(BuiltinFunctionName.GEOIP.name(), geoIpFunction);
    } else {
      logger.info(
          "Function [GEOIP] not registered: incompatible client type {}",
          client.getClass().getName());
    }

    SqlUserDefinedAggFunction approxDistinctCountFunction =
        UserDefinedFunctionUtils.createUserDefinedAggFunction(
            DistinctCountApproxAggFunction.class,
            BuiltinFunctionName.DISTINCT_COUNT_APPROX.name(),
            ReturnTypes.BIGINT_FORCE_NULLABLE,
            null);
    PPLFuncImpTable.INSTANCE.registerExternalAggOperator(
        BuiltinFunctionName.DISTINCT_COUNT_APPROX, approxDistinctCountFunction);
    OperatorTable.addOperator(
        BuiltinFunctionName.DISTINCT_COUNT_APPROX.name(), approxDistinctCountFunction);

    // Note: GraphLookup is now implemented as a custom RelNode (LogicalGraphLookup)
    // instead of a UDF, so no registration is needed here.
  }

  /**
   * Dynamic SqlOperatorTable that allows adding operators after initialization. Similar to
   * PPLBuiltinOperator.instance() or SqlStdOperatorTable.instance().
   */
  public static class OperatorTable extends ListSqlOperatorTable {
    private static final Supplier<OperatorTable> INSTANCE =
        Suppliers.memoize(() -> (OperatorTable) new OperatorTable().init());
    // Use map instead of list to avoid duplicated elements if the class is initialized multiple
    // times
    private static final Map<String, SqlOperator> operators = new ConcurrentHashMap<>();

    public static SqlOperatorTable instance() {
      return INSTANCE.get();
    }

    private ListSqlOperatorTable init() {
      setOperators(buildIndex(operators.values()));
      return this;
    }

    public static synchronized void addOperator(String name, SqlOperator operator) {
      operators.put(name, operator);
    }
  }
}
