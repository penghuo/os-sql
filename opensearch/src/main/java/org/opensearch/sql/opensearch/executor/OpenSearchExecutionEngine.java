/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.statement.Explain.ExplainFormat;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.Explain;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.executor.protector.ResourceMonitorPlan;
import org.opensearch.sql.opensearch.functions.DistinctCountApproxAggFunction;
import org.opensearch.sql.opensearch.functions.GeoIpFunction;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.util.JdbcOpenSearchDataTypeConvertor;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.TableScanOperator;

/** OpenSearch execution engine implementation. */
public class OpenSearchExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(OpenSearchExecutionEngine.class);

  private final OpenSearchClient client;

  private final ExecutionProtector executionProtector;
  private final PlanSerializer planSerializer;
  private final ResourceMonitor resourceMonitor;

  public OpenSearchExecutionEngine(
      OpenSearchClient client,
      ExecutionProtector executionProtector,
      PlanSerializer planSerializer,
      ResourceMonitor resourceMonitor) {
    this.client = client;
    this.executionProtector = executionProtector;
    this.planSerializer = planSerializer;
    this.resourceMonitor = resourceMonitor;
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

  private Hook.Closeable getCodegenInHook(AtomicReference<String> codegen) {
    return Hook.JAVA_PLAN.addThread(
        obj -> {
          codegen.set((String) obj);
        });
  }

  @Override
  public void explain(
      RelNode rel,
      ExplainFormat format,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    client.schedule(
        () -> {
          try {
            if (format == ExplainFormat.SIMPLE) {
              String logical = RelOptUtil.toString(rel, SqlExplainLevel.NO_ATTRIBUTES);
              listener.onResponse(
                  new ExplainResponse(new ExplainResponseNodeV2(logical, null, null)));
            } else {
              SqlExplainLevel level =
                  format == ExplainFormat.COST
                      ? SqlExplainLevel.ALL_ATTRIBUTES
                      : SqlExplainLevel.EXPPLAN_ATTRIBUTES;
              String logical = RelOptUtil.toString(rel, level);
              AtomicReference<String> physical = new AtomicReference<>();
              AtomicReference<String> javaCode = new AtomicReference<>();
              try (Hook.Closeable closeable = getPhysicalPlanInHook(physical, level)) {
                if (format == ExplainFormat.EXTENDED) {
                  getCodegenInHook(javaCode);
                  CalcitePlanContext.skipEncoding.set(true);
                }
                // triggers the hook
                AccessController.doPrivileged(
                    (PrivilegedAction<PreparedStatement>)
                        () -> OpenSearchRelRunners.run(context, rel));
              }
              listener.onResponse(
                  new ExplainResponse(
                      new ExplainResponseNodeV2(logical, physical.get(), javaCode.get())));
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
    try {
      ensureMemoryHealthy(new AtomicBoolean(false));
    } catch (IllegalStateException e) {
      listener.onFailure(e);
      return;
    }
    client.schedule(
        () -> {
          try {
            AccessController.doPrivileged(
                (PrivilegedAction<Void>)
                    () -> {
                      AtomicBoolean memoryAbort = new AtomicBoolean(false);
                      Thread watchdog = null;
                      Thread executionThread = Thread.currentThread();
                      try (PreparedStatement statement = OpenSearchRelRunners.run(context, rel)) {
                        watchdog = startCalciteWatchdog(statement, executionThread, memoryAbort);
                        try (ResultSet result = statement.executeQuery()) {
                          buildResultSet(
                              result,
                              rel.getRowType(),
                              context.querySizeLimit,
                              memoryAbort,
                              listener);
                        }
                      } catch (SQLException e) {
                        if (memoryAbort.get()) {
                          throw new IllegalStateException(
                              "insufficient resources to run the query, quit.", e);
                        }
                        throw new RuntimeException(e);
                      } finally {
                        if (watchdog != null) {
                          memoryAbort.set(true);
                          watchdog.interrupt();
                        }
                      }
                      return null;
                    });
          } catch (IllegalStateException e) {
            listener.onFailure(e);
          } catch (RuntimeException e) {
            listener.onFailure(e);
          }
        });
  }

  private void buildResultSet(
      ResultSet resultSet,
      RelDataType rowTypes,
      Integer querySizeLimit,
      AtomicBoolean memoryAbort,
      ResponseListener<QueryResponse> listener)
      throws SQLException {
    // Get the ResultSet metadata to know about columns
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<RelDataType> fieldTypes =
        rowTypes.getFieldList().stream().map(RelDataTypeField::getType).toList();
    List<ExprValue> values = new ArrayList<>();
    long rowCount = 0L;
    // Iterate through the ResultSet
    while (resultSet.next() && (querySizeLimit == null || values.size() < querySizeLimit)) {
      ensureMemoryHealthy(memoryAbort);
      Map<String, ExprValue> row = new LinkedHashMap<String, ExprValue>();
      // Loop through each column
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        int sqlType = metaData.getColumnType(i);
        RelDataType fieldType = fieldTypes.get(i - 1);
        ExprValue exprValue =
            JdbcOpenSearchDataTypeConvertor.getExprValueFromSqlType(
                resultSet, i, sqlType, fieldType, columnName);
        row.put(columnName, exprValue);
      }
      values.add(ExprTupleValue.fromExprValueMap(row));
      rowCount++;
      if (rowCount % ResourceMonitorPlan.NUMBER_OF_NEXT_CALL_TO_CHECK == 0) {
        ensureMemoryHealthy(memoryAbort);
      }
    }

    List<Column> columns = new ArrayList<>(metaData.getColumnCount());
    for (int i = 1; i <= columnCount; ++i) {
      String columnName = metaData.getColumnName(i);
      RelDataType fieldType = fieldTypes.get(i - 1);
      // TODO: Correct this after fixing issue github.com/opensearch-project/sql/issues/3751
      //  The element type of struct and array is currently set to ANY.
      //  We set them using the runtime type as a workaround.
      ExprType exprType;
      if (fieldType.getSqlTypeName() == SqlTypeName.ANY) {
        if (!values.isEmpty()) {
          exprType = values.getFirst().tupleValue().get(columnName).type();
        } else {
          // Using UNDEFINED instead of UNKNOWN to avoid throwing exception
          exprType = ExprCoreType.UNDEFINED;
        }
      } else {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType);
      }
      columns.add(new Column(columnName, null, exprType));
    }
    Schema schema = new Schema(columns);
    QueryResponse response = new QueryResponse(schema, values, null);
    listener.onResponse(response);
  }

  private Thread startCalciteWatchdog(
      PreparedStatement statement, Thread executionThread, AtomicBoolean memoryAbort) {
    Thread watchdog =
        new Thread(
            () -> {
              while (!memoryAbort.get() && !Thread.currentThread().isInterrupted()) {
                try {
                  ensureMemoryHealthy(memoryAbort);
                } catch (IllegalStateException e) {
                  logger.warn("Calcite query canceled due to memory pressure");
                  try {
                    statement.cancel();
                  } catch (SQLException ex) {
                    logger.debug("Failed to cancel statement after memory trigger", ex);
                  }
                  try {
                    executionThread.stop(e);
                  } catch (Throwable stopError) {
                    logger.debug("Failed to stop execution thread gracefully", stopError);
                  }
                  break;
                }
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
            },
            "calcite-memory-watchdog");
    watchdog.setDaemon(true);
    watchdog.start();
    return watchdog;
  }

  private void ensureMemoryHealthy(AtomicBoolean memoryAbort) {
    try {
      if (!resourceMonitor.isHealthy()) {
        memoryAbort.set(true);
        throw new IllegalStateException("insufficient resources to run the query, quit.");
      }
    } catch (RuntimeException e) {
      memoryAbort.set(true);
      throw new IllegalStateException("insufficient resources to run the query, quit.", e);
    }
  }

  /** Registers opensearch-dependent functions */
  private void registerOpenSearchFunctions() {
    if (client instanceof OpenSearchNodeClient) {
      SqlUserDefinedFunction geoIpFunction =
          new GeoIpFunction(client.getNodeClient()).toUDF("GEOIP");
      PPLFuncImpTable.INSTANCE.registerExternalOperator(BuiltinFunctionName.GEOIP, geoIpFunction);
    } else {
      logger.info(
          "Function [GEOIP] not registered: incompatible client type {}",
          client.getClass().getName());
    }

    SqlUserDefinedAggFunction approxDistinctCountFunction =
        UserDefinedFunctionUtils.createUserDefinedAggFunction(
            DistinctCountApproxAggFunction.class,
            "APPROX_DISTINCT_COUNT",
            ReturnTypes.BIGINT_FORCE_NULLABLE,
            null);
    PPLFuncImpTable.INSTANCE.registerExternalAggOperator(
        BuiltinFunctionName.DISTINCT_COUNT_APPROX, approxDistinctCountFunction);
  }
}
