/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.tools.RelRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.opensearch.dqe.exchange.Exchange;
import org.opensearch.sql.opensearch.dqe.exchange.ShardResult;
import org.opensearch.sql.opensearch.dqe.serde.RelNodeSerializer;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Coordinator-side executor for distributed query plans. Orchestrates plan execution by:
 *
 * <ol>
 *   <li>For each Exchange, serializing the shard plan via {@link RelNodeSerializer}
 *   <li>Resolving target shards via {@link ShardRoutingResolver}
 *   <li>Dispatching plan fragments to each shard via {@link ShardQueryDispatcher}
 *   <li>Collecting responses and feeding them into Exchange merge logic
 *   <li>Executing the coordinator plan which reads merged results from Exchanges
 * </ol>
 *
 * <p>Fail-fast: if any shard returns an error, the query fails immediately.
 */
public class DistributedExecutor {

  private static final Logger LOG = LogManager.getLogger(DistributedExecutor.class);

  private final ShardRoutingResolver resolver;
  private final ShardQueryDispatcher dispatcher;

  public DistributedExecutor(ShardRoutingResolver resolver, ShardQueryDispatcher dispatcher) {
    this.resolver = resolver;
    this.dispatcher = dispatcher;
  }

  /**
   * Execute a distributed plan. For each Exchange, dispatches the shard plan to all relevant shards,
   * collects results, then builds a QueryResponse from the coordinator plan.
   *
   * @param plan the distributed plan from PlanSplitter
   * @param connection Calcite JDBC connection for executing coordinator plans (may be null for fast
   *     path)
   * @param listener callback for the query response
   */
  public void execute(
      DistributedPlan plan, Connection connection, ResponseListener<QueryResponse> listener) {
    try {
      // Phase 1: Execute all exchanges (shard dispatch + collect)
      for (Exchange exchange : plan.getExchanges()) {
        executeExchange(exchange);
      }

      // Phase 2: Build QueryResponse from the coordinator plan
      QueryResponse response = buildResponse(plan, connection);
      listener.onResponse(response);

    } catch (Exception e) {
      listener.onFailure(e);
    } finally {
      closeQuietly(connection);
    }
  }

  /**
   * Executes a single exchange: serialize, resolve shards, dispatch, collect, merge.
   *
   * @throws Exception if serialization fails, no shards found, or any shard returns an error
   */
  private void executeExchange(Exchange exchange) throws Exception {
    RelNode shardPlan = exchange.getShardPlan();

    // Step 1: Extract index name from the shard plan
    String indexName = extractIndexName(shardPlan);

    // Step 2: Serialize the shard plan
    String planJson;
    try {
      planJson = RelNodeSerializer.serialize(shardPlan);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to serialize shard plan for index [" + indexName + "]: " + e.getMessage(), e);
    }

    // Step 3: Resolve shard IDs
    List<Integer> shardIds = resolver.resolve(indexName);

    // Step 4: Dispatch to all shards and collect results
    List<ShardResult> shardResults = dispatchAndCollect(planJson, indexName, shardIds, exchange);

    // Step 5: Feed results into the exchange for merging
    exchange.setShardResults(shardResults);
  }

  /**
   * Dispatches the plan to all shards in parallel and waits for all responses. Fail-fast: if any
   * shard returns an error, throws immediately.
   */
  private List<ShardResult> dispatchAndCollect(
      String planJson, String indexName, List<Integer> shardIds, Exchange exchange)
      throws Exception {

    int shardCount = shardIds.size();
    CountDownLatch latch = new CountDownLatch(shardCount);
    AtomicReference<Exception> firstError = new AtomicReference<>();

    @SuppressWarnings("unchecked")
    ShardResult[] results = new ShardResult[shardCount];

    for (int i = 0; i < shardCount; i++) {
      final int idx = i;
      int shardId = shardIds.get(i);

      dispatcher.dispatch(
          planJson,
          indexName,
          shardId,
          new ActionListener<ShardQueryDispatcher.ShardResponse>() {
            @Override
            public void onResponse(ShardQueryDispatcher.ShardResponse response) {
              try {
                if (response.hasError()) {
                  firstError.compareAndSet(
                      null,
                      new RuntimeException(
                          "Shard ["
                              + shardId
                              + "] of index ["
                              + indexName
                              + "] returned error: "
                              + response.getError().getMessage(),
                          response.getError()));
                } else {
                  results[idx] =
                      new ShardResult(shardId, response.getRows(), exchange.getRowType());
                }
              } finally {
                latch.countDown();
              }
            }

            @Override
            public void onFailure(Exception e) {
              firstError.compareAndSet(
                  null,
                  new RuntimeException(
                      "Transport error for shard ["
                          + shardId
                          + "] of index ["
                          + indexName
                          + "]: "
                          + e.getMessage(),
                      e));
              latch.countDown();
            }
          });
    }

    // Wait for all shards
    latch.await();

    // Fail-fast check
    Exception error = firstError.get();
    if (error != null) {
      throw error;
    }

    // Collect non-null results
    List<ShardResult> resultList = new ArrayList<>(shardCount);
    for (ShardResult result : results) {
      if (result != null) {
        resultList.add(result);
      }
    }
    return resultList;
  }

  private static void closeQuietly(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception e) {
        LOG.debug("Failed to close connection", e);
      }
    }
  }

  /**
   * Two-path response builder. Fast path: if the coordinator plan is just an ExchangeLeaf, read
   * directly from the exchanges. Slow path: execute the coordinator plan via Calcite RelRunner.
   */
  private QueryResponse buildResponse(DistributedPlan plan, Connection connection)
      throws Exception {
    RelNode coordinatorPlan = plan.getCoordinatorPlan();

    // Fast path: coordinator IS ExchangeLeaf — read from exchanges directly
    if (coordinatorPlan instanceof PlanSplitter.ExchangeLeaf) {
      return buildResponseDirectRead(plan);
    }

    // Slow path: execute coordinator plan via Calcite
    return executeCoordinatorPlan(coordinatorPlan, connection);
  }

  /**
   * Executes the coordinator plan via Calcite's RelRunner. Used when the coordinator plan has
   * operators above the ExchangeLeaf (e.g., Sort, Project).
   */
  private QueryResponse executeCoordinatorPlan(RelNode plan, Connection connection)
      throws Exception {
    RelRunner runner = connection.unwrap(RelRunner.class);
    try (PreparedStatement stmt = runner.prepareStatement(plan);
        ResultSet rs = stmt.executeQuery()) {
      return OpenSearchExecutionEngine.buildResultSet(rs, plan.getRowType(), null);
    }
  }

  /**
   * Builds a QueryResponse by reading directly from exchanges. Used when the coordinator plan is
   * just an ExchangeLeaf (no additional coordinator-side operators needed).
   *
   * <p>Each exchange (ConcatExchange, MergeSortExchange, MergeAggregateExchange) already produces
   * the correct final merged results via its {@code scan()} method. The coordinator plan's row type
   * is used for schema and type conversion.
   */
  private QueryResponse buildResponseDirectRead(DistributedPlan plan) {
    RelNode coordinatorPlan = plan.getCoordinatorPlan();
    List<Exchange> exchanges = plan.getExchanges();

    // Get the output row type from the coordinator plan
    RelDataType rowType = coordinatorPlan.getRowType();

    // Build schema
    List<Column> columns = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      ExprType exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
      columns.add(new Column(field.getName(), null, exprType));
    }
    Schema schema = new Schema(columns);

    // Pre-compute column types for type-aware conversion (e.g., timestamps, dates)
    List<RelDataTypeField> fields = rowType.getFieldList();
    ExprType[] columnTypes = new ExprType[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      columnTypes[i] = OpenSearchTypeFactory.convertRelDataTypeToExprType(fields.get(i).getType());
    }

    // Read merged rows from all exchanges. Each exchange's scan() returns the fully merged
    // result: ConcatExchange concatenates, MergeSortExchange merge-sorts with limit,
    // MergeAggregateExchange groups and applies merge functions.
    List<ExprValue> resultValues = new ArrayList<>();
    List<String> fieldNames = rowType.getFieldNames();
    for (Exchange exchange : exchanges) {
      var iterator = exchange.scan();
      while (iterator.hasNext()) {
        Object[] row = iterator.next();
        Map<String, ExprValue> rowMap = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size() && i < row.length; i++) {
          Object value = coerceForType(row[i], columnTypes[i]);
          rowMap.put(fieldNames.get(i), ExprValueUtils.fromObjectValue(value, columnTypes[i]));
        }
        resultValues.add(ExprTupleValue.fromExprValueMap(rowMap));
      }
    }

    return new QueryResponse(schema, resultValues, null);
  }

  /**
   * Coerces a raw value to the format expected by {@link ExprValueUtils#fromObjectValue(Object,
   * ExprType)}. For temporal types, converts epoch millis (Long) to formatted String since
   * ExprTimestampValue/ExprDateValue/ExprTimeValue constructors expect String input.
   */
  private static Object coerceForType(Object value, ExprType type) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return value;
    }
    if (type == ExprCoreType.TIMESTAMP && value instanceof Long) {
      Instant instant = Instant.ofEpochMilli((Long) value);
      return DateTimeFormatter.ISO_LOCAL_DATE_TIME
          .format(instant.atZone(ZoneOffset.UTC).toLocalDateTime());
    }
    if (type == ExprCoreType.DATE && value instanceof Long) {
      LocalDate date = Instant.ofEpochMilli((Long) value).atZone(ZoneOffset.UTC).toLocalDate();
      return date.toString();
    }
    if (type == ExprCoreType.DATE && value instanceof Integer) {
      // Calcite sometimes returns date as days since epoch
      LocalDate date = LocalDate.ofEpochDay((Integer) value);
      return date.toString();
    }
    if (type == ExprCoreType.TIME && value instanceof Long) {
      LocalTime time = Instant.ofEpochMilli((Long) value).atZone(ZoneOffset.UTC).toLocalTime();
      return time.toString();
    }
    if (type == ExprCoreType.TIME && value instanceof Integer) {
      // Calcite sometimes returns time as millis since midnight
      LocalTime time = LocalTime.ofNanoOfDay((long) (Integer) value * 1_000_000L);
      return time.toString();
    }
    return value;
  }

  /**
   * Extracts the OpenSearch index name from a plan tree by finding the AbstractCalciteIndexScan
   * leaf.
   *
   * @param plan the RelNode plan tree
   * @return the index name
   * @throws IllegalStateException if no scan node is found
   */
  static String extractIndexName(RelNode plan) {
    if (plan instanceof AbstractCalciteIndexScan) {
      AbstractCalciteIndexScan scan = (AbstractCalciteIndexScan) plan;
      List<String> qualifiedName = scan.getTable().getQualifiedName();
      // The qualified name is typically [schemaName, tableName] or just [tableName]
      return qualifiedName.get(qualifiedName.size() - 1);
    }
    for (RelNode child : plan.getInputs()) {
      try {
        return extractIndexName(child);
      } catch (IllegalStateException e) {
        // Continue searching other children
      }
    }
    throw new IllegalStateException(
        "No AbstractCalciteIndexScan found in plan tree. Cannot determine index name.");
  }
}
