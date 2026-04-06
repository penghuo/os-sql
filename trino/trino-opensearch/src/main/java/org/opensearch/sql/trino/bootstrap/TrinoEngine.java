/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.bootstrap;

import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.SqlTaskManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.hadoop.HadoopNative;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.security.Identity;
import io.trino.spi.type.Type;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import java.io.Closeable;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.temporal.Temporal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Embedded Trino SQL execution engine using the real Trino distributed query engine.
 *
 * <p>Uses {@link DistributedQueryRunner} for intra-JVM distributed execution with multiple
 * in-process Trino nodes (coordinator + workers). This provides real distributed query planning,
 * hash-partitioned shuffles, and parallel GROUP BY/JOIN execution.
 *
 * <p>For cross-OpenSearch-node distributed execution, the transport-based communication layer
 * ({@code TransportRemoteTask}, {@code TransportExchangeClient}) handles inter-node coordination
 * via OpenSearch TransportActions. These are registered in {@code SQLPlugin.getActions()}.
 *
 * <p>All external access is through the {@code /_plugins/_trino_sql/v1/statement} REST endpoint.
 */
public class TrinoEngine implements Closeable {

  private static final Logger LOG = LogManager.getLogger(TrinoEngine.class);

  private final DistributedQueryRunner queryRunner;

  /**
   * Create a TrinoEngine with default temp warehouse directories.
   */
  public TrinoEngine() {
    this("");
  }

  /**
   * Create a TrinoEngine with a configurable Iceberg warehouse path.
   *
   * @param icebergWarehouse path to Iceberg warehouse directory. If empty, checks the system
   *     property {@code trino.iceberg.warehouse} as a fallback. If both are empty, a temp
   *     directory is created. The path should match the warehouse used by external tools
   *     (e.g. Spark) that create Iceberg tables.
   */
  public TrinoEngine(String icebergWarehouse) {
    // Allow system property override (useful for ./gradlew run)
    if (icebergWarehouse == null || icebergWarehouse.isEmpty()) {
      icebergWarehouse = System.getProperty("trino.iceberg.warehouse", "");
    }
    LOG.info("Initializing Trino engine via DistributedQueryRunner");
    try {
      initHadoopNative();

      Session session = createDefaultSession();
      // Configure Trino memory adaptively based on available heap.
      // Use 70% of heap for queries, 15% for headroom, rest for OS overhead.
      long maxHeap = Runtime.getRuntime().maxMemory();
      long queryMem = (long) (maxHeap * 0.70);
      long headroom = (long) (maxHeap * 0.15);
      String queryMemStr = (queryMem / (1024 * 1024)) + "MB";
      String headroomStr = (headroom / (1024 * 1024)) + "MB";
      LOG.info("Trino memory config: heap={}MB, queryMem={}, headroom={}",
          maxHeap / (1024 * 1024), queryMemStr, headroomStr);

      // Scale task concurrency to available processors for analytical workloads.
      int processors = Runtime.getRuntime().availableProcessors();
      int taskConcurrency = Math.max(4, processors);
      LOG.info("Trino parallelism config: processors={}, taskConcurrency={}",
          processors, taskConcurrency);

      // Use single coordinator node. Split-level distribution across OpenSearch cluster
      // nodes is handled by TransportRemoteTaskFactory + OpenSearchNodeManager, which are
      // patched into the coordinator via TransportDistributionPatcher after build.
      // Hash partitioning within the single node still provides parallelism via task_concurrency.
      int nodeCount = 1;
      LOG.info("Trino node count: {} (single coordinator, transport distribution)", nodeCount);
      DistributedQueryRunner runner =
          DistributedQueryRunner.builder(session)
              .setNodeCount(nodeCount)
              .addExtraProperty("query.max-memory-per-node", queryMemStr)
              .addExtraProperty("query.max-memory", queryMemStr)
              .addExtraProperty("memory.heap-headroom-per-node", headroomStr)
              .build();
      runner.installPlugin(new TpchPlugin());
      runner.createCatalog("tpch", "tpch", Map.of());
      runner.installPlugin(new MemoryPlugin());
      runner.createCatalog("memory", "memory", Map.of());

      // Iceberg catalog: use configured warehouse or create temp directory.
      runner.installPlugin(new IcebergPlugin());
      Path warehouseDir;
      String catalogType;
      if (icebergWarehouse != null && !icebergWarehouse.isEmpty()) {
        warehouseDir = Path.of(icebergWarehouse);
        if (!Files.exists(warehouseDir)) {
          Files.createDirectories(warehouseDir);
        }
        catalogType = "TESTING_FILE_METASTORE";
        LOG.info("Using configured Iceberg warehouse: {}", warehouseDir);
      } else {
        warehouseDir = Files.createTempDirectory("trino-iceberg-warehouse");
        catalogType = "TESTING_FILE_METASTORE";
        LOG.info("Using temp Iceberg warehouse: {} (TESTING_FILE_METASTORE)", warehouseDir);
      }
      runner.createCatalog(
          "iceberg",
          "iceberg",
          Map.of(
              "iceberg.catalog.type", catalogType,
              "hive.metastore.catalog.dir",
              warehouseDir.toUri().toString()));
      // Hive catalog for reading raw Parquet files from local filesystem.
      runner.installPlugin(new HivePlugin());
      Path hiveWarehouse = Files.createTempDirectory("trino-hive-warehouse");
      runner.createCatalog(
          "hive",
          "hive",
          Map.of(
              "hive.metastore", "file",
              "hive.metastore.catalog.dir",
              hiveWarehouse.toUri().toString()));
      this.queryRunner = runner;
      LOG.info(
          "Trino engine initialized with TPCH, Memory, Iceberg, and Hive catalogs"
              + " (iceberg warehouse: {}, hive warehouse: {})",
          warehouseDir, hiveWarehouse);
    } catch (Exception e) {
      LOG.error("Failed to initialize Trino engine", e);
      throw new RuntimeException("Failed to initialize Trino engine", e);
    }
  }

  @SuppressWarnings("JavaReflectionMemberAccess")
  private static void initHadoopNative() {
    try {
      HadoopNative.requireHadoopNative();
      LOG.info("Hadoop native library loaded successfully");
    } catch (Throwable t) {
      LOG.warn(
          "Hadoop native library failed to load ({}); "
              + "bypassing via reflection for shadow-jar compatibility",
          t.getMessage());
      try {
        Field loadedField = HadoopNative.class.getDeclaredField("loaded");
        loadedField.setAccessible(true);
        loadedField.setBoolean(null, true);
        Field errorField = HadoopNative.class.getDeclaredField("error");
        errorField.setAccessible(true);
        errorField.set(null, null);
        LOG.info("Hadoop native library bypass applied via reflection");
      } catch (Exception reflectionEx) {
        LOG.warn("Failed to bypass Hadoop native library check", reflectionEx);
      }
    }
  }

  private static Session createDefaultSession() {
    SessionPropertyManager spm =
        new SessionPropertyManager(Collections.emptySet(), CatalogServiceProvider.fail());
    Identity identity = Identity.ofUser("opensearch");
    return Session.builder(spm)
        .setIdentity(identity)
        .setOriginalIdentity(identity)
        .setSource("opensearch-sql-plugin")
        .setQueryId(QueryId.valueOf("init"))
        .setCatalog("tpch")
        .setSchema("tiny")
        .build();
  }

  public String executeAndSerializeJson(String sql) {
    return executeAndSerializeJson(sql, null, null);
  }

  public String executeAndSerializeJson(String sql, String catalog, String schema) {
    String queryId = UUID.randomUUID().toString().replace("-", "");
    LOG.debug("TrinoEngine executing query [{}] with id [{}]", sql, queryId);

    try {
      Session session = createSessionWithCatalogSchema(catalog, schema);
      MaterializedResult result = queryRunner.execute(session, sql);
      return serializeToTrinoJson(queryId, result);
    } catch (Exception e) {
      LOG.error("Query execution failed: {}", e.getMessage(), e);
      return serializeErrorJson(queryId, e);
    }
  }

  private Session createSessionWithCatalogSchema(String catalog, String schema) {
    SessionPropertyManager spm = queryRunner.getSessionPropertyManager();
    Identity identity = Identity.ofUser("opensearch");
    int taskConcurrency = Runtime.getRuntime().availableProcessors();
    Session.SessionBuilder builder =
        Session.builder(spm)
            .setIdentity(identity)
            .setOriginalIdentity(identity)
            .setSource("opensearch-sql-plugin")
            .setQueryId(QueryId.valueOf(UUID.randomUUID().toString().replace("-", "")))
            .setSystemProperty("task_concurrency", String.valueOf(taskConcurrency))
            .setSystemProperty("dictionary_aggregation", "true")
            .setSystemProperty("max_hash_partition_count", String.valueOf(taskConcurrency))
            .setSystemProperty("min_hash_partition_count", String.valueOf(taskConcurrency))
            .setCatalogSessionProperty("iceberg", "parquet_max_read_block_row_count", "65536")
            .setCatalogSessionProperty("iceberg", "parquet_max_read_block_size", "64MB");
    builder.setCatalog(catalog != null ? catalog : "tpch");
    builder.setSchema(schema != null ? schema : "tiny");
    return builder.build();
  }

  /** Get the underlying query runner for access to internal components. */
  public DistributedQueryRunner getQueryRunner() {
    return queryRunner;
  }

  /**
   * Get the coordinator's SqlTaskManager. Used to back the OpenSearchSqlTaskManager so that
   * incoming transport requests (from remote coordinators) are handled by the local Trino engine.
   */
  public SqlTaskManager getCoordinatorTaskManager() {
    return queryRunner.getCoordinator().getTaskManager();
  }

  /**
   * Phase 1: Initialize the TrinoServiceHolder with this engine and SqlTaskManager. The
   * OpenSearchNodeManager and split-level distribution patching are deferred to Phase 2
   * ({@link #enableSplitLevelDistribution}) because the ClusterState and TransportService are not
   * yet available during createComponents().
   */
  public void initializeTransportServices() {
    org.opensearch.sql.trino.plugin.TrinoServiceHolder.initializeWithEngine(this);

    // Store the Trino HTTP URL for exchange — remote nodes need this to fetch pages
    java.net.URI trinoHttpUrl = queryRunner.getCoordinator().getBaseUrl();
    org.opensearch.sql.trino.plugin.TrinoServiceHolder.getInstance()
        .setTrinoHttpUrl(trinoHttpUrl);
    LOG.info("Trino transport services initialized (phase 1) — HTTP URL: {}", trinoHttpUrl);
  }

  /**
   * Phase 2: Set up OpenSearchNodeManager and enable split-level distribution. Called after the
   * cluster is fully started and TransportService is available (triggered by transport action
   * @Inject).
   *
   * @param clusterService OpenSearch cluster service
   * @param transportService OpenSearch transport service
   */
  public void enableSplitLevelDistribution(
      org.opensearch.cluster.service.ClusterService clusterService,
      org.opensearch.transport.TransportService transportService) {
    try {
      org.opensearch.cluster.node.DiscoveryNode localNode = clusterService.localNode();
      java.net.URI trinoHttpUrl = queryRunner.getCoordinator().getBaseUrl();

      org.opensearch.sql.trino.node.OpenSearchNodeManager nodeManager =
          new org.opensearch.sql.trino.node.OpenSearchNodeManager(localNode);
      // Register the Trino HTTP URL BEFORE rebuilding nodes
      nodeManager.registerTrinoHttpUrl(localNode.getId(), trinoHttpUrl);
      // Rebuild active nodes from current cluster state (now with correct HTTP URLs)
      org.opensearch.cluster.ClusterState state = clusterService.state();
      nodeManager.clusterChanged(
          new org.opensearch.cluster.ClusterChangedEvent("trino-init", state, state));
      clusterService.addListener(nodeManager);

      org.opensearch.sql.trino.plugin.TrinoServiceHolder holder =
          org.opensearch.sql.trino.plugin.TrinoServiceHolder.getInstance();
      holder.setNodeManager(nodeManager);

      // Patch the coordinator's Guice-injected components for transport-based distribution
      org.opensearch.sql.trino.transport.TransportDistributionPatcher.patch(
          queryRunner, transportService, nodeManager, holder.getCodec());

      // Broadcast this node's Trino HTTP URL to all other cluster nodes so they can
      // construct correct InternalNode URIs for the exchange mechanism.
      // Also query remote nodes for their HTTP URLs (they may have started before us).
      broadcastTrinoHttpUrl(clusterService, transportService, localNode, trinoHttpUrl);

      LOG.info("Split-level distribution enabled — NodeManager and RemoteTaskFactory patched. "
          + "Local node: {}, HTTP URL: {}", localNode.getName(), trinoHttpUrl);
    } catch (Exception e) {
      LOG.warn("Split-level distribution not enabled: {}", e.getMessage(), e);
    }
  }

  private void broadcastTrinoHttpUrl(
      org.opensearch.cluster.service.ClusterService clusterService,
      org.opensearch.transport.TransportService transportService,
      org.opensearch.cluster.node.DiscoveryNode localNode,
      java.net.URI trinoHttpUrl) {
    org.opensearch.cluster.node.DiscoveryNodes nodes = clusterService.state().nodes();
    for (org.opensearch.cluster.node.DiscoveryNode node : nodes) {
      if (node.getId().equals(localNode.getId())) {
        continue; // Skip self — already registered locally
      }
      org.opensearch.sql.trino.transport.TrinoNodeRegisterRequest request =
          new org.opensearch.sql.trino.transport.TrinoNodeRegisterRequest(
              localNode.getId(), trinoHttpUrl.toString());
      transportService.sendRequest(
          node,
          org.opensearch.sql.trino.transport.TrinoNodeRegisterAction.NAME,
          request,
          new org.opensearch.transport.TransportResponseHandler<
              org.opensearch.sql.trino.transport.TrinoNodeRegisterResponse>() {
            @Override
            public org.opensearch.sql.trino.transport.TrinoNodeRegisterResponse read(
                org.opensearch.core.common.io.stream.StreamInput in)
                throws java.io.IOException {
              return new org.opensearch.sql.trino.transport.TrinoNodeRegisterResponse(in);
            }

            @Override
            public void handleResponse(
                org.opensearch.sql.trino.transport.TrinoNodeRegisterResponse response) {
              LOG.info("Registered Trino HTTP URL with node {}: success={}",
                  node.getName(), response.isSuccess());
              // Bidirectional: register the remote node's URL in our NodeManager
              if (response.isSuccess()
                  && !response.getReceiverTrinoHttpUrl().isEmpty()) {
                org.opensearch.sql.trino.plugin.TrinoServiceHolder holder =
                    org.opensearch.sql.trino.plugin.TrinoServiceHolder.getInstance();
                if (holder.getNodeManager() != null) {
                  holder.getNodeManager().registerTrinoHttpUrl(
                      response.getReceiverNodeId(),
                      java.net.URI.create(response.getReceiverTrinoHttpUrl()));
                  LOG.info("Received remote Trino HTTP URL: node={}, url={}",
                      response.getReceiverNodeId(), response.getReceiverTrinoHttpUrl());
                }
              }
            }

            @Override
            public void handleException(org.opensearch.transport.TransportException exp) {
              LOG.warn("Failed to register Trino HTTP URL with node {}: {}",
                  node.getName(), exp.getMessage());
            }

            @Override
            public String executor() {
              return org.opensearch.threadpool.ThreadPool.Names.SAME;
            }
          });
    }
  }

  private String serializeToTrinoJson(String queryId, MaterializedResult result) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"id\":\"").append(queryId).append("\",");

    List<String> columnNames;
    try {
      columnNames = result.getColumnNames();
    } catch (IllegalStateException e) {
      columnNames = null;
    }
    List<? extends Type> types = result.getTypes();
    sb.append("\"columns\":[");
    for (int i = 0; i < types.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      String colName =
          (columnNames != null && i < columnNames.size()) ? columnNames.get(i) : "_col" + i;
      Type type = types.get(i);
      String typeName = type.getDisplayName();
      String rawType = type.getBaseName();

      sb.append("{\"name\":\"").append(escapeJson(colName)).append("\",");
      sb.append("\"type\":\"").append(escapeJson(typeName)).append("\",");
      sb.append("\"typeSignature\":{\"rawType\":\"").append(escapeJson(rawType)).append("\"}}");
    }
    sb.append("],");

    sb.append("\"data\":[");
    List<MaterializedRow> rows = result.getMaterializedRows();
    for (int r = 0; r < rows.size(); r++) {
      if (r > 0) {
        sb.append(",");
      }
      sb.append("[");
      List<Object> fields = rows.get(r).getFields();
      for (int c = 0; c < fields.size(); c++) {
        if (c > 0) {
          sb.append(",");
        }
        appendJsonValue(sb, fields.get(c));
      }
      sb.append("]");
    }
    sb.append("],");

    sb.append("\"stats\":{");
    sb.append("\"state\":\"FINISHED\",");
    sb.append("\"queued\":false,");
    sb.append("\"scheduled\":true,");
    sb.append("\"nodes\":1,");
    sb.append("\"totalSplits\":1,");
    sb.append("\"queuedSplits\":0,");
    sb.append("\"runningSplits\":0,");
    sb.append("\"completedSplits\":1,");
    sb.append("\"cpuTimeMillis\":0,");
    sb.append("\"wallTimeMillis\":0,");
    sb.append("\"queuedTimeMillis\":0,");
    sb.append("\"elapsedTimeMillis\":0,");
    sb.append("\"processedRows\":").append(rows.size()).append(",");
    sb.append("\"processedBytes\":0,");
    sb.append("\"peakMemoryBytes\":0");
    sb.append("}}");

    return sb.toString();
  }

  private String serializeErrorJson(String queryId, Exception exception) {
    String message = exception.getMessage();
    String errorName = "GENERIC_INTERNAL_ERROR";
    String errorType = "INTERNAL_ERROR";
    int errorCode = 1;

    Throwable cause = exception;
    while (cause != null) {
      if (cause instanceof TrinoException trinoEx) {
        message = trinoEx.getMessage();
        errorName = trinoEx.getErrorCode().getName();
        errorType = trinoEx.getErrorCode().getType().name();
        errorCode = trinoEx.getErrorCode().getCode();
        break;
      }
      cause = cause.getCause();
    }

    return "{"
        + "\"id\":\""
        + queryId
        + "\","
        + "\"stats\":{\"state\":\"FAILED\"},"
        + "\"error\":{"
        + "\"message\":\""
        + escapeJson(message)
        + "\","
        + "\"errorCode\":"
        + errorCode
        + ","
        + "\"errorName\":\""
        + escapeJson(errorName)
        + "\","
        + "\"errorType\":\""
        + escapeJson(errorType)
        + "\""
        + "}}";
  }

  @SuppressWarnings("unchecked")
  private static void appendJsonValue(StringBuilder sb, Object val) {
    if (val == null) {
      sb.append("null");
    } else if (val instanceof String s) {
      sb.append("\"").append(escapeJson(s)).append("\"");
    } else if (val instanceof Boolean) {
      sb.append(val.toString());
    } else if (val instanceof Number) {
      sb.append(val);
    } else if (val instanceof Temporal) {
      sb.append("\"").append(val).append("\"");
    } else if (val instanceof List<?> list) {
      sb.append("[");
      for (int i = 0; i < list.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        appendJsonValue(sb, list.get(i));
      }
      sb.append("]");
    } else if (val instanceof Map<?, ?> map) {
      sb.append("{");
      boolean first = true;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        sb.append("\"").append(escapeJson(String.valueOf(entry.getKey()))).append("\":");
        appendJsonValue(sb, entry.getValue());
      }
      sb.append("}");
    } else if (val instanceof byte[] bytes) {
      sb.append("\"")
          .append(java.util.Base64.getEncoder().encodeToString(bytes))
          .append("\"");
    } else {
      sb.append("\"").append(escapeJson(val.toString())).append("\"");
    }
  }

  private static String escapeJson(String s) {
    if (s == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\' -> sb.append("\\\\");
        case '"' -> sb.append("\\\"");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        case '\b' -> sb.append("\\b");
        case '\f' -> sb.append("\\f");
        default -> {
          if (c < 0x20) {
            sb.append("\\u").append(String.format("%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.toString();
  }

  @Override
  public void close() {
    LOG.info("Closing Trino engine");
    try {
      queryRunner.close();
    } catch (Exception e) {
      LOG.warn("Error closing Trino engine", e);
    }
  }
}
