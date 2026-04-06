/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.bootstrap;

import com.google.inject.Key;
import io.trino.Session;
import io.trino.execution.SqlTaskManager;
import io.trino.hadoop.HadoopNative;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.security.Identity;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingTrinoClient;
import java.io.Closeable;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Embedded Trino SQL execution engine using a single {@link TestingTrinoServer} per OpenSearch node.
 *
 * <p>Each OpenSearch node runs one Trino server (coordinator + worker, symmetric design). For
 * distributed execution across multiple OpenSearch nodes, the transport-based communication layer
 * ({@code TransportRemoteTask}, {@code OpenSearchSqlTaskManager}) handles inter-node coordination
 * via OpenSearch TransportActions.
 *
 * <p>On a single-node cluster, all transport actions are delivered via loopback (in-memory). On a
 * multi-node cluster, they go over TCP. Same code path — only topology differs.
 *
 * <p>No DistributedQueryRunner. No fake multi-node simulation.
 */
public class TrinoEngine implements Closeable {

  private static final Logger LOG = LogManager.getLogger(TrinoEngine.class);

  private final TestingTrinoServer server;
  private final TestingTrinoClient client;

  /** Create a TrinoEngine with default temp warehouse directories. */
  public TrinoEngine() {
    this("");
  }

  /**
   * Create a TrinoEngine with a configurable Iceberg warehouse path.
   *
   * @param icebergWarehouse path to Iceberg warehouse directory. If empty, checks the system
   *     property {@code trino.iceberg.warehouse} as a fallback. If both are empty, a temp directory
   *     is created.
   */
  public TrinoEngine(String icebergWarehouse) {
    if (icebergWarehouse == null || icebergWarehouse.isEmpty()) {
      icebergWarehouse = System.getProperty("trino.iceberg.warehouse", "");
    }
    LOG.info("Initializing Trino engine via TestingTrinoServer (single node, transport-based)");
    try {
      initHadoopNative();

      // Configure memory: 60% of heap for queries, 15% headroom.
      // Cluster-wide limit set to 10x per-node to avoid starving remote nodes.
      long maxHeap = Runtime.getRuntime().maxMemory();
      long queryMemPerNode = (long) (maxHeap * 0.60);
      long headroom = (long) (maxHeap * 0.15);
      long queryMemCluster = queryMemPerNode * 10;
      String perNodeMemStr = (queryMemPerNode / (1024 * 1024)) + "MB";
      String clusterMemStr = (queryMemCluster / (1024 * 1024)) + "MB";
      String headroomStr = (headroom / (1024 * 1024)) + "MB";
      LOG.info(
          "Trino memory config: heap={}MB, perNodeQueryMem={}, clusterQueryMem={}, headroom={}",
          maxHeap / (1024 * 1024), perNodeMemStr, clusterMemStr, headroomStr);

      int processors = Runtime.getRuntime().availableProcessors();
      int taskConcurrency = Math.min(16, Math.max(4, processors / 2));
      LOG.info("Trino parallelism: processors={}, taskConcurrency={}", processors, taskConcurrency);

      // Build a single TestingTrinoServer per OpenSearch node.
      // Spill disabled: StackCallerProtectionDomainChainExtractor NPEs in plugin classloader
      // because CodeSource.getLocation() returns null for classes loaded from JARs-inside-ZIP.
      TestingTrinoServer trinoServer =
          TestingTrinoServer.builder()
              .setProperties(
                  Map.of(
                      "query.max-memory-per-node", perNodeMemStr,
                      "query.max-memory", clusterMemStr,
                      "memory.heap-headroom-per-node", headroomStr,
                      "spill-enabled", "false",
                      "exchange.compression-codec", "LZ4",
                      "query.max-execution-time", "10m"))
              .build();

      // Install connector plugins
      trinoServer.installPlugin(new TpchPlugin());
      trinoServer.createCatalog("tpch", "tpch", Map.of());
      trinoServer.installPlugin(new MemoryPlugin());
      trinoServer.createCatalog("memory", "memory", Map.of());

      // Iceberg catalog
      trinoServer.installPlugin(new IcebergPlugin());
      Path warehouseDir;
      if (icebergWarehouse != null && !icebergWarehouse.isEmpty()) {
        warehouseDir = Path.of(icebergWarehouse);
        if (!Files.exists(warehouseDir)) {
          Files.createDirectories(warehouseDir);
        }
        LOG.info("Using configured warehouse: {}", warehouseDir);
      } else {
        warehouseDir = Files.createTempDirectory("trino-iceberg-warehouse");
        LOG.info("Using temp warehouse: {}", warehouseDir);
      }
      trinoServer.createCatalog(
          "iceberg",
          "iceberg",
          Map.of(
              "iceberg.catalog.type", "TESTING_FILE_METASTORE",
              "hive.metastore.catalog.dir", warehouseDir.toUri().toString()));

      // Hive catalog — use the SAME warehouse dir so all nodes share catalog metadata.
      // Previous bug: each node created its own temp dir, causing "No catalog 'hive'" on
      // remote nodes because their metastore pointed to a nonexistent directory.
      trinoServer.installPlugin(new HivePlugin());
      Path hiveWarehouse = warehouseDir;
      trinoServer.createCatalog(
          "hive",
          "hive",
          Map.of(
              "hive.metastore", "file",
              "hive.metastore.catalog.dir", hiveWarehouse.toUri().toString()));

      this.server = trinoServer;
      Session defaultSession = createDefaultSession(trinoServer.getSessionPropertyManager());
      this.client = new TestingTrinoClient(trinoServer, defaultSession);

      LOG.info(
          "Trino engine initialized: TPCH, Memory, Iceberg, Hive (warehouse: {})", warehouseDir);
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
          "Hadoop native library failed to load ({}); bypassing via reflection",
          t.getMessage());
      try {
        Field loadedField = HadoopNative.class.getDeclaredField("loaded");
        loadedField.setAccessible(true);
        loadedField.setBoolean(null, true);
        Field errorField = HadoopNative.class.getDeclaredField("error");
        errorField.setAccessible(true);
        errorField.set(null, null);
      } catch (Exception reflectionEx) {
        LOG.warn("Failed to bypass Hadoop native library check", reflectionEx);
      }
    }
  }

  private static Session createDefaultSession(SessionPropertyManager spm) {
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
      MaterializedResult result = client.execute(session, sql).getResult();
      return serializeToTrinoJson(queryId, result);
    } catch (Exception e) {
      LOG.error("Query execution failed: {}", e.getMessage(), e);
      return serializeErrorJson(queryId, e);
    }
  }

  private Session createSessionWithCatalogSchema(String catalog, String schema) {
    SessionPropertyManager spm = server.getSessionPropertyManager();
    Identity identity = Identity.ofUser("opensearch");
    int processors = Runtime.getRuntime().availableProcessors();
    int taskConcurrency = Math.min(16, Math.max(4, processors / 2));
    Session.SessionBuilder builder =
        Session.builder(spm)
            .setIdentity(identity)
            .setOriginalIdentity(identity)
            .setSource("opensearch-sql-plugin")
            .setQueryId(QueryId.valueOf(UUID.randomUUID().toString().replace("-", "")))
            .setSystemProperty("task_concurrency", String.valueOf(taskConcurrency))
            .setSystemProperty("dictionary_aggregation", "true")
            .setSystemProperty("max_hash_partition_count", String.valueOf(taskConcurrency))
            .setSystemProperty("min_hash_partition_count",
                String.valueOf(Math.min(4, taskConcurrency)))
            .setCatalogSessionProperty("iceberg", "parquet_max_read_block_row_count", "65536")
            .setCatalogSessionProperty("iceberg", "parquet_max_read_block_size", "64MB");
    builder.setCatalog(catalog != null ? catalog : "tpch");
    builder.setSchema(schema != null ? schema : "tiny");
    return builder.build();
  }

  /** Get the underlying TestingTrinoServer for Guice access. */
  public TestingTrinoServer getServer() {
    return server;
  }

  /** Get the SessionPropertyManager from the server. */
  public SessionPropertyManager getSessionPropertyManager() {
    return server.getSessionPropertyManager();
  }

  /** Get the SqlTaskManager for worker-side task handling. */
  public SqlTaskManager getTaskManager() {
    return server.getTaskManager();
  }

  /**
   * Phase 1: Initialize TrinoServiceHolder with this engine. Deferred to Phase 2 for
   * OpenSearchNodeManager and split-level distribution because ClusterState/TransportService
   * are not yet available during createComponents().
   */
  public void initializeTransportServices() {
    org.opensearch.sql.trino.plugin.TrinoServiceHolder.initializeWithEngine(this);

    java.net.URI trinoHttpUrl = server.getBaseUrl();
    org.opensearch.sql.trino.plugin.TrinoServiceHolder.getInstance()
        .setTrinoHttpUrl(trinoHttpUrl);
    LOG.info("Trino transport services initialized (phase 1) — HTTP URL: {}", trinoHttpUrl);
  }

  /**
   * Phase 2: Set up OpenSearchNodeManager and enable split-level distribution. Called after the
   * cluster is fully started and TransportService is available.
   */
  public void enableSplitLevelDistribution(
      org.opensearch.cluster.service.ClusterService clusterService,
      org.opensearch.transport.TransportService transportService) {
    try {
      org.opensearch.cluster.node.DiscoveryNode localNode = clusterService.localNode();
      java.net.URI trinoHttpUrl = server.getBaseUrl();

      // Get the server's Trino node ID — TestingTrinoServer assigns a UUID that differs from
      // the OpenSearch DiscoveryNode ID. NodeManager needs this for split assignment mapping.
      String serverTrinoNodeId = server.getInstance(Key.get(InternalNodeManager.class))
          .getCurrentNode().getNodeIdentifier();

      org.opensearch.sql.trino.node.OpenSearchNodeManager nodeManager =
          new org.opensearch.sql.trino.node.OpenSearchNodeManager(localNode);
      nodeManager.registerCoordinatorTrinoNodeId(serverTrinoNodeId, localNode);
      nodeManager.registerTrinoHttpUrl(localNode.getId(), trinoHttpUrl);

      org.opensearch.cluster.ClusterState state = clusterService.state();
      nodeManager.clusterChanged(
          new org.opensearch.cluster.ClusterChangedEvent("trino-init", state, state));
      clusterService.addListener(nodeManager);

      org.opensearch.sql.trino.plugin.TrinoServiceHolder holder =
          org.opensearch.sql.trino.plugin.TrinoServiceHolder.getInstance();
      holder.setNodeManager(nodeManager);

      org.opensearch.sql.trino.transport.TransportDistributionPatcher.patch(
          server, transportService, nodeManager, holder.getCodec());

      broadcastTrinoHttpUrl(clusterService, transportService, localNode, trinoHttpUrl);

      LOG.info("Split-level distribution enabled — Local node: {}, HTTP URL: {}",
          localNode.getName(), trinoHttpUrl);
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
        continue;
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
              if (response.isSuccess() && !response.getReceiverTrinoHttpUrl().isEmpty()) {
                org.opensearch.sql.trino.plugin.TrinoServiceHolder holder =
                    org.opensearch.sql.trino.plugin.TrinoServiceHolder.getInstance();
                if (holder.getNodeManager() != null) {
                  holder.getNodeManager().registerTrinoHttpUrl(
                      response.getReceiverNodeId(),
                      java.net.URI.create(response.getReceiverTrinoHttpUrl()));
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
      sb.append("{\"name\":\"").append(escapeJson(colName)).append("\",");
      sb.append("\"type\":\"").append(escapeJson(type.getDisplayName())).append("\",");
      sb.append("\"typeSignature\":{\"rawType\":\"")
          .append(escapeJson(type.getBaseName())).append("\"}}");
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
        + "\"id\":\"" + queryId + "\","
        + "\"stats\":{\"state\":\"FAILED\"},"
        + "\"error\":{"
        + "\"message\":\"" + escapeJson(message) + "\","
        + "\"errorCode\":" + errorCode + ","
        + "\"errorName\":\"" + escapeJson(errorName) + "\","
        + "\"errorType\":\"" + escapeJson(errorType) + "\""
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
        if (i > 0) sb.append(",");
        appendJsonValue(sb, list.get(i));
      }
      sb.append("]");
    } else if (val instanceof Map<?, ?> map) {
      sb.append("{");
      boolean first = true;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!first) sb.append(",");
        first = false;
        sb.append("\"").append(escapeJson(String.valueOf(entry.getKey()))).append("\":");
        appendJsonValue(sb, entry.getValue());
      }
      sb.append("}");
    } else if (val instanceof byte[] bytes) {
      sb.append("\"").append(java.util.Base64.getEncoder().encodeToString(bytes)).append("\"");
    } else {
      sb.append("\"").append(escapeJson(val.toString())).append("\"");
    }
  }

  private static String escapeJson(String s) {
    if (s == null) return "";
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
      client.close();
    } catch (Exception e) {
      LOG.warn("Error closing Trino client", e);
    }
    try {
      server.close();
    } catch (Exception e) {
      LOG.warn("Error closing Trino server", e);
    }
  }
}
