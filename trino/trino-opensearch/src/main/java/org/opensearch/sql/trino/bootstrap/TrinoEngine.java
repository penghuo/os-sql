/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.bootstrap;

import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.SessionPropertyManager;
import io.trino.hadoop.HadoopNative;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Embedded Trino SQL execution engine using a single {@link TestingTrinoServer} per OpenSearch node.
 *
 * <p>Each OpenSearch node runs one Trino server instance (coordinator + worker, symmetric design).
 * For distributed execution across multiple OpenSearch nodes, the transport-based communication
 * layer ({@code TransportRemoteTask}, {@code TransportExchangeClient}) handles inter-node
 * coordination via OpenSearch TransportActions.
 *
 * <p>On a single-node cluster, all transport actions are delivered via loopback (in-memory). On a
 * multi-node cluster, they go over TCP. Same code path — only topology differs.
 *
 * <p>No DistributedQueryRunner. No fake multi-node simulation. No fallback.
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

      // Configure memory adaptively based on available heap
      long maxHeap = Runtime.getRuntime().maxMemory();
      long queryMem = (long) (maxHeap * 0.70);
      long headroom = (long) (maxHeap * 0.15);
      String queryMemStr = (queryMem / (1024 * 1024)) + "MB";
      String headroomStr = (headroom / (1024 * 1024)) + "MB";
      LOG.info(
          "Trino memory config: heap={}MB, queryMem={}, headroom={}",
          maxHeap / (1024 * 1024),
          queryMemStr,
          headroomStr);

      // Build a single TestingTrinoServer — one per OpenSearch node.
      // For multi-node distributed execution, each OpenSearch node starts its own server
      // and they communicate via TransportActions (not HTTP).
      TestingTrinoServer trinoServer =
          TestingTrinoServer.builder()
              .setProperties(
                  Map.of(
                      "query.max-memory-per-node", queryMemStr,
                      "query.max-memory", queryMemStr,
                      "memory.heap-headroom-per-node", headroomStr))
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
        LOG.info("Using configured Iceberg warehouse: {}", warehouseDir);
      } else {
        warehouseDir = Files.createTempDirectory("trino-iceberg-warehouse");
        LOG.info("Using temp Iceberg warehouse: {}", warehouseDir);
      }
      trinoServer.createCatalog(
          "iceberg",
          "iceberg",
          Map.of(
              "iceberg.catalog.type",
              "TESTING_FILE_METASTORE",
              "hive.metastore.catalog.dir",
              warehouseDir.toUri().toString()));

      // Hive catalog for raw Parquet files
      trinoServer.installPlugin(new HivePlugin());
      Path hiveWarehouse = Files.createTempDirectory("trino-hive-warehouse");
      trinoServer.createCatalog(
          "hive",
          "hive",
          Map.of(
              "hive.metastore",
              "file",
              "hive.metastore.catalog.dir",
              hiveWarehouse.toUri().toString()));

      this.server = trinoServer;
      // TestingTrinoClient for executing queries via the server
      Session defaultSession = createDefaultSession(trinoServer.getSessionPropertyManager());
      this.client = new TestingTrinoClient(trinoServer, defaultSession);

      LOG.info(
          "Trino engine initialized with TPCH, Memory, Iceberg, and Hive catalogs"
              + " (iceberg warehouse: {}, hive warehouse: {})",
          warehouseDir,
          hiveWarehouse);
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

  /**
   * Execute a SQL query and return the result in Trino client protocol JSON format.
   *
   * @param sql the SQL query to execute
   * @return JSON string in Trino v1/statement response format
   */
  public String executeAndSerializeJson(String sql) {
    return executeAndSerializeJson(sql, null, null);
  }

  /**
   * Execute a SQL query with optional catalog/schema and return the result in Trino client protocol
   * JSON format.
   */
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

  /** Get the underlying TestingTrinoServer for accessing internal components. */
  public TestingTrinoServer getServer() {
    return server;
  }

  /** Get the SessionPropertyManager. */
  public SessionPropertyManager getSessionPropertyManager() {
    return server.getSessionPropertyManager();
  }

  /** Serialize a MaterializedResult to Trino client protocol JSON. */
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
