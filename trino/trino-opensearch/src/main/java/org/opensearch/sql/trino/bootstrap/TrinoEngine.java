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
 * Embedded Trino SQL execution engine using the real Trino query engine.
 *
 * <p>Uses {@link DistributedQueryRunner} to run an in-process Trino engine with coordinator and
 * worker on a single node. This supports both DDL (CREATE TABLE, DROP TABLE) and DML (INSERT,
 * SELECT) operations, unlike StandaloneQueryRunner which deadlocks on DML.
 *
 * <p>The DistributedQueryRunner starts an internal HTTP server on an ephemeral localhost port.
 * All external access is still through the OpenSearch REST API — the internal HTTP server is
 * an implementation detail not exposed to users.
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
    LOG.info("Initializing real Trino engine via DistributedQueryRunner");
    try {
      // Pre-initialize Hadoop native library.  In a shadow-jar environment the
      // CodeSource.getLocation() can return null which causes HadoopNative to
      // fail with a NullPointerException.  We catch that and mark the native
      // library as "loaded" via reflection so the Iceberg connector can proceed
      // without native Hadoop codecs.
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

      // Scale node count for distributed execution parallelism.
      // 4 nodes on a 32-CPU machine gives good parallelism for GROUP BY/JOIN.
      int nodeCount = Math.max(2, Math.min(4, processors / 8));
      LOG.info("Trino node count: {} (processors={})", nodeCount, processors);
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
      // When a warehouse path is provided (e.g. from plugins.trino.catalog.iceberg.warehouse),
      // use Hadoop catalog type so Trino can read tables created by external tools like Spark.
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

  /**
   * Pre-initialize Hadoop native library for shadow-jar environments. In a shaded jar the
   * {@code CodeSource.getLocation()} can return null which causes {@code HadoopNative} to fail
   * with a NullPointerException. We try the normal path first; if it fails we mark the native
   * library as "loaded" and clear the error via reflection so that downstream code (e.g. the
   * Iceberg connector's HdfsEnvironment) does not throw.
   */
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
        // Set HadoopNative.loaded = true and HadoopNative.error = null
        // so subsequent calls to requireHadoopNative() succeed.
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
    // Use the constructor with empty properties to avoid SystemSessionProperties
    // hardware detection. The DistributedQueryRunner initializes its own session
    // properties during server bootstrap.
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
   *
   * @param sql the SQL query to execute
   * @param catalog optional catalog name (uses default if null)
   * @param schema optional schema name (uses default if null)
   * @return JSON string in Trino v1/statement response format
   */
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
    // Use the queryRunner's SessionPropertyManager so that catalog-level properties are resolved
    SessionPropertyManager spm = queryRunner.getSessionPropertyManager();
    Identity identity = Identity.ofUser("opensearch");
    int taskConcurrency = Runtime.getRuntime().availableProcessors();
    Session.SessionBuilder builder =
        Session.builder(spm)
            .setIdentity(identity)
            .setOriginalIdentity(identity)
            .setSource("opensearch-sql-plugin")
            .setQueryId(QueryId.valueOf(UUID.randomUUID().toString().replace("-", "")))
            // Performance: scale parallelism to available CPUs
            .setSystemProperty("task_concurrency", String.valueOf(taskConcurrency))
            .setSystemProperty("dictionary_aggregation", "true")
            // Optimize hash partitioning for single-node execution
            .setSystemProperty("max_hash_partition_count", String.valueOf(taskConcurrency))
            .setSystemProperty("min_hash_partition_count", String.valueOf(taskConcurrency))
            // Increase Parquet read block size for better vectorization
            .setCatalogSessionProperty("iceberg", "parquet_max_read_block_row_count", "65536")
            .setCatalogSessionProperty("iceberg", "parquet_max_read_block_size", "64MB");
    builder.setCatalog(catalog != null ? catalog : "tpch");
    builder.setSchema(schema != null ? schema : "tiny");
    return builder.build();
  }

  /** Serialize a MaterializedResult to Trino client protocol JSON. */
  private String serializeToTrinoJson(String queryId, MaterializedResult result) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"id\":\"").append(queryId).append("\",");

    // Columns — DDL statements may not have column names
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

    // Data
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
        Object val = fields.get(c);
        appendJsonValue(sb, val);
      }
      sb.append("]");
    }
    sb.append("],");

    // Stats
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

  /** Serialize an error response in Trino client protocol format. */
  private String serializeErrorJson(String queryId, Exception exception) {
    String message = exception.getMessage();
    String errorName = "GENERIC_INTERNAL_ERROR";
    String errorType = "INTERNAL_ERROR";
    int errorCode = 1;

    // Extract error details from TrinoException if available
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

  /**
   * Append a value as valid JSON to the StringBuilder. Handles null, String, Boolean, Number
   * (including NaN/Infinity), Date/Time types, List (arrays), and Map types. Falls back to quoted
   * toString() for unknown types.
   */
  @SuppressWarnings("unchecked")
  private static void appendJsonValue(StringBuilder sb, Object val) {
    if (val == null) {
      sb.append("null");
    } else if (val instanceof String s) {
      sb.append("\"").append(escapeJson(s)).append("\"");
    } else if (val instanceof Boolean) {
      sb.append(val.toString());
    } else if (val instanceof Number) {
      // Numbers (including NaN and Infinity) are written as-is. The client-side
      // ObjectMapper must have ALLOW_NON_NUMERIC_NUMBERS enabled to parse NaN/Infinity.
      sb.append(val);
    } else if (val instanceof Temporal) {
      // LocalDate, LocalTime, LocalDateTime, etc. — quote as strings
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
        // JSON object keys must be strings
        sb.append("\"").append(escapeJson(String.valueOf(entry.getKey()))).append("\":");
        appendJsonValue(sb, entry.getValue());
      }
      sb.append("}");
    } else if (val instanceof byte[] bytes) {
      // Base64 encode binary data
      sb.append("\"")
          .append(java.util.Base64.getEncoder().encodeToString(bytes))
          .append("\"");
    } else {
      // Unknown type: quote as string to ensure valid JSON
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
            // Escape all other control characters as unicode escape sequences
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
