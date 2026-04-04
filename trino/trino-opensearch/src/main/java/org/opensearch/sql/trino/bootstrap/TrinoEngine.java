/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.bootstrap;

import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import java.io.Closeable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.LocalDate;
import java.time.LocalTime;
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
 * <p>Uses {@link StandaloneQueryRunner} to run an in-process Trino engine with the TPCH catalog for
 * testing. Queries are executed directly through the Trino engine API (no external HTTP server).
 * Results are serialized to Trino client protocol JSON format for the REST endpoint.
 *
 * <p>Initialization is wrapped in {@code AccessController.doPrivileged()} to grant the necessary
 * permissions for Trino's embedded HTTP server under OpenSearch's security manager.
 */
@SuppressWarnings("removal") // AccessController is deprecated but still required by OpenSearch
public class TrinoEngine implements Closeable {

  private static final Logger LOG = LogManager.getLogger(TrinoEngine.class);

  private final StandaloneQueryRunner queryRunner;

  public TrinoEngine() {
    LOG.info("Initializing real Trino engine via StandaloneQueryRunner");
    try {
      this.queryRunner =
          AccessController.doPrivileged(
              (PrivilegedAction<StandaloneQueryRunner>)
                  () -> {
                    Session session = createDefaultSession();
                    StandaloneQueryRunner runner = new StandaloneQueryRunner(session);
                    runner.installPlugin(new TpchPlugin());
                    runner.createCatalog("tpch", "tpch", Map.of());
                    runner.installPlugin(new MemoryPlugin());
                    runner.createCatalog("memory", "memory", Map.of());
                    return runner;
                  });
      LOG.info("Trino engine initialized with TPCH and Memory catalogs");
    } catch (Exception e) {
      LOG.error("Failed to initialize Trino engine", e);
      throw new RuntimeException("Failed to initialize Trino engine", e);
    }
  }

  @SuppressWarnings("removal")
  private static Session createDefaultSession() {
    // Use the constructor with empty properties to avoid SystemSessionProperties
    // hardware detection. The StandaloneQueryRunner initializes its own session
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
  @SuppressWarnings("removal")
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
  @SuppressWarnings("removal")
  public String executeAndSerializeJson(String sql, String catalog, String schema) {
    String queryId = UUID.randomUUID().toString().replace("-", "");
    LOG.debug("TrinoEngine executing query [{}] with id [{}]", sql, queryId);

    try {
      Session session = createSessionWithCatalogSchema(catalog, schema);
      MaterializedResult result =
          AccessController.doPrivileged(
              (PrivilegedAction<MaterializedResult>) () -> queryRunner.execute(session, sql));
      return serializeToTrinoJson(queryId, result);
    } catch (Exception e) {
      LOG.error("Query execution failed: {}", e.getMessage(), e);
      return serializeErrorJson(queryId, e);
    }
  }

  @SuppressWarnings("removal")
  private Session createSessionWithCatalogSchema(String catalog, String schema) {
    // Use the queryRunner's SessionPropertyManager so that catalog-level properties are resolved
    SessionPropertyManager spm = queryRunner.getSessionPropertyManager();
    Identity identity = Identity.ofUser("opensearch");
    Session.SessionBuilder builder =
        Session.builder(spm)
            .setIdentity(identity)
            .setOriginalIdentity(identity)
            .setSource("opensearch-sql-plugin")
            .setQueryId(QueryId.valueOf(UUID.randomUUID().toString().replace("-", "")));
    builder.setCatalog(catalog != null ? catalog : "tpch");
    builder.setSchema(schema != null ? schema : "tiny");
    return builder.build();
  }

  /** Serialize a MaterializedResult to Trino client protocol JSON. */
  private String serializeToTrinoJson(String queryId, MaterializedResult result) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"id\":\"").append(queryId).append("\",");

    // Columns
    List<String> columnNames = result.getColumnNames();
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
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
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
