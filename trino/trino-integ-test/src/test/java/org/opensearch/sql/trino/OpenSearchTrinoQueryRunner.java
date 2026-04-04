/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingGroupProviderManager;
import io.trino.transaction.TransactionManager;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.opentelemetry.sdk.trace.data.SpanData;

import io.trino.spi.type.DecimalType;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A Trino {@link QueryRunner} that routes all queries through HTTP to an OpenSearch
 * cluster's {@code /_plugins/_trino_sql/v1/statement} REST endpoint.
 *
 * <p>This implementation follows the Trino client protocol: POST the SQL statement,
 * then follow {@code nextUri} links until the query is complete, accumulating rows.</p>
 */
public class OpenSearchTrinoQueryRunner implements QueryRunner {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

  private final String baseUrl;
  private final HttpClient httpClient;
  private final Session defaultSession;
  private final Lock exclusiveLock = new ReentrantLock();

  private OpenSearchTrinoQueryRunner(String baseUrl) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();

    SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
    Identity identity = Identity.ofUser("trino-test");
    this.defaultSession = Session.builder(sessionPropertyManager)
        .setIdentity(identity)
        .setOriginalIdentity(identity)
        .setSource("opensearch-trino-test")
        .setCatalog("tpch")
        .setSchema("tiny")
        .setQueryId(QueryId.valueOf("test"))
        .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  // ---- Core execute methods (HTTP-based) ----

  @Override
  public MaterializedResult execute(Session session, String sql) {
    try {
      return executeViaHttp(sql);
    } catch (io.trino.spi.TrinoException e) {
      // Preserve TrinoException unwrapped so test framework can detect it
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute query: " + sql, e);
    }
  }

  @Override
  public MaterializedResult execute(String sql) {
    return execute(defaultSession, sql);
  }

  /**
   * Executes SQL via the Trino HTTP protocol against the OpenSearch endpoint.
   * POST to /v1/statement, then follow nextUri until complete.
   */
  private MaterializedResult executeViaHttp(String sql) throws IOException, InterruptedException {
    String statementUrl = baseUrl + "/_plugins/_trino_sql/v1/statement";

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(statementUrl))
        .header("Content-Type", "application/json")
        .header("X-Trino-User", "trino-test")
        .header("X-Trino-Source", "opensearch-trino-test")
        .header("X-Trino-Catalog", "tpch")
        .header("X-Trino-Schema", "tiny")
        .POST(HttpRequest.BodyPublishers.ofString(sql))
        .timeout(Duration.ofSeconds(60))
        .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    JsonNode root = MAPPER.readTree(response.body());

    List<Type> columnTypes = new ArrayList<>();
    List<String> columnTypeNames = new ArrayList<>(); // raw type name strings
    List<String> columnNames = new ArrayList<>();
    List<MaterializedRow> rows = new ArrayList<>();

    // Parse columns from first response
    if (root.has("columns")) {
      for (JsonNode col : root.get("columns")) {
        columnNames.add(col.get("name").asText());
        String typeName = col.get("type").asText();
        columnTypeNames.add(typeName);
        columnTypes.add(parseTrinoType(typeName));
      }
    }

    // Collect data from first response
    collectRows(root, columnTypes, columnTypeNames, rows);

    // Follow nextUri pagination until query completes
    while (root.has("nextUri")) {
      String nextUri = root.get("nextUri").asText();
      HttpRequest nextRequest = HttpRequest.newBuilder()
          .uri(URI.create(nextUri))
          .header("X-Trino-User", "trino-test")
          .header("X-Trino-Source", "opensearch-trino-test")
          .GET()
          .timeout(Duration.ofSeconds(60))
          .build();

      response = httpClient.send(nextRequest, HttpResponse.BodyHandlers.ofString());
      root = MAPPER.readTree(response.body());

      // Update columns if they appear in a later response
      if (columnTypes.isEmpty() && root.has("columns")) {
        for (JsonNode col : root.get("columns")) {
          columnNames.add(col.get("name").asText());
          String typeName = col.get("type").asText();
          columnTypeNames.add(typeName);
          columnTypes.add(parseTrinoType(typeName));
        }
      }

      collectRows(root, columnTypes, columnTypeNames, rows);
    }

    // Check for error
    if (root.has("error")) {
      JsonNode error = root.get("error");
      String message = error.has("message") ? error.get("message").asText() : "Unknown error";

      // Map errorName/errorType to a StandardErrorCode when possible
      io.trino.spi.StandardErrorCode errorCode = io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
      if (error.has("errorName")) {
        String errorName = error.get("errorName").asText();
        try {
          errorCode = io.trino.spi.StandardErrorCode.valueOf(errorName);
        } catch (IllegalArgumentException ignored) {
          // fall back to GENERIC_INTERNAL_ERROR
        }
      }
      throw new io.trino.spi.TrinoException(errorCode, message);
    }

    // Parse update type and count if present
    Optional<String> updateType = root.has("updateType")
        ? Optional.of(root.get("updateType").asText())
        : Optional.empty();

    return new MaterializedResult(
        rows,
        columnTypes,
        Optional.of(columnNames));
  }

  private void collectRows(JsonNode root, List<Type> columnTypes,
      List<String> columnTypeNames, List<MaterializedRow> rows) {
    if (!root.has("data")) {
      return;
    }
    for (JsonNode dataRow : root.get("data")) {
      List<Object> values = new ArrayList<>();
      for (int i = 0; i < dataRow.size(); i++) {
        JsonNode val = dataRow.get(i);
        Type type = i < columnTypes.size() ? columnTypes.get(i) : VarcharType.VARCHAR;
        String typeName = i < columnTypeNames.size() ? columnTypeNames.get(i) : "varchar";
        values.add(parseValue(val, type, typeName));
      }
      rows.add(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, values));
    }
  }

  /**
   * Parse a Trino type name string into a {@link Type} object.
   */
  static Type parseTrinoType(String typeName) {
    if (typeName == null) {
      return VarcharType.VARCHAR;
    }
    String normalized = typeName.toLowerCase().trim();

    // Handle parameterized types
    if (normalized.startsWith("varchar")) {
      return VarcharType.VARCHAR;
    }
    if (normalized.startsWith("char")) {
      return VarcharType.VARCHAR; // simplify char to varchar
    }
    if (normalized.startsWith("decimal")) {
      // Parse decimal(precision, scale)
      try {
        String params = normalized.substring("decimal".length()).trim();
        if (params.startsWith("(") && params.endsWith(")")) {
          String[] parts = params.substring(1, params.length() - 1).split(",");
          int precision = Integer.parseInt(parts[0].trim());
          int scale = Integer.parseInt(parts[1].trim());
          return DecimalType.createDecimalType(precision, scale);
        }
      } catch (Exception e) {
        // fall through
      }
      return DoubleType.DOUBLE; // fallback for unparseable decimal
    }
    if (normalized.startsWith("timestamp")) {
      return TimestampType.TIMESTAMP_MILLIS;
    }
    if (normalized.startsWith("time")) {
      return VarcharType.VARCHAR; // simplify time types to varchar
    }
    if (normalized.startsWith("array") || normalized.startsWith("map")
        || normalized.startsWith("row")) {
      return VarcharType.VARCHAR; // complex types as varchar
    }

    switch (normalized) {
      case "boolean":
        return BooleanType.BOOLEAN;
      case "tinyint":
        return TinyintType.TINYINT;
      case "smallint":
        return SmallintType.SMALLINT;
      case "integer":
        return IntegerType.INTEGER;
      case "bigint":
        return BigintType.BIGINT;
      case "real":
        return RealType.REAL;
      case "double":
        return DoubleType.DOUBLE;
      case "date":
        return DateType.DATE;
      case "varbinary":
        return VarbinaryType.VARBINARY;
      default:
        return VarcharType.VARCHAR;
    }
  }

  /**
   * Parse a JSON value into a Java object matching the given Trino type.
   * The returned Java types must match what Trino's test framework expects.
   */
  private static Object parseValue(JsonNode node, Type type, String typeName) {
    if (node == null || node.isNull()) {
      return null;
    }

    if (type instanceof BooleanType) {
      return node.asBoolean();
    }
    if (type instanceof TinyintType || type instanceof SmallintType
        || type instanceof IntegerType) {
      return (int) node.asLong();
    }
    if (type instanceof BigintType) {
      return node.asLong();
    }
    if (type instanceof RealType) {
      return (float) node.asDouble();
    }
    if (type instanceof DoubleType) {
      return node.asDouble();
    }
    if (type instanceof DecimalType decimalType) {
      String text = node.isTextual() ? node.asText() : node.toString();
      try {
        BigDecimal bd = new BigDecimal(text);
        bd = bd.setScale(decimalType.getScale(), java.math.RoundingMode.HALF_UP);
        return new io.trino.spi.type.SqlDecimal(
            bd.unscaledValue(), decimalType.getPrecision(), decimalType.getScale());
      } catch (Exception e) {
        return node.asDouble();
      }
    }
    if (type instanceof DateType) {
      return LocalDate.parse(node.asText());
    }
    if (type instanceof TimestampType) {
      String text = node.asText();
      try {
        return java.time.LocalDateTime.parse(text.replace(" ", "T"));
      } catch (Exception e) {
        return text;
      }
    }
    if (type instanceof VarbinaryType) {
      return node.asText();
    }

    // Handle complex types using the raw type name for element type info
    String normalizedTypeName = typeName != null ? typeName.toLowerCase().trim() : "";

    // Array types: array(element_type)
    if (normalizedTypeName.startsWith("array(") && node.isArray()) {
      String elementTypeName = extractInnerType(normalizedTypeName, "array(");
      Type elementType = parseTrinoType(elementTypeName);
      List<Object> list = new ArrayList<>();
      for (JsonNode element : node) {
        list.add(parseValue(element, elementType, elementTypeName));
      }
      return list;
    }

    // Map types: map(key_type, value_type)
    if (normalizedTypeName.startsWith("map(") && node.isObject()) {
      String innerTypes = normalizedTypeName.substring(4, normalizedTypeName.length() - 1);
      String[] kvTypes = splitMapTypes(innerTypes);
      String keyTypeName = kvTypes[0];
      String valueTypeName = kvTypes[1];
      Type keyType = parseTrinoType(keyTypeName);
      Type valueType = parseTrinoType(valueTypeName);
      java.util.LinkedHashMap<Object, Object> map = new java.util.LinkedHashMap<>();
      node.fields().forEachRemaining(entry -> {
        // JSON keys are always strings — parse them according to the key type
        Object key = parseTypedString(entry.getKey(), keyType);
        Object val = parseValue(entry.getValue(), valueType, valueTypeName);
        map.put(key, val);
      });
      return map;
    }

    // For any unrecognized complex type, try to preserve JSON structure
    if (node.isArray()) {
      List<Object> list = new ArrayList<>();
      for (JsonNode element : node) {
        list.add(parseGenericValue(element));
      }
      return list;
    }
    if (node.isObject()) {
      java.util.LinkedHashMap<Object, Object> map = new java.util.LinkedHashMap<>();
      node.fields().forEachRemaining(entry ->
          map.put(entry.getKey(), parseGenericValue(entry.getValue())));
      return map;
    }

    // Default: return as string
    return node.asText();
  }

  /**
   * Extract the inner type from a parameterized type like "array(bigint)" -> "bigint".
   */
  private static String extractInnerType(String typeName, String prefix) {
    // Remove prefix and trailing ")"
    return typeName.substring(prefix.length(), typeName.length() - 1).trim();
  }

  /**
   * Split "key_type, value_type" for map types, handling nested parentheses.
   */
  private static String[] splitMapTypes(String inner) {
    int depth = 0;
    for (int i = 0; i < inner.length(); i++) {
      char c = inner.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        return new String[]{inner.substring(0, i).trim(), inner.substring(i + 1).trim()};
      }
    }
    return new String[]{inner, "varchar"}; // fallback
  }

  /**
   * Parse a string value according to a specific type (used for JSON map keys).
   */
  private static Object parseTypedString(String value, Type type) {
    if (type instanceof BigintType) {
      return Long.parseLong(value);
    }
    if (type instanceof IntegerType) {
      return Integer.parseInt(value);
    }
    if (type instanceof SmallintType || type instanceof TinyintType) {
      return Integer.parseInt(value);
    }
    if (type instanceof DoubleType) {
      return Double.parseDouble(value);
    }
    if (type instanceof RealType) {
      return Float.parseFloat(value);
    }
    if (type instanceof BooleanType) {
      return Boolean.parseBoolean(value);
    }
    return value;
  }

  /**
   * Parse a JSON value into its natural Java type (for array/map elements).
   */
  private static Object parseGenericValue(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isBoolean()) {
      return node.asBoolean();
    }
    if (node.isIntegralNumber()) {
      return node.asLong();
    }
    if (node.isFloatingPointNumber()) {
      return node.asDouble();
    }
    if (node.isArray()) {
      List<Object> list = new ArrayList<>();
      for (JsonNode element : node) {
        list.add(parseGenericValue(element));
      }
      return list;
    }
    if (node.isObject()) {
      java.util.LinkedHashMap<Object, Object> map = new java.util.LinkedHashMap<>();
      node.fields().forEachRemaining(entry ->
          map.put(entry.getKey(), parseGenericValue(entry.getValue())));
      return map;
    }
    return node.asText();
  }

  // ---- Session and lifecycle ----

  @Override
  public Session getDefaultSession() {
    return defaultSession;
  }

  @Override
  public int getNodeCount() {
    return 1;
  }

  @Override
  public void close() {
    // HTTP client does not need explicit closing in Java 11+
  }

  @Override
  public Lock getExclusiveLock() {
    return exclusiveLock;
  }

  // ---- Methods not supported via HTTP ----

  @Override
  public MaterializedResultWithPlan executeWithPlan(Session session, String sql) {
    MaterializedResult result = execute(session, sql);
    return new MaterializedResultWithPlan(
        QueryId.valueOf("http-query"),
        Optional.empty(),
        result);
  }

  @Override
  public TestingTrinoServer getCoordinator() {
    throw new UnsupportedOperationException(
        "getCoordinator() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public TransactionManager getTransactionManager() {
    throw new UnsupportedOperationException(
        "getTransactionManager() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public PlannerContext getPlannerContext() {
    throw new UnsupportedOperationException(
        "getPlannerContext() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public QueryExplainer getQueryExplainer() {
    throw new UnsupportedOperationException(
        "getQueryExplainer() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public SessionPropertyManager getSessionPropertyManager() {
    return new SessionPropertyManager();
  }

  @Override
  public SplitManager getSplitManager() {
    throw new UnsupportedOperationException(
        "getSplitManager() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public PageSourceManager getPageSourceManager() {
    throw new UnsupportedOperationException(
        "getPageSourceManager() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public NodePartitioningManager getNodePartitioningManager() {
    throw new UnsupportedOperationException(
        "getNodePartitioningManager() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public StatsCalculator getStatsCalculator() {
    throw new UnsupportedOperationException(
        "getStatsCalculator() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public TestingGroupProviderManager getGroupProvider() {
    throw new UnsupportedOperationException(
        "getGroupProvider() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public TestingAccessControlManager getAccessControl() {
    throw new UnsupportedOperationException(
        "getAccessControl() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public List<SpanData> getSpans() {
    return Collections.emptyList();
  }

  @Override
  public Plan createPlan(Session session, String sql) {
    throw new UnsupportedOperationException(
        "createPlan() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public List<QualifiedObjectName> listTables(Session session, String catalog, String schema) {
    throw new UnsupportedOperationException(
        "listTables() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public boolean tableExists(Session session, String table) {
    throw new UnsupportedOperationException(
        "tableExists() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public void installPlugin(Plugin plugin) {
    throw new UnsupportedOperationException(
        "installPlugin() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public void addFunctions(FunctionBundle functionBundle) {
    throw new UnsupportedOperationException(
        "addFunctions() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public void createCatalog(String catalogName, String connectorName,
      Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "createCatalog() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public void injectTaskFailure(String nodeId, int stageId, int partitionId, int attemptId,
      InjectedFailureType type, Optional<ErrorType> errorType) {
    throw new UnsupportedOperationException(
        "injectTaskFailure() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  @Override
  public void loadExchangeManager(String name, Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "loadExchangeManager() is not supported by HTTP-based OpenSearchTrinoQueryRunner");
  }

  // ---- Builder ----

  public static class Builder {
    private String baseUrl = "http://localhost:9200";

    public Builder setBaseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    public OpenSearchTrinoQueryRunner build() {
      return new OpenSearchTrinoQueryRunner(baseUrl);
    }
  }
}
