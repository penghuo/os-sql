/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * ClickBench integration test. Runs all 43 ClickBench queries against a real Iceberg table
 * {@code iceberg.clickbench.hits} with Parquet format via the
 * {@code /_plugins/_trino_sql/v1/statement} REST endpoint.
 *
 * <p>The table is created using the Iceberg connector with TESTING_FILE_METASTORE catalog and
 * Parquet storage format. Sample data (3 rows) is inserted via INSERT statements, and all 43
 * ClickBench queries run against the real Iceberg/Parquet table.
 *
 * <p>The test verifies that each query parses, plans, and executes successfully (no errors).
 * Result correctness is not validated since we use a tiny sample dataset rather than the full
 * 14GB ClickBench dataset.
 */
public class ClickBenchIT {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

  private static String baseUrl;
  private static HttpClient httpClient;

  private static final String CATALOG = "iceberg";
  private static final String SCHEMA = "clickbench";
  private static final String TABLE_NAME = CATALOG + "." + SCHEMA + ".hits";

  /** DDL to create the schema in Iceberg. */
  private static final String CREATE_SCHEMA_SQL =
      "CREATE SCHEMA IF NOT EXISTS " + CATALOG + "." + SCHEMA;

  /** DDL to create the ClickBench hits table in the Iceberg connector with Parquet format. */
  private static final String CREATE_TABLE_SQL =
      "CREATE TABLE " + TABLE_NAME + " (\n"
          + "  WatchID BIGINT,\n"
          + "  JavaEnable INTEGER,\n"
          + "  Title VARCHAR,\n"
          + "  GoodEvent INTEGER,\n"
          + "  EventTime TIMESTAMP,\n"
          + "  EventDate DATE,\n"
          + "  CounterID INTEGER,\n"
          + "  ClientIP INTEGER,\n"
          + "  RegionID INTEGER,\n"
          + "  UserID BIGINT,\n"
          + "  CounterClass INTEGER,\n"
          + "  OS INTEGER,\n"
          + "  UserAgent INTEGER,\n"
          + "  URL VARCHAR,\n"
          + "  Referer VARCHAR,\n"
          + "  IsRefresh INTEGER,\n"
          + "  RefererCategoryID INTEGER,\n"
          + "  RefererRegionID INTEGER,\n"
          + "  URLCategoryID INTEGER,\n"
          + "  URLRegionID INTEGER,\n"
          + "  ResolutionWidth INTEGER,\n"
          + "  ResolutionHeight INTEGER,\n"
          + "  ResolutionDepth INTEGER,\n"
          + "  FlashMajor INTEGER,\n"
          + "  FlashMinor INTEGER,\n"
          + "  FlashMinor2 VARCHAR,\n"
          + "  NetMajor INTEGER,\n"
          + "  NetMinor INTEGER,\n"
          + "  UserAgentMajor INTEGER,\n"
          + "  UserAgentMinor VARCHAR,\n"
          + "  CookieEnable INTEGER,\n"
          + "  JavascriptEnable INTEGER,\n"
          + "  IsMobile INTEGER,\n"
          + "  MobilePhone INTEGER,\n"
          + "  MobilePhoneModel VARCHAR,\n"
          + "  Params VARCHAR,\n"
          + "  IPNetworkID INTEGER,\n"
          + "  TraficSourceID INTEGER,\n"
          + "  SearchEngineID INTEGER,\n"
          + "  SearchPhrase VARCHAR,\n"
          + "  AdvEngineID INTEGER,\n"
          + "  IsArtifical INTEGER,\n"
          + "  WindowClientWidth INTEGER,\n"
          + "  WindowClientHeight INTEGER,\n"
          + "  ClientTimeZone INTEGER,\n"
          + "  ClientEventTime TIMESTAMP,\n"
          + "  SilverlightVersion1 INTEGER,\n"
          + "  SilverlightVersion2 INTEGER,\n"
          + "  SilverlightVersion3 INTEGER,\n"
          + "  SilverlightVersion4 INTEGER,\n"
          + "  PageCharset VARCHAR,\n"
          + "  CodeVersion INTEGER,\n"
          + "  IsLink INTEGER,\n"
          + "  IsDownload INTEGER,\n"
          + "  IsNotBounce INTEGER,\n"
          + "  FUniqID BIGINT,\n"
          + "  OriginalURL VARCHAR,\n"
          + "  HID INTEGER,\n"
          + "  IsOldCounter INTEGER,\n"
          + "  IsEvent INTEGER,\n"
          + "  IsParameter INTEGER,\n"
          + "  DontCountHits INTEGER,\n"
          + "  WithHash INTEGER,\n"
          + "  HitColor VARCHAR,\n"
          + "  LocalEventTime TIMESTAMP,\n"
          + "  Age INTEGER,\n"
          + "  Sex INTEGER,\n"
          + "  Income INTEGER,\n"
          + "  Interests INTEGER,\n"
          + "  Robotness INTEGER,\n"
          + "  RemoteIP INTEGER,\n"
          + "  WindowName INTEGER,\n"
          + "  OpenerName INTEGER,\n"
          + "  HistoryLength INTEGER,\n"
          + "  BrowserLanguage VARCHAR,\n"
          + "  BrowserCountry VARCHAR,\n"
          + "  SocialNetwork VARCHAR,\n"
          + "  SocialAction VARCHAR,\n"
          + "  HTTPError INTEGER,\n"
          + "  SendTiming INTEGER,\n"
          + "  DNSTiming INTEGER,\n"
          + "  ConnectTiming INTEGER,\n"
          + "  ResponseStartTiming INTEGER,\n"
          + "  ResponseEndTiming INTEGER,\n"
          + "  FetchTiming INTEGER,\n"
          + "  SocialSourceNetworkID INTEGER,\n"
          + "  SocialSourcePage VARCHAR,\n"
          + "  ParamPrice BIGINT,\n"
          + "  ParamOrderID VARCHAR,\n"
          + "  ParamCurrency VARCHAR,\n"
          + "  ParamCurrencyID INTEGER,\n"
          + "  OpenstatServiceName VARCHAR,\n"
          + "  OpenstatCampaignID VARCHAR,\n"
          + "  OpenstatAdID VARCHAR,\n"
          + "  OpenstatSourceID VARCHAR,\n"
          + "  UTMSource VARCHAR,\n"
          + "  UTMMedium VARCHAR,\n"
          + "  UTMCampaign VARCHAR,\n"
          + "  UTMContent VARCHAR,\n"
          + "  UTMTerm VARCHAR,\n"
          + "  FromTag VARCHAR,\n"
          + "  HasGCLID INTEGER,\n"
          + "  RefererHash BIGINT,\n"
          + "  URLHash BIGINT,\n"
          + "  CLID INTEGER\n"
          + ") WITH (format = 'PARQUET')";

  /** INSERT statement with 3 sample rows covering all 105 columns. */
  private static final String INSERT_DATA_SQL =
      "INSERT INTO " + TABLE_NAME + " VALUES\n"
          + "  (BIGINT '6842488688498454842', INTEGER '1', 'Google Search Result',"
          + " INTEGER '1', TIMESTAMP '2013-07-15 10:30:00', DATE '2013-07-15',"
          + " 62, 1234567, 229, BIGINT '435090932899640449', INTEGER '0', INTEGER '4',"
          + " INTEGER '2', 'http://www.google.com/search?q=test',"
          + " 'http://www.google.com/', INTEGER '0', INTEGER '0', 229,"
          + " INTEGER '0', 229, INTEGER '1920', INTEGER '1080', INTEGER '24',"
          + " INTEGER '11', INTEGER '0', '', INTEGER '3', INTEGER '5',"
          + " INTEGER '48', '5.0', INTEGER '1', INTEGER '1', INTEGER '0',"
          + " INTEGER '0', 'iPhone', '', 12345, INTEGER '2',"
          + " INTEGER '2', 'test search phrase', INTEGER '2', INTEGER '0',"
          + " INTEGER '1920', INTEGER '1080', INTEGER '3',"
          + " TIMESTAMP '2013-07-15 10:30:00', INTEGER '5', INTEGER '1', 1970,"
          + " INTEGER '0', 'utf-8', 223, INTEGER '0', INTEGER '0',"
          + " INTEGER '1', BIGINT '5765412345678901234',"
          + " 'http://www.google.com/search?q=test', 12345,"
          + " INTEGER '0', INTEGER '0', INTEGER '0', INTEGER '0', INTEGER '0',"
          + " 'W', TIMESTAMP '2013-07-15 13:30:00', INTEGER '25', INTEGER '1',"
          + " INTEGER '3', INTEGER '10', INTEGER '0', 167772161, 0, 0,"
          + " INTEGER '5', 'en', 'US', '', '',"
          + " INTEGER '0', 100, 50, 30, 200, 300, 350, INTEGER '0', '',"
          + " BIGINT '0', '', '', INTEGER '0', '',"
          + " '', '', '', '', '',"
          + " '', '', '', '', INTEGER '0',"
          + " BIGINT '3594120000172545465', BIGINT '2868770270353813622', 42),\n"
          + "  (BIGINT '6842488688498454843', INTEGER '0', 'Yandex Main Page',"
          + " INTEGER '1', TIMESTAMP '2013-07-15 11:45:00', DATE '2013-07-15',"
          + " 62, 9876543, 1, BIGINT '435090932899640450', INTEGER '0', INTEGER '7',"
          + " INTEGER '1', 'http://yandex.ru/',"
          + " 'http://example.com/', INTEGER '0', INTEGER '0', 1,"
          + " INTEGER '0', 1, INTEGER '1366', INTEGER '768', INTEGER '24',"
          + " INTEGER '0', INTEGER '0', '', INTEGER '0', INTEGER '0',"
          + " INTEGER '37', '4.0', INTEGER '1', INTEGER '1', INTEGER '1',"
          + " INTEGER '1', 'Samsung Galaxy', '', 54321, INTEGER '-1',"
          + " INTEGER '0', '', INTEGER '0', INTEGER '0',"
          + " INTEGER '1366', INTEGER '768', INTEGER '5',"
          + " TIMESTAMP '2013-07-15 11:45:00', INTEGER '0', INTEGER '0', 0,"
          + " INTEGER '0', 'windows-1251', 100, INTEGER '1', INTEGER '0',"
          + " INTEGER '0', BIGINT '1234567890123456789',"
          + " 'http://yandex.ru/', 54321,"
          + " INTEGER '0', INTEGER '1', INTEGER '0', INTEGER '0', INTEGER '0',"
          + " 'R', TIMESTAMP '2013-07-15 16:45:00', INTEGER '30', INTEGER '2',"
          + " INTEGER '5', INTEGER '15', INTEGER '1', 167772162, 0, 0,"
          + " INTEGER '3', 'ru', 'RU', 'Facebook',"
          + " 'share', INTEGER '0', 200, 60, 40, 250, 400, 500,"
          + " INTEGER '1', 'http://facebook.com/page',"
          + " BIGINT '100', 'order123', 'USD', INTEGER '1',"
          + " 'service1', 'camp1', 'ad1', 'src1',"
          + " 'google', 'cpc', 'campaign1',"
          + " 'content1', 'term1', 'tag1', INTEGER '1',"
          + " BIGINT '7594120000172545465', BIGINT '3868770270353813622', 100),\n"
          + "  (BIGINT '6842488688498454844', INTEGER '1', 'OpenSearch Dashboard',"
          + " INTEGER '1', TIMESTAMP '2013-07-14 09:15:00', DATE '2013-07-14',"
          + " 62, 5555555, 100, BIGINT '435090932899640451', INTEGER '1', INTEGER '2',"
          + " INTEGER '3', 'http://opensearch.org/docs',"
          + " 'http://www.google.com/search?q=opensearch', INTEGER '0',"
          + " INTEGER '0', 100, INTEGER '0', 100, INTEGER '2560', INTEGER '1440',"
          + " INTEGER '32', INTEGER '11', INTEGER '0', '11.2',"
          + " INTEGER '4', INTEGER '0', INTEGER '52', '6.0',"
          + " INTEGER '1', INTEGER '1', INTEGER '0', INTEGER '0', '',"
          + " '', 67890, INTEGER '6', INTEGER '1',"
          + " 'opensearch documentation', INTEGER '0', INTEGER '0',"
          + " INTEGER '2560', INTEGER '1440', INTEGER '-5',"
          + " TIMESTAMP '2013-07-14 09:15:00', INTEGER '5', INTEGER '2', 2000,"
          + " INTEGER '0', 'utf-8', 500, INTEGER '0', INTEGER '0',"
          + " INTEGER '1', BIGINT '876543210987654321',"
          + " 'http://opensearch.org/docs', 67890,"
          + " INTEGER '1', INTEGER '0', INTEGER '1', INTEGER '0', INTEGER '1',"
          + " 'G', TIMESTAMP '2013-07-14 04:15:00', INTEGER '35', INTEGER '1',"
          + " INTEGER '4', INTEGER '20', INTEGER '0', 167772163, 0, 0,"
          + " INTEGER '10', 'en', 'GB', '', '',"
          + " INTEGER '0', 150, 45, 25, 180, 280, 320, INTEGER '0', '',"
          + " BIGINT '50', '', '', INTEGER '0', '',"
          + " '', '', '', 'bing',"
          + " 'organic', '', '', 'opensearch',"
          + " '', INTEGER '0', BIGINT '3594120000172545465',"
          + " BIGINT '2868770270353813622', 200)";

  @BeforeAll
  static void setUp() throws Exception {
    String cluster = System.getProperty("tests.rest.cluster", "localhost:9200");
    String firstHost = cluster.split(",")[0].trim();
    for (String part : cluster.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.startsWith("[")) {
        firstHost = trimmed;
        break;
      }
    }
    if (!firstHost.startsWith("http://") && !firstHost.startsWith("https://")) {
      firstHost = "http://" + firstHost;
    }
    baseUrl = firstHost;
    httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

    // Create schema, table, and insert data
    System.out.println("Setting up ClickBench Iceberg table: " + TABLE_NAME);

    // Drop table if it exists from a previous run
    executeTrinoSql("DROP TABLE IF EXISTS " + TABLE_NAME);

    // Create the Iceberg schema
    JsonNode schemaResult = executeTrinoSql(CREATE_SCHEMA_SQL);
    assertEquals(
        "FINISHED",
        getState(schemaResult),
        "CREATE SCHEMA should succeed: " + getErrorMessage(schemaResult));
    System.out.println("  CREATE SCHEMA: OK");

    // Create the hits table with Parquet format
    JsonNode createResult = executeTrinoSql(CREATE_TABLE_SQL);
    assertEquals(
        "FINISHED",
        getState(createResult),
        "CREATE TABLE should succeed: " + getErrorMessage(createResult));
    System.out.println("  CREATE TABLE: OK");

    // Insert sample data
    JsonNode insertResult = executeTrinoSql(INSERT_DATA_SQL);
    assertEquals(
        "FINISHED",
        getState(insertResult),
        "INSERT should succeed: " + getErrorMessage(insertResult));
    System.out.println("  INSERT 3 rows: OK");

    // Verify data was inserted
    JsonNode countResult = executeTrinoSql("SELECT COUNT(*) FROM " + TABLE_NAME);
    assertEquals("FINISHED", getState(countResult), "COUNT should succeed");
    System.out.println("  Verified row count in " + TABLE_NAME);
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (httpClient != null && baseUrl != null) {
      System.out.println("Cleaning up ClickBench Iceberg table: " + TABLE_NAME);
      try {
        executeTrinoSql("DROP TABLE IF EXISTS " + TABLE_NAME);
        System.out.println("  DROP TABLE: OK");
        executeTrinoSql("DROP SCHEMA IF EXISTS " + CATALOG + "." + SCHEMA);
        System.out.println("  DROP SCHEMA: OK");
      } catch (Exception e) {
        System.out.println("  Cleanup failed (non-fatal): " + e.getMessage());
      }
    }
  }

  @Test
  void runAllClickBenchQueries() throws Exception {
    // First verify the table is accessible
    JsonNode sanity = executeTrinoSql("SELECT COUNT(*) FROM " + TABLE_NAME);
    assertEquals("FINISHED", getState(sanity), "Sanity check: SELECT COUNT should succeed");

    List<String> queries = loadQueries();
    assertEquals(43, queries.size(), "Expected exactly 43 ClickBench queries");

    List<String> passed = new ArrayList<>();
    List<String> failed = new ArrayList<>();

    for (int i = 0; i < queries.size(); i++) {
      String query = queries.get(i);
      int queryNum = i + 1;

      long startMs = System.currentTimeMillis();
      try {
        JsonNode result = executeTrinoSql(query, CATALOG, SCHEMA);
        long elapsedMs = System.currentTimeMillis() - startMs;

        String state = getState(result);
        if ("FINISHED".equals(state)) {
          passed.add(String.format("Q%02d", queryNum));
          System.out.printf("  Q%02d PASS (%d ms)%n", queryNum, elapsedMs);
        } else {
          String errorMsg = getErrorMessage(result);
          failed.add(String.format("Q%02d: state=%s error=%s", queryNum, state, errorMsg));
          System.out.printf(
              "  Q%02d FAIL (%d ms): state=%s, error=%s%n",
              queryNum, elapsedMs, state, errorMsg);
        }
      } catch (Exception e) {
        long elapsedMs = System.currentTimeMillis() - startMs;
        failed.add(String.format("Q%02d: exception=%s", queryNum, e.getMessage()));
        System.out.printf("  Q%02d FAIL (%d ms): %s%n", queryNum, elapsedMs, e.getMessage());
      }
    }

    System.out.printf(
        "%nClickBench Results: %d/43 passed, %d/43 failed%n", passed.size(), failed.size());
    if (!failed.isEmpty()) {
      System.out.println("Failed queries:");
      for (String f : failed) {
        System.out.println("  " + f);
      }
    }

    assertEquals(
        43,
        passed.size(),
        String.format("Expected all 43 queries to pass. Failed: %s", failed));
  }

  // --- Helpers ---

  private static JsonNode executeTrinoSql(String sql)
      throws IOException, InterruptedException {
    return executeTrinoSql(sql, CATALOG, SCHEMA);
  }

  private static JsonNode executeTrinoSql(String sql, String catalog, String schema)
      throws IOException, InterruptedException {
    String statementUrl = baseUrl + "/_plugins/_trino_sql/v1/statement";

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(statementUrl))
            .header("Content-Type", "application/json")
            .header("X-Trino-User", "trino-test")
            .header("X-Trino-Catalog", catalog)
            .header("X-Trino-Schema", schema)
            .POST(HttpRequest.BodyPublishers.ofString(sql))
            .timeout(Duration.ofSeconds(120))
            .build();

    HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), "HTTP status should be 200, body: " + response.body());
    return MAPPER.readTree(response.body());
  }

  private static String getState(JsonNode result) {
    if (result.has("stats") && result.get("stats").has("state")) {
      return result.get("stats").get("state").asText();
    }
    return "UNKNOWN";
  }

  private static String getErrorMessage(JsonNode result) {
    if (result.has("error") && result.get("error").has("message")) {
      return result.get("error").get("message").asText();
    }
    return "none";
  }

  private List<String> loadQueries() throws IOException {
    String content = loadResource("clickbench/queries.sql");
    List<String> queries = new ArrayList<>();
    for (String line : content.split("\n")) {
      String trimmed = line.trim();
      if (!trimmed.isEmpty() && !trimmed.startsWith("--")) {
        if (trimmed.endsWith(";")) {
          trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }
        queries.add(trimmed);
      }
    }
    return queries;
  }

  private String loadResource(String path) throws IOException {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
      if (is == null) {
        throw new IOException("Resource not found: " + path);
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    }
  }
}
