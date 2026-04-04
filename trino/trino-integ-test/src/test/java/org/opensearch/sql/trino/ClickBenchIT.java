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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * ClickBench integration test. Runs all 43 ClickBench queries via the
 * {@code /_plugins/_trino_sql/v1/statement} REST endpoint.
 *
 * <p>Since the Memory connector's DDL/DML operations hang due to distributed task scheduling issues
 * under OpenSearch's security agent, each query is wrapped with a CTE that provides a small inline
 * sample dataset. This avoids the need to create physical tables.
 *
 * <p>The test verifies that each query parses, plans, and executes successfully (no errors). Result
 * correctness is not validated since we use a tiny sample dataset rather than the full 14GB
 * ClickBench dataset.
 */
public class ClickBenchIT {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

  private static String baseUrl;
  private static HttpClient httpClient;

  /**
   * CTE prefix providing the ClickBench {@code hits} table schema with 3 sample rows.
   * All 105 columns are covered with realistic data types matching the ClickBench schema.
   */
  private static final String HITS_CTE =
      "WITH hits AS (\n"
          + "  SELECT * FROM (VALUES\n"
          + "    (BIGINT '6842488688498454842', SMALLINT '1', VARCHAR 'Google Search Result',"
          + " SMALLINT '1', TIMESTAMP '2013-07-15 10:30:00', DATE '2013-07-15',"
          + " 62, 1234567, 229, BIGINT '435090932899640449', SMALLINT '0', SMALLINT '4',"
          + " SMALLINT '2', VARCHAR 'http://www.google.com/search?q=test',"
          + " VARCHAR 'http://www.google.com/', SMALLINT '0', SMALLINT '0', 229,"
          + " SMALLINT '0', 229, SMALLINT '1920', SMALLINT '1080', SMALLINT '24',"
          + " SMALLINT '11', SMALLINT '0', VARCHAR '', SMALLINT '3', SMALLINT '5',"
          + " SMALLINT '48', VARCHAR '5.0', SMALLINT '1', SMALLINT '1', SMALLINT '0',"
          + " SMALLINT '0', VARCHAR 'iPhone', VARCHAR '', 12345, SMALLINT '2',"
          + " SMALLINT '2', VARCHAR 'test search phrase', SMALLINT '2', SMALLINT '0',"
          + " SMALLINT '1920', SMALLINT '1080', SMALLINT '3',"
          + " TIMESTAMP '2013-07-15 10:30:00', SMALLINT '5', SMALLINT '1', 1970,"
          + " SMALLINT '0', VARCHAR 'utf-8', 223, SMALLINT '0', SMALLINT '0',"
          + " SMALLINT '1', BIGINT '5765412345678901234',"
          + " VARCHAR 'http://www.google.com/search?q=test', 12345,"
          + " SMALLINT '0', SMALLINT '0', SMALLINT '0', SMALLINT '0', SMALLINT '0',"
          + " VARCHAR 'W', TIMESTAMP '2013-07-15 13:30:00', SMALLINT '25', SMALLINT '1',"
          + " SMALLINT '3', SMALLINT '10', SMALLINT '0', 167772161, 0, 0,"
          + " SMALLINT '5', VARCHAR 'en', VARCHAR 'US', VARCHAR '', VARCHAR '',"
          + " SMALLINT '0', 100, 50, 30, 200, 300, 350, SMALLINT '0', VARCHAR '',"
          + " BIGINT '0', VARCHAR '', VARCHAR '', SMALLINT '0', VARCHAR '',"
          + " VARCHAR '', VARCHAR '', VARCHAR '', VARCHAR '', VARCHAR '',"
          + " VARCHAR '', VARCHAR '', VARCHAR '', VARCHAR '', SMALLINT '0',"
          + " BIGINT '3594120000172545465', BIGINT '2868770270353813622', 42),\n"
          + "    (BIGINT '6842488688498454843', SMALLINT '0', VARCHAR 'Yandex Main Page',"
          + " SMALLINT '1', TIMESTAMP '2013-07-15 11:45:00', DATE '2013-07-15',"
          + " 62, 9876543, 1, BIGINT '435090932899640450', SMALLINT '0', SMALLINT '7',"
          + " SMALLINT '1', VARCHAR 'http://yandex.ru/',"
          + " VARCHAR 'http://example.com/', SMALLINT '0', SMALLINT '0', 1,"
          + " SMALLINT '0', 1, SMALLINT '1366', SMALLINT '768', SMALLINT '24',"
          + " SMALLINT '0', SMALLINT '0', VARCHAR '', SMALLINT '0', SMALLINT '0',"
          + " SMALLINT '37', VARCHAR '4.0', SMALLINT '1', SMALLINT '1', SMALLINT '1',"
          + " SMALLINT '1', VARCHAR 'Samsung Galaxy', VARCHAR '', 54321, SMALLINT '-1',"
          + " SMALLINT '0', VARCHAR '', SMALLINT '0', SMALLINT '0',"
          + " SMALLINT '1366', SMALLINT '768', SMALLINT '5',"
          + " TIMESTAMP '2013-07-15 11:45:00', SMALLINT '0', SMALLINT '0', 0,"
          + " SMALLINT '0', VARCHAR 'windows-1251', 100, SMALLINT '1', SMALLINT '0',"
          + " SMALLINT '0', BIGINT '1234567890123456789',"
          + " VARCHAR 'http://yandex.ru/', 54321,"
          + " SMALLINT '0', SMALLINT '1', SMALLINT '0', SMALLINT '0', SMALLINT '0',"
          + " VARCHAR 'R', TIMESTAMP '2013-07-15 16:45:00', SMALLINT '30', SMALLINT '2',"
          + " SMALLINT '5', SMALLINT '15', SMALLINT '1', 167772162, 0, 0,"
          + " SMALLINT '3', VARCHAR 'ru', VARCHAR 'RU', VARCHAR 'Facebook',"
          + " VARCHAR 'share', SMALLINT '0', 200, 60, 40, 250, 400, 500,"
          + " SMALLINT '1', VARCHAR 'http://facebook.com/page',"
          + " BIGINT '100', VARCHAR 'order123', VARCHAR 'USD', SMALLINT '1',"
          + " VARCHAR 'service1', VARCHAR 'camp1', VARCHAR 'ad1', VARCHAR 'src1',"
          + " VARCHAR 'google', VARCHAR 'cpc', VARCHAR 'campaign1',"
          + " VARCHAR 'content1', VARCHAR 'term1', VARCHAR 'tag1', SMALLINT '1',"
          + " BIGINT '7594120000172545465', BIGINT '3868770270353813622', 100),\n"
          + "    (BIGINT '6842488688498454844', SMALLINT '1', VARCHAR 'OpenSearch Dashboard',"
          + " SMALLINT '1', TIMESTAMP '2013-07-14 09:15:00', DATE '2013-07-14',"
          + " 62, 5555555, 100, BIGINT '435090932899640451', SMALLINT '1', SMALLINT '2',"
          + " SMALLINT '3', VARCHAR 'http://opensearch.org/docs',"
          + " VARCHAR 'http://www.google.com/search?q=opensearch', SMALLINT '0',"
          + " SMALLINT '0', 100, SMALLINT '0', 100, SMALLINT '2560', SMALLINT '1440',"
          + " SMALLINT '32', SMALLINT '11', SMALLINT '0', VARCHAR '11.2',"
          + " SMALLINT '4', SMALLINT '0', SMALLINT '52', VARCHAR '6.0',"
          + " SMALLINT '1', SMALLINT '1', SMALLINT '0', SMALLINT '0', VARCHAR '',"
          + " VARCHAR '', 67890, SMALLINT '6', SMALLINT '1',"
          + " VARCHAR 'opensearch documentation', SMALLINT '0', SMALLINT '0',"
          + " SMALLINT '2560', SMALLINT '1440', SMALLINT '-5',"
          + " TIMESTAMP '2013-07-14 09:15:00', SMALLINT '5', SMALLINT '2', 2000,"
          + " SMALLINT '0', VARCHAR 'utf-8', 500, SMALLINT '0', SMALLINT '0',"
          + " SMALLINT '1', BIGINT '876543210987654321',"
          + " VARCHAR 'http://opensearch.org/docs', 67890,"
          + " SMALLINT '1', SMALLINT '0', SMALLINT '1', SMALLINT '0', SMALLINT '1',"
          + " VARCHAR 'G', TIMESTAMP '2013-07-14 04:15:00', SMALLINT '35', SMALLINT '1',"
          + " SMALLINT '4', SMALLINT '20', SMALLINT '0', 167772163, 0, 0,"
          + " SMALLINT '10', VARCHAR 'en', VARCHAR 'GB', VARCHAR '', VARCHAR '',"
          + " SMALLINT '0', 150, 45, 25, 180, 280, 320, SMALLINT '0', VARCHAR '',"
          + " BIGINT '50', VARCHAR '', VARCHAR '', SMALLINT '0', VARCHAR '',"
          + " VARCHAR '', VARCHAR '', VARCHAR '', VARCHAR 'bing',"
          + " VARCHAR 'organic', VARCHAR '', VARCHAR '', VARCHAR 'opensearch',"
          + " VARCHAR '', SMALLINT '0', BIGINT '3594120000172545465',"
          + " BIGINT '2868770270353813622', 200)\n"
          + "  ) AS t(WatchID, JavaEnable, Title, GoodEvent, EventTime, EventDate,"
          + " CounterID, ClientIP, RegionID, UserID, CounterClass, OS, UserAgent,"
          + " URL, Referer, IsRefresh, RefererCategoryID, RefererRegionID,"
          + " URLCategoryID, URLRegionID, ResolutionWidth, ResolutionHeight,"
          + " ResolutionDepth, FlashMajor, FlashMinor, FlashMinor2, NetMajor,"
          + " NetMinor, UserAgentMajor, UserAgentMinor, CookieEnable,"
          + " JavascriptEnable, IsMobile, MobilePhone, MobilePhoneModel, Params,"
          + " IPNetworkID, TraficSourceID, SearchEngineID, SearchPhrase,"
          + " AdvEngineID, IsArtifical, WindowClientWidth, WindowClientHeight,"
          + " ClientTimeZone, ClientEventTime, SilverlightVersion1,"
          + " SilverlightVersion2, SilverlightVersion3, SilverlightVersion4,"
          + " PageCharset, CodeVersion, IsLink, IsDownload, IsNotBounce, FUniqID,"
          + " OriginalURL, HID, IsOldCounter, IsEvent, IsParameter,"
          + " DontCountHits, WithHash, HitColor, LocalEventTime, Age, Sex,"
          + " Income, Interests, Robotness, RemoteIP, WindowName, OpenerName,"
          + " HistoryLength, BrowserLanguage, BrowserCountry, SocialNetwork,"
          + " SocialAction, HTTPError, SendTiming, DNSTiming, ConnectTiming,"
          + " ResponseStartTiming, ResponseEndTiming, FetchTiming,"
          + " SocialSourceNetworkID, SocialSourcePage, ParamPrice, ParamOrderID,"
          + " ParamCurrency, ParamCurrencyID, OpenstatServiceName,"
          + " OpenstatCampaignID, OpenstatAdID, OpenstatSourceID, UTMSource,"
          + " UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID,"
          + " RefererHash, URLHash, CLID)\n"
          + ")\n";

  @BeforeAll
  static void setUp() {
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
  }

  @Test
  void runAllClickBenchQueries() throws Exception {
    // First verify the endpoint works with a simple query
    JsonNode sanity = executeTrinoSql("SELECT 1");
    assertEquals("FINISHED", getState(sanity), "Sanity check: SELECT 1 should succeed");

    List<String> queries = loadQueries();
    assertEquals(43, queries.size(), "Expected exactly 43 ClickBench queries");

    List<String> passed = new ArrayList<>();
    List<String> failed = new ArrayList<>();

    for (int i = 0; i < queries.size(); i++) {
      String originalQuery = queries.get(i);
      int queryNum = i + 1;

      // Wrap the query with the CTE that provides the hits table
      String wrappedQuery = wrapWithCte(originalQuery);

      long startMs = System.currentTimeMillis();
      try {
        JsonNode result = executeTrinoSql(wrappedQuery);
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

  /**
   * Wrap a ClickBench query with the CTE that defines the hits table. Handles both
   * {@code SELECT ...} and {@code SELECT * FROM hits ...} patterns.
   */
  private String wrapWithCte(String query) {
    return HITS_CTE + query;
  }

  private JsonNode executeTrinoSql(String sql) throws IOException, InterruptedException {
    String statementUrl = baseUrl + "/_plugins/_trino_sql/v1/statement";

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(statementUrl))
            .header("Content-Type", "application/json")
            .header("X-Trino-User", "trino-test")
            .header("X-Trino-Catalog", "tpch")
            .header("X-Trino-Schema", "tiny")
            .POST(HttpRequest.BodyPublishers.ofString(sql))
            .timeout(Duration.ofSeconds(120))
            .build();

    HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), "HTTP status should be 200, body: " + response.body());
    return MAPPER.readTree(response.body());
  }

  private String getState(JsonNode result) {
    if (result.has("stats") && result.get("stats").has("state")) {
      return result.get("stats").get("state").asText();
    }
    return "UNKNOWN";
  }

  private String getErrorMessage(JsonNode result) {
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
