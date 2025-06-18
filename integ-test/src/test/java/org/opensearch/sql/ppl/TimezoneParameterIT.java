/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Integration test for timezone parameter in PPL queries. Specifically tests the fix for GitHub
 * issue #3725: PPL date functions not handling timezone correctly.
 */
public class TimezoneParameterIT extends PPLIntegTestCase {

  private static final String TEST_INDEX_TIMESTAMP = "test_timestamp_index";

  @Override
  public void init() throws IOException {
    createIndexWithTimestampData();
  }

  /**
   * Creates a test index with documents having timestamps relative to current time. This is used to
   * test issue #3725 where date functions weren't handling timezones correctly.
   */
  public void createIndexWithTimestampData() throws IOException {
    String mapping =
        "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"@timestamp\": {\n"
            + "        \"type\": \"date\"\n"
            + "      },\n"
            + "      \"value\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

    Request createIndexRequest = new Request("PUT", "/" + TEST_INDEX_TIMESTAMP);
    createIndexRequest.setJsonEntity(mapping);
    TestUtils.performRequest(client(), createIndexRequest);

    Instant now = Instant.now();
    // Create documents with timestamps at now-1h, now-2h, and now-3h
    for (int i = 1; i <= 3; i++) {
      Map<String, Object> document = new HashMap<>();
      Instant timestamp = now.minusSeconds(i * 3600); // i hours ago
      String timestampString = DateTimeFormatter.ISO_INSTANT.format(timestamp);
      document.put("@timestamp", timestampString);
      document.put("value", i);

      JSONObject jsonDocument = new JSONObject(document);
      Request indexDocRequest = new Request("POST", "/" + TEST_INDEX_TIMESTAMP + "/_doc");
      indexDocRequest.setJsonEntity(jsonDocument.toString());
      TestUtils.performRequest(client(), indexDocRequest);
    }

    Request refreshRequest = new Request("POST", "/" + TEST_INDEX_TIMESTAMP + "/_refresh");
    TestUtils.performRequest(client(), refreshRequest);
  }

  /**
   * Test that issue #3725 is fixed:
   *
   * <p>This test creates documents with timestamps from the last few hours, then queries for
   * documents within the last day using date_sub(now(), INTERVAL 1 DAY) to ensure timezone handling
   * works properly in date comparison operations.
   */
  @Test
  public void testIssue3725() throws IOException {
    String query =
        "source=" + TEST_INDEX_TIMESTAMP + " | where @timestamp >= date_sub(now(), INTERVAL 1 DAY)";

    JSONObject result = executeQuery(query);
    assertEquals(
        "Query should return all 3 documents with default timezone ",
        3,
        result.getJSONArray("datarows").length());

    String[] timezones = {"UTC", "America/Los_Angeles", "Asia/Tokyo"};
    for (String timezone : timezones) {
      result = executeQueryWithTimezone(query, timezone);
      assertEquals(
          "Query should return all 3 documents with timezone " + timezone,
          3,
          result.getJSONArray("datarows").length());
    }
  }
}
