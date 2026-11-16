/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * A filter that folds to FALSE must plan as fast as any other query, also on an OpenTelemetry logs
 * index whose {@code attributes} object holds many sub-fields. The index exposes each sub-field as
 * a column of its own, so its row type is wide although it has 16 top-level fields. Calcite turns
 * such a filter into an empty {@code Values} over every column, and the collations it derives for
 * that node are quadratic in the column count, cubic with projections above it.
 */
public class CalciteWideIndexEmptyFilterIT extends PPLIntegTestCase {

  private static final String INDEX = "test_otel_logs_many_attributes";
  private static final int ATTRIBUTE_COUNT = 2000;

  /** Planning budget per query; a few tens of milliseconds are expected at this field count. */
  private static final double MAX_PLANNING_MS = 1000;

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    if (!TestUtils.isIndexExist(client(), INDEX)) {
      JSONObject keyword = new JSONObject().put("type", "keyword");
      JSONObject date = new JSONObject().put("type", "date");
      JSONObject number = new JSONObject().put("type", "long");
      JSONObject attributes = new JSONObject();
      for (int i = 0; i < ATTRIBUTE_COUNT; i++) {
        attributes.put("attr_" + i, keyword);
      }
      JSONObject properties =
          new JSONObject()
              .put("@timestamp", date)
              .put("observedTimestamp", date)
              .put("time", date)
              .put("traceId", keyword)
              .put("spanId", keyword)
              .put("severityText", keyword)
              .put("severityNumber", number)
              .put("body", new JSONObject().put("type", "text"))
              .put("flags", number)
              .put("droppedAttributesCount", number)
              .put("schemaUrl", keyword)
              .put("serviceName", keyword)
              .put("event", objectOf(new JSONObject().put("domain", keyword).put("name", keyword)))
              .put(
                  "instrumentationScope",
                  objectOf(new JSONObject().put("name", keyword).put("version", keyword)))
              .put(
                  "resource",
                  objectOf(
                      new JSONObject()
                          .put(
                              "attributes",
                              objectOf(
                                  new JSONObject()
                                      .put("service.name", keyword)
                                      .put("host.name", keyword)))))
              .put("attributes", objectOf(attributes));
      JSONObject body =
          new JSONObject()
              .put(
                  "settings",
                  new JSONObject().put("index.mapping.total_fields.limit", ATTRIBUTE_COUNT + 100))
              .put("mappings", new JSONObject().put("properties", properties));
      TestUtils.createIndexByRestClient(client(), INDEX, body.toString());

      Request doc = new Request("POST", "/" + INDEX + "/_doc?refresh=true");
      doc.setJsonEntity(
          "{\"@timestamp\": \"2026-01-01T00:00:00Z\", \"severityText\": \"INFO\","
              + " \"severityNumber\": 9, \"body\": \"hello\", \"serviceName\": \"svc\","
              + " \"attributes\": {\"attr_1\": \"x\"}}");
      client().performRequest(doc);
    }
  }

  @Test
  public void testLiteralComparisonFoldedToFalse() throws IOException {
    verifyEmptyAndFast(
        String.format(Locale.ROOT, "source=%s | where 'a' = 'b' | eval x = 1 | head 10", INDEX));
  }

  @Test
  public void testNumericFieldComparedToEmptyStringFoldedToFalse() throws IOException {
    verifyEmptyAndFast(
        String.format(
            Locale.ROOT,
            "source=%s | where isnotnull(severityNumber) and severityNumber != ''"
                + " | eval x = 1 | head 10",
            INDEX));
  }

  @Test
  public void testContradictoryFiltersMergedToFalse() throws IOException {
    verifyEmptyAndFast(
        String.format(
            Locale.ROOT,
            "source=%s | where severityText = 'a' | where severityText = 'b' | head 10",
            INDEX));
  }

  @Test
  public void testFilterFoldedToFalseWithProjectionsAndSort() throws IOException {
    verifyEmptyAndFast(
        String.format(
            Locale.ROOT,
            "source=%s | where 'a' = 'b' | eval x = 1, y = 2 | sort severityText"
                + " | fields severityText, x, y",
            INDEX));
  }

  private static JSONObject objectOf(JSONObject properties) {
    return new JSONObject().put("properties", properties);
  }

  private void verifyEmptyAndFast(String query) throws IOException {
    // The first run also pays for class loading and code generation, so only the second is timed.
    executeProfile(query);
    JSONObject result = executeProfile(query);
    assertEquals(0, result.getInt("total"));
    JSONObject phases = result.getJSONObject("profile").getJSONObject("phases");
    double planningMs =
        phases.getJSONObject("analyze").getDouble("time_ms")
            + phases.getJSONObject("optimize").getDouble("time_ms");
    assertThat(query, planningMs, lessThan(MAX_PLANNING_MS));
  }

  private JSONObject executeProfile(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(new JSONObject().put("query", query).put("profile", true).toString());
    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    request.setOptions(opts);
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response, true));
  }
}
