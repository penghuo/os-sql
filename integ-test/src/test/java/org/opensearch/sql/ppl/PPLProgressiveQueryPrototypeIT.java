/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.ASYNC_JOB_API_ENDPOINT;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

/**
 * Manual integration prototype for the two progressive result production paths.
 *
 * <p>This intentionally creates a larger data set than normal integration tests. It is used to
 * validate the design before production PRs are split and is not intended to remain in the final
 * test suite.
 */
public class PPLProgressiveQueryPrototypeIT extends PPLIntegTestCase {
  private static final String INDEX = "ppl-progressive-prototype";
  private static final int DOCUMENT_COUNT = 400_000;
  private static final int ROOT_RESULT_COUNT = 100_000;
  private static final int BULK_SIZE = 5_000;

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    setQuerySizeLimit(DOCUMENT_COUNT);
    if (!indexExists()) {
      createPrototypeIndex();
      indexDocuments();
    }
  }

  @Test
  public void rootRowsAreVisibleBeforeFinalResult() throws Exception {
    String query =
        "source="
            + INDEX
            + " | rex field=body \"level[^a-z]+(?<loglevel>error|warn|info)\""
            + " | fields event_id, body, loglevel | head "
            + ROOT_RESULT_COUNT;
    Observation observation = observe(query);
    JSONObject synchronous = executeSynchronous(query);

    assertNotNull("Expected a RUNNING response containing rows", observation.firstData());
    assertTrue(observation.firstData().getInt("total") > 0);
    assertTrue(observation.firstData().getInt("total") < synchronous.getInt("total"));
    assertEquals(
        synchronous.getJSONArray("schema").toString(),
        observation.finalResponse().getJSONArray("schema").toString());
    assertEquals(
        synchronous.getJSONArray("datarows").toString(),
        observation.finalResponse().getJSONArray("datarows").toString());
  }

  @Test
  public void aggregationReduceIsVisibleBeforeFinalResult() throws Exception {
    String query =
        "source="
            + INDEX
            + " | stats sum(event_id % 1000) as total"
            + " | eval doubled = total * 2"
            + " | fields total, doubled";
    Observation observation = observe(query);
    JSONObject synchronous = executeSynchronous(query);

    assertNotNull(
        "Expected a RUNNING aggregation response containing rows", observation.firstData());
    assertEquals(1, observation.firstData().getInt("total"));
    long partialCount = observation.firstData().getJSONArray("datarows").getJSONArray(0).getLong(0);
    long finalCount =
        observation.finalResponse().getJSONArray("datarows").getJSONArray(0).getLong(0);
    assertTrue(partialCount > 0);
    assertTrue(partialCount < finalCount);
    assertEquals(
        synchronous.getJSONArray("datarows").toString(),
        observation.finalResponse().getJSONArray("datarows").toString());
  }

  @Test
  public void aggregationReduceAtPhysicalRootIsVisibleBeforeFinalResult() throws Exception {
    String query = "source=" + INDEX + " | stats sum(event_id % 1000) as total";
    Observation observation = observe(query);
    JSONObject synchronous = executeSynchronous(query);

    assertNotNull(
        "Expected a RUNNING root aggregation response containing rows", observation.firstData());
    assertEquals(1, observation.firstData().getInt("total"));
    long partialValue = observation.firstData().getJSONArray("datarows").getJSONArray(0).getLong(0);
    long finalValue =
        observation.finalResponse().getJSONArray("datarows").getJSONArray(0).getLong(0);
    assertTrue(partialValue > 0);
    assertTrue(partialValue < finalValue);
    assertEquals(
        synchronous.getJSONArray("schema").toString(),
        observation.finalResponse().getJSONArray("schema").toString());
    assertEquals(
        synchronous.getJSONArray("datarows").toString(),
        observation.finalResponse().getJSONArray("datarows").toString());
  }

  @Test
  public void compositePagesAreVisibleBeforeFinalResult() throws Exception {
    String query =
        "source="
            + INDEX
            + " | stats count() as cnt by event_id"
            + " | fields event_id, cnt"
            + " | head "
            + ROOT_RESULT_COUNT;
    Observation observation = observe(query);
    JSONObject synchronous = executeSynchronous(query);

    assertNotNull("Expected a RUNNING composite response containing rows", observation.firstData());
    assertTrue(observation.firstData().getInt("total") > 0);
    assertTrue(observation.firstData().getInt("total") < synchronous.getInt("total"));
    assertEquals(
        synchronous.getJSONArray("schema").toString(),
        observation.finalResponse().getJSONArray("schema").toString());
    assertEquals(
        synchronous.getJSONArray("datarows").toString(),
        observation.finalResponse().getJSONArray("datarows").toString());
  }

  @Test
  public void countAsTotalHitsUsesTheStandardMapperAndMatchesSynchronousResult() throws Exception {
    String query =
        "source="
            + INDEX
            + " | where event_id >= 0"
            + " | stats count() as total"
            + " | eval doubled = total * 2"
            + " | fields total, doubled";
    Observation observation = observe(query);
    JSONObject synchronous = executeSynchronous(query);

    assertEquals(
        synchronous.getJSONArray("datarows").toString(),
        observation.finalResponse().getJSONArray("datarows").toString());
    assertEquals(
        400_000L, observation.finalResponse().getJSONArray("datarows").getJSONArray(0).getLong(0));
  }

  private Observation observe(String query) throws Exception {
    long startNanos = System.nanoTime();
    JSONObject submit = new JSONObject(getResponseBody(submit(query), true));
    assertEquals("RUNNING", submit.getString("status"));
    String id = submit.getString("id");

    JSONObject firstData = null;
    long firstDataMillis = -1;
    JSONObject response = submit;
    for (int attempt = 0; attempt < 2_000; attempt++) {
      response = get(id);
      if (firstData == null
          && "RUNNING".equals(response.getString("status"))
          && response.getInt("total") > 0) {
        firstData = response;
        firstDataMillis = elapsedMillis(startNanos);
      }
      if (!"RUNNING".equals(response.getString("status"))) {
        long finalMillis = elapsedMillis(startNanos);
        System.out.printf(
            Locale.ROOT,
            "progressive prototype first_data=%dms final=%dms partial_total=%d final_total=%d%n",
            firstDataMillis,
            finalMillis,
            firstData == null ? 0 : firstData.getInt("total"),
            response.getInt("total"));
        return new Observation(firstData, response, firstDataMillis, finalMillis);
      }
      Thread.sleep(5);
    }
    throw new AssertionError("Query did not complete: " + response);
  }

  private static long elapsedMillis(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }

  private Response submit(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("wait_for_completion_timeout", "0s")
            .put("keep_alive", "5m")
            .toString());
    return client().performRequest(request);
  }

  private JSONObject executeSynchronous(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(new JSONObject().put("query", query).toString());
    return new JSONObject(getResponseBody(client().performRequest(request), true));
  }

  private JSONObject get(String id) throws IOException {
    String endpoint = ASYNC_JOB_API_ENDPOINT.replace("{id}", id);
    return new JSONObject(
        getResponseBody(client().performRequest(new Request("GET", endpoint)), true));
  }

  private boolean indexExists() throws IOException {
    return isIndexExist(client(), INDEX);
  }

  private void createPrototypeIndex() throws IOException {
    Request request = new Request("PUT", "/" + INDEX);
    request.setJsonEntity(
        """
        {
          "settings": {
            "number_of_shards": 12,
            "number_of_replicas": 0,
            "refresh_interval": "-1"
          },
          "mappings": {
            "properties": {
              "event_id": {"type": "long"},
              "body": {"type": "keyword"},
              "productid": {"type": "keyword"}
            }
          }
        }
        """);
    client().performRequest(request);
  }

  private void indexDocuments() throws IOException {
    for (int start = 0; start < DOCUMENT_COUNT; start += BULK_SIZE) {
      int end = Math.min(DOCUMENT_COUNT, start + BULK_SIZE);
      StringBuilder bulk = new StringBuilder((end - start) * 160);
      for (int id = start; id < end; id++) {
        bulk.append("{\"index\":{\"_index\":\"").append(INDEX).append("\"}}\n");
        bulk.append("{\"event_id\":")
            .append(id)
            .append(",\"body\":\"level=")
            .append(id % 10 == 0 ? "warn" : "error")
            .append(" event=")
            .append(id)
            .append("\",\"productid\":\"pr")
            .append(id % 100)
            .append("\"}\n");
      }
      Request request = new Request("POST", "/_bulk");
      request.setJsonEntity(bulk.toString());
      request.setOptions(
          RequestOptions.DEFAULT.toBuilder()
              .addHeader("Content-Type", "application/x-ndjson")
              .build());
      JSONObject response = new JSONObject(getResponseBody(client().performRequest(request), true));
      if (response.getBoolean("errors")) {
        throw new AssertionError("Bulk indexing failed: " + response);
      }
    }
    client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
  }

  private record Observation(
      JSONObject firstData, JSONObject finalResponse, long firstDataMillis, long finalMillis) {}
}
