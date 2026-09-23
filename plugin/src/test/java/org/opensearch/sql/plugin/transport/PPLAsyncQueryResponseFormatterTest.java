/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

public class PPLAsyncQueryResponseFormatterTest {
  private final PPLAsyncQueryResponseFormatter formatter = new PPLAsyncQueryResponseFormatter();

  @Test
  public void runningResponseContainsOnlyMinimalEmptySnapshot() {
    TransportPPLQueryResponse response =
        formatter.format(
            new PPLAsyncQueryService.JobSnapshot(
                "job-id", PPLAsyncQueryService.Status.RUNNING, null, null, -1));
    JSONObject json = json(response);

    assertTrue(response.isAsyncQueryResponse());
    assertEquals("job-id", json.getString("id"));
    assertEquals("RUNNING", json.getString("status"));
    assertEquals(0, json.getJSONArray("schema").length());
    assertEquals(0, json.getJSONArray("datarows").length());
    assertEquals(0, json.getInt("total"));
    assertFalse(json.has("progress"));
    assertFalse(json.has("size"));
    assertFalse(json.has("window"));
    assertFalse(json.has("update_mode"));
    assertFalse(json.has("start_time_in_millis"));
    assertFalse(json.has("expiration_time_in_millis"));
  }

  @Test
  public void successPreservesSchemaRowsAndOrderingWithoutSize() {
    LinkedHashMap<String, Object> row = new LinkedHashMap<>();
    row.put("event_id", 1L);
    row.put("email", "user1@example.com");
    QueryResponse response =
        new QueryResponse(
            new Schema(
                List.of(
                    new Column("event_id", null, ExprCoreType.LONG),
                    new Column("email", null, ExprCoreType.STRING))),
            List.of(ExprValueUtils.tupleValue(row)),
            null);

    JSONObject json =
        json(
            formatter.format(
                new PPLAsyncQueryService.JobSnapshot(
                    null, PPLAsyncQueryService.Status.SUCCEEDED, response, null, 42)));

    assertFalse(json.has("id"));
    assertEquals("SUCCEEDED", json.getString("status"));
    assertEquals(42, json.getLong("took"));
    assertEquals(2, json.getJSONArray("schema").length());
    assertEquals(1L, json.getJSONArray("datarows").getJSONArray(0).getLong(0));
    assertEquals("user1@example.com", json.getJSONArray("datarows").getJSONArray(0).getString(1));
    assertEquals(1, json.getInt("total"));
    assertFalse(json.has("size"));
  }

  @Test
  public void failedResponseContainsSanitizedLifecycleShape() {
    JSONObject json =
        json(
            formatter.format(
                new PPLAsyncQueryService.JobSnapshot(
                    "job-id",
                    PPLAsyncQueryService.Status.FAILED,
                    null,
                    new PPLAsyncQueryService.Failure(
                        "IllegalStateException", "query execution failed"),
                    10)));

    assertEquals("FAILED", json.getString("status"));
    assertEquals("IllegalStateException", json.getJSONObject("error").getString("type"));
    assertEquals("query execution failed", json.getJSONObject("error").getString("reason"));
    assertTrue(json.getJSONArray("datarows").isEmpty());
    assertFalse(json.has("took"));
  }

  private static JSONObject json(TransportPPLQueryResponse response) {
    return new JSONObject(response.getResult());
  }
}
