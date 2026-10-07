/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.COMPACT;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.spark.transport.model.AsyncQueryResult;

public class AsyncQueryResultResponseFormatterTest {

  private final ExecutionEngine.Schema schema =
      new ExecutionEngine.Schema(
          ImmutableList.of(
              new ExecutionEngine.Schema.Column("firstname", null, STRING),
              new ExecutionEngine.Schema.Column("age", null, INTEGER)));

  @Test
  void formatSparkQueryResponse() {
    assertSuccessfulResponse("success");
  }

  @Test
  void formatPplQueryResponse() {
    assertSuccessfulResponse("SUCCEEDED");
  }

  private void assertSuccessfulResponse(String status) {
    AsyncQueryResult response =
        new AsyncQueryResult(
            status,
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("firstname", "John", "age", 20)),
                tupleValue(ImmutableMap.of("firstname", "Smith", "age", 30))),
            null);
    AsyncQueryResultResponseFormatter formatter = new AsyncQueryResultResponseFormatter(COMPACT);
    assertEquals(
        "{\"status\":\""
            + status
            + "\",\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":"
            + "[[\"John\",20],[\"Smith\",30]],\"total\":2,\"size\":2}",
        formatter.format(response));
  }

  @Test
  void formatAsyncQueryError() {
    AsyncQueryResult response = new AsyncQueryResult("FAILED", null, null, "foo");
    AsyncQueryResultResponseFormatter formatter = new AsyncQueryResultResponseFormatter(COMPACT);
    assertEquals("{\"status\":\"FAILED\",\"error\":\"foo\"}", formatter.format(response));
  }

  @Test
  void formatAsyncQueryStructuredErrorEmitsObject() {
    // The PPL FAILED path populates errorDetails with the sync-shape error map; the formatter
    // must emit it as a JSON object, not a string.
    java.util.LinkedHashMap<String, Object> details = new java.util.LinkedHashMap<>();
    details.put("type", "IllegalArgumentException");
    details.put("reason", "Field [x] not found.");
    details.put("code", "FIELD_NOT_FOUND");
    AsyncQueryResult response = new AsyncQueryResult("FAILED", null, null, "fallback", details);
    AsyncQueryResultResponseFormatter formatter = new AsyncQueryResultResponseFormatter(COMPACT);
    assertEquals(
        "{\"status\":\"FAILED\",\"error\":{\"type\":\"IllegalArgumentException\","
            + "\"reason\":\"Field [x] not found.\",\"code\":\"FIELD_NOT_FOUND\"}}",
        formatter.format(response));
  }

  @Test
  void formatAsyncQueryEmptyErrorDetailsFallsBackToErrorString() {
    // Guard the "no renderer / empty map" case: formatter must not emit `"error": {}` and must
    // not swallow the Spark-style string error.
    AsyncQueryResult response =
        new AsyncQueryResult("FAILED", null, null, "fallback", java.util.Map.of());
    AsyncQueryResultResponseFormatter formatter = new AsyncQueryResultResponseFormatter(COMPACT);
    assertEquals("{\"status\":\"FAILED\",\"error\":\"fallback\"}", formatter.format(response));
  }

  @Test
  void formatSucceededResponseWithProgress() {
    AsyncQueryResult response =
        new AsyncQueryResult(
            "SUCCEEDED",
            schema,
            Arrays.asList(tupleValue(ImmutableMap.of("firstname", "John", "age", 20))),
            Cursor.None,
            null,
            null,
            QueryProgress.COMPLETE);

    assertEquals(
        "{\"status\":\"SUCCEEDED\",\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":[[\"John\",20]],"
            + "\"total\":1,\"size\":1,\"progress\":{\"fraction_done\":1.0}}",
        new AsyncQueryResultResponseFormatter(COMPACT).format(response));
  }

  @Test
  void formatRunningResponseWithFractionalProgress() {
    // A running job carries progress but no rows, which is the shape a client polls most often.
    AsyncQueryResult response =
        new AsyncQueryResult(
            "RUNNING",
            new ExecutionEngine.Schema(ImmutableList.of()),
            ImmutableList.of(),
            Cursor.None,
            null,
            null,
            new QueryProgress(0.44));

    assertEquals(
        "{\"status\":\"RUNNING\",\"progress\":{\"fraction_done\":0.44}}",
        new AsyncQueryResultResponseFormatter(COMPACT).format(response));
  }

  @Test
  void formatFailedResponseKeepsFrozenProgressAlongsideTheError() {
    AsyncQueryResult response =
        new AsyncQueryResult(
            "FAILED", null, null, Cursor.None, "boom", null, new QueryProgress(0.6));

    assertEquals(
        "{\"status\":\"FAILED\",\"error\":\"boom\",\"progress\":{\"fraction_done\":0.6}}",
        new AsyncQueryResultResponseFormatter(COMPACT).format(response));
  }

  @Test
  void formatFailedResponseKeepsStructuredErrorAndFrozenProgress() {
    java.util.LinkedHashMap<String, Object> details = new java.util.LinkedHashMap<>();
    details.put("type", "IllegalArgumentException");
    details.put("reason", "Field [x] not found.");
    details.put("code", "FIELD_NOT_FOUND");
    AsyncQueryResult response =
        new AsyncQueryResult(
            "FAILED", null, null, Cursor.None, "fallback", details, new QueryProgress(0.6));

    assertEquals(
        "{\"status\":\"FAILED\",\"error\":{\"type\":\"IllegalArgumentException\","
            + "\"reason\":\"Field [x] not found.\",\"code\":\"FIELD_NOT_FOUND\"},"
            + "\"progress\":{\"fraction_done\":0.6}}",
        new AsyncQueryResultResponseFormatter(COMPACT).format(response));
  }

  @Test
  void omitsProgressWhenNoneIsReported() {
    // The Spark async-query path reports no progress. Its response shape must stay exactly as it was, so the
    // object is omitted rather than emitted as a fabricated zero.
    AsyncQueryResult response =
        new AsyncQueryResult("SUCCESS", schema, ImmutableList.of(), Cursor.None, null, null);

    assertEquals(
        "{\"status\":\"SUCCESS\",\"schema\":[{\"name\":\"firstname\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"integer\"}],\"datarows\":[],\"total\":0,\"size\":0}",
        new AsyncQueryResultResponseFormatter(COMPACT).format(response));
  }
}
