/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;

/** Formats the minimal asynchronous lifecycle envelope. */
final class PPLAsyncQueryResponseFormatter {

  TransportPPLQueryResponse format(PPLAsyncQueryService.JobSnapshot snapshot) {
    JSONObject json = snapshot.response() == null ? emptyResult() : formatRows(snapshot.response());

    if (snapshot.id() != null) {
      json.put("id", snapshot.id());
    }
    json.put("status", snapshot.status().name());

    if (snapshot.status() == PPLAsyncQueryService.Status.SUCCEEDED) {
      json.put("took", snapshot.tookMillis());
    } else if (snapshot.status() == PPLAsyncQueryService.Status.FAILED) {
      json.put("error", formatFailure(snapshot.failure()));
    }
    return new TransportPPLQueryResponse(json.toString(2));
  }

  TransportPPLQueryResponse format(PPLAsyncQueryService.DeleteResult result) {
    return new TransportPPLQueryResponse(
        new JSONObject().put("id", result.id()).put("status", result.status().name()).toString(2));
  }

  private static JSONObject formatRows(QueryResponse response) {
    SimpleJsonResponseFormatter formatter = new SimpleJsonResponseFormatter(PRETTY);
    JSONObject json =
        new JSONObject(
            formatter.format(
                new QueryResult(
                    response.getSchema(),
                    response.getResults(),
                    response.getCursor(),
                    PPL_SPEC,
                    response.getWarnings())));
    json.remove("size");
    return json;
  }

  private static JSONObject emptyResult() {
    return new JSONObject()
        .put("schema", new JSONArray())
        .put("datarows", new JSONArray())
        .put("total", 0);
  }

  private static JSONObject formatFailure(PPLAsyncQueryService.Failure failure) {
    String type = failure == null ? "Exception" : failure.type();
    String reason = failure == null ? "query execution failed" : failure.reason();
    return new JSONObject().put("type", type).put("reason", reason);
  }
}
