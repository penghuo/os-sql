/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;

/** Formats the minimal asynchronous lifecycle envelope. */
public final class PPLAsyncQueryResponseFormatter {

  /** Creates an asynchronous PPL response formatter. */
  public PPLAsyncQueryResponseFormatter() {}

  /**
   * Formats a point-in-time job snapshot.
   *
   * @param snapshot immutable lifecycle and result snapshot
   * @return transport response containing the asynchronous response JSON
   */
  public TransportPPLQueryResponse format(PPLAsyncQueryService.JobSnapshot snapshot) {
    JSONObject json =
        switch (snapshot) {
          case PPLAsyncQueryService.JobSnapshot.Running ignored -> emptyResult();
          case PPLAsyncQueryService.JobSnapshot.Succeeded succeeded ->
              formatRows(succeeded.response()).put("took", succeeded.tookMillis());
          case PPLAsyncQueryService.JobSnapshot.Failed failed ->
              emptyResult().put("error", formatFailure(failed.failure()));
        };

    snapshot.id().ifPresent(id -> json.put("id", id));
    json.put("status", snapshot.status().name());
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
    return new JSONObject().put("type", failure.type()).put("reason", failure.reason());
  }
}
