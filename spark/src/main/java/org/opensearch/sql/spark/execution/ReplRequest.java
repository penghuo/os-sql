/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.Client;

@RequiredArgsConstructor
public class ReplRequest {

  private static final Logger LOG = LogManager.getLogger(ReplSession.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final String indexName;

  private final Client client;

  private final Function<String, JSONObject> resultReader;

  public ReplRequestState state(ReplQueryId queryId) {
    GetRequest getRequest = new GetRequest().index(indexName).id(queryId.getQueryId());
    try {
      GetResponse response = client.get(getRequest).actionGet();
      ReplRequestMeta replRequestMeta =
          AccessController.doPrivileged(
              (PrivilegedAction<ReplRequestMeta>)
                  () -> {
                    try {
                      return objectMapper.readValue(
                          response.getSourceAsString(), ReplRequestMeta.class);
                    } catch (JsonProcessingException e) {
                      LOG.info("objectMapper exception", e);
                      throw new RuntimeException(e);
                    }
                  });
      return replRequestMeta.getState();
    } catch (Exception e) {
      LOG.error("fetch state exception", e);
      throw new RuntimeException(e);
    }
  }

  public JSONObject result(ReplQueryId queryId) {
    ReplRequestState state = state(queryId);
    if (state == ReplRequestState.SUCCESS) {
      JSONObject result = resultReader.apply(queryId.getQueryId());
      result.put("status", state.name().toUpperCase(Locale.ROOT));
      return result;
    } else {
      JSONObject result = new JSONObject();
      result.put("status", state.name().toUpperCase(Locale.ROOT));
      return result;
    }
  }

  // todo
  public void cancel() {}
}
