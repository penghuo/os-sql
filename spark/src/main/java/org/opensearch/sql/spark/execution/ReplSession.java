/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution;

import static org.opensearch.client.Requests.INDEX_CONTENT_TYPE;
import static org.opensearch.sql.spark.execution.ReplSessionMeta.standBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.index.engine.VersionConflictEngineException;

/** Session define the statement execution context. Each session is binding to one Spark Job. */
public class ReplSession {

  private static final Logger LOG = LogManager.getLogger(ReplSession.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Getter private final String sessionId;

  // session_state index name
  private final String indexName;

  // os client
  private final Client client;

  // todo, change to SessionId
  public ReplSession(String sessionId, String indexName, Client client) {
    this.sessionId = sessionId;
    this.indexName = indexName;
    this.client = client;
  }

  public void startSession(Runnable bootstrap) {
    create();
    bootstrap.run();
  }

  public ReplQueryId submit(String query) {
    ReplQueryId queryId = ReplQueryId.generate();
    ReplRequestMeta request = ReplRequestMeta.init(sessionId, queryId.getQueryId(), query);
    try {
      IndexRequest indexRequest = new IndexRequest(indexName);
      indexRequest.id(queryId.getQueryId());
      indexRequest.opType(DocWriteRequest.OpType.CREATE);
      indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
      AccessController.doPrivileged(
          (PrivilegedAction<IndexRequest>)
              () -> {
                try {
                  return indexRequest.source(
                      objectMapper.writeValueAsString(request), INDEX_CONTENT_TYPE);
                } catch (JsonProcessingException e) {
                  LOG.info("objectMapper exception", e);
                  throw new RuntimeException(e);
                }
              });
      IndexResponse indexResponse = client.index(indexRequest).actionGet();
      if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
        LOG.info("Successfully created {}", request);
      } else {
        throw new RuntimeException(
            "submit request failed with result : " + indexResponse.getResult().getLowercase());
      }
    } catch (VersionConflictEngineException exception) {
      throw new IllegalArgumentException(
          "A request already exists with id: " + request.getQueryId());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return queryId;
  }

  public Optional<ReplSessionState> sessionState() {
    GetRequest getRequest = new GetRequest().index(indexName).id(sessionId);
    try {
      GetResponse response = client.get(getRequest).actionGet();
      if (!response.isExists()) {
        return Optional.empty();
      }

      ReplSessionMeta metaData =
          AccessController.doPrivileged(
              (PrivilegedAction<ReplSessionMeta>)
                  () -> {
                    try {
                      return objectMapper.readValue(
                          response.getSourceAsString(), ReplSessionMeta.class);
                    } catch (JsonProcessingException e) {
                      LOG.info("objectMapper exception", e);
                      throw new RuntimeException(e);
                    }
                  });
      return Optional.ofNullable(metaData.getState());
    } catch (Exception e) {
      LOG.error("fetch state exception", e);
      throw new RuntimeException(e);
    }
  }

  private void create() {
    ReplSessionMeta replSessionMeta = standBy(sessionId);
    try {
      IndexRequest indexRequest = new IndexRequest(indexName);
      indexRequest.id(sessionId);
      indexRequest.opType(DocWriteRequest.OpType.CREATE);
      indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

      AccessController.doPrivileged(
          (PrivilegedAction<IndexRequest>)
              () -> {
                try {
                  return indexRequest.source(
                      objectMapper.writeValueAsString(replSessionMeta), INDEX_CONTENT_TYPE);
                } catch (JsonProcessingException e) {
                  LOG.info("objectMapper exception", e);
                  throw new RuntimeException(e);
                }
              });
      IndexResponse indexResponse = client.index(indexRequest).actionGet();
      if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
        LOG.info("Successfully created {}", replSessionMeta);
      } else {
        throw new RuntimeException(
            "Saving dataSource metadata information failed with result : "
                + indexResponse.getResult().getLowercase());
      }
    } catch (VersionConflictEngineException exception) {
      throw new IllegalArgumentException(
          "A datasource already exists with name: " + replSessionMeta.getSessionId());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
