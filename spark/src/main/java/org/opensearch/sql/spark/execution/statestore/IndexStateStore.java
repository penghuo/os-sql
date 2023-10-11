/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentType;

/**
 * StateStore based on OpenSearch Indices.
 */
@RequiredArgsConstructor
public class IndexStateStore {
  private static final Logger LOG = LogManager.getLogger();

  private final String indexName;
  private final Client client;

  public void create(StateModel state) {
    IndexRequest indexRequest = new IndexRequest(indexName)
        .id(state.getDocId())
        .source(state.getSource(), XContentType.JSON)
        .setIfSeqNo(state.getSeqNo())
        .setIfPrimaryTerm(state.getPrimaryTerm())
        .create(true)
        .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    IndexResponse indexResponse = client.index(indexRequest).actionGet();

    if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
      LOG.debug("Successfully created doc. id: {}", state.getDocId());
    } else {
      throw new RuntimeException(
          String.format(Locale.ROOT, "Failed create doc. id: %s, error: %s", state.getDocId(),
              indexResponse.getResult().getLowercase()));
    }
  }

  public <T extends StateModel> Optional<T> get(String docId, Class<T> subclass) {
    GetRequest getRequest = new GetRequest().index(indexName).id(docId);

    GetResponse getResponse = client.get(getRequest).actionGet();
    if (getResponse.isExists()) {
      return Optional.of(subclass.cast(new StateModel(getResponse.getId(),
          getResponse.getSourceAsString(),
          getResponse.getSeqNo(),
          getResponse.getPrimaryTerm())));
    } else {
      return Optional.empty();
    }
  }

  public StateModel update(StateModel state) {
    UpdateRequest updateRequest = new UpdateRequest().index(indexName)
        .id(state.getDocId())
        .setIfSeqNo(state.getSeqNo())
        .setIfPrimaryTerm(state.getPrimaryTerm())
        .doc(state.getSource(), XContentType.JSON)
        .fetchSource(true)
        .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

    UpdateResponse updateResponse = client.update(updateRequest).actionGet();
    if (updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
      LOG.debug("Successfully update doc. id: {}", state.getDocId());
      return new StateModel(state.getDocId(), state.getSource(), updateResponse.getSeqNo(),
          updateResponse.getPrimaryTerm());
    } else {
      throw new RuntimeException(
          String.format(Locale.ROOT, "Failed update doc. id: %s, error: %s", state.getDocId(),
              updateResponse.getResult().getLowercase()));
    }
  }

  public void delete(StateModel state) {
    throw new UnsupportedOperationException("delete");
  }

  public List<String> search(String query) {
    throw new UnsupportedOperationException("search");
  }
}
