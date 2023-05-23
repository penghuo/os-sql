/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

/**
 * OpenSearch Bulk writer. More reading https://opensearch.org/docs/1.2/opensearch/rest-api/document-apis/bulk/.
 */
public class OpenSearchWriter extends FlintWriter {

  private final String indexName;

  private final StringWriter writer;

  private RestHighLevelClient client;

  public OpenSearchWriter(RestHighLevelClient client, String indexName) {
    this.client = client;
    this.indexName = indexName;
    this.writer = new StringWriter();
  }

  @Override public void write(char[] cbuf, int off, int len) {
    writer.write(cbuf, off, len);
  }

  /**
   * Todo. StringWriter is not efficient. it will copy the cbuf when create bytes.
   */
  @Override public void flush() throws IOException {
    try {
      byte[] bytes = writer.toString().getBytes();
      BulkRequest request = new BulkRequest(indexName).add(bytes, 0, bytes.length, XContentType.JSON);
      BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
      // fail entire bulk request even one doc failed.
      if (response.hasFailures() && Arrays.stream(response.getItems()).anyMatch(itemResp -> !isCreateConflict(itemResp))) {
        throw new RuntimeException(response.buildFailureMessage());
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to execute bulk request on index: %s", indexName), e);
    } finally {
      writer.close();
    }
  }

  @Override public void close() {
    try {
      if (client != null) {
        client.close();
        client = null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isCreateConflict(BulkItemResponse itemResp) {
    return itemResp.getOpType() == DocWriteRequest.OpType.CREATE && itemResp.getFailure().getStatus() == RestStatus.CONFLICT;
  }
}


